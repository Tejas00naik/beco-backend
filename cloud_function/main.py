"""
Gmail Pub/Sub Cloud Function

This is the entry point for the Cloud Function that processes Gmail Pub/Sub notifications,
fetches emails, extracts payment advice data, and handles Gmail watch refresh.
"""

import os
import sys
import base64
import json
import logging
import asyncio
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the directory containing this file to Python path so imports work correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import project modules
try:
    # Ensure src is in the Python path
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from src.batch_worker.batch_worker_v2 import BatchWorkerV2
    from src.repositories.firestore_dao import FirestoreDAO
    from src.external_apis.gcp.gmail_reader import GmailReader
    # Use separate import for Secret Manager (if needed)
    # from google.cloud import secretmanager
except ImportError as e:
    logger.error(f"Error importing required modules: {str(e)}")
    raise

# Helper functions for code optimization

def extract_message_id(message_data):
    """
    Helper function to extract message ID from different parts of the payload.
    
    Args:
        message_data (dict): The decoded Pub/Sub message data.
        
    Returns:
        str: The extracted message ID or None.
    """
    logger.info("EXTRACTING MESSAGE ID - DETAILED DEBUG:")
    
    # 1. Check top-level 'messageId'
    if 'messageId' in message_data:
        message_id = message_data.get('messageId')
        logger.info(f"✓ Found message ID at top level: {message_id}")
        return message_id
    
    # 2. Check top-level 'id'
    if 'id' in message_data:
        message_id = message_data.get('id')
        logger.info(f"✓ Found message ID as direct 'id' field: {message_id}")
        return message_id
    
    # 3. Check 'messagesAdded' structure (array of messages)
    if 'messagesAdded' in message_data and isinstance(message_data['messagesAdded'], list) and len(message_data['messagesAdded']) > 0:
        if 'message' in message_data['messagesAdded'][0] and 'id' in message_data['messagesAdded'][0]['message']:
            message_id = message_data['messagesAdded'][0]['message']['id']
            logger.info(f"✓ Found message ID in messagesAdded array: {message_id}")
            return message_id
    
    # 4. Check 'message' structure (nested 'message' object)
    if 'message' in message_data:
        logger.info("Checking 'message' structure")
        # 4a. Check for direct id in message
        if 'id' in message_data['message']:
            message_id = message_data['message']['id']
            logger.info(f"✓ Found message ID in message.id: {message_id}")
            return message_id
            
        # 4b. Check for data field that needs decoding (common in Pub/Sub)
        elif 'data' in message_data['message']:
            logger.info("Found message.data field, attempting to decode")
            try:
                # Sometimes data is already decoded, sometimes it needs decoding
                message_contents = None
                try:
                    message_contents = json.loads(message_data['message']['data'])
                    logger.info(f"Parsed message.data as JSON")
                except json.JSONDecodeError:
                    # Try base64 decoding first
                    try:
                        decoded_data = base64.b64decode(message_data['message']['data']).decode('utf-8')
                        logger.info(f"Base64 decoded message.data")
                        message_contents = json.loads(decoded_data)
                        logger.info(f"Parsed decoded message.data as JSON")
                    except:
                        logger.warning("Failed to base64 decode and parse message.data")
                        message_contents = message_data['message']['data']  # Use as-is as last resort
                        
                # Now extract ID from decoded contents
                if isinstance(message_contents, dict):
                    # Try standard ID fields
                    for id_field in ['id', 'messageId', 'message_id']:
                        if id_field in message_contents:
                            message_id = message_contents[id_field]
                            logger.info(f"✓ Found message ID in message.data.{id_field}: {message_id}")
                            return message_id
                            
            except Exception as e:
                logger.warning(f"Error extracting message ID from nested structure: {str(e)}")
    
    # No message ID found
    return None

def process_message_and_history(message_data, history_id):
    """
    Simplifies the logic of checking message_id and history_id.
    
    Args:
        message_data (dict): The decoded Pub/Sub message data.
        history_id (str): The history ID passed in the notification.
        
    Returns:
        tuple: (message_id, history_id)
    """
    message_id = extract_message_id(message_data)
    if message_id:
        logger.info(f"✅ Successfully extracted message ID: {message_id}")
    else:
        logger.warning("❌ Failed to extract message ID from notification payload")
    
    if not history_id:
        logger.warning("No historyId in notification. Using message_id if available.")
        history_id = message_id  # Default to message_id if no history_id is provided
    
    return message_id, history_id

def get_credentials_path():
    """
    Checks and returns the path of the credentials and token files.
    
    Returns:
        tuple: (credentials_path, token_path)
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(current_dir, 'secrets', 'email-client-secret.json')
    token_path = os.path.join(current_dir, 'secrets', 'token.json')
    
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credential files not found: {credentials_path}")
    
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"Token file not found: {token_path}")
    
    return credentials_path, token_path

async def handle_error(error_message, error_details=None):
    """
    Centralized error handling function.
    
    Args:
        error_message (str): The main error message.
        error_details (str): Additional details to log (optional).
        
    Returns:
        tuple: Error message and status code
    """
    if error_details:
        logger.error(f"{error_message}: {error_details}")
    else:
        logger.error(error_message)
    return f"{error_message}", 500


async def check_and_refresh_gmail_watch(email_reader, dao, email_address):
    """
    Check and refresh Gmail watch if needed.
    This function should be called on every cloud function invocation.
    
    Args:
        email_reader: The email reader instance (GmailReader)
        dao: Data access object for database operations
        email_address: Email address to check/refresh watch for
        
    Returns:
        bool: True if refresh was successful or not needed, False if error occurred
    """
    try:
        # Only attempt to refresh if we're using GmailReader (not MockEmailReader)
        from src.external_apis.gcp.gmail_reader import GmailReader
        
        logger.info(f"WATCH CHECK: Checking if email_reader is GmailReader: {isinstance(email_reader, GmailReader)}")
        if isinstance(email_reader, GmailReader) and email_address:
            logger.info(f"WATCH CHECK: Found email address for watch refresh: {email_address}")
            # Use the async version of check_and_refresh_watch
            logger.info("WATCH CHECK: Using async_check_and_refresh_watch")
            await email_reader.async_check_and_refresh_watch(email_address, dao)
            return True
        else:
            if not isinstance(email_reader, GmailReader):
                logger.warning("WATCH CHECK: Not using GmailReader, skipping watch refresh")
            if not email_address:
                logger.warning("WATCH CHECK: No valid email address provided for watch refresh")
            return True
    except Exception as watch_error:
        logger.error(f"WATCH CHECK: Error checking/refreshing Gmail watch: {str(watch_error)}")
        import traceback
        logger.error(f"WATCH CHECK: {traceback.format_exc()}")
        # Don't fail the entire process due to watch refresh issues
        return False

def access_secret(project_id, secret_id, version_id="latest"):
    """
    Access a secret stored locally in the secrets directory.
    
    Args:
        project_id: Not used, kept for compatibility
        secret_id: The secret file name without .json extension
        version_id: Not used, kept for compatibility
        
    Returns:
        Secret content as a string
    """
    try:
        # Use local file in the secrets directory
        secrets_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'secrets')
        secret_file = os.path.join(secrets_dir, f"{secret_id}.json")
        
        if not os.path.exists(secret_file):
            # Try without appending .json
            secret_file = os.path.join(secrets_dir, secret_id)
        
        if not os.path.exists(secret_file):
            raise FileNotFoundError(f"Secret file not found: {secret_id}")
            
        with open(secret_file, 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error accessing local secret {secret_id}: {str(e)}")
        raise

def process_pubsub_message(event, context):
    """
    Cloud Function entry point: process a Pub/Sub message from Gmail push notifications.
    This is a synchronous wrapper that calls the async implementation.
    
    Args:
        event: Pub/Sub message data
        context: Cloud Function context
        
    Returns:
        Response message or error
    """
    try:
        logger.info(f"Received Pub/Sub message, running synchronous wrapper")
        return asyncio.run(_async_process_pubsub_message(event, context))
    except Exception as e:
        logger.error(f"Error in synchronous wrapper: {str(e)}")
        return f"Error in Cloud Function: {str(e)}", 500


async def _async_process_pubsub_message(event, context):
    """
    Async implementation of the Cloud Function that processes Gmail push notifications.
    
    Args:
        event: Pub/Sub message data
        context: Cloud Function context
        
    Returns:
        Response message or error
    """
    try:
        logger.info(f"Function triggered by Pub/Sub message: {context.event_id}")
        
        # Extract the data from Pub/Sub message
        if 'data' not in event:
            logger.warning("No data field in Pub/Sub message")
            return "No data in message", 400
        
        # Decode and parse the message with enhanced logging
        pubsub_data = base64.b64decode(event['data']).decode('utf-8')
        logger.info(f"RAW PUB/SUB DATA: {pubsub_data}")
        
        message_data = json.loads(pubsub_data)
        logger.info(f"FULL PUB/SUB JSON PAYLOAD: {json.dumps(message_data, indent=2)}")
        
        # Check for required Gmail push notification fields
        # https://developers.google.com/gmail/api/guides/push#receiving_notifications
        if 'emailAddress' not in message_data:
            logger.warning("Missing emailAddress in Gmail notification")
            return "Invalid Gmail notification format", 400
            
        # Extract fields from the message data
        email_address = message_data.get('emailAddress')
        history_id = message_data.get('historyId')
        message_id = extract_message_id(message_data)
        
        logger.info(f"Initial notification data: email: {email_address}, historyId: {history_id}, messageId: {message_id}")
        
        # ALWAYS USE SINGLE MODE: If we don't have a message ID but we have history_id, get the message ID
        if not message_id and history_id:
            logger.info(f"No message ID found in notification. Looking up the most recent message ID based on historyId: {history_id}")
            try:
                # Get credential paths using the helper function
                credentials_path, token_path = get_credentials_path()
                logger.info(f"Using local credential files from {credentials_path} and {token_path}")
                
                # Initialize Gmail reader
                from src.external_apis.gcp.gmail_reader import GmailReader
                gmail_reader = GmailReader(
                    credentials_path=credentials_path, 
                    token_path=token_path, 
                    mailbox_id=email_address
                )
                
                # Get the most recent message ID based on history ID
                message_id = gmail_reader.get_most_recent_email_id_from_history(history_id)
                
                if message_id:
                    logger.info(f"Retrieved message ID {message_id} based on historyId {history_id}")
                else:
                    logger.warning(f"Unable to find a message ID for historyId {history_id}")
            except Exception as e:
                logger.error(f"Error retrieving message ID from history: {str(e)}")
                
        logger.info(f"Processing notification for email: {email_address}, historyId: {history_id}, messageId: {message_id}")
        
        # Skip processing if we couldn't get a message ID
        if not message_id:
            logger.warning("No message ID available for processing. Skipping.")
            return "No message ID available for processing", 200
        
        # Use local credential and token files
        try:
            # Get credential paths using the helper function
            credentials_path, token_path = get_credentials_path()
            logger.info(f"Using local credential files from {credentials_path} and {token_path}")
        except Exception as e:
            return await handle_error("Error accessing credentials", str(e))
        
        # Initialize DAO and BatchWorkerV2
        try:
            # Initialize the Firestore DAO with proper logging
            dao = FirestoreDAO()
            logger.info(f"Initialized FirestoreDAO for cloud function")
            
            # Initialize BatchWorkerV2
            # FORCE SINGLE MODE: Always use single mode processing
            run_mode = "single"
            
            logger.info(f"[MODE DETECTION] Message ID present: {'YES' if message_id else 'NO'}, Using forced run_mode: {run_mode}")
            logger.info(f"[SINGLE MODE ENFORCED] Processing in single message mode with message ID: {message_id}")
            logger.info(f"[PubSub Payload] email_address: {email_address}, historyId: {history_id}, messageId: {message_id}")
            
            worker = BatchWorkerV2(
                is_test=False,
                mailbox_id="default",  # Use default mailbox ID
                run_mode=run_mode,
                use_gmail=True,
                gmail_credentials_path=credentials_path,
                token_path=token_path  # Pass the token_path explicitly
            )
            
            # If we have a message ID, process ONLY that specific email
            if message_id:
                logger.info(f"Processing SINGLE email with ID: {message_id}")
                
                # Check if this email has already been processed (idempotency check)
                firestore_dao = FirestoreDAO()
                try:
                    # Query to see if this email ID is already in email_processing_log
                    collection_name = "email_processing_log"
                    query_result = await firestore_dao.query_documents(
                        collection_name, 
                        [("email_id", "==", message_id)]
                    )
                    
                    if query_result and len(query_result) > 0:
                        logger.info(f"Email ID {message_id} has already been processed. Skipping to prevent duplicate processing.")
                        return f"Email ID {message_id} already processed (idempotent operation)", 200
                    
                    logger.info(f"Email ID {message_id} has not been processed before. Proceeding with processing.")
                except Exception as e:
                    # If we can't check, proceed with processing anyway but log the error
                    logger.warning(f"Could not check if email {message_id} was already processed. Will attempt processing: {str(e)}")
                
                try:
                    # STRICT SINGLE EMAIL PROCESSING ENFORCEMENT:
                    # We are explicitly in single mode, only process this one email
                    logger.info(f"[STRICT-SINGLE-MODE] Starting processing for ONLY message ID: {message_id}")
                    
                    # Start a new batch run explicitly marked as single-mode
                    from src.batch_worker.batch_manager import BatchManager
                    from src.models.schemas import BatchRunStatus
                    
                    # Create a dedicated batch manager for this single email
                    batch_manager = BatchManager(
                        dao=firestore_dao,
                        is_test=False,
                        mailbox_id="single_mode",  # Explicitly mark as single mode
                        run_mode="single"  # Single email mode
                    )
                    # Start a batch run with default parameters
                    batch_run_id = await batch_manager.start_batch_run()
                    
                    # Now process the single email with this specific batch run context
                    success = await worker.process_single_email(message_id)
                    
                    # Update the batch run status
                    if success:
                        logger.info(f"[STRICT-SINGLE-MODE] Successfully processed single email: {message_id}")
                        await batch_manager.finish_batch_run(
                            batch_run_id=batch_run_id,
                            status=BatchRunStatus.SUCCESS,
                            emails_processed=1,
                            errors=0
                        )
                        
                        # Check and refresh Gmail watch for the single email case as well
                        logger.info("Performing Gmail watch check after single email processing")
                        await check_and_refresh_gmail_watch(
                            email_reader=worker.email_reader,
                            dao=firestore_dao,
                            email_address=email_address
                        )
                        
                        # Return after single email processing
                        return f"Successfully processed single email ID {message_id}", 200
                    else:
                        error_msg = f"Failed to process email ID {message_id}"
                        logger.warning(error_msg)
                        return error_msg, 500
                except Exception as e:
                    return await handle_error(f"Error processing email ID {message_id}", str(e))
            
            # Only get here if no message_id was provided - use history_id instead
            # We need a historyId to fetch recent changes
            if not history_id:
                logger.warning("No historyId in notification. Cannot process without a historyId.")
                return "No historyId provided in notification", 200
                
            # Create a repository for watch status
            watch_repo = None
            if dao:
                from src.repositories.gmail_watch_repository import GmailWatchRepository
                watch_repo = GmailWatchRepository(dao)
                
            # Get current stored historyId (if available)
            last_history_id = None
            if watch_repo:
                try:
                    watch_status = await watch_repo.get_watch_status()
                    if watch_status:
                        last_history_id = watch_status.history_id
                        logger.info(f"Found stored historyId: {last_history_id}")
                except Exception as e:
                    logger.warning(f"Error getting stored historyId: {str(e)}")
            
            # Use the received historyId as starting point if we don't have a stored one
            if not last_history_id:
                last_history_id = history_id
                logger.info(f"No stored historyId found, using received historyId: {last_history_id}")
            
            # Fetch message changes from history
            logger.info(f"Fetching message changes since historyId: {last_history_id}")
            message_ids = await worker.email_reader.get_history_changes(last_history_id)
            
            if not message_ids:
                logger.info("No new messages found in history")
                return "No new messages found", 200
                
            # Process each message found
            logger.info(f"Processing {len(message_ids)} new messages from history")
            success_count = 0
            error_count = 0
            
            for msg_id in message_ids:
                try:
                    # Process the single email
                    logger.info(f"Processing email with ID: {msg_id}")
                    success = await worker.process_single_email(msg_id)
                    
                    if success:
                        logger.info(f"Successfully processed email: {msg_id}")
                        success_count += 1
                    else:
                        logger.warning(f"Failed to process email: {msg_id}")
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error processing message {msg_id}: {str(e)}")
                    error_count += 1
            
            # Update stored historyId to the latest one received
            if watch_repo and history_id:
                try:
                    # Get current watch status
                    watch_status = await watch_repo.get_watch_status()
                    if watch_status:
                        # Update with new historyId
                        await watch_repo.save_watch_status(
                            email_address=email_address,
                            history_id=history_id,
                            expiration=watch_status.expiration,
                            # Fix: GmailWatchStatus doesn't have topic_name attribute
                            # Use a default pubsub_topic name instead
                            pubsub_topic="projects/vaulted-channel-462118-a5/topics/gmail-notifications"
                        )
                        logger.info(f"Updated stored historyId to: {history_id}")
                except Exception as e:
                    logger.error(f"Error updating stored historyId: {str(e)}")
            
            # Check and refresh Gmail watch at the end of processing
            # This ensures we do it on every cloud function invocation, not per email
            logger.info("Performing Gmail watch check after processing")
            await check_and_refresh_gmail_watch(
                email_reader=worker.email_reader,
                dao=dao,
                email_address=email_address
            )
            
            # Return results
            if error_count > 0:
                msg = f"Processed {success_count} emails successfully, {error_count} failed"
                logger.warning(msg)
                return msg, 200
            else:
                msg = f"Successfully processed {success_count} emails"
                logger.info(msg)
                return msg, 200
            
        except Exception as e:
            return await handle_error("Error in async processing", str(e))
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(error_details)
        return f"Error: {str(e)}", 500

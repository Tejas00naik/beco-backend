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


from src.batch_worker.batch_worker_v2 import BatchWorkerV2
from src.repositories.firestore_dao import FirestoreDAO
from src.external_apis.gcp.gmail_reader import GmailReader
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Helper functions for code optimization

def extract_history_id(message_data):
    """
    Helper function to extract history ID from Gmail push notification payload.
    Based on Gmail API docs, the notification only contains emailAddress and historyId.
    
    Args:
        message_data (dict): The decoded Pub/Sub message data.
        
    Returns:
        str: The extracted history ID or None.
    """
    logger.info("EXTRACTING HISTORY ID - DETAILED DEBUG:")
    
    # Direct extraction from top level - standard Gmail notification format
    if 'historyId' in message_data:
        history_id = message_data.get('historyId')
        logger.info(f"✓ Found history ID at top level: {history_id}")
        return history_id

    logger.warning("No history ID found in notification data")
    return None

def get_history_id_from_notification(message_data):
    """
    Extract history ID from Gmail push notification.
    According to Gmail API docs, notifications only contain emailAddress and historyId.
    
    Args:
        message_data (dict): The decoded Pub/Sub message data.
        
    Returns:
        str: The extracted history ID or None.
    """
    history_id = extract_history_id(message_data)
    
    if history_id:
        logger.info(f"✅ Successfully extracted history ID: {history_id}")
        return history_id
    else:
        logger.warning("❌ Failed to extract history ID from notification payload")
        return None

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
        history_id = get_history_id_from_notification(message_data)
        
        # Initialize FirestoreDAO and GmailReader early for watch refresh
        firestore_dao = FirestoreDAO()
        
        # Get paths to credential files
        client_secret_path, token_path = get_credentials_path()
        
        # Create email reader instance early
        gmail_reader = GmailReader(
            credentials_path=client_secret_path,
            token_path=token_path,
            mailbox_id=email_address
        )
        
        # Perform Gmail watch refresh at the beginning, regardless of what happens next
        logger.info("Performing Gmail watch check at the beginning of function execution")
        try:
            await check_and_refresh_gmail_watch(
                email_reader=gmail_reader,
                dao=firestore_dao,
                email_address=email_address
            )
            logger.info("Gmail watch refresh completed successfully")
        except Exception as watch_error:
            # Log but don't fail if watch refresh fails
            logger.error(f"Gmail watch refresh error (non-critical): {str(watch_error)}")
            # Continue with processing even if watch refresh fails
        
        logger.info(f"Initial notification data: email: {email_address}, historyId: {history_id}")
        
        # Check if we have a valid history_id to proceed
        if not history_id:
            logger.warning("No history ID found in notification. Cannot process without a historyId.")
            return "Invalid Gmail notification format - missing historyId", 400
            
        # IMPORTANT: NEW APPROACH - Always fetch the most recent emails instead of using history
        # This is more reliable than using historyId which might miss messages
        logger.info(f"USING NEW APPROACH: Ignoring historyId and fetching most recent emails directly")
        try:
            # Get the most recent email using our new method
            recent_emails = gmail_reader.get_most_recent_emails(num_emails=1)
            
            if not recent_emails or len(recent_emails) == 0:
                logger.info(f"No recent emails found in the mailbox")
                return "No recent emails to process", 200
                
            # Get the most recent email
            recent_email = recent_emails[0]
            message_id = recent_email.get('gmail_id')  # This is the Gmail message ID
            
            logger.info(f"Retrieved most recent email with ID: {message_id}")
            
            # For debugging, log email details
            logger.info(f"Recent email details: Subject: {recent_email.get('subject')}, From: {recent_email.get('sender_mail')}")
            
            # At this point, we should have a valid message_id
            # If not, it means something went wrong
            if not message_id:
                logger.error("Critical error: No message ID available in the most recent email")
                return "Critical error: Failed to determine message ID for processing", 500
            
        except Exception as e:
            logger.error(f"Error retrieving message IDs from history: {str(e)}")
            return f"Error retrieving message IDs from history: {str(e)}", 500
                
        logger.info(f"Processing notification for email: {email_address}, historyId: {history_id}, messageId: {message_id}")
        
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
           
            logger.info(f"Processing SINGLE email with ID: {message_id}")
            
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
                    # Return after single email processing
                    return f"Successfully processed single email ID {message_id}", 200
                else:
                    error_msg = f"Failed to process email ID {message_id}"
                    logger.warning(error_msg)
                    return error_msg, 500
            except Exception as e:
                return await handle_error(f"Error processing email ID {message_id}", str(e))
                            
        except Exception as e:
            return await handle_error("Error in async processing", str(e))
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(error_details)
        return f"Error: {str(e)}", 500

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
    from src.batch_worker.batch_worker_v2 import BatchWorkerV2
    from src.repositories.firestore_dao import FirestoreDAO
    from src.external_apis.gcp.gmail_reader import GmailReader
    from google.cloud import secretmanager
except ImportError as e:
    logger.error(f"Error importing required modules: {str(e)}")
    raise

def access_secret(project_id, secret_id, version_id="latest"):
    """
    Access a secret stored in Google Secret Manager.
    
    Args:
        project_id: Google Cloud project ID
        secret_id: The secret ID
        version_id: Version of the secret (default "latest")
        
    Returns:
        Secret content as a string
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error accessing secret {secret_id}: {str(e)}")
        raise

def process_pubsub_message(event, context):
    """
    Cloud Function entry point: process a Pub/Sub message from Gmail push notifications.
    
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
        
        # Decode and parse the message
        pubsub_data = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(pubsub_data)
        logger.info(f"Received message data: {message_data}")
        
        # Check for required Gmail push notification fields
        # https://developers.google.com/gmail/api/guides/push#receiving_notifications
        if 'emailAddress' not in message_data:
            logger.warning("Missing emailAddress in Gmail notification")
            return "Invalid Gmail notification format", 400
            
        # Extract relevant fields from the Gmail notification
        email_address = message_data.get('emailAddress')
        history_id = message_data.get('historyId')
        
        # Get the message ID if available (not always present in notifications)
        message_id = None
        if 'message' in message_data and 'data' in message_data['message']:
            try:
                message_contents = json.loads(message_data['message']['data'])
                message_id = message_contents.get('id')
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Error extracting message ID: {str(e)}")
        
        logger.info(f"Processing notification for email: {email_address}, historyId: {history_id}, messageId: {message_id}")
        
        # If we don't have a specific message ID, we'll need to query Gmail for recent changes
        if not message_id:
            logger.warning("No message ID in notification. Would need to fetch history changes.")
            # For now, we'll just return as this requires additional implementation
            return "No specific message to process", 200
        
        # Get credentials and token from Secret Manager
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'vaulted-channel-462118-a5')
        try:
            credentials_json = access_secret(project_id, 'gmail_credentials')
            token_json = access_secret(project_id, 'gmail_token')
            
            # Write credentials and token to temporary files
            credentials_path = '/tmp/client_secret.json'
            token_path = '/tmp/token.json'
            
            with open(credentials_path, 'w') as f:
                f.write(credentials_json)
                
            with open(token_path, 'w') as f:
                f.write(token_json)
                
            logger.info("Credentials and token retrieved from Secret Manager")
        except Exception as e:
            logger.error(f"Error accessing credentials: {str(e)}")
            return f"Error accessing credentials: {str(e)}", 500
        
        # Initialize DAO and BatchWorkerV2
        try:
            dao = FirestoreDAO()
            worker = BatchWorkerV2(
                is_test=False,
                mailbox_id="default",  # Use default mailbox ID
                run_mode="incremental",
                use_gmail=True,
                gmail_credentials_path=credentials_path,
                token_path=token_path
            )
            
            logger.info("BatchWorkerV2 initialized. Starting single email processing.")
            
            # Process the single email using asyncio
            asyncio.run(worker.process_single_email(message_id))
            
            logger.info(f"Successfully processed email: {message_id}")
            return f"Email {message_id} processed successfully", 200
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"Error processing email: {str(e)}")
            logger.error(error_details)
            return f"Error processing email: {str(e)}", 500
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(error_details)
        return f"Error: {str(e)}", 500

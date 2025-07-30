"""
Gmail Reader Adapter

This module provides an implementation of an email reader that retrieves
emails from Gmail using the Gmail API. It supports both incremental fetch
(based on last processed timestamp) and full refresh modes.
"""

import os
import base64
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
from email.parser import BytesParser
from email.policy import default
from typing import Dict, List, Optional, Any, Tuple

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)

# Define OAuth2 scopes needed for Gmail API
# This scope allows read-only access to Gmail and setting up watch notifications
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']

# Using GmailWatchStatus from repository instead
from src.repositories.gmail_watch_repository import GmailWatchStatus, GmailWatchRepository

# Indicate that Gmail adapter is available
GMAIL_AVAILABLE = True

class GmailReader:
    """Gmail API-based email reader implementation."""

    def __init__(
        self, 
        credentials_path: str,
        token_path: str = None,
        mailbox_id: str = "default",
        data_path: str = None
    ):
        """
        Initialize the Gmail reader.
        
        Args:
            credentials_path: Path to client_secret.json file from Google Cloud Console
            token_path: Path to save/load the token.json file (optional)
            mailbox_id: Identifier for the mailbox being processed
            data_path: Path to store downloaded email data (optional)
        """
        self.credentials_path = credentials_path
        self.token_path = token_path or os.path.join(os.getcwd(), "token.json")
        self.mailbox_id = mailbox_id
        
        # Use /tmp for Cloud Function compatibility
        if os.environ.get("FUNCTION_TARGET"):
            # Running in Cloud Functions environment
            self.data_path = data_path or os.path.join("/tmp", "gmail_data")
            logger.info(f"Running in Cloud Functions environment, using temp directory: {self.data_path}")
        else:
            # Running in local environment
            self.data_path = data_path or os.path.join(os.getcwd(), "gmail_data")
            logger.info(f"Running in local environment, using directory: {self.data_path}")
            
        self.service = None
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_path, exist_ok=True)
        
        # Authenticate and create the Gmail API service
        self._authenticate()
        
        logger.info(f"Initialized GmailReader for mailbox_id: {mailbox_id}")

    def _authenticate(self):
        """Authenticate with Gmail API and create service."""
        creds = None
        force_new_token = False
        is_cloud_function = os.environ.get("FUNCTION_TARGET") is not None
        
        # Check if token file exists and load it
        if os.path.exists(self.token_path):
            try:
                creds = Credentials.from_authorized_user_info(
                    json.load(open(self.token_path)), SCOPES
                )
            except Exception as e:
                logger.warning(f"Error loading credentials: {str(e)}")
                if is_cloud_function:
                    raise Exception(f"Failed to load token file in Cloud Function environment: {str(e)}")
                # If there's an error loading credentials in local env, force new token
                force_new_token = True
        
        # If credentials don't exist, are invalid, or we need to force new token
        if not creds or not creds.valid or force_new_token:
            # If we have existing credentials that just need refresh
            if creds and creds.expired and creds.refresh_token and not force_new_token:
                try:
                    creds.refresh(Request())
                    # In Cloud Functions, we can't save the refreshed token
                    # Just use the refreshed credentials in memory
                    if not is_cloud_function:
                        # Save the refreshed token locally
                        with open(self.token_path, 'w') as token:
                            token.write(creds.to_json())
                except Exception as e:
                    logger.warning(f"Error refreshing token (possibly due to scope change): {str(e)}")
                    if is_cloud_function:
                        raise Exception(f"Failed to refresh token in Cloud Function environment: {str(e)}")
                    # If refresh fails in local env, force new token
                    force_new_token = True
            
            # If we need a completely new token - this can only happen in local environment
            if (not creds or force_new_token) and not is_cloud_function:
                logger.info("Getting new Gmail API token with updated scopes")
                # First try to delete the existing token file if it exists
                if os.path.exists(self.token_path):
                    try:
                        os.remove(self.token_path)
                        logger.info(f"Deleted old token file at {self.token_path}")
                    except Exception as e:
                        logger.warning(f"Error deleting old token file: {str(e)}")
                
                # Run OAuth2 flow to get new credentials
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=0)
                
                # Save the credentials for the next run
                with open(self.token_path, 'w') as token:
                    token.write(creds.to_json())
            elif (not creds or force_new_token) and is_cloud_function:
                # In Cloud Functions, if we can't load or refresh token, we can't get a new one
                raise Exception("Token is invalid/expired in Cloud Function environment and cannot be refreshed. Generate a new token locally and redeploy.")
        
        # Create the Gmail API service
        self.service = build('gmail', 'v1', credentials=creds)
        logger.info("Successfully authenticated with Gmail API")

    def _build_query(self, since_timestamp: Optional[datetime] = None) -> str:
        """
        Build Gmail search query to filter emails.
        
        Args:
            since_timestamp: Only get emails after this timestamp (for incremental mode)
            
        Returns:
            Gmail search query string
        """
        query_parts = []
        
        # Add date filter for incremental mode
        if since_timestamp:
            # Format as RFC 3339 timestamp format for more precise filtering
            # Gmail API uses after: with format YYYY/MM/DD for date filtering
            # For more precision we can use rfc3339 format
            date_str = since_timestamp.strftime("%Y/%m/%d")
            time_str = since_timestamp.strftime("%H:%M:%S")
            
            # Format date as YYYY/MM/DD for Gmail query
            query_parts.append(f"after:{date_str}")
            
            # You can also add time filtering if Gmail API supports it:
            # query_parts.append(f"newer_than:{int((datetime.now() - since_timestamp).total_seconds())}s")
        
        # Add mailbox specific filters if needed
        if hasattr(self, 'query_filters') and self.query_filters:
            query_parts.extend(self.query_filters)
            
        # Return the combined query, or empty string if no parts
        return " ".join(query_parts)

    def _get_email_content(self, email_id: str) -> Dict[str, Any]:
        """
        Get full content of an email using its ID.
        
        Args:
            email_id: Gmail message ID
            
        Returns:
            Dict containing email data including raw content for GCS storage
        """
        # Generate a UUID for this email
        email_uuid = str(uuid.uuid4())
        
        # Get the full email content from Gmail API
        full_message = self.service.users().messages().get(
            userId='me', id=email_id, format='full'
        ).execute()
        
        # Get raw email content for storage in GCS
        raw_message = self.service.users().messages().get(
            userId='me', id=email_id, format='raw'
        ).execute()
        
        raw_email_data = base64.urlsafe_b64decode(raw_message['raw'])
        
        # Use BytesParser to parse the raw email
        email_message = BytesParser(policy=default).parsebytes(raw_email_data)
        
        # Extract basic metadata
        headers = full_message['payload']['headers']
        
        # Extract subject, sender, and received date from headers
        subject = None
        sender = None
        received = None
        
        for header in headers:
            name = header['name'].lower()
            if name == 'subject':
                subject = header['value']
            elif name == 'from':
                sender = header['value']
                # Extract email address if in format "Name <email>"
                if '<' in sender and '>' in sender:
                    sender = sender.split('<')[1].split('>')[0]
            elif name == 'date':
                received = header['value']
        
        # Extract plain text and HTML content
        text_content = None
        html_content = None
        attachments = []
        
        def extract_parts(message_part):
            """Recursive function to extract parts from the email"""
            nonlocal text_content, html_content, attachments
            
            if message_part.is_multipart():
                # Multipart message, process each part
                for part in message_part.iter_parts():
                    extract_parts(part)
            else:
                # Single part, check content type
                content_type = message_part.get_content_type()
                disposition = str(message_part.get("Content-Disposition") or "")
                
                # Handle attachments (has a filename or content-disposition is attachment)
                if "attachment" in disposition or message_part.get_filename():
                    filename = message_part.get_filename()
                    if not filename:
                        filename = f"unnamed_attachment_{len(attachments)}"
                    
                    content = message_part.get_payload(decode=True)
                    attachments.append({
                        "filename": filename,
                        "content": content,
                        "content_type": content_type
                    })
                else:
                    # Handle text content
                    if content_type == "text/plain" and not text_content:
                        text_content = message_part.get_payload(decode=True).decode(message_part.get_content_charset() or 'utf-8', errors='replace')
                    elif content_type == "text/html" and not html_content:
                        html_content = message_part.get_payload(decode=True).decode(message_part.get_content_charset() or 'utf-8', errors='replace')
        
        # Extract all parts from the email message
        try:
            extract_parts(email_message)
        except Exception as e:
            logger.warning(f"Error extracting email parts: {str(e)}")
        
        # Check if this is a forwarded email and try to extract the original sender
        original_sender = None
        
        if subject and ('fwd:' in subject.lower() or 'fw:' in subject.lower()):
            try:
                # Try to extract original sender from plain text content
                if text_content:
                    # Common patterns in forwarded emails
                    patterns = [
                        r'From:\s*([^<]+)<([^>]+)>',  # From: Name <email@domain.com>
                        r'From:\s*"?([^"]+)"?\s*<([^>]+)>',  # From: "Name" <email@domain.com>
                        r'From:\s*([^\n]+)'  # From: some text (fallback)
                    ]
                    
                    for pattern in patterns:
                        match = re.search(pattern, text_content)
                        if match:
                            if len(match.groups()) > 1 and '@' in match.group(2):
                                original_sender = match.group(2).strip()
                            else:
                                original_sender = match.group(1).strip()
                            break
            except Exception as e:
                logger.warning(f"Error parsing forwarded email for original sender: {str(e)}")
                # Fall back to sender if we can't extract the original sender
                original_sender = None
            
        # Parse received date
        try:
            received_datetime = parsedate_to_datetime(received)
        except Exception as e:
            logger.warning(f"Error parsing received date: {str(e)}")
            received_datetime = datetime.now()
        
        # Create the email object with all components
        email_obj = {
            "email_id": email_uuid,  # Use UUID instead of Gmail ID
            "gmail_id": email_id,  # Keep original Gmail ID for reference
            "raw_email": raw_email_data,  # Raw email data as bytes for storage in GCS
            "text_content": text_content,  # Plain text version of the email
            "html_content": html_content,  # HTML version of the email
            "attachments": attachments,  # List of attachments
            "sender_mail": sender,
            "original_sender_mail": original_sender,
            "received_at": received_datetime,
            "subject": subject,
            "mailbox_id": self.mailbox_id,
            "attachment_count": len(attachments)
        }
        
        logger.info(f"Retrieved email {email_id} (UUID: {email_uuid}) with {len(attachments)} attachments")
        
        return email_obj

    def get_unprocessed_emails(self, since_timestamp: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get unprocessed emails from Gmail.
        
        Args:
            since_timestamp: Only get emails after this timestamp (for incremental mode)
            
        Returns:
            List of email objects that haven't been processed yet
        """
        try:
            # Build the search query
            query = self._build_query(since_timestamp)
            
            # Call Gmail API to list messages matching the query
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=100
            ).execute()
            
            messages = results.get('messages', [])
            new_emails = []
            
            for message in messages:
                email_id = message['id']
                email_obj = self._get_email_content(email_id)
                new_emails.append(email_obj)
            
            logger.info(f"Retrieved {len(new_emails)} emails from Gmail")
            return new_emails
            
        except HttpError as error:
            logger.error(f"An error occurred while accessing Gmail API: {error}")
            return []

    def mark_as_processed(self, email_ids: List[str]) -> None:
        """
        Mark emails as processed.
        
        Args:
            email_ids: List of email IDs to mark as processed
        """
        # In Gmail, we don't need to mark anything as Gmail doesn't track what we've processed
        # This is just for API compatibility with MockEmailReader
        logger.info(f"Marked {len(email_ids)} emails as processed (no-op for Gmail)")
        pass
        
    def get_email_by_id(self, email_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific email by its ID for single-email processing mode.
        
        Args:
            email_id: The Gmail message ID or UUID from email_log
            
        Returns:
            Dict containing email data or None if not found
        """
        logger.info(f"Fetching single email with ID: {email_id}")
        
        # Check if the ID is a UUID format (from our database) or a Gmail message ID
        is_uuid_format = len(email_id) == 36 and email_id.count('-') == 4
        
        if is_uuid_format:
            logger.info(f"{email_id} appears to be a UUID format, using most recent emails to find actual Gmail ID")
            # Get most recent emails to try to match the UUID
            recent_emails = self.get_unprocessed_emails(datetime.now() - timedelta(days=7))
            
            # Search for matching UUID in the fetched emails
            for email in recent_emails:
                if email.get('email_log_uuid') == email_id:
                    gmail_id = email.get('email_id')
                    logger.info(f"Found matching Gmail ID {gmail_id} for UUID {email_id}")
                    return email
                    
            logger.error(f"Could not find email with UUID {email_id} in recent emails")
            return None
        else:
            try:
                # Treat as Gmail message ID
                return self._get_email_content(email_id)
                
            except HttpError as error:
                logger.error(f"Error fetching email {email_id}: {error}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error fetching email {email_id}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return None
                
    async def async_check_and_refresh_watch(self, email_address: str, dao=None, pubsub_topic: str = None):
        """
        Async version of check_and_refresh_watch.
        Check if Gmail API watch needs to be refreshed and refresh if needed.
        
        Args:
            email_address: Email address to set up the watch for
            dao: Firestore DAO instance for storage (optional)
            pubsub_topic: Full PubSub topic name (projects/PROJECT_ID/topics/TOPIC_NAME)
                          If not provided, will use default format with vaulted-channel-462118-a5
        """
        logger.info(f"========== GMAIL WATCH CHECK STARTED for {email_address} ==========")
        
        # Create watch repository if DAO provided
        watch_repo = None
        if dao:
            watch_repo = GmailWatchRepository(dao)
            logger.info(f"GmailWatchRepository created with DAO: {dao.__class__.__name__}")
        else:
            logger.warning("No DAO provided, watch status cannot be persisted")
        
        # Get watch status from repository if available
        watch_status = None
        if watch_repo:
            try:
                watch_status = await watch_repo.get_watch_status()
                if watch_status:
                    logger.info(f"Current watch status found: ID={watch_status.watch_id}, email={watch_status.email_address}, "  
                                f"expiration={datetime.fromtimestamp(watch_status.expiration/1000).isoformat() if watch_status.expiration else 'None'}")
                else:
                    logger.info("No existing watch status found in database")
            except Exception as e:
                logger.error(f"Error getting watch status: {str(e)}")
            
        current_time = time.time() * 1000  # Current time in milliseconds (same format as Gmail API expiration)
        logger.info(f"Current time (ms): {current_time}, ISO: {datetime.fromtimestamp(current_time/1000).isoformat()}")
        
        should_refresh = False
        if not watch_status:
            # No watch status exists, we should create one
            logger.info("No existing Gmail watch found, will create a new one")
            should_refresh = True
        elif watch_status.expiration:
            # Check if expiration is approaching (within 1 day)
            expiration_time = watch_status.expiration
            one_day_in_ms = 24 * 60 * 60 * 1000
            time_to_expiration_days = (expiration_time - current_time) / (24 * 60 * 60 * 1000)
            
            if current_time + one_day_in_ms >= expiration_time:
                logger.info(f"Gmail watch expiration approaching in {time_to_expiration_days:.2f} days, "  
                            f"current: {datetime.fromtimestamp(current_time/1000).isoformat()}, "  
                            f"expiration: {datetime.fromtimestamp(expiration_time/1000).isoformat()}")
                should_refresh = True
            else:
                logger.info(f"Gmail watch is still valid, expiration in {time_to_expiration_days:.2f} days ("  
                            f"expires: {datetime.fromtimestamp(expiration_time/1000).isoformat()})")
        else:
            # Expiration unknown, refresh to be safe
            logger.info("Watch status has no expiration time, will refresh to be safe")
            should_refresh = True
        
        if should_refresh:
            logger.info("Initiating Gmail watch refresh...")
            await self.async_refresh_watch(email_address, dao, pubsub_topic)
        else:
            logger.info("No Gmail watch refresh needed at this time")
            
        logger.info(f"========== GMAIL WATCH CHECK COMPLETED for {email_address} ==========\n")
    
    async def async_refresh_watch(self, email_address: str, dao=None, pubsub_topic: str = None):
        """
        Async version of _refresh_watch.
        Refresh the Gmail API watch subscription.
        
        Args:
            email_address: Email address to set up the watch for
            dao: Firestore DAO instance for storage (optional)
            pubsub_topic: Full PubSub topic name (optional)
            
        Returns:
            bool: True if refresh was successful, False otherwise
        """
        logger.info(f"========== GMAIL WATCH REFRESH STARTED for {email_address} ==========")
        try:
            # Default PubSub topic if not provided
            if not pubsub_topic:
                pubsub_topic = 'projects/vaulted-channel-462118-a5/topics/gmail-notifications'
                
            logger.info(f"Using PubSub topic: {pubsub_topic}")
                
            # Set up watch request for the inbox
            request = {
                'labelIds': ['INBOX'],  # Only watch inbox
                'topicName': pubsub_topic,
                'labelFilterBehavior': 'INCLUDE'
            }
            
            logger.info("Calling Gmail API watch method...")
            # Call the Gmail API watch method
            response = self.service.users().watch(userId='me', body=request).execute()
            
            # Extract response data
            history_id = response.get('historyId')
            expiration = response.get('expiration')
            
            logger.info(f"Gmail watch API call successful. Response data:")
            logger.info(f"- History ID: {history_id}")
            logger.info(f"- Expiration: {expiration} (" + 
                        f"{datetime.fromtimestamp(int(expiration)/1000).isoformat() if expiration else 'None'})")
            logger.info(f"- Full response: {response}")
            
            # Save watch status using repository if DAO provided
            if dao:
                # Create repository
                watch_repo = GmailWatchRepository(dao)
                logger.info(f"Created GmailWatchRepository to save status")
                
                try:
                    logger.info("Saving watch status to Firestore...")
                    result = await watch_repo.save_watch_status(
                        email_address=email_address,
                        history_id=history_id,
                        expiration=int(expiration),
                        pubsub_topic=pubsub_topic
                    )
                    logger.info(f"Watch status saved successfully: {result}")
                except Exception as e:
                    logger.error(f"Error saving watch status to repository: {str(e)}")
            
            logger.info("Gmail watch refresh completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error refreshing Gmail watch: {str(e)}")
            # Continue execution - don't fail just because watch refresh failed
        
        logger.info(f"========== GMAIL WATCH REFRESH COMPLETED for {email_address} ==========\n")
        
    def get_most_recent_email_id_from_history(self, history_id):
        """
        Get the most recent email message ID based on a history ID.
        
        Args:
            history_id: Gmail history ID to find the most recent email message
            
        Returns:
            str: The message ID of the most recent email, or None if not found
        """
        logger.info(f"Getting most recent message ID from history ID: {history_id}")
        try:
            # Call the history.list API
            response = self.service.users().history().list(
                userId=self.mailbox_id,
                startHistoryId=history_id,
                maxResults=1,  # We only need the most recent message
                historyTypes=['messageAdded']
            ).execute()
            
            # Extract the most recent message ID from the history
            history_records = response.get('history', [])
            
            # Look for the most recent message
            for record in history_records:
                messages_added = record.get('messagesAdded', [])
                if messages_added:  # If there are messages in this record
                    # Get the first (most recent) message
                    message_data = messages_added[0]
                    message = message_data.get('message', {})
                    message_id = message.get('id')
                    if message_id:
                        logger.info(f"Found most recent message ID: {message_id} from history")
                        return message_id
            
            # If no messages found in history records, try listing messages
            logger.info("No messages found in history, trying to list recent messages")
            response = self.service.users().messages().list(
                userId=self.mailbox_id,
                maxResults=1  # Just get the most recent
            ).execute()
            
            messages = response.get('messages', [])
            if messages and len(messages) > 0:
                message_id = messages[0].get('id')
                logger.info(f"Found most recent message ID: {message_id} from messages list")
                return message_id
            
            logger.warning("No recent messages found")
            return None
            
        except Exception as e:
            logger.error(f"Error getting most recent message ID: {str(e)}")
            return None
    
    async def get_history_changes(self, start_history_id, max_results=100):
        """
        Get message changes using Gmail history.list API.
        
        Args:
            start_history_id: History ID to start fetching changes from
            max_results: Maximum number of history records to fetch
            
        Returns:
            List of message IDs added since the start_history_id
        """
        logger.info(f"Fetching Gmail history changes since history ID: {start_history_id}")
        added_message_ids = []
        try:
            # Call the history.list API
            response = self.service.users().history().list(
                userId=self.mailbox_id,
                startHistoryId=start_history_id,
                maxResults=max_results,
                historyTypes=['messageAdded']
            ).execute()
            
            # Extract message IDs from the history
            logger.info("Processing history records")
            history_records = response.get('history', [])
            
            for record in history_records:
                messages_added = record.get('messagesAdded', [])
                for message_data in messages_added:
                    message = message_data.get('message', {})
                    message_id = message.get('id')
                    if message_id:
                        added_message_ids.append(message_id)
            
            if added_message_ids:
                logger.info(f"Found {len(added_message_ids)} new messages in history")
            else:
                logger.info("No new messages found in history")
                
            return added_message_ids
        except Exception as e:
            logger.error(f"Error fetching Gmail history: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

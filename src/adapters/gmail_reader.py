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
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)

# Define OAuth2 scopes needed for Gmail API
# This scope allows read-only access to Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

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
        self.data_path = data_path or os.path.join(os.getcwd(), "gmail_data")
        self.service = None
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_path, exist_ok=True)
        
        # Authenticate and create the Gmail API service
        self._authenticate()
        
        logger.info(f"Initialized GmailReader for mailbox_id: {mailbox_id}")

    def _authenticate(self):
        """Authenticate with Gmail API and create service."""
        creds = None
        
        # Check if token file exists and load it
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_info(
                json.load(open(self.token_path)), SCOPES
            )
        
        # If credentials don't exist or are invalid, refresh or create new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # Run OAuth2 flow to get credentials
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            with open(self.token_path, 'w') as token:
                token.write(creds.to_json())
        
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
            # Format date as YYYY/MM/DD for Gmail query
            date_str = since_timestamp.strftime("%Y/%m/%d")
            query_parts.append(f"after:{date_str}")
        
        # You can add more filters here, e.g.:
        # - Specific label: query_parts.append("label:PaymentAdvice")
        # - Specific subject: query_parts.append("subject:\"Payment Advice\"")
        
        return " ".join(query_parts)

    def _get_email_content(self, email_id: str) -> Dict[str, Any]:
        """
        Get full content of an email using its ID.
        
        Args:
            email_id: Gmail message ID
            
        Returns:
            Dict containing email data
        """
        # Get the email message
        message = self.service.users().messages().get(
            userId='me', id=email_id, format='full'
        ).execute()
        
        # Extract headers
        headers = message['payload']['headers']
        subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), '')
        sender = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
        received = next((h['value'] for h in headers if h['name'].lower() == 'date'), '')
        
        # Parse received date
        try:
            received_datetime = parsedate_to_datetime(received)
        except:
            received_datetime = datetime.now()
        
        # Get the email body content
        content = ""
        if 'parts' in message['payload']:
            for part in message['payload']['parts']:
                if part['mimeType'] == 'text/plain':
                    body_data = part['body'].get('data', '')
                    if body_data:
                        content = base64.urlsafe_b64decode(body_data).decode('utf-8')
        elif 'body' in message['payload'] and 'data' in message['payload']['body']:
            body_data = message['payload']['body']['data']
            content = base64.urlsafe_b64decode(body_data).decode('utf-8')
        
        # Save the raw email data
        email_file_path = os.path.join(self.data_path, f"{email_id}.eml")
        
        # Create the email object
        email_obj = {
            "email_id": email_id,
            "object_file_path": email_file_path,
            "sender_mail": sender,
            "original_sender_mail": None,  # Will be populated if forwarded
            "received_at": received_datetime.isoformat(),
            "subject": subject,
            "content": content,
            "customer_uuid": None,  # Will be populated by downstream processing
            "legal_entity_uuid": None,  # Will be populated by downstream processing
            "mailbox_id": self.mailbox_id
        }
        
        # Save the processed email data
        with open(f"{email_file_path}.json", 'w') as f:
            json.dump(email_obj, f, indent=2)
        
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

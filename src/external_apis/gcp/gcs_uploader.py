"""
Google Cloud Storage adapter for uploading and accessing email objects.
"""
import logging
import os
import uuid
from typing import Dict, List, Optional, Any, Tuple, BinaryIO, Union

from google.cloud import storage
from google.cloud.exceptions import NotFound

from src.config import (
    EMAIL_OBJECT_FILENAME,
    EMAIL_TEXT_FILENAME,
    EMAIL_HTML_FILENAME,
    ATTACHMENT_PREFIX
)

logger = logging.getLogger(__name__)

class GCSUploader:
    """
    Handles uploading email objects to Google Cloud Storage.
    """
    
    def __init__(self, bucket_name: str):
        """
        Initialize GCS client and bucket.
        
        Args:
            bucket_name: Name of the GCS bucket to use
        """
        self.storage_client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.bucket(bucket_name)
        
        # Verify bucket exists
        try:
            self.storage_client.get_bucket(bucket_name)
            logger.info(f"Connected to GCS bucket: {bucket_name}")
        except NotFound:
            logger.error(f"GCS bucket not found: {bucket_name}")
            raise ValueError(f"GCS bucket '{bucket_name}' not found.")
    
    def upload_email_object(self, email_uuid: str, email_data: bytes) -> str:
        """
        Upload raw email object to GCS and return the file path.
        
        Args:
            email_uuid: UUID of the email, used to create folder structure
            email_data: Raw email data as bytes
            
        Returns:
            GCS path to the uploaded file
        """
        object_name = f"{email_uuid}/{EMAIL_OBJECT_FILENAME}"
        blob = self.bucket.blob(object_name)
        
        # Upload the file
        blob.upload_from_string(email_data)
        
        logger.info(f"Uploaded raw email to gs://{self.bucket_name}/{object_name}")
        
        return object_name
        
    def upload_email_content(self, email_uuid: str, content_type: str, content: str) -> str:
        """
        Upload email text or HTML content to GCS.
        
        Args:
            email_uuid: UUID of the email, used to create folder structure
            content_type: Either 'text' or 'html'
            content: The content to upload
            
        Returns:
            GCS path to the uploaded file
        """
        if content_type == 'text':
            filename = EMAIL_TEXT_FILENAME
        elif content_type == 'html':
            filename = EMAIL_HTML_FILENAME
        else:
            raise ValueError(f"Unsupported content type: {content_type}")
            
        object_name = f"{email_uuid}/{filename}"
        blob = self.bucket.blob(object_name)
        
        # Upload the file
        blob.upload_from_string(content)
        
        logger.info(f"Uploaded {content_type} content to gs://{self.bucket_name}/{object_name}")
        
        return object_name
        
    def upload_attachment(self, email_uuid: str, attachment_name: str, attachment_data: bytes) -> str:
        """
        Upload an email attachment to GCS.
        
        Args:
            email_uuid: UUID of the email
            attachment_name: Original filename of the attachment
            attachment_data: Binary content of the attachment
            
        Returns:
            GCS path to the uploaded file
        """
        # Sanitize filename to avoid path traversal issues
        safe_filename = os.path.basename(attachment_name)
        
        # Create a path with the attachment prefix
        object_name = f"{email_uuid}/{ATTACHMENT_PREFIX}{safe_filename}"
        blob = self.bucket.blob(object_name)
        
        # Upload the file
        blob.upload_from_string(attachment_data)
        
        logger.info(f"Uploaded attachment to gs://{self.bucket_name}/{object_name}")
        
        return object_name
        
    def upload_email_complete(self, email_uuid: str, raw_data: bytes, 
                             text_content: Optional[str] = None, 
                             html_content: Optional[str] = None,
                             attachments: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Upload all components of an email to GCS.
        
        Args:
            email_uuid: UUID of the email
            raw_data: Raw email data as bytes
            text_content: Plain text content of the email (optional)
            html_content: HTML content of the email (optional)
            attachments: List of attachments, each a dict with 'filename' and 'content' keys
            
        Returns:
            Dict with paths to all uploaded files
        """
        result = {
            "raw_path": self.upload_email_object(email_uuid, raw_data),
            "attachments": []
        }
        
        # Upload text content if available
        if text_content:
            result["text_path"] = self.upload_email_content(email_uuid, 'text', text_content)
        
        # Upload HTML content if available
        if html_content:
            result["html_path"] = self.upload_email_content(email_uuid, 'html', html_content)
        
        # Upload attachments if any
        if attachments:
            for attachment in attachments:
                filename = attachment.get('filename', 'unknown_file')
                content = attachment.get('content', b'')
                attachment_path = self.upload_attachment(email_uuid, filename, content)
                result["attachments"].append({
                    "filename": filename,
                    "path": attachment_path
                })
        
        logger.info(f"Completed uploading email {email_uuid} with {len(result['attachments'])} attachments")
        
        return result
    
    def check_email_exists(self, email_uuid: str) -> bool:
        """
        Check if an email object already exists for the given UUID.
        
        Args:
            email_uuid: UUID of the email
            
        Returns:
            True if the email object exists, False otherwise
        """
        object_name = f"{email_uuid}/{EMAIL_OBJECT_FILENAME}"
        blob = self.bucket.blob(object_name)
        
        return blob.exists()
    
    def get_email_object(self, email_uuid: str) -> Optional[bytes]:
        """
        Get the email object from GCS.
        
        Args:
            email_uuid: UUID of the email
            
        Returns:
            Raw email data as bytes, or None if not found
        """
        object_name = f"{email_uuid}/{EMAIL_OBJECT_FILENAME}"
        blob = self.bucket.blob(object_name)
        
        if not blob.exists():
            logger.warning(f"Email object not found: {object_name}")
            return None
        
        # Download the file
        return blob.download_as_bytes()

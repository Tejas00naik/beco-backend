"""
Google Cloud Storage adapter for uploading and accessing email objects.
"""
import logging
import os
import uuid
from typing import Dict, List, Optional, Any, Tuple, BinaryIO, Union
from datetime import datetime, timedelta

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
        
    def generate_signed_url(self, object_name: str, expiration_days: int = 7) -> Optional[str]:
        """
        Generate a presigned URL for a file in GCS that's valid for the specified number of days.
        
        Args:
            object_name: Path to the object in the bucket
            expiration_days: Number of days until the URL expires
            
        Returns:
            Presigned URL if successful, None otherwise
        """
        try:
            blob = self.bucket.blob(object_name)
            
            if not blob.exists():
                logger.warning(f"Object not found: {object_name}")
                return None
                
            # Generate a signed URL that's valid for the specified number of days
            url = blob.generate_signed_url(
                version="v4",
                expiration=datetime.utcnow() + timedelta(days=expiration_days),
                method="GET"
            )
            
            logger.info(f"Generated signed URL for gs://{self.bucket_name}/{object_name} valid for {expiration_days} days")
            return url
            
        except Exception as e:
            logger.error(f"Error generating signed URL for {object_name}: {str(e)}")
            return None
            
    def upload_file(self, file_path: str, destination_path: str) -> Optional[str]:
        """
        Upload a file to GCS and return the object path.
        
        Args:
            file_path: Path to the local file
            destination_path: Path in GCS where the file should be stored
            
        Returns:
            GCS object path if successful, None otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None
                
            blob = self.bucket.blob(destination_path)
            blob.upload_from_filename(file_path)
            
            logger.info(f"Uploaded {file_path} to gs://{self.bucket_name}/{destination_path}")
            return destination_path
            
        except Exception as e:
            logger.error(f"Error uploading {file_path} to GCS: {str(e)}")
            return None
            
    def upload_and_get_signed_url(self, file_path: str, destination_folder: str, 
                                   filename: Optional[str] = None, 
                                   expiration_days: int = 7) -> Optional[str]:
        """
        Upload a file to GCS and generate a presigned URL.
        
        Args:
            file_path: Path to the local file
            destination_folder: Folder path in GCS
            filename: Name to use for the file in GCS (defaults to basename of file_path)
            expiration_days: Number of days until the URL expires
            
        Returns:
            Presigned URL if successful, None otherwise
        """
        try:
            # Use provided filename or extract from path
            actual_filename = filename or os.path.basename(file_path)
            
            # Create destination path
            destination_path = f"{destination_folder}/{actual_filename}"
            
            # Upload the file
            result = self.upload_file(file_path, destination_path)
            if not result:
                return None
                
            # Generate signed URL
            return self.generate_signed_url(destination_path, expiration_days)
            
        except Exception as e:
            logger.error(f"Error in upload_and_get_signed_url for {file_path}: {str(e)}")
            return None

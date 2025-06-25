"""
Google Cloud Storage adapter for uploading and accessing email objects.
"""
import logging
import os
from typing import Optional

from google.cloud import storage
from google.cloud.exceptions import NotFound

from src.config import EMAIL_OBJECT_FILENAME

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
        Upload email object to GCS and return the file path.
        
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
        
        logger.info(f"Uploaded email object to gs://{self.bucket_name}/{object_name}")
        
        return object_name
    
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

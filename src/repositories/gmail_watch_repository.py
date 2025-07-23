"""
Gmail Watch Repository

This module provides repository classes for managing Gmail watch status
in Firestore. It abstracts database interactions for Gmail watch operations.
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

from src.repositories.firestore_dao import FirestoreDAO

# Schema class for Gmail watch status
class GmailWatchStatus:
    """Schema for tracking Gmail API watch status"""
    def __init__(self, 
                 watch_id: str = None,
                 email_address: str = None, 
                 history_id: str = None,
                 expiration: int = None, 
                 last_refreshed: datetime = None):
        self.watch_id = watch_id
        self.email_address = email_address
        self.history_id = history_id
        self.expiration = expiration
        self.last_refreshed = last_refreshed or datetime.utcnow()

logger = logging.getLogger(__name__)

class GmailWatchRepository:
    """Repository for managing Gmail watch status in Firestore."""
    
    # Collection names
    WATCH_STATUS_COLLECTION = "gmail_watch_status"
    WATCH_HISTORY_COLLECTION = "gmail_watch_history"
    CURRENT_STATUS_DOC_ID = "current"
    
    def __init__(self, dao: FirestoreDAO):
        """
        Initialize the Gmail watch repository.
        
        Args:
            dao: Firestore DAO instance for database operations
        """
        self.dao = dao
    
    async def get_watch_status(self) -> Optional[GmailWatchStatus]:
        """
        Get the current Gmail watch status from Firestore.
        
        Returns:
            GmailWatchStatus or None if not found
        """
        try:
            # Get watch status from Firestore
            watch_data = await self.dao.get_document(
                self.WATCH_STATUS_COLLECTION, 
                self.CURRENT_STATUS_DOC_ID
            )
            
            if not watch_data:
                return None
                
            # Convert to GmailWatchStatus object
            watch_status = GmailWatchStatus(
                watch_id=watch_data.get("watch_id"),
                email_address=watch_data.get("email_address"),
                history_id=watch_data.get("history_id"),
                expiration=watch_data.get("expiration"),
                last_refreshed=watch_data.get("last_refreshed")
            )
            
            return watch_status
        except Exception as e:
            logger.error(f"Error getting Gmail watch status: {str(e)}")
            return None
    
    async def save_watch_status(self, 
                          email_address: str,
                          history_id: str,
                          expiration: int,
                          pubsub_topic: str) -> Dict[str, Any]:
        """
        Save Gmail watch status to Firestore.
        
        Args:
            email_address: Email address the watch is set for
            history_id: Gmail history ID from the watch response
            expiration: Expiration time in milliseconds
            pubsub_topic: PubSub topic name configured for notifications
            
        Returns:
            The saved watch data
        """
        try:
            current_time = datetime.utcnow()
            
            # Main watch status document
            watch_data = {
                "watch_id": str(uuid.uuid4()),  # Generate a unique ID for this watch
                "email_address": email_address,
                "history_id": history_id,
                "expiration": expiration,
                "last_refreshed": current_time,
                "pubsub_topic": pubsub_topic,
                "refresh_count": 1  # Initialize or will be incremented below
            }
            
            # Get existing data to update refresh count if it exists
            existing_watch = await self.dao.get_document(
                self.WATCH_STATUS_COLLECTION,
                self.CURRENT_STATUS_DOC_ID
            )
            
            if existing_watch and "refresh_count" in existing_watch:
                watch_data["refresh_count"] = existing_watch["refresh_count"] + 1
            
            # Update current status - use add_document which can create or overwrite documents
            await self.dao.add_document(
                self.WATCH_STATUS_COLLECTION,
                self.CURRENT_STATUS_DOC_ID,
                watch_data
            )
            
            # Also add to history collection for audit trail
            history_id = str(uuid.uuid4())
            history_data = {
                **watch_data,  # Include all watch data
                "timestamp": current_time,  # When this history entry was created
                "operation": "refresh"
            }
            
            await self.dao.add_document(
                self.WATCH_HISTORY_COLLECTION,
                history_id,
                history_data
            )
            
            logger.info(f"Gmail watch status saved to Firestore. Refresh #{watch_data['refresh_count']}")
            return watch_data
            
        except Exception as e:
            logger.error(f"Error saving Gmail watch status: {str(e)}")
            # Continue execution - don't fail just because watch status save failed
            return None

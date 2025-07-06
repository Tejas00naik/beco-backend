"""Repository for OtherDoc entity operations."""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from uuid import uuid4

from src.models.schemas import OtherDoc
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class OtherDocRepository:
    """Repository for OtherDoc data operations."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with a FirestoreDAO instance."""
        self.dao = dao
        
    async def create(self, other_doc: OtherDoc) -> str:
        """
        Create a new other document record.
        
        Args:
            other_doc: OtherDoc data object
            
        Returns:
            other_doc_uuid: The UUID of the created other document
        """
        try:
            # Generate UUID if not already set
            if not other_doc.other_doc_uuid:
                other_doc.other_doc_uuid = str(uuid4())
                
            # Set creation timestamp if not set
            if not other_doc.created_at:
                other_doc.created_at = datetime.utcnow()
                
            # Set updated timestamp
            other_doc.updated_at = datetime.utcnow()
            
            # Add to Firestore
            await self.dao.add_document("other_doc", other_doc.other_doc_uuid, other_doc)
            logger.info(f"Created other doc {other_doc.other_doc_uuid} for payment advice {other_doc.payment_advice_uuid}")
            return other_doc.other_doc_uuid
            
        except Exception as e:
            logger.error(f"Error creating other doc: {str(e)}")
            raise
            
    async def get_by_id(self, other_doc_uuid: str) -> Optional[OtherDoc]:
        """
        Get an other doc by its UUID.
        
        Args:
            other_doc_uuid: UUID of the other doc
            
        Returns:
            OtherDoc object or None if not found
        """
        try:
            doc = await self.dao.get_document("other_doc", other_doc_uuid)
            if doc:
                return OtherDoc(**doc)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving other doc {other_doc_uuid}: {str(e)}")
            raise
            
    async def get_by_payment_advice(self, payment_advice_uuid: str) -> List[OtherDoc]:
        """
        Get all other docs for a specific payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            List of OtherDoc objects
        """
        try:
            docs = await self.dao.query_documents(
                "other_doc", 
                filters=[("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            return [OtherDoc(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving other docs for payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def get_by_other_doc_number(self, other_doc_number: str) -> List[OtherDoc]:
        """
        Get other docs by document number.
        
        Args:
            other_doc_number: Other doc number
            
        Returns:
            List of OtherDoc objects
        """
        try:
            docs = await self.dao.query_documents(
                "other_doc", 
                filters=[("other_doc_number", "==", other_doc_number)]
            )
            return [OtherDoc(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving other docs with number {other_doc_number}: {str(e)}")
            raise
            
    async def update(self, other_doc_uuid: str, updates: Dict[str, Any]) -> None:
        """
        Update an other doc with new data.
        
        Args:
            other_doc_uuid: UUID of the other doc
            updates: Dictionary of fields to update
        """
        try:
            # Always update the updated_at timestamp
            if "updated_at" not in updates:
                updates["updated_at"] = datetime.utcnow()
                
            await self.dao.update_document("other_doc", other_doc_uuid, updates)
            logger.info(f"Updated other doc {other_doc_uuid} with {len(updates)} fields")
            
        except Exception as e:
            logger.error(f"Error updating other doc {other_doc_uuid}: {str(e)}")
            raise
            
    async def delete(self, other_doc_uuid: str) -> None:
        """
        Delete an other doc.
        
        Args:
            other_doc_uuid: UUID of the other doc
        """
        try:
            await self.dao.delete_document("other_doc", other_doc_uuid)
            logger.info(f"Deleted other doc {other_doc_uuid}")
            
        except Exception as e:
            logger.error(f"Error deleting other doc {other_doc_uuid}: {str(e)}")
            raise
            
    async def find_by_unique_key(self, payment_advice_uuid: str, other_doc_number: str) -> Optional[OtherDoc]:
        """
        Find an other doc by its unique key (payment_advice_uuid, other_doc_number).
        Used to prevent duplicate other docs within the same payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            other_doc_number: Other doc number
            
        Returns:
            OtherDoc object or None if not found
        """
        try:
            docs = await self.dao.query_documents(
                "other_doc", 
                filters=[
                    ("payment_advice_uuid", "==", payment_advice_uuid),
                    ("other_doc_number", "==", other_doc_number)
                ]
            )
            
            if docs and len(docs) > 0:
                return OtherDoc(**docs[0])
            return None
            
        except Exception as e:
            logger.error(f"Error finding other doc by unique key (payment_advice={payment_advice_uuid}, other_doc_number={other_doc_number}): {str(e)}")
            raise

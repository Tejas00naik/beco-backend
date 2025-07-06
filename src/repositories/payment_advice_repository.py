"""Repository for PaymentAdvice entity operations."""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from uuid import uuid4

from src.models.schemas import PaymentAdvice, PaymentAdviceStatus
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class PaymentAdviceRepository:
    """Repository for PaymentAdvice data operations."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with a FirestoreDAO instance."""
        self.dao = dao
        
    async def create(self, payment_advice: PaymentAdvice) -> str:
        """
        Create a new payment advice record.
        
        Args:
            payment_advice: PaymentAdvice data object
            
        Returns:
            payment_advice_uuid: The UUID of the created payment advice
        """
        try:
            # Generate UUID if not already set
            if not payment_advice.payment_advice_uuid:
                payment_advice.payment_advice_uuid = str(uuid4())
                
            # Set creation timestamp if not set
            if not payment_advice.created_at:
                payment_advice.created_at = datetime.utcnow()
                
            # Set updated timestamp
            payment_advice.updated_at = datetime.utcnow()
            
            # Add to Firestore
            await self.dao.add_document("payment_advice", payment_advice.payment_advice_uuid, payment_advice)
            logger.info(f"Created payment advice {payment_advice.payment_advice_uuid} for email log {payment_advice.email_log_uuid}")
            return payment_advice.payment_advice_uuid
            
        except Exception as e:
            logger.error(f"Error creating payment advice: {str(e)}")
            raise
            
    async def get_by_id(self, payment_advice_uuid: str) -> Optional[PaymentAdvice]:
        """
        Get a payment advice by its UUID.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            PaymentAdvice object or None if not found
        """
        try:
            doc = await self.dao.get_document("payment_advice", payment_advice_uuid)
            if doc:
                return PaymentAdvice(**doc)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def get_by_email_log(self, email_log_uuid: str) -> List[PaymentAdvice]:
        """
        Get all payment advices for a specific email log.
        
        Args:
            email_log_uuid: UUID of the email log
            
        Returns:
            List of PaymentAdvice objects
        """
        try:
            docs = await self.dao.query_documents(
                "payment_advice", 
                filters=[("email_log_uuid", "==", email_log_uuid)]
            )
            return [PaymentAdvice(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving payment advices for email log {email_log_uuid}: {str(e)}")
            raise
            
    async def get_by_status(self, status: PaymentAdviceStatus) -> List[PaymentAdvice]:
        """
        Get payment advices by status.
        
        Args:
            status: PaymentAdviceStatus enum value
            
        Returns:
            List of PaymentAdvice objects
        """
        try:
            docs = await self.dao.query_documents(
                "payment_advice", 
                filters=[("payment_advice_status", "==", status.value)]
            )
            return [PaymentAdvice(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving payment advices with status {status}: {str(e)}")
            raise
            
    async def update(self, payment_advice_uuid: str, updates: Dict[str, Any]) -> None:
        """
        Update a payment advice with new data.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            updates: Dictionary of fields to update
        """
        try:
            # Always update the updated_at timestamp
            if "updated_at" not in updates:
                updates["updated_at"] = datetime.utcnow()
                
            await self.dao.update_document("payment_advice", payment_advice_uuid, updates)
            logger.info(f"Updated payment advice {payment_advice_uuid} with {len(updates)} fields")
            
        except Exception as e:
            logger.error(f"Error updating payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def delete(self, payment_advice_uuid: str) -> None:
        """
        Delete a payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
        """
        try:
            await self.dao.delete_document("payment_advice", payment_advice_uuid)
            logger.info(f"Deleted payment advice {payment_advice_uuid}")
            
        except Exception as e:
            logger.error(f"Error deleting payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def update_status(self, payment_advice_uuid: str, status: PaymentAdviceStatus) -> None:
        """
        Update the status of a payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            status: New PaymentAdviceStatus value
        """
        try:
            updates = {
                "payment_advice_status": status.value,
                "updated_at": datetime.utcnow()
            }
            
            await self.dao.update_document("payment_advice", payment_advice_uuid, updates)
            logger.info(f"Updated payment advice {payment_advice_uuid} status to {status}")
            
        except Exception as e:
            logger.error(f"Error updating payment advice {payment_advice_uuid} status: {str(e)}")
            raise

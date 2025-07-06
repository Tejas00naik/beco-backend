"""Repository for Settlement entity operations."""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from uuid import uuid4

from src.models.schemas import Settlement, SettlementStatus
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class SettlementRepository:
    """Repository for Settlement data operations."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with a FirestoreDAO instance."""
        self.dao = dao
        
    async def create(self, settlement: Settlement) -> str:
        """
        Create a new settlement record.
        
        Args:
            settlement: Settlement data object
            
        Returns:
            settlement_uuid: The UUID of the created settlement
        """
        try:
            # Generate UUID if not already set
            if not settlement.settlement_uuid:
                settlement.settlement_uuid = str(uuid4())
                
            # Set creation timestamp if not set
            if not settlement.created_at:
                settlement.created_at = datetime.utcnow()
                
            # Set updated timestamp
            settlement.updated_at = datetime.utcnow()
            
            # Create a composite ID to enforce uniqueness
            # {payment_advice_uuid}_{invoice_uuid ?? "_"}_{other_doc_uuid ?? "_"}
            composite_id = f"{settlement.payment_advice_uuid}_"
            composite_id += f"{settlement.invoice_uuid if settlement.invoice_uuid else '_'}_"
            composite_id += f"{settlement.other_doc_uuid if settlement.other_doc_uuid else '_'}"
            
            # Add to Firestore with composite ID
            await self.dao.add_document("settlement", composite_id, settlement)
            
            # Update the settlement object with the composite ID
            settlement.settlement_uuid = composite_id
            
            logger.info(f"Created settlement {composite_id} for payment advice {settlement.payment_advice_uuid}")
            return composite_id
            
        except Exception as e:
            logger.error(f"Error creating settlement: {str(e)}")
            raise
            
    async def get_by_id(self, settlement_uuid: str) -> Optional[Settlement]:
        """
        Get a settlement by its UUID.
        
        Args:
            settlement_uuid: UUID of the settlement
            
        Returns:
            Settlement object or None if not found
        """
        try:
            doc = await self.dao.get_document("settlement", settlement_uuid)
            if doc:
                return Settlement(**doc)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving settlement {settlement_uuid}: {str(e)}")
            raise
            
    async def get_by_payment_advice(self, payment_advice_uuid: str) -> List[Settlement]:
        """
        Get all settlements for a specific payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            List of Settlement objects
        """
        try:
            docs = await self.dao.query_documents(
                "settlement", 
                filters=[("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            return [Settlement(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving settlements for payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def get_by_invoice(self, invoice_uuid: str) -> List[Settlement]:
        """
        Get all settlements for a specific invoice.
        
        Args:
            invoice_uuid: UUID of the invoice
            
        Returns:
            List of Settlement objects
        """
        try:
            docs = await self.dao.query_documents(
                "settlement", 
                filters=[("invoice_uuid", "==", invoice_uuid)]
            )
            return [Settlement(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving settlements for invoice {invoice_uuid}: {str(e)}")
            raise
            
    async def get_by_other_doc(self, other_doc_uuid: str) -> List[Settlement]:
        """
        Get all settlements for a specific other doc.
        
        Args:
            other_doc_uuid: UUID of the other doc
            
        Returns:
            List of Settlement objects
        """
        try:
            docs = await self.dao.query_documents(
                "settlement", 
                filters=[("other_doc_uuid", "==", other_doc_uuid)]
            )
            return [Settlement(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving settlements for other doc {other_doc_uuid}: {str(e)}")
            raise
            
    async def update(self, settlement_uuid: str, updates: Dict[str, Any]) -> None:
        """
        Update a settlement with new data.
        
        Args:
            settlement_uuid: UUID of the settlement
            updates: Dictionary of fields to update
        """
        try:
            # Always update the updated_at timestamp
            if "updated_at" not in updates:
                updates["updated_at"] = datetime.utcnow()
                
            await self.dao.update_document("settlement", settlement_uuid, updates)
            logger.info(f"Updated settlement {settlement_uuid} with {len(updates)} fields")
            
        except Exception as e:
            logger.error(f"Error updating settlement {settlement_uuid}: {str(e)}")
            raise
            
    async def delete(self, settlement_uuid: str) -> None:
        """
        Delete a settlement.
        
        Args:
            settlement_uuid: UUID of the settlement
        """
        try:
            await self.dao.delete_document("settlement", settlement_uuid)
            logger.info(f"Deleted settlement {settlement_uuid}")
            
        except Exception as e:
            logger.error(f"Error deleting settlement {settlement_uuid}: {str(e)}")
            raise
            
    async def update_status(self, settlement_uuid: str, status: SettlementStatus) -> None:
        """
        Update the status of a settlement.
        
        Args:
            settlement_uuid: UUID of the settlement
            status: New SettlementStatus value
        """
        try:
            updates = {
                "settlement_status": status.value,
                "updated_at": datetime.utcnow()
            }
            
            await self.dao.update_document("settlement", settlement_uuid, updates)
            logger.info(f"Updated settlement {settlement_uuid} status to {status}")
            
        except Exception as e:
            logger.error(f"Error updating settlement {settlement_uuid} status: {str(e)}")
            raise

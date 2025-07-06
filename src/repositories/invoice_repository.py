"""Repository for Invoice entity operations."""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from uuid import uuid4

from src.models.schemas import Invoice
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class InvoiceRepository:
    """Repository for Invoice data operations."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with a FirestoreDAO instance."""
        self.dao = dao
        
    async def create(self, invoice: Invoice) -> str:
        """
        Create a new invoice record.
        
        Args:
            invoice: Invoice data object
            
        Returns:
            invoice_uuid: The UUID of the created invoice
        """
        try:
            # Generate UUID if not already set
            if not invoice.invoice_uuid:
                invoice.invoice_uuid = str(uuid4())
                
            # Set creation timestamp if not set
            if not invoice.created_at:
                invoice.created_at = datetime.utcnow()
                
            # Set updated timestamp
            invoice.updated_at = datetime.utcnow()
            
            # Add to Firestore
            await self.dao.add_document("invoice", invoice.invoice_uuid, invoice)
            logger.info(f"Created invoice {invoice.invoice_uuid} for payment advice {invoice.payment_advice_uuid}")
            return invoice.invoice_uuid
            
        except Exception as e:
            logger.error(f"Error creating invoice: {str(e)}")
            raise
            
    async def get_by_id(self, invoice_uuid: str) -> Optional[Invoice]:
        """
        Get an invoice by its UUID.
        
        Args:
            invoice_uuid: UUID of the invoice
            
        Returns:
            Invoice object or None if not found
        """
        try:
            doc = await self.dao.get_document("invoice", invoice_uuid)
            if doc:
                return Invoice(**doc)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving invoice {invoice_uuid}: {str(e)}")
            raise
            
    async def get_by_payment_advice(self, payment_advice_uuid: str) -> List[Invoice]:
        """
        Get all invoices for a specific payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            List of Invoice objects
        """
        try:
            docs = await self.dao.query_documents(
                "invoice", 
                filters=[("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            return [Invoice(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving invoices for payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def get_by_invoice_number(self, invoice_number: str) -> List[Invoice]:
        """
        Get invoices by invoice number.
        
        Args:
            invoice_number: Invoice number
            
        Returns:
            List of Invoice objects
        """
        try:
            docs = await self.dao.query_documents(
                "invoice", 
                filters=[("invoice_number", "==", invoice_number)]
            )
            return [Invoice(**doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error retrieving invoices with number {invoice_number}: {str(e)}")
            raise
            
    async def update(self, invoice_uuid: str, updates: Dict[str, Any]) -> None:
        """
        Update an invoice with new data.
        
        Args:
            invoice_uuid: UUID of the invoice
            updates: Dictionary of fields to update
        """
        try:
            # Always update the updated_at timestamp
            if "updated_at" not in updates:
                updates["updated_at"] = datetime.utcnow()
                
            await self.dao.update_document("invoice", invoice_uuid, updates)
            logger.info(f"Updated invoice {invoice_uuid} with {len(updates)} fields")
            
        except Exception as e:
            logger.error(f"Error updating invoice {invoice_uuid}: {str(e)}")
            raise
            
    async def delete(self, invoice_uuid: str) -> None:
        """
        Delete an invoice.
        
        Args:
            invoice_uuid: UUID of the invoice
        """
        try:
            await self.dao.delete_document("invoice", invoice_uuid)
            logger.info(f"Deleted invoice {invoice_uuid}")
            
        except Exception as e:
            logger.error(f"Error deleting invoice {invoice_uuid}: {str(e)}")
            raise
            
    async def find_by_unique_key(self, payment_advice_uuid: str, invoice_number: str) -> Optional[Invoice]:
        """
        Find an invoice by its unique key (payment_advice_uuid, invoice_number).
        Used to prevent duplicate invoices within the same payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            invoice_number: Invoice number
            
        Returns:
            Invoice object or None if not found
        """
        try:
            docs = await self.dao.query_documents(
                "invoice", 
                filters=[
                    ("payment_advice_uuid", "==", payment_advice_uuid),
                    ("invoice_number", "==", invoice_number)
                ]
            )
            
            if docs and len(docs) > 0:
                return Invoice(**docs[0])
            return None
            
        except Exception as e:
            logger.error(f"Error finding invoice by unique key (payment_advice={payment_advice_uuid}, invoice_number={invoice_number}): {str(e)}")
            raise

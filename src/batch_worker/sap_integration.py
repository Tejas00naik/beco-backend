"""SAP integration for the batch worker."""

import logging
from typing import Optional, Dict, Any, List
from uuid import uuid4

from src.mocks.sap_client import MockSapClient
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class SapIntegrator:
    """Handles SAP integration operations."""
    
    def __init__(self, dao: FirestoreDAO):
        """
        Initialize the SAP integrator.
        
        Args:
            dao: Firestore DAO for database operations
        """
        self.dao = dao
        # Use mock SAP client for testing
        self.sap_client = MockSapClient()
        logger.info("Initialized SapIntegrator with MockSapClient")
        
    async def enrich_documents_with_sap_data(self, payment_advice_uuid: str) -> bool:
        """
        Enrich invoice and other doc records with SAP data.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get all invoices for this payment advice
            invoices = await self.dao.query_documents("invoice", [("payment_advice_uuid", "==", payment_advice_uuid)])
            logger.info(f"Found {len(invoices)} invoices to enrich with SAP data")
            
            # Get all other docs for this payment advice
            other_docs = await self.dao.query_documents("other_doc", [("payment_advice_uuid", "==", payment_advice_uuid)])
            logger.info(f"Found {len(other_docs)} other docs to enrich with SAP data")
            
            # Enrich invoices
            for invoice in invoices:
                invoice_number = invoice.get("invoice_number")
                if not invoice_number:
                    continue
                    
                # Mock SAP lookup for invoice
                sap_data = self.sap_client.get_transaction_by_document_number(invoice_number)
                if sap_data:
                    # Update invoice with SAP data
                    updates = {
                        "sap_transaction_id": sap_data.get("transaction_id"),
                        "customer_uuid": sap_data.get("customer_uuid")
                    }
                    await self.dao.update_document("invoice", invoice.get("invoice_uuid"), updates)
                    logger.info(f"Enriched invoice {invoice_number} with SAP data")
            
            # Enrich other docs
            for other_doc in other_docs:
                other_doc_number = other_doc.get("other_doc_number")
                if not other_doc_number:
                    continue
                    
                # Mock SAP lookup for other doc
                sap_data = self.sap_client.get_transaction_by_document_number(other_doc_number)
                if sap_data:
                    # Update other doc with SAP data
                    updates = {
                        "sap_transaction_id": sap_data.get("transaction_id"),
                        "customer_uuid": sap_data.get("customer_uuid")
                    }
                    await self.dao.update_document("other_doc", other_doc.get("other_doc_uuid"), updates)
                    logger.info(f"Enriched other doc {other_doc_number} with SAP data")
                    
            return True
            
        except Exception as e:
            logger.error(f"Error enriching documents with SAP data: {str(e)}")
            return False

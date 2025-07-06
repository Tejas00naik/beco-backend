"""Invoice processing functionality.

This module contains the InvoiceProcessor class which handles creating
invoice records in Firestore.
"""

import logging
import uuid
from typing import Dict, Any, List

# Import models
from src.models.schemas import Invoice, InvoiceStatus

# Import helpers
from .utils import parse_date, parse_amount, check_document_exists

logger = logging.getLogger(__name__)


class InvoiceProcessor:
    """
    Handles invoice record creation and processing.
    """
    
    def __init__(self, dao):
        """
        Initialize the invoice processor.
        
        Args:
            dao: Firestore DAO instance
        """
        self.dao = dao
    
    async def process_invoices(self, invoice_table: List[Dict[str, Any]], payment_advice_uuid: str, payment_advice_date) -> Dict[str, int]:
        """
        Process invoice table from LLM output and create Invoice records.
        
        Args:
            invoice_table: List of invoice records from LLM output
            payment_advice_uuid: UUID of the payment advice
            payment_advice_date: Date of the payment advice
            
        Returns:
            Dictionary with counts of created and skipped invoices
        """
        results = {"created": 0, "skipped": 0}
        
        if not invoice_table:
            logger.info("No invoice table found in LLM output, skipping invoice processing")
            return results
        
        logger.info(f"Processing {len(invoice_table)} invoices for payment advice {payment_advice_uuid}")
        
        for invoice_data in invoice_table:
            try:
                # Extract invoice fields
                invoice_number = invoice_data.get('invoiceNumber')
                if not invoice_number:
                    logger.warning("Skipping invoice with missing invoice number")
                    results["skipped"] += 1
                    continue
                
                # Check if invoice already exists for this payment advice
                existing_invoice = await check_document_exists(
                    self.dao, "invoice",
                    {"payment_advice_uuid": payment_advice_uuid, "invoice_number": invoice_number}
                )
                
                if existing_invoice:
                    logger.info(f"Invoice {invoice_number} already exists for payment advice {payment_advice_uuid}, skipping")
                    results["skipped"] += 1
                    continue
                
                # Process invoice fields
                invoice_date = parse_date(invoice_data.get('invoiceDate'))
                booking_amount = parse_amount(invoice_data.get('bookingAmount'))
                total_settlement_amount = parse_amount(invoice_data.get('totalSettlementAmount'))
                
                # Create Invoice record
                invoice_uuid = str(uuid.uuid4())
                invoice = Invoice(
                    invoice_uuid=invoice_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    customer_uuid=None,  # Will be populated by SAP integration
                    invoice_number=invoice_number,
                    invoice_date=invoice_date,
                    booking_amount=booking_amount,
                    total_settlement_amount=total_settlement_amount,
                    invoice_status=InvoiceStatus.OPEN,
                    sap_transaction_id=None  # Will be populated by SAP integration
                )
                
                # Add Invoice to Firestore
                await self.dao.add_document("invoice", invoice_uuid, invoice.__dict__)
                logger.info(f"Created invoice {invoice_uuid} with number {invoice_number}")
                results["created"] += 1
                
            except Exception as e:
                logger.warning(f"Failed to create invoice: {str(e)}")
                results["skipped"] += 1
        
        return results
    
    async def get_invoice_uuid_by_number(self, payment_advice_uuid: str, invoice_number: str) -> str:
        """
        Get the UUID of an invoice by its invoice number and payment advice UUID.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            invoice_number: Invoice number to look up
            
        Returns:
            UUID of the invoice, or None if not found
        """
        try:
            query_results = await self.dao.query_documents(
                "invoice",
                [
                    ("payment_advice_uuid", "==", payment_advice_uuid),
                    ("invoice_number", "==", invoice_number)
                ]
            )
            
            if query_results and len(query_results) > 0:
                return query_results[0]["invoice_uuid"]
            return None
            
        except Exception as e:
            logger.error(f"Error looking up invoice UUID: {str(e)}")
            return None

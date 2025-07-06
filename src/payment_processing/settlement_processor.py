"""Settlement processing functionality.

This module contains the SettlementProcessor class which handles creating
settlement records in Firestore and linking them to invoices and other docs.
"""

import logging
import uuid
from typing import Dict, Any, List

# Import models
from models.schemas import Settlement, SettlementStatus

# Import helpers
from .utils import parse_amount

logger = logging.getLogger(__name__)


class SettlementProcessor:
    """
    Handles settlement record creation and linking to invoices and other docs.
    """
    
    def __init__(self, dao):
        """
        Initialize the settlement processor.
        
        Args:
            dao: Firestore DAO instance
        """
        self.dao = dao
        
        # These will be dynamically imported in process_settlements to avoid circular imports
        self.invoice_processor = None
        self.other_doc_processor = None
    
    async def process_settlements(self, settlement_table: List[Dict[str, Any]], payment_advice_uuid: str, payment_advice_date) -> Dict[str, int]:
        """
        Process settlement table from LLM output and create Settlement records.
        
        Args:
            settlement_table: List of settlement records from LLM output
            payment_advice_uuid: UUID of the payment advice
            payment_advice_date: Date of the payment advice
            
        Returns:
            Dictionary with counts of created and skipped settlements
        """
        results = {"created": 0, "skipped": 0}
        
        if not settlement_table:
            logger.info("No settlement table found in LLM output, skipping settlement processing")
            return results
        
        # Dynamically import processors here to avoid circular imports
        if not self.invoice_processor:
            from .invoice_processor import InvoiceProcessor
            self.invoice_processor = InvoiceProcessor(self.dao)
        
        if not self.other_doc_processor:
            from .other_doc_processor import OtherDocProcessor
            self.other_doc_processor = OtherDocProcessor(self.dao)
        
        logger.info(f"Processing {len(settlement_table)} settlements for payment advice {payment_advice_uuid}")
        
        for settlement_data in settlement_table:
            try:
                # Extract settlement fields
                invoice_number = settlement_data.get('invoiceNumber')
                settlement_doc_number = settlement_data.get('settlementDocNumber')
                settlement_amount = parse_amount(settlement_data.get('settlementAmount'))
                
                if not invoice_number or not settlement_doc_number:
                    logger.warning(f"Skipping settlement with missing invoice number '{invoice_number}' or settlement doc number '{settlement_doc_number}'")
                    results["skipped"] += 1
                    continue
                
                # Look up UUIDs for the invoice and other doc
                invoice_uuid = await self.invoice_processor.get_invoice_uuid_by_number(
                    payment_advice_uuid, invoice_number
                )
                
                other_doc_uuid = await self.other_doc_processor.get_other_doc_uuid_by_number(
                    payment_advice_uuid, settlement_doc_number
                )
                
                # Validate both invoice and other doc exist
                if not invoice_uuid:
                    logger.warning(f"Cannot create settlement: Invoice {invoice_number} not found for payment advice {payment_advice_uuid}")
                    results["skipped"] += 1
                    continue
                
                if not other_doc_uuid:
                    logger.warning(f"Cannot create settlement: Other doc {settlement_doc_number} not found for payment advice {payment_advice_uuid}")
                    results["skipped"] += 1
                    continue
                
                # Create Settlement record
                settlement_uuid = str(uuid.uuid4())
                settlement = Settlement(
                    settlement_uuid=settlement_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    customer_uuid=None,  # Will be derived from invoice/other_doc in future
                    invoice_uuid=invoice_uuid,
                    other_doc_uuid=other_doc_uuid,
                    settlement_date=payment_advice_date,
                    settlement_amount=settlement_amount,
                    settlement_status=SettlementStatus.READY
                )
                
                # Add Settlement to Firestore
                await self.dao.add_document("settlement", settlement_uuid, settlement.__dict__)
                logger.info(f"Created settlement {settlement_uuid} linked to invoice {invoice_number} and other doc {settlement_doc_number}")
                results["created"] += 1
                
            except Exception as e:
                logger.warning(f"Failed to create settlement: {str(e)}")
                results["skipped"] += 1
        
        return results

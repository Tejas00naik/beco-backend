"""Main payment processor coordinator module.

This module contains the PaymentProcessor class which orchestrates the processing
of payment advice data from LLM output.
"""

import logging
import uuid
from typing import Dict, Any, Optional, List

# Import processors
from .payment_advice_processor import PaymentAdviceProcessor
from .invoice_processor import InvoiceProcessor
from .other_doc_processor import OtherDocProcessor
from .settlement_processor import SettlementProcessor

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """
    Orchestrates payment processing operations.
    
    This class delegates specific processing tasks to specialized processor classes.
    It serves as the main entry point for payment processing operations.
    """
    
    def __init__(self, dao, legal_entity_lookup):
        """
        Initialize the payment processor.
        
        Args:
            dao: Firestore DAO instance
            legal_entity_lookup: Legal entity lookup service
        """
        self.dao = dao
        self.legal_entity_lookup = legal_entity_lookup
        
        # Initialize specialized processors
        self.payment_advice_processor = PaymentAdviceProcessor(dao, legal_entity_lookup)
        self.invoice_processor = InvoiceProcessor(dao)
        self.other_doc_processor = OtherDocProcessor(dao)
        self.settlement_processor = SettlementProcessor(dao)
    
    async def create_payment_advice_from_llm_output(self, llm_output: Dict[str, Any], email_log_uuid: str) -> Optional[str]:
        """
        Process payment advice data from LLM output and create all related records in Firestore.
        
        Args:
            llm_output: The structured output from LLM containing metaTable, invoiceTable, etc.
            email_log_uuid: UUID of the email being processed
            
        Returns:
            The UUID of the created payment advice, or None if creation failed
        """
        try:
            # Generate a unique payment advice UUID
            payment_advice_uuid = str(uuid.uuid4())
            
            # 1. Create payment advice record using the meta table
            payment_advice = await self.payment_advice_processor.create_payment_advice(
                llm_output, email_log_uuid, payment_advice_uuid
            )
            
            if not payment_advice:
                logger.error("Failed to create payment advice record")
                return None
            
            # 2. Process invoice table
            invoice_results = await self.invoice_processor.process_invoices(
                llm_output.get('invoiceTable', []),
                payment_advice_uuid,
                payment_advice.get('payment_advice_date')
            )
            logger.info(f"Created {invoice_results['created']} invoices, skipped {invoice_results['skipped']}")
            
            # 3. Process other doc table
            other_doc_results = await self.other_doc_processor.process_other_docs(
                llm_output.get('otherDocTable', []),
                payment_advice_uuid,
                payment_advice.get('payment_advice_date')
            )
            logger.info(f"Created {other_doc_results['created']} other docs, skipped {other_doc_results['skipped']}")
            
            # 4. Process settlement table and link to invoices/other docs
            settlement_results = await self.settlement_processor.process_settlements(
                llm_output.get('settlementTable', []),
                payment_advice_uuid,
                payment_advice.get('payment_advice_date')
            )
            
            # 5. Update payment advice status based on settlement results
            await self.payment_advice_processor.update_payment_advice_status(
                payment_advice_uuid,
                settlement_results['created'],
                settlement_results['skipped'],
                len(llm_output.get('settlementTable', []))
            )
            
            return payment_advice_uuid
            
        except Exception as e:
            logger.error(f"Failed to process payment advice: {str(e)}")
            return None
    
    async def process_payment_advice(self, email_log_uuid: str, pa_data: Dict[str, Any],
                                   email_data: Dict[str, Any], pa_index: int) -> None:
        """
        Process a single payment advice and create all related records.
        
        Args:
            email_log_uuid: UUID of the email log entry
            pa_data: Payment advice data from LLM
            email_data: Original email data
            pa_index: Index of this payment advice in the email
        """
        # This method is kept for backward compatibility
        # Its functionality is now primarily in create_payment_advice_from_llm_output
        pass

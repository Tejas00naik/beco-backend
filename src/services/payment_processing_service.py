"""Service layer for payment processing operations."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from uuid import uuid4

from src.models.schemas import (
    PaymentAdvice, 
    PaymentAdviceStatus, 
    Invoice, 
    InvoiceStatus,
    OtherDoc,
    OtherDocType,
    Settlement,
    SettlementStatus
)
from src.repositories import (
    PaymentAdviceRepository,
    InvoiceRepository,
    OtherDocRepository,
    SettlementRepository
)
from src.payment_processing.utils import parse_date, parse_amount

logger = logging.getLogger(__name__)

class PaymentProcessingService:
    """Service for processing payment advice data."""
    
    def __init__(
        self,
        payment_advice_repo: PaymentAdviceRepository,
        invoice_repo: InvoiceRepository,
        other_doc_repo: OtherDocRepository,
        settlement_repo: SettlementRepository
    ):
        """Initialize with repositories."""
        self.payment_advice_repo = payment_advice_repo
        self.invoice_repo = invoice_repo
        self.other_doc_repo = other_doc_repo
        self.settlement_repo = settlement_repo
        
    async def create_payment_advice(
        self,
        email_log_uuid: str,
        legal_entity_uuid: str,
        group_uuids: List[str],
        llm_output: Dict[str, Any]
    ) -> str:
        """
        Create a payment advice record and its related invoice, other doc, and settlement records.
        
        Args:
            email_log_uuid: UUID of the email log
            legal_entity_uuid: UUID of the legal entity
            group_uuids: List of group UUIDs
            llm_output: Output from LLM extraction
            
        Returns:
            payment_advice_uuid: UUID of the created payment advice
        """
        try:
            # Create payment advice
            payment_advice_uuid = str(uuid4())
            payment_advice = PaymentAdvice(
                payment_advice_uuid=payment_advice_uuid,
                email_log_uuid=email_log_uuid,
                legal_entity_uuid=legal_entity_uuid,
                payment_advice_status=PaymentAdviceStatus.NEW.value,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            await self.payment_advice_repo.create(payment_advice)
            logger.info(f"Created payment advice {payment_advice_uuid} for email log {email_log_uuid}")
            
            # Process invoice data
            if "invoiceTable" in llm_output and llm_output["invoiceTable"]:
                await self._process_invoices(payment_advice_uuid, llm_output["invoiceTable"])
                
            # Process other doc data
            if "otherDocTable" in llm_output and llm_output["otherDocTable"]:
                await self._process_other_docs(payment_advice_uuid, llm_output["otherDocTable"])
                
            # Process settlement data
            if "settlementTable" in llm_output and llm_output["settlementTable"]:
                await self._process_settlements(payment_advice_uuid, llm_output["settlementTable"])
                
            # Update payment advice status
            await self.payment_advice_repo.update_status(payment_advice_uuid, PaymentAdviceStatus.FETCHED)
            
            return payment_advice_uuid
            
        except Exception as e:
            logger.error(f"Error creating payment advice: {str(e)}")
            raise
            
    async def _process_invoices(self, payment_advice_uuid: str, invoice_data: List[Dict[str, Any]]) -> None:
        """Process invoice data and create invoice records."""
        try:
            for item in invoice_data:
                # Skip if missing required fields
                if "invoice_number" not in item:
                    logger.warning(f"Skipping invoice without invoice_number: {item}")
                    continue
                    
                # Check if invoice already exists for this payment advice
                existing = await self.invoice_repo.find_by_unique_key(
                    payment_advice_uuid, 
                    item["invoice_number"]
                )
                
                if existing:
                    logger.info(f"Invoice {item['invoice_number']} already exists for payment advice {payment_advice_uuid}")
                    continue
                    
                # Create new invoice
                invoice = Invoice(
                    invoice_uuid=str(uuid4()),
                    payment_advice_uuid=payment_advice_uuid,
                    invoice_number=item["invoice_number"],
                    invoice_date=parse_date(item.get("invoice_date")),
                    booking_amount=parse_amount(item.get("booking_amount")),
                    total_settlement_amount=parse_amount(item.get("total_settlement_amount")),
                    invoice_status=InvoiceStatus.OPEN.value,
                    sap_transaction_id=None,
                    customer_uuid=None,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                
                await self.invoice_repo.create(invoice)
                logger.info(f"Created invoice {invoice.invoice_uuid} for payment advice {payment_advice_uuid}")
                
        except Exception as e:
            logger.error(f"Error processing invoices for payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def _process_other_docs(self, payment_advice_uuid: str, other_doc_data: List[Dict[str, Any]]) -> None:
        """Process other doc data and create other doc records."""
        try:
            for item in other_doc_data:
                # Skip if missing required fields
                if "other_doc_number" not in item:
                    logger.warning(f"Skipping other doc without other_doc_number: {item}")
                    continue
                    
                # Check if other doc already exists for this payment advice
                existing = await self.other_doc_repo.find_by_unique_key(
                    payment_advice_uuid, 
                    item["other_doc_number"]
                )
                
                if existing:
                    logger.info(f"Other doc {item['other_doc_number']} already exists for payment advice {payment_advice_uuid}")
                    continue
                    
                # Create new other doc
                other_doc = OtherDoc(
                    other_doc_uuid=str(uuid4()),
                    payment_advice_uuid=payment_advice_uuid,
                    other_doc_number=item["other_doc_number"],
                    other_doc_type=item.get("other_doc_type", OtherDocType.OTHER.value),
                    other_doc_date=parse_date(item.get("other_doc_date")),
                    other_doc_amount=parse_amount(item.get("other_doc_amount")),
                    sap_transaction_id=None,
                    customer_uuid=None,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                
                await self.other_doc_repo.create(other_doc)
                logger.info(f"Created other doc {other_doc.other_doc_uuid} for payment advice {payment_advice_uuid}")
                
        except Exception as e:
            logger.error(f"Error processing other docs for payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def _process_settlements(self, payment_advice_uuid: str, settlement_data: List[Dict[str, Any]]) -> None:
        """Process settlement data and create settlement records."""
        try:
            for item in settlement_data:
                # Both invoice and other doc numbers are required
                if "invoice_number" not in item or "other_doc_number" not in item:
                    logger.warning(f"Skipping settlement without invoice_number or other_doc_number: {item}")
                    continue
                    
                # Find invoice and other doc UUIDs
                invoices = await self.invoice_repo.find_by_unique_key(
                    payment_advice_uuid, 
                    item["invoice_number"]
                )
                
                other_docs = await self.other_doc_repo.find_by_unique_key(
                    payment_advice_uuid, 
                    item["other_doc_number"]
                )
                
                if not invoices:
                    logger.warning(f"Cannot find invoice {item['invoice_number']} for payment advice {payment_advice_uuid}")
                    continue
                    
                if not other_docs:
                    logger.warning(f"Cannot find other doc {item['other_doc_number']} for payment advice {payment_advice_uuid}")
                    continue
                
                # Create settlement
                settlement_uuid = str(uuid4())
                invoice_uuid = invoices.invoice_uuid
                other_doc_uuid = other_docs.other_doc_uuid
                settlement_amount = parse_amount(item.get("settlement_amount"))
                settlement_date = parse_date(item.get("settlement_date"))
                
                settlement = Settlement(
                    settlement_uuid=settlement_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    invoice_uuid=invoice_uuid,
                    other_doc_uuid=other_doc_uuid,
                    settlement_amount=settlement_amount,
                    settlement_date=settlement_date,
                    settlement_status=SettlementStatus.READY.value,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                
                await self.settlement_repo.create(settlement)
                logger.info(f"Created settlement for payment advice {payment_advice_uuid}")
                
        except Exception as e:
            logger.error(f"Error processing settlements for payment advice {payment_advice_uuid}: {str(e)}")
            raise
            
    async def get_payment_advice(self, payment_advice_uuid: str) -> Optional[PaymentAdvice]:
        """Get a payment advice by UUID."""
        return await self.payment_advice_repo.get_by_id(payment_advice_uuid)
        
    async def get_payment_advices_by_status(self, status: PaymentAdviceStatus) -> List[PaymentAdvice]:
        """Get payment advices by status."""
        return await self.payment_advice_repo.get_by_status(status)
        
    async def update_payment_advice_status(
        self, 
        payment_advice_uuid: str, 
        status: PaymentAdviceStatus
    ) -> None:
        """Update the status of a payment advice."""
        await self.payment_advice_repo.update_status(payment_advice_uuid, status)

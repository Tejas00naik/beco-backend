"""Service layer for payment processing operations."""

import logging
import json
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
            
            # Extract meta fields from the LLM output
            # Check both camelCase and snake_case keys for metaTable/meta_table compatibility
            meta_table = llm_output.get('metaTable', llm_output.get('meta_table', {}))
            
            # Extract payment advice number - check both camelCase and snake_case keys
            payment_advice_number = meta_table.get('paymentAdviceNumber') or meta_table.get('payment_advice_number')
            logger.debug(f"Raw paymentAdviceNumber from LLM: {payment_advice_number}")
            
            # Extract payment advice date - check both camelCase and snake_case keys
            payment_advice_date = None
            date_str = meta_table.get('paymentAdviceDate') or meta_table.get('payment_advice_date') or meta_table.get('settlement_date')
            if date_str:
                payment_advice_date = parse_date(date_str)
                logger.debug(f"Parsed paymentAdviceDate from LLM: {date_str} -> {payment_advice_date}")
            
            # Extract payment advice amount - check both camelCase and snake_case keys
            payment_advice_amount = None
            amount_str = meta_table.get('paymentAdviceAmount') or meta_table.get('payment_advice_amount') or meta_table.get('payment_amount')
            if amount_str:
                payment_advice_amount = parse_amount(amount_str)
                logger.debug(f"Parsed paymentAdviceAmount from LLM: {amount_str} -> {payment_advice_amount}")
                
            # Extract payer and payee names - check both camelCase and snake_case keys
            payer_name = meta_table.get('payersLegalName') or meta_table.get('payer_legal_name') or meta_table.get('payer_name')
            payee_name = meta_table.get('payeesLegalName') or meta_table.get('payee_legal_name') or meta_table.get('payee_name')
            logger.debug(f"Extracted payer/payee names: {payer_name} / {payee_name}")
            
            # For debugging
            logger.debug(f"Full meta_table keys: {meta_table.keys()}")
            
            # Log meta fields extraction
            logger.info(f"Extracted meta fields from LLM output:")
            logger.info(f"  Payment Advice Number: {payment_advice_number}")
            logger.info(f"  Payment Advice Date: {payment_advice_date}")
            logger.info(f"  Payment Advice Amount: {payment_advice_amount}")
            logger.info(f"  Payer Name: {payer_name}")
            logger.info(f"  Payee Name: {payee_name}")
            
            # Log meta fields extracted from LLM for debugging
            logger.info(f"META FIELDS FROM LLM OUTPUT: {json.dumps(meta_table)}")
            logger.info(f"META FIELD paymentAdviceNumber: {meta_table.get('paymentAdviceNumber')}")
            logger.info(f"META FIELD paymentAdviceDate: {meta_table.get('paymentAdviceDate')}")
            logger.info(f"META FIELD paymentAdviceAmount: {meta_table.get('paymentAdviceAmount')}")
            logger.info(f"META FIELD payersLegalName: {meta_table.get('payersLegalName')}")
            logger.info(f"META FIELD payeesLegalName: {meta_table.get('payeesLegalName')}")
            
            # Create the payment advice object with meta fields
            payment_advice = PaymentAdvice(
                payment_advice_uuid=payment_advice_uuid,
                email_log_uuid=email_log_uuid,
                legal_entity_uuid=legal_entity_uuid,
                payment_advice_status=PaymentAdviceStatus.NEW.value,
                payment_advice_number=payment_advice_number,
                payment_advice_date=payment_advice_date,
                payment_advice_amount=payment_advice_amount,
                payer_name=payer_name,
                payee_name=payee_name,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            logger.info(f"PAYMENT ADVICE OBJECT: payment_advice_number={payment_advice.payment_advice_number}, payment_advice_date={payment_advice.payment_advice_date}, payment_advice_amount={payment_advice.payment_advice_amount}, payer_name={payment_advice.payer_name}, payee_name={payment_advice.payee_name}")
            
            await self.payment_advice_repo.create(payment_advice)
            logger.info(f"Created payment advice {payment_advice_uuid} for email log {email_log_uuid}")
            
            # Log full LLM output for debugging
            logger.info(f"FULL LLM OUTPUT: {json.dumps(llm_output, default=str)}")
            logger.info(f"LLM OUTPUT KEYS: {list(llm_output.keys())}")
            
            # Process invoice data - check both camelCase and snake_case keys
            if "invoiceTable" in llm_output and llm_output["invoiceTable"]:
                await self._process_invoices(payment_advice_uuid, llm_output["invoiceTable"])
            elif "invoice_table" in llm_output and llm_output["invoice_table"]:
                await self._process_invoices(payment_advice_uuid, llm_output["invoice_table"])
                
            # Process other doc data (LLM calls this settlement_table or settlementDocTable)
            if "settlementDocTable" in llm_output and llm_output["settlementDocTable"]:
                await self._process_other_docs(payment_advice_uuid, llm_output["settlementDocTable"])
            elif "settlement_table" in llm_output and llm_output["settlement_table"]:
                await self._process_other_docs(payment_advice_uuid, llm_output["settlement_table"])
            elif "otherDocTable" in llm_output and llm_output["otherDocTable"]:
                await self._process_other_docs(payment_advice_uuid, llm_output["otherDocTable"])
            elif "other_doc_table" in llm_output and llm_output["other_doc_table"]:
                await self._process_other_docs(payment_advice_uuid, llm_output["other_doc_table"])
                
            # Process settlement data (LLM calls this reconciliation_statement or reconciliationTable)
            if "reconciliationTable" in llm_output and llm_output["reconciliationTable"]:
                await self._process_settlements(payment_advice_uuid, llm_output["reconciliationTable"])
            elif "reconciliation_table" in llm_output and llm_output["reconciliation_table"]:
                await self._process_settlements(payment_advice_uuid, llm_output["reconciliation_table"])
            elif "reconciliation_statement" in llm_output and llm_output["reconciliation_statement"]:
                await self._process_settlements(payment_advice_uuid, llm_output["reconciliation_statement"])
            elif "settlementTable" in llm_output and llm_output["settlementTable"]:
                await self._process_settlements(payment_advice_uuid, llm_output["settlementTable"])
            elif "settlement_data" in llm_output and llm_output["settlement_data"]:
                await self._process_settlements(payment_advice_uuid, llm_output["settlement_data"])
                
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
                # Check invoice number is available
                if "invoice_number" not in item:
                    logger.warning(f"Skipping settlement without invoice_number: {item}")
                    continue
                
                # Map settlement_doc_number to other_doc_number if needed
                other_doc_number = item.get("other_doc_number")
                if not other_doc_number and "settlement_doc_number" in item:
                    other_doc_number = item["settlement_doc_number"]
                    # Store it in the item for later use
                    item["other_doc_number"] = other_doc_number
                
                # Skip if we still don't have an other_doc_number
                if not other_doc_number:
                    logger.warning(f"Skipping settlement without other_doc_number or settlement_doc_number: {item}")
                    continue
                    
                # Find invoice UUID
                invoices = await self.invoice_repo.find_by_unique_key(
                    payment_advice_uuid, 
                    item["invoice_number"]
                )
                
                if not invoices:
                    logger.warning(f"Cannot find invoice {item['invoice_number']} for payment advice {payment_advice_uuid}")
                    continue
                
                # Find other_doc UUID - or create if missing
                other_docs = await self.other_doc_repo.find_by_unique_key(
                    payment_advice_uuid, 
                    other_doc_number
                )
                
                # Create other_doc if it doesn't exist
                if not other_docs:
                    logger.info(f"Creating missing other_doc {other_doc_number} for payment advice {payment_advice_uuid}")
                    other_doc_uuid = str(uuid4())
                    
                    # Extract other doc fields from the settlement item
                    other_doc = OtherDoc(
                        other_doc_uuid=other_doc_uuid,
                        payment_advice_uuid=payment_advice_uuid,
                        other_doc_number=other_doc_number,
                        other_doc_type=item.get("settlement_doc_type", OtherDocType.OTHER.value),
                        other_doc_date=parse_date(item.get("settlement_date")),
                        other_doc_amount=parse_amount(item.get("total_sd_amount")),
                        sap_transaction_id=None,
                        customer_uuid=None,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    
                    await self.other_doc_repo.create(other_doc)
                    other_docs = other_doc  # Use the newly created doc
                    logger.info(f"Created other doc {other_doc_uuid} for payment advice {payment_advice_uuid}")
                else:
                    logger.info(f"Found existing other doc for {other_doc_number} in payment advice {payment_advice_uuid}")

                
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

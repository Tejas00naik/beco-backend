"""Payment advice processing functionality for the batch worker."""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

# Import models
from models.schemas import PaymentAdvice, Invoice, PaymentAdviceStatus, InvoiceStatus

# Import helpers
from src.batch_worker.helpers import parse_date, parse_amount, check_document_exists

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """
    Handles payment advice processing operations for the batch worker.
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
    
    async def create_payment_advice_from_llm_output(self, llm_output: Dict[str, Any], email_log_uuid: str) -> Optional[str]:
        """
        Process payment advice data from LLM output and create PaymentAdvice record in Firestore.
        Also update EmailLog.group_uuids with the legal entity's group_uuid.
        
        Args:
            llm_output: The structured output from LLM containing metaTable, invoiceTable, etc.
            email_log_uuid: UUID of the email being processed
            
        Returns:
            The UUID of the created payment advice, or None if creation failed
        """
        try:
            meta_table = llm_output.get('metaTable', {})
            
            # Generate a unique payment advice UUID
            payment_advice_uuid = str(uuid.uuid4())
            
            # Extract payer and payee names from LLM output
            payer_name = meta_table.get('payersLegalName')
            payee_name = meta_table.get('payeesLegalName') 
            
            # Extract other payment advice fields
            payment_advice_number = meta_table.get('paymentAdviceNumber')
            payment_advice_date = parse_date(meta_table.get('paymentAdviceDate'))
            payment_advice_amount = parse_amount(meta_table.get('paymentAdviceAmount'))
            
            # Look up legal entity UUID by payer_name
            legal_entity_uuid = None
            group_uuid = None
            if payer_name:
                try:
                    legal_entity_uuid = await self.legal_entity_lookup.lookup_legal_entity_uuid(payer_name)
                    logger.info(f"Looked up legal entity UUID for payer '{payer_name}': {legal_entity_uuid}")
                except ValueError as e:
                    logger.warning(f"Legal entity lookup error: {str(e)}")
                    # Continue with null legal_entity_uuid
                
                # If we found a legal entity, fetch its group_uuid
                if legal_entity_uuid:
                    legal_entity = await self.dao.get_document("legal_entity", legal_entity_uuid)
                    if legal_entity and "group_uuid" in legal_entity and legal_entity["group_uuid"]:
                        group_uuid = legal_entity["group_uuid"]
                        logger.info(f"Found group_uuid '{group_uuid}' for legal entity '{legal_entity_uuid}'")
                        
                        # Update EmailLog.group_uuids array - upsert group_uuid if not already present
                        email_log = await self.dao.get_document("email_log", email_log_uuid)
                        if email_log:
                            # Initialize group_uuids as empty list if it doesn't exist
                            group_uuids = email_log.get("group_uuids", [])
                            
                            # Only add if not already in the list
                            if group_uuid not in group_uuids:
                                group_uuids.append(group_uuid)
                                await self.dao.update_document("email_log", email_log_uuid, {
                                    "group_uuids": group_uuids,
                                    "updated_at": datetime.utcnow()
                                })
                                logger.info(f"Updated email_log {email_log_uuid} with group_uuid {group_uuid}")
            
            # Create PaymentAdvice object
            payment_advice = PaymentAdvice(
                payment_advice_uuid=payment_advice_uuid,
                email_log_uuid=email_log_uuid,
                legal_entity_uuid=legal_entity_uuid,
                payment_advice_number=payment_advice_number,
                payment_advice_date=payment_advice_date,
                payment_advice_amount=payment_advice_amount,
                payment_advice_status=PaymentAdviceStatus.NEW,
                payer_name=payer_name,
                payee_name=payee_name
            )
            
            # Add PaymentAdvice to Firestore
            await self.dao.add_document("payment_advice", payment_advice_uuid, payment_advice.__dict__)
            
            logger.info(f"Created payment advice {payment_advice_uuid} for email {email_log_uuid}")
            
            # Process invoices from LLM output
            invoice_table = llm_output.get('invoiceTable', [])
            for invoice_data in invoice_table:
                # Extract invoice fields from LLM output
                invoice_number = invoice_data.get('invoiceNumber')
                
                # Skip if invoice number is missing
                if not invoice_number:
                    logger.warning("Skipping invoice with missing invoice_number")
                    continue
                    
                # Check if invoice number already exists (uniqueness constraint)
                invoice_exists = await check_document_exists(self.dao, "invoice", "invoice_number", invoice_number)
                if invoice_exists:
                    logger.warning(f"Invoice with number {invoice_number} already exists - skipping")
                    continue
                
                # Parse invoice date and amounts
                invoice_date = parse_date(invoice_data.get('invoiceDate'))
                booking_amount = parse_amount(invoice_data.get('bookingAmount'))
                total_settlement_amount = parse_amount(invoice_data.get('totalSettlementAmount'))
                
                # Create Invoice object
                invoice_uuid = str(uuid.uuid4())
                invoice = Invoice(
                    invoice_uuid=invoice_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    invoice_number=invoice_number,
                    invoice_date=invoice_date,
                    booking_amount=booking_amount,
                    total_settlement_amount=total_settlement_amount,
                    invoice_status=InvoiceStatus.OPEN,
                    sap_transaction_id=None  # Will be set after successful SAP reconciliation
                )
                
                # Add Invoice to Firestore
                await self.dao.add_document("invoice", invoice_uuid, invoice.__dict__)
                logger.info(f"Created invoice {invoice_uuid} with number {invoice_number}")
            
            # TODO: Process other_docs and settlements from LLM output
            
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
        # It's functionality is now primarily in create_payment_advice_from_llm_output
        pass

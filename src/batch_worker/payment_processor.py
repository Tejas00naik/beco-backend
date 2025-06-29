"""Payment advice processing functionality for the batch worker."""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

# Import models
from models.schemas import (
    PaymentAdvice, Invoice, OtherDoc, Settlement,
    PaymentAdviceStatus, InvoiceStatus, OtherDocType, SettlementStatus
)

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
                    
                # Parse invoice date and amounts
                invoice_date = parse_date(invoice_data.get('invoiceDate'))
                booking_amount = parse_amount(invoice_data.get('bookingAmount'))
                total_settlement_amount = parse_amount(invoice_data.get('totalSettlementAmount'))
                
                # Check if invoice number already exists (uniqueness constraint)
                invoice_docs = await self.dao.query_documents("invoice", [("invoice_number", "==", invoice_number)])
                
                # Check if this exact combination of invoice_number and payment_advice_uuid already exists
                
                if invoice_docs and len(invoice_docs) > 0:
                    # We found existing invoices with this number
                    # Check if any have the same payment_advice_uuid
                    existing_invoice = None
                    for inv in invoice_docs:
                        if inv.get("payment_advice_uuid") == payment_advice_uuid:
                            existing_invoice = inv
                            break
                    
                    if existing_invoice:
                        # This exact combination of invoice_number and payment_advice_uuid already exists
                        # Log and skip - no need to create duplicate
                        logger.info(f"Invoice with number {invoice_number} already exists for this payment advice - skipping")
                        invoice_uuid = existing_invoice.get("invoice_uuid")
                        continue
                    else:
                        # There are invoices with this number but for different payment advices
                        # Since we're enforcing (payment_advice_uuid, invoice_number) uniqueness, we need to create a new one
                        # This enforces our requirement that invoice table should be unique on (payment_advice_uuid, invoice_number)
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
                        logger.info(f"Created invoice {invoice_uuid} with number {invoice_number} for payment advice {payment_advice_uuid}")
                else:
                    # Create new Invoice object
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
            
            # Process other docs from LLM output
            other_doc_table = llm_output.get('otherDocTable', [])
            other_doc_uuids = {}  # To map otherDocNumber -> other_doc_uuid for settlements
            
            for other_doc_data in other_doc_table:
                # Extract other doc fields from LLM output
                other_doc_number = other_doc_data.get('otherDocNumber')
                
                # Skip if other doc number is missing
                if not other_doc_number:
                    logger.warning("Skipping other doc with missing other_doc_number")
                    continue
                    
                # Check if other doc already exists (uniqueness constraint)
                other_doc_docs = await self.dao.query_documents("other_doc", [("other_doc_number", "==", other_doc_number)])
                if other_doc_docs and len(other_doc_docs) > 0:
                    # Check if any have the same payment_advice_uuid
                    existing_other_doc = None
                    for doc in other_doc_docs:
                        if doc.get("payment_advice_uuid") == payment_advice_uuid:
                            existing_other_doc = doc
                            break
                            
                    if existing_other_doc:
                        # This exact combination of other_doc_number and payment_advice_uuid already exists
                        # Log and skip - no need to create duplicate
                        logger.info(f"Other doc with number {other_doc_number} already exists for this payment advice - skipping")
                        other_doc_uuid = existing_other_doc.get("other_doc_uuid")
                        other_doc_uuids[other_doc_number] = other_doc_uuid
                        continue
                    else:
                        # There are other docs with this number but for different payment advices
                        # Since we're enforcing (payment_advice_uuid, other_doc_number) uniqueness, we need to create a new one
                        # This enforces our requirement that other_doc table should be unique on (payment_advice_uuid, other_doc_number)
                        other_doc_uuid = str(uuid.uuid4())
                        
                        # Parse other doc date and amount if available
                        other_doc_date = parse_date(other_doc_data.get('otherDocDate'))
                        other_doc_amount = parse_amount(other_doc_data.get('otherDocAmount'))
                        
                        # Create a new Other Doc object
                        other_doc = OtherDoc(
                            other_doc_uuid=other_doc_uuid,
                            payment_advice_uuid=payment_advice_uuid,
                            customer_uuid=None,  # Will be populated by SAP enrichment later
                            other_doc_number=other_doc_number,
                            other_doc_date=other_doc_date,
                            other_doc_amount=other_doc_amount,
                            other_doc_type=OtherDocType[other_doc_data.get('otherDocType', 'UNKNOWN')],
                            sap_transaction_id=None  # Will be populated by SAP enrichment later
                        )
                        
                        # Add Other Doc to Firestore
                        await self.dao.add_document("other_doc", other_doc_uuid, other_doc.__dict__)
                        logger.info(f"Created other doc {other_doc_uuid} with number {other_doc_number}")
                        other_doc_uuids[other_doc_number] = other_doc_uuid
                        continue
                
                # Parse other doc date and amount
                other_doc_date = parse_date(other_doc_data.get('otherDocDate'))
                other_doc_amount = parse_amount(other_doc_data.get('otherDocAmount'))
                
                # Determine other doc type
                other_doc_type_str = other_doc_data.get('otherDocType', 'OTHER')
                try:
                    other_doc_type = OtherDocType(other_doc_type_str)
                except ValueError:
                    logger.warning(f"Invalid other doc type '{other_doc_type_str}', using OTHER")
                    other_doc_type = OtherDocType.OTHER
                
                # Create OtherDoc object
                other_doc_uuid = str(uuid.uuid4())
                other_doc = OtherDoc(
                    other_doc_uuid=other_doc_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    customer_uuid=None,  # Will be set in future SAP integration
                    other_doc_number=other_doc_number,
                    other_doc_date=other_doc_date,
                    other_doc_type=other_doc_type,
                    other_doc_amount=other_doc_amount,
                    sap_transaction_id=None  # Will be set after successful SAP reconciliation
                )
                
                # Add OtherDoc to Firestore
                await self.dao.add_document("other_doc", other_doc_uuid, other_doc.__dict__)
                logger.info(f"Created other doc {other_doc_uuid} with number {other_doc_number}")
                
                # Store mapping for settlement linking
                other_doc_uuids[other_doc_number] = other_doc_uuid
            
            # Process settlements from LLM output
            settlement_table = llm_output.get('settlementTable', [])
            invoice_uuids = {}  # Need to look up invoice UUIDs by invoice number
            settlements_created = 0
            settlement_errors = 0
            
            # First, query all invoices for this payment advice for faster lookup
            # We can't just query by payment_advice_uuid since we might have updated existing invoices
            # to associate them with the new payment_advice_uuid but haven't created new invoice records
            # Instead we'll build a list of invoice numbers from the settlement table and query for those
            invoice_numbers = []
            for settlement in settlement_table:
                if 'invoiceNumber' in settlement and settlement['invoiceNumber']:
                    invoice_numbers.append(settlement['invoiceNumber'])
            
            # Get unique invoice numbers
            invoice_numbers = list(set(invoice_numbers))
            
            # If we have invoice numbers, query for them
            if invoice_numbers:
                for invoice_number in invoice_numbers:
                    invoice_query = await self.dao.query_documents("invoice", [("invoice_number", "==", invoice_number)])
                    if invoice_query and len(invoice_query) > 0:
                        invoice_uuids[invoice_number] = invoice_query[0].get("invoice_uuid")
            
            logger.info(f"Found {len(invoice_uuids)} invoices and {len(other_doc_uuids)} other docs for payment advice {payment_advice_uuid}")
            
            for settlement_data in settlement_table:
                # Extract settlement fields
                settlement_doc_number = settlement_data.get('settlementDocNumber')
                invoice_number = settlement_data.get('invoiceNumber')
                settlement_amount = parse_amount(settlement_data.get('settlementAmount'))
                
                # Skip if settlement doc number is missing
                if not settlement_doc_number:
                    logger.warning("Skipping settlement with missing settlement_doc_number")
                    settlement_errors += 1
                    continue
                
                # Determine if this settlement is linked to an invoice or other doc
                invoice_uuid = None
                other_doc_uuid = None
                
                # Try to find matching invoice
                if invoice_number:
                    if invoice_number in invoice_uuids:
                        invoice_uuid = invoice_uuids[invoice_number]
                    else:
                        # Try to find it directly in the database by invoice number
                        invoice_query = await self.dao.query_documents("invoice", [("invoice_number", "==", invoice_number)])
                        if invoice_query and len(invoice_query) > 0:
                            invoice_uuid = invoice_query[0].get("invoice_uuid")
                            # Update our cache
                            invoice_uuids[invoice_number] = invoice_uuid
                        else:
                            logger.warning(f"Invoice number {invoice_number} not found in database for payment advice {payment_advice_uuid}")
                
                # Try to find matching other doc
                if settlement_doc_number in other_doc_uuids:
                    other_doc_uuid = other_doc_uuids[settlement_doc_number]
                else:
                    # Try to find it directly in the database by other doc number
                    other_doc_query = await self.dao.query_documents("other_doc", [("other_doc_number", "==", settlement_doc_number)])
                    if other_doc_query and len(other_doc_query) > 0:
                        other_doc_uuid = other_doc_query[0].get("other_doc_uuid")
                        # Update our cache
                        other_doc_uuids[settlement_doc_number] = other_doc_uuid
                    else:
                        logger.warning(f"Other doc number {settlement_doc_number} not found in database for payment advice {payment_advice_uuid}")
                
                # Now we need BOTH invoice_uuid and other_doc_uuid to be set
                if not invoice_uuid or not other_doc_uuid:
                    logger.error(f"Skipping settlement with doc number {settlement_doc_number} and invoice number {invoice_number} - both must be linked")
                    settlement_errors += 1
                    continue
                
                # Log that we're linking to both invoice and other doc
                logger.info(f"Settlement will link to both invoice {invoice_uuid} and other doc {other_doc_uuid}")
                
                # Create Settlement object
                try:
                    settlement_uuid = str(uuid.uuid4())
                    settlement = Settlement(
                        settlement_uuid=settlement_uuid,
                        payment_advice_uuid=payment_advice_uuid,
                        customer_uuid=None,  # Will be derived from invoice/other_doc in future
                        invoice_uuid=invoice_uuid,
                        other_doc_uuid=other_doc_uuid,
                        settlement_date=payment_advice_date,  # Use payment advice date as settlement date
                        settlement_amount=settlement_amount,
                        settlement_status=SettlementStatus.READY
                    )
                    
                    # Add Settlement to Firestore
                    await self.dao.add_document("settlement", settlement_uuid, settlement.__dict__)
                    logger.info(f"Created settlement {settlement_uuid} linked to invoice {invoice_number} and other doc {settlement_doc_number}")
                    settlements_created += 1
                    
                except ValueError as e:
                    logger.warning(f"Failed to create settlement: {str(e)}")
                    settlement_errors += 1
                    continue
            
            # Update payment advice status based on settlement processing result
            if settlements_created > 0 and settlement_errors == 0 and len(settlement_table) == settlements_created:
                # Only set to FETCHED if all settlements were processed successfully (no errors)
                await self.dao.update_document("payment_advice", payment_advice_uuid, {
                    "payment_advice_status": PaymentAdviceStatus.FETCHED
                })
                logger.info(f"Updated payment advice {payment_advice_uuid} status to FETCHED after processing all {settlements_created} settlements successfully")
            elif settlements_created > 0 and settlement_errors > 0:
                # If some settlements were created but others failed, mark as PARTIAL_FETCHED
                await self.dao.update_document("payment_advice", payment_advice_uuid, {
                    "payment_advice_status": PaymentAdviceStatus.PARTIAL_FETCHED
                })
                logger.warning(f"Updated payment advice {payment_advice_uuid} status to PARTIAL_FETCHED with {settlements_created} successful and {settlement_errors} failed settlements")
            elif settlement_errors > 0:
                # If all settlements failed (none created), mark as ERROR
                await self.dao.update_document("payment_advice", payment_advice_uuid, {
                    "payment_advice_status": PaymentAdviceStatus.ERROR
                })
                logger.warning(f"Updated payment advice {payment_advice_uuid} status to ERROR due to {settlement_errors} settlement errors with no successful settlements")
            
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

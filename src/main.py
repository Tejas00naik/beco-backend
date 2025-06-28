"""
Main Orchestrator for Payment Advice Batch Worker

This is the entry point for the batch worker that processes emails,
extracts payment advice data, stores it in Firestore, and calls SAP B1.
"""

import os
import sys
import json
import logging
import asyncio
import uuid
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple, Union, Literal, Set
from pathlib import Path
import argparse
from dotenv import load_dotenv
from os.path import abspath, dirname

# Add project root to Python path
project_root = dirname(dirname(abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import data models
from models.schemas import (
    EmailLog, PaymentAdvice, Invoice, OtherDoc, Settlement,
    BatchRun, EmailProcessingLog, ProcessingStatus, BatchRunStatus,
    PaymentAdviceStatus, InvoiceStatus, SettlementStatus, OtherDocType
)
from models.firestore_dao import FirestoreDAO

# Import adapters
from src.adapters.gcs_uploader import GCSUploader
from src.adapters.gmail_reader import GmailReader, GMAIL_AVAILABLE
from src.config import DEFAULT_GCS_BUCKET_NAME
from src.services.legal_entity_lookup import LegalEntityLookupService

# Import configuration
from src.config import (
    TARGET_MAILBOX_ID,
    ALLOWED_MAILBOX_IDS,
    DEFAULT_FETCH_DAYS,
    DEFAULT_GMAIL_CREDENTIALS_PATH,
    EMAIL_OBJECT_FILENAME
)

# Import application components
from src.mocks.email_reader import MockEmailReader
from src.mocks.llm_extractor import MockLLMExtractor
from src.mocks.sap_caller import MockSapCaller

# Check if Gmail is available (already imported at the top)
GMAIL_AVAILABLE = True


class BatchWorker:
    """Main orchestrator for the email processing batch worker."""
    
    async def check_document_exists(self, collection: str, field: str, value: str) -> bool:
        """
        Check if a document with the given field value already exists in the collection.
        
        Args:
            collection: Collection name to check in
            field: Field name to check
            value: Value to check for
            
        Returns:
            True if document exists, False otherwise
        """
        if not value:  # Skip check for empty values
            return False
            
        # Use Firestore query to check if document exists
        try:
            # Get documents with the matching field value
            docs = await self.dao.query_documents(
                collection,
                filters=[(field, "==", value)],
                limit=1
            )
            # If any documents are found, the value exists
            return len(docs) > 0
        except Exception as e:
            logger.error(f"Error checking if document exists in {collection}: {str(e)}")
            return False

    def __init__(self, 
                 is_test: bool = False, 
                 mailbox_id: str = "default",
                 run_mode: Literal["incremental", "full_refresh"] = "incremental",
                 use_gmail: bool = False,
                 gmail_credentials_path: str = None):
        """
        Initialize the batch worker.
        
        Args:
            is_test: If True, use test mode with dev_ collection prefix
            mailbox_id: Identifier for the mailbox being processed
            run_mode: Either 'incremental' or 'full_refresh'
            use_gmail: Whether to use the Gmail adapter instead of mock reader
            gmail_credentials_path: Path to Gmail API credentials file
            gcs_bucket_name: Name of the GCS bucket for storing email objects (optional)
        """
        # Load environment variables
        self.project_id = os.environ.get("FIRESTORE_PROJECT_ID")
        if not self.project_id:
            raise ValueError("FIRESTORE_PROJECT_ID environment variable not set")
            
        collection_prefix = "dev_" if is_test else ""
        self.mailbox_id = mailbox_id
        self.run_mode = run_mode
        
        # Initialize DAO
        self.dao = FirestoreDAO(project_id=self.project_id, collection_prefix=collection_prefix)
        
        # Initialize email reader (mock or Gmail)
        if use_gmail:
            if not GMAIL_AVAILABLE:
                raise ImportError("Gmail adapter not available. Make sure required packages are installed.")
            if not gmail_credentials_path:
                gmail_credentials_path = os.environ.get("GMAIL_CREDENTIALS_PATH")
                if not gmail_credentials_path:
                    raise ValueError("Gmail credentials path required but not provided")
            self.email_reader = GmailReader(credentials_path=gmail_credentials_path, mailbox_id=mailbox_id)
            logger.info(f"Using Gmail reader with mailbox_id={mailbox_id}")
        else:
            self.email_reader = MockEmailReader(is_test=True, max_emails=int(os.environ.get("MOCK_EMAIL_MAX_COUNT", "5")))
            logger.info(f"Using mock email reader")
            
        # Initialize LLM extractor, SAP caller, and legal entity lookup service
        self.llm_extractor = MockLLMExtractor()
        self.sap_caller = MockSapCaller()
        self.legal_entity_lookup = LegalEntityLookupService(dao=self.dao)
        
        # Initialize GCS uploader using bucket name from environment variable or default
        self.gcs_uploader = None
        gcs_bucket_name = os.environ.get("GCS_BUCKET_NAME", DEFAULT_GCS_BUCKET_NAME)
        try:
            self.gcs_uploader = GCSUploader(gcs_bucket_name)
            logger.info(f"Initialized GCS uploader for bucket: {gcs_bucket_name}")
        except Exception as e:
            logger.warning(f"Failed to initialize GCS uploader: {str(e)}. Email raw data will not be stored.")
        
        # Initialize batch run stats
        self.batch_run = None
        self.emails_processed = 0
        self.errors = 0
        
        logger.info(f"BatchWorker initialized with project_id={self.project_id}, is_test={is_test}, " +
                   f"mailbox_id={mailbox_id}, run_mode={run_mode}")

    async def start_batch_run(self) -> str:
        """
        Start a new batch run and log it.
        
        Returns:
            Batch run ID
        """
        run_id = str(uuid.uuid4())
        self.batch_run = BatchRun(
            run_id=run_id,
            start_ts=datetime.utcnow(),
            status=BatchRunStatus.SUCCESS,
            mailbox_id=self.mailbox_id,
            run_mode=self.run_mode
        )
        
        # Store batch run in Firestore
        await self.dao.add_document("batch_run", run_id, self.batch_run)
        logger.info(f"Started batch run with ID {run_id}, mailbox_id={self.mailbox_id}, mode={self.run_mode}")
        
        return run_id
    
    async def process_email(self, email_data: Dict[str, Any]) -> bool:
        """
        Process a single email.
        
        Args:
            email_data: Email data from the reader
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Get or generate email UUID 
            email_log_uuid = email_data.get("email_id", str(uuid.uuid4()))
            # The original gmail_id is not needed in our simplified schema
            
            # Extract components from the email - handle both mock and real emails
            raw_email_data = email_data.get("raw_email")
            # For mock emails, use 'content' as text_content if raw_email is not available
            text_content = email_data.get("text_content") or email_data.get("content")
            html_content = email_data.get("html_content")
            attachments = email_data.get("attachments", [])
            
            gcs_folder_uri = None
            try:
                # Upload all email components to GCS in a single folder
                upload_result = self.gcs_uploader.upload_email_complete(
                    email_log_uuid,
                    raw_email_data,
                    text_content,
                    html_content,
                    attachments
                )
                
                # We only need to store the base folder URI
                # The folder contains all email components with standard naming
                folder_path = f"emails/{email_log_uuid}"
                gcs_folder_uri = f"gs://{self.gcs_uploader.bucket_name}/{folder_path}"
                
                logger.info(f"Uploaded email {email_log_uuid} to GCS with {len(attachments)} attachments")
            except Exception as e:
                logger.error(f"Failed to upload email to GCS: {str(e)}")
                # Continue processing even if GCS upload fails
                gcs_folder_uri = None
            
            # Prepare data for EmailLog
            received_at = email_data.get("received_at") or datetime.now()
            
            # Create EmailLog in Firestore
            email_log = EmailLog(
                email_log_uuid=email_log_uuid,  # Fixed to use email_log_uuid
                email_subject=email_data.get("subject", ""),
                sender_mail=email_data.get("sender_mail", ""),
                original_sender_mail=email_data.get("original_sender_mail"),
                mailbox_id=self.mailbox_id,
                received_at=received_at,
                gcs_folder_uri=gcs_folder_uri,
                group_uuids=[],  # Will be populated later during payment advice processing
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            # Add EmailLog to Firestore
            # Use __dict__ to get dict representation of the dataclass
            await self.dao.add_document("email_log", email_log.email_log_uuid, email_log.__dict__)
            
            # Track email processing with EmailProcessingLog
            processing_id = f"{email_log.email_log_uuid}_{self.batch_run.run_id}"
            email_processing_log = EmailProcessingLog(
                email_log_uuid=email_log.email_log_uuid,
                run_id=self.batch_run.run_id,
                processing_status=ProcessingStatus.PARSED,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            await self.dao.add_document(
                "email_processing_log", 
                processing_id,
                email_processing_log.__dict__
            )
            
            # For first phase, we just need to store the email metadata
            # and mark the processing as complete
            await self.dao.update_document(
                "email_processing_log",
                processing_id,
                {
                    "processing_status": ProcessingStatus.PARSED, 
                    "updated_at": datetime.utcnow()
                }
            )
            
            # Process each attachment through LLM extractor
            # Each attachment is treated as a separate payment advice
            if self.llm_extractor and attachments:
                logger.info(f"Processing {len(attachments)} attachments with LLM for email {email_log.email_log_uuid}")
                
                # Get the email text content
                email_text_content = text_content or ""
                
                processed_attachments = 0
                for attachment_idx, attachment in enumerate(attachments):
                    try:
                        attachment_filename = attachment.get('filename', f'attachment-{attachment_idx}')
                        logger.info(f"Processing attachment {attachment_idx+1}/{len(attachments)}: {attachment_filename}")
                        
                        # Call LLM for this specific attachment
                        llm_output = self.llm_extractor.process_attachment_for_payment_advice(
                            email_text_content, attachment
                        )
                        
                        # Print summary of extracted data
                        logger.info(f"LLM extracted data for attachment {attachment_filename}:")
                        logger.info(f"  Meta Table: Payment advice number {llm_output.get('metaTable', {}).get('paymentAdviceNumber')}")
                        logger.info(f"  Invoice Table: {len(llm_output.get('invoiceTable', []))} items")
                        logger.info(f"  Other Doc Table: {len(llm_output.get('otherDocTable', []))} items")
                        logger.info(f"  Settlement Table: {len(llm_output.get('settlementTable', []))} items")
                        
                        # Process payment advice data and create records in Firestore
                        await self.create_payment_advice_from_llm_output(llm_output, email_log.email_log_uuid)
                        
                        processed_attachments += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing attachment {attachment_idx} with LLM: {str(e)}")
                
                logger.info(f"Successfully processed {processed_attachments}/{len(attachments)} attachments with LLM")
            
            # Update success count
            self.emails_processed += 1
            
            logger.info(f"Successfully processed email {email_log.email_log_uuid}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing email: {str(e)}")
            self.errors += 1
            
            # Create error log
            try:
                # Use the same email_log_uuid we generated earlier, or get it from data
                error_email_uuid = email_data.get("email_id", str(uuid.uuid4()))
                processing_log = EmailProcessingLog(
                    email_log_uuid=error_email_uuid,
                    run_id=self.batch_run.run_id,
                    processing_status=ProcessingStatus.ERROR,
                    error_msg=str(e)
                )
                
                doc_id = f"{error_email_uuid}_{self.batch_run.run_id}"
                await self.dao.add_document("email_processing_log", doc_id, processing_log.__dict__)
            except Exception as log_error:
                logger.error(f"Failed to create error log: {str(log_error)}")
            
            return False

    async def create_payment_advice_from_llm_output(self, llm_output: Dict[str, Any], email_log_uuid: str) -> Optional[str]:
        """
        Process payment advice data from LLM output and create PaymentAdvice record in Firestore.
        
        Args:
            llm_output: The structured output from LLM containing metaTable, invoiceTable, etc.
            email_log_uuid: The UUID of the EmailLog this payment advice is associated with
            
        Returns:
            The UUID of the created payment advice, or None if creation failed
        """
        try:
            # Extract metadata from LLM output
            meta_table = llm_output.get('metaTable', {})
            
            # Generate a unique payment advice UUID
            payment_advice_uuid = str(uuid.uuid4())  # Use uuid.uuid4() instead of uuid4
            
            # Extract payer and payee names from LLM output
            payer_name = meta_table.get('payersLegalName')
            payee_name = meta_table.get('payeesLegalName')
            
            # Get legal entity UUID using two-step lookup (direct lookup + LLM fallback)
            legal_entity_uuid = None
            if payer_name:
                try:
                    legal_entity_uuid = await self.legal_entity_lookup.lookup_legal_entity_uuid(payer_name)
                    logger.info(f"Looked up legal entity UUID for payer '{payer_name}': {legal_entity_uuid}")
                except ValueError as e:
                    # Legal entity not registered - log error but continue with null UUID
                    logger.error(f"Legal entity lookup error: {str(e)}")
                    # For now, we'll continue with a null legal_entity_uuid
                    # In production, you might want to flag this payment advice or handle differently
            
            # Parse payment advice date
            payment_advice_date = None
            date_str = meta_table.get('paymentAdviceDate')
            if date_str:
                try:
                    # Try to parse date string in common formats
                    formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y']
                    for fmt in formats:
                        try:
                            payment_advice_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                except Exception as e:
                    logger.warning(f"Failed to parse payment advice date '{date_str}': {str(e)}")
            
            # Extract other payment advice details
            payment_advice_number = meta_table.get('paymentAdviceNumber')
            
            # Calculate payment advice amount as the sum of all invoice amounts minus the sum of all other doc amounts
            # This is just an example calculation - adjust based on your business rules
            invoice_amounts = [float(inv.get('bookingAmount', 0) or 0) for inv in llm_output.get('invoiceTable', [])]
            other_doc_amounts = [float(doc.get('otherDocAmount', 0) or 0) for doc in llm_output.get('otherDocTable', [])]
            
            payment_advice_amount = sum(invoice_amounts) + sum(other_doc_amounts)  # other_doc_amounts may be negative
            
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
            
            # TODO: Process invoices, other_docs, and settlements from LLM output
            # This would involve creating records in the respective collections
            # and linking them to this payment_advice_uuid
            
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
        try:
            # Create payment advice record
            advice_uuid = str(uuid.uuid4())
            payment_advice = PaymentAdvice(
                payment_advice_uuid=advice_uuid,
                email_log_uuid=email_log_uuid,
                legal_entity_uuid=pa_data.get("legal_entity_uuid"),  # Legal entity UUID from LLM
                payment_advice_number=pa_data.get("payment_advice_number"),
                payment_advice_date=pa_data.get("payment_advice_date"),
                payment_advice_amount=pa_data.get("payment_advice_amount"),
                payment_advice_status=PaymentAdviceStatus.NEW,
                payer_name=pa_data.get("payer_name"),
                payee_name=pa_data.get("payee_name")
            )      
            
            # Save payment advice to Firestore
            await self.dao.add_document("payment_advice", payment_advice.payment_advice_uuid, payment_advice)
            logger.info(f"Created payment advice {payment_advice.payment_advice_uuid}")
            
            # Extract transaction details using LLM
            invoices_data, other_docs_data, settlements_data = self.llm_extractor.extract_transaction_details(
                email_data, pa_index
            )
            
            # Create invoice records
            invoice_uuids = []
            for i, inv_data in enumerate(invoices_data):
                invoice_number = inv_data.get("invoice_number")
                
                # Check if invoice number already exists (uniqueness constraint)
                if invoice_number:
                    invoice_exists = await self.check_document_exists("invoice", "invoice_number", invoice_number)
                    if invoice_exists:
                        logger.warning(f"Invoice with number {invoice_number} already exists - skipping")
                        continue
                
                invoice_uuid = str(uuid.uuid4())
                invoice = Invoice(
                    invoice_uuid=invoice_uuid,
                    payment_advice_uuid=advice_uuid,
                    customer_uuid=inv_data.get("customer_uuid"),  # Customer UUID from LLM per invoice
                    invoice_number=invoice_number,
                    invoice_date=inv_data.get("invoice_date"),
                    booking_amount=inv_data.get("booking_amount"),
                    total_settlement_amount=None,  # Will be updated as settlements are added
                    invoice_status=InvoiceStatus.OPEN,
                    sap_transaction_id=None  # Will be set after successful SAP reconciliation
                )
                
                # Save invoice to Firestore
                await self.dao.add_document("invoice", invoice_uuid, invoice)
                invoice_uuids.append(invoice_uuid)
                logger.info(f"Created invoice {invoice.invoice_uuid} with number {invoice_number}")
            
            # Create other document records
            other_doc_uuids = []
            for i, doc_data in enumerate(other_docs_data):
                other_doc_number = doc_data.get("other_doc_number")
                
                # Check if other doc number already exists (uniqueness constraint)
                if other_doc_number:
                    other_doc_exists = await self.check_document_exists("other_doc", "other_doc_number", other_doc_number)
                    if other_doc_exists:
                        logger.warning(f"Other document with number {other_doc_number} already exists - skipping")
                        continue
                
                other_doc_uuid = str(uuid.uuid4())
                other_doc = OtherDoc(
                    other_doc_uuid=other_doc_uuid,
                    payment_advice_uuid=advice_uuid,
                    customer_uuid=doc_data.get("customer_uuid"),  # Customer UUID from LLM per other_doc
                    other_doc_number=other_doc_number,
                    other_doc_date=doc_data.get("other_doc_date"),
                    other_doc_type=doc_data.get("other_doc_type", OtherDocType.OTHER),
                    other_doc_amount=doc_data.get("other_doc_amount"),
                    sap_transaction_id=None  # Will be set after successful SAP reconciliation
                )
                
                # Save other doc to Firestore
                await self.dao.add_document("other_doc", other_doc_uuid, other_doc)
                other_doc_uuids.append(other_doc_uuid)
                logger.info(f"Created other doc {other_doc.other_doc_uuid} with number {other_doc_number}")
            
            # Process settlements and call SAP
            settlement_idx = 0
            
            # Create settlements for invoices
            for i, invoice_uuid in enumerate(invoice_uuids):
                if i < len(settlements_data):
                    settlement_uuid = str(uuid.uuid4())
                    settlement = Settlement(
                        settlement_uuid=settlement_uuid,
                        payment_advice_uuid=advice_uuid,
                        customer_uuid=invoices_data[i].get("customer_uuid"),  # Customer UUID from invoice
                        invoice_uuid=invoice_uuid,
                        other_doc_uuid=None,
                        settlement_date=settlements_data[i].get("settlement_date"),
                        settlement_amount=settlements_data[i].get("settlement_amount"),
                        settlement_status=SettlementStatus.READY
                    )
                    
                    # Save settlement to Firestore
                    await self.dao.create_settlement(settlement)
                    logger.info(f"Created settlement {settlement.settlement_uuid}")
                    
                    # Call SAP endpoint
                    await self.call_sap_reconciliation(payment_advice, settlement)
                    settlement_idx += 1
            
            # Create settlements for other documents
            for i, other_doc_uuid in enumerate(other_doc_uuids):
                if i + len(invoice_uuids) < len(settlements_data):
                    settlement_uuid = str(uuid.uuid4())
                    settlement = Settlement(
                        settlement_uuid=settlement_uuid,
                        payment_advice_uuid=advice_uuid,
                        customer_uuid=other_docs_data[i].get("customer_uuid"),  # Customer UUID from other_doc
                        invoice_uuid=None,
                        other_doc_uuid=other_doc_uuid,
                        settlement_date=settlements_data[i + len(invoice_uuids)].get("settlement_date"),
                        settlement_amount=settlements_data[i + len(invoice_uuids)].get("settlement_amount"),
                        settlement_status=SettlementStatus.READY
                    )
                    
                    # Save settlement to Firestore
                    await self.dao.create_settlement(settlement)
                    logger.info(f"Created settlement {settlement.settlement_uuid}")
                    
                    # Call SAP endpoint
                    await self.call_sap_reconciliation(payment_advice, settlement)
                    settlement_idx += 1
                    
        except Exception as e:
            logger.error(f"Error processing payment advice {pa_index} from email {email_log_uuid}: {str(e)}")
            self.errors += 1
            raise

    async def call_sap_reconciliation(self, payment_advice: PaymentAdvice, settlement: Settlement) -> None:
        """
        Call the SAP B1 reconciliation endpoint for a settlement.
        
        Args:
            payment_advice: Payment advice data
            settlement: Settlement data
        """
        try:
            # Convert dataclasses to dicts for the SAP call
            # Handle both datetime objects and string dates
            pa_date = payment_advice.payment_advice_date
            if pa_date and isinstance(pa_date, str):
                pa_date_str = pa_date  # Already a string, use as is
            elif pa_date:  # It's a datetime object
                pa_date_str = pa_date.isoformat()
            else:
                pa_date_str = None
                
            pa_dict = {
                "payment_advice_uuid": payment_advice.payment_advice_uuid,
                "payment_advice_number": payment_advice.payment_advice_number,
                "payment_advice_date": pa_date_str,
                "payment_advice_amount": payment_advice.payment_advice_amount
            }
            
            # Same handling for settlement date
            settlement_date = settlement.settlement_date
            if settlement_date and isinstance(settlement_date, str):
                settlement_date_str = settlement_date  # Already a string, use as is
            elif settlement_date:  # It's a datetime object
                settlement_date_str = settlement_date.isoformat()
            else:
                settlement_date_str = None
                
            settlement_dict = {
                "settlement_uuid": settlement.settlement_uuid,
                "settlement_date": settlement_date_str,
                "settlement_amount": settlement.settlement_amount,
                "invoice_uuid": settlement.invoice_uuid,
                "other_doc_uuid": settlement.other_doc_uuid
            }
            
            # Call the mock SAP endpoint
            success, response = self.sap_caller.reconcile_payment(pa_dict, settlement_dict)
            
            if success:
                # Update settlement status to 'pushed'
                await self.dao.update_document("settlement", settlement.settlement_uuid, {
                    "settlement_status": "pushed"
                })
                logger.info(f"SAP reconciliation successful for settlement {settlement.settlement_uuid}")
            else:
                # Update settlement status to 'error' and log the error
                await self.dao.update_document("settlement", settlement.settlement_uuid, {
                    "settlement_status": "error"
                })
                
                # Optionally, add to error DLQ for retry
                logger.error(f"SAP reconciliation failed for settlement {settlement.settlement_uuid}: {response}")
                
        except Exception as e:
            logger.error(f"Exception during SAP call for settlement {settlement.settlement_uuid}: {str(e)}")
            # Update settlement status to error
            try:
                await self.dao.update_document("settlement", settlement.settlement_uuid, {
                    "settlement_status": "error"
                })
            except Exception as update_error:
                logger.error(f"Failed to update settlement status: {str(update_error)}")

    async def finish_batch_run(self) -> None:
        """Complete the batch run and update the status."""
        if not self.batch_run:
            return
            
        # Determine final status
        if self.errors == 0:
            final_status = BatchRunStatus.SUCCESS
        elif self.emails_processed > 0:
            final_status = BatchRunStatus.PARTIAL
        else:
            final_status = BatchRunStatus.FAILED
        
        # Update batch run record
        updates = {
            "end_ts": datetime.utcnow(),
            "status": final_status,
            "emails_processed": self.emails_processed,
            "errors": self.errors
        }
        
        await self.dao.update_document("batch_run", self.batch_run.run_id, updates)
        logger.info(f"Finished batch run {self.batch_run.run_id} with status {final_status}")

    async def run(self) -> None:
        """Main entry point for the batch worker."""
        try:
            # Handle full refresh mode first if requested
            if self.run_mode == "full_refresh":
                logger.info(f"Running in FULL REFRESH mode for mailbox {self.mailbox_id}")
                await self.dao.clear_mailbox_data(self.mailbox_id)
            
            # Start the batch run
            run_id = await self.start_batch_run()
            logger.info(f"Starting batch worker run {run_id}")
            
            # Get the timestamp for email fetching
            since_timestamp = None
            
            if self.run_mode == "incremental":
                # Query for the most recent email log for this mailbox
                latest_emails = await self.dao.query_documents(
                    "email_log",
                    filters=[("mailbox_id", "==", self.mailbox_id)],
                    order_by="received_at",
                    desc=True,  # Get most recent first
                    limit=1
                )
                
                if latest_emails:
                    # Parse the timestamp from the most recent email
                    latest_ts = latest_emails[0].get("received_at")
                    if isinstance(latest_ts, str):
                        since_timestamp = datetime.fromisoformat(latest_ts)
                    else:
                        since_timestamp = latest_ts
                    logger.info(f"Incremental mode: Processing emails since {since_timestamp}")
                else:
                    # No existing emails - try to get initial fetch date from environment
                    initial_date_str = os.environ.get("INITIAL_FETCH_START_DATE")
                    if initial_date_str:
                        try:
                            # Parse date in ISO format (YYYY-MM-DD)
                            since_timestamp = datetime.fromisoformat(initial_date_str)
                            logger.info(f"First run: Using INITIAL_FETCH_START_DATE={initial_date_str}")
                        except ValueError:
                            logger.warning(f"Invalid INITIAL_FETCH_START_DATE format: {initial_date_str}. Using default.")
                            # Default to 7 days ago if parsing fails
                            since_timestamp = datetime.now() - timedelta(days=7)
                    else:
                        # Default to configured days ago if no environment variable
                        since_timestamp = datetime.now() - timedelta(days=DEFAULT_FETCH_DAYS)
                        logger.info(f"First run: No INITIAL_FETCH_START_DATE set, using last {DEFAULT_FETCH_DAYS} days")
            
            # Get unprocessed emails with timestamp
            try:
                logger.info(f"Fetching emails {self.mailbox_id} with since_timestamp={since_timestamp}")
                new_emails = self.email_reader.get_unprocessed_emails(since_timestamp)
            except Exception as e:
                logger.error(f"Error getting unprocessed emails: {str(e)}")
                new_emails = []
                
            logger.info(f"Found {len(new_emails)} new emails to process")
            
            if not new_emails:
                logger.info("No new emails to process")
                await self.finish_batch_run()
                return
            
            # Process each email
            processed_email_ids = []
            for email_data in new_emails:
                success = await self.process_email(email_data)
                if success:
                    processed_email_ids.append(email_data["email_id"])
            
            # Mark emails as processed in the reader
            if processed_email_ids and hasattr(self.email_reader, 'mark_as_processed'):
                self.email_reader.mark_as_processed(processed_email_ids)
            
            # Finish the batch run
            await self.finish_batch_run()
            
            logger.info(f"Batch run completed. Processed {self.emails_processed} emails with {self.errors} errors")
            
        except Exception as e:
            logger.error(f"Fatal error in batch worker: {str(e)}")
            if self.batch_run:
                try:
                    await self.dao.update_document("batch_run", self.batch_run.run_id, {
                        "end_ts": datetime.utcnow(),
                        "status": BatchRunStatus.FAILED,
                        "emails_processed": self.emails_processed,
                        "errors": self.errors + 1
                    })
                except Exception as update_error:
                    logger.error(f"Failed to update failed batch run: {str(update_error)}")
            raise


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Payment Advice Batch Worker")
    parser.add_argument("--test", action="store_true", help="Run in test mode with dev_ collection prefix")
    parser.add_argument("--mode", choices=["incremental", "full_refresh"], default="incremental", 
                        help="Run mode: incremental or full_refresh")
    parser.add_argument("--gmail", action="store_true", help="Use Gmail adapter instead of mock email reader")
    parser.add_argument("--credentials", default=DEFAULT_GMAIL_CREDENTIALS_PATH, 
                        help=f"Path to Gmail API credentials file (default: {DEFAULT_GMAIL_CREDENTIALS_PATH})")
    
    args = parser.parse_args()
    
    # Also check environment variables
    is_test = args.test or os.environ.get("TEST_MODE", "false").lower() == "true"
    
    # Initialize and run the batch worker
    worker = BatchWorker(
        is_test=is_test,
        mailbox_id=TARGET_MAILBOX_ID,  # Use hardcoded mailbox ID from config
        run_mode=args.mode,
        use_gmail=args.gmail,
        gmail_credentials_path=args.credentials
    )
    
    logger.info(f"Using hardcoded mailbox ID: {TARGET_MAILBOX_ID}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

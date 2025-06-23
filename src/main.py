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
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Literal
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure models and src are in the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import application components
from models.schemas import (
    EmailLog, PaymentAdvice, Invoice, OtherDoc, Settlement,
    BatchRun, EmailProcessingLog, ProcessingStatus, BatchRunStatus,
    PaymentAdviceStatus, InvoiceStatus, SettlementStatus, OtherDocType
)
from models.firestore_dao import FirestoreDAO
from src.mocks.email_reader import MockEmailReader
from src.mocks.llm_extractor import MockLLMExtractor
from src.mocks.sap_caller import MockSapCaller

# Import Gmail adapter if available
try:
    from src.adapters.gmail_reader import GmailReader
    GMAIL_AVAILABLE = True
except ImportError:
    GMAIL_AVAILABLE = False


class BatchWorker:
    """Main orchestrator for the email processing batch worker."""

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
            self.email_reader = MockEmailReader()
            logger.info("Using mock email reader")
            
        # Initialize other components
        self.llm_extractor = MockLLMExtractor()
        self.sap_caller = MockSapCaller()
        
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
            # Extract metadata using LLM first
            metadata = self.llm_extractor.extract_email_metadata(email_data)
            
            # Create email log entry
            email_log = EmailLog(
                email_log_uuid=email_data["email_id"],
                group_uuid=metadata.get("group_uuid"),  # Group UUID from LLM metadata
                email_object_file_path=email_data["object_file_path"],
                received_at=datetime.fromisoformat(email_data["received_at"]),
                sender_mail=email_data["sender_mail"].lower(),
                original_sender_mail=email_data.get("original_sender_mail"),
                email_subject=metadata.get("email_subject"),
                mailbox_id=self.mailbox_id
            )
            
            # Add additional metadata from LLM
            email_log.payer_name = metadata.get("payer_name")
            email_log.payee_name = metadata.get("payee_name")
            
            # Save email log to Firestore
            await self.dao.add_document("email_log", email_log.email_log_uuid, email_log)
            
            # Create processing log entry
            processing_log = EmailProcessingLog(
                email_log_uuid=email_log.email_log_uuid,
                run_id=self.batch_run.run_id,
                processing_status=ProcessingStatus.PARSED
            )
            
            # Extract payment advices
            payment_advices = self.llm_extractor.extract_payment_advices(email_data)
            
            # Process each payment advice
            for i, pa_data in enumerate(payment_advices):
                await self.process_payment_advice(email_log.email_log_uuid, pa_data, email_data, i)
            
            # Update processing log as successful
            processing_log.processing_status = ProcessingStatus.SAP_PUSHED
            doc_id = f"{email_log.email_log_uuid}_{self.batch_run.run_id}"
            await self.dao.add_document("email_processing_log", doc_id, processing_log)
            
            self.emails_processed += 1
            logger.info(f"Successfully processed email {email_log.email_log_uuid}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing email {email_data.get('email_id')}: {str(e)}")
            
            # Create error log
            try:
                processing_log = EmailProcessingLog(
                    email_log_uuid=email_data["email_id"],
                    run_id=self.batch_run.run_id,
                    processing_status=ProcessingStatus.ERROR,
                    error_msg=str(e)
                )
                
                doc_id = f"{email_data['email_id']}_{self.batch_run.run_id}"
                await self.dao.add_document("email_processing_log", doc_id, processing_log)
            except Exception as log_error:
                logger.error(f"Failed to create error log: {str(log_error)}")
            
            self.errors += 1
            return False

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
                invoice_uuid = str(uuid.uuid4())
                invoice = Invoice(
                    invoice_uuid=invoice_uuid,
                    payment_advice_uuid=advice_uuid,
                    customer_uuid=inv_data.get("customer_uuid"),  # Customer UUID from LLM per invoice
                    invoice_number=inv_data.get("invoice_number"),
                    invoice_date=inv_data.get("invoice_date"),
                    booking_amount=inv_data.get("booking_amount"),
                    invoice_status=InvoiceStatus.OPEN
                )
                
                # Save invoice to Firestore
                await self.dao.add_document("invoice", invoice_uuid, invoice)
                invoice_uuids.append(invoice_uuid)
                logger.info(f"Created invoice {invoice.invoice_uuid}")
            
            # Create other document records
            other_doc_uuids = []
            for i, doc_data in enumerate(other_docs_data):
                other_doc_uuid = str(uuid.uuid4())
                other_doc = OtherDoc(
                    other_doc_uuid=other_doc_uuid,
                    payment_advice_uuid=advice_uuid,
                    customer_uuid=doc_data.get("customer_uuid"),  # Customer UUID from LLM per other_doc
                    other_doc_number=doc_data.get("other_doc_number"),
                    other_doc_date=doc_data.get("other_doc_date"),
                    other_doc_type=doc_data.get("other_doc_type", OtherDocType.OTHER),
                    other_doc_amount=doc_data.get("other_doc_amount")
                )
                
                # Save other doc to Firestore
                await self.dao.add_document("other_doc", other_doc_uuid, other_doc)
                other_doc_uuids.append(other_doc_uuid)
                logger.info(f"Created other doc {other_doc.other_doc_uuid}")
            
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
                    
                    # Save settlement to Firestore (using deterministic ID)
                    await self.dao.create_settlement(settlement)
                    logger.info(f"Created settlement {settlement.settlement_uuid}")
                    
                    # Call SAP endpoint
                    await self.call_sap_reconciliation(payment_advice, settlement)
                    settlement_idx += 1
            
            # Create settlements for other documents
            for i, other_doc_uuid in enumerate(other_doc_uuids):
                if i + len(invoices_data) < len(settlements_data):
                    settlement_uuid = str(uuid.uuid4())
                    settlement = Settlement(
                        settlement_uuid=settlement_uuid,
                        payment_advice_uuid=advice_uuid,
                        customer_uuid=other_docs_data[i].get("customer_uuid"),  # Customer UUID from other_doc
                        invoice_uuid=None,
                        other_doc_uuid=other_doc_uuid,
                        settlement_date=settlements_data[i + len(invoices_data)].get("settlement_date"),
                        settlement_amount=settlements_data[i + len(invoices_data)].get("settlement_amount"),
                        settlement_status=SettlementStatus.READY
                    )
                    
                    # Save settlement to Firestore (using deterministic ID)
                    await self.dao.create_settlement(settlement)
                    logger.info(f"Created settlement {settlement.settlement_uuid}")
                    
                    # Call SAP endpoint
                    await self.call_sap_reconciliation(payment_advice, settlement)
                    settlement_idx += 1
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
            
            # Get the last processed timestamp for incremental mode
            since_timestamp = None
            if self.run_mode == "incremental":
                # Query for the most recent email log for this mailbox
                latest_emails = await self.dao.query_documents(
                    "email_log",
                    filters=[("mailbox_id", "==", self.mailbox_id)],
                    order_by="received_at",
                    limit=1
                )
                
                if latest_emails:
                    # Parse the timestamp string to datetime
                    latest_ts = latest_emails[0].get("received_at")
                    if isinstance(latest_ts, str):
                        since_timestamp = datetime.fromisoformat(latest_ts)
                    else:
                        since_timestamp = latest_ts
                    logger.info(f"Incremental mode: Processing emails since {since_timestamp}")
            
            # Get unprocessed emails with timestamp (now supported by both gmail and mock)
            try:
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
    parser.add_argument("--mailbox_id", default="default", help="Mailbox identifier")
    parser.add_argument("--mode", choices=["incremental", "full_refresh"], default="incremental", 
                        help="Run mode: incremental or full_refresh")
    parser.add_argument("--gmail", action="store_true", help="Use Gmail adapter instead of mock email reader")
    parser.add_argument("--credentials", help="Path to Gmail API credentials file")
    
    args = parser.parse_args()
    
    # Also check environment variables
    is_test = args.test or os.environ.get("TEST_MODE", "false").lower() == "true"
    
    # Initialize and run the batch worker
    worker = BatchWorker(
        is_test=is_test,
        mailbox_id=args.mailbox_id,
        run_mode=args.mode,
        use_gmail=args.gmail,
        gmail_credentials_path=args.credentials
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

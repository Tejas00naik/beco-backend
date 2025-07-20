"""Batch Worker V2 for processing Zepto payment advice documents.

This version focuses on processing emails with Zepto payment advice attachments and
uses a simplified flow with only meta and body tables from LLM output,
and maps them to a single paymentadvice_lines table in Firestore.
"""

import logging
import os
import uuid
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Literal

# Import models
from src.repositories.firestore_dao import FirestoreDAO
from src.models.schemas import BatchRunStatus

# Import components
from src.batch_worker.batch_manager import BatchManager
from src.services.email.email_processor import EmailProcessor
from src.services.payment_processing_service import PaymentProcessingService
from src.services.llm_extraction_service import LLMExtractionService
from src.external_apis.sap.sap_integration import SapIntegrator
import src.batch_worker.helpers as helpers

# Import configuration
from src.config import (
    TARGET_MAILBOX_ID,
    DEFAULT_FETCH_DAYS,
    DEFAULT_GMAIL_CREDENTIALS_PATH,
    DEFAULT_GCS_BUCKET_NAME
)

# Zepto group UUID - will be dynamically looked up from Firestore using legal entity name

logger = logging.getLogger(__name__)


class BatchWorkerV2:
    """
    Version 2 of the batch worker specifically for Zepto payment advice processing.
    Only processes meta and body tables and maps to paymentadvice_lines table.
    """
    
    def __init__(self, is_test: bool = False,
                 mailbox_id: str = "default",
                 run_mode: Literal["incremental", "full_refresh"] = "incremental",
                 use_gmail: bool = False,
                 gmail_credentials_path: str = None,
                 since_timestamp: Optional[datetime] = None,
                 last_n_emails: Optional[int] = None):
        """
        Initialize the batch worker v2.
        
        Args:
            is_test: If True, use test mode with dev_ collection prefix
            mailbox_id: Identifier for the mailbox being processed
            run_mode: Either 'incremental' or 'full_refresh'
            use_gmail: If True, use Gmail adapter instead of mock
            gmail_credentials_path: Path to Gmail API credentials
            since_timestamp: Optional datetime to start processing from
            last_n_emails: Optional limit for number of emails to process
        """
        self.is_test = is_test
        self.mailbox_id = mailbox_id
        self.run_mode = run_mode
        self.collection_prefix = "dev_" if is_test else ""
        self.initial_timestamp = since_timestamp  # Store the provided timestamp
        self.last_n_emails = last_n_emails  # Store the last N emails limit
        
        logger.info(f"Initializing BatchWorkerV2 (Zepto). is_test={is_test}, mailbox_id={mailbox_id}")
        
        # Initialize the DAO with the appropriate collection prefix
        self.dao = FirestoreDAO(collection_prefix=self.collection_prefix)
        
        # Initialize GCS uploader
        from src.external_apis.gcp.gcs_uploader import GCSUploader
        self.gcs_uploader = GCSUploader(bucket_name=DEFAULT_GCS_BUCKET_NAME)
        
        # Initialize email reader based on configuration
        if use_gmail and gmail_credentials_path:
            # Use Gmail adapter if available and requested
            from src.external_apis.gcp.gmail_reader import GmailReader, GMAIL_AVAILABLE
            if not GMAIL_AVAILABLE:
                raise ImportError("Gmail adapter was requested but dependencies are not available")
                
            self.email_reader = GmailReader(
                credentials_path=gmail_credentials_path or DEFAULT_GMAIL_CREDENTIALS_PATH
            )
            logger.info(f"Using GmailReader with credentials from {gmail_credentials_path}")
        else:
            # Use mock email reader only if explicitly not using Gmail
            logger.warning("Gmail adapter not requested or credentials not available")
            logger.warning("Email processing will use mock data - this should only be used for testing")
            from src.mocks.email_reader import MockEmailReader
            self.email_reader = MockEmailReader()
            logger.info("Using MockEmailReader for testing only")
        
        # Initialize services
        # Import repositories here to avoid circular imports
        from src.repositories import (
            LegalEntityRepository, 
            PaymentAdviceRepository
        )
        
        # Initialize repositories (no invoice, settlement, or other doc repos needed for V2)
        self.legal_entity_repo = LegalEntityRepository(self.dao)
        self.payment_advice_repo = PaymentAdviceRepository(self.dao)
        
        # Initialize LLM extraction service
        if not os.environ.get("OPENAI_API_KEY"):
            logger.error("OPENAI_API_KEY environment variable not set. LLM extraction will fail.")
        self.llm_service = LLMExtractionService(self.dao, self.legal_entity_repo)
        logger.info("Using LLMExtractionService with OpenAI API (GPT-4.1)")
        
        # Initialize payment processing service (modified version needed for V2)
        self.payment_service = PaymentProcessingService(
            self.payment_advice_repo,
            None,  # No invoice repo for V2
            None,  # No other_doc repo for V2
            None   # No settlement repo for V2
        )
        logger.info("Initialized PaymentProcessingService for Zepto-only processing")
        
        from src.services.legal_entity_lookup import LegalEntityLookupService
        self.legal_entity_lookup = LegalEntityLookupService(self.dao)
        
        # Initialize SAP client for SAP B1 integration
        from src.mocks.sap_client import MockSapClient
        self.sap_client = MockSapClient()
        
        # Initialize batch manager
        self.batch_manager = BatchManager(
            dao=self.dao,
            is_test=self.is_test,
            mailbox_id=self.mailbox_id,
            run_mode=self.run_mode
        )
        
        # Initialize SAP integrator
        self.sap_integrator = SapIntegrator(self.dao)
        self.sap_integrator.sap_client = self.sap_client  # Ensure SAP integrator uses our mock client
        
        # Initialize email processor
        self.email_processor = EmailProcessor(self.dao, self.gcs_uploader, self.llm_service, self.sap_integrator)
        
        # Initialize counters
        self.emails_processed = 0
        self.errors = 0
        
        # For testing - store last processed output and PDF text
        self.last_processed_output = None
        self.last_pdf_text = None
    
    async def start_batch_run(self):
        """Start a new batch run."""
        return await self.batch_manager.start_batch_run()
    
    async def finish_batch_run(self):
        """Finish the current batch run."""
        await self.batch_manager.finish_batch_run()
    
    async def process_email(self, email_data: Dict[str, Any]):
        """Process a single email."""
        try:
            # Process email and create email log
            email_log_uuid, llm_output = await self.email_processor.process_email(email_data)
            
            # Associate the email processing log with this batch run
            processing_log_id = email_log_uuid
            await self.dao.update_document("email_processing_log", processing_log_id, {
                "run_id": self.batch_manager.batch_run.run_id
            })
            
            # Print detected legal entity and group UUIDs
            legal_entity_uuid = llm_output.get("legal_entity_uuid")
            group_uuids = llm_output.get("group_uuids", [])
            
            logger.info(f"Detected legal entity UUID: {legal_entity_uuid}")
            logger.info(f"Detected group UUIDs: {group_uuids}")
            
            # Get legal entity details for verification
            if legal_entity_uuid:
                legal_entity = await self.dao.get_document("legal_entity", legal_entity_uuid)
                if legal_entity:
                    logger.info(f"Legal entity details: Name={legal_entity.get('payer_legal_name')}, Group UUID={legal_entity.get('group_uuid')}")
                    print(f"\n\n=== DETECTED LEGAL ENTITY ===\nName: {legal_entity.get('payer_legal_name')}\nUUID: {legal_entity_uuid}\nGroup UUID: {legal_entity.get('group_uuid')}\n===========================\n\n")
            
            # Print the raw LLM output
            logger.info(f"LLM OUTPUT FOR EMAIL: {llm_output}")
            print(f"\n\n=== LLM OUTPUT FOR EMAIL ===\n{json.dumps(llm_output, indent=2)}\n===========================\n\n")
            
            # Store the processed output for testing
            self.last_processed_output = llm_output
            
            # Store PDF text if available from email attachments
            if 'attachments' in email_data and email_data['attachments']:
                for attachment in email_data['attachments']:
                    if 'text_content' in attachment and attachment.get('content_type', '').lower().endswith('pdf'):
                        self.last_pdf_text = attachment['text_content']
                        logger.info(f"Stored PDF text for analysis: {len(self.last_pdf_text)} characters")
                        break
            
            # Update batch run stats
            self.batch_manager.increment_processed_count()
            self.emails_processed += 1
            return True
                
        except Exception as e:
            logger.error(f"Error processing email: {str(e)}")
            self.batch_manager.increment_error_count()
            self.errors += 1
            return False
    
    async def process_last_email(self):
        """Process just the last email for testing."""
        logger.info("Retrieving last email for Zepto testing...")
        
        # Get current timestamp
        now = datetime.now()
        
        # Get emails from the last 24 hours
        since_timestamp = now - timedelta(days=1)
        new_emails = self.email_reader.get_unprocessed_emails(since_timestamp)
        
        if not new_emails:
            logger.error("No emails found in the last 24 hours")
            return False
            
        # Sort by received_at in descending order to get most recent emails
        new_emails.sort(key=lambda x: x.get("received_at", datetime.min), reverse=True)
        
        # Get the most recent email
        last_email = new_emails[0]
        logger.info(f"Processing last email: {last_email.get('subject', 'No subject')}, received at: {last_email.get('received_at')}")
        print(f"\n\n=== PROCESSING LATEST EMAIL ===\nSubject: {last_email.get('subject', 'No subject')}\nDate: {last_email.get('received_at')}\n===========================\n\n")
        
        # Start batch run
        await self.start_batch_run()
        
        # Process the email
        success = await self.process_email(last_email)
        
        # Finish the batch run
        await self.finish_batch_run()
        
        return success
    
    async def run(self):
        """Main entry point for the batch worker."""
        try:
            # Start batch run
            await self.start_batch_run()
            
            # Determine since_timestamp based on run mode
            since_timestamp = None
            
            # If initial_timestamp was provided in constructor, use it directly
            if self.initial_timestamp:
                since_timestamp = self.initial_timestamp
                logger.info(f"Using provided initial timestamp: {since_timestamp}")
            # Otherwise set the initial timestamp based on run mode
            elif self.run_mode == "full_refresh":
                try:
                    # Check for MAX_HISTORICAL_DAYS in environment
                    max_days = os.environ.get("MAX_HISTORICAL_DAYS")
                    days = int(max_days) if max_days else 180  # Default to 180 days
                    since_timestamp = datetime.now() - timedelta(days=days)
                except ValueError:
                    logger.warning(f"Invalid MAX_HISTORICAL_DAYS value: {max_days}. Using default of 180 days.")
                    since_timestamp = datetime.now() - timedelta(days=180)
                logger.info(f"Full refresh mode: fetching emails since {since_timestamp}")
                    
            else:  # incremental mode
                # Check if this is the first run for this mailbox
                try:
                    # Query previous batch runs for this mailbox to find most recent timestamp
                    latest_batch = await self.dao.query_documents(
                        "batch_run", 
                        filters=[("is_test", "==", self.is_test)], 
                        order_by="start_ts", 
                        desc=True,
                        limit=1
                    )
                    
                    if latest_batch and len(latest_batch) > 0 and latest_batch[0].get("status") == BatchRunStatus.COMPLETED:
                        # Use timestamp from previous successful run
                        prev_start_ts = latest_batch[0].get("start_ts")
                        if prev_start_ts:
                            # Subtract a buffer period (e.g., 1 hour) to avoid missing emails at time boundaries
                            since_timestamp = prev_start_ts - timedelta(hours=1)
                            logger.info(f"Incremental mode: using timestamp from previous batch run: {since_timestamp}")
                    
                    # Check for INITIAL_FETCH_START_DATE regardless of previous batch run
                    initial_date_str = os.environ.get("INITIAL_FETCH_START_DATE", None)
                    if initial_date_str:
                        try:
                            # Parse YYYY-MM-DD format
                            since_timestamp = datetime.strptime(initial_date_str, "%Y-%m-%d")
                            logger.info(f"Found INITIAL_FETCH_START_DATE, overriding timestamp: {since_timestamp}")
                        except ValueError:
                            logger.warning(f"Invalid INITIAL_FETCH_START_DATE format: {initial_date_str}. Using default or previous.")
                            if not since_timestamp:
                                # Default to 7 days ago if parsing fails and no previous timestamp
                                since_timestamp = datetime.now() - timedelta(days=7)
                    elif not since_timestamp:  # Only set default if no timestamp and no environment variable
                        # Default to configured days ago if no environment variable
                        since_timestamp = datetime.now() - timedelta(days=DEFAULT_FETCH_DAYS)
                        logger.info(f"No start date specified, using last {DEFAULT_FETCH_DAYS} days")
                except Exception as e:
                    logger.error(f"Error finding previous batch run: {str(e)}")
                    # Default to 7 days ago
                    since_timestamp = datetime.now() - timedelta(days=DEFAULT_FETCH_DAYS)
            
            # Get unprocessed emails with timestamp
            try:
                logger.info(f"Fetching emails for {self.mailbox_id} with since_timestamp={since_timestamp}")
                new_emails = self.email_reader.get_unprocessed_emails(since_timestamp)
                
                # Apply last_n_emails limit if specified
                if self.last_n_emails and len(new_emails) > self.last_n_emails:
                    logger.info(f"Limiting to last {self.last_n_emails} emails (from {len(new_emails)} total)")
                    # Sort by received_at in descending order to get most recent emails
                    new_emails.sort(key=lambda x: x.get("received_at", datetime.min), reverse=True)
                    new_emails = new_emails[:self.last_n_emails]
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
            if self.batch_manager.batch_run:
                try:
                    await self.dao.update_document("batch_run", self.batch_manager.batch_run.run_id, {
                        "end_ts": datetime.utcnow(),
                        "status": BatchRunStatus.FAILED,
                        "emails_processed": self.emails_processed,
                        "errors": self.errors + 1
                    })
                except Exception as update_error:
                    logger.error(f"Failed to update failed batch run: {str(update_error)}")
            raise

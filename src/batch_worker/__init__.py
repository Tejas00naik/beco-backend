"""Batch worker package for processing emails, extracting payment advice data,
and storing it in Firestore.
"""

import logging
import os
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Literal

# Import models
from models.firestore_dao import FirestoreDAO
from models.schemas import BatchRunStatus

# Import components
from src.batch_worker.batch_manager import BatchManager
from src.batch_worker.email_processor import EmailProcessor
from src.batch_worker.payment_processor import PaymentProcessor
from src.batch_worker.sap_integration import SapIntegrator
import src.batch_worker.helpers as helpers

# Import configuration
from src.config import (
    TARGET_MAILBOX_ID,
    DEFAULT_FETCH_DAYS,
    DEFAULT_GMAIL_CREDENTIALS_PATH,
    DEFAULT_GCS_BUCKET_NAME
)

logger = logging.getLogger(__name__)


class BatchWorker:
    """Main orchestrator for the email processing batch worker."""
    
    def __init__(self, is_test: bool = False,
                 mailbox_id: str = "default",
                 run_mode: Literal["incremental", "full_refresh"] = "incremental",
                 use_gmail: bool = False,
                 gmail_credentials_path: str = None,
                 since_timestamp: Optional[datetime] = None):
        """
        Initialize the batch worker.
        
        Args:
            is_test: If True, use test mode with dev_ collection prefix
            mailbox_id: Identifier for the mailbox being processed
            run_mode: Either 'incremental' or 'full_refresh'
            use_gmail: If True, use Gmail adapter instead of mock
            gmail_credentials_path: Path to Gmail API credentials
        """
        self.is_test = is_test
        self.mailbox_id = mailbox_id
        self.run_mode = run_mode
        self.collection_prefix = "dev_" if is_test else ""
        self.initial_timestamp = since_timestamp  # Store the provided timestamp
        
        logger.info(f"Initializing batch worker. is_test={is_test}, mailbox_id={mailbox_id}")
        
        # Initialize the DAO with the appropriate collection prefix
        self.dao = FirestoreDAO(collection_prefix=self.collection_prefix)
        
        # Initialize GCS uploader
        from src.adapters.gcs_uploader import GCSUploader
        self.gcs_uploader = GCSUploader(bucket_name=DEFAULT_GCS_BUCKET_NAME)
        
        # Initialize email reader based on configuration
        if use_gmail and gmail_credentials_path:
            # Use Gmail adapter if available and requested
            from src.adapters.gmail_reader import GmailReader, GMAIL_AVAILABLE
            if not GMAIL_AVAILABLE:
                raise ImportError("Gmail adapter was requested but dependencies are not available")
                
            self.email_reader = GmailReader(
                credentials_path=gmail_credentials_path or DEFAULT_GMAIL_CREDENTIALS_PATH
            )
            logger.info(f"Using GmailReader with credentials from {gmail_credentials_path}")
        else:
            # Use mock email reader
            from src.mocks.email_reader import MockEmailReader
            self.email_reader = MockEmailReader()
            logger.info("Using MockEmailReader")
        
        # Initialize LLM extractor
        from src.mocks.llm_extractor import MockLLMExtractor
        self.llm_extractor = MockLLMExtractor()
        
        # Initialize Legal Entity Lookup Service
        from src.services.legal_entity_lookup import LegalEntityLookupService
        self.legal_entity_lookup = LegalEntityLookupService(self.dao)
        
        # Initialize SAP client
        from src.mocks.sap_client import MockSapClient
        self.sap_client = MockSapClient()
        
        # Initialize component modules
        self.batch_manager = BatchManager(self.dao, is_test, self.mailbox_id, self.run_mode)
        self.sap_integrator = SapIntegrator(self.dao, self.sap_client)
        self.email_processor = EmailProcessor(self.dao, self.gcs_uploader, self.llm_extractor, self.sap_integrator)
        self.payment_processor = PaymentProcessor(self.dao, self.legal_entity_lookup)
        
        # Initialize counters
        self.emails_processed = 0
        self.errors = 0
    
    async def start_batch_run(self):
        """Start a new batch run."""
        return await self.batch_manager.start_batch_run()
    
    async def process_email(self, email_data: Dict[str, Any]) -> bool:
        """Process a single email."""
        try:
            result = await self.email_processor.process_email(
                email_data, 
                self.batch_manager.batch_run.run_id, 
                self.payment_processor
            )
            
            if result:
                self.batch_manager.increment_processed_count()
                self.emails_processed += 1
            else:
                self.batch_manager.increment_error_count()
                self.errors += 1
                
            return result
            
        except Exception as e:
            logger.error(f"Error in process_email: {str(e)}")
            self.batch_manager.increment_error_count()
            self.errors += 1
            return False
    
    async def create_payment_advice_from_llm_output(self, llm_output: Dict[str, Any], email_log_uuid: str) -> Optional[str]:
        """Create payment advice from LLM output."""
        return await self.payment_processor.create_payment_advice_from_llm_output(llm_output, email_log_uuid)
    
    async def call_sap_reconciliation(self, payment_advice, settlement):
        """Call SAP reconciliation for a settlement."""
        return await self.sap_integrator.call_sap_reconciliation(payment_advice, settlement)
    
    async def finish_batch_run(self):
        """Finish the current batch run."""
        await self.batch_manager.finish_batch_run()
    
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
                    # Check for MAX_HISTORICAL_DAYS env variable
                    max_days = os.environ.get("MAX_HISTORICAL_DAYS", "180")
                    days = int(max_days)
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
                    # This allows overriding the date even when a previous batch exists
                    import os
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

"""Batch Worker V2 for processing Zepto payment advice documents.

This version focuses on processing emails with Zepto payment advice attachments and
uses a simplified flow with only meta and body tables from LLM output,
and maps them to a single paymentadvice_lines table in Firestore.
"""

import logging
import os
import json
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import asyncio
from uuid import uuid4
from enum import Enum
import time

# Import configuration
from src.config import (
    DEFAULT_FETCH_DAYS,
    DEFAULT_GMAIL_CREDENTIALS_PATH,
    DEFAULT_GCS_BUCKET_NAME
)

# Import models
from src.models.schemas import (
    EmailProcessingLog, ProcessingStatus
)

# Import components
from src.batch_worker.batch_manager import BatchManager, BatchRunStatus
from src.repositories.firestore_dao import FirestoreDAO
from src.external_apis.gcp.gcs_uploader import GCSUploader
from src.services.llm_extraction_service import LLMExtractionService
from src.external_apis.gcp.gmail_reader import GmailReader, GMAIL_AVAILABLE
from src.services.email.email_processor import EmailProcessor
from src.services.payment_processing_service import PaymentProcessingService
from src.services.sap_export_service import SAPExportService
from src.services.account_enrichment_service import AccountEnrichmentService
from src.services.monitoring_service import MonitoringService
from src.mocks.email_reader import MockEmailReader

# Import repositories
from src.repositories import (
    LegalEntityRepository, 
    PaymentAdviceRepository, 
    InvoiceRepository, 
    OtherDocRepository, 
    SettlementRepository
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
                 run_mode: str = "incremental",
                 use_gmail: bool = False,
                 gmail_credentials_path: str = None,
                 since_timestamp: Optional[datetime] = None,
                 last_n_emails: Optional[int] = None,
                 token_path: str = None):
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
            token_path: Optional path to OAuth token.json file
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
        self.gcs_uploader = GCSUploader(bucket_name=DEFAULT_GCS_BUCKET_NAME)
        
        # Initialize email reader based on configuration
        if use_gmail and gmail_credentials_path:
            # Use Gmail adapter if available and requested
            if not GMAIL_AVAILABLE:
                raise ImportError("Gmail adapter was requested but dependencies are not available")
                
            self.email_reader = GmailReader(
                credentials_path=gmail_credentials_path or DEFAULT_GMAIL_CREDENTIALS_PATH,
                token_path=token_path  # Pass token_path to GmailReader
            )
            logger.info(f"Using GmailReader with credentials from {gmail_credentials_path}")
        else:
            # Use mock email reader only if explicitly not using Gmail
            logger.warning("Gmail adapter not requested or credentials not available")
            logger.warning("Email processing will use mock data - this should only be used for testing")
            self.email_reader = MockEmailReader()
            logger.info("Using MockEmailReader for testing only")

        
        # Initialize repositories (all repos needed for payment_advice creation)
        self.legal_entity_repo = LegalEntityRepository(self.dao)
        self.payment_advice_repo = PaymentAdviceRepository(self.dao)
        self.invoice_repo = InvoiceRepository(self.dao)
        self.other_doc_repo = OtherDocRepository(self.dao)
        self.settlement_repo = SettlementRepository(self.dao)
        
        # Initialize payment service for creating payment_advice entries
        self.payment_service = PaymentProcessingService(
            payment_advice_repo=self.payment_advice_repo,
            invoice_repo=self.invoice_repo,
            other_doc_repo=self.other_doc_repo,
            settlement_repo=self.settlement_repo
        )
        
        # Initialize LLM extraction service
        if not os.environ.get("OPENAI_API_KEY"):
            logger.error("OPENAI_API_KEY environment variable not set. LLM extraction will fail.")
        self.llm_service = LLMExtractionService(self.dao, self.legal_entity_repo)
        logger.info("Using LLMExtractionService with OpenAI API (GPT-4.1)")
        
        # Initialize SAP export service
        self.sap_export_service = SAPExportService(dao=self.dao)
        
        # Initialize Account Enrichment Service
        self.account_enrichment_service = AccountEnrichmentService(dao=self.dao)
        
        # Initialize Monitoring Service
        self.monitoring_service = MonitoringService(dao=self.dao)
        
        # Initialize the payment processing service V2 for Zepto (no legacy repos)
        from src.services.payment_processing_service_v2 import PaymentProcessingServiceV2
        self.payment_service = PaymentProcessingServiceV2(
            payment_advice_repo=self.payment_advice_repo,
            dao=self.dao  # Pass DAO for direct payment advice line operations
        )
        logger.info("Initialized PaymentProcessingServiceV2 for Zepto-only processing")
        
        from src.services.legal_entity_lookup import LegalEntityLookupService
        self.legal_entity_lookup = LegalEntityLookupService(self.dao)
        
        # SAP integration removed per user request - not needed in BatchWorkerV2
        
        # Initialize batch manager
        self.batch_manager = BatchManager(
            dao=self.dao,
            is_test=self.is_test,
            mailbox_id=self.mailbox_id,
            run_mode=self.run_mode
        )
        
        # Initialize EmailProcessor with dependencies
        self.email_processor = EmailProcessor(self.dao, self.gcs_uploader, self.llm_service)
        
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
        
    async def create_payment_advice_from_llm_output(self, llm_output: Dict[str, Any], email_log_uuid: str) -> Optional[str]:
        """Create payment advice from LLM output using the payment service."""
        try:
            # Import constants for LLM output keys
            from src.external_apis.llm.constants import LLM_LEGAL_ENTITY_UUID_KEY, LLM_GROUP_UUIDS_KEY
            
            # Extract legal entity information from LLM output
            legal_entity_uuid = llm_output.get(LLM_LEGAL_ENTITY_UUID_KEY)
            group_uuids = llm_output.get(LLM_GROUP_UUIDS_KEY, [])
            
            # Enhanced logging for legal entity information
            logger.info(f"Legal entity information from LLM output:")
            logger.info(f"  - Legal entity UUID: {legal_entity_uuid or 'Not detected'}")
            logger.info(f"  - Group UUIDs: {group_uuids or ['None']}")
            
            # If legal entity is not detected, try to detect it with the lookup service
            if not legal_entity_uuid:
                raise ValueError("Legal entity UUID not detected from LLM output")
            
            # Use payment service to process LLM output with correct parameter order
            payment_advice_uuid = await self.payment_service.create_payment_advice(
                email_log_uuid=email_log_uuid,
                legal_entity_uuid=legal_entity_uuid,
                group_uuids=group_uuids,
                llm_output=llm_output
            )
            logger.info(f"Created payment advice {payment_advice_uuid} from LLM output for email {email_log_uuid}")
            logger.info(f"Payment advice created with legal_entity_uuid={legal_entity_uuid}, group_uuids={group_uuids}")
            return payment_advice_uuid
        except Exception as e:
            logger.error(f"Error creating payment advice from LLM output: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def process_email(self, email_data: Dict[str, Any]):
        """
        Process a single email and track the processing status at each step.
        
        Args:
            email_data: Dictionary with email details
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        # Generate a unique processing ID if we don't have one
        email_id = email_data.get('email_id') or str(uuid4())
        
        # Create initial email processing log entry with PENDING status
        processing_log = EmailProcessingLog(
            email_log_uuid=email_id,
            run_id=self.batch_manager.batch_run.run_id,
            processing_status=ProcessingStatus.EMAIL_RECEIVED,  # Initially set to EMAIL_RECEIVED
            error_msg=None
        )
        
        # Save initial processing log to Firestore
        try:
            await self.dao.add_document("email_processing_log", email_id, processing_log.__dict__)
            logger.info(f"Created email processing log for email {email_id}, batch run {self.batch_manager.batch_run.run_id}")
        except Exception as log_error:
            logger.error(f"Error creating initial processing log: {str(log_error)}")
            # Continue processing - we don't want to fail just because logging failed
        
        try:
            # Process email and create email log
            email_log_uuid, llm_output = await self.email_processor.process_email(email_data)
            
            # Update the processing log with actual email_log_uuid if different
            if email_log_uuid != email_id:
                logger.info(f"Email log UUID {email_log_uuid} differs from email ID {email_id}")
                await self.dao.update_document("email_processing_log", email_id, {
                    "email_log_uuid": email_log_uuid
                })
                # Also create a reference with the email_log_uuid for easier querying
                processing_log.email_log_uuid = email_log_uuid
                await self.dao.add_document("email_processing_log", email_log_uuid, processing_log.__dict__)
            
            # Update processing log with this batch run
            await self.dao.update_document("email_processing_log", email_log_uuid, {
                "run_id": self.batch_manager.batch_run.run_id,
                "processing_status": ProcessingStatus.PARSING_COMPLETED.value
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
            
            # Create a serializable copy of llm_output (without PaymentAdviceLine objects)
            def make_serializable(obj):
                """Convert an object to a JSON serializable format."""
                if isinstance(obj, dict):
                    return {k: make_serializable(v) for k, v in obj.items() if not k.startswith('paymentadvice_line_')}
                elif isinstance(obj, list):
                    return [make_serializable(item) for item in obj]
                elif hasattr(obj, '__dict__'):
                    # For objects like PaymentAdviceLine, convert to dict
                    return {"type": obj.__class__.__name__, "data": make_serializable(obj.__dict__)}
                else:
                    return obj
            
            # Create a serializable version of the output
            serializable_output = make_serializable(llm_output)
            
            try:
                print(f"\n\n=== LLM OUTPUT FOR EMAIL ===\n{json.dumps(serializable_output, indent=2)}\n===========================\n\n")
            except TypeError as e:
                logger.warning(f"Could not fully serialize LLM output: {e}")
                # Fall back to a simpler representation
                basic_output = {k: str(v) if not isinstance(v, (dict, list)) else v 
                               for k, v in serializable_output.items() 
                               if not k.startswith('paymentadvice_line_') and k != 'payment_advice_lines'}
                print(f"\n\n=== LLM OUTPUT FOR EMAIL (SIMPLIFIED) ===\n{json.dumps(basic_output, indent=2)}\n===========================\n\n")
            
            # Store the processed output for testing
            self.last_processed_output = llm_output
            
            # Store PDF text if available from email attachments
            if 'attachments' in email_data and email_data['attachments']:
                for attachment in email_data['attachments']:
                    if 'text_content' in attachment and attachment.get('content_type', '').lower().endswith('pdf'):
                        self.last_pdf_text = attachment['text_content']
                        logger.info(f"Stored PDF text for analysis: {len(self.last_pdf_text)} characters")
                        break
            
            # Create payment advice in Firestore first (similar to v1)
            payment_advice_uuid = await self.create_payment_advice_from_llm_output(llm_output, email_log_uuid)
            
            # Update the llm_output with the payment_advice_uuid
            if payment_advice_uuid:
                llm_output['payment_advice_uuid'] = payment_advice_uuid
                logger.info(f"Added payment_advice_uuid {payment_advice_uuid} to llm_output")
            else:
                # If payment advice creation failed, generate a UUID for consistency
                logger.error("Payment advice creation failed")
                raise ValueError("Payment advice creation failed")            
            # Payment advice lines are already saved by PaymentProcessingServiceV2 during create_payment_advice
            
            # Enrich payment advice lines with BP/GL codes first
            try:
                logger.info(f"Enriching payment advice lines with BP/GL codes for {payment_advice_uuid}")
                await self.account_enrichment_service.enrich_payment_advice_lines(payment_advice_uuid)
            except Exception as enrich_error:
                logger.error(f"Error enriching payment advice lines: {str(enrich_error)}")
                import traceback
                logger.error(f"Account Enrichment Traceback: {traceback.format_exc()}")
                # Continue processing - we don't want to fail the entire process for enrichment errors
            
            # Generate and upload SAP XLSX file for the payment advice
            try:
                logger.info(f"Generating SAP export for payment advice {payment_advice_uuid}")
                url = await self.sap_export_service.process_payment_advice_export(payment_advice_uuid)
                if url:
                    logger.info(f"Successfully generated and uploaded SAP export: {url}")
                    
                    # Update email processing log with SAP_PUSHED status
                    await self.dao.update_document("email_processing_log", email_log_uuid, {
                        "processing_status": ProcessingStatus.SAP_EXPORT_GENERATED.value,
                        "sap_doc_num": payment_advice_uuid  # Using payment advice UUID as SAP doc number
                    })
                    logger.info(f"Updated email processing log {email_log_uuid} with SAP_PUSHED status")
                else:
                    logger.warning(f"Failed to generate or upload SAP export for payment advice {payment_advice_uuid}")
            except Exception as sap_error:
                logger.error(f"Error generating SAP export: {str(sap_error)}")
                import traceback
                logger.error(f"SAP Export Traceback: {traceback.format_exc()}")
                # Continue processing - we don't want to fail the entire process for SAP export errors
            
            # If no payment advice lines found in LLM output
            if 'paymentadvice_lines' not in llm_output or not llm_output['paymentadvice_lines']:
                logger.warning("No payment advice lines found in LLM output")
            
            # Update batch run stats
            self.batch_manager.increment_processed_count()
            self.emails_processed += 1
            return True
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing email: {error_msg}")
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Traceback: {error_trace}")
            
            # Update email processing log with ERROR status
            try:
                # Try both email_id and email_log_uuid as the document ID
                email_log_id = email_log_uuid if 'email_log_uuid' in locals() else email_id
                await self.dao.update_document("email_processing_log", email_log_id, {
                    "processing_status": ProcessingStatus.PROCESSING_FAILED.value,
                    "error_msg": f"{error_msg}\n{error_trace[:500]}"  # Limit error trace length
                })
                logger.info(f"Updated email processing log {email_log_id} with ERROR status")
            except Exception as log_error:
                logger.error(f"Failed to update processing log with error status: {str(log_error)}")
                
            # Update batch level counters
            self.batch_manager.increment_error_count()
            self.errors += 1
            return False

    async def process_single_email(self, email_id: str) -> bool:
        """
        Process a single email by ID (used for PubSub triggered serverless processing).
        
        Args:
            email_id: Gmail message ID or email log UUID to process
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        logger.info(f"========== STARTING SINGLE EMAIL PROCESSING FOR ID: {email_id} ==========")
        try:
            # Start batch run (even for single email we track it as a batch run)
            await self.start_batch_run()
            
            # Get email from the email reader using the ID
            logger.info(f"Getting email data for ID: {email_id}")
            email_data = self.email_reader.get_email_by_id(email_id)
            if not email_data:
                logger.error(f"Could not find email with ID: {email_id}")
                await self.finish_batch_run()
                return False
                
            logger.info(f"Found email with ID: {email_id}, from: {email_data.get('from')}, to: {email_data.get('to')}")
            
            # Process the email
            success = await self.process_email(email_data)
            
            # Update monitoring sheet after successful processing
            if success:
                try:
                    email_log_uuid = email_data.get('email_log_uuid') or email_data.get('id') or email_data.get('email_id')
                    if email_log_uuid:
                        logger.info(f"Updating monitoring sheet with email log UUID: {email_log_uuid}")
                        await self.monitoring_service.update_after_batch_processing(email_log_uuid)
                except Exception as monitor_error:
                    logger.error(f"Error updating monitoring sheet: {str(monitor_error)}")
            
            # Gmail watch check has been moved to the cloud function for better architectural separation
            # and to ensure it runs once per cloud function invocation rather than per email processing
                
            # Finish the batch run
            await self.finish_batch_run()
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing single email {email_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            # Make sure to finish the batch run with error status
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
            
            return False
    
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
                    # Update monitoring sheet after successful processing
                    try:
                        # Log the full email data structure for debugging
                        logger.info(f"Email data structure: {json.dumps(email_data, default=str)}")
                        
                        # Get the email_log_uuid from wherever it's available
                        email_log_uuid = None
                        
                        # Try multiple potential field names
                        if 'gcs_folder_uri' in email_data:
                            email_log_uuid = email_data.get('gcs_folder_uri')
                            logger.info(f"Using gcs_folder_uri as email_log_uuid: {email_log_uuid}")
                        elif 'email_log_uuid' in email_data:
                            email_log_uuid = email_data.get('email_log_uuid')
                            logger.info(f"Using email_log_uuid directly: {email_log_uuid}")
                        elif 'id' in email_data:
                            email_log_uuid = email_data.get('id')
                            logger.info(f"Using id as email_log_uuid: {email_log_uuid}")
                        elif 'email_id' in email_data:
                            # For Gmail adapter, use the email_id
                            email_log_uuid = email_data.get('email_id')
                            logger.info(f"Using email_id as email_log_uuid: {email_log_uuid}")
                        
                        if email_log_uuid:
                            logger.info(f"Updating monitoring sheet with email log UUID: {email_log_uuid}")
                            await self.monitoring_service.update_after_batch_processing(email_log_uuid)
                        else:
                            # If all attempts failed, try to get the latest email log from Firestore
                            logger.warning("Could not find email_log_uuid in email data, trying to get from Firestore")
                            recent_logs = await self.dao.get_email_logs(limit=1)
                            if recent_logs and len(recent_logs) > 0:
                                email_log_uuid = recent_logs[0].get('document_id') or recent_logs[0].get('id') or recent_logs[0].get('email_log_uuid')
                                if email_log_uuid:
                                    logger.info(f"Using recent email log UUID from Firestore: {email_log_uuid}")
                                    await self.monitoring_service.update_after_batch_processing(email_log_uuid)
                                else:
                                    logger.error("Cannot update monitoring - email log UUID not found in any source")
                            else:
                                logger.error("Cannot update monitoring - email_log_uuid not found in email data or Firestore")
                    except Exception as monitor_error:
                        logger.error(f"Error updating monitoring sheet: {str(monitor_error)}", exc_info=True)
            
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

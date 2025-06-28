"""Email processing functionality for the batch worker."""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

# Import models
from models.schemas import EmailLog, EmailProcessingLog, ProcessingStatus

logger = logging.getLogger(__name__)


class EmailProcessor:
    """
    Handles email processing operations for the batch worker.
    """
    
    def __init__(self, dao, gcs_uploader, llm_extractor, sap_integrator=None):
        """
        Initialize the email processor.
        
        Args:
            dao: Firestore DAO instance
            gcs_uploader: GCS uploader instance
            llm_extractor: LLM extractor instance
            sap_integrator: Optional SAP integrator instance for enriching documents
        """
        self.dao = dao
        self.gcs_uploader = gcs_uploader
        self.llm_extractor = llm_extractor
        self.sap_integrator = sap_integrator
    
    async def process_email(self, email_data: Dict[str, Any], batch_run_id: str, 
                           payment_processor) -> bool:
        """
        Process a single email.
        
        Args:
            email_data: Email data from the reader
            batch_run_id: Current batch run ID
            payment_processor: Payment processor instance for payment advice processing
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Extract email metadata
            email_id = email_data.get("email_id", str(uuid.uuid4()))
            sender = email_data.get("sender")
            subject = email_data.get("subject", "(No Subject)")
            received_at = email_data.get("received_at", datetime.utcnow())
            
            logger.info(f"Processing email {email_id} from {sender}: {subject}")
            
            # Upload email content to GCS
            email_uuid = email_id
            raw_data = email_data.get("raw_data", b'')
            text_content = email_data.get("text_content")
            html_content = email_data.get("html_content")
            attachments = email_data.get("attachments")
            
            upload_result = self.gcs_uploader.upload_email_complete(
                email_uuid=email_uuid,
                raw_data=raw_data,
                text_content=text_content,
                html_content=html_content,
                attachments=attachments
            )
            
            if not upload_result:
                raise Exception(f"Failed to upload email to GCS")
            
            # Create EmailLog record
            email_log = EmailLog(
                email_log_uuid=email_id,
                sender_mail=sender,
                original_sender_mail=email_data.get("original_sender"),
                email_subject=subject,
                mailbox_id=email_data.get("mailbox_id"),
                received_at=received_at,
                gcs_folder_uri=upload_result.get("raw_path").split('/')[0],  # Extract folder path from raw_path
                group_uuids=[],  # Will be populated during payment advice processing
            )
            
            # Add EmailLog to Firestore
            await self.dao.add_document("email_log", email_log.email_log_uuid, email_log.__dict__)
            
            logger.info(f"Created email_log {email_log.email_log_uuid} in Firestore")
            
            # Create a processing log
            processing_log = EmailProcessingLog(
                email_log_uuid=email_log.email_log_uuid,
                run_id=batch_run_id,
                processing_status=ProcessingStatus.PARSED
            )
            
            doc_id = f"{email_log.email_log_uuid}_{batch_run_id}"
            await self.dao.add_document("email_processing_log", doc_id, processing_log.__dict__)
            
            # Get text content for LLM processing
            email_text_content = email_data.get("text_content", "")
            if not email_text_content:
                logger.warning(f"Email {email_id} has no text content. May affect extraction.")
            
            # Process attachments
            attachments = email_data.get("attachments", [])
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
                    payment_advice_uuid = await payment_processor.create_payment_advice_from_llm_output(llm_output, email_log.email_log_uuid)
                    
                    # If payment advice was created and SAP integrator is available, enrich with SAP data
                    if payment_advice_uuid and self.sap_integrator:
                        try:
                            logger.info(f"Enriching payment advice {payment_advice_uuid} with SAP data")
                            success_count, fail_count = await self.sap_integrator.enrich_documents_with_sap_data(payment_advice_uuid)
                            logger.info(f"SAP enrichment complete: {success_count} successful updates, {fail_count} failed updates")
                        except Exception as sap_error:
                            logger.error(f"Error during SAP enrichment: {str(sap_error)}")
                    
                    processed_attachments += 1
                    
                except Exception as e:
                    logger.error(f"Error processing attachment {attachment_idx} with LLM: {str(e)}")
            
            logger.info(f"Successfully processed {processed_attachments}/{len(attachments)} attachments with LLM")
            
            # Update processing log status
            processing_log.processing_status = ProcessingStatus.SAP_PUSHED
            await self.dao.update_document("email_processing_log", doc_id, 
                                         {"processing_status": ProcessingStatus.SAP_PUSHED})
            
            logger.info(f"Successfully processed email {email_log.email_log_uuid}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing email: {str(e)}")
            
            # Create error log
            try:
                # Use the same email_log_uuid we generated earlier, or get it from data
                error_email_uuid = email_data.get("email_id", str(uuid.uuid4()))
                processing_log = EmailProcessingLog(
                    email_log_uuid=error_email_uuid,
                    run_id=batch_run_id,
                    processing_status=ProcessingStatus.ERROR,
                    error_msg=str(e)
                )
                
                # Create processing log for error
                doc_id = f"{error_email_uuid}_{batch_run_id}"
                await self.dao.add_document("email_processing_log", doc_id, processing_log.__dict__)
            except Exception as log_error:
                logger.error(f"Failed to create error log: {str(log_error)}")
                
            return False

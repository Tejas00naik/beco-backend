"""Email processing functionality for the batch worker."""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

# Import models
from src.models.schemas import EmailLog, EmailProcessingLog, ProcessingStatus

# Import legal entity lookup service
from src.services.legal_entity_lookup import LegalEntityLookupService

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
        
        # Initialize the legal entity lookup service
        self.legal_entity_lookup = LegalEntityLookupService(dao)
    
    async def process_email(self, email_data: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """
        Process a single email.
        
        Args:
            email_data: Email data from the reader
            
        Returns:
            Tuple of (email_log_uuid, llm_output) if processing was successful
            The caller can then use these to create payment advice records
        """
        try:
            # Extract email metadata
            email_id = email_data.get("email_id", str(uuid.uuid4()))
            sender = email_data.get("sender_mail")  # Fix: Use sender_mail instead of sender
            original_sender = email_data.get("original_sender_mail")  # Fix: Get original_sender_mail
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
                original_sender_mail=original_sender,  # Fix: Use the extracted original_sender
                email_subject=subject,
                mailbox_id=email_data.get("mailbox_id"),
                received_at=received_at,
                gcs_folder_uri=upload_result.get("raw_path").split('/')[0],  # Extract folder path from raw_path
                group_uuids=[],  # Will be populated during payment advice processing
            )
            
            # Add EmailLog to Firestore
            await self.dao.add_document("email_log", email_log.email_log_uuid, email_log.__dict__)
            
            # Detailed logging of created EmailLog fields
            logger.info(f"Created email_log with the following details:")
            logger.info(f"  email_log_uuid: {email_log.email_log_uuid}")
            logger.info(f"  sender_mail: {email_log.sender_mail}")
            logger.info(f"  original_sender_mail: {email_log.original_sender_mail}")
            logger.info(f"  email_subject: {email_log.email_subject}")
            logger.info(f"  mailbox_id: {email_log.mailbox_id}")
            logger.info(f"  received_at: {email_log.received_at}")
            logger.info(f"  gcs_folder_uri: {email_log.gcs_folder_uri}")
            logger.info(f"  group_uuids: {email_log.group_uuids} (will be populated later)")
            
            # Create processing log
            processing_log = EmailProcessingLog(
                email_log_uuid=email_log.email_log_uuid,
                run_id="",  # Will be set by the BatchWorker later
                processing_status=ProcessingStatus.PARSED
            )
            
            # Add processing log to Firestore
            doc_id = f"{email_log.email_log_uuid}"
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
                    
                    # Log detailed attachment info before LLM processing
                    logger.info(f"Processing attachment {attachment_idx+1}/{len(attachments)}:")
                    logger.info(f"  Filename: {attachment_filename}")
                    content_type = attachment.get('content_type', 'unknown')
                    logger.info(f"  Content type: {content_type}")
                    logger.info(f"  Size: {len(attachment.get('content', b''))} bytes")
                    
                    # Extract text content from PDF if needed
                    if 'pdf' in content_type.lower() and 'text_content' not in attachment:
                        try:
                            import os
                            import tempfile
                            import PyPDF2
                            
                            logger.info(f"Extracting text from PDF attachment: {attachment_filename}")
                            
                            # Create a temporary file to save the PDF
                            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                                temp_file.write(attachment.get('content', b''))
                                temp_pdf_path = temp_file.name
                            
                            # Extract text from PDF
                            pdf_text = ""
                            try:
                                with open(temp_pdf_path, "rb") as pdf_file:
                                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                                    for page_num in range(len(pdf_reader.pages)):
                                        page = pdf_reader.pages[page_num]
                                        pdf_text += page.extract_text() + "\n\n"
                                
                                logger.info(f"Extracted {len(pdf_text)} characters from PDF attachment")
                                # Add extracted text to attachment data
                                attachment['text_content'] = pdf_text
                            except Exception as pdf_err:
                                logger.error(f"Error extracting text from PDF: {str(pdf_err)}")
                            finally:
                                # Clean up the temporary file
                                try:
                                    os.unlink(temp_pdf_path)
                                except:
                                    pass
                        except ImportError:
                            logger.warning("PyPDF2 not installed. Installing now...")
                            import subprocess
                            import sys
                            subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2"])
                            logger.warning("Failed to extract text from PDF attachment due to missing dependencies")
                    
                    # Extract text content from attachment if needed (e.g., PDF)
                    attachment_text = attachment.get('text_content', '') or ''
                    
                    # STEP 1: Legal entity detection using the service layer
                    logger.info(f"STEP 1: Starting legal entity detection for attachment {attachment_filename}")
                    detection_result = await self.legal_entity_lookup.detect_legal_entity(
                        email_body=email_text_content,
                        document_text=attachment_text
                    )
                    
                    legal_entity_uuid = detection_result.get('legal_entity_uuid')
                    group_uuid = detection_result.get('group_uuid')
                    
                    logger.info(f"Legal entity detection result: legal_entity_uuid={legal_entity_uuid}, group_uuid={group_uuid}")
                    
                    # Update the email_log with group_uuid immediately if found
                    if group_uuid:
                        # Add the group_uuid to the email_log.group_uuids array if not already present
                        if not email_log.group_uuids:
                            email_log.group_uuids = []
                            
                        if group_uuid not in email_log.group_uuids:
                            email_log.group_uuids.append(group_uuid)
                            logger.info(f"Added group_uuid {group_uuid} to email_log {email_log.email_log_uuid}")
                            
                            # Update the email_log in Firestore
                            await self.dao.update_document("email_log", email_log.email_log_uuid, {
                                "group_uuids": email_log.group_uuids
                            })
                            logger.info(f"Updated email_log {email_log.email_log_uuid} with group_uuids: {email_log.group_uuids}")
                    
                    # STEP 2: Process the attachment with the full extraction prompt
                    logger.info(f"STEP 2: Starting full payment advice extraction for attachment {attachment_filename}")
                    llm_output = await self.llm_extractor.process_attachment_for_payment_advice(
                        email_text_content, attachment, group_uuid=group_uuid
                    )
                    
                    # Add the legal entity and group from step 1 to the llm_output
                    if legal_entity_uuid:
                        llm_output["legal_entity_uuid"] = legal_entity_uuid
                    if group_uuid:
                        llm_output["group_uuid"] = group_uuid
                    
                    # Print detailed summary of extracted data
                    logger.info(f"LLM extracted data for attachment {attachment_filename}:")
                    # Meta Table detailed logging
                    meta_table = llm_output.get('metaTable', {})
                    logger.info(f"  Meta Table:")
                    logger.info(f"    paymentAdviceNumber: {meta_table.get('paymentAdviceNumber')}")
                    logger.info(f"    paymentAdviceDate: {meta_table.get('paymentAdviceDate')}")
                    logger.info(f"    paymentAdviceAmount: {meta_table.get('paymentAdviceAmount')}")
                    logger.info(f"    payersLegalName: {meta_table.get('payersLegalName')}")
                    logger.info(f"    payeesLegalName: {meta_table.get('payeesLegalName')}")
                    
                    # Invoice Table summary
                    invoice_table = llm_output.get('invoiceTable', [])
                    logger.info(f"  Invoice Table: {len(invoice_table)} items")
                    for i, invoice in enumerate(invoice_table[:3]):  # Log first 3 invoices
                        logger.info(f"    Invoice {i+1}: {invoice.get('invoiceNumber')} - Amount: {invoice.get('totalSettlementAmount')}")
                    if len(invoice_table) > 3:
                        logger.info(f"    ... and {len(invoice_table) - 3} more invoices")
                    
                    # Other Doc Table summary
                    other_doc_table = llm_output.get('otherDocTable', [])
                    logger.info(f"  Other Doc Table: {len(other_doc_table)} items")
                    for i, doc in enumerate(other_doc_table[:3]):  # Log first 3 other docs
                        logger.info(f"    Other Doc {i+1}: {doc.get('otherDocNumber')} ({doc.get('otherDocType')}) - Amount: {doc.get('otherDocAmount')}")
                    if len(other_doc_table) > 3:
                        logger.info(f"    ... and {len(other_doc_table) - 3} more other docs")
                    
                    # Settlement Table summary
                    settlement_table = llm_output.get('settlementTable', [])
                    logger.info(f"  Settlement Table: {len(settlement_table)} items")
                    for i, settlement in enumerate(settlement_table[:3]):  # Log first 3 settlements
                        logger.info(f"    Settlement {i+1}: {settlement.get('invoiceNumber')} -> {settlement.get('settlementDocNumber')} - Amount: {settlement.get('settlementAmount')}")
                    if len(settlement_table) > 3:
                        logger.info(f"    ... and {len(settlement_table) - 3} more settlements")
                    
                    # Return LLM output for further processing by the calling service
                    # The calling service will handle payment advice creation and SAP enrichment
                    
                    processed_attachments += 1
                    
                except Exception as e:
                    logger.error(f"Error processing attachment {attachment_idx} with LLM: {str(e)}")
            
            logger.info(f"Successfully processed {processed_attachments}/{len(attachments)} attachments with LLM")
            
            # Update processing log status
            processing_log.processing_status = ProcessingStatus.PARSED
            await self.dao.update_document("email_processing_log", doc_id, 
                                         {"processing_status": ProcessingStatus.PARSED})
            
            logger.info(f"Successfully processed email {email_log.email_log_uuid}")
            return email_log.email_log_uuid, llm_output
            
        except Exception as e:
            logger.error(f"Error processing email: {str(e)}")
            
            # Create error log
            try:
                # Use the same email_log_uuid we generated earlier, or get it from data
                error_email_uuid = email_data.get("email_id", str(uuid.uuid4()))
                processing_log = EmailProcessingLog(
                    email_log_uuid=error_email_uuid,
                    run_id="",  # Will be set by BatchWorker
                    processing_status=ProcessingStatus.ERROR,
                    error_msg=str(e)
                )
                
                # Create processing log for error
                doc_id = f"{error_email_uuid}"
                await self.dao.add_document("email_processing_log", doc_id, processing_log.__dict__)
            except Exception as log_error:
                logger.error(f"Failed to create error log: {str(log_error)}")
                
            # Re-raise the exception to be handled by the caller
            raise

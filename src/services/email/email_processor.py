"""Email processing service for payment advice extraction."""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
import os
import tempfile
import fitz  # PyMuPDF
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
        
    def _log_llm_output_summary(self, llm_output, source_name):
        """
        Log detailed summary of extracted LLM data.
        
        Args:
            llm_output: The LLM extraction output dictionary
            source_name: Name of the source ("email body" or attachment filename)
        """
        logger.info(f"LLM extracted data for {source_name}:")
        
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
    
    async def _preprocess_attachment(self, attachment, attachment_idx=0, total_attachments=1):
        """
        Preprocess an attachment to extract its text content based on file type.
        This function can be extended to support more file types in the future.
        
        Args:
            attachment: The attachment dictionary
            attachment_idx: Index of the attachment for logging purposes
            total_attachments: Total number of attachments for logging purposes
            
        Returns:
            The attachment with text_content field added if applicable
        """
        attachment_filename = attachment.get('filename', f'attachment-{attachment_idx}')
        logger.info(f"Processing attachment {attachment_idx+1}/{total_attachments}: {attachment_filename}")
        
        # Log detailed attachment info before processing
        logger.info(f"  Filename: {attachment_filename}")
        content_type = attachment.get('content_type', 'unknown')
        logger.info(f"  Content type: {content_type}")
        logger.info(f"  Size: {len(attachment.get('content', b''))} bytes")
        
        # If text content is already extracted, return as is
        if 'text_content' in attachment:
            logger.info(f"Attachment already has text content of {len(attachment['text_content'])} characters")
            return attachment
            
        # Extract text content from PDF if needed
        if 'pdf' in content_type.lower():
            try:
                logger.info(f"Extracting text from PDF attachment using PyMuPDF: {attachment_filename}")
                
                # Create a temporary file to save the PDF
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                    temp_file.write(attachment.get('content', b''))
                    temp_pdf_path = temp_file.name
                
                # Extract text from PDF using PyMuPDF (fitz)
                pdf_text = ""
                raw_text = ""
                try:
                    # Open the PDF with PyMuPDF
                    pdf_document = fitz.open(temp_pdf_path)
                    
                    # Process each page
                    for page_num in range(len(pdf_document)):
                        page = pdf_document.load_page(page_num)
                        
                        # Extract text with better layout preservation
                        page_text = page.get_text("text")
                        pdf_text += page_text + "\n\n"
                        
                        # Also store raw text version for debugging
                        raw_text += page.get_text("rawdict") + "\n\n"
                    
                    # Close the document
                    pdf_document.close()
                    
                    logger.info(f"Extracted {len(pdf_text)} characters from PDF attachment")
                    # Add extracted text to attachment data
                    attachment['text_content'] = pdf_text
                    attachment['raw_text_content'] = raw_text
                    attachment['extraction_method'] = 'PyMuPDF'
                    
                    # Log sample of extracted text for debugging
                    text_preview = pdf_text[:500] + '...' if len(pdf_text) > 500 else pdf_text
                    logger.info(f"PDF Text Preview:\n{text_preview}")
                    
                except Exception as pdf_err:
                    logger.error(f"Error extracting text from PDF using PyMuPDF: {str(pdf_err)}")
                    # Fallback to simple text extraction if PyMuPDF fails
                    try:
                        import PyPDF2
                        logger.info("Falling back to PyPDF2 for text extraction")
                        with open(temp_pdf_path, "rb") as pdf_file:
                            pdf_reader = PyPDF2.PdfReader(pdf_file)
                            fallback_text = ""
                            for page_num in range(len(pdf_reader.pages)):
                                page = pdf_reader.pages[page_num]
                                fallback_text += page.extract_text() + "\n\n"
                        
                        logger.info(f"Extracted {len(fallback_text)} characters using PyPDF2 fallback")
                        # Add extracted text to attachment data
                        attachment['text_content'] = fallback_text
                        attachment['extraction_method'] = 'PyPDF2_fallback'
                    except Exception as fallback_err:
                        logger.error(f"Fallback extraction also failed: {str(fallback_err)}")
                finally:
                    # Clean up the temporary file
                    try:
                        os.unlink(temp_pdf_path)
                    except:
                        pass
            except ImportError:
                logger.warning("PyMuPDF not installed. PDF text extraction will be skipped.")
                logger.warning("Failed to extract text from PDF attachment due to missing dependencies")
        
        # Future extensions can be added here for other file types
        # elif 'xlsx' in content_type.lower() or 'excel' in content_type.lower():
        #     # Process Excel files
        #     pass
        # elif 'docx' in content_type.lower() or 'word' in content_type.lower():
        #     # Process Word files
        #     pass
        
        # Ensure text_content exists, even if empty
        if 'text_content' not in attachment:
            attachment['text_content'] = ""
            
        return attachment, attachment_filename
        
    async def _process_content_for_payment_advice(self, email_text_content, content_source, content_text, source_name, email_log):
        """
        Process either an email body or attachment for payment advice extraction.
        
        Args:
            email_text_content: The email body text content
            content_source: The content source dict (attachment or pseudo-attachment)
            content_text: The text to analyze for legal entity detection
            source_name: Name of the source for logging ("email body" or attachment filename)
            email_log: The email log object to update
            
        Returns:
            Dict containing the LLM output with legal entity info
        """
        # STEP 1: Legal entity detection using the service layer
        logger.info(f"STEP 1: Starting legal entity detection for {source_name}")
        detection_result = await self.legal_entity_lookup.detect_legal_entity(
            email_body=email_text_content,
            document_text=content_text
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
        
        # STEP 2: Process the content with the full extraction prompt
        logger.info(f"STEP 2: Starting full payment advice extraction for {source_name}")
        llm_output = await self.llm_extractor.process_attachment_for_payment_advice(
            email_text_content, content_source, group_uuid=group_uuid
        )
        
        # Add the legal entity and group from step 1 to the llm_output
        if legal_entity_uuid:
            llm_output["legal_entity_uuid"] = legal_entity_uuid
        if group_uuid:
            llm_output["group_uuid"] = group_uuid
            
        logger.info(f"Successfully processed {source_name} with LLM")
        return llm_output
    
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
            
            # Initialize llm_output to empty dict in case there are no attachments
            llm_output = {}
            
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
                processing_status=ProcessingStatus.EMAIL_RECEIVED
            )
            
            # Add processing log to Firestore
            doc_id = f"{email_log.email_log_uuid}"
            await self.dao.add_document("email_processing_log", doc_id, processing_log.__dict__)
            
            # Get text content for LLM processing
            email_text_content = email_data.get("text_content", "")
            if not email_text_content:
                logger.warning(f"Email {email_id} has no text content. May affect extraction.")
            
            # Process attachments or email body directly
            attachments = email_data.get("attachments", [])
            processed_attachments = 0
            
            # If no attachments, process the email body directly
            if not attachments and email_text_content:
                logger.info(f"No attachments found. Processing email body directly.")
                
                # Create a pseudo-attachment with the email body content for processing
                # Note: We'll send the text content directly to avoid PDF extraction errors
                body_as_attachment = {
                    'filename': 'email_body.txt',
                    'content_type': 'text/plain',
                    'text_content': email_text_content,
                    'content': email_text_content.encode('utf-8'),  # Add content field as bytes
                    'is_plain_text': True  # Flag to indicate this is plain text and not a PDF
                }
                
                # Process the email body using the common helper function
                llm_output = await self._process_content_for_payment_advice(
                    email_text_content=email_text_content,
                    content_source=body_as_attachment,
                    content_text=email_text_content,
                    source_name="email body",
                    email_log=email_log
                )
                
                # Log summary of extracted LLM data
                self._log_llm_output_summary(llm_output, "email body")
                
                logger.info("Successfully processed email body directly with LLM")
                processed_attachments = 1  # Mark as processed one item
            
            # Process each attachment if there are any
            for attachment_idx, attachment in enumerate(attachments):
                try:
                    # Preprocess the attachment to extract text content
                    attachment, attachment_filename = await self._preprocess_attachment(attachment, attachment_idx, len(attachments))
                    
                    # Get the extracted text content
                    attachment_text = attachment.get('text_content', '') or ''
                    
                    # Process the attachment using the common helper function
                    llm_output = await self._process_content_for_payment_advice(
                        email_text_content=email_text_content,
                        content_source=attachment,
                        content_text=attachment_text,
                        source_name=f"attachment {attachment_filename}",
                        email_log=email_log
                    )
                    
                    # Log summary of extracted LLM data
                    self._log_llm_output_summary(llm_output, f"attachment {attachment_filename}")
                    
                    # Return LLM output for further processing by the calling service
                    # The calling service will handle payment advice creation and SAP enrichment
                    
                    processed_attachments += 1
                    
                except Exception as e:
                    logger.error(f"Error processing attachment {attachment_idx} with LLM: {str(e)}")
            
            logger.info(f"Successfully processed {processed_attachments}/{len(attachments)} attachments with LLM")
            
            # Update processing log status
            processing_log.processing_status = ProcessingStatus.LLM_READ
            await self.dao.update_document("email_processing_log", doc_id, 
                                         {"processing_status": ProcessingStatus.LLM_READ})
            
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
                    processing_status=ProcessingStatus.PROCESSING_FAILED,
                    error_msg=str(e)
                )
                
                # Create processing log for error
                doc_id = f"{error_email_uuid}"
                await self.dao.add_document("email_processing_log", doc_id, processing_log.__dict__)
            except Exception as log_error:
                logger.error(f"Failed to create error log: {str(log_error)}")
                
            # Re-raise the exception to be handled by the caller
            raise

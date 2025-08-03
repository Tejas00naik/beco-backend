"""Service layer for LLM extraction operations."""

import logging
import json
from typing import Dict, Any, List, Optional, Union, BinaryIO
from pathlib import Path
import os

from src.repositories import FirestoreDAO
from src.repositories import LegalEntityRepository
from src.external_apis.llm.extractor import LLMExtractor
from src.external_apis.llm.config import PROMPT_MAP

logger = logging.getLogger(__name__)

class LLMExtractionService:
    """Service for LLM extraction operations."""
    
    def __init__(
        self, 
        firestore_dao: FirestoreDAO,
        legal_entity_repo: LegalEntityRepository
    ):
        """Initialize with dependencies."""
        self.dao = firestore_dao
        self.legal_entity_repo = legal_entity_repo
        self.llm_extractor = LLMExtractor(self.dao)
        
    async def extract_from_attachment(
        self,
        attachment_file: Union[str, Path, BinaryIO],
        email_body: str,
        legal_entity_uuid: Optional[str] = None,
        group_uuids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Extract structured data from an attachment using the LLM.
        
        Args:
            attachment_file: File path or file object for the attachment
            email_body: Email body text for context
            legal_entity_uuid: Optional UUID of the legal entity
            group_uuids: Optional list of group UUIDs
            
        Returns:
            Extracted structured data
        """
        try:
            # If we have legal entity and group information, use it to select the appropriate prompt
            prompt = None
            if group_uuids and len(group_uuids) > 0:
                # Use the first group for prompt selection
                group_id = group_uuids[0]
                if group_id in PROMPT_MAP:
                    prompt = PROMPT_MAP[group_id]
                    logger.info(f"Using group-specific prompt for group {group_id}")
            
            # Extract data using LLM
            # Using the correct method in LLMExtractor
            extracted_data = await self.llm_extractor.process_document(
                pdf_path=attachment_file if isinstance(attachment_file, (str, Path)) else None,
                document_text=None,  # We're providing the PDF path directly
                email_body=email_body,
                group_uuid=group_uuids[0] if group_uuids and len(group_uuids) > 0 else None
            )
            
            # Log meta fields from LLM output
            meta_table = extracted_data.get('metaTable', {})
            logger.debug(f"LLM META FIELDS: {json.dumps(meta_table, default=str)}")
            
            # Check specific meta fields
            logger.debug(f"META paymentAdviceNumber: {meta_table.get('paymentAdviceNumber')}")
            logger.debug(f"META paymentAdviceDate: {meta_table.get('paymentAdviceDate')}")
            logger.debug(f"META paymentAdviceAmount: {meta_table.get('paymentAdviceAmount')}")
            logger.debug(f"META payersLegalName: {meta_table.get('payersLegalName')}")
            logger.debug(f"META payeesLegalName: {meta_table.get('payeesLegalName')}")
            
            # Post-process the extraction based on group requirements if needed
            if group_uuids and len(group_uuids) > 0:
                extracted_data = await self._post_process_by_group(extracted_data, group_uuids[0])
                
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extracting data from attachment: {str(e)}")
            raise
            
    async def process_attachment_for_payment_advice(self, email_text_content: str, attachment_obj: Dict[str, Any], attachment_text: str, group_uuid: str = None) -> Dict[str, Any]:
        """
        Process an attachment for payment advice extraction.
        
        Args:
            email_text_content: Text content of the email for context
            attachment_obj: Dictionary with attachment data including content
            attachment_text: Text content of the attachment for context
            group_uuid: Optional UUID of the group for prompt selection and post-processing
            
        Returns:
            Extracted structured data
        """
        try:
            # Extract attachment content and file information
            attachment_content = attachment_obj.get("content")
            filename = attachment_obj.get("filename")
            content_type = attachment_obj.get("content_type")
            is_plain_text = attachment_obj.get("is_plain_text", False)
            
            if not attachment_content:
                raise ValueError("Attachment content is missing")
            
            # Convert group_uuid to list for extract_from_attachment
            group_uuids = [group_uuid] if group_uuid else []
            
            # For plain text (like email body), skip file creation and directly process the text content
            if is_plain_text:
                logger.info(f"Processing plain text content directly, skipping file creation")
                # Decode bytes to string if needed
                text_content = attachment_obj.get("text_content", "")
                if not text_content and isinstance(attachment_content, bytes):
                    try:
                        text_content = attachment_content.decode("utf-8")
                    except UnicodeDecodeError:
                        logger.warning("Failed to decode attachment content as UTF-8")
                        text_content = ""
                
                # Process the plain text content directly
                extracted_data = await self.llm_extractor.process_document(
                    document_text=attachment_text,
                    email_body=email_text_content,
                    group_uuid=group_uuid if group_uuid else None
                )
                
                return extracted_data
            
            # For non-plain-text attachments, create a temporary file for processing
            import tempfile
            import os
            
            # Create a temporary file with the correct extension based on content type
            extension = ".pdf"  # Default to PDF
            if content_type:
                if "pdf" in content_type.lower():
                    extension = ".pdf"
                elif "excel" in content_type.lower() or "spreadsheet" in content_type.lower():
                    extension = ".xlsx"
                elif "word" in content_type.lower() or "document" in content_type.lower():
                    extension = ".docx"
            
            with tempfile.NamedTemporaryFile(suffix=extension, delete=False) as temp_file:
                temp_file.write(attachment_content)
                temp_file_path = temp_file.name
            
            logger.info(f"Created temporary file for attachment: {temp_file_path}")
            
            try:
                # Process file-based attachment
                
                # Use our existing extract_from_attachment method
                result = await self.extract_from_attachment(
                    attachment_file=temp_file_path,
                    email_body=email_text_content,
                    group_uuids=group_uuids
                )
                
                return result
            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Removed temporary file: {temp_file_path}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file {temp_file_path}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing attachment for payment advice: {str(e)}")
            raise
    
    async def _post_process_by_group(self, extracted_data: Dict[str, Any], group_uuid: str) -> Dict[str, Any]:
        """
        Perform group-specific post-processing on extracted data.
        
        Args:
            extracted_data: Data extracted by the LLM
            group_uuid: UUID of the group
            
        Returns:
            Post-processed data
        """
        try:
            if not group_uuid:
                logger.warning("No group UUID provided, skipping post-processing")
                return extracted_data
                
            # Instead of querying for group information, use the group_uuid directly
            # This follows proper separation of concerns and avoids unnecessary queries
            
            # Check if this is an Amazon group based on the UUID
            is_amazon_group = "amazon" in group_uuid.lower()
            
            # Amazon-specific post-processing
            if is_amazon_group:
                return await self._process_amazon_extraction(extracted_data)
                
            # Add more group-specific processing here
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error in post-processing for group {group_uuid}: {str(e)}")
            # Return original data on error
            return extracted_data
            
    async def _process_amazon_extraction(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Amazon-specific post-processing for LLM extraction.
        
        For Amazon group: ensure all invoices referenced in settlements are present in invoice table.
        If any invoice in settlements is missing from invoice table, add it with:
        - Amount = sum of all settlement amounts for that invoice
        - No date/booking_amount
        
        Args:
            extracted_data: Data extracted by the LLM
            
        Returns:
            Post-processed data
        """
        try:
            # Create copy to avoid modifying original
            processed_data = extracted_data.copy()
            
            # Skip if no settlement data
            if not processed_data.get("settlementTable"):
                return processed_data
                
            # Get all invoice numbers from invoice table
            invoice_numbers = set()
            if processed_data.get("invoiceTable"):
                invoice_numbers = {inv.get("invoice_number") for inv in processed_data["invoiceTable"] if inv.get("invoice_number")}
                
            # Find invoice numbers in settlement table that are missing from invoice table
            missing_invoices = {}
            for settlement in processed_data.get("settlementTable", []):
                invoice_number = settlement.get("invoice_number")
                if not invoice_number:
                    continue
                    
                if invoice_number not in invoice_numbers:
                    # Track total settlement amount for each missing invoice
                    if invoice_number not in missing_invoices:
                        missing_invoices[invoice_number] = 0
                        
                    # Add settlement amount to total
                    settlement_amount = settlement.get("settlement_amount")
                    if settlement_amount:
                        try:
                            amount = float(settlement_amount)
                            missing_invoices[invoice_number] += amount
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid settlement amount for invoice {invoice_number}: {settlement_amount}")
            
            # Create entries for missing invoices
            if missing_invoices:
                if "invoiceTable" not in processed_data:
                    processed_data["invoiceTable"] = []
                    
                for invoice_number, total_amount in missing_invoices.items():
                    logger.info(f"Adding missing invoice {invoice_number} with total amount {total_amount}")
                    processed_data["invoiceTable"].append({
                        "invoice_number": invoice_number,
                        "invoice_date": None,
                        "booking_amount": None,
                        "total_settlement_amount": str(total_amount)
                    })
                    invoice_numbers.add(invoice_number)
                    
            return processed_data
            
        except Exception as e:
            logger.error(f"Error in Amazon post-processing: {str(e)}")
            # Return original data on error
            return extracted_data

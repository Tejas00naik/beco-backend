"""LLM Extractor for payment advice documents."""

import json
import logging
import os
import sys
import tempfile
import base64
from typing import Dict, Any, Optional, List
from pathlib import Path

from openai import OpenAI
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from src.llm_integration.config import PROMPT_MAP, DEFAULT_MODEL
from models.firestore_dao import FirestoreDAO
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class LLMExtractor:
    """
    Extracts structured data from payment advice documents using LangChain and OpenAI.
    """
    
    def __init__(self, dao: Optional[FirestoreDAO] = None):
        """
        Initialize the LLM extractor.
        
        Args:
            dao: Optional FirestoreDAO instance for group/legal entity lookups
        """
        # Initialize OpenAI client
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OpenAI API key not provided in environment variables")
            
        self.openai_client = OpenAI(api_key=openai_api_key)
        self.model = os.environ.get('OPENAI_MODEL', 'gpt-4.1')  # Default to gpt-4.1 if not specified
        
        # Keep langchain client for backward compatibility
        self.llm = ChatOpenAI(
            openai_api_key=openai_api_key,
            model=DEFAULT_MODEL,
            temperature=0.0  # Deterministic output
        )
        
        logger.info(f"Initialized LLM extractor with model {self.model}")
        
        self.dao = dao
        
    async def process_document(self, document_text: Optional[str] = None, pdf_path: Optional[str] = None, email_body: Optional[str] = None, group_uuid: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a document with LLM extraction.
        
        Args:
            document_text: Text content of the document to process
            pdf_path: Path to PDF file to process (alternative to document_text)
            email_body: Optional email body text to provide additional context
            group_uuid: Optional group UUID for group-specific prompt selection
            
        Returns:
            Extracted JSON data
        """
        # Extract text if we have a PDF file
        if pdf_path and not document_text:
            logger.info(f"Extracting text from PDF for pre-processing: {pdf_path}")
            document_text = self._extract_text_from_pdf(pdf_path)
            
        if not document_text:
            logger.error("No document text or PDF provided")
            return {}
            
        # Pre-detect group using a simple extraction technique if not provided
        if not group_uuid:
            logger.info("No group_uuid provided, attempting to pre-detect group...")
            # Quick heuristic for Amazon detection
            if "Clicktech Retail" in document_text or "Amazon" in document_text:
                group_uuid = "group-amazon-12345"
                logger.info(f"Pre-detected group_uuid: {group_uuid} from document text")
            
        # Select the appropriate prompt based on group_uuid
        prompt_template = self._get_prompt_template(group_uuid)
        
        # Combine any provided email body with instructions
        instruction = prompt_template["template"]
        if email_body:
            instruction = f"EMAIL BODY:\n{email_body}\n\nINSTRUCTIONS:\n{instruction}"
            
        # Two different approaches based on whether we want to use direct file upload
        use_file_api = pdf_path and False  # Disabled for now due to API errors
        
        if use_file_api:
            logger.info(f"Processing PDF file using direct file upload: {pdf_path}")
            try:
                # Using the file upload API for PDFs
                with open(pdf_path, "rb") as file:
                    # Step 1: Upload the PDF file
                    upload = self.openai_client.files.create(
                        file=file,
                        purpose="user_data"
                    )
                    file_id = upload.id
                    logger.info(f"Uploaded PDF with file ID: {file_id}")
                
                # Step 2: Include the file in the conversation input
                logger.info(f"Calling {self.model} with direct file upload")
                response = self.openai_client.responses.create(
                    model="gpt-4.1-mini",  # Using gpt-4.1-mini for file processing
                    input=[
                        {"type": "text", "text": instruction},
                        {"type": "file", "file_id": file_id}
                    ],
                    temperature=0.0  # Deterministic output
                )
                
                response_text = response.output_text
                logger.info(f"Got response with {len(response_text)} chars")
            except Exception as e:
                logger.error(f"Error using file upload API: {str(e)}")
                use_file_api = False  # Fall back to text extraction method
        
        if not use_file_api:
            # Traditional text-based approach
            logger.info(f"Processing document using text input with prompt template: {prompt_template['name']}")
            full_text = document_text
            if email_body:
                full_text = f"EMAIL BODY:\n{email_body}\n\nDOCUMENT CONTENT:\n{document_text}"
                
            # Call the LLM with text
            logger.info(f"Calling {self.model} with document text")
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt_template["template"]},
                    {"role": "user", "content": full_text}
                ],
                temperature=0.0  # Deterministic output
            )
            
            response_text = response.choices[0].message.content
            logger.info(f"Got response with {len(response_text)} chars")
        
        # Extract JSON from response
        processed_output = self._extract_json_from_response(response_text)
        
        # Detect legal entity and get final group_uuid from extracted data
        detected_legal_entity = await self._detect_legal_entity(processed_output)
        if detected_legal_entity:
            processed_output.update(detected_legal_entity)
            # Update group_uuid for post-processing if it was found
            if "group_uuid" in detected_legal_entity:
                group_uuid = detected_legal_entity["group_uuid"]
        
        # Apply any group-specific post-processing
        if group_uuid == "group-amazon-12345" or self._detect_amazon_format(processed_output):
            processed_output = self._post_process_amazon_output(processed_output)
            
        return processed_output
            
    async def _process_pdf_file(self, pdf_path: str, prompt: str, email_body: str = None, group_uuid: str = None) -> Dict[str, Any]:
        """
        Process a PDF file by first extracting text and then using OpenAI's API.
        
        Args:
            pdf_path: Path to the PDF file
            prompt: Prompt to use for extraction
            email_body: Optional email body text
            group_uuid: Optional group UUID for post-processing
            
        Returns:
            Extracted data from the PDF
        """
        try:
            # Check if file exists
            if not os.path.exists(pdf_path):
                raise FileNotFoundError(f"PDF file not found at path: {pdf_path}")
                
            logger.info(f"Processing PDF file: {pdf_path}")
            
            # Import PyPDF2 here to avoid circular imports
            try:
                import PyPDF2
            except ImportError:
                logger.warning("PyPDF2 not installed. Installing now...")
                import subprocess
                subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2"])
                import PyPDF2
            
            # Extract text from PDF
            logger.info("Extracting text from PDF...")
            pdf_text = ""
            with open(pdf_path, "rb") as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    pdf_text += page.extract_text() + "\n\n"
            
            logger.info(f"Extracted {len(pdf_text)} characters from PDF")
            
            # Prepare the message content and prompt
            extraction_prompt = f"{prompt}\n\nExtract the payment advice data from the following text (extracted from PDF) in JSON format. Include invoice details, settlement information, and any other relevant data.\n\nPDF TEXT:\n{pdf_text}"
            
            # Add email body if provided
            if email_body:
                extraction_prompt += f"\n\nAdditional context from email body:\n{email_body}"
            
            # Make the API call with the text
            logger.info(f"Calling {self.model} with extracted PDF text")
            
            # Call the API using chat completions with text only
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": extraction_prompt
                    }
                ],
                max_tokens=4000  # Ensure we have enough tokens for the full response
            )
            
            # Get the text response
            response_content = response.choices[0].message.content
            logger.info(f"Got response with {len(response_content)} chars")
            
            # Extract JSON from response
            output = self._extract_json_from_response(response_content)
            
            # Post-process output for Amazon group if applicable
            if group_uuid == "group-amazon-12345" or self._detect_amazon_format(output):
                output = self._post_process_amazon_output(output)
                
            return output
            
        except Exception as e:
            logger.error(f"Error processing PDF file with OpenAI API: {str(e)}")
            raise
    
    def _get_prompt_template(self, group_uuid: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the appropriate prompt template for a given group.
        
        Args:
            group_uuid: Optional group UUID
            
        Returns:
            Dictionary with prompt template information
        """
        template_name = "default"
        if group_uuid and group_uuid in PROMPT_MAP:
            template_name = group_uuid
        
        template = PROMPT_MAP.get(template_name, PROMPT_MAP['default'])
        prompt_preview = template[:50] + "..." if template else "[No template found]"
        logger.info(f"Selected prompt template '{template_name}', preview: {prompt_preview}")
            
        return {
            "name": template_name,
            "template": template
        }
    
    def _extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text from a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Extracted text from the PDF
        """
        # Import PyPDF2 here to avoid circular imports
        try:
            import PyPDF2
        except ImportError:
            logger.warning("PyPDF2 not installed. Installing now...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2"])
            import PyPDF2
        
        # Extract text from PDF
        logger.info("Extracting text from PDF...")
        pdf_text = ""
        with open(pdf_path, "rb") as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                pdf_text += page.extract_text() + "\n\n"
        
        logger.info(f"Extracted {len(pdf_text)} characters from PDF")
        return pdf_text
    
    def _get_prompt_for_group(self, group_uuid: str) -> str:
        """
        Get the appropriate prompt template for the given group UUID.
        
        Args:
            group_uuid: The group UUID to get a prompt for
            
        Returns:
            The prompt template to use
        """
        if not group_uuid or group_uuid not in PROMPT_MAP:
            logger.info(f"Using default prompt template (group_uuid={group_uuid})")
            return PROMPT_MAP['default']
            
        logger.info(f"Using group-specific prompt template for group_uuid={group_uuid}")
        return PROMPT_MAP[group_uuid]
    
    def _extract_json_from_response(self, response_text: str) -> Dict[str, Any]:
        """
        Extract JSON from LLM response text.
        
        Args:
            response_text: Raw response text from LLM
            
        Returns:
            Extracted JSON data
        """
        # Define pattern to extract JSON content
        import re
        json_pattern = r'```json\s*([\s\S]*?)\s*```'
        
        # Try to find JSON content in the response
        match = re.search(json_pattern, response_text)
        
        if match:
            # Extract the JSON content from the markdown code block
            json_str = match.group(1)
        else:
            # If no markdown code block, try to extract the entire JSON object
            json_pattern = r'(\{[\s\S]*\})'
            match = re.search(json_pattern, response_text)
            if match:
                json_str = match.group(1)
            else:
                json_str = response_text
        
        # Clean up potential issues
        json_str = json_str.strip()
        
        # Try to parse the JSON
        try:
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from LLM response: {e}")
            logger.debug(f"Problematic JSON string: {json_str}")
            # Return empty dict on error
            return {}
            
    async def _detect_legal_entity(self, output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect the legal entity and group from the extracted output.
        
        Args:
            output: Extracted output from LLM
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid if found
        """
        result = {}
        
        # Extract payer name from meta table
        payer_name = None
        if isinstance(output, dict) and 'meta_table' in output:
            meta = output['meta_table']
            if isinstance(meta, dict) and 'payer_legal_name' in meta:
                payer_name = meta['payer_legal_name']
        
        if not payer_name:
            return result
            
        # Look up legal entity by name
        logger.info(f"Detecting legal entity for payer_name: {payer_name}")
        try:
            legal_entities = await self.dao.query_documents(
                "legal_entity",
                filters=[("payer_legal_name", "==", payer_name)]
            )
            
            if legal_entities:
                legal_entity = legal_entities[0]
                legal_entity_uuid = legal_entity.get("uuid")
                group_uuid = legal_entity.get("group_uuid")
                
                if legal_entity_uuid:
                    result["legal_entity_uuid"] = legal_entity_uuid
                    
                if group_uuid:
                    result["group_uuid"] = group_uuid
                    logger.info(f"Found legal entity UUID {legal_entity_uuid} with group UUID {group_uuid} for payer {payer_name}")
        except Exception as e:
            logger.error(f"Error detecting legal entity: {str(e)}")
            
        # Also detect group directly if needed
        if not result.get("group_uuid") and payer_name:
            logger.info(f"Detecting group for payer_name: {payer_name}")
            try:
                legal_entities = await self.dao.query_documents(
                    "legal_entity",
                    filters=[("payer_legal_name", "==", payer_name)]
                )
                
                if legal_entities:
                    legal_entity = legal_entities[0]
                    group_uuid = legal_entity.get("group_uuid")
                    
                    if group_uuid:
                        result["group_uuid"] = group_uuid
                        logger.info(f"Found group UUID {group_uuid} for payer {payer_name}")
            except Exception as e:
                logger.error(f"Error detecting group: {str(e)}")
                
        return result
    
    def _detect_amazon_format(self, output: Dict[str, Any]) -> bool:
        """
        Detect if the output appears to be from an Amazon payment advice.
        
        Args:
            output: Extracted output from LLM
            
        Returns:
            True if Amazon format detected, False otherwise
        """
        # Amazon format detection logic
        if not isinstance(output, dict):
            return False
            
        # Check for common Amazon-specific fields
        meta = output.get('meta_table', {})
        if isinstance(meta, dict) and 'payer_legal_name' in meta:
            payer = meta['payer_legal_name']
            if payer and ('Amazon' in payer or 'Clicktech' in payer):
                return True
                
        # Check for Amazon in sender or recipient names
        meta_table = output.get('meta_table', {})
        sender = meta_table.get('sender', '').lower()
        recipient = meta_table.get('recipient', '').lower()
        
        if 'amazon' in sender or 'amazon' in recipient:
            return True
            
        return False
        
    def _post_process_amazon_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply Amazon-specific post-processing to the LLM output.
        
        For Amazon payment advices, we need to ensure all invoices referenced in settlements
        exist in the invoice table.
        
        Args:
            processed_output: The LLM-processed output
            
        Returns:
            Updated processed output
        """
        # Get all invoice numbers from the invoice table
        invoice_numbers = set()
        for invoice in processed_output.get("invoice_table", []):
            if "invoice_number" in invoice:
                invoice_numbers.add(invoice["invoice_number"])
        
        # Get all settlement invoice mappings
        settlement_count = len(processed_output.get("reconciliation_statement", []))
        invoice_count = len(processed_output.get("invoice_table", []))
        
        logger.info(f"Amazon post-processing: Before - {invoice_count} invoices, {settlement_count} settlements")
        logger.info(f"Invoice numbers already in invoice table: {invoice_numbers}")
        
        # Check for invoice numbers in reconciliation that are missing in invoice table
        for recon_item in processed_output.get("reconciliation_statement", []):
            invoice_number = recon_item.get("invoice_number")
            if not invoice_number or invoice_number in invoice_numbers:
                continue
                
            # This invoice number is mentioned in settlement but missing from invoice table
            logger.info(f"Amazon post-processing: Found missing invoice {invoice_number} in reconciliation")
            
            # Calculate total settlement amount for this invoice number
            settlement_amounts = []
            total_amount = 0
            
            for s in processed_output.get("reconciliation_statement", []):
                if s.get("invoice_number") == invoice_number and s.get("settlement_amount"):
                    try:
                        amount = float(s["settlement_amount"])
                        settlement_amounts.append(amount)
                        total_amount += amount
                    except (ValueError, TypeError):
                        pass
            
            logger.info(f"  Found {len(settlement_amounts)} matching settlements with amounts: {settlement_amounts}")
            logger.info(f"  Total calculated settlement amount: {total_amount}")
            
            # Create a new invoice entry
            invoice_entry = {
                "invoice_number": invoice_number,
                "invoice_date": None,
                "booking_amount": None,
                "total_invoice_settlement_amount": total_amount
            }
            
            # Add to the invoice table
            if "invoice_table" not in processed_output:
                processed_output["invoice_table"] = []
                
            processed_output["invoice_table"].append(invoice_entry)
            invoice_numbers.add(invoice_number)  # Add to set to avoid duplicates
            logger.info(f"Amazon post-processing: Added missing invoice {invoice_number} to invoice table with amount {total_amount}")
        
        # Log after post-processing counts
        invoice_count_after = len(processed_output.get("invoice_table", []))
        logger.info(f"Amazon post-processing: After - {invoice_count_after} invoices, {settlement_count} settlements")
        logger.info(f"Amazon post-processing: Added {invoice_count_after - invoice_count} new invoice records")
        
        return processed_output
            
    async def process_attachment_for_payment_advice(self, email_text_content: str, attachment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single attachment as a payment advice.
        
        This method provides compatibility with the EmailProcessor interface, which
        expects this method signature from the LLM extractor.
        
        Args:
            email_text_content: Text content of the email
            attachment_data: Dictionary containing attachment data
            
        Returns:
            Processed payment advice data in the format expected by PaymentProcessor
        """
        try:
            # Extract the attachment text
            attachment_text = attachment_data.get("text_content", "")
            if not attachment_text:
                logger.warning("No text content in attachment, using email body instead")
                attachment_text = email_text_content
                email_text_content = ""
                
            # Get filename for logging
            attachment_filename = attachment_data.get("filename", "unknown")
            logger.info(f"Processing attachment {attachment_filename} with real LLM using GPT-4-turbo")
            
            # Check if we can detect a group for this document
            # This is a pre-check to use the right prompt template
            detected_group_uuid = None
            if self.dao:
                # Use text from both email body and attachment to improve detection
                combined_text = f"{email_text_content} {attachment_text}"
                payee_name = self._extract_potential_payee_name(combined_text)
                
                if payee_name:
                    logger.info(f"Pre-detected payee name: {payee_name}")
                    detected_group_uuid = await self._get_group_uuid_for_legal_entity(payee_name)
                    logger.info(f"Pre-detected group UUID: {detected_group_uuid}")
            
            # Process the document with the LLM
            output = await self.process_document(
                document_text=attachment_text,
                email_body=email_text_content,
                group_uuid=detected_group_uuid
            )
            
            # Post-processing for expected format
            from src.llm_integration.utils import convert_llm_output_to_processor_format
            processed_output = await convert_llm_output_to_processor_format(output)
            
            # Add legal entity and group UUIDs if detected
            legal_entity_uuid = await self.detect_legal_entity_from_output(output)
            group_uuid = await self.detect_group_from_output(output)
            
            if legal_entity_uuid:
                processed_output["legal_entity_uuid"] = legal_entity_uuid
                
            if group_uuid:
                processed_output["group_uuid"] = group_uuid
                
            return processed_output
            
        except Exception as e:
            logger.error(f"Error processing attachment with LLM: {str(e)}")
            raise
    
    async def detect_legal_entity_from_output(self, output: Dict[str, Any]) -> Optional[str]:
        """
        Detect legal entity UUID from the LLM output.
        
        Args:
            output: The LLM-processed output
            
        Returns:
            Legal entity UUID if found, None otherwise
        """
        if not self.dao:
            logger.warning("No DAO provided, cannot detect legal entity")
            return None
            
        try:
            # Get payer legal name from meta_table
            payer_name = output.get("meta_table", {}).get("payer_legal_name")
            if not payer_name:
                logger.warning("No payer_legal_name found in LLM output")
                return None
                
            logger.info(f"Detecting legal entity for payer_name: {payer_name}")
            
            # Query legal entity table to find matching entity
            legal_entities = await self.dao.query_documents(
                "legal_entity",
                [("legal_entity_name", "==", payer_name)]
            )
            
            if not legal_entities or len(legal_entities) == 0:
                logger.warning(f"No legal entity found for payer name: {payer_name}")
                return None
                
            legal_entity = legal_entities[0]
            legal_entity_uuid = legal_entity.get("legal_entity_uuid")
            group_uuid = legal_entity.get("group_uuid")
            
            logger.info(f"Found legal entity UUID {legal_entity_uuid} with group UUID {group_uuid} for payer {payer_name}")
            return legal_entity_uuid
            
        except Exception as e:
            logger.error(f"Error detecting legal entity: {str(e)}")
            return None
            
    async def detect_group_from_output(self, output: Dict[str, Any]) -> Optional[str]:
        """
        Detect group UUID from the LLM output.
        
        Args:
            output: The LLM-processed output
            
        Returns:
            Group UUID if found, None otherwise
        """
        if not self.dao:
            logger.warning("No DAO provided, cannot detect group")
            return None
            
        try:
            # Get payer legal name from meta_table
            payer_name = output.get("meta_table", {}).get("payer_legal_name")
            if not payer_name:
                logger.warning("No payer_legal_name found in LLM output")
                return None
                
            logger.info(f"Detecting group for payer_name: {payer_name}")
            
            # Query legal entity table to find matching entity
            legal_entities = await self.dao.query_documents(
                "legal_entity",
                [("legal_entity_name", "==", payer_name)]
            )
            
            if not legal_entities or len(legal_entities) == 0:
                logger.warning(f"No legal entity found for payer name: {payer_name}")
                return None
                
            legal_entity = legal_entities[0]
            group_uuid = legal_entity.get("group_uuid")
            
            logger.info(f"Found group UUID {group_uuid} for payer {payer_name}")
            return group_uuid
            
        except Exception as e:
            logger.error(f"Error detecting group: {str(e)}")
            return None

"""LLM Extractor for payment advice documents."""

import json
import logging
import os
import sys
import tempfile
import base64
import re
import PyPDF2
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from openai import OpenAI
from openai.types.beta.threads.message_create_params import Attachment, AttachmentToolFileSearch
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from src.external_apis.llm.config import DEFAULT_MODEL
from src.external_apis.llm.group_factory import GroupProcessorFactory
from src.repositories.firestore_dao import FirestoreDAO

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
        logger.info("Loading OpenAI API key from environment...")
        # Force load from .env again to ensure latest values
        load_dotenv(override=True)
        
        # Get API key directly from .env file for maximum reliability
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            logger.error("OpenAI API key not found in environment variables!")
            raise ValueError("OpenAI API key not provided in environment variables")
        else:
            # Log the first and last few characters of the key for debugging
            key_prefix = openai_api_key[:8] if len(openai_api_key) > 8 else openai_api_key
            key_suffix = openai_api_key[-4:] if len(openai_api_key) > 4 else ""
            logger.info(f"Found OpenAI API key in environment: {key_prefix}...{key_suffix}")
            
            # Store the API key explicitly as an instance variable for direct access
            self.api_key = openai_api_key
            
        # Create a new OpenAI client with the API key
        self.client = OpenAI(api_key=self.api_key)
        
        self.dao = dao
        
        # Initialize the group processor factory
        self.group_processor_factory = GroupProcessorFactory()
        
        self.model = os.environ.get('OPENAI_MODEL', 'gpt-4.1')  # Default to gpt-4.1 if not specified

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
            raise Exception("No group_uuid provided")
            
        # Select the appropriate prompt based on group_uuid
        prompt_template = self._get_prompt_template(group_uuid)
        
        # Combine any provided email body with instructions
        instruction = prompt_template["template"]
        if email_body:
            instruction = f"EMAIL BODY:\n{email_body}\n\nINSTRUCTIONS:\n{instruction}"
            
        # Traditional text-based approach
        logger.info(f"Processing document using text input with prompt template: {prompt_template['name']}")
        full_text = document_text
        if email_body:
            full_text = f"EMAIL BODY:\n{email_body}\n\nDOCUMENT CONTENT:\n{document_text}"
        
        # Log document size information
        doc_size_kb = len(full_text) / 1024
        prompt_size_kb = len(prompt_template["template"]) / 1024
        logger.info(f"Document size: {doc_size_kb:.2f} KB, Prompt size: {prompt_size_kb:.2f} KB")
        
        # Estimate token count (rough approximation: 1 token ≈ 4 characters for English text)
        doc_tokens = len(full_text) / 4
        prompt_tokens = len(prompt_template["template"]) / 4
        total_tokens = doc_tokens + prompt_tokens
        
        logger.info(f"Estimated token counts - Document: {doc_tokens:.0f}, Prompt: {prompt_tokens:.0f}, Total: {total_tokens:.0f}")
        
        # Check if likely to exceed token limits
        if total_tokens > 128000:  # GPT-4 Turbo max context window
            logger.warning(f"⚠️ POTENTIAL TOKEN LIMIT ISSUE: Estimated tokens ({total_tokens:.0f}) may exceed model context limit")
            
        # Call the LLM with text
        logger.info(f"Calling {self.model} with document text")
        try:
            # Add timeout to prevent hanging indefinitely
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt_template["template"]},
                    {"role": "user", "content": full_text}
                ],
                temperature=0.0,  # Deterministic output
                timeout=90.0  # 60 second timeout
            )
            
            # Log token usage from response
            if hasattr(response, 'usage') and response.usage:
                logger.info(f"Actual token usage - Prompt: {response.usage.prompt_tokens}, "  
                           f"Completion: {response.usage.completion_tokens}, "  
                           f"Total: {response.usage.total_tokens}")
            
            response_text = response.choices[0].message.content
            logger.info(f"Got response with {len(response_text)} chars")
        except Exception as e:
            # Enhanced error logging with specific error types
            error_type = type(e).__name__
            error_msg = str(e).lower()
            
            logger.error(f"Error calling OpenAI API ({error_type}): {str(e)}")
            
            # Check for specific error conditions
            if any(token_err in error_msg for token_err in ["maximum context length", "token limit", "tokens in prompt"]):
                logger.error(f"⚠️ TOKEN LIMIT EXCEEDED: Document is too large for {self.model}. "  
                           f"Estimated tokens: {total_tokens:.0f}")
            elif "rate limit" in error_msg:
                logger.error(f"⚠️ RATE LIMIT: OpenAI API rate limit reached")
            elif "timeout" in error_msg:
                logger.error(f"⚠️ TIMEOUT: Request timed out after 90 seconds. Document may be too large or complex")
            
            # Include full traceback for detailed debugging
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Provide a fallback response structure to avoid cascading failures
            response_text = '{"meta_table": {}, "invoice_table": [], "other_doc_table": [], "settlement_table": []}'
            logger.warning(f"Using fallback response structure due to API error: {response_text}")
            # You may want to re-raise the error after logging in certain cases
            # raise
        
        # Extract JSON from response
        processed_output = self._extract_json_from_response(response_text)
        
        # Apply group-specific post-processing using the factory pattern
        processed_output = self._post_process_group_output(processed_output, group_uuid)
            
        return processed_output

    def _get_prompt_template(self, group_uuid: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the appropriate prompt template for a given group using the factory pattern.
        
        Args:
            group_uuid: Optional group UUID
            
        Returns:
            Dictionary with prompt template information
        """
        try:
            # Get the appropriate processor for this group UUID
            processor = self.group_processor_factory.get_processor(group_uuid)
            template = processor.get_prompt_template()
            template_name = processor.__class__.__name__
            
            prompt_preview = template[:50] + "..." if template else "[No template found]"
            logger.info(f"Selected prompt template from '{template_name}', preview: {prompt_preview}")
                
            return {
                "name": template_name,
                "template": template
            }
        except Exception as e:
            logger.error(f"Error getting prompt template: {str(e)}")
            # Fall back to default processor if anything goes wrong
            processor = self.group_processor_factory.get_processor(None)
            template = processor.get_prompt_template()
            template_name = processor.__class__.__name__
            
            logger.warning(f"Falling back to default prompt template '{template_name}'")
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
    

    def _extract_json_from_response(self, response_text: str) -> Dict[str, Any]:
        """
        Extract JSON from LLM response text.
        
        Args:
            response_text: Raw response text from LLM
            
        Returns:
            Extracted JSON data with at least empty tables for downstream processing
        """
        if not response_text:
            logger.warning("Empty response text received from LLM")
            return self._create_default_empty_structure()
            
        # Define pattern to extract JSON content
        import re
        json_pattern = r'```json\s*([\s\S]*?)\s*```'
        
        # Try to find JSON content in the response
        match = re.search(json_pattern, response_text)
        
        if match:
            # Extract the JSON content from the markdown code block
            json_str = match.group(1)
            logger.debug(f"Extracted JSON from markdown code block: {json_str[:100]}...")
        else:
            # If no markdown code block, try to extract the entire JSON object
            json_pattern = r'(\{[\s\S]*\})'
            match = re.search(json_pattern, response_text)
            if match:
                json_str = match.group(1)
                logger.debug(f"Extracted JSON from raw text: {json_str[:100]}...")
            else:
                logger.warning("Could not extract JSON pattern from response")
                json_str = response_text
                logger.debug(f"Using raw response text: {json_str[:100]}...")
        
        # Clean up potential issues
        json_str = json_str.strip()
        
        # Try to parse the JSON
        try:
            data = json.loads(json_str)
            
            # Validate that we have the required structure
            if not isinstance(data, dict):
                logger.warning(f"Extracted data is not a dictionary: {type(data)}")
                data = self._create_default_empty_structure()
                
            # Ensure all required tables exist even if empty
            if 'meta_table' not in data:
                logger.warning("meta_table missing from extracted data, adding empty one")
                data['meta_table'] = {}
                
            if 'invoice_table' not in data:
                logger.warning("invoice_table missing from extracted data, adding empty one")
                data['invoice_table'] = []
                
            if 'reconciliation_statement' not in data:
                logger.warning("reconciliation_statement missing from extracted data, adding empty one")
                data['reconciliation_statement'] = []
                
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from LLM response: {e}")
            logger.debug(f"Problematic JSON string: {json_str}")
            return self._create_default_empty_structure()
            
    def _create_default_empty_structure(self) -> Dict[str, Any]:
        """
        Create a default empty structure with all required tables.
        
        Returns:
            Default empty structure
        """
        return {
            'meta_table': {},
            'invoice_table': [],
            'reconciliation_statement': [],
            'other_doc_table': []
        }
            
    def _extract_potential_payee_name(self, text_content: str) -> Optional[str]:
        """
        Extract a potential payee name from text content for group detection.
        
        Args:
            text_content: Text content to extract payee name from
            
        Returns:
            Potential payee name or None if not found
        """
        if not text_content:
            return None
            
        # Simple extraction based on common patterns in payment advice documents
        payee_patterns = [
            r"(?:payee|paid to|payment to)[:\s]+([A-Za-z0-9\s&.,()'-]{3,50})",
            r"(?:recipient|beneficiary)[:\s]+([A-Za-z0-9\s&.,()'-]{3,50})",
            r"(?:remit(?:ted)? to)[:\s]+([A-Za-z0-9\s&.,()'-]{3,50})",
            r"(?:KWICK LIVING|Amazon|Clicktech)"
        ]
        
        for pattern in payee_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                # Return the first match, cleaned up
                payee_name = matches[0].strip()
                # Remove any trailing punctuation
                payee_name = re.sub(r'[.,;:\s]+$', '', payee_name)
                return payee_name
                
        # Look for specific known payees
        known_payees = ["KWICK LIVING", "Amazon", "Clicktech"]
        for payee in known_payees:
            if payee in text_content:
                return payee
                
        return None
        
    async def _get_group_uuid_for_legal_entity(self, payee_name: str) -> Optional[str]:
        """
        Look up a group UUID for a given payee name.
        
        Args:
            payee_name: Name of payee to search for
            
        Returns:
            Group UUID if found, None otherwise
        """
        if not self.dao or not payee_name:
            return None
            
        try:
            # First try exact match
            legal_entities = await self.dao.query_documents(
                "legal_entity",
                filters=[("payer_legal_name", "==", payee_name)]
            )
            
            if not legal_entities:
                # Try case-insensitive contains match
                payee_lower = payee_name.lower()
                all_legal_entities = await self.dao.query_documents("legal_entity")
                legal_entities = [le for le in all_legal_entities 
                                if le.get("payer_legal_name", "").lower().find(payee_lower) >= 0]
            
            if legal_entities:
                legal_entity = legal_entities[0]
                group_uuid = legal_entity.get("group_uuid")
                logger.info(f"Found group UUID {group_uuid} for payee {payee_name}")
                return group_uuid
                
        except Exception as e:
            logger.error(f"Error looking up group UUID for payee {payee_name}: {str(e)}")
 
        return None
        
    async def _detect_legal_entity(self, output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect the legal entity and group from the extracted output.
        
        Args:
            output: Extracted output from LLM
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid if found
        """
        logger.debug(f"_detect_legal_entity called with output type: {type(output)}")
        
        # Initialize empty result
        result = {}
        
        try:
            if not output or not isinstance(output, dict):
                logger.warning(f"_detect_legal_entity received invalid output: {output}")
                return result
                
            # Check if meta_table exists and is a dict
            meta_table = output.get('meta_table')
            if not meta_table or not isinstance(meta_table, dict):
                logger.warning(f"meta_table missing or not a dict: {meta_table}")
                return result
                
            # Extract payer name from meta table - with detailed logging
            payer_name = None
            logger.debug(f"Meta table keys: {meta_table.keys() if isinstance(meta_table, dict) else 'Not a dict'}")
            
            if 'payer_legal_name' in meta_table:
                payer_name = meta_table['payer_legal_name']
                logger.debug(f"Found payer_legal_name: {payer_name}")
            elif 'payer' in meta_table:
                payer_name = meta_table['payer']
                logger.debug(f"Found payer: {payer_name}")
            
            if not payer_name or not isinstance(payer_name, str) or payer_name.strip() == "":
                logger.warning(f"No valid payer name found in meta_table")
                return result
                
            # Look up legal entity by name
            logger.info(f"Detecting legal entity for payer_name: {payer_name}")
            try:
                if self.dao is None:
                    logger.warning("No DAO available for legal entity lookup")
                    return result
                    
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
                logger.error(f"Error querying legal entity: {str(e)}")
                
            # Also detect group directly if needed
            if not result.get("group_uuid") and payer_name:
                logger.info(f"Detecting group for payer_name: {payer_name}")
                try:
                    if self.dao is None:
                        logger.warning("No DAO available for group lookup")
                        return result
                        
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
        except Exception as e:
            logger.error(f"Unexpected error in _detect_legal_entity: {str(e)}")
            # Return empty result on error
            return {}
    
    def _pre_detect_amazon_format(self, text: str) -> bool:
        """
        Pre-detect Amazon format from email body or document text before LLM processing.
        
        Args:
            text: Email body or document text to check
            
        Returns:
            True if Amazon format detected, False otherwise
        """
        if not isinstance(text, str):
            return False
            
        # Look for Amazon-related keywords in the text
        amazon_keywords = [
            "amazon", "clicktech", "retail private limited", 
            "clicktech retail", "payment advice", "amazon.in",
            "amazon development center", "amazon seller services"
        ]
        
        text_lower = text.lower()
        for keyword in amazon_keywords:
            if keyword.lower() in text_lower:
                logger.debug(f"Pre-detected Amazon format from keyword: {keyword}")
                return True
                
        return False
        
    def _detect_amazon_format(self, output: Dict[str, Any]) -> bool:
        """
        Detect if the output appears to be from an Amazon payment advice.
        
        Args:
            output: Extracted output from LLM
            
        Returns:
            True if Amazon format detected, False otherwise
        """
        try:
            # Amazon format detection logic
            if not isinstance(output, dict):
                logger.debug("Output is not a dict in _detect_amazon_format")
                return False
                
            # Get meta_table and ensure it's a dictionary
            meta_table = output.get('meta_table')
            if meta_table is None or not isinstance(meta_table, dict):
                logger.debug(f"meta_table is not valid in _detect_amazon_format: {type(meta_table)}")
                return False
                
            # Check for Amazon in payer_legal_name
            payer = meta_table.get('payer_legal_name')
            if payer and isinstance(payer, str) and ('Amazon' in payer or 'Clicktech' in payer):
                logger.debug(f"Detected Amazon format from payer_legal_name: {payer}")
                return True
                
            # Check for Amazon in sender name
            sender = meta_table.get('sender')
            if sender and isinstance(sender, str) and 'amazon' in sender.lower():
                logger.debug(f"Detected Amazon format from sender: {sender}")
                return True
                
            # Check for Amazon in recipient name
            recipient = meta_table.get('recipient')
            if recipient and isinstance(recipient, str) and 'amazon' in recipient.lower():
                logger.debug(f"Detected Amazon format from recipient: {recipient}")
                return True
                
            logger.debug("No Amazon format detected in _detect_amazon_format")
            return False
        except Exception as e:
            logger.warning(f"Error in _detect_amazon_format: {e}")
            import traceback
            logger.warning(f"Traceback: {traceback.format_exc()}")
            return False
        
    def _post_process_group_output(self, processed_output: Dict[str, Any], group_uuid: str) -> Dict[str, Any]:
        """
        Apply group-specific post-processing to the LLM output using factory pattern.
        
        Args:
            processed_output: The LLM-processed output
            group_uuid: The group UUID for selecting the appropriate processor
            
        Returns:
            Updated processed output
        """
        try:
            # Get the appropriate processor for this group UUID
            processor = self.group_processor_factory.get_processor(group_uuid)
            logger.info(f"Post-processing output with {processor.__class__.__name__}")
            
            # Apply the processor's post-processing logic
            processed_output = processor.post_process_output(processed_output)
            
            # Always include group_uuid in output
            if processed_output and isinstance(processed_output, dict):
                processed_output["group_uuid"] = group_uuid
                
            return processed_output
        except Exception as e:
            logger.error(f"Error in post-processing output with processor: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return the original output to avoid making the situation worse
            return processed_output

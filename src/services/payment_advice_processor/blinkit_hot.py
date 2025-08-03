import logging
from typing import Dict, Any, List
import pandas as pd
import json
import re

from src.services.payment_advice_processor.group_factory import GroupProcessor

logger = logging.getLogger(__name__)

class HOTGroupProcessor(GroupProcessor):
    """HandsOnTrade-specific group processor for Excel attachments that contain multiple payment advices."""
    
    def get_prompt_template(self) -> str:
        """Get the HOT-specific prompt template."""
        # This is a placeholder - HOT processor will handle Excel files directly, not use LLM
        return """"""
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """Process HOT Excel file to extract multiple payment advices."""
        # This method will be implemented to parse Excel and create multiple payment advices
        # But for now, we return the input unchanged as this is a skeleton implementation
        return processed_output
    
    def is_hot_excel(self, attachment: Dict[str, Any]) -> bool:
        """Check if the attachment is a HOT Excel file that needs special processing.
        
        Args:
            attachment: Attachment data dictionary
            
        Returns:
            bool: True if this is a HOT Excel attachment
        """
        # Get filename and content type
        filename = attachment.get('filename', '').lower()
        content_type = attachment.get('content_type', '').lower()
        
        # Check if this is an Excel file
        is_excel = (content_type in ['application/vnd.ms-excel', 
                                   'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'] or
                   filename.endswith(('.xls', '.xlsx')))
        
        if not is_excel:
            return False
            
        # Look for HOT-specific patterns in the filename
        hot_indicators = ['hot', 'handson', 'hands-on', 'trade']
        return any(indicator in filename for indicator in hot_indicators)
    
    async def process_excel_attachment(self, attachment: Dict[str, Any], email_body: str = None) -> List[Dict[str, Any]]:
        """
        Process a HOT Excel file and extract multiple payment advices.
        
        Args:
            attachment: Attachment data dictionary
            email_body: Optional email body text for additional context
            
        Returns:
            List of payment advice data dictionaries
        """
        logger.info("Processing HOT Excel attachment directly")
        
        try:
            # Extract Excel file content directly from attachment
            excel_data = self._extract_excel_data(attachment)
            
            # Process Excel data to extract payment advices
            payment_advices = self._process_excel_data(excel_data, email_body)
            
            logger.info(f"Successfully extracted {len(payment_advices)} payment advices from HOT Excel")
            return payment_advices
            
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"Error processing HOT Excel attachment ({error_type}): {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []
    
    def _extract_excel_data(self, attachment: Dict[str, Any]) -> pd.DataFrame:
        """
        Extract data from Excel attachment.
        
        Args:
            attachment: Attachment data dictionary
            
        Returns:
            DataFrame with Excel content
        """
        # This is a placeholder - will be implemented to extract data from Excel files
        # For now, return an empty DataFrame
        return pd.DataFrame()
        
    def _process_excel_data(self, excel_data: pd.DataFrame, email_body: str = None) -> List[Dict[str, Any]]:
        """
        Process Excel data to extract payment advices.
        
        Args:
            excel_data: DataFrame with Excel content
            email_body: Optional email body text for additional context
            
        Returns:
            List of payment advice data dictionaries
        """
        # This is a placeholder - will be implemented to extract data from Excel files
        # For now, return an empty list
        return []
        
    async def process_payment_advice(self, attachment_text: str, email_body: str, attachment_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process payment advice for HOT, with special handling for Excel attachments.
        
        For HOT, we prioritize direct Excel processing over LLM extraction since the 
        HOT format is structured and more reliable to parse programmatically.
        
        Args:
            attachment_text: Text content of the attachment
            email_body: Email body text for additional context
            attachment_obj: Dictionary with attachment metadata
            
        Returns:
            List of processed payment advice dictionaries
        """
        logger.info("Processing payment advice with HOTGroupProcessor")
        
        # Check if this is an Excel file first - the primary use case for HOT
        if attachment_obj and self.is_hot_excel(attachment_obj):
            logger.info("Detected HOT Excel file, processing directly")
            return await self.process_excel_attachment(attachment_obj, email_body)
        
        # Fallback to LLM processing if not Excel or Excel processing fails
        logger.info("Not a HOT Excel file or Excel processing failed, falling back to LLM")
        
        # Import here to avoid circular imports
        from src.external_apis.llm.client import LLMClient
        
        # Initialize the LLM client
        llm_client = LLMClient()
        
        # Get the prompt template
        prompt_text = self.get_prompt_template()
        if not prompt_text:
            # HOT might not have a prompt template if it's primarily for Excel
            # Use a generic template as fallback
            prompt_text = """
            You are a payment advice processing assistant. Please extract structured data from the document.
            Format your response as a JSON object with the following structure:
            {
                "meta_table": {"payment_advice_number": "", "payment_advice_date": "", "payer_legal_name": "", "payee_legal_name": "", "payment_advice_amount": ""},
                "settlement_table": [{"ref_invoice_no": "", "amount": "", "ref1": "", "ref2": "", "ref3": ""}]
            }
            """
        
        # Prepare full text with email body context if available
        if email_body:
            full_text = f"EMAIL BODY:\n{email_body}\n\nDOCUMENT CONTENT:\n{attachment_text}"
        else:
            full_text = attachment_text
        
        try:
            # Call the LLM API
            logger.info("Calling LLM API for HOT payment advice extraction as fallback")
            llm_result = await llm_client.call_chat_api(
                system_prompt=prompt_text,
                user_content=full_text,
                temperature=0.0
            )
            
            response_text = llm_result["response_text"]
            
            # Extract JSON from response
            processed_output = self._extract_json_from_response(response_text)
            
            # Apply post-processing
            processed_output = self.post_process_output(processed_output)
            
            return [processed_output] if processed_output else []
            
        except Exception as e:
            logger.error(f"Error processing payment advice via LLM: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []
    
    def _extract_json_from_response(self, response_text: str) -> Dict[str, Any]:
        """
        Extract JSON from the LLM response text.
        
        Args:
            response_text: Raw response text from LLM
            
        Returns:
            Extracted JSON as dictionary
        """
        try:
            # Try to find a JSON block in the response
            json_match = re.search(r'```(?:json)?\s*({[\s\S]*?})\s*```', response_text)
            if json_match:
                json_str = json_match.group(1)
                return json.loads(json_str)
            
            # If no JSON block, try to parse the entire response as JSON
            return json.loads(response_text)
            
        except Exception as e:
            logger.error(f"Error extracting JSON: {str(e)}")
            # Return empty structure as fallback
            return {"meta_table": {}, "settlement_table": []}


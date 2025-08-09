from src.external_apis.llm.constants import (
    META_PAYMENT_ADVICE_NUMBER,
    META_PAYER_LEGAL_NAME,
    META_PAYMENT_ADVICE_DATE
)
from src.repositories.firestore_dao import FirestoreDAO
import logging
import json
import re

from typing import Dict, Any, List
from uuid import uuid4
from src.services.payment_advice_processor.prompts import AMAZON_PROMPT
from src.services.payment_advice_processor.group_factory import GroupProcessor
logger = logging.getLogger(__name__)


class AmazonGroupProcessor(GroupProcessor):
    """Amazon-specific group processor."""
    
    def get_group_name(self) -> str:
        """Get the name of the group."""
        return "Amazon"


    def get_prompt_template(self) -> str:
        """Get the Amazon-specific prompt template."""
        return AMAZON_PROMPT
    
    async def process_payment_advice(self, attachment_text: str, email_body: str, attachment_obj: Dict[str, Any], attachment_file_format: str) -> Dict[str, Any]:
        """
        Process payment advice using LLM extraction with Amazon-specific logic.
        
        Args:
            attachment_text: Text content of the attachment
            email_body: Email body text for additional context
            attachment_obj: Dictionary with attachment metadata
            attachment_file_format: Format of the attachment file
            
        Returns:
            List of processed payment advice dictionaries
        """
        logger.info("Processing payment advice with AmazonGroupProcessor")
        
        # Import here to avoid circular imports
        from src.external_apis.llm.client import LLMClient
        
        # Initialize the LLM client
        llm_client = LLMClient()
        
        # Get the prompt template for Amazon
        prompt_text = self.get_prompt_template()
        
        # Prepare full text with email body context if available
        if email_body:
            logger.info("Adding email body as context to document content")
            full_text = f"EMAIL BODY:\n{email_body}\n\nDOCUMENT CONTENT:\n{attachment_text}"
        else:
            full_text = attachment_text
        
        # Log document size information
        doc_size_kb = len(full_text) / 1024
        prompt_size_kb = len(prompt_text) / 1024
        logger.info(f"Document size: {doc_size_kb:.2f} KB, Prompt size: {prompt_size_kb:.2f} KB")
        
        try:
            # Call the LLM API
            logger.info("Calling LLM API for Amazon payment advice extraction")
            llm_result = await llm_client.call_chat_api(
                system_prompt=prompt_text,
                user_content=full_text,
                temperature=0.0,
                timeout=90.0
            )
            
            response_text = llm_result["response_text"]
            logger.info(f"Got LLM response with {len(response_text)} chars")
            
            # Extract JSON from response
            processed_output = self._extract_json_from_response(response_text)
            
            # Apply Amazon-specific post-processing
            processed_output = self.post_process_output(processed_output)
            
            # Return as a list since we might have multiple payment advices
            return processed_output if processed_output else None
            
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"Error processing payment advice ({error_type}): {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return

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
                logger.info(f"Found JSON block in response: {len(json_str)} chars")
                return json.loads(json_str)
            
            # If no JSON block, try to parse the entire response as JSON
            logger.info("No JSON block found, trying to parse entire response")
            return json.loads(response_text)
            
        except Exception as e:
            logger.error(f"Error extracting JSON: {str(e)}")
            logger.error(f"Response text: {response_text[:500]}...")
            # Return empty structure as fallback
            return {"meta_table": {}, "l2_table": []}
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Amazon L2 table to OP table (PaymentAdviceLine format)."""
        try:
            logger.info("Starting Amazon post-processing to transform L2 table to OP table")
            
            # Check if we have the L2 table in the processed output
            if "l2_table" not in processed_output or not processed_output["l2_table"]:
                logger.warning("No L2 table found in processed output, skipping post-processing")
                return processed_output
            
            # Get meta table info
            meta_table = processed_output.get("meta_table", {})
            payment_advice_number = meta_table.get(META_PAYMENT_ADVICE_NUMBER, "")
            payer_name = meta_table.get(META_PAYER_LEGAL_NAME, "")
            payment_advice_date = meta_table.get(META_PAYMENT_ADVICE_DATE, "")
            
            # Initialize paymentadvice_lines list
            paymentadvice_lines = []
            
            # Process each row in the L2 table
            l2_table = processed_output["l2_table"]
            
            logger.info(f"Processing {len(l2_table)} rows from L2 table")
            
            # First, collect all TDS entries to calculate their sum
            tds_entries = []
            total_tds_amount = 0
            
            # Identify and sum all TDS entries
            for row in l2_table:
                invoice_description = row.get("invoice_description", "")
                amount_paid = row.get("amount_paid", 0)
                
                # Skip rows with None/null amount paid
                if amount_paid is None:
                    continue
                    
                # Convert amount_paid to float if it's a string
                if isinstance(amount_paid, str):
                    try:
                        # Remove commas and convert to float
                        amount_paid = float(amount_paid.replace(",", ""))
                    except ValueError:
                        logger.warning(f"Could not convert amount_paid to float: {amount_paid}")
                        amount_paid = 0
                
                # Identify TDS entries
                if invoice_description and "tds" in invoice_description.lower():
                    tds_entries.append(row)
                    total_tds_amount += amount_paid
            
            # Process non-TDS entries
            for row in l2_table:
                invoice_number = row.get("invoice_number")
                invoice_description = row.get("invoice_description", "")
                amount_paid = row.get("amount_paid", 0)
                
                # Skip rows with None/null amount paid
                if amount_paid is None:
                    continue
                    
                # Convert amount_paid to float if it's a string
                if isinstance(amount_paid, str):
                    try:
                        # Remove commas and convert to float
                        amount_paid = float(amount_paid.replace(",", ""))
                    except ValueError:
                        logger.warning(f"Could not convert amount_paid to float: {amount_paid}")
                        amount_paid = 0
                
                abs_amount = abs(amount_paid)
                
                # Skip TDS entries - will handle them separately with aggregated total
                if invoice_description and "tds" in invoice_description.lower():
                    continue
                
                # Default values for all document types
                doc_number = invoice_number if invoice_number else ""
                ref_invoice_no = None  # Always null per requirements
                ref_1 = doc_number
                ref_2 = doc_number
                account_type = "BP"  # Default account type
                
                # Apply different logic based on document type
                
                # 1. BDPO - Identified by "Co-op" in description
                keyword_list = ["co-op"]
                if invoice_description and any(keyword in invoice_description.lower() for keyword in keyword_list):
                    doc_type = "BDPO"
                    ref_1 = doc_number
                    ref_2 = ref_1
                    ref_3 = "BDPO"
                    dr_cr = "Dr"
                    dr_amt = abs_amount
                    cr_amt = 0
                
                # 2. RTV/Credit note - Identified by "RTV" or "VRET" or negative amount not TDS/BDPO
                keyword_list = ["rtv", "vret in credit", "contra"]
                negative_keyword_list = ["tds", "co-op", "bank receipt", "invoice"]
                if invoice_description and any(keyword in invoice_description.lower() for keyword in keyword_list) and not any(keyword in invoice_description.lower() for keyword in negative_keyword_list):
                    doc_type = "Credit Note"
                    ref_1 = doc_number
                    ref_2 = ref_1.split('-')[-1] if "vret" in invoice_description.lower() else ref_1
                    ref_3 = "RTV"
                    dr_cr = "Dr"  # Always Debit per requirements
                    dr_amt = abs_amount
                    cr_amt = 0
                
                # 3. Bank Receipt - Identified by "Bank Receipt" in description
                elif invoice_description and "bank receipt" in invoice_description.lower():
                    doc_type = "Bank Receipt"
                    doc_number = payment_advice_number
                    ref_1 = doc_number
                    ref_2 = ref_1
                    ref_3 = "REC"  # Per requirements
                    dr_cr = "Dr"  # Always Debit per requirements
                    dr_amt = abs_amount
                    cr_amt = 0
                
                # 4. Invoice (default) - Any remaining entries
                else:
                    doc_type = "Invoice"
                    ref_1 = doc_number
                    # Special logic for Ref 2: Value from 'Ref 1' without the prefix
                    if ref_1 and "/" in ref_1:
                        ref_2 = ref_1.split("/", 1)[1]  # Get everything after the first '/'
                    else:
                        ref_2 = ref_1
                    ref_3 = "INV"
                    if amount_paid > 0:
                        dr_cr = "Cr"  # Invoice is Credit per updated requirements
                        dr_amt = 0
                        cr_amt = abs_amount
                    else:
                        dr_cr = "Dr"  # Invoice is Debit per updated requirements
                        dr_amt = abs_amount
                        cr_amt = 0
                
                # Create a paymentadvice_line entry
                line_entry = {
                    "bp_code": None,  # Will be enriched later via SAP
                    "gl_code": None,  # Will be enriched later via SAP
                    "account_type": account_type,
                    "customer": payer_name,
                    "doc_type": doc_type,
                    "doc_number": doc_number,
                    "ref_invoice_no": ref_invoice_no,
                    "ref_1": ref_1,
                    "ref_2": ref_2,
                    "ref_3": ref_3,
                    "amount": abs_amount,  # Always store as positive value
                    "dr_cr": dr_cr,
                    "dr_amt": dr_amt,
                    "cr_amt": cr_amt,
                    "branch_name": "Maharashtra"  # Default branch name
                }
                
                paymentadvice_lines.append(line_entry)
                logger.info(f"Created Amazon OP table entry: {line_entry}")
            
            # Add a single aggregated TDS entry if TDS entries exist
            if tds_entries and total_tds_amount != 0:
                # TDS logic per requirements
                doc_type = "TDS"
                doc_number = payment_advice_number
                ref_invoice_no = None
                ref_1 = doc_number
                ref_2 = ref_1
                ref_3 = "TDS"
                
                # Dr/Cr logic for TDS: If sum is negative then Debit, otherwise Credit
                abs_amount = abs(total_tds_amount)
                if total_tds_amount < 0:
                    dr_cr = "Dr"
                    dr_amt = abs_amount
                    cr_amt = 0
                else:
                    dr_cr = "Cr"
                    dr_amt = 0
                    cr_amt = abs_amount
                
                # Create the aggregated TDS entry
                tds_entry = {
                    "bp_code": None,
                    "gl_code": None,
                    "account_type": "GL",  # Only TDS has GL account type per requirements
                    "customer": payer_name,
                    "doc_type": doc_type,
                    "doc_number": doc_number,
                    "ref_invoice_no": ref_invoice_no,
                    "ref_1": ref_1,
                    "ref_2": ref_2,
                    "ref_3": ref_3,
                    "amount": abs_amount,
                    "dr_cr": dr_cr,
                    "dr_amt": dr_amt,
                    "cr_amt": cr_amt,
                    "branch_name": "Maharashtra"  # Default branch name
                }
                
                paymentadvice_lines.append(tds_entry)
                logger.info(f"Added aggregated TDS entry with total amount {abs_amount} and Dr/Cr {dr_cr}")
            
            # Update processed output to include the new format
            processed_output["paymentadvice_lines"] = paymentadvice_lines
            logger.info(f"Transformed {len(paymentadvice_lines)} rows into paymentadvice_lines format for Amazon")
            
            # Keep empty legacy tables for compatibility with BatchWorkerV1
            if "meta_table" not in processed_output:
                processed_output["meta_table"] = {}
            if "invoice_table" not in processed_output:
                processed_output["invoice_table"] = []
            if "settlement_table" not in processed_output:
                processed_output["settlement_table"] = []
            if "reconciliation_statement" not in processed_output:
                processed_output["reconciliation_statement"] = []
            
            # Create and save PaymentAdviceLine objects to Firestore
            try:
                # Initialize the DAO with the appropriate collection prefix (if test mode is detected)
                collection_prefix = ""
                if processed_output.get("is_test", False):
                    collection_prefix = "dev_"
                    
                dao = FirestoreDAO(collection_prefix=collection_prefix)
                
                # Create and save each payment advice line
                payment_advice_uuid = processed_output.get("payment_advice_uuid")
                if not payment_advice_uuid:
                    payment_advice_uuid = str(uuid4())  # Generate a UUID if not provided
                    logger.info(f"Generated payment_advice_uuid: {payment_advice_uuid}")
                
                # Don't actually save to Firestore here - this will be handled by the calling code
                # Just include the payment_advice_uuid in the output
                processed_output["payment_advice_uuid"] = payment_advice_uuid
            
            except Exception as e:
                logger.error(f"Error preparing Firestore data for Amazon: {str(e)}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
            
            return processed_output
            
        except Exception as e:
            logger.error(f"Error in Amazon post-processing: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return processed_output  # Return original output on error

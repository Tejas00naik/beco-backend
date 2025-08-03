
from src.models.schemas import PaymentAdviceLine
import logging
from typing import Dict, Any
from uuid import uuid4
import re
import json

# Import field name constants
from src.external_apis.llm.constants import (
    META_PAYMENT_ADVICE_DATE, META_PAYMENT_ADVICE_NUMBER,
    META_PAYER_LEGAL_NAME, META_PAYEE_LEGAL_NAME
)
# Import constants for LLM output keys
from src.external_apis.llm.constants import LLM_META_TABLE_KEY, LLM_BODY_TABLE_KEY
from src.services.payment_advice_processor.prompts import ZEPTO_PROMPT
from src.services.payment_advice_processor.group_factory import GroupProcessor
from typing import List

logger = logging.getLogger(__name__)


class ZeptoGroupProcessor(GroupProcessor):
    """Zepto-specific group processor."""
    
    def get_prompt_template(self) -> str:
        """Get the Zepto-specific prompt template."""
        return ZEPTO_PROMPT
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply Zepto-specific post-processing to the LLM output.
        
        Transforms L2 table (LLM output) to OP table (PaymentAdviceLine format) using detailed rules:
        
        - For 'Credit Memo': Map to doc_type='CM', use proper reference fields, amount sign determines Dr/Cr
        - For 'Invoice Payment': Map to doc_type='INV', extract reference numbers properly
        - For 'Bank receipt': Map to doc_type='BR', account_type='GL'
        - For 'AP-AR Adjustment': Map to doc_type='BDPO'
        - For 'TDS': Special handling for TDS amounts and references
        
        The paymentadvice_lines table has these columns:
        BP code, GL code, Account Type (GL/BP), Customer (Legal entity code), Doc type, Doc number,
        Ref invoice no., Ref 1, Ref 2, Ref 3, Amount, Dr/Cr, Dr amt, Cr amt, Branch Name
        """
        try:
            logger.info("Applying Zepto-specific post-processing")
            
            # Check if processed output has expected structure from Zepto LLM prompt
            # Look for both potential formats - capitalized format (old) or snake_case constants (new)
            meta_table_key = LLM_META_TABLE_KEY
            body_table_key = LLM_BODY_TABLE_KEY
            
            if meta_table_key not in processed_output and body_table_key not in processed_output:
                # Try legacy formats
                legacy_keys = ["meta_table", "Meta Table"]
                legacy_body_keys = ["body_table", "Body Table"]
                
                for key in legacy_keys:
                    if key in processed_output:
                        logger.info(f"Using legacy format with key: {key}")
                        meta_table_key = key
                        break
                
                for key in legacy_body_keys:
                    if key in processed_output:
                        logger.info(f"Using legacy format with key: {key}")
                        body_table_key = key
                        break
                        
                if meta_table_key not in processed_output or body_table_key not in processed_output:
                    logger.error(f"No valid table format found in LLM output. Keys present: {list(processed_output.keys())}")
                    return processed_output
            
            # Extract tables using detected keys
            meta_table = processed_output.get(meta_table_key, {})
            body_table = processed_output.get(body_table_key, [])
            
            logger.info(f"Found Meta Table: {meta_table}")
            logger.info(f"Found Body Table with {len(body_table)} rows")
            

            # Extract key information from Meta Table using constants
            # Try both the constant keys and legacy hard-coded keys for backwards compatibility
            settlement_date = (
                meta_table.get(META_PAYMENT_ADVICE_DATE) or 
                meta_table.get("payment_advice_date") or 
                meta_table.get("Settlement Date")
            )
            payment_advice_number = (
                meta_table.get(META_PAYMENT_ADVICE_NUMBER) or 
                meta_table.get("payment_advice_number") or 
                meta_table.get("Payment Advice Number")
            )
            payer_name = (
                meta_table.get(META_PAYER_LEGAL_NAME) or 
                meta_table.get("payer_legal_name") or 
                meta_table.get("Payer's Name")
            )
            payee_name = (
                meta_table.get(META_PAYEE_LEGAL_NAME) or 
                meta_table.get("payee_legal_name") or 
                meta_table.get("Payee's Legal Name")
            )
            
            logger.info(f"Payer: {payer_name}, Payee: {payee_name}, Advice #: {payment_advice_number}")
            
            # Transform body table into paymentadvice_lines format
            paymentadvice_lines = []
            
            # Track TDS amounts for special handling
            tds_invoice_payment_total = 0
            tds_other_total = 0
            
            for row in body_table:
                if not isinstance(row, dict):
                    logger.warning(f"Invalid row in Body Table: {row}")
                    continue
                
                doc_type = row.get("Type of Document")
                doc_number = row.get("Doc No")
                ref_doc = row.get("Ref Doc")
                amount_str = row.get("Amount")
                payment_amt_str = row.get("Payment Amt.")
                tds_str = row.get("TDS")
                
                # Skip if missing critical information
                if not doc_type or not doc_number:
                    logger.warning(f"Skipping row with missing doc_type or doc_number: {row}")
                    continue
                
                # Track TDS amounts for TDS handling
                if tds_str:
                    try:
                        tds_str = tds_str.replace(",", "") if tds_str else "0"
                        tds_amount = float(tds_str)
                        if doc_type.lower() == "invoice payment":
                            tds_invoice_payment_total += tds_amount
                        else:
                            tds_other_total += tds_amount
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing TDS amount '{tds_str}': {e}")
                
                # Parse and format the amount
                try:
                    # Handle both positive and negative amounts with commas
                    amount_str = amount_str.replace(",", "") if amount_str else "0"
                    amount = float(amount_str)
                    
                    payment_amt_str = payment_amt_str.replace(",", "") if payment_amt_str else amount_str
                    payment_amt = float(payment_amt_str)
                    
                    # Always store absolute values in the amount field as per requirements
                    abs_payment_amt = abs(payment_amt)
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing amount '{amount_str}' or payment amount '{payment_amt_str}': {e}")
                    amount = 0
                    payment_amt = 0
                    abs_payment_amt = 0
                
                # Initialize variables for the OP table entry
                mapped_doc_type = ""
                ref_invoice_no = ""
                ref_1 = None
                ref_2 = payment_advice_number
                ref_3 = settlement_date
                account_type = "BP"  # Default for most transactions
                
                # Apply transformation rules based on document type
                if doc_type.lower() == "credit memo":
                    mapped_doc_type = "Credit note"

                    # -------- case 1: ref_doc starts with 'KK' --------
                    if ref_doc and ref_doc.startswith("KK"):
                        # KK‑coded credit note
                        doc_number      = ref_doc                 # OP‑table Doc number
                        ref_invoice_no  = ""                      # blank
                        ref_1           = doc_number              # same KK code
                        ref_2           = doc_number              # same KK code
                        ref_3           = "RTV"

                    # -------- case 2: ref_doc without 'KK' --------
                    else:
                        doc_number      = doc_number              # keep L2 Doc No
                        if ref_doc and "_" in ref_doc:
                            ref_invoice_no = ref_doc.split("_")[0]
                        else:
                            ref_invoice_no = ref_doc or ""
                        ref_1           = doc_number
                        ref_2           = ref_1
                        ref_3           = "RTV"

                    # Dr/Cr logic (same for both cases)
                    if payment_amt < 0:
                        dr_cr = "Dr"; dr_amt = abs_payment_amt; cr_amt = 0
                    else:
                        dr_cr = "Cr"; dr_amt = 0; cr_amt = abs_payment_amt

                        
                elif doc_type.lower() == "invoice payment":
                    mapped_doc_type = "Invoice"  # Per matrix: Invoice
                    ref_invoice_no = ""  # Ref Doc column from L2 table
                    doc_number = ref_doc
                    ref_1 = doc_number  # Doc number from this table itself per matrix
                    
                    # Extract Ref 2: Value from 'Ref 1' without the prefix before '/' per matrix
                    # Example: 'B2BOS24/22468' to '22468'
                    if ref_1 and "/" in ref_1:
                        ref_2 = ref_1.split("/")[1]  # Value after '/' in ref_invoice_no
                    
                    # Set Ref 3 to INV per matrix
                    ref_3 = "INV"
                    
                    # Always set as Credit per matrix for Invoice Payment
                    dr_cr = "Cr"  
                    dr_amt = 0
                    cr_amt = abs_payment_amt
                    
                elif doc_type.lower() == "bank receipt":
                    mapped_doc_type = "Bank receipt"  # Per matrix: Bank receipt
                    ref_invoice_no = ""  # Per matrix: "-"
                    ref_1 = doc_number  # Per matrix: Doc number from this table itself
                    
                    # Set Ref 2 same as Ref 1 per matrix
                    ref_2 = ref_1
                    
                    # Set Ref 3 to REC per matrix
                    ref_3 = "REC"
                    
                    # Always set as Debit per matrix
                    dr_cr = "Dr"  # Always Debit per matrix
                    dr_amt = abs_payment_amt
                    cr_amt = 0
                
                elif doc_type.lower() == "ap-ar adjustment":
                    mapped_doc_type = "BDPO"  # Per matrix: BDPO
                    ref_invoice_no = ref_doc if ref_doc else ""  # Per matrix: Value in 'Ref Doc' from L2 table
                    ref_1 = doc_number  # Per matrix: 'Doc number' from this table itself
                    
                    # Set Ref 2 same as Ref 1 per matrix
                    ref_2 = ref_1
                    
                    # Set Ref 3 to BDPO per matrix
                    ref_3 = "BDPO"
                    
                    # Set Dr/Cr based on amount sign (opposite of typical logic per matrix)
                    if payment_amt < 0:  # Negative amount
                        dr_cr = "Dr"  # Is Debit per matrix when negative
                        dr_amt = abs_payment_amt
                        cr_amt = 0
                    else:  # Positive amount
                        dr_cr = "Cr"  # Is Credit per matrix when positive
                        dr_amt = 0
                        cr_amt = abs_payment_amt
                
                else:
                    # Default handling for other document types
                    mapped_doc_type = doc_type[:3].upper()
                
                # Create a paymentadvice_line entry
                line_entry = {
                    "bp_code": None,  # Will be enriched later via SAP
                    "gl_code": None,  # Will be enriched later via SAP
                    "account_type": account_type,
                    "customer": payer_name,  # Legal entity name
                    "doc_type": mapped_doc_type,
                    "doc_number": doc_number,
                    "ref_invoice_no": ref_invoice_no,
                    "ref_1": ref_1,
                    "ref_2": ref_2,
                    "ref_3": ref_3,
                    "amount": abs_payment_amt,  # Always store as positive value
                    "dr_cr": dr_cr,
                    "dr_amt": dr_amt,
                    "cr_amt": cr_amt,
                    "branch_name": "Maharashtra"
                }
                
                paymentadvice_lines.append(line_entry)
                logger.info(f"Created OP table entry: {line_entry}")
                
            # Add TDS entry if TDS amounts exist
            # From matrix: "Sum (all amounts in TDS columns against type of document 'Invoice Payment') - Sum (all the amounts in TDS columns against other than 'Invoice payment')"
            tds_net = tds_invoice_payment_total - tds_other_total
            if tds_net != 0:
                # Per matrix: TDS special handling
                doc_number = payment_advice_number if payment_advice_number else ""  # Payment advice no. from meta table
                ref_1 = doc_number  # Doc number from this table itself per matrix
                
                tds_entry = {
                    "bp_code": None,
                    "gl_code": None,
                    "account_type": "GL",
                    "customer": payer_name,
                    "doc_type": "TDS",  # Per matrix: TDS
                    "doc_number": doc_number,
                    "ref_invoice_no": "",  # Per matrix: "-"
                    "ref_1": ref_1,
                    "ref_2": ref_1,  # Same as Ref 1 per matrix
                    "ref_3": "TDS",  # Per matrix: TDS
                    "amount": abs(tds_net),  # Always store as positive value
                    # Per matrix: if calculation is positive then Debit, otherwise Credit
                    "dr_cr": "Dr" if tds_net > 0 else "Cr",
                    "dr_amt": abs(tds_net) if tds_net > 0 else 0,
                    "cr_amt": abs(tds_net) if tds_net < 0 else 0,
                    "branch_name": None
                }
                
                paymentadvice_lines.append(tds_entry)
                logger.info(f"Added TDS entry with amount {tds_net}: {tds_entry}")
            
            logger.info(f"Transformed {len(paymentadvice_lines)} rows into paymentadvice_lines format")
            
            # Update processed output to include the new format
            # Keep original tables for reference
            processed_output["paymentadvice_lines"] = paymentadvice_lines
            logger.info(f"Transformed {len(paymentadvice_lines)} rows into paymentadvice_lines format")
            
            # Keep empty legacy tables for compatibility with BatchWorkerV1
            if "meta_table" not in processed_output:
                processed_output["meta_table"] = {}
            if "invoice_table" not in processed_output:
                processed_output["invoice_table"] = []
            if "other_doc_table" not in processed_output:
                processed_output["other_doc_table"] = []
            if "settlement_table" not in processed_output:
                processed_output["settlement_table"] = []
            
            if "reconciliation_statement" not in processed_output:
                processed_output["reconciliation_statement"] = []
            
            # Store paymentadvice_lines in the output
            processed_output["paymentadvice_lines"] = paymentadvice_lines
            
            # Create and save PaymentAdviceLine objects to Firestore
            try:

                # Create and save each payment advice line
                payment_advice_uuid = processed_output.get("payment_advice_uuid")
                if not payment_advice_uuid:
                    payment_advice_uuid = str(uuid4())  # Generate a UUID if not provided
                    logger.info(f"Generated payment_advice_uuid: {payment_advice_uuid}")
                
                for line in paymentadvice_lines:
                    # Create a unique UUID for each payment advice line
                    line_uuid = str(uuid4())
                    
                    # Create PaymentAdviceLine object
                    payment_advice_line = PaymentAdviceLine(
                        payment_advice_line_uuid=line_uuid,
                        payment_advice_uuid=payment_advice_uuid,
                        bp_code=line.get("bp_code"),
                        gl_code=line.get("gl_code"),
                        account_type=line.get("account_type"),
                        customer=line.get("customer"),
                        doc_type=line.get("doc_type"),
                        doc_number=line.get("doc_number"),
                        ref_invoice_no=line.get("ref_invoice_no"),
                        ref_1=line.get("ref_1"),
                        ref_2=line.get("ref_2"),
                        ref_3=line.get("ref_3"),
                        amount=line.get("amount"),
                        dr_cr=line.get("dr_cr"),
                        dr_amt=line.get("dr_amt"),
                        cr_amt=line.get("cr_amt"),
                        branch_name=line.get("branch_name") or "Maharashtra"  # Default to Maharashtra if not set
                    )
                    
                    # Save to Firestore
                    logger.info(f"Saving payment advice line to Firestore: {line_uuid}")
                    # The actual Firestore save needs to happen in an async context
                    processed_output[f"paymentadvice_line_{line_uuid}"] = payment_advice_line
                
                logger.info(f"Prepared {len(paymentadvice_lines)} payment advice lines for Firestore")
                
            except Exception as e:
                logger.error(f"Error saving payment advice lines to Firestore: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
            
            return processed_output
            
        except Exception as e:
            logger.error(f"Error in Zepto post-processing: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return processed_output  # Return original output on error

    async def process_payment_advice(self, attachment_text: str, email_body: str, attachment_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process payment advice using LLM extraction with Zepto-specific logic.
        
        Args:
            attachment_text: Text content of the attachment
            email_body: Email body text for additional context
            attachment_obj: Dictionary with attachment metadata
            
        Returns:
            List of processed payment advice dictionaries
        """
        logger.info("Processing payment advice with ZeptoGroupProcessor")
        
        # Import here to avoid circular imports
        from src.external_apis.llm.client import LLMClient
        import json
        import re
        
        # Initialize the LLM client
        llm_client = LLMClient()
        
        # Get the prompt template for Zepto
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
            logger.info("Calling LLM API for Zepto payment advice extraction")
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
            
            # Apply Zepto-specific post-processing
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
            return {"meta_table": {}, "invoice_table": [], "other_doc_table": [], "settlement_table": []}
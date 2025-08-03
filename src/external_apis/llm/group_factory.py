"""Group-specific factory pattern for LLM extraction and processing."""

from abc import ABC, abstractmethod
import logging
import re
import traceback
from typing import Dict, Any, Optional, List
from uuid import uuid4

from src.models.schemas import PaymentAdviceLine
from src.repositories.firestore_dao import FirestoreDAO
from src.external_apis.llm.constants import (
    META_PAYMENT_ADVICE_NUMBER,
    META_PAYER_LEGAL_NAME, META_PAYEE_LEGAL_NAME,
    META_PAYMENT_ADVICE_DATE, META_PAYMENT_ADVICE_AMOUNT
)
logger = logging.getLogger(__name__)

class GroupProcessor(ABC):
    """Abstract base class for group-specific processing logic."""
    
    @abstractmethod
    def get_prompt_template(self) -> str:
        """Get the group-specific prompt template for LLM extraction."""
        pass
        
    @abstractmethod
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply group-specific post-processing to the LLM output.
        
        Args:
            processed_output: The LLM-processed output
            
        Returns:
            Updated processed output
        """
        pass
    
    @classmethod
    def get_group_name(cls) -> str:
        """Get the name of this group processor."""
        return cls.__name__.replace('GroupProcessor', '')


class DefaultGroupProcessor(GroupProcessor):
    """Default group processor when no specific group is identified."""
    
    def get_prompt_template(self) -> str:
        """Get the default prompt template."""
        
        return f""""""
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Amazon L2 table to OP table (PaymentAdviceLine format)."""
        return

class AmazonGroupProcessor(GroupProcessor):
    """Amazon-specific group processor."""
    
    def get_group_name(self) -> str:
        """Get the name of the group."""
        return "Amazon"
        
    def get_prompt_template(self) -> str:
        """Get the Amazon-specific prompt template."""

        return f"""System: You are an AI assistant that extracts and structures data from Amazon payment advice PDFs.

User: I need to extract key information from an Amazon payment advice PDF in a structured format.

Please extract the following information into a well-structured format:
 - Return one JSON object containing meta_table and l2_table, nothing else.

# META_TABLE FIELDS
 - {META_PAYMENT_ADVICE_DATE} – value after "Payment date:" converted to DD-MM-YYYY
 - {META_PAYMENT_ADVICE_NUMBER} – value after "Payment number:"
 - {META_PAYER_LEGAL_NAME} – entity name that appears before the word "issued" in the first sentence
 - {META_PAYEE_LEGAL_NAME} – value after "Payment made to:" with any codes put at the end of the name should be removed
 - {META_PAYMENT_ADVICE_AMOUNT} – value after "Payment amount:" converted to bare JSON number

# L2_TABLE RULES
 - Wrap-join: if Invoice Number is split by newline or space, join with no separator (e.g. 2640135110↵4458 → 26401351104458).
 - invoice_date: convert MMM format to DD-MM-YYYY; if blank, use null.
 - Ignore leading asterisks in numeric fields.
 - Parentheses indicate negative numbers.
 - Strip commas from numbers and output as bare JSON numbers.
 - Preserve Invoice description text as its is always.
- Discount taken column is often just blanks, do not put values from Amount paid column in it

## SYNTHETIC BANK-RECEIPT ROW (append last)
 - invoice_number = null
 - invoice_date = settlement_date
 - invoice_description = Bank Receipt
 - discount_taken = null
 - amount_paid = negative of header "Payment amount:"
 - amount_remaining = null

# EXAMPLE INPUT TEXT FORMAT
Here's an example of what the payment advice document looks like:

Payment made to:       KWICK LIVING (I) PRIVATE LIMITED(1MC3G)
Our Supplier No:       98499554
Supplier site name:    KWIGLCRPL
Payment number:        340290516
Payment date:          11-JUL-2025
Payment currency:      INR
Payment amount:        719,489.19

Invoice Number:    Invoice Date:    Invoice description             Discount Taken    Amount Paid    Amount Remaining
MH25/252601063    05-JUN-2025     8OXO1R7L/ISK3/##NOT_AVAILABLE                     158,104.90     0.00
MH25/252601063-   05-JUN-2025     India TDS Invoice for AP-194Q                     (133.99)       0.00
TDS-CM-0997
MH25/252601289    11-JUN-2025     2XFB1ZSB/ISK3/##NOT_AVAILABLE                    562,835.46     0.00
MH25/252601289-   11-JUN-2025     India TDS Invoice for AP-194Q                     (476.98)       0.00
TDS-CM-8360
KWIGM-           07-JUL-2025      RTV FCN-KWIGM-30292516672552-                    (65.18)        0.00
30292516672552-                    AMD2-L-1405
AMD2-L-1405
KWIGM-           07-JUL-2025      RTV FCN-KWIGM-30296863171552-                    (775.02)       0.00
30296863171552-                    PAX1-L-189
PAX1-L-189


## Another input example
Payment Made To:
Our Supplier Number:
Supplier Site Name:
Payment Ref. Number:
KWICK LIVING (I) PRIVATE
LIMITED (BECO)
10218
KWIFTCRTPL
45732
Payment Date: May 20, 2025
Payment Currency: INR
Payment Amount: 412,667.52
Invoice Number Invoice Date Invoice Description Amount Paid Discount Taken Amount
Remaining
B2BOS24/22374SCRSCRSCR Aug 10, 2024 30001109196411 23,187.00 0.00 0.00
B2BOS24/22546SCRSCR Aug 20, 2024 30001119082896 9,027.00 0.00 0.00
B2BOS24/23341SCRSCR Sep 21, 2024 30001113141964 4,552.34 0.00 0.00
B2BOS24/23380SCRSCR Sep 23, 2024 77,095.52 0.00 0.00
B2BOS24/24256SCR Oct 26, 2024 30001111821172 285,142.38 0.00 0.00
B2BOS24/24256SCRSC Oct 26, 2024 30001111821172 -155,193.60 0.00 0.00
B2BOS24/24256SCRSCR Oct 26, 2024 30001111821172 155,193.60 0.00 0.00
B2BOS24/24258SCRSCR Oct 27, 2024 30001111777592 4,130.76 0.00 0.00
B2BOS24/24261SCRSCR Oct 27, 2024 30001114737067 9,532.52 0.00 0.00
Total 412,667.52 0.00 0.00


# EXAMPLE OUTPUT FORMAT
Here's the exact format your JSON output should follow:

{{
	"meta_table": {{
		"{META_PAYMENT_ADVICE_DATE}": "11-07-2025",
		"{META_PAYMENT_ADVICE_NUMBER}": "340290516",
		"{META_PAYER_LEGAL_NAME}": "Clicktech Retail Private Limited",
		"{META_PAYEE_LEGAL_NAME}": "KWICK LIVING (I) PRIVATE LIMITED",
        "{META_PAYMENT_ADVICE_AMOUNT}": "719489.19",
	}},
	"l2_table": [
		{{
			"invoice_number": "MH25/252601063",
			"invoice_date": "05-06-2025",
			"invoice_description": "8OXO1R7L/ISK3/##NOT_AVAILABLE",
			"discount_taken": null,
			"amount_paid": 158104.9,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "MH25/252601063-TDS-CM-0997",
			"invoice_date": "05-06-2025",
			"invoice_description": "India TDS Invoice for AP-194Q",
			"discount_taken": null,
			"amount_paid": -133.99,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "MH25/252601289",
			"invoice_date": "11-06-2025",
			"invoice_description": "2XFB1ZSB/ISK3/##NOT_AVAILABLE",
			"discount_taken": null,
			"amount_paid": 562835.46,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "MH25/252601289-TDS-CM-8360",
			"invoice_date": "11-06-2025",
			"invoice_description": "India TDS Invoice for AP-194Q",
			"discount_taken": null,
			"amount_paid": -476.98,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "KWIGM-30292516672552-AMD2-L-1405",
			"invoice_date": "07-07-2025",
			"invoice_description": "RTV FCN-KWIGM-30292516672552-AMD2- L-1405",
			"discount_taken": null,
			"amount_paid": -65.18,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "KWIGM-30296863171552-PAX1-L-189",
			"invoice_date": "07-07-2025",
			"invoice_description": "RTV FCN-KWIGM-30296863171552-PAX1- L-189",
			"discount_taken": null,
			"amount_paid": -775.02,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": null,
			"invoice_date": "11-07-2025",
			"invoice_description": "Bank Receipt",
			"discount_taken": null,
			"amount_paid": -719489.19,
			"amount_remaining": null
		}}
	]
}}

# CONSTRAINTS
 - Do not include markdown, headers, or commentary.
 - Return JSON exactly in the schema above.
 - Asterisks, commas, and parentheses must be cleaned per rules.
 - If any required data is missing, return the error JSON and stop.
- The amounts should retain their '-' negative sign if they are in () brackets
 - Read the first non-zero numeric value in the row (either "Discount Taken" or "Amount Paid").
      – Assign that value to amount_paid.
      – If this rule moves a value out of "Discount Taken", set discount_taken to null.
- If the synthetic Bank-Receipt row does not show the same payment_advice_number in invoice_number, return:
  {{"error":"bank-receipt invoice_number missing or mismatched"}} and stop.
        """
    
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
    
    async def process_excel_attachment(self, attachment: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a HOT Excel file and extract multiple payment advices.
        
        Args:
            attachment: Attachment data dictionary
            
        Returns:
            List of payment advice data dictionaries
        """
        # This is a placeholder - will be implemented to extract data from Excel files
        # For now, return an empty list
        return []


class ZeptoGroupProcessor(GroupProcessor):
    """Zepto-specific group processor."""
    
    def get_prompt_template(self) -> str:
        """Get the Zepto-specific prompt template."""
        # Import constants for LLM output keys
        from src.external_apis.llm.constants import (
            LLM_META_TABLE_KEY, LLM_BODY_TABLE_KEY,
            META_PAYMENT_ADVICE_NUMBER, META_PAYMENT_ADVICE_DATE,
            META_PAYER_LEGAL_NAME, META_PAYEE_LEGAL_NAME
        )
        
        return f"""
    System prompt

    You are an intelligent document processor designed to extract structured data from payment advice PDFs.
    You will:
    1. Extract high-level details into a Meta Table from the header.
    2. Extract line-item table data and normalize invoice numbers in the Ref Doc column.
    Follow all output rules strictly and avoid assumptions or summarization.


    User prompt

    Extract structured data from the provided payment advice PDF. Your output must include two tables:

    Table 1: Meta Table
    Extract the following fields from the header section:

    Settlement Date: Value beside 'Payment Date'
    Payment Advice Number: Value beside 'Payment Doc' (fifth line item in the header). Do not use 'Payment Ref No.'
    Payer's Name: Topmost entity name on the document
    Payee's Legal Name: Value beside 'Payee' (strip internal codes if any)
    Return this as a JSON object with key-value pairs.

    Table 2: Body Table (with Ref Doc Normalization)
    Extract all rows under the section with headers:

    Sr No., Type of Document, Doc No, Ref Doc, Amount, Currency, TDS, Payment Amt.

    IMPORTANT: For each row's "Ref Doc" field:
    - For "Invoice Payment" type documents, extract ONLY the invoice reference number WITHOUT the amount
    - Example: If the PDF shows "B2BOS24/22468 39,012.76", extract ONLY "B2BOS24/22468" as the Ref Doc value
    - The amount should ONLY appear in the "Amount" column
    - When the Ref Doc field spans multiple lines, join them without any separator

    Here are examples of how different row types should be parsed:
    
    Example 1 - Credit Memo:
    Sr No. | Type of Document | Doc No    | Ref Doc      | Amount    | Currency | TDS  | Payment Amt.
    1      | Credit Memo      | 100024216 | KK10009485   | -295,000  | INR      | 0    | -295,000
    
    Example 2 - Credit Memo with split reference:
    Sr No. | Type of Document | Doc No    | Ref Doc      | Amount    | Currency | TDS  | Payment Amt.
    3      | Credit Memo      | 1700032041| B2BOS24/22463| -158.4    | INR      | 0.13 | -158.27
    
    Example 3 - Invoice Payment:
    Sr No. | Type of Document | Doc No    | Ref Doc      | Amount    | Currency | TDS  | Payment Amt.
    7      | Invoice Payment  | 1900165619| B2BOS24/22468| 39,012.76 | INR      | 33.06| 38,979.7

    Output Format:
    Return both tables in structured JSON format.

    The Meta Table should be a single JSON object with key '{LLM_META_TABLE_KEY}' and should contain:
    "{META_PAYMENT_ADVICE_DATE}": Value beside 'Payment Date'
    "{META_PAYMENT_ADVICE_NUMBER}": Value beside 'Payment Doc'
    "{META_PAYER_LEGAL_NAME}": Topmost entity name on the document
    "{META_PAYEE_LEGAL_NAME}": Value beside 'Payee'

    The Body Table should be a JSON array with key '{LLM_BODY_TABLE_KEY}' with row-wise objects. Each row should contain:
    "Sr No."
    "Type of Document"
    "Doc No"
    "Ref Doc"
    "Amount"
    "Currency"
    "TDS"
    "Payment Amt."

    Strict rules:
    Do not return headers and rows separately
    Do not output markdown
    Do not summarize or infer missing fields
    Do not include amounts in the Ref Doc field

    Note: The bank payment which we have received is mentioned in the header. I want you to include that also in the body table as the last entry. Sr.no can be kept blank, Type of Document shall be 'Bank receipt', Doc no. will be the number given against 'Payment Ref No' in the header, Ref doc can be kept blank, Amount shall be the amount mentioned in the header, Currency should be as mentioned in the header, TDs shall be blank and Payment amount shall be equal to the Amount also. Make both these amounts in negative
    """
    
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
            
            # Import constants for LLM output keys
            from src.external_apis.llm.constants import LLM_META_TABLE_KEY, LLM_BODY_TABLE_KEY
            
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
            
            # Import field name constants
            from src.external_apis.llm.constants import (
                META_PAYMENT_ADVICE_DATE, META_PAYMENT_ADVICE_NUMBER,
                META_PAYER_LEGAL_NAME, META_PAYEE_LEGAL_NAME
            )
            
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


class GroupProcessorFactory:
    """Factory class for creating group-specific processors."""
    
    # Map of group UUIDs to processor classes
    _processor_map = {
        # Example group UUID for Amazon (use the actual UUID from your database)
        'group-amazon-12345': AmazonGroupProcessor,
        # New group UUID for Zepto (replace with actual UUID)
        'group-zepto-67890': ZeptoGroupProcessor,
        # HOT Group processor (replace with actual UUID)
        'group-hot-54321': HOTGroupProcessor,
    }
    
    @classmethod
    def get_processor(cls, group_uuid: Optional[str] = None) -> GroupProcessor:
        """
        Get the appropriate processor for the given group UUID.
        
        Args:
            group_uuid: The group UUID to get a processor for
            
        Returns:
            An instance of the appropriate GroupProcessor
        """
        if not group_uuid or group_uuid not in cls._processor_map:
            logger.info(f"Using default group processor (group_uuid={group_uuid})")
            return DefaultGroupProcessor()
        
        processor_class = cls._processor_map[group_uuid]
        logger.info(f"Using {processor_class.__name__} for group_uuid={group_uuid}")
        return processor_class()
    
    @classmethod
    def register_processor(cls, group_uuid: str, processor_class: type) -> None:
        """
        Register a new processor for a group UUID.
        
        Args:
            group_uuid: The group UUID to register the processor for
            processor_class: The processor class to register
        """
        cls._processor_map[group_uuid] = processor_class
        logger.info(f"Registered {processor_class.__name__} for group_uuid={group_uuid}")
    
    @classmethod
    def get_group_uuid_for_name(cls, group_name: str) -> Optional[str]:
        """
        Get the group UUID for a given group name.
        
        Args:
            group_name: The name of the group (case-insensitive)
            
        Returns:
            Group UUID if found, None otherwise
        """
        group_name_lower = group_name.lower()
        
        for group_uuid, processor_class in cls._processor_map.items():
            processor_name = processor_class().get_group_name().lower()
            if group_name_lower in processor_name or processor_name in group_name_lower:
                return group_uuid
        
        return None

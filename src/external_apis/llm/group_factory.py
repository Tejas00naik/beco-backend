"""Group-specific factory pattern for LLM extraction and processing."""

from abc import ABC, abstractmethod
import logging
import re
import traceback
from typing import Dict, Any, Optional, List
from uuid import uuid4

from src.models.schemas import PaymentAdviceLine
from src.repositories.firestore_dao import FirestoreDAO

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
        # Import constants for LLM output keys
        from src.external_apis.llm.constants import (
            LLM_META_TABLE_KEY, LLM_SETTLEMENT_TABLE_KEY, 
            LLM_INVOICE_TABLE_KEY, LLM_RECONCILIATION_STATEMENT_KEY,
            META_SETTLEMENT_DATE, META_PAYMENT_ADVICE_NUMBER,
            META_PAYER_LEGAL_NAME, META_PAYEE_LEGAL_NAME
        )
        
        return f"""
        You are an expert financial analyst. I will provide you with a payment advice document.
        Extract the following information in JSON format:
        
        1. Meta Table: Contains payment advice metadata
           - {META_SETTLEMENT_DATE}: The date when the payment was made (format: DD-MM-YYYY)
           - {META_PAYMENT_ADVICE_NUMBER}: The unique identifier for this payment advice
           - {META_PAYER_LEGAL_NAME}: The full legal name of the entity making the payment
           - {META_PAYEE_LEGAL_NAME}: The full legal name of the entity receiving the payment
           
        2. Settlement Table: List of all settlement entries
           For each settlement, extract:
           - settlement_doc_type: The type of settlement document (BR, BD, TDS, RTV, etc.)
           - settlement_doc_number: The unique identifier for this settlement document
           - settlement_amount: The amount settled in this entry
           
        3. Invoice Table: List of all invoices mentioned
           For each invoice, extract:
           - invoice_number: The unique identifier for this invoice
           - invoice_date: The date of the invoice (format: DD-MM-YYYY)
           - total_invoice_settlement_amount: The total amount being settled for this invoice
           - booking_amount: The total booking amount for this invoice (may be null)
           
        4. Reconciliation Statement: Maps settlements to invoices
           For each entry, extract:
           - settlement_doc_type: The type of settlement document
           - settlement_doc_number: The unique identifier for this settlement document
           - invoice_number: The invoice number this settlement is applied to (may be null)
           - settlement_amount: The amount settled for this invoice (may be null)
           - total_sd_amount: The total settlement document amount
        
        Return your answer as a JSON object with keys: {LLM_META_TABLE_KEY}, {LLM_SETTLEMENT_TABLE_KEY}, {LLM_INVOICE_TABLE_KEY}, {LLM_RECONCILIATION_STATEMENT_KEY}
        """
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """Default post-processing (no modifications)."""
        return processed_output


class AmazonGroupProcessor(GroupProcessor):
    """Amazon-specific group processor."""
    
    def get_prompt_template(self) -> str:
        """Get the Amazon-specific prompt template."""
        return """
        Extract structured data from the payment advice provided to you which i have received from a customer, give the output in 4 distinct tables :
        Table 1. Meta Table
        Table 2. Settlement Table
        Table 3. Invoice Table
        Table 4. Reconciliation Table
        Below is guidance on how to prepare these four tables:
        They will be prepared step by step in the order of table 1 to 2 to 3 to 4.
        Table 1. Meta Table:
        Extract the high-level details about the payment advice, these details will be available in the header section.
        Columns:
        1. Settlement Date: 
        Its the date mentioned against ' Payment date' in header

        2. Payment Advice Number: 
        Unique identifier for that payment advice, mentioned in the header against 'Paymentnumber' 

        3. Payer's Name:
        Name of the entity who is making the payment and has sent this advice (without internal codes).

        4. Payee's Legal Name: 
        Name of the entity receiving the funds, mentioned beside 'Payment made to' in the header (without internal codes).

        Table 2. Settlement Table:
        List all the settlement documents mentioned in the payment advice.
        Columns:
        1. Settlement Document Type:
        The type of document being settled (BR, BD, TDS, CN, DN etc., see detailed guide below)

        2. Settlement Document Number:
        The unique identifier for that settlement document.

        3. Settlement Amount:
        Total amount of the document as applied to one or more invoices

        Detailed guide on Settlement Document Types:
        a. Bank Receipt: 
        - This is found mostly in header Terms like "payment by bank transfer," "UTR," "NEFT," "RTGS."
        - Doc Number: Typically mentioned in the header against ' Payment number' or UTR or reference number.

        b. BDPO (Business Development and Promotional Outlay):
        - These are promotional expenses/adjustments by the payer.
        - Terms like "Co-op," "BDPO," "Business Development," "Marketing," "Promotion," "Allowance," "Co-op Adjustment" will be mentioned.
        - Purpose: Represents marketing incentives or co-op program adjustments.

        c. Tax Deduction at Source (TDS):
        - They are found in the table and  Any of the Terms like "TDS," "Tax Deducted at Source," "TDS.", "Withholding tax", "Deduction" will be mentioned in them.
        - Purpose: Statutory withholding of taxes by the payer.

        d. Credit Note (CN):
        - Terms like "credit note," "credit memo," "CN," etc. will be mentioned.
        - Purpose: Represents credit provided for various reasons (returns, overcharges).

        e. Debit Note (DN):
        - Terms like "debit note," "debit memo," "DN," etc. will be mentioned.
        - Purpose: Represents additional charges by the payer.

        Table 3. Invoice Table:
        List all the invoices mentioned in the payment advice document.
        Columns:
        1. Invoice Number
        This is the unique identifier number for this invoice

        2. Invoice Date
        This is the date of creation of the invoice, format DD-MM-YYYY

        3. Total Invoice Settlement Amount
        This is the total amount of this invoice which is being settled.

        4. Booking Amount 
        This is the amount at which the payer has booked this invoice, you need to only give this if it's explicitly mentioned in the payment advice. Otherwise keep this blank


        Table 4. Reconciliation Statement:
        Based on the data from the Invoice Table, Settlement Table, and details in the payment advice, I expect you to construct a Reconciliation Statement showing how each settlement document maps to one or more invoices along with the amount settled .

        Columns:
        1. Settlement Document Type
        The type of document from Table 2 (BR, BD, TDS, etc.)

        2. Settlement Document Number
        The document number from Table 2

        3. Invoice Number
        The invoice number from Table 3 to which this settlement applies (can be null if not linked to any specific invoice)

        4. Settlement Amount
        The amount of this settlement applied to this specific invoice (can be null)

        5. Total Settlement Document Amount
        The total amount of the settlement document (from Table 2)

        IMPORTANT! Ensure all settlement docs are included in the reconciliation statement. Do not link any settlement docs to invoices unless the exact invoice numbers are mentioned in the document. 


        Output format:
        {
          "meta_table": {
            "settlement_date": "<DD-MM-YYYY>",
            "payment_advice_number": "<string>",
            "payer_legal_name": "<string>",
            "payee_legal_name": "<string>"
          },

          "invoice_table": [
            {
              "invoice_number": "<string>",
              "invoice_date": "<DD-MM-YYYY>",
              "total_invoice_settlement_amount": <number>,
              "booking_amount": <number or null>
            }
          ],

          "settlement_table": [
            {
              "settlement_doc_type": "<string>",
              "settlement_doc_number": "<string>",
              "settlement_amount": <number>
            }
          ],

          "reconciliation_statement": [
            {
              "settlement_doc_type": "<string>",
              "settlement_doc_number": "<string>",
              "invoice_number": "<string or null>",
              "settlement_amount": <number or null>,
              "total_sd_amount": <number>
            }
          ]
        }

        Rules 
        - Mutual-Exclusivity – A document can appear in either invoice_table or settlement_table, never both.
        - BD / BDPO – Always a settlement document (settlement_doc_type = "BD"). Must never enter invoice_table.
        - BR in Header – If a Bank-Receipt (UTR/NEFT/RTGS) appears only in the header, still create a BR row in settlement_table. Do not map its amount to invoices unless those invoice numbers are explicitly shown.
        - Completeness – Each invoice listed must have its total_invoice_settlement_amount equal to the sum of all settlement_amounts allocated to it across all rows in the reconciliation_statement.
        - Numeric Precision – All money fields must be JSON numbers, not strings.
        - No Guesswork – No heuristic or auto-allocation of invoices against settlement doc is permitted.
        - Fail-Safe – If any required data is missing or ambiguous, return
        - BDPO Parsing Rule – If a line shows Co-op/BDPO wording, treat the left-most number as
          settlement_doc_number; never add it to invoice_table.
        - If a BD/BDPO document number equals the numeric string found in the "invoice number" column on the same row, treat that value only as settlement_doc_number.
        - In that case set "invoice_number": null in reconciliation_statement unless other, different invoice numbers are explicitly listed alongside the BDPO line.
        - Never place the BDPO's own doc number in the invoice_number field.
        - Orphan-Doc Inclusion – Every settlement_table entry must also have a reconciliation_statement row. When no invoice is listed, set "invoice_number": null and "settlement_amount": null.
        """
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply Amazon-specific post-processing to the LLM output.
        
        For Amazon payment advices, we need to ensure all invoices referenced in settlements
        exist in the invoice table.
        """
        try:
            # Get tables from the processed output
            invoice_table = processed_output.get("invoice_table", [])
            settlement_table = processed_output.get("settlement_table", [])
            reconciliation_statement = processed_output.get("reconciliation_statement", [])
            
            # Extract all invoice numbers from invoice table
            existing_invoice_numbers = set(
                invoice["invoice_number"] for invoice in invoice_table 
                if invoice.get("invoice_number")
            )
            
            # Extract invoice numbers from reconciliation statement that should be in invoice table
            referenced_invoice_numbers = set()
            for rec in reconciliation_statement:
                invoice_number = rec.get("invoice_number")
                if invoice_number:
                    referenced_invoice_numbers.add(invoice_number)
            
            # Find invoice numbers that are referenced but not in invoice table
            missing_invoice_numbers = referenced_invoice_numbers - existing_invoice_numbers
            
            # For each missing invoice number, extract data from reconciliation and add to invoice table
            for invoice_number in missing_invoice_numbers:
                logger.info(f"Found invoice {invoice_number} in reconciliation but not in invoice table, creating entry")
                
                # Calculate total settlement amount for this invoice from reconciliation
                total_invoice_settlement_amount = 0
                for rec in reconciliation_statement:
                    if rec.get("invoice_number") == invoice_number and rec.get("settlement_amount"):
                        total_invoice_settlement_amount += rec["settlement_amount"]
                
                # Create new invoice entry
                new_invoice = {
                    "invoice_number": invoice_number,
                    "invoice_date": None,  # We don't have this information
                    "total_invoice_settlement_amount": total_invoice_settlement_amount,
                    "booking_amount": None  # We don't have this information
                }
                
                # Add to invoice table
                invoice_table.append(new_invoice)
                logger.info(f"Added missing invoice {invoice_number} to invoice table")
            
            # Update processed output with potentially modified invoice table
            processed_output["invoice_table"] = invoice_table
            
            return processed_output
            
        except Exception as e:
            logger.error(f"Error in Amazon post-processing: {str(e)}")
            return processed_output  # Return original output on error


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

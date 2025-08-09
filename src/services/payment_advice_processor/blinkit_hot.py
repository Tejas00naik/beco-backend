import logging
import pandas as pd
import io
from typing import Dict, Any, List
from uuid import uuid4
from datetime import datetime

# Import base processor
from src.services.payment_advice_processor.base_processor import GroupProcessor
from src.models.schemas import PaymentAdviceLine, PaymentAdvice, PaymentAdviceStatus
from src.config import CLIENT_ID

logger = logging.getLogger(__name__)

class HOTGroupProcessor(GroupProcessor):
    """HandsOnTrade-specific group processor for Excel attachments that contain multiple payment advices."""
    
    def get_prompt_template(self) -> str:
        """Get the HOT-specific prompt template."""
        return ""  # No prompt needed for direct Excel processing
        
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """Process HOT Excel file to extract multiple payment advices."""
        return processed_output

    def _is_fuzzy_match(self, string1: str, string2: str) -> bool:
        """Check if two strings are fuzzy matches."""
        if string1.lower() == string2.lower():
            return True

        set_1 = set(string1.lower().split(" "))
        set_2 = set(string2.lower().split(" "))
        
        return len(set_1.intersection(set_2)) > 2
            
    
    async def process_payment_advice(self, attachment_text: str, email_body: str, attachment_obj: Dict[str, Any], attachment_file_format: str) -> List[Dict[str, Any]]:
        """
        Process payment advice for HOT with Excel attachment handling.
        
        Args:
            attachment_text: Text content of the attachment
            email_body: Email body text for additional context
            attachment_obj: Dictionary with attachment metadata
            attachment_file_format: Format of the attachment file
            
        Returns:
            List of processed payment advice dictionaries
        """
        logger.info("Processing payment advice with HOTGroupProcessor")
        filename = attachment_obj.get('filename', '').lower() if attachment_obj else ''

        # Check if this is an Excel file by file format or extension
        is_excel = False
        if attachment_file_format:
            is_excel = ('excel' in attachment_file_format.lower() or 
                      'spreadsheet' in attachment_file_format.lower() or
                      'xlsx' in attachment_file_format.lower())
        if not is_excel and filename:
            is_excel = filename.endswith(('.xlsx', '.xls'))
        
        # Debug logging
        logger.info(f"Filename: {filename}")
        logger.info(f"Attachment file format: {attachment_file_format}")
        logger.info(f"Is Excel based on type detection: {is_excel}")
        logger.info(f"Has attachment object: {attachment_obj is not None}")
        
        # Check for binary data in both 'content' and 'data' keys (email processor uses 'content')
        binary_data = attachment_obj.get('content') if attachment_obj else None
        logger.info(f"Has binary data in attachment: {bool(binary_data)}")
        logger.info(f"Binary data found in key: {'content' if attachment_obj and 'content' in attachment_obj else 'data' if attachment_obj and 'data' in attachment_obj else 'none'}")
        
        # Process Excel if appropriate
        if is_excel and attachment_obj and binary_data:
            logger.info("Processing HOT Excel attachment")
            try:
                # Load Excel data into DataFrame - check both 'content' and 'data' keys
                excel_binary = attachment_obj.get('content')
                
                # Validate that we have binary data before processing
                if not excel_binary:
                    logger.warning("No binary data found in the attachment (checked both 'content' and 'data' keys)")
                    return []
                    
                # Try to read the Excel file
                logger.info(f"Attempting to read Excel with size: {len(excel_binary)} bytes")
                with io.BytesIO(excel_binary) as excel_io:
                    excel_df = pd.read_excel(excel_io, engine='openpyxl')
                    # Log column names to help with debugging
                    logger.info(f"Excel columns found: {list(excel_df.columns)}")
               
                
                if excel_df.empty:
                    logger.warning("Excel file was empty")
                    return []
                
                # Initialize list for payment advices
                payment_advices = []
                
                # Check if the DataFrame has any of the expected columns
                expected_columns = ['invoice_id', 'payment_date', 'utr_number', 'payment_amount']
                columns_present = [col for col in expected_columns if col in excel_df.columns]
                logger.info(f"Expected columns present: {columns_present} out of {expected_columns}")
                
                if not columns_present:
                    logger.warning("Excel file doesn't contain any of the expected columns for HOT format")
                    return []
                
                # Process each row to create payment advices
                logger.info(f"Processing {len(excel_df)} rows in Excel file")
                for idx, row in excel_df.iterrows():   
                    # if row['vendor_name'] != CLIENT_ID:               
                    #     continue
                    if not self._is_fuzzy_match(row['vendor_name'], CLIENT_ID):
                        continue

                    # Print row data for debugging
                    logger.info(f"Processing row {idx} with data: {row.to_dict()}")
                    
                    # Handle any NaN/None values safely with dict comprehension
                    row_dict = {k: ('' if pd.isna(v) else v) for k, v in row.to_dict().items()}

                                        
                    payment_advice = {}

                    payment_advice["meta_table"] = {
                        "payment_advice_number": str(uuid4()),
                        # payment_date format 2025-06-27 00:00:00+00:00
                        # rror processing Excel: 'str' object has no attribute 'strftime'
                        # 2025-08-07 01:45:44,757 - src.services.payment_advice_processor.blinkit_hot - ERROR - Traceback: Traceback (most recent call last):
                        #   File "/Users/macbookpro/RECOCENT/beco-backend/src/services/payment_advice_processor/blinkit_hot.py", line 128, in process_payment_advice
                        #     "payment_advice_date": row_dict.get('payment_date', '').strftime('%Y-%m-%d'),
                        #                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                        # AttributeError: 'str' object has no attribute 'strftime'


                        "payment_advice_date": row_dict.get('payment_date', ''),
                        "payer_legal_name": row_dict.get('buyer_name', ''),
                        "payee_legal_name": CLIENT_ID
                    }

                    # Create the payment advice lines according to specified rules
                    lines = []
                    invoice_id = str(row_dict.get('invoice_id', ''))
                    line_number = 1
                    
                    # Convert numeric values safely
                    def safe_float(value, default=0.0):
                        try:
                            if pd.isna(value) or value is None or value == '':
                                return default
                            return float(value)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert '{value}' to float, using {default}")
                            return default
                    
                    # 1. RTV/Credit Note line (only if total_dn_amount > 0)
                    total_dn_amount = safe_float(row_dict.get('total_dn_amount', 0.0))
                    logger.info(f"Credit note check - total_dn_amount: {total_dn_amount}")
                    if total_dn_amount > 0:
                        credit_note_uuid = str(uuid4())
                        credit_note_line = PaymentAdviceLine(
                            payment_advice_line_uuid=credit_note_uuid,
                            payment_advice_uuid="",
                            email_log_uuid="",
                            bp_code=None,
                            gl_code=None,
                            account_type="BP",
                            doc_type="Credit Note",
                            doc_number=invoice_id,
                            ref_invoice_no=invoice_id,
                            ref_1=invoice_id,  # Doc number from this table
                            ref_2=invoice_id,   # Same as Ref 1
                            ref_3="RTV",
                            amount=total_dn_amount,
                            dr_cr="Dr",
                            dr_amt=total_dn_amount,
                            cr_amt=0.0,
                            branch_name="Maharashtra",
                            sap_enrichment_status="Not Enriched",
                            created_at=datetime.now().isoformat(),
                            updated_at=datetime.now().isoformat()
                        )

                        lines.append(credit_note_line.to_dict())
                        line_number += 1
                    
                    # 2. TDS line (only if tds_amount > 0)
                    tds_amount = safe_float(row_dict.get('tds_amount', 0.0))
                    logger.info(f"TDS check - tds_amount: {tds_amount}")
                    if tds_amount > 0:
                        tds_uuid = str(uuid4())
                        tds_line = PaymentAdviceLine(
                            payment_advice_line_uuid=tds_uuid,
                            payment_advice_uuid="",
                            email_log_uuid="",
                            bp_code=None,
                            gl_code=None,
                            account_type="GL",                 # <-- TDS is GL
                            doc_type="TDS",
                            doc_number=invoice_id,             # from L2
                            ref_invoice_no=invoice_id,         # from L2
                            ref_1=invoice_id,                  # Doc number from this table
                            ref_2=invoice_id,                  # Same as Ref 1
                            ref_3="TDS",
                            amount=tds_amount,
                            dr_cr="Dr",
                            dr_amt=tds_amount,
                            cr_amt=0.0,
                            branch_name="Maharashtra",
                            sap_enrichment_status="Not Enriched",
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        lines.append(tds_line.to_dict())
                        line_number += 1

                
                    # 3. Invoice line (always included)
                    invoice_uuid = str(uuid4())

                    # pick invoice_idr if present, else fallback to invoice_id
                    invoice_idr = str(row_dict.get('invoice_idr', invoice_id)) or invoice_id

                    # Calculate invoice amount: after_tax + total_grn_amount - total_grn_difference
                    invoice_amount_after_tax = safe_float(row_dict.get('invoice_amount_after_tax', 0.0))
                    total_dn_amount = safe_float(row_dict.get('total_dn_amount', 0.0))
                    total_grn_difference = safe_float(row_dict.get('total_grn_difference', 0.0))
                    invoice_amount = invoice_amount_after_tax + total_dn_amount - total_grn_difference
                    logger.info(f"Invoice amount calculation: {invoice_amount_after_tax} + {total_dn_amount} - {total_grn_difference} = {invoice_amount}")
                    #clamp final inv amount to 2 decimal places
                    invoice_amount = round(invoice_amount, 2)

                    # Ref1: only the numbers/part after '/'
                    ref1 = invoice_idr.split('/')[-1] if '/' in invoice_idr else invoice_idr

                    invoice_line = PaymentAdviceLine(
                        payment_advice_line_uuid=invoice_uuid,
                        payment_advice_uuid="",
                        email_log_uuid="",
                        bp_code=None,
                        gl_code=None,
                        account_type="BP",                 # <-- Invoice is BP
                        doc_type="Invoice",
                        doc_number=invoice_idr,            # from 'invoice_idr' (or fallback)
                        ref_invoice_no="",
                        ref_1=ref1,                 # Only part after '/'
                        ref_2=invoice_idr,                        # Doc number from this table
                        ref_3="INV",
                        amount=invoice_amount,
                        dr_cr="Cr",
                        dr_amt=0.0,
                        cr_amt=invoice_amount,
                        branch_name="Maharashtra",
                        sap_enrichment_status="Not Enriched",
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    lines.append(invoice_line.to_dict())
                    line_number += 1

                    # 4. Bank Receipts line (always included)
                    bank_receipt_uuid = str(uuid4())
                    utr_number = str(row_dict.get('utr_number', ''))
                    payment_amount = safe_float(row_dict.get('payment_amount', 0.0))
                    logger.info(f"Bank receipt - utr_number: {utr_number}, payment_amount: {payment_amount}")

                    bank_receipt_line = PaymentAdviceLine(
                        payment_advice_line_uuid=bank_receipt_uuid,
                        payment_advice_uuid="",
                        email_log_uuid="",
                        bp_code=None,
                        gl_code=None,
                        account_type="BP",                 # <-- Bank Receipt is BP
                        doc_type="Bank receipt",
                        doc_number=utr_number,             # from 'utr_number'
                        ref_invoice_no="",
                        ref_1=utr_number,                  # Doc number from this table
                        ref_2=utr_number,                  # Same as Ref 1
                        ref_3="REC",
                        amount=payment_amount,
                        dr_cr="Dr",
                        dr_amt=payment_amount,
                        cr_amt=0.0,
                        branch_name="Maharashtra",
                        sap_enrichment_status="Not Enriched",
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    lines.append(bank_receipt_line.to_dict())
                    line_number += 1
                    payment_advice["paymentadvice_lines"] = lines
                    payment_advices.append(payment_advice)
                    
                logger.info(f"Processed {len(payment_advices)} payment advices from Excel data")
                # Return the list of payment advice dicts directly
                # Each dict represents a single payment advice (one row from Excel)
                # Each dict has paymentadvice_lines key containing a list of 4 items
                return payment_advices
                
            except Exception as e:
                logger.error(f"Error processing Excel: {str(e)}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                return []
        # If we get here, it means either:
        # 1. This is not a valid Excel file
        # 2. Excel processing failed
        # 3. The attachment doesn't have the expected HOT format
        # In all cases, return an empty list as HOT processor only handles specific Excel files
        logger.info("Not a valid HOT Excel file or Excel processing failed")
        return []

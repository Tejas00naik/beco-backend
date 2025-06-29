"""
Mock LLM Extractor

This module simulates an LLM-based document extractor that parses emails
to identify and extract payment advice information, including invoices, 
credit notes, and settlement details.
"""

import logging
import random
import copy
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple, Callable
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)


class MockLLMExtractor:
    """
    Mock implementation of an LLM-based document extractor.
    
    In a real implementation, this would use an actual large language model,
    but for the proof of concept we simply return the pre-generated mock data.
    """
    
    def __init__(self):
        """Initialize the mock LLM extractor."""
        logger.info("Initialized MockLLMExtractor")
        
        # Group-specific prompt and post-processing mapping
        self.group_processors = {
            # Amazon group (default for this PoC)
            'group-amazon-12345': {
                'prompt': self._get_amazon_prompt,
                'post_process': self._post_process_amazon_output
            },
            # Default fallback processor
            'default': {
                'prompt': self._get_default_prompt,
                'post_process': lambda output: output  # No post-processing for default
            }
        }
    
    def extract_email_metadata(self, email_content: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metadata from an email.
        
        Args:
            email_content: Mock email content with pre-generated fields
            
        Returns:
            Dict containing extracted metadata
        """
        logger.info(f"Extracting metadata from email {email_content.get('email_id')}")
        
        # In a real implementation, the LLM would analyze the email content
        # and extract these fields. For our mock, we'll simply use the 
        # In the new schema, group_uuid is associated with email_log
        # The LLM would determine this based on the legal entity mentioned in the email
        # For mock, we'll use legal_entity_uuid to look up a related group_uuid
        
        # Convert single group_uuid to list if present, or use group_uuids if available, or empty list if neither exists
        group_uuids = []
        if "group_uuids" in email_content:
            group_uuids = email_content.get("group_uuids")
        elif "group_uuid" in email_content and email_content.get("group_uuid"):
            group_uuids = [email_content.get("group_uuid")]
        
        metadata = {
            "sender_mail": email_content.get("sender_mail"),
            "original_sender_mail": email_content.get("original_sender_mail"),
            "email_subject": email_content.get("subject", ""),
            "group_uuids": group_uuids  # Updated: list of group_uuids at email level
        }
        
        logger.info(f"Extracted metadata for email {email_content.get('email_id')}")
        return metadata
    
    def extract_payment_advices(self, email_content: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract payment advices from email content.
        
        Args:
            email_content: Mock email content with pre-generated payment advices
            
        Returns:
            List of extracted payment advice dictionaries
        """
        logger.info(f"Extracting payment advices from email {email_content.get('email_id')}")
        
        # In a real implementation, the LLM would parse the email text and extract
        # structured payment advice information. For our mock, we'll use the 
        # pre-generated data.
        
        # Return just the payment advice details without the nested invoices/other_docs
        # Those will be extracted separately
        payment_advices = []
        for advice in email_content.get("payment_advices", []):
            payment_advice = {
                "payment_advice_number": advice.get("payment_advice_number"),
                "payment_advice_date": advice.get("payment_advice_date"),
                "payment_advice_amount": advice.get("payment_advice_amount"),
                "payment_advice_status": "new",  # Default status for newly extracted advices
                "payer_name": advice.get("payer_name"),
                "payee_name": advice.get("payee_name"),
                "legal_entity_uuid": advice.get("legal_entity_uuid")  # New: legal_entity_uuid per payment advice
            }
            payment_advices.append(payment_advice)
        
        logger.info(f"Extracted {len(payment_advices)} payment advices from email {email_content.get('email_id')}")
        return payment_advices
    
    def extract_transaction_details(self, 
                                    email_content: Dict[str, Any], 
                                    payment_advice_idx: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract invoice, other document, and settlement details for a specific payment advice.
        
        Args:
            email_content: Mock email content with pre-generated details
            payment_advice_idx: Index of the payment advice to extract details for
            
        Returns:
            Tuple of (invoices, other_docs, settlements)
        """
        if not email_content.get("payment_advices") or payment_advice_idx >= len(email_content["payment_advices"]):
            logger.warning(f"Payment advice index {payment_advice_idx} out of range")
            return [], [], []
            
        payment_advice = email_content["payment_advices"][payment_advice_idx]
        payment_advice_id = payment_advice.get("payment_advice_number", f"PA-{payment_advice_idx}")
        
        logger.info(f"Extracting transaction details for payment advice {payment_advice_id}")
        
        # Extract invoices with customer_uuid for each invoice
        invoices = []
        for inv in payment_advice.get("invoices", []):
            invoice = {
                "invoice_number": inv.get("invoice_number"),
                "invoice_date": inv.get("invoice_date"),
                "booking_amount": inv.get("booking_amount"),
                "invoice_status": "open",  # Default status for new invoices
                "customer_uuid": inv.get("customer_uuid")  # New: customer_uuid per invoice
            }
            invoices.append(invoice)
        
        # Extract other documents (credit notes, etc.)
        other_docs = []
        for doc in payment_advice.get("other_docs", []):
            other_doc = {
                "other_doc_number": doc.get("other_doc_number"),
                "other_doc_date": doc.get("other_doc_date"),
                "other_doc_type": doc.get("other_doc_type"),
                "other_doc_amount": doc.get("other_doc_amount"),
                "customer_uuid": doc.get("customer_uuid")  # New: customer_uuid per other_doc
            }
            other_docs.append(other_doc)
        
        # Generate settlements
        settlements = []
        
        # Create settlement for each invoice, with customer_uuid from invoice
        for inv_idx, inv in enumerate(invoices):
            settlement = {
                "invoice_uuid": None,  # This will be set later when we know the invoice UUID
                "other_doc_uuid": None,
                "settlement_date": payment_advice.get("payment_advice_date"),
                "settlement_amount": inv.get("booking_amount"),
                "settlement_status": "ready",
                "customer_uuid": inv.get("customer_uuid")  # New: customer_uuid from invoice
            }
            settlements.append(settlement)
        
        # Create settlement for each other document, with customer_uuid from other_doc
        for doc_idx, doc in enumerate(other_docs):
            settlement = {
                "invoice_uuid": None,
                "other_doc_uuid": None,  # This will be set later when we know the other_doc UUID
                "settlement_date": payment_advice.get("payment_advice_date"),
                "settlement_amount": doc.get("other_doc_amount"),
                "settlement_status": "ready",
                "customer_uuid": doc.get("customer_uuid")  # New: customer_uuid from other_doc
            }
            settlements.append(settlement)
        
        logger.info(f"Extracted {len(invoices)} invoices, {len(other_docs)} other docs, "
                   f"and {len(settlements)} settlements for payment advice {payment_advice_id}")
        
        return invoices, other_docs, settlements
    
    def process_attachment_for_payment_advice(self, email_text_content: str, attachment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single attachment as a payment advice.
        
        In a real implementation, this would pass the email text content and attachment data to a LLM
        service, which would extract the payment advice details. In this mock implementation, we generate
        standardized output based on the attachment filename and some mock data.
        
        Args:
            email_text_content: Text content of the email
            attachment_data: Dictionary containing attachment filename and other metadata
            
        Returns:
            Dictionary with standardized LLM output format containing metaTable, invoiceTable, 
            otherDocTable, and settlementTable
        """
        # Log the processing
        filename = attachment_data.get('filename', 'unknown_file')
        logger.info(f"Processing attachment '{filename}' as payment advice")
        logger.info(f"LLM Extraction: Email content length: {len(email_text_content)} chars")
        logger.info(f"LLM Extraction: Attachment type: {attachment_data.get('content_type', 'unknown')}")
        logger.info(f"LLM Extraction: Attachment size: {len(attachment_data.get('content', b''))} bytes")
        
        # For demonstration, use a fixed group_uuid (in real implementation, get from legal entity lookup)
        group_uuid = 'group-amazon-12345'
        logger.info(f"LLM Extraction: Using group_uuid: {group_uuid} for processing")
        
        # Select the appropriate prompt and post-processing based on group_uuid
        group_processor = self.group_processors.get(group_uuid, self.group_processors['default'])
        
        # Generate the raw LLM output using the appropriate prompt
        output = group_processor['prompt'](filename)
        
        # Apply group-specific post-processing
        processed_output = group_processor['post_process'](output)
        
        # Log the results in detail
        meta_table = processed_output.get('metaTable', {})
        logger.info(f"LLM Extraction results - Meta Table:")
        logger.info(f"  paymentAdviceNumber: {meta_table.get('paymentAdviceNumber')}")
        logger.info(f"  paymentAdviceDate: {meta_table.get('paymentAdviceDate')}")
        logger.info(f"  paymentAdviceAmount: {meta_table.get('paymentAdviceAmount')}")
        logger.info(f"  payersLegalName: {meta_table.get('payersLegalName')}")
        logger.info(f"  payeesLegalName: {meta_table.get('payeesLegalName')}")
        
        invoice_table = processed_output.get('invoiceTable', [])
        logger.info(f"LLM Extraction results - Invoice Table: {len(invoice_table)} items")
        for i, invoice in enumerate(invoice_table[:3]):  # Log first 3 invoices
            logger.info(f"  Invoice {i+1}: {invoice.get('invoiceNumber')} - Amount: {invoice.get('totalSettlementAmount')}")
        if len(invoice_table) > 3:
            logger.info(f"  ... and {len(invoice_table) - 3} more invoices")
        
        other_doc_table = processed_output.get('otherDocTable', [])
        logger.info(f"LLM Extraction results - Other Doc Table: {len(other_doc_table)} items")
        for i, doc in enumerate(other_doc_table[:3]):  # Log first 3 other docs
            logger.info(f"  Other Doc {i+1}: {doc.get('otherDocNumber')} ({doc.get('otherDocType')}) - Amount: {doc.get('otherDocAmount')}")
        if len(other_doc_table) > 3:
            logger.info(f"  ... and {len(other_doc_table) - 3} more other docs")
        
        settlement_table = processed_output.get('settlementTable', [])
        logger.info(f"LLM Extraction results - Settlement Table: {len(settlement_table)} items")
        for i, settlement in enumerate(settlement_table[:3]):  # Log first 3 settlements
            logger.info(f"  Settlement {i+1}: {settlement.get('invoiceNumber')} -> {settlement.get('settlementDocNumber')} - Amount: {settlement.get('settlementAmount')}")
        if len(settlement_table) > 3:
            logger.info(f"  ... and {len(settlement_table) - 3} more settlements")
            
        logger.info(f"Generated payment advice data for attachment '{filename}' with "
                  f"{len(processed_output['invoiceTable'])} invoices, "
                  f"{len(processed_output['otherDocTable'])} other docs, and "
                  f"{len(processed_output['settlementTable'])} settlements")
        
        return processed_output
    
    def _get_default_prompt(self, filename: str) -> Dict[str, Any]:
        """Default prompt generation for any group."""
        # Use fixed date and payment advice numbers for complete determinism
        fixed_date = datetime(2025, 6, 1)
        date_str = "01-JUN-2025"
        
        # Deterministic payment advice number based on file_hash
        file_hash = hash(filename) % 5
        advice_number = f"PA-{100000 + file_hash}"
        
        # Default mock data with fixed document numbers from CSV-based SAP mock data
        invoice_numbers = ["B2B2526/1112344", "B2B2526/1112417"]
        other_doc_numbers = ["337027143", "TDS-CM-4208"]
        
        # Fixed amounts from CSV mock data
        invoice_amount1 = 762928.0  # B2B2526/1112344 amount
        invoice_amount2 = 742084.0  # B2B2526/1112417 amount
            
        # Fixed TDS and BR amounts
        br_amount = 3547260.0  # 337027143 amount
        tds_amount = 4.0  # TDS-CM-4208 amount
        
        return {
            "metaTable": {
                "paymentAdviceDate": date_str,
                "paymentAdviceNumber": advice_number,
                "payersLegalName": "Amazon Clicktech Services Private Limited",
                "payeesLegalName": "Beco Trading Ltd"
            },
            "invoiceTable": [
                {
                    "invoiceNumber": invoice_numbers[0],
                    "invoiceDate": "01-MAY-2025",  # Fixed date
                    "bookingAmount": invoice_amount1,
                    "totalSettlementAmount": invoice_amount1
                },
                {
                    "invoiceNumber": invoice_numbers[1],
                    "invoiceDate": "15-MAY-2025",  # Fixed date
                    "bookingAmount": invoice_amount2,
                    "totalSettlementAmount": invoice_amount2
                }
            ],
            "otherDocTable": [
                {
                    "otherDocType": "BR",
                    "otherDocNumber": other_doc_numbers[0],
                    "otherDocAmount": -br_amount
                },
                {
                    "otherDocType": "TDS",
                    "otherDocNumber": other_doc_numbers[1],
                    "otherDocAmount": -tds_amount
                }
            ],
            "settlementTable": [
                {
                    "settlementDocNumber": other_doc_numbers[0],
                    "invoiceNumber": invoice_numbers[0],
                    "settlementAmount": -br_amount
                },
                {
                    "settlementDocNumber": other_doc_numbers[1],
                    "invoiceNumber": invoice_numbers[1],
                    "settlementAmount": -tds_amount
                }
            ]
        }
    
    def _get_amazon_prompt(self, filename: str) -> Dict[str, Any]:
        """Amazon-specific prompt generation."""
        # For now, we'll use the same base data as default but with some modifications
        # In a real implementation, this would contain Amazon-specific prompt templates
        base_output = self._get_default_prompt(filename)
        
        # Intentionally create a scenario where an invoice is missing from invoiceTable but present in settlementTable
        # This tests our post-processing logic
        # Using CSV SAP mock data document numbers
        missing_invoice_number = "B2B2526/1112484"  # A missing invoice that will be in settlements
        missing_invoice_amount = 953938.0
        
        # Add a settlement referencing the missing invoice
        base_output["settlementTable"].append({
            "settlementDocNumber": "TDS-CM-9522",
            "invoiceNumber": missing_invoice_number,
            "settlementAmount": -61.0  # TDS amount from CSV
        })
        
        # Add another settlement for the same missing invoice to test summing logic
        base_output["settlementTable"].append({
            "settlementDocNumber": "TDS-CM-1539",
            "invoiceNumber": missing_invoice_number,
            "settlementAmount": -982.0  # TDS amount from CSV
        })
        
        # Add the other docs for these settlements
        base_output["otherDocTable"].append({
            "otherDocType": "TDS",
            "otherDocNumber": "TDS-CM-9522",
            "otherDocAmount": -61.0
        })
        
        base_output["otherDocTable"].append({
            "otherDocType": "TDS",
            "otherDocNumber": "TDS-CM-1539",
            "otherDocAmount": -982.0
        })
        
        return base_output
    
    def _post_process_amazon_output(self, output: Dict[str, Any]) -> Dict[str, Any]:
        """Amazon-specific post-processing logic.
        
        For Amazon group, we need to:
        1. Check if any invoice numbers in settlementTable are missing from invoiceTable
        2. Add those missing invoices to invoiceTable
        3. If a missing invoice appears multiple times, sum the amounts
        """
        logger.info("Applying Amazon-specific post-processing")
        
        # Create a deep copy to avoid modifying the original
        processed_output = copy.deepcopy(output)
        
        # Log before post-processing counts
        invoice_count = len(processed_output.get("invoiceTable", []))
        settlement_count = len(processed_output.get("settlementTable", []))
        logger.info(f"Amazon post-processing: Before - {invoice_count} invoices, {settlement_count} settlements")
        
        # Build a set of invoice numbers already in the invoiceTable
        existing_invoice_numbers = set(invoice["invoiceNumber"] for invoice in processed_output.get("invoiceTable", []))
        logger.info(f"Amazon post-processing: Existing invoice numbers: {existing_invoice_numbers}")
        
        # Collect missing invoice numbers and their settlement amounts
        missing_invoices = defaultdict(list)
        for settlement in processed_output.get("settlementTable", []):
            invoice_number = settlement.get("invoiceNumber")
            if invoice_number and invoice_number not in existing_invoice_numbers:
                missing_invoices[invoice_number].append(settlement)
        
        if missing_invoices:
            logger.info(f"Amazon post-processing: Found {len(missing_invoices)} missing invoice numbers in settlements: {list(missing_invoices.keys())}")
        else:
            logger.info("Amazon post-processing: No missing invoices found")
        
        # If there are missing invoices, add them to the invoiceTable
        for invoice_number, settlements in missing_invoices.items():
            # Calculate the total settlement amount (absolute value of the sum of settlement amounts)
            settlement_amounts = [s.get("settlementAmount", 0) for s in settlements]
            total_amount = sum(abs(float(amount)) for amount in settlement_amounts)
            
            logger.info(f"Amazon post-processing: For missing invoice {invoice_number}:")
            logger.info(f"  Found {len(settlements)} matching settlements with amounts: {settlement_amounts}")
            logger.info(f"  Total calculated settlement amount: {total_amount}")
            
            # Create a new invoice entry
            invoice_entry = {
                "invoiceNumber": invoice_number,
                "invoiceDate": None,
                "bookingAmount": None,
                "totalSettlementAmount": total_amount
            }
            
            # Add to the invoiceTable
            processed_output["invoiceTable"].append(invoice_entry)
            logger.info(f"Amazon post-processing: Added missing invoice {invoice_number} to invoiceTable with amount {total_amount}")
        
        # Log after post-processing counts
        invoice_count_after = len(processed_output.get("invoiceTable", []))
        logger.info(f"Amazon post-processing: After - {invoice_count_after} invoices, {settlement_count} settlements")
        logger.info(f"Amazon post-processing: Added {invoice_count_after - invoice_count} new invoice records")
        
        return processed_output

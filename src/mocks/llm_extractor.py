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
        
        # For demonstration, use a fixed group_uuid (in real implementation, get from legal entity lookup)
        group_uuid = 'group-amazon-12345'
        
        # Select the appropriate prompt and post-processing based on group_uuid
        group_processor = self.group_processors.get(group_uuid, self.group_processors['default'])
        
        # Generate the raw LLM output using the appropriate prompt
        output = group_processor['prompt'](filename)
        
        # Apply group-specific post-processing
        processed_output = group_processor['post_process'](output)
        
        # Log the results
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
        
        # Default mock data with fixed document numbers that match SAP mock data
        invoice_numbers = ["INV-1234", "INV-5678"]
        other_doc_numbers = ["BDPO-12345", "TDS-CM-1234"]
        
        # Fixed amounts for complete determinism
        invoice_amount1 = 35000.00
        invoice_amount2 = 42000.00
            
        # Fixed TDS and BDPO amounts (10% of invoice amounts)
        tds_amount = round(invoice_amount2 * 0.10, 2)  # 4200.00
        bdpo_amount = round(invoice_amount1 * 0.10, 2)  # 3500.00
        
        return {
            "metaTable": {
                "paymentAdviceDate": date_str,
                "paymentAdviceNumber": advice_number,
                "payersLegalName": "Clicktech Retail Private Limited",
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
                    "otherDocType": "BDPO",
                    "otherDocNumber": other_doc_numbers[0],
                    "otherDocAmount": -bdpo_amount
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
                    "settlementAmount": -bdpo_amount
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
        missing_invoice_number = "INV-9999"  # A missing invoice that will be in settlements
        missing_invoice_amount = 25000.00
        
        # Add a settlement referencing the missing invoice
        base_output["settlementTable"].append({
            "settlementDocNumber": "TDS-CM-9999",
            "invoiceNumber": missing_invoice_number,
            "settlementAmount": -2500.00  # 10% TDS
        })
        
        # Add another settlement for the same missing invoice to test summing logic
        base_output["settlementTable"].append({
            "settlementDocNumber": "BDPO-9999",
            "invoiceNumber": missing_invoice_number,
            "settlementAmount": -1250.00  # 5% BDPO
        })
        
        # Add the other docs for these settlements
        base_output["otherDocTable"].append({
            "otherDocType": "TDS",
            "otherDocNumber": "TDS-CM-9999",
            "otherDocAmount": -2500.00
        })
        
        base_output["otherDocTable"].append({
            "otherDocType": "BDPO",
            "otherDocNumber": "BDPO-9999",
            "otherDocAmount": -1250.00
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
        
        # Build a set of invoice numbers already in the invoiceTable
        existing_invoice_numbers = set(invoice["invoiceNumber"] for invoice in processed_output.get("invoiceTable", []))
        
        # Collect missing invoice numbers and their settlement amounts
        missing_invoices = defaultdict(list)
        for settlement in processed_output.get("settlementTable", []):
            invoice_number = settlement.get("invoiceNumber")
            if invoice_number and invoice_number not in existing_invoice_numbers:
                missing_invoices[invoice_number].append(settlement)
        
        # If there are missing invoices, add them to the invoiceTable
        for invoice_number, settlements in missing_invoices.items():
            # Calculate the total settlement amount (absolute value of the sum of settlement amounts)
            total_amount = sum(abs(float(s.get("settlementAmount", 0))) for s in settlements)
            
            # Create a new invoice entry
            invoice_entry = {
                "invoiceNumber": invoice_number,
                "invoiceDate": None,
                "bookingAmount": None,
                "totalSettlementAmount": total_amount
            }
            
            # Add to the invoiceTable
            processed_output["invoiceTable"].append(invoice_entry)
            logger.info(f"Added missing invoice {invoice_number} to invoiceTable with amount {total_amount}")
        
        return processed_output

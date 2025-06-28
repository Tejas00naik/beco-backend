"""
Mock LLM Extractor

This module simulates an LLM-based document extractor that parses emails
to identify and extract payment advice information, including invoices, 
credit notes, and settlement details.
"""

import logging
import random
from typing import Dict, List, Any, Optional, Tuple
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
        
        # Generate mock metadata based on attachment filename
        # In a real implementation, this would come from the LLM's analysis of the attachment
        current_date = datetime.utcnow()
        date_str = current_date.strftime("%d-%b-%Y").upper()
        advice_number = f"PA-{hash(filename) % 1000000:06d}"
        
        # Create a mock LLM output in the standardized format
        output = {
            "metaTable": {
                "paymentAdviceDate": date_str,
                "paymentAdviceNumber": advice_number,
                "payersLegalName": "Clicktech Retail Private Limited",
                "payeesLegalName": "Beco Trading Ltd"
            },
            "invoiceTable": [
                {
                    "invoiceNumber": f"INV-{hash(filename) % 10000:04d}",
                    "invoiceDate": (current_date - timedelta(days=30)).strftime("%d-%b-%Y").upper(),
                    "bookingAmount": float(f"{(hash(filename) % 100000) / 100:.2f}")
                },
                {
                    "invoiceNumber": f"INV-{(hash(filename) + 1) % 10000:04d}",
                    "invoiceDate": (current_date - timedelta(days=15)).strftime("%d-%b-%Y").upper(),
                    "bookingAmount": float(f"{(hash(filename + 'second') % 50000) / 100:.2f}")
                }
            ],
            "otherDocTable": [
                {
                    "otherDocType": "BDPO",
                    "otherDocNumber": f"BDPO-{hash(filename) % 100000:05d}",
                    "otherDocAmount": float(f"-{(hash(filename) % 100000) / 100:.2f}")
                },
                {
                    "otherDocType": "TDS",
                    "otherDocNumber": f"TDS-CM-{hash(filename) % 10000:04d}",
                    "otherDocAmount": float(f"-{(hash(filename + 'tds') % 1000) / 100:.2f}")
                }
            ],
            "settlementTable": [
                {
                    "settlementDocNumber": f"BDPO-{hash(filename) % 100000:05d}", # Same as otherDocNumber above
                    "invoiceNumber": None,
                    "settlementAmount": None
                },
                {
                    "settlementDocNumber": f"TDS-CM-{hash(filename) % 10000:04d}", # Same as otherDocNumber above
                    "invoiceNumber": f"INV-{hash(filename) % 10000:04d}", # Same as first invoiceNumber
                    "settlementAmount": float(f"-{(hash(filename + 'settle') % 1000) / 100:.2f}")
                }
            ]
        }
        
        logger.info(f"Generated payment advice data for attachment '{filename}' with {len(output['invoiceTable'])} invoices, "
                   f"{len(output['otherDocTable'])} other docs, and {len(output['settlementTable'])} settlements")
        
        return output

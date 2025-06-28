"""
Mock Email Reader

This module provides a mock implementation of an email reader that simulates
retrieving emails from customer inboxes. It generates realistic-looking mock
payment advice emails for testing the batch processor.
"""

import os
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Sample data for generating mock emails
SAMPLE_SENDERS = [
    "accounts@acme-industries.com",
    "payments@globex-corporation.com",
    "ar@wayne-enterprises.org",
    "finance@stark-industries.com",
    "treasury@oscorp.net"
]

SAMPLE_SUBJECTS = [
    "Payment Advice for Invoices",
    "Payment Notification",
    "Remittance Advice",
    "Payment Confirmation",
    "Invoice Payment Details"
]

SAMPLE_LEGAL_ENTITIES = [
    {"uuid": "le-001", "name": "Beco Technologies Ltd"},
    {"uuid": "le-002", "name": "Beco Solutions Inc"},
    {"uuid": "le-003", "name": "Beco Ventures LLC"}
]

SAMPLE_CUSTOMERS = [
    {"uuid": "cust-001", "name": "Acme Industries", "legal_entity_uuid": "le-001"},
    {"uuid": "cust-002", "name": "Globex Corporation", "legal_entity_uuid": "le-001"},
    {"uuid": "cust-003", "name": "Wayne Enterprises", "legal_entity_uuid": "le-002"},
    {"uuid": "cust-004", "name": "Stark Industries", "legal_entity_uuid": "le-002"},
    {"uuid": "cust-005", "name": "Oscorp", "legal_entity_uuid": "le-003"}
]


class MockEmailReader:
    """Mock implementation of an email reader for testing."""

    def __init__(self, data_path: str = None, max_emails: int = 5, is_test: bool = False):
        """
        Initialize the mock email reader.
        
        Args:
            data_path: Path to store mock email data (optional)
            max_emails: Maximum number of emails to generate per batch
            is_test: Flag to indicate test mode (will always generate emails regardless of timestamp)
        """
        self.data_path = data_path or os.path.join(os.getcwd(), "mock_data")
        self.max_emails = max_emails
        self.is_test = is_test
        self.processed_emails = set()  # Track processed email IDs
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_path, exist_ok=True)
        
        # Load processed emails tracking file if it exists
        tracking_file = os.path.join(self.data_path, "processed_emails.json")
        if os.path.exists(tracking_file):
            with open(tracking_file, "r") as f:
                self.processed_emails = set(json.load(f))
                
        logger.info(f"Initialized MockEmailReader with {len(self.processed_emails)} previously processed emails")
        
        # For test mode, always reset processed emails to ensure we generate new ones
        if self.is_test:
            self.processed_emails = set()

    def _save_processed_emails(self):
        """Save the set of processed email IDs to a file."""
        tracking_file = os.path.join(self.data_path, "processed_emails.json")
        with open(tracking_file, "w") as f:
            json.dump(list(self.processed_emails), f)

    def _generate_mock_email(self, email_id: str) -> Dict[str, Any]:
        """
        Generate a mock email with payment advice information.
        
        Args:
            email_id: Unique identifier for this email
            
        Returns:
            Dict containing mock email data
        """
        # Select a random customer and matching sender
        customer_idx = random.randint(0, len(SAMPLE_CUSTOMERS) - 1)
        customer = SAMPLE_CUSTOMERS[customer_idx]
        sender = SAMPLE_SENDERS[customer_idx]
        
        # Generate received timestamp (within past week)
        days_ago = random.randint(0, 7)
        hours_ago = random.randint(0, 23)
        received_at = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
        
        # Generate between 1-3 payment advices for this email
        num_advices = random.randint(1, 3)
        payment_advices = []
        
        for i in range(num_advices):
            # Generate payment advice
            advice_number = f"PA-{random.randint(10000, 99999)}"
            advice_amount = round(random.uniform(1000, 10000), 2)
            
            # Generate between 1-5 invoices for this payment advice
            num_invoices = random.randint(1, 5)
            invoices = []
            other_docs = []
            total_invoice_amount = 0
            
            for j in range(num_invoices):
                invoice_number = f"INV-{random.randint(100000, 999999)}"
                invoice_amount = round(advice_amount / num_invoices, 2)
                total_invoice_amount += invoice_amount
                
                invoices.append({
                    "invoice_number": invoice_number,
                    "invoice_date": (received_at - timedelta(days=random.randint(30, 60))).strftime("%Y-%m-%d"),
                    "booking_amount": invoice_amount
                })
            
            # Add a credit note occasionally
            if random.random() > 0.7:
                credit_note_number = f"CN-{random.randint(10000, 99999)}"
                credit_amount = round(random.uniform(100, 500), 2)
                
                other_docs.append({
                    "other_doc_number": credit_note_number,
                    "other_doc_type": "CN",
                    "other_doc_date": (received_at - timedelta(days=random.randint(10, 30))).strftime("%Y-%m-%d"),
                    "other_doc_amount": credit_amount
                })
            
            payment_advices.append({
                "payment_advice_number": advice_number,
                "payment_advice_date": received_at.strftime("%Y-%m-%d"),
                "payment_advice_amount": advice_amount,
                "payer_name": customer["name"],
                "payee_name": "Beco Technologies",
                "invoices": invoices,
                "other_docs": other_docs
            })
        
        # Generate email content
        email_content = f"""
Dear Accounts Receivable Team,

Please find the attached payment advice for the following invoices:

{self._format_payment_advices(payment_advices)}

If you have any questions, please contact our accounts department.

Best regards,
Finance Team
{customer["name"]}
"""

        # Create the mock email object
        email = {
            "email_id": email_id,
            "object_file_path": f"mock_emails/{email_id}.eml",
            "sender_mail": sender,
            "original_sender_mail": None,  # Assuming no forwarding in mock
            "received_at": received_at.isoformat(),
            "subject": random.choice(SAMPLE_SUBJECTS),
            "content": email_content,
            "customer_uuid": customer["uuid"],
            "legal_entity_uuid": customer["legal_entity_uuid"],
            "payment_advices": payment_advices
        }
        
        return email

    def _format_payment_advices(self, payment_advices: List[Dict[str, Any]]) -> str:
        """Format payment advice information as text for the email body."""
        formatted_text = ""
        
        for advice in payment_advices:
            formatted_text += f"Payment Advice Number: {advice['payment_advice_number']}\n"
            formatted_text += f"Payment Date: {advice['payment_advice_date']}\n"
            formatted_text += f"Amount: ${advice['payment_advice_amount']:.2f}\n\n"
            
            formatted_text += "Invoices:\n"
            for invoice in advice["invoices"]:
                formatted_text += f"- Invoice #{invoice['invoice_number']}, "
                formatted_text += f"Date: {invoice['invoice_date']}, "
                formatted_text += f"Amount: ${invoice['booking_amount']:.2f}\n"
            
            if advice["other_docs"]:
                formatted_text += "\nOther Documents:\n"
                for doc in advice["other_docs"]:
                    formatted_text += f"- {doc['other_doc_type']} #{doc['other_doc_number']}, "
                    formatted_text += f"Date: {doc['other_doc_date']}, "
                    formatted_text += f"Amount: ${doc['other_doc_amount']:.2f}\n"
            
            formatted_text += "\n" + "-"*40 + "\n\n"
            
        return formatted_text

    def _save_mock_email(self, email: Dict[str, Any]) -> None:
        """Save a mock email to the data directory."""
        email_file = os.path.join(self.data_path, f"{email['email_id']}.json")
        with open(email_file, "w") as f:
            json.dump(email, f, indent=2)

    def get_unprocessed_emails(self, since_timestamp: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get a list of unprocessed emails.

        Args:
            since_timestamp: If provided, only return emails after this timestamp
            
        Returns:
            List of unprocessed email objects
        """
        # When running in test mode, clear processed emails to ensure we generate new ones
        # ONLY if the processed emails list is empty to begin with
        if self.is_test and not self.processed_emails:
            self.processed_emails = set()
            
        # Generate mock emails
        new_emails = []
        
        # Log if we're using incremental mode
        if since_timestamp:
            logger.info(f"Mock email reader using incremental mode since {since_timestamp}")
        
        # Generate a random number of new emails
        num_emails = random.randint(1, self.max_emails)
        
        for i in range(num_emails):
            email_id = f"email-{datetime.now().strftime('%Y%m%d')}-{i+1}"
            
            # Skip if this email has been processed already
            if email_id in self.processed_emails:
                continue
                
            # Generate and save the mock email
            email = self._generate_mock_email(email_id)
            
            # Always include emails in test mode, regardless of timestamp
            if not self.is_test and since_timestamp:
                try:
                    received_at = datetime.fromisoformat(email["received_at"])
                    # Only compare if since_timestamp is a proper datetime
                    if isinstance(since_timestamp, datetime) and received_at < since_timestamp:
                        # Skip emails older than the since_timestamp
                        # We want emails that arrived ON or AFTER the since_timestamp
                        continue
                except (ValueError, TypeError):
                    # If we can't parse the timestamp or compare, just include the email
                    pass
                    
            self._save_mock_email(email)
            new_emails.append(email)
            
            # Mark as processed
            self.processed_emails.add(email_id)
            
        # Save the updated processed emails list
        self._save_processed_emails()
        
        logger.info(f"Generated {len(new_emails)} new mock emails")
        return new_emails

    def mark_as_processed(self, email_ids: List[str]) -> None:
        """
        Mark emails as processed.
        
        Args:
            email_ids: List of email IDs to mark as processed
        """
        for email_id in email_ids:
            self.processed_emails.add(email_id)
        
        self._save_processed_emails()
        logger.info(f"Marked {len(email_ids)} emails as processed")

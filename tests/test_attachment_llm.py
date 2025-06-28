"""
Test script for the new attachment-based LLM extraction workflow.

This script demonstrates how to process each attachment as a separate payment advice
using the new LLM extraction method.
"""

import os
import sys
import json
import logging
import asyncio
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to Python path
project_root = Path(__file__).parent.parent
if project_root not in sys.path:
    sys.path.append(str(project_root))

# Import components
from src.mocks.llm_extractor import MockLLMExtractor
from src.mocks.email_reader import MockEmailReader
from src.config import EMAIL_TEXT_FILENAME

async def test_attachment_llm_extraction():
    """
    Test the attachment-based LLM extraction workflow.
    
    For each email, process each attachment as a separate payment advice.
    """
    logger.info("Starting attachment-based LLM extraction test")
    
    # Initialize components
    email_reader = MockEmailReader()
    llm_extractor = MockLLMExtractor()
    
    # Create mock emails with attachments for testing
    emails = [
        {
            "email_id": "test-email-1",
            "sender_mail": "sender@example.com",
            "subject": "Test Email with Attachments",
            "text_content": "Please find attached payment advices for processing.",
            "html_content": "<p>Please find attached payment advices for processing.</p>",
            "raw_email": b"Mock raw email content",
            "received_at": datetime.utcnow(),
            "attachments": [
                {
                    "filename": "payment_advice_001.pdf",
                    "content_type": "application/pdf",
                    "data": b"Mock PDF content for payment advice 1"
                },
                {
                    "filename": "payment_advice_002.pdf",
                    "content_type": "application/pdf",
                    "data": b"Mock PDF content for payment advice 2"
                },
                {
                    "filename": "invoice_details.xlsx",
                    "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "data": b"Mock Excel content with invoice details"
                }
            ]
        },
        {
            "email_id": "test-email-2",
            "sender_mail": "another@example.com",
            "subject": "Additional Payment Documents",
            "text_content": "Please process these additional payment documents.",
            "html_content": "<p>Please process these additional payment documents.</p>",
            "raw_email": b"Another mock raw email content",
            "received_at": datetime.utcnow(),
            "attachments": [
                {
                    "filename": "settlement_123.pdf",
                    "content_type": "application/pdf",
                    "data": b"Mock PDF content for settlement document"
                }
            ]
        }
    ]
    
    logger.info(f"Created {len(emails)} test emails with attachments")

    logger.info(f"Retrieved {len(emails)} test emails")
    
    for email in emails:
        email_id = email.get("email_id", str(uuid.uuid4()))
        text_content = email.get("text_content", "")
        attachments = email.get("attachments", [])
        
        logger.info(f"Processing email {email_id} with {len(attachments)} attachments")
        
        if not attachments:
            logger.warning(f"Email {email_id} has no attachments to process")
            continue
        
        # Process each attachment as a separate payment advice
        for idx, attachment in enumerate(attachments):
            try:
                logger.info(f"Processing attachment {idx+1}/{len(attachments)}: {attachment.get('filename', 'unknown')}")
                
                # Call the LLM extractor with email text and the attachment
                payment_advice_data = llm_extractor.process_attachment_for_payment_advice(
                    text_content, attachment
                )
                
                # Print the resulting structured data
                logger.info(f"Extracted payment advice data for attachment {idx+1}:")
                logger.info(f"  Meta Table: {json.dumps(payment_advice_data['metaTable'], indent=2)}")
                logger.info(f"  Invoice Table: {len(payment_advice_data['invoiceTable'])} items")
                logger.info(f"  Other Doc Table: {len(payment_advice_data['otherDocTable'])} items")
                logger.info(f"  Settlement Table: {len(payment_advice_data['settlementTable'])} items")
                
                # Here in a real implementation, we would process this data by:
                # 1. Creating invoice records for each item in the invoiceTable
                # 2. Creating other_doc records for each item in the otherDocTable 
                # 3. Creating settlement records for each item in the settlementTable
                
            except Exception as e:
                logger.error(f"Error processing attachment {idx+1}: {str(e)}")

async def main():
    """Main entry point."""
    await test_attachment_llm_extraction()

if __name__ == "__main__":
    asyncio.run(main())

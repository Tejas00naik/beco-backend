#!/usr/bin/env python

"""
Test script for the new modular payment processing system.

This script tests the new payment processing structure with a sample LLM output.
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime
import uuid

# Add the project root to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary components
from models.firestore_dao import FirestoreDAO
from src.services.legal_entity_lookup import LegalEntityLookupService
from src.payment_processing import PaymentProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Sample LLM output for testing
SAMPLE_LLM_OUTPUT = {
    "metaTable": {
        "payersLegalName": "Amazon Technologies India",
        "payeesLegalName": "BECO.AI",
        "paymentAdviceNumber": "PA-TEST-001",
        "paymentAdviceDate": "2025-06-30",
        "paymentAdviceAmount": 25000.00,
        "paymentMethod": "Bank Transfer",
        "paymentReference": "REF12345",
        "currency": "INR"
    },
    "invoiceTable": [
        {
            "invoiceNumber": "INV-TEST-001",
            "invoiceDate": "2025-06-01",
            "bookingAmount": 10000.00,
            "totalSettlementAmount": 9000.00
        },
        {
            "invoiceNumber": "INV-TEST-002",
            "invoiceDate": "2025-06-10",
            "bookingAmount": 15000.00,
            "totalSettlementAmount": 15000.00
        }
    ],
    "otherDocTable": [
        {
            "otherDocNumber": "TDS-CM-001",
            "otherDocDate": "2025-06-30",
            "otherDocType": "TDS Certificate",
            "otherDocAmount": 1000.00
        },
        {
            "otherDocNumber": "CM-001",
            "otherDocDate": "2025-06-30",
            "otherDocType": "Credit Note",
            "otherDocAmount": 0.00
        }
    ],
    "settlementTable": [
        {
            "invoiceNumber": "INV-TEST-001",
            "settlementDocNumber": "TDS-CM-001",
            "settlementAmount": 1000.00
        },
        {
            "invoiceNumber": "INV-TEST-002",
            "settlementDocNumber": "CM-001",
            "settlementAmount": 0.00
        }
    ]
}


async def main():
    """Main test function."""
    try:
        # Initialize Firestore DAO with test prefix
        dao = FirestoreDAO(collection_prefix="dev_test_")
        
        # Initialize Legal Entity Lookup Service
        legal_entity_lookup = LegalEntityLookupService(dao)
        
        # Create test email log
        email_log_uuid = str(uuid.uuid4())
        email_log = {
            "email_log_uuid": email_log_uuid,
            "sender_mail": "test@example.com",
            "email_subject": "Test Payment Advice",
            "received_at": datetime.now(),
            "group_uuids": []
        }
        await dao.add_document("email_log", email_log_uuid, email_log)
        logger.info(f"Created test email log with UUID {email_log_uuid}")
        
        # Initialize Payment Processor
        payment_processor = PaymentProcessor(dao, legal_entity_lookup)
        
        # Process the sample LLM output
        logger.info("Processing payment advice with refactored payment processing modules...")
        payment_advice_uuid = await payment_processor.create_payment_advice_from_llm_output(
            SAMPLE_LLM_OUTPUT, email_log_uuid)
        
        if payment_advice_uuid:
            logger.info(f"✅ Successfully created payment advice with UUID {payment_advice_uuid}")
            
            # Verify created records
            payment_advice = await dao.get_document("payment_advice", payment_advice_uuid)
            invoices = await dao.query_documents("invoice", [("payment_advice_uuid", "==", payment_advice_uuid)])
            other_docs = await dao.query_documents("other_doc", [("payment_advice_uuid", "==", payment_advice_uuid)])
            settlements = await dao.query_documents("settlement", [("payment_advice_uuid", "==", payment_advice_uuid)])
            
            logger.info(f"Payment Advice: {json.dumps(payment_advice, default=str)}")
            logger.info(f"Created {len(invoices)} invoices")
            logger.info(f"Created {len(other_docs)} other documents")
            logger.info(f"Created {len(settlements)} settlements")
            
            # Clean up test data
            if input("Clean up test data? (y/n): ").lower() == 'y':
                logger.info("Cleaning up test data...")
                for settlement in settlements:
                    await dao.delete_document("settlement", settlement["settlement_uuid"])
                
                for invoice in invoices:
                    await dao.delete_document("invoice", invoice["invoice_uuid"])
                    
                for other_doc in other_docs:
                    await dao.delete_document("other_doc", other_doc["other_doc_uuid"])
                
                await dao.delete_document("payment_advice", payment_advice_uuid)
                await dao.delete_document("email_log", email_log_uuid)
                logger.info("Test data cleaned up successfully")
        else:
            logger.error("❌ Failed to create payment advice")
    
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())

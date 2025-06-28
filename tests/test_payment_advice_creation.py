"""
Test script for payment advice creation from LLM output.

This script tests the integration between LLM output parsing,
legal entity lookup, and payment advice record creation.
"""

import asyncio
import os
import sys
import logging
from datetime import datetime
from uuid import uuid4

# Add src to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.main import BatchWorker
from models.schemas import PaymentAdviceStatus

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Sample LLM output mimicking the structure expected from the LLM extractor
SAMPLE_LLM_OUTPUT = {
    "metaTable": {
        "payersLegalName": "Acme Corp",
        "payeesLegalName": "Beco Trading Ltd",
        "paymentAdviceNumber": "PA-2023-05-001",
        "paymentAdviceDate": "15-May-2023",
        "paymentMethod": "Bank Transfer",
        "currency": "USD"
    },
    "invoiceTable": [
        {
            "invoiceNumber": "INV-2023-001",
            "invoiceDate": "01-Apr-2023",
            "bookingAmount": 5000.00,
            "customerName": "Acme Corp - North Division"
        },
        {
            "invoiceNumber": "INV-2023-002",
            "invoiceDate": "15-Apr-2023",
            "bookingAmount": 7500.00,
            "customerName": "Acme Corp - South Division"
        }
    ],
    "otherDocTable": [
        {
            "otherDocNumber": "DN-2023-001", 
            "otherDocDate": "05-May-2023",
            "otherDocType": "DN",
            "otherDocAmount": -1200.00,
            "customerName": "Acme Corp - North Division"
        }
    ],
    "settlementTable": [
        {
            "settlementDate": "15-May-2023",
            "settlementAmount": 5000.00,
            "relatedDocNumber": "INV-2023-001"
        },
        {
            "settlementDate": "15-May-2023",
            "settlementAmount": 7500.00,
            "relatedDocNumber": "INV-2023-002"
        },
        {
            "settlementDate": "15-May-2023",
            "settlementAmount": -1200.00,
            "relatedDocNumber": "DN-2023-001"
        }
    ]
}

async def test_payment_advice_creation():
    """Test payment advice creation from LLM output."""
    
    # Initialize BatchWorker with test mode
    worker = BatchWorker(is_test=True)
    
    # Create mock email_log_uuid
    email_log_uuid = str(uuid4())
    logger.info(f"Created mock email_log_uuid: {email_log_uuid}")
    
    try:
        # Call create_payment_advice_from_llm_output with sample data
        payment_advice_uuid = await worker.create_payment_advice_from_llm_output(
            SAMPLE_LLM_OUTPUT, 
            email_log_uuid
        )
        
        if payment_advice_uuid:
            logger.info(f"Successfully created payment advice: {payment_advice_uuid}")
            
            # Fetch the created payment advice from Firestore to verify
            payment_advice = await worker.dao.get_document("payment_advice", payment_advice_uuid)
            
            if payment_advice:
                logger.info("Payment advice details:")
                logger.info(f"  UUID: {payment_advice.get('payment_advice_uuid')}")
                logger.info(f"  Payer: {payment_advice.get('payer_name')}")
                logger.info(f"  Payee: {payment_advice.get('payee_name')}")
                logger.info(f"  Legal Entity UUID: {payment_advice.get('legal_entity_uuid')}")
                logger.info(f"  Number: {payment_advice.get('payment_advice_number')}")
                logger.info(f"  Date: {payment_advice.get('payment_advice_date')}")
                logger.info(f"  Amount: {payment_advice.get('payment_advice_amount')}")
                logger.info(f"  Status: {payment_advice.get('payment_advice_status')}")
                
                # Verify the payment advice details
                assert payment_advice.get('payer_name') == "Acme Corp"
                assert payment_advice.get('payee_name') == "Beco Trading Ltd"
                assert payment_advice.get('legal_entity_uuid') is not None
                assert payment_advice.get('payment_advice_status') == PaymentAdviceStatus.NEW.value
                assert payment_advice.get('email_log_uuid') == email_log_uuid
                
                logger.info("✅ All payment advice fields verified successfully!")
            else:
                logger.error("Failed to retrieve payment advice from Firestore")
                
        else:
            logger.error("Failed to create payment advice")
    
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        raise
    
    finally:
        # Clean up (optional) - delete the test record
        if 'payment_advice_uuid' in locals() and payment_advice_uuid:
            try:
                await worker.dao.delete_document("payment_advice", payment_advice_uuid)
                logger.info(f"Cleaned up test data: deleted payment advice {payment_advice_uuid}")
            except Exception as e:
                logger.warning(f"Failed to clean up test data: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_payment_advice_creation())

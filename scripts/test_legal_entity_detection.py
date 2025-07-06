"""Test the LLM-based legal entity detection."""

import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

# Add the project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.firestore_dao import FirestoreDAO
from src.services.legal_entity_lookup import LegalEntityLookupService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sample email content from Amazon payment advice
SAMPLE_EMAIL_BODY = """
Please do not reply to this message. This email was sent from an unmonitored account.
Clicktech Retail Private Limited issued the following payment by bank transfer directly into your bank account.
Please wait for 5 business days before reporting your payment as late or missing, as it can take time for the funds to
arrive in your bank account.
If you have any questions about this payment, please contact us by choosing the most relevant issue through Vendor
Central > Support > Contact Us > Accounting.
Payment made to: KWICK LIVING (I) PRIVATE LIMITED(1MC3G)
"""

# Sample attachment content (simplified for testing)
SAMPLE_ATTACHMENT_TEXT = """
Payment Advice

Payer: Clicktech Retail Private Limited
Payment Date: 01-07-2025
Payment Amount: INR 25,000.00

INVOICE DETAILS:
Invoice Number: INV-001
Invoice Date: 15-06-2025
Invoice Amount: INR 25,000.00
"""

async def test_legal_entity_detection():
    """Test the LLM-based legal entity detection."""
    try:
        # Load environment variables
        load_dotenv()
        
        logger.info("Starting legal entity detection test")
        
        # Create FirestoreDAO and LegalEntityLookupService instances
        dao = FirestoreDAO()
        legal_entity_service = LegalEntityLookupService(dao=dao)
        
        # Test the detection with sample email and attachment
        logger.info("Testing legal entity detection with sample email and attachment")
        result = await legal_entity_service.detect_legal_entity_with_llm(
            email_body=SAMPLE_EMAIL_BODY,
            document_text=SAMPLE_ATTACHMENT_TEXT
        )
        
        # Log the result
        logger.info(f"Detection result: {result}")
        
        if result and result.get("legal_entity_uuid") and result.get("group_uuid"):
            logger.info("✅ Legal entity detection successful!")
            logger.info(f"Detected legal_entity_uuid: {result.get('legal_entity_uuid')}")
            logger.info(f"Detected group_uuid: {result.get('group_uuid')}")
            
            # Verify group is Amazon group
            if result.get("group_uuid") == "group-amazon-12345":
                logger.info("✅ Correct Amazon group detected!")
            else:
                logger.info("❌ Wrong group detected!")
        else:
            logger.error("❌ Legal entity detection failed!")
        
    except Exception as e:
        logger.error(f"Error in test: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_legal_entity_detection())

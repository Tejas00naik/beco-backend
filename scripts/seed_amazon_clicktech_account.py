"""
Seed script to add BP account for Amazon Clicktech legal entity to Firestore.

This script adds a BP account linked to the Amazon Clicktech legal entity in the Firestore database.
"""

import asyncio
from datetime import datetime
import os
import sys
import logging
from uuid import uuid4
from dotenv import load_dotenv

# Add the parent directory to the Python path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.repositories.firestore_dao import FirestoreDAO
from src.models.account import Account

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Amazon Clicktech legal entity UUID (should match what's already in Firestore)
AMAZON_LEGAL_ENTITY_UUID = "amazon-clicktech-retail-123456"

# BP account for Amazon Clicktech
AMAZON_BP_ACCOUNT = {
    "account_uuid": str(uuid4()),
    "account_name": "Amazon Clicktech Retail",
    "account_type": "BP",
    "sap_account_id": "BP20001",  # Sample BP code - change as needed
    "legal_entity_uuid": AMAZON_LEGAL_ENTITY_UUID,
    "is_active": True,
    "is_tds_account": False
}

async def seed_amazon_account():
    """Seed the Firestore database with BP account for Amazon Clicktech legal entity."""
    try:
        # Load environment variables from .env file
        dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(dotenv_path)
        
        # Create a FirestoreDAO instance
        dao = FirestoreDAO()
        
        logger.info(f"Connected to Firestore project {dao.project_id}, database {dao.database_id}")
        
        # First check if the Amazon legal entity exists
        existing_entity = await dao.get_document("legal_entity", AMAZON_LEGAL_ENTITY_UUID)
        if not existing_entity:
            logger.error(f"Amazon legal entity with UUID {AMAZON_LEGAL_ENTITY_UUID} not found in Firestore")
            return
            
        logger.info(f"Found Amazon legal entity: {existing_entity.get('legal_entity_name', existing_entity.get('name', 'Unknown'))}")
        
        # Check if a BP account already exists for this legal entity
        existing_accounts = await dao.query_documents(
            "account", 
            [
                ("legal_entity_uuid", "==", AMAZON_LEGAL_ENTITY_UUID),
                ("account_type", "==", "BP")
            ]
        )
        
        if existing_accounts:
            logger.info(f"Found existing BP account for Amazon legal entity: {existing_accounts[0].get('account_name')}")
            
            # Update the account to ensure it has the correct BP code
            existing_account = existing_accounts[0]
            account_uuid = existing_account.get('account_uuid')
            
            await dao.update_document(
                "account", 
                account_uuid, 
                {
                    "sap_account_id": AMAZON_BP_ACCOUNT.get("sap_account_id"),
                    "updated_at": datetime.utcnow()
                }
            )
            logger.info(f"Updated BP account {account_uuid} with SAP ID {AMAZON_BP_ACCOUNT.get('sap_account_id')}")
        else:
            # Create a new BP account for the Amazon legal entity
            account_dict = {
                "account_uuid": AMAZON_BP_ACCOUNT.get("account_uuid"),
                "account_name": AMAZON_BP_ACCOUNT.get("account_name"),
                "account_type": AMAZON_BP_ACCOUNT.get("account_type"),
                "sap_account_id": AMAZON_BP_ACCOUNT.get("sap_account_id"),
                "legal_entity_uuid": AMAZON_BP_ACCOUNT.get("legal_entity_uuid"),
                "is_active": AMAZON_BP_ACCOUNT.get("is_active"),
                "is_tds_account": AMAZON_BP_ACCOUNT.get("is_tds_account"),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            await dao.add_document("account", AMAZON_BP_ACCOUNT.get("account_uuid"), account_dict)
            logger.info(f"Created BP account {AMAZON_BP_ACCOUNT.get('account_uuid')} with SAP ID {AMAZON_BP_ACCOUNT.get('sap_account_id')} for Amazon legal entity")

    except Exception as e:
        logger.error(f"Error seeding Amazon account: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # If the error is related to Firestore project ID, show a helpful message
        if "Firestore project ID not provided" in str(e):
            logger.info("Make sure the FIRESTORE_PROJECT_ID environment variable is set in your .env file.")
            logger.info("Example: FIRESTORE_PROJECT_ID=your-project-id")


if __name__ == "__main__":
    # Run the async function
    asyncio.run(seed_amazon_account())

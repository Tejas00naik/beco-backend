"""
Seed script to add BP account for Kiranakart/Zepto legal entity to Firestore.

This script adds a BP account linked to the Zepto legal entity in the Firestore database.
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

# Zepto legal entity UUID (should match what's already in Firestore)
ZEPTO_LEGAL_ENTITY_UUID = "handsontrade-private-limited-12345"

# BP account for Zepto
ZEPTO_BP_ACCOUNT = {
    "account_uuid": str(uuid4()),
    "account_name": "HandsOnTrade Private Limited",
    "account_type": "BP",
    "sap_account_id": "HOT1212",  # Sample BP code - change as needed
    "legal_entity_uuid": ZEPTO_LEGAL_ENTITY_UUID,
    "is_active": True,
    "is_tds_account": False
}

async def seed_zepto_account():
    """Seed the Firestore database with BP account for Zepto legal entity."""
    try:
        # Load environment variables from .env file
        dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(dotenv_path)
        
        # Create a FirestoreDAO instance
        dao = FirestoreDAO()
        
        logger.info(f"Connected to Firestore project {dao.project_id}, database {dao.database_id}")
        
        # First check if the Zepto legal entity exists
        existing_entity = await dao.get_document("legal_entity", ZEPTO_LEGAL_ENTITY_UUID)
        if not existing_entity:
            logger.error(f"Zepto legal entity with UUID {ZEPTO_LEGAL_ENTITY_UUID} not found in Firestore")
            logger.info("Please run seed_zepto_entity.py first to create the legal entity")
            return
            
        logger.info(f"Found Zepto legal entity: {existing_entity.get('legal_entity_name')}")
        
        # Check if a BP account already exists for this legal entity
        existing_accounts = await dao.query_documents(
            "account", 
            [
                ("legal_entity_uuid", "==", ZEPTO_LEGAL_ENTITY_UUID),
                ("account_type", "==", "BP")
            ]
        )
        
        if existing_accounts:
            logger.info(f"Found existing BP account for Zepto legal entity: {existing_accounts[0].get('account_name')}")
            
            # Update the account to ensure it has the correct BP code
            existing_account = existing_accounts[0]
            account_uuid = existing_account.get('account_uuid')
            
            await dao.update_document(
                "account", 
                account_uuid, 
                {
                    "sap_account_id": ZEPTO_BP_ACCOUNT.get("sap_account_id"),
                    "updated_at": datetime.utcnow()
                }
            )
            logger.info(f"Updated BP account {account_uuid} with SAP ID {ZEPTO_BP_ACCOUNT.get('sap_account_id')}")
        else:
            # Create a new BP account for the Zepto legal entity
            account_dict = {
                "account_uuid": ZEPTO_BP_ACCOUNT.get("account_uuid"),
                "account_name": ZEPTO_BP_ACCOUNT.get("account_name"),
                "account_type": ZEPTO_BP_ACCOUNT.get("account_type"),
                "sap_account_id": ZEPTO_BP_ACCOUNT.get("sap_account_id"),
                "legal_entity_uuid": ZEPTO_BP_ACCOUNT.get("legal_entity_uuid"),
                "is_active": ZEPTO_BP_ACCOUNT.get("is_active"),
                "is_tds_account": ZEPTO_BP_ACCOUNT.get("is_tds_account"),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            await dao.add_document("account", ZEPTO_BP_ACCOUNT.get("account_uuid"), account_dict)
            logger.info(f"Created BP account {ZEPTO_BP_ACCOUNT.get('account_uuid')} with SAP ID {ZEPTO_BP_ACCOUNT.get('sap_account_id')} for Zepto legal entity")
        
        logger.info("Successfully added/updated BP account for Zepto legal entity in Firestore")
        
        # Verify the BP account can be found by the account repository
        from src.repositories.account_repository import AccountRepository
        account_repo = AccountRepository(dao)
        bp_account = await account_repo.get_bp_account_by_legal_entity(ZEPTO_LEGAL_ENTITY_UUID)
        
        if bp_account:
            logger.info(f"Verified BP account lookup: Found account {bp_account.account_uuid} with SAP ID {bp_account.sap_account_id}")
        else:
            logger.error("Failed to lookup BP account by legal entity UUID")
        
    except Exception as e:
        logger.error(f"Failed to seed Zepto BP account: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    # Run the async function
    asyncio.run(seed_zepto_account())

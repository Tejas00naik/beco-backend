"""
Seed script to add Zepto legal entity to Firestore.

This script adds the Zepto group and legal entity to the Firestore database.
"""

import asyncio
from datetime import datetime
import os
import sys
import logging
from dotenv import load_dotenv

# Add the parent directory to the Python path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.repositories.firestore_dao import FirestoreDAO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Zepto group
ZEPTO_GROUP = {
    "group_uuid": "group-zepto-67890",
    "group_name": "Zepto Group"
}

# Zepto legal entity with the exact name matching what appears in the PDF
ZEPTO_LEGAL_ENTITY = {
    "legal_entity_uuid": "kiranakart-technologies-12345",
    "legal_entity_name": "KIRANAKART TECHNOLOGIES PRIVATE LIMITED",
    "group_uuid": "group-zepto-67890",
    # Additional identifiers to help with matching
    "alternate_names": ["Kiranakart Technologies", "Kiranakart"]
}

async def seed_zepto_entity():
    """Seed the Firestore database with Zepto group and legal entity."""
    try:
        # Load environment variables from .env file
        dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(dotenv_path)
        
        # Create a FirestoreDAO instance
        dao = FirestoreDAO()
        
        logger.info(f"Connected to Firestore project {dao.project_id}, database {dao.database_id}")
        
        # Check if Zepto group already exists
        existing_groups = await dao.query_documents("group", [("group_uuid", "==", ZEPTO_GROUP["group_uuid"])])
        if existing_groups:
            logger.info(f"Found existing Zepto group in Firestore: {existing_groups[0].get('group_name')}")
        else:
            # Add Zepto group to Firestore
            logger.info("Adding Zepto group to Firestore...")
            await dao.add_document(
                "group",
                ZEPTO_GROUP.get("group_uuid"),
                {
                    "group_uuid": ZEPTO_GROUP.get("group_uuid"),
                    "group_name": ZEPTO_GROUP.get("group_name"),
                    "is_active": True,
                    "metadata": None,
                    "group_created_at": datetime.utcnow().isoformat(),
                    "group_updated_at": datetime.utcnow().isoformat(),
                    "group_deleted_at": None,
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            )
            logger.info(f"Added Zepto group: {ZEPTO_GROUP.get('group_name')}")
        
        # Check if Zepto legal entity already exists
        existing_entities = await dao.query_documents(
            "legal_entity", 
            [("legal_entity_uuid", "==", ZEPTO_LEGAL_ENTITY["legal_entity_uuid"])]
        )
        
        if existing_entities:
            logger.info(f"Found existing Zepto legal entity in Firestore: {existing_entities[0].get('legal_entity_name')}")
            
            # Update the entity to ensure it has the correct group UUID and alternate names
            await dao.add_document(
                "legal_entity", 
                ZEPTO_LEGAL_ENTITY.get("legal_entity_uuid"), 
                {
                    "legal_entity_uuid": ZEPTO_LEGAL_ENTITY.get("legal_entity_uuid"),
                    "legal_entity_name": ZEPTO_LEGAL_ENTITY.get("legal_entity_name"),
                    "is_active": True,
                    "group_uuid": ZEPTO_LEGAL_ENTITY.get("group_uuid"),
                    "alternate_names": ZEPTO_LEGAL_ENTITY.get("alternate_names", []),
                    "updated_at": datetime.utcnow().isoformat()
                },
                merge=True
            )
            logger.info(f"Updated Zepto legal entity: {ZEPTO_LEGAL_ENTITY.get('legal_entity_name')}")
        else:
            # Add Zepto legal entity to Firestore
            logger.info("Adding Zepto legal entity to Firestore...")
            await dao.add_document(
                "legal_entity", 
                ZEPTO_LEGAL_ENTITY.get("legal_entity_uuid"), 
                {
                    "legal_entity_uuid": ZEPTO_LEGAL_ENTITY.get("legal_entity_uuid"),
                    "legal_entity_name": ZEPTO_LEGAL_ENTITY.get("legal_entity_name"),
                    "is_active": True,
                    "group_uuid": ZEPTO_LEGAL_ENTITY.get("group_uuid"),
                    "alternate_names": ZEPTO_LEGAL_ENTITY.get("alternate_names", []),
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            )
            logger.info(f"Added Zepto legal entity: {ZEPTO_LEGAL_ENTITY.get('legal_entity_name')}")
        
        logger.info("Successfully added/updated Zepto group and legal entity in Firestore")
        
    except Exception as e:
        logger.error(f"Failed to seed Zepto entity: {e}")
        raise

if __name__ == "__main__":
    # Run the async function
    asyncio.run(seed_zepto_entity())

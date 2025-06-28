"""
Seed script to populate legal entities in Firestore.

This script creates sample legal entities in the Firestore database
for development and testing purposes.
"""

import asyncio
from datetime import datetime
import os
import sys
import logging
from dotenv import load_dotenv

# Add the parent directory to the Python path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.firestore_dao import FirestoreDAO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample groups to seed
SAMPLE_GROUPS = [
    {"group_uuid": "group-amazon-12345", "group_name": "Amazon Group"},
    {"group_uuid": "group-acme-67890", "group_name": "Acme Group"},
    {"group_uuid": "group-conglomerate-24680", "group_name": "Conglomerate Group"},
    {"group_uuid": "group-misc-13579", "group_name": "Miscellaneous Group"}
]

# Sample legal entities to seed with group associations
SAMPLE_LEGAL_ENTITIES = [
    {"legal_entity_uuid": "acme-corp-legal-entity-12345", "legal_entity_name": "Acme Corp", "group_uuid": "group-acme-67890"},
    {"legal_entity_uuid": "globex-corp-legal-entity-67890", "legal_entity_name": "Globex Corporation", "group_uuid": "group-conglomerate-24680"},
    {"legal_entity_uuid": "stark-ind-legal-entity-24680", "legal_entity_name": "Stark Industries", "group_uuid": "group-conglomerate-24680"},
    {"legal_entity_uuid": "wayne-ent-legal-entity-13579", "legal_entity_name": "Wayne Enterprises", "group_uuid": "group-conglomerate-24680"},
    {"legal_entity_uuid": "umbrella-corp-legal-entity-98765", "legal_entity_name": "Umbrella Corporation", "group_uuid": "group-misc-13579"},
    {"legal_entity_uuid": "beco-trading-85412", "legal_entity_name": "Beco Trading Ltd", "group_uuid": "group-misc-13579"},
    # Amazon's legal entities for testing direct lookup - both in the same group
    {"legal_entity_uuid": "amazon-clicktech-retail-123456", "legal_entity_name": "Clicktech Retail Private Limited", "group_uuid": "group-amazon-12345"},
    {"legal_entity_uuid": "amazon-seller-services-789012", "legal_entity_name": "Amazon Seller Services", "group_uuid": "group-amazon-12345"}
]

async def seed_legal_entities():
    """Seed the Firestore database with sample groups and legal entities."""
    try:
        # Load environment variables from .env file
        dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(dotenv_path)
        
        # Print the loaded Firestore project ID for confirmation
        project_id = os.environ.get('FIRESTORE_PROJECT_ID')
        database_id = os.environ.get('FIRESTORE_DATABASE_ID', 'beco-payment-advice-dev')
        logger.info(f"Loaded project ID from .env: {project_id}")
        logger.info(f"Using database ID: {database_id}")
        
        # Create a FirestoreDAO instance with settings from environment variables
        dao = FirestoreDAO()
        
        logger.info(f"Connected to Firestore project {dao.project_id}, database {dao.database_id}")
        
        # Check if groups already exist
        existing_groups = await dao.query_documents("group")
        if existing_groups:
            logger.info(f"Found {len(existing_groups)} existing groups in Firestore")
            for group in existing_groups:
                logger.info(f"  - {group.get('group_name')} ({group.get('group_uuid')})")
        
        # Check if entities already exist
        existing_entities = await dao.query_documents("legal_entity")
        if existing_entities:
            logger.info(f"Found {len(existing_entities)} existing legal entities in Firestore")
            
            # Display existing entities
            for entity in existing_entities:
                logger.info(f"  - {entity.get('legal_entity_name')} ({entity.get('legal_entity_uuid')})")
            
            confirm = input("Do you want to add/update groups and entities? (y/n): ")
            if confirm.lower() != 'y':
                logger.info("Exiting without adding/updating records")
                return
        
        # Add sample groups to Firestore
        logger.info("Adding sample groups to Firestore...")
        for group in SAMPLE_GROUPS:
            await dao.add_document(
                "group",
                group.get("group_uuid"),
                {
                    "group_uuid": group.get("group_uuid"),
                    "group_name": group.get("group_name"),
                    "is_active": True,
                    "metadata": None,
                    "group_created_at": datetime.utcnow().isoformat(),
                    "group_updated_at": datetime.utcnow().isoformat(),
                    "group_deleted_at": None,
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            )
            logger.info(f"Added/updated group: {group.get('group_name')}")
        
        # Add sample entities to Firestore
        logger.info("Adding sample legal entities to Firestore...")
        for entity in SAMPLE_LEGAL_ENTITIES:
            # Also add to Firestore for persistence
            await dao.add_document(
                "legal_entity", 
                entity.get("legal_entity_uuid"), 
                {
                    "legal_entity_uuid": entity.get("legal_entity_uuid"),
                    "legal_entity_name": entity.get("legal_entity_name"),
                    "is_active": True,
                    "group_uuid": entity.get("group_uuid", ""),  # Use provided group_uuid
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            )
            logger.info(f"Added/updated legal entity: {entity.get('legal_entity_name')} (Group: {entity.get('group_uuid')})")
        
        logger.info(f"Successfully added {len(SAMPLE_GROUPS)} groups and {len(SAMPLE_LEGAL_ENTITIES)} legal entities to Firestore")
        
    except Exception as e:
        logger.error(f"Failed to seed legal entities: {e}")
        raise

if __name__ == "__main__":
    # Run the async function
    asyncio.run(seed_legal_entities())

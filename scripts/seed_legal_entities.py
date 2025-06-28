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

# Sample legal entities to seed
SAMPLE_LEGAL_ENTITIES = [
    {"legal_entity_uuid": "acme-corp-legal-entity-12345", "legal_entity_name": "Acme Corp"},
    {"legal_entity_uuid": "globex-corp-legal-entity-67890", "legal_entity_name": "Globex Corporation"},
    {"legal_entity_uuid": "stark-ind-legal-entity-24680", "legal_entity_name": "Stark Industries"},
    {"legal_entity_uuid": "wayne-ent-legal-entity-13579", "legal_entity_name": "Wayne Enterprises"},
    {"legal_entity_uuid": "umbrella-corp-legal-entity-98765", "legal_entity_name": "Umbrella Corporation"},
    {"legal_entity_uuid": "beco-trading-85412", "legal_entity_name": "Beco Trading Ltd"},
    # Amazon's legal entity name for testing direct lookup
    {"legal_entity_uuid": "amazon-clicktech-retail-123456", "legal_entity_name": "Clicktech Retail Private Limited"}
]

async def seed_legal_entities():
    """Seed the Firestore database with sample legal entities."""
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
        
        # Check if entities already exist
        existing_entities = await dao.query_documents("legal_entity")
        if existing_entities:
            logger.info(f"Found {len(existing_entities)} existing legal entities in Firestore")
            
            # Display existing entities
            for entity in existing_entities:
                logger.info(f"  - {entity.get('legal_entity_name')} ({entity.get('legal_entity_uuid')})")
            
            confirm = input("Do you want to add additional entities? (y/n): ")
            if confirm.lower() != 'y':
                logger.info("Exiting without adding entities")
                return
        
        # Add sample entities to Firestore
        for entity in SAMPLE_LEGAL_ENTITIES:
            # Also add to Firestore for persistence
            await dao.add_document(
                "legal_entity", 
                entity.get("legal_entity_uuid"), 
                {
                    "legal_entity_uuid": entity.get("legal_entity_uuid"),
                    "legal_entity_name": entity.get("legal_entity_name"),
                    "is_active": True,
                    "group_uuid": "",  # Default empty group UUID
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            )
            logger.info(f"Added legal entity: {entity.get('legal_entity_name')}")
        
        logger.info(f"Successfully added {len(SAMPLE_LEGAL_ENTITIES)} legal entities to Firestore")
        
    except Exception as e:
        logger.error(f"Failed to seed legal entities: {e}")
        raise

if __name__ == "__main__":
    # Run the async function
    asyncio.run(seed_legal_entities())

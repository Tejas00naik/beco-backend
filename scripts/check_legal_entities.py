"""Check the legal entity data in Firestore."""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.firestore_dao import FirestoreDAO

async def main():
    """Check legal entity data."""
    # Initialize DAO
    dao = FirestoreDAO(collection_prefix="")  # Use production data (no prefix)
    
    # Query legal entities
    legal_entities = await dao.query_documents("legal_entity", [])
    
    print(f"Found {len(legal_entities)} legal entities:")
    for entity in legal_entities:
        print(f"  {entity.get('legal_entity_uuid')}: {entity.get('legal_entity_name')} -> Group: {entity.get('group_uuid')}")
    
    # Query groups
    groups = await dao.query_documents("group", [])
    
    print(f"\nFound {len(groups)} groups:")
    for group in groups:
        print(f"  {group.get('group_uuid')}: {group.get('group_name')}")

if __name__ == "__main__":
    asyncio.run(main())

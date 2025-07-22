"""Script to check legal entities in Firestore."""

import asyncio
from src.repositories.firestore_dao import FirestoreDAO

async def check_legal_entities():
    """Check legal entities in Firestore."""
    dao = FirestoreDAO()
    legal_entities = await dao.query_documents('legal_entity', [])
    print(f'Found {len(legal_entities)} legal entities:')
    for entity in legal_entities:
        print(f'- {entity.get("payer_legal_name")}: Group UUID = {entity.get("group_uuid")}')
        print(f'  UUID: {entity.get("legal_entity_uuid")}')

if __name__ == "__main__":
    asyncio.run(check_legal_entities())

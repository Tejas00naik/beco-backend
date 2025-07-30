#!/usr/bin/env python
"""Debug script for identifying account enrichment issues."""

import os
import asyncio
import logging
import json
from typing import Dict, Any, List
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set environment variables needed for Firestore DAO
os.environ["FIRESTORE_PROJECT_ID"] = "vaulted-channel-462118-a5"
os.environ["FIRESTORE_DATABASE_ID"] = "beco-payment-advice-dev"
os.environ["ENVIRONMENT"] = "dev"

class Account(BaseModel):
    """Account model."""
    account_uuid: str
    name: str = ""
    account_type: str = ""  # BP, GL
    sap_account_id: str = ""
    legal_entity_uuid: str = ""
    
    class Config:
        """Pydantic config."""
        arbitrary_types_allowed = True
        extra = "allow"

class FirestoreDAO:
    """A simple version of the FirestoreDAO for testing."""
    
    def __init__(self):
        """Initialize Firestore client."""
        from google.cloud import firestore
        database_id = os.environ.get("FIRESTORE_DATABASE_ID", "beco-payment-advice-dev")
        project_id = os.environ["FIRESTORE_PROJECT_ID"]
        self.client = firestore.Client(project=project_id, database=database_id)
        logger.info(f"Initialized Firestore client with project ID: {project_id} and database ID: {database_id}")
    
    async def get_document(self, collection: str, doc_id: str) -> Dict[str, Any]:
        """Get a document by ID."""
        doc_ref = self.client.collection(collection).document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict()
            # Add document ID to data
            data[f"{collection.rstrip('s')}_uuid"] = doc_id
            return data
        return None
    
    async def query_documents(self, collection: str, filters=None, limit=None) -> List[Dict[str, Any]]:
        """Query documents with filters."""
        query = self.client.collection(collection)
        
        if filters:
            for field, op, value in filters:
                query = query.where(field, op, value)
        
        if limit:
            query = query.limit(limit)
            
        docs = query.stream()
        results = []
        for doc in docs:
            data = doc.to_dict()
            # Add document ID to data based on collection name
            if collection == "paymentadvice_lines":
                data["payment_advice_line_uuid"] = doc.id
            elif collection.endswith("s"):
                data[f"{collection[:-1]}_uuid"] = doc.id
            else:
                data[f"{collection}_uuid"] = doc.id
            results.append(data)
        
        return results

class AccountRepository:
    """Repository for account data."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with DAO."""
        self.dao = dao
    
    async def get_bp_account_by_legal_entity(self, legal_entity_uuid: str) -> Account:
        """Get BP account for legal entity."""
        try:
            # Check if legal entity exists
            legal_entity = await self.dao.get_document("legal_entity", legal_entity_uuid)
            if not legal_entity:
                logger.error(f"Legal entity {legal_entity_uuid} not found")
                return None
            
            logger.info(f"Found legal entity: {legal_entity}")
            
            # Check 'accounts' collection first (correct collection name)
            logger.info(f"Checking 'accounts' collection for BP account with legal_entity_uuid={legal_entity_uuid}")
            accounts_data = await self.dao.query_documents(
                "accounts",
                [
                    ("legal_entity_uuid", "==", legal_entity_uuid),
                    ("account_type", "==", "BP")
                ]
            )
            
            if accounts_data:
                logger.info(f"Found {len(accounts_data)} BP accounts in 'accounts' collection: {accounts_data}")
                account_data = accounts_data[0]
                return Account(**account_data)
            else:
                logger.warning(f"No BP accounts found in 'accounts' collection")
            
            # Also check 'account' collection (possibly incorrect collection name)
            logger.info(f"Checking 'account' collection for BP account with legal_entity_uuid={legal_entity_uuid}")
            account_data = await self.dao.query_documents(
                "account",
                [
                    ("legal_entity_uuid", "==", legal_entity_uuid),
                    ("account_type", "==", "BP")
                ]
            )
            
            if account_data:
                logger.info(f"Found {len(account_data)} BP accounts in 'account' collection: {account_data}")
                return Account(**account_data[0])
            else:
                logger.warning(f"No BP accounts found in 'account' collection")
            
            logger.error(f"No BP account found for legal entity {legal_entity_uuid}")
            return None
        except Exception as e:
            logger.error(f"Error getting BP account: {str(e)}", exc_info=True)
            return None

async def debug_account_data():
    """Debug account data and relationships."""
    try:
        dao = FirestoreDAO()
        account_repo = AccountRepository(dao)
        
        # Step 1: Check specific legal entity
        legal_entity_uuid = "kiranakart-technologies-12345"
        logger.info(f"Checking legal entity {legal_entity_uuid}")
        
        legal_entity = await dao.get_document("legal_entity", legal_entity_uuid)
        if legal_entity:
            logger.info(f"Legal entity found: {json.dumps(legal_entity, indent=2)}")
        else:
            logger.error(f"Legal entity {legal_entity_uuid} not found!")
            
        # Step 2: Check BP account for this legal entity
        logger.info(f"Looking for BP account linked to legal entity {legal_entity_uuid}")
        bp_account = await account_repo.get_bp_account_by_legal_entity(legal_entity_uuid)
        
        if bp_account:
            logger.info(f"Found BP account: {bp_account.json(indent=2)}")
        else:
            logger.error(f"No BP account found for legal entity {legal_entity_uuid}")
            
            # Step 2b: Check if there are any accounts at all for this legal entity
            accounts = await dao.query_documents("accounts", [("legal_entity_uuid", "==", legal_entity_uuid)])
            if accounts:
                logger.info(f"Found {len(accounts)} accounts of any type for this legal entity")
                for idx, acc in enumerate(accounts):
                    logger.info(f"Account {idx+1}: {json.dumps(acc, indent=2)}")
            else:
                logger.error(f"No accounts of any type found for legal entity {legal_entity_uuid}")
                
                # Try the singular collection name as well
                accounts = await dao.query_documents("account", [("legal_entity_uuid", "==", legal_entity_uuid)])
                if accounts:
                    logger.info(f"Found {len(accounts)} accounts in 'account' collection")
                    for idx, acc in enumerate(accounts):
                        logger.info(f"Account {idx+1}: {json.dumps(acc, indent=2)}")
                else:
                    logger.error(f"No accounts found in 'account' collection either")
        
        # Step 3: Check recent payment advice with this legal entity
        logger.info(f"Checking payment advices with legal entity {legal_entity_uuid}")
        payment_advices = await dao.query_documents(
            "payment_advice", 
            [("legal_entity_uuid", "==", legal_entity_uuid)],
            limit=1
        )
        
        if not payment_advices:
            logger.error(f"No payment advices found for legal entity {legal_entity_uuid}")
            return
            
        payment_advice = payment_advices[0]
        payment_advice_uuid = payment_advice.get("payment_advice_uuid")
        logger.info(f"Found payment advice {payment_advice_uuid}: {json.dumps(payment_advice, indent=2)}")
        
        # Step 4: Check payment advice lines
        logger.info(f"Checking payment advice lines for payment advice {payment_advice_uuid}")
        lines = await dao.query_documents(
            "paymentadvice_lines",
            [("payment_advice_uuid", "==", payment_advice_uuid)]
        )
        
        logger.info(f"Found {len(lines)} payment advice lines")
        for idx, line in enumerate(lines[:3]):  # Just show first 3 for brevity
            logger.info(f"Line {idx+1}: {json.dumps(line, indent=2)}")
            
        # Step 5: Check if there's a collection naming issue
        collections = {
            "legal_entity": None,
            "legal_entities": None,
            "account": None,
            "accounts": None,
            "payment_advice": None,
            "payment_advices": None,
            "paymentadvice_line": None,
            "paymentadvice_lines": None
        }
        
        for collection in collections:
            try:
                # Just try to get one document to check if collection exists
                docs = list(dao.client.collection(collection).limit(1).stream())
                collections[collection] = len(docs) > 0
                logger.info(f"Collection '{collection}' exists: {collections[collection]}")
            except Exception as e:
                logger.error(f"Error checking collection '{collection}': {str(e)}")
                collections[collection] = False
                
        # Step 6: Suggest fixes based on findings
        logger.info("Analysis complete. Suggestions:")
        
        if not bp_account:
            if collections.get("account") and not collections.get("accounts"):
                logger.info("ISSUE: You're querying 'accounts' but the collection is actually named 'account'")
                logger.info("FIX: Update your code to use 'account' instead of 'accounts' in your queries")
            elif collections.get("accounts") and not collections.get("account"):
                logger.info("ISSUE: You're querying 'account' but the collection is actually named 'accounts'")
                logger.info("FIX: Update your code to use 'accounts' instead of 'account' in your queries")
            else:
                logger.info("ISSUE: No BP accounts found linked to the legal entity")
                logger.info("FIX: Check that you've created BP accounts with the legal_entity_uuid and account_type='BP'")

    except Exception as e:
        logger.error(f"Error in debug_account_data: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(debug_account_data())

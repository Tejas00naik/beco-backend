"""Repository for Account entities in Firestore."""

import logging
from typing import Dict, List, Optional, Any, Tuple
from uuid import uuid4
from datetime import datetime

from src.models.account import Account
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class AccountRepository:
    """Repository for Account entities in Firestore."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with Firestore DAO."""
        self.dao = dao
    
    async def get_account_by_uuid(self, account_uuid: str) -> Optional[Account]:
        """
        Get an account by UUID.
        
        Args:
            account_uuid: UUID of the account
            
        Returns:
            Account object if found, None otherwise
        """
        try:
            # Get account document
            account_data = await self.dao.get_document("account", account_uuid)
            if not account_data:
                logger.warning(f"Account {account_uuid} not found")
                return None
                
            # Convert to Account object
            account = Account(**account_data)
            return account
        except Exception as e:
            logger.error(f"Error getting account {account_uuid}: {str(e)}")
            return None
            
    async def get_accounts_by_legal_entity(self, legal_entity_uuid: str) -> List[Account]:
        """
        Get accounts associated with a legal entity.
        
        Args:
            legal_entity_uuid: UUID of the legal entity
            
        Returns:
            List of Account objects
        """
        try:
            # Query accounts with the given legal entity UUID
            account_data_list = await self.dao.query_documents(
                "account", 
                [("legal_entity_uuid", "==", legal_entity_uuid)]
            )
            
            accounts = []
            for account_data in account_data_list:
                try:
                    account = Account(**account_data)
                    accounts.append(account)
                except Exception as e:
                    logger.error(f"Error converting account data to Account object: {str(e)}")
                    
            logger.info(f"Found {len(accounts)} accounts for legal entity {legal_entity_uuid}")
            return accounts
        except Exception as e:
            logger.error(f"Error getting accounts for legal entity {legal_entity_uuid}: {str(e)}")
            return []
            
    async def get_bp_account_by_legal_entity(self, legal_entity_uuid: str) -> Optional[Account]:
        """
        Get the BP account for a legal entity.
        
        Args:
            legal_entity_uuid: UUID of the legal entity
            
        Returns:
            BP account if found, None otherwise
        """
        try:
            # Check if legal entity exists first
            legal_entity = await self.dao.get_document("legal_entity", legal_entity_uuid)
            if not legal_entity:
                logger.error(f"Legal entity {legal_entity_uuid} not found in database")
                return None
            else:
                logger.info(f"Legal entity found: {legal_entity.get('name', 'Unknown')} (UUID: {legal_entity_uuid})")
            
            # Log the collection name we're querying
            collection = "account"
            logger.info(f"Querying collection '{collection}' for BP accounts linked to legal entity {legal_entity_uuid}")
            
            logger.info(f"Querying account collection for legal entity UUID {legal_entity_uuid}")
            account_data_list = await self.dao.query_documents(
                "account", 
                [
                    ("legal_entity_uuid", "==", legal_entity_uuid),
                    ("account_type", "==", "BP")
                ]
            )
            
            # Log the query results
            if not account_data_list:
                logger.error(f"No BP accounts found for legal entity {legal_entity_uuid} in collection '{collection}'")
                
                # Try another query without the account_type filter to see if any accounts exist
                logger.info(f"Trying broader query without account_type filter for legal entity {legal_entity_uuid}")
                all_accounts = await self.dao.query_documents(
                    collection,
                    [("legal_entity_uuid", "==", legal_entity_uuid)]
                )
                
                if all_accounts:
                    logger.info(f"Found {len(all_accounts)} accounts with different types for legal entity {legal_entity_uuid}:")
                    for acc in all_accounts:
                        logger.info(f"Account {acc.get('account_uuid', 'Unknown')}: type={acc.get('account_type', 'Unknown')}, ")
                else:
                    logger.error(f"No accounts of any type found for legal entity {legal_entity_uuid}")
                
                # Try listing all BP accounts to see if they exist but with different legal entities
                bp_accounts = await self.dao.query_documents(
                    collection,
                    [("account_type", "==", "BP")],
                    limit=5
                )
                
                if bp_accounts:
                    logger.info(f"Found {len(bp_accounts)} BP accounts (showing up to 5):")
                    for acc in bp_accounts:
                        logger.info(f"BP Account {acc.get('account_uuid', 'Unknown')}: legal_entity={acc.get('legal_entity_uuid', 'None')}")
                else:
                    logger.error("No BP accounts found in the entire collection")
                    
                return None
                
            # Use the first account (should only be one)
            account_data = account_data_list[0]
            logger.info(f"Found account data: {account_data}")
            
            # Try to create Account object and check if it has required fields
            if 'account_uuid' not in account_data:
                logger.error(f"Account data missing 'account_uuid' field: {account_data}")
                return None
                
            if 'sap_account_id' not in account_data or not account_data['sap_account_id']:
                logger.warning(f"Account {account_data.get('account_uuid')} has no SAP account ID")
            
            # Remove document_id field if it exists to prevent initialization error
            if 'document_id' in account_data:
                logger.info(f"Removing document_id field from account data before Account initialization")
                account_data.pop('document_id')
                
            account = Account(**account_data)
            
            logger.info(f"Successfully created Account object with UUID={account.account_uuid} and SAP ID={account.sap_account_id}")
            return account
            
        except Exception as e:
            import traceback
            logger.error(f"Error getting BP account for legal entity {legal_entity_uuid}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
            
    async def get_tds_account(self) -> Optional[Account]:
        """
        Get the TDS account (GL account specifically for TDS).
        
        Returns:
            Account object if found, None otherwise
        """
        try:
            # Query accounts with is_tds_account = True
            logger.info("Querying account collection for TDS account")
            account_data_list = await self.dao.query_documents(
                "account", 
                [
                    ("is_tds_account", "==", True),
                    ("account_type", "==", "GL")
                ]
            )
            
            if not account_data_list:
                logger.warning("No TDS account found")
                return None
                
            # Use the first account (should only be one)
            account_data = account_data_list[0]
            
            # Remove document_id field if it exists to prevent initialization error
            if 'document_id' in account_data:
                logger.info(f"Removing document_id field from account data before Account initialization")
                account_data.pop('document_id')
                
            account = Account(**account_data)
            
            logger.info(f"Found TDS account {account.account_uuid} with SAP ID {account.sap_account_id}")
            return account
        except Exception as e:
            logger.error(f"Error getting TDS account: {str(e)}")
            return None
            
    async def get_accounts_by_type(self, account_type: str) -> List[Account]:
        """
        Get accounts by type (BP or GL).
        
        Args:
            account_type: Account type (BP or GL)
            
        Returns:
            List of Account objects
        """
        try:
            # Query accounts with the given type
            account_data_list = await self.dao.query_documents(
                "account", 
                [("account_type", "==", account_type)]
            )
            
            accounts = []
            for account_data in account_data_list:
                try:
                    account = Account(**account_data)
                    accounts.append(account)
                except Exception as e:
                    logger.error(f"Error converting account data to Account object: {str(e)}")
                    
            logger.info(f"Found {len(accounts)} accounts with type {account_type}")
            return accounts
        except Exception as e:
            logger.error(f"Error getting accounts with type {account_type}: {str(e)}")
            return []

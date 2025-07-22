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
        Get the BP account associated with a legal entity.
        Assumes a 1:1 relationship between legal entity and BP account.
        
        Args:
            legal_entity_uuid: UUID of the legal entity
            
        Returns:
            Account object if found, None otherwise
        """
        try:
            # Query accounts with the given legal entity UUID and account_type = BP
            account_data_list = await self.dao.query_documents(
                "account", 
                [
                    ("legal_entity_uuid", "==", legal_entity_uuid),
                    ("account_type", "==", "BP")
                ]
            )
            
            if not account_data_list:
                logger.warning(f"No BP account found for legal entity {legal_entity_uuid}")
                return None
                
            # Use the first account (should only be one)
            account_data = account_data_list[0]
            account = Account(**account_data)
            
            logger.info(f"Found BP account {account.account_uuid} with SAP ID {account.sap_account_id} for legal entity {legal_entity_uuid}")
            return account
        except Exception as e:
            logger.error(f"Error getting BP account for legal entity {legal_entity_uuid}: {str(e)}")
            return None
            
    async def get_tds_account(self) -> Optional[Account]:
        """
        Get the TDS account (GL account specifically for TDS).
        
        Returns:
            Account object if found, None otherwise
        """
        try:
            # Query accounts with is_tds_account = True
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

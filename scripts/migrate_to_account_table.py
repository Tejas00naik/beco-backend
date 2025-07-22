#!/usr/bin/env python
"""
Script to migrate data from the customer table to the new account table
and create a TDS account.
"""

import os
import sys
import asyncio
import logging
import uuid
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.account import Account
from src.repositories.firestore_dao import FirestoreDAO
from src.config import TDS_ACCOUNT_NAME, TDS_ACCOUNT_CODE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def create_tds_account(dao: FirestoreDAO) -> None:
    """
    Create a TDS account in the account table.
    
    Args:
        dao: Firestore DAO instance
    """
    try:
        # Check if TDS account already exists
        existing_tds = await dao.query_documents(
            "account", 
            [("is_tds_account", "==", True)]
        )
        
        if existing_tds:
            logger.info(f"TDS account already exists: {existing_tds[0].get('account_uuid')}")
            return
            
        # Create TDS account
        tds_account = Account(
            account_uuid=str(uuid.uuid4()),
            account_name=TDS_ACCOUNT_NAME,
            account_type="GL",
            sap_account_id=TDS_ACCOUNT_CODE,
            sap_account_name=TDS_ACCOUNT_NAME,
            is_active=True,
            is_tds_account=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Convert to dict for Firestore
        tds_account_dict = {
            "account_uuid": tds_account.account_uuid,
            "account_name": tds_account.account_name,
            "account_type": tds_account.account_type,
            "sap_account_id": tds_account.sap_account_id,
            "sap_account_name": tds_account.sap_account_name,
            "is_active": tds_account.is_active,
            "is_tds_account": tds_account.is_tds_account,
            "created_at": tds_account.created_at,
            "updated_at": tds_account.updated_at
        }
        
        # Save to Firestore
        await dao.add_document("account", tds_account.account_uuid, tds_account_dict)
        
        logger.info(f"Created TDS account: {tds_account.account_uuid} with GL code: {TDS_ACCOUNT_CODE}")
        
    except Exception as e:
        logger.error(f"Error creating TDS account: {str(e)}")

async def migrate_customers_to_accounts(dao: FirestoreDAO) -> None:
    """
    Migrate data from the customer table to the account table.
    
    Args:
        dao: Firestore DAO instance
    """
    try:
        # Get all customers using query with no filters
        customers = await dao.query_documents("customer", [])
        
        if not customers:
            logger.warning("No customers found to migrate")
            return
            
        logger.info(f"Found {len(customers)} customers to migrate")
        
        # Migrate each customer to account
        for customer in customers:
            customer_uuid = customer.get("customer_uuid")
            
            if not customer_uuid:
                logger.warning("Customer missing UUID, skipping")
                continue
                
            # Check if account already exists for this customer
            existing_account = await dao.query_documents(
                "account", 
                [("customer_uuid", "==", customer_uuid)]
            )
            
            if existing_account:
                logger.info(f"Account already exists for customer {customer_uuid}")
                continue
                
            # Create new account
            account = Account(
                account_uuid=str(uuid.uuid4()),
                account_name=customer.get("customer_name", ""),
                account_type="BP",
                sap_account_id=customer.get("sap_customer_id"),
                sap_account_name=customer.get("sap_customer_name"),
                state=customer.get("state"),
                payment_term_in_days=customer.get("payment_term_in_days", 0),
                is_active=customer.get("is_active", True),
                legal_entity_uuid=customer.get("legal_entity_uuid"),
                is_tds_account=False,
                metadata={
                    "customer_uuid": customer_uuid,
                    "migrated_at": datetime.utcnow().isoformat()
                },
                created_at=customer.get("created_at", datetime.utcnow()),
                updated_at=datetime.utcnow()
            )
            
            # Convert to dict for Firestore
            account_dict = {
                "account_uuid": account.account_uuid,
                "account_name": account.account_name,
                "account_type": account.account_type,
                "sap_account_id": account.sap_account_id,
                "sap_account_name": account.sap_account_name,
                "state": account.state,
                "payment_term_in_days": account.payment_term_in_days,
                "is_active": account.is_active,
                "legal_entity_uuid": account.legal_entity_uuid,
                "is_tds_account": account.is_tds_account,
                "metadata": account.metadata,
                "created_at": account.created_at,
                "updated_at": account.updated_at
            }
            
            # Save to Firestore
            await dao.add_document("account", account.account_uuid, account_dict)
            
            logger.info(f"Migrated customer {customer_uuid} to account {account.account_uuid}")
            
        logger.info("Customer migration completed")
            
    except Exception as e:
        logger.error(f"Error migrating customers: {str(e)}")

async def main() -> None:
    """Main function to run migration."""
    try:
        # Load environment variables from .env file
        dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
        load_dotenv(dotenv_path)
        
        # Parse arguments
        parser = argparse.ArgumentParser(description='Migrate customer data to account table')
        parser.add_argument('--project-id', '-p', type=str, help='Firestore project ID')
        parser.add_argument('--dry-run', '-d', action='store_true', help='Dry run mode (no writes)')
        args = parser.parse_args()
        
        # Get project ID from args, env var, or default
        project_id = args.project_id or os.environ.get('FIRESTORE_PROJECT_ID')
        if not project_id:
            logger.error("Firestore project ID not provided. Use --project-id or set FIRESTORE_PROJECT_ID env var")
            return
            
        logger.info(f"Starting migration with project ID: {project_id}")
        logger.info(f"Dry run mode: {args.dry_run}")
        
        # Initialize DAO with project ID
        dao = FirestoreDAO(project_id=project_id)
        
        if not args.dry_run:
            # Create TDS account
            await create_tds_account(dao)
            
            # Migrate customers to accounts
            await migrate_customers_to_accounts(dao)
            
            logger.info("Migration completed successfully")
        else:
            logger.info("Dry run completed - no data was modified")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python
"""Debug script for account enrichment issues."""

import asyncio
import logging
from src.repositories.account_repository import AccountRepository
from src.repositories.firestore_dao import FirestoreDAO
from src.services.account_enrichment_service import AccountEnrichmentService

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def debug_account_enrichment():
    """Debug account enrichment issues."""
    try:
        # Initialize DAO and repositories
        dao = FirestoreDAO()
        account_repo = AccountRepository(dao)
        
        # Test legal entity retrieval
        legal_entity_uuid = "kiranakart-technologies-12345"
        logger.info(f"Testing BP account retrieval for legal entity: {legal_entity_uuid}")
        
        # Check if the legal entity exists
        legal_entity = await dao.get_document("legal_entity", legal_entity_uuid)
        if legal_entity:
            logger.info(f"Found legal entity: {legal_entity}")
        else:
            logger.error(f"Legal entity not found with UUID: {legal_entity_uuid}")
            
        # Check for BP accounts linked to this legal entity
        logger.info("Querying accounts collection for BP accounts linked to this legal entity...")
        account_data_list = await dao.query_documents(
            "accounts", 
            [
                ("legal_entity_uuid", "==", legal_entity_uuid),
                ("account_type", "==", "BP")
            ]
        )
        
        if account_data_list:
            logger.info(f"Found {len(account_data_list)} BP accounts for legal entity")
            for idx, account in enumerate(account_data_list):
                logger.info(f"Account {idx+1}: UUID={account.get('account_uuid')}, "
                          f"SAP ID={account.get('sap_account_id')}, "
                          f"Account Type={account.get('account_type')}")
        else:
            logger.error(f"No BP accounts found for legal entity {legal_entity_uuid}")
        
        # Try using the account repository method directly
        bp_account = await account_repo.get_bp_account_by_legal_entity(legal_entity_uuid)
        if bp_account:
            logger.info(f"BP account from repository: UUID={bp_account.account_uuid}, "
                      f"SAP ID={bp_account.sap_account_id}")
        else:
            logger.error("No BP account returned from repository method")
            
        # Check a recent payment advice and its lines
        logger.info("Checking a recent payment advice...")
        payment_advices = await dao.query_documents("payment_advice", 
                                                  [("legal_entity_uuid", "==", legal_entity_uuid)],
                                                  limit=1)
        if not payment_advices:
            logger.error("No payment advices found with this legal entity UUID")
            return
            
        payment_advice = payment_advices[0]
        payment_advice_uuid = payment_advice.get("payment_advice_uuid")
        logger.info(f"Found payment advice: {payment_advice_uuid}")
        
        # Check payment advice lines
        lines = await dao.query_documents("paymentadvice_lines", 
                                        [("payment_advice_uuid", "==", payment_advice_uuid)])
        logger.info(f"Found {len(lines)} payment advice lines")
        
        # Print details of first few lines
        for idx, line in enumerate(lines[:3]):
            logger.info(f"Line {idx+1}: account_type={line.get('account_type')}, "
                      f"bp_code={line.get('bp_code')}, "
                      f"gl_code={line.get('gl_code')}")
        
        # Test account enrichment service
        enrichment_service = AccountEnrichmentService(dao)
        logger.info(f"Running enrichment for payment advice {payment_advice_uuid}...")
        result = await enrichment_service.enrich_payment_advice_lines(payment_advice_uuid)
        logger.info(f"Enrichment result: {result}")
        
        # Check lines after enrichment
        lines_after = await dao.query_documents("paymentadvice_lines", 
                                             [("payment_advice_uuid", "==", payment_advice_uuid)])
        logger.info(f"Found {len(lines_after)} payment advice lines after enrichment")
        
        # Print details of first few lines after enrichment
        for idx, line in enumerate(lines_after[:3]):
            logger.info(f"Line {idx+1} after: account_type={line.get('account_type')}, "
                      f"bp_code={line.get('bp_code')}, "
                      f"gl_code={line.get('gl_code')}")
            
    except Exception as e:
        logger.error(f"Error in debug_account_enrichment: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(debug_account_enrichment())

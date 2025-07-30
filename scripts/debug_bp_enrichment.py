#!/usr/bin/env python
import os
import sys
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add src directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Set environment variables for Firestore
os.environ["FIRESTORE_PROJECT_ID"] = "vaulted-channel-462118-a5"
os.environ["FIRESTORE_DATABASE_ID"] = "beco-payment-advice-dev"

from src.repositories.firestore_dao import FirestoreDAO
from src.repositories.account_repository import AccountRepository
from src.services.account_enrichment_service import AccountEnrichmentService

async def debug_bp_account():
    """Debug BP account lookup and enrichment process."""
    logger.info("Starting BP account debugging process")
    
    # Initialize Firestore DAO
    dao = FirestoreDAO()
    
    # Initialize repositories and services
    account_repo = AccountRepository(dao)
    enrichment_service = AccountEnrichmentService(dao)
    
    # 1. Check the "account" collection for BP accounts
    logger.info("Step 1: Checking 'account' collection for BP accounts")
    bp_accounts = await dao.query_documents(
        "account", 
        [("account_type", "==", "BP")],
        limit=10
    )
    
    if bp_accounts:
        logger.info(f"Found {len(bp_accounts)} BP accounts in 'account' collection:")
        for i, acc in enumerate(bp_accounts):
            logger.info(f"BP Account {i+1}: UUID={acc.get('account_uuid')}, SAP ID={acc.get('sap_account_id')}, Legal Entity={acc.get('legal_entity_uuid')}")
    else:
        logger.error("No BP accounts found in the 'account' collection")
        
    # 2. Get a sample legal entity to test
    logger.info("Step 2: Getting sample legal entities to test")
    legal_entities = await dao.query_documents(
        "legal_entity", 
        [],
        limit=5
    )
    
    if not legal_entities:
        logger.error("No legal entities found in the database.")
        return
        
    logger.info(f"Found {len(legal_entities)} legal entities:")
    for i, le in enumerate(legal_entities):
        logger.info(f"Legal Entity {i+1}: UUID={le.get('document_id', le.get('legal_entity_uuid'))}, Name={le.get('name')}")
    
    # 3. Test BP account lookup for each legal entity
    logger.info("Step 3: Testing BP account lookup for each legal entity")
    for le in legal_entities:
        legal_entity_uuid = le.get('document_id', le.get('legal_entity_uuid'))
        logger.info(f"Testing BP account lookup for legal entity {legal_entity_uuid}")
        
        # Test repository lookup
        bp_account = await account_repo.get_bp_account_by_legal_entity(legal_entity_uuid)
        if bp_account:
            logger.info(f"Success: Found BP account via repository: UUID={bp_account.account_uuid}, SAP ID={bp_account.sap_account_id}")
        else:
            logger.warning(f"Repository lookup failed for legal entity {legal_entity_uuid}")
            
        # Test direct query
        direct_accounts = await dao.query_documents(
            "account", 
            [
                ("legal_entity_uuid", "==", legal_entity_uuid),
                ("account_type", "==", "BP")
            ]
        )
        
        if direct_accounts:
            logger.info(f"Direct query found {len(direct_accounts)} BP accounts for legal entity {legal_entity_uuid}")
            for acc in direct_accounts:
                logger.info(f"Direct query account: UUID={acc.get('account_uuid')}, SAP ID={acc.get('sap_account_id')}")
        else:
            logger.warning(f"Direct query found no BP accounts for legal entity {legal_entity_uuid}")
    
    # 4. Get recent payment advices
    logger.info("Step 4: Getting recent payment advices")
    payment_advices = await dao.query_documents(
        "payment_advice", 
        [],
        limit=5
    )
    
    if not payment_advices:
        logger.error("No recent payment advices found.")
        return
    
    logger.info(f"Found {len(payment_advices)} recent payment advices:")
    for i, pa in enumerate(payment_advices):
        logger.info(f"Payment Advice {i+1}: UUID={pa.get('payment_advice_uuid')}, Legal Entity={pa.get('legal_entity_uuid')}")
    
    # 5. Test line enrichment for a payment advice
    logger.info("Step 5: Testing line enrichment for a payment advice")
    for pa in payment_advices:
        payment_advice_uuid = pa.get('payment_advice_uuid')
        legal_entity_uuid = pa.get('legal_entity_uuid')
        
        if not legal_entity_uuid:
            logger.warning(f"Payment advice {payment_advice_uuid} has no legal entity UUID, skipping")
            continue
            
        logger.info(f"Testing line enrichment for payment advice {payment_advice_uuid} with legal entity {legal_entity_uuid}")
        
        # Get payment advice lines
        lines = await enrichment_service.get_payment_advice_lines(payment_advice_uuid)
        if not lines:
            logger.warning(f"No payment advice lines found for {payment_advice_uuid}")
            continue
            
        logger.info(f"Found {len(lines)} payment advice lines")
        
        # Check if lines already have BP codes
        bp_codes_present = sum(1 for line in lines if line.get('bp_code'))
        logger.info(f"Lines with BP codes already present: {bp_codes_present} out of {len(lines)}")
        
        if bp_codes_present > 0:
            for line in lines[:5]:  # Show first 5 lines
                logger.info(f"Line {line.get('payment_advice_line_uuid')}: BP Code = {line.get('bp_code')}")
        
        # Categorize lines
        bp_lines, gl_lines = await enrichment_service.categorize_lines(lines)
        logger.info(f"Categorized into {len(bp_lines)} BP lines and {len(gl_lines)} GL lines")
        
        # Test BP line enrichment
        if bp_lines:
            logger.info(f"Testing BP line enrichment for {len(bp_lines)} BP lines")
            enriched_bp_lines = await enrichment_service.enrich_bp_lines(bp_lines, legal_entity_uuid)
            
            # Check if BP codes were applied
            bp_codes_after = sum(1 for line in enriched_bp_lines if line.get('bp_code'))
            logger.info(f"BP lines with BP codes after enrichment: {bp_codes_after} out of {len(enriched_bp_lines)}")
            
            if bp_codes_after > 0:
                for line in enriched_bp_lines[:5]:  # Show first 5 lines
                    logger.info(f"Enriched line {line.get('payment_advice_line_uuid')}: BP Code = {line.get('bp_code')}")
            
                # Update lines in Firestore if BP codes were added
                logger.info("Updating lines in Firestore with BP codes")
                for line in enriched_bp_lines:
                    line_uuid = line.get('payment_advice_line_uuid')
                    await dao.update_document("paymentadvice_lines", line_uuid, {"bp_code": line.get('bp_code')})
                    logger.info(f"Updated line {line_uuid} with BP code {line.get('bp_code')}")
                
                # Break after successful enrichment
                logger.info("Found and updated BP codes, stopping iteration")
                break
    
    logger.info("BP account debugging process completed")

if __name__ == "__main__":
    import asyncio
    asyncio.run(debug_bp_account())

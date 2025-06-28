"""Test script for SAP data enrichment."""

import asyncio
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add the project root to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import required modules
from models.firestore_dao import FirestoreDAO
from src.mocks.sap_client import MockSapClient
from src.batch_worker.sap_integration import SapIntegrator

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


async def seed_customers():
    """Seed customer data if needed"""
    # Execute seed_customers script
    from scripts.seed_customers import seed_customers as _seed_customers
    await _seed_customers()


async def enrich_payment_advice(payment_advice_uuid: str):
    """Enrich a payment advice with SAP data"""
    dao = FirestoreDAO()
    sap_client = MockSapClient()
    sap_integrator = SapIntegrator(dao, sap_client)
    
    # Get the payment advice
    payment_advice = await dao.get_document("payment_advice", payment_advice_uuid)
    if not payment_advice:
        logger.error(f"Payment advice {payment_advice_uuid} not found")
        return
        
    logger.info(f"Enriching payment advice {payment_advice_uuid} with SAP data")
    
    # Enrich invoices and other docs with SAP transaction IDs and customer UUIDs
    successful_updates, failed_updates = await sap_integrator.enrich_documents_with_sap_data(payment_advice_uuid)
    logger.info(f"Document enrichment complete: {successful_updates} successful, {failed_updates} failed updates")
    
    # Update settlements with customer UUIDs
    if successful_updates > 0:
        settlement_updates, settlement_failures = await sap_integrator.enrich_settlement_customer_data(payment_advice_uuid)
        logger.info(f"Settlement enrichment complete: {settlement_updates} successful, {settlement_failures} failed updates")


async def list_recent_payment_advices():
    """List recently created payment advices"""
    dao = FirestoreDAO()
    
    # Query for payment advices, ordered by created_at (most recent first)
    payment_advices = await dao.query_documents(
        "payment_advice",
        order_by="created_at",   # Use string for order_by
        desc=True,              # Use desc=True instead of descending
        limit=5
    )
    
    if not payment_advices:
        logger.error("No payment advices found")
        return []
        
    logger.info(f"Found {len(payment_advices)} recent payment advices:")
    for pa in payment_advices:
        created_at = pa.get("created_at")
        if isinstance(created_at, datetime):
            created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            created_at_str = "Unknown date"
            
        logger.info(f"  {pa['payment_advice_uuid']} (Created: {created_at_str}) - Status: {pa['payment_advice_status']}")
        
    return payment_advices


async def main():
    """Main test function"""
    try:
        # Load environment variables from .env file
        load_dotenv()
        
        # Display Firestore configuration
        logger.info(f"Using Firestore Project ID: {os.environ.get('FIRESTORE_PROJECT_ID')}")
        logger.info(f"Using Firestore Database ID: {os.environ.get('FIRESTORE_DATABASE_ID', 'beco-payment-advice-dev')}")
        # First, seed customer data if needed
        await seed_customers()
        
        # List recent payment advices
        payment_advices = await list_recent_payment_advices()
        
        if not payment_advices:
            logger.error("No payment advices found. Please run the batch worker first.")
            return
            
        # Ask which payment advice to enrich
        print("\nWhich payment advice would you like to enrich with SAP data?")
        for i, pa in enumerate(payment_advices):
            print(f"{i+1}. {pa['payment_advice_uuid']} - {pa.get('payer_name', 'Unknown payer')}")
            
        print(f"{len(payment_advices)+1}. Enrich all")
        print("q. Quit")
        
        choice = input("Enter your choice (1-5, 'all', or 'q'): ").lower()
        
        if choice == 'q':
            return
        elif choice in ['all', str(len(payment_advices)+1)]:
            # Enrich all payment advices
            for pa in payment_advices:
                await enrich_payment_advice(pa['payment_advice_uuid'])
        else:
            try:
                index = int(choice) - 1
                if 0 <= index < len(payment_advices):
                    # Enrich the selected payment advice
                    await enrich_payment_advice(payment_advices[index]['payment_advice_uuid'])
                else:
                    logger.error(f"Invalid choice: {choice}")
            except ValueError:
                logger.error(f"Invalid choice: {choice}")
                
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())

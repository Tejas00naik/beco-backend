"""Seed customer data in Firestore for testing purposes."""

import asyncio
import logging
import os
import sys

# Add the project root to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.firestore_dao import FirestoreDAO
from models.schemas import Customer

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

# Sample customer data that matches our mock SAP BP accounts
CUSTOMER_DATA = [
    {
        "customer_uuid": "amazon-services-customer-123",
        "customer_name": "Amazon Seller Services",
        "sap_customer_id": "BP001",
        "sap_customer_name": "Amazon Seller Services",
        "state": "Karnataka",
        "payment_term_in_days": 30,
        "is_active": True,
        "legal_entity_uuid": "amazon-services-123456"
    },
    {
        "customer_uuid": "amazon-development-customer-123",
        "customer_name": "Amazon Development Center",
        "sap_customer_id": "BP002",
        "sap_customer_name": "Amazon Development Center",
        "state": "Karnataka",
        "payment_term_in_days": 30,
        "is_active": True,
        "legal_entity_uuid": "amazon-development-123456"
    },
    {
        "customer_uuid": "flipkart-india-customer-123",
        "customer_name": "Flipkart India Private Limited",
        "sap_customer_id": "BP003",
        "sap_customer_name": "Flipkart India Private Limited",
        "state": "Karnataka",
        "payment_term_in_days": 45,
        "is_active": True,
        "legal_entity_uuid": "flipkart-india-123456"
    },
    {
        "customer_uuid": "myntra-designs-customer-123",
        "customer_name": "Myntra Designs",
        "sap_customer_id": "BP004",
        "sap_customer_name": "Myntra Designs",
        "state": "Karnataka",
        "payment_term_in_days": 30,
        "is_active": True,
        "legal_entity_uuid": "myntra-designs-123456"
    },
    {
        "customer_uuid": "amazon-clicktech-customer-123",
        "customer_name": "Clicktech Retail Private Limited",
        "sap_customer_id": "BP005",
        "sap_customer_name": "Clicktech Retail Private Limited",
        "state": "Maharashtra",
        "payment_term_in_days": 30,
        "is_active": True,
        "legal_entity_uuid": "amazon-clicktech-retail-123456"
    }
]


async def seed_customers():
    """
    Seed customer data in Firestore.
    """
    dao = FirestoreDAO()
    
    # Check if any customers already exist
    existing_customers = await dao.query_documents("customer")
    if existing_customers and len(existing_customers) > 0:
        logger.info(f"Found {len(existing_customers)} existing customers in the database")
        
        # Check if our test customers are already present
        sap_customer_ids = [c.get("sap_customer_id") for c in existing_customers]
        if all(c["sap_customer_id"] in sap_customer_ids for c in CUSTOMER_DATA):
            logger.info("All test customers already exist in the database. Skipping seeding.")
            return
    
    # Add each customer to Firestore
    for customer_data in CUSTOMER_DATA:
        customer_uuid = customer_data["customer_uuid"]
        
        # Check if this customer already exists
        existing = await dao.query_documents("customer", [("customer_uuid", "==", customer_uuid)])
        if existing and len(existing) > 0:
            logger.info(f"Customer {customer_uuid} already exists, skipping")
            continue
            
        # Create Customer object
        customer = Customer(**customer_data)
        
        # Add to Firestore
        await dao.add_document("customer", customer_uuid, customer.__dict__)
        logger.info(f"Added customer {customer_uuid} with SAP ID {customer_data['sap_customer_id']}")
    
    logger.info("Customer seeding complete")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(seed_customers())

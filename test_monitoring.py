"""
Test script to directly test the monitoring service with a specific email log UUID.
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

from src.services.monitoring_service import MonitoringService
from src.repositories.firestore_dao import FirestoreDAO
from src.external_apis.gcp.sheets_service import SheetsService
import src.config as config

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def main():
    # Load environment variables from .env
    load_dotenv()
    
    # Initialize services
    firestore_dao = FirestoreDAO()
    sheets_service = SheetsService()
    monitoring_service = MonitoringService(firestore_dao, sheets_service)
    
    # Email log UUID from Firestore
    email_log_uuid = "5fffbc45-6ce4-4183-8f70-eed89aa20789"
    
    logger.info(f"Testing monitoring service with email log UUID: {email_log_uuid}")
    
    # Update monitoring sheet
    result = await monitoring_service.update_after_batch_processing(email_log_uuid)
    
    if result:
        logger.info("Successfully updated monitoring sheet!")
    else:
        logger.error("Failed to update monitoring sheet.")

if __name__ == "__main__":
    asyncio.run(main())

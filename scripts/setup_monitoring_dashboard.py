#!/usr/bin/env python
"""
Setup and Test Script for Monitoring Dashboard

This script sets up the monitoring dashboard with Google Sheets
and tests the integration by populating it with data from Firestore.
"""

import os
import sys
import asyncio
from dotenv import load_dotenv
import logging

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import components
from src.services.monitoring_service import MonitoringService
from src.repositories.firestore_dao import FirestoreDAO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def setup_monitoring_sheet():
    """Set up the monitoring sheet with headers and formatting."""
    
    # Create monitoring service
    monitoring_service = MonitoringService()
    
    # Set up the sheet with headers and formatting
    success = await monitoring_service.setup_monitoring_sheet()
    
    if success:
        logger.info("Successfully set up monitoring sheet")
    else:
        logger.error("Failed to set up monitoring sheet")
    
    return success

async def populate_with_existing_data(limit: int = 100):
    """Populate the monitoring sheet with existing data from Firestore."""
    
    # Create monitoring service
    monitoring_service = MonitoringService()
    
    # Update the sheet with existing data
    success = await monitoring_service.update_monitoring_sheet(limit=limit)
    
    if success:
        logger.info(f"Successfully populated monitoring sheet with existing data")
    else:
        logger.error("Failed to populate monitoring sheet")
    
    return success

async def main():
    """Main entry point for the script."""
    
    # Load environment variables
    load_dotenv()
    
    # Setup the monitoring sheet
    await setup_monitoring_sheet()
    
    # Populate with existing data
    await populate_with_existing_data(limit=100)
    
    logger.info("Monitoring dashboard setup complete")

if __name__ == "__main__":
    asyncio.run(main())

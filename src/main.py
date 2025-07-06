"""Main Orchestrator for Payment Advice Batch Worker

This is the entry point for the batch worker that processes emails,
extracts payment advice data, stores it in Firestore, and calls SAP B1.
"""

import os
import sys
import logging
import asyncio
import argparse
from typing import Literal
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from os.path import abspath, dirname

# Add project root to Python path
project_root = dirname(dirname(abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import batch worker package
from src.batch_worker import BatchWorker

# Import configuration
from src.config import (
    TARGET_MAILBOX_ID,
    DEFAULT_GMAIL_CREDENTIALS_PATH
)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Payment Advice Batch Worker")
    parser.add_argument("--test", action="store_true", help="Run in test mode with dev_ collection prefix")
    parser.add_argument("--mode", choices=["incremental", "full_refresh"], default="incremental", 
                      help="Run mode: incremental or full_refresh")
    parser.add_argument("--gmail", action="store_true", help="Use Gmail adapter instead of mock email reader")
    parser.add_argument("--credentials", default=DEFAULT_GMAIL_CREDENTIALS_PATH, 
                      help=f"Path to Gmail API credentials file (default: {DEFAULT_GMAIL_CREDENTIALS_PATH})")
    parser.add_argument("--start-date", help="Start date for email fetching in YYYY-MM-DD format")
    parser.add_argument("--last-n", type=int, default=None, help="Only process the last N emails")
    
    args = parser.parse_args()
    
    # Also check environment variables
    is_test = args.test or os.environ.get("TEST_MODE", "false").lower() == "true"
    
    # Parse start date if provided
    start_date = None
    if args.start_date:
        try:
            # Parse the date as IST (UTC+5:30)
            ist_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            
            # Add IST timezone info
            ist = timezone(timedelta(hours=5, minutes=30))
            ist_date = ist_date.replace(tzinfo=ist)
            
            # Convert to UTC
            utc_date = ist_date.astimezone(timezone.utc)
            
            # Remove tzinfo to make it naive UTC (as expected by the rest of the code)
            start_date = utc_date.replace(tzinfo=None)
            
            logger.info(f"Using custom start date: {args.start_date} IST, converted to {start_date} UTC")
        except ValueError:
            logger.error(f"Invalid start date format: {args.start_date}. Should be YYYY-MM-DD")
            sys.exit(1)
    
    # Set environment variable for batch worker
    if start_date:
        os.environ["INITIAL_FETCH_START_DATE"] = args.start_date
    
    # Initialize and run the batch worker
    worker = BatchWorker(
        is_test=is_test,
        mailbox_id=TARGET_MAILBOX_ID,  # Use hardcoded mailbox ID from config
        run_mode=args.mode,
        use_gmail=args.gmail,
        gmail_credentials_path=args.credentials,
        since_timestamp=start_date if start_date else None,  # Pass the start date directly to BatchWorker
        last_n_emails=args.last_n  # Limit to last N emails if specified
    )
    
    logger.info(f"Using hardcoded mailbox ID: {TARGET_MAILBOX_ID}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

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

# Import batch worker packages
from src.batch_worker import BatchWorker
from src.batch_worker.batch_worker_v2 import BatchWorkerV2

# Import configuration
from src.config import (
    TARGET_MAILBOX_ID,
    DEFAULT_GMAIL_CREDENTIALS_PATH
)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Payment Advice Batch Worker")
    parser.add_argument("--mode", choices=["incremental", "full_refresh"], default="incremental", 
                      help="Run mode: incremental or full_refresh")
    parser.add_argument("--v2", action="store_true", help="Use BatchWorkerV2 for Zepto payment advice processing")
    
    # Add batch/single mode processing arguments
    parser.add_argument("--batch", action="store_true", help="Run in batch mode (process multiple emails, default)")
    parser.add_argument("--single", action="store_true", help="Run in single email mode (process one specific email)")
    parser.add_argument("--email-id", help="Email ID to process (required for --single mode)")
    
    # Optional email fetching parameters
    # Only used if you want to override the default behavior (fetch since last processed email)  
    parser.add_argument("--start-date", help="Override: Start date for email fetching in YYYY-MM-DD format")
    parser.add_argument("--last-n", type=int, default=None, help="Override: Only process the last N emails")
    
    
    args = parser.parse_args()
    
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

    # Initialize and run the batch worker
    if args.v2:
        logger.info("Using BatchWorkerV2 for Zepto payment advice processing")
        worker = BatchWorkerV2(
            is_test=False,  # Always use production mode
            mailbox_id=TARGET_MAILBOX_ID,  # Use hardcoded mailbox ID from config
            run_mode=args.mode,
            use_gmail=True,  # Always use real Gmail
            gmail_credentials_path=DEFAULT_GMAIL_CREDENTIALS_PATH,  # Always use default credentials path
            since_timestamp=start_date if start_date else None,
            last_n_emails=args.last_n  # Limit to last N emails if specified
        )
    else:
        logger.info("Using standard BatchWorker")
        worker = BatchWorker(
            is_test=False,  # Always use production mode
            mailbox_id=TARGET_MAILBOX_ID,  # Use hardcoded mailbox ID from config
            run_mode=args.mode,
            use_gmail=True,  # Always use real Gmail
            gmail_credentials_path=DEFAULT_GMAIL_CREDENTIALS_PATH,  # Always use default credentials path
            since_timestamp=start_date if start_date else None,
            last_n_emails=args.last_n  # Limit to last N emails if specified
        )
    
    logger.info(f"Using hardcoded mailbox ID: {TARGET_MAILBOX_ID}")
    
    # Handle batch vs single mode operation
    if args.single:
        if not args.email_id:
            logger.error("--email-id is required when using --single mode")
            sys.exit(1)
            
        # Single email mode
        logger.info(f"Running in SINGLE EMAIL MODE for email ID: {args.email_id}")
        
        # Only BatchWorkerV2 has single email processing capability
        if not args.v2:
            logger.error("Single email mode is only supported with BatchWorkerV2 (--v2)")
            sys.exit(1)
            
        success = await worker.process_single_email(args.email_id)
        
        if success:
            logger.info(f"Successfully processed single email: {args.email_id}")
        else:
            logger.error(f"Failed to process single email: {args.email_id}")
            sys.exit(1)
    else:
        # Default to batch mode
        logger.info("Running in BATCH MODE")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

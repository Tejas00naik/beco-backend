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
    
    args = parser.parse_args()
    
    # Also check environment variables
    is_test = args.test or os.environ.get("TEST_MODE", "false").lower() == "true"
    
    # Initialize and run the batch worker
    worker = BatchWorker(
        is_test=is_test,
        mailbox_id=TARGET_MAILBOX_ID,  # Use hardcoded mailbox ID from config
        run_mode=args.mode,
        use_gmail=args.gmail,
        gmail_credentials_path=args.credentials
    )
    
    logger.info(f"Using hardcoded mailbox ID: {TARGET_MAILBOX_ID}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

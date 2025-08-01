#!/usr/bin/env python3
"""
Enhanced test script for local testing of Cloud Function processing
with improved error handling and compatibility with latest code changes
"""

import os
import sys
import asyncio
import json
import base64
import traceback
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Import cloud function's async implementation directly
from cloud_function.main import _async_process_pubsub_message

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Test the Cloud Function locally')
    parser.add_argument('--history-id', help='Gmail history ID to process', required=True)
    parser.add_argument('--email', help='Email address to use', default='paymentadvice@beco.co.in')
    return parser.parse_args()

# Create a mock PubSub event with history ID
def create_mock_event(history_id=None, email_address="paymentadvice@beco.co.in"):
    """Create a mock PubSub event for testing"""
    
    # Create a properly formatted Gmail notification
    # Real Gmail push notifications through Pub/Sub have this format in production:
    # {
    #   "message": {
    #     "data": "base64-encoded-json",
    #     "messageId": "pub-sub-message-id",
    #     "publishTime": "timestamp"
    #   },
    #   "subscription": "projects/project-id/subscriptions/subscription-name"
    # }
    # 
    # The actual Gmail notification data (after base64 decoding) is:
    # {
    #   "emailAddress": "paymentadvice@beco.co.in",
    #   "historyId": "7922"
    # }
    
    # Create the notification data - standard Gmail notification format
    notification_data = {
        "emailAddress": email_address
    }
    
    # Add history_id (either provided or default)
    # Use 7922 as default since we know there are messages after this ID
    if history_id:
        notification_data["historyId"] = history_id
    else:
        notification_data["historyId"] = "7922" # A known history ID with messages after it
    
    # Convert data to JSON string and print for debugging
    json_data = json.dumps(notification_data)
    logger.info(f"Created mock Gmail notification with data: {json_data}")
    
    # Base64 encode the Gmail notification data
    base64_encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
    
    # Create a mock PubSub event that more closely matches production structure
    # But we simplify it to just what our Cloud Function actually needs
    mock_event = {
        "data": base64_encoded_data
    }
    
    return mock_event

# Create a mock context object
class MockContext:
    def __init__(self):
        self.event_id = "test-event-id"
        self.timestamp = "2025-07-29T01:10:00Z"
        self.resource = {"name": "projects/test-project/topics/gmail-notifications"}

async def test_gmail_notification(history_id):
    """Test processing with history_id as it would come from a real Gmail notification"""
    logger.info("\n===== Testing with Gmail notification format =====\n")
    
    # Use the history ID passed in via command line arguments
    logger.info(f"Using history ID: {history_id}")
    
    # Create mock event with history ID only (Gmail notifications only contain historyId)
    event = create_mock_event(history_id=history_id)
    context = MockContext()
    
    logger.info("\n>>> EXPECTED BEHAVIOR: Cloud Function should:")
    logger.info(">>> 1. Extract the historyId from the notification (but will ignore it)")
    logger.info(">>> 2. Call gmail_reader.get_most_recent_emails() to get the latest emails directly")
    logger.info(">>> 3. Check if the message has already been processed (idempotency check)")
    logger.info(">>> 4. Process the most recent unprocessed email\n")
    
    # Call the cloud function's async implementation directly
    try:
        logger.info("Starting test with Gmail notification format...")
        result = await _async_process_pubsub_message(event, context)
        logger.info(f"\nTest complete! Result: {result}")
        
        # Give guidance on what to look for in the logs
        logger.info("\nLook at the logs above to confirm:")
        logger.info("1. Gmail watch was refreshed at the beginning")
        logger.info("2. get_most_recent_emails() was called to get the latest email directly")
        logger.info("3. Idempotency check was performed before processing")
        logger.info("4. A single email was processed (if it wasn't already processed before)")
    except Exception as e:
        logger.error(f"\nERROR: {str(e)}")
        logger.error(traceback.format_exc())

async def test_with_invalid_notification():
    """Test processing with an invalid notification (missing historyId)"""
    logger.info("\n===== Testing with invalid notification (missing historyId) =====")
    
    # Create mock event with missing historyId
    mock_event = {
        "message": {
            "data": base64.b64encode(json.dumps({"emailAddress": "paymentadvice@beco.co.in"}).encode('utf-8')).decode('utf-8')
        }
    }
    mock_context = MockContext()
    
    logger.info("Starting test with invalid notification - this should be rejected")
    try:
        result = await _async_process_pubsub_message(mock_event, mock_context)
        logger.info(f"Result: {result}")
        logger.info("\nExpected: Function should reject this notification as invalid (missing historyId)")
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        logger.error(traceback.format_exc())

async def main():
    """Run the tests"""
    print("\n========================================")
    print("CLOUD FUNCTION GMAIL NOTIFICATION TEST HARNESS")
    print("========================================\n")
    
    print("This test script verifies that the Cloud Function correctly processes:")
    print("1. Gmail notifications (using our NEW approach to fetch recent emails directly)")
    print("2. Invalid notifications (missing historyId)")
    print("NOTE: The historyId is still needed for the notification format, but the")
    print("      Cloud Function now IGNORES it and fetches the latest email directly instead\n")
    
    # Parse command line arguments
    args = parse_args()
    
    if not args.history_id:
        logger.error("Error: --history-id is required")
        sys.exit(1)
    
    print("TESTING SCENARIO 1: VALID GMAIL NOTIFICATION WITH HISTORY ID")
    await test_gmail_notification(args.history_id)
    
    print("\nTESTING SCENARIO 2: INVALID NOTIFICATION WITHOUT HISTORY ID")
    await test_with_invalid_notification()

if __name__ == "__main__":
    asyncio.run(main())

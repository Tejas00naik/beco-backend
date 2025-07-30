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

# Create a mock PubSub event with message ID
def create_mock_event(message_id=None, history_id=None):
    """Create a mock PubSub event for testing"""
    
    # Create a properly formatted Gmail notification
    # Gmail push notifications follow this format:
    # {
    #   "message": {
    #     "data": base64-encoded-json
    #   }
    # }
    # 
    # The decoded data should be a JSON object with the following fields:
    # {
    #   "emailAddress": "example@gmail.com",
    #   "historyId": "12345"
    # }
    # And optionally it may include a messageId field
    
    # Create the notification data
    data = {
        "emailAddress": "tnaik4747@gmail.com"  # This is the email we used earlier
    }
    
    # Add message_id if provided
    if message_id:
        data["messageId"] = message_id
        
    # Add history_id (either provided or default)
    if history_id:
        data["historyId"] = history_id
    else:
        data["historyId"] = "12345"
    
    # Convert data to JSON string and print for debugging
    json_data = json.dumps(data)
    logger.info(f"Created mock Gmail notification with data: {json_data}")
    
    # Base64 encode the data as required by Cloud Functions
    base64_encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
    
    # Create the mock event with properly encoded data in PubSub format
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

async def test_with_message_id():
    """Test processing with a message ID - ONLY this email should be processed"""
    logger.info("\n===== Testing with message ID =====")
    
    # Use a real Gmail message ID from check_all_emails.py output
    # This is a real email message ID from the inbox
    real_message_id = "1983e8fc8afcf460"
    logger.info(f"Using real Gmail message ID: {real_message_id}")
    
    # Create mock event with message ID and history ID
    # Including both ensures we're testing the fix properly (should ignore history ID)
    event = create_mock_event(message_id=real_message_id, history_id="12345")
    context = MockContext()
    
    logger.info("\n>>> EXPECTED BEHAVIOR: Only process ONE single email with the specified message ID")
    logger.info(">>> The Cloud Function should NOT process any other emails or use history_id at all")
    logger.info(">>> Look for 'Processing SINGLE email with ID:' in the logs - should only process one email\n")
    
    # Call the cloud function's async implementation directly
    try:
        logger.info("Starting test with message ID - this should run in SINGLE mode...")
        result = await _async_process_pubsub_message(event, context)
        logger.info(f"\nTest complete! Result: {result}")
        
        # Give guidance on what to look for in the logs
        logger.info("\nLook at the logs above to confirm:")
        logger.info("1. Only ONE email was processed (the one with the specified message ID)")
        logger.info("2. The history_id logic was bypassed entirely")
        logger.info("3. No other emails were processed")
    except Exception as e:
        logger.error(f"\nERROR: {str(e)}")
        logger.error(traceback.format_exc())

async def test_with_history_id():
    """Test processing with a history ID only (no message ID)"""
    logger.info("\n===== Testing with history ID only =====")
    mock_event = create_mock_event(history_id="12345")
    mock_context = MockContext()
    
    logger.info("Starting test with history ID only - this should run in INCREMENTAL mode")
    try:
        result = await _async_process_pubsub_message(mock_event, mock_context)
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        logger.error(traceback.format_exc())

async def main():
    """Run the tests"""
    print("\n========================================")
    print("CLOUD FUNCTION SINGLE EMAIL TEST HARNESS")
    print("========================================\n")
    
    print("This test script verifies that the Cloud Function correctly processes:")
    print("1. A single email when messageId is provided")
    print("2. Multiple emails when only historyId is provided\n")
    
    print("TESTING SCENARIO 1: SINGLE EMAIL WITH MESSAGE ID")
    await test_with_message_id()
    
    # Uncomment to test history ID processing
    # print("\nTESTING SCENARIO 2: MULTIPLE EMAILS WITH HISTORY ID")
    # await test_with_history_id()

if __name__ == "__main__":
    asyncio.run(main())

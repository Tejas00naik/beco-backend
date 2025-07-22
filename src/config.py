"""
Configuration constants for the batch worker application.
"""

import os
from datetime import datetime

# Email and mailbox configuration
TARGET_MAILBOX_ID = "me"  # Special value that refers to the authenticated user's mailbox in Gmail API
ALLOWED_MAILBOX_IDS = ["me"]  # List of allowed mailbox IDs (currently only one)

# Email fetch defaults
DEFAULT_FETCH_DAYS = 0  # Default days to look back for emails (0 = only today)

# Check for environment variable override first
INITIAL_FETCH_START_DATE = os.environ.get("INITIAL_FETCH_START_DATE", datetime.now().strftime('%Y-%m-%d'))

# Collection prefixes
TEST_COLLECTION_PREFIX = "dev_"
PROD_COLLECTION_PREFIX = ""

# File paths
DEFAULT_GMAIL_CREDENTIALS_PATH = "secrets/email-client-secret.json"

# GCS configuration
DEFAULT_GCS_BUCKET_NAME = "beco-mails"  # Default bucket name if not specified

# Email file naming in GCS
EMAIL_OBJECT_FILENAME = "email.raw"  # Name of the file to store raw email content in GCS
EMAIL_TEXT_FILENAME = "email.txt"  # Plain text version of email
EMAIL_HTML_FILENAME = "email.html"  # HTML version of email
ATTACHMENT_PREFIX = "attachment_"  # Prefix for attachment filenames

# Account and SAP export configuration
TDS_ACCOUNT_NAME = "TDS Account"  # Default name for TDS account
TDS_ACCOUNT_CODE = "30-03-04-06-001"  # Default GL code for TDS account

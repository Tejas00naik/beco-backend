"""
Configuration constants for the batch worker application.
"""

# Email and mailbox configuration
TARGET_MAILBOX_ID = "me"  # Special value that refers to the authenticated user's mailbox in Gmail API
ALLOWED_MAILBOX_IDS = ["me"]  # List of allowed mailbox IDs (currently only one)

# Default values
DEFAULT_FETCH_DAYS = 1  # Default number of days to fetch emails if no start date is provided

# Collection prefixes
TEST_COLLECTION_PREFIX = "dev_"
PROD_COLLECTION_PREFIX = ""

# File paths
DEFAULT_GMAIL_CREDENTIALS_PATH = "secrets/email-client-secret.json"

# GCS configuration
EMAIL_OBJECT_FILENAME = "email.raw"  # Name of the file to store raw email content in GCS
DEFAULT_GCS_BUCKET_NAME = "beco-mails"  # Default bucket name if not specified

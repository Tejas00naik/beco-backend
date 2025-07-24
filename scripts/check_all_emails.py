#!/usr/bin/env python3
from src.external_apis.gcp.gmail_reader import GmailReader
from datetime import datetime, timedelta

def main():
    # Initialize the Gmail reader
    reader = GmailReader(
        credentials_path='secrets/email-client-secret.json',
        token_path='secrets/token.json',
        mailbox_id='paymentadvice'
    )
    
    # First check for ALL recent emails (past 2 hours)
    print("Checking for emails in the past 2 hours...")
    start_time = datetime.now() - timedelta(hours=2)
    emails = reader.get_unprocessed_emails(since_timestamp=start_time)
    
    print(f'Found {len(emails)} emails in the past 2 hours')
    
    # Print details of each email
    for email in emails:
        print(f"Subject: {email.get('subject')}")
        print(f"From: {email.get('sender_mail')}")
        print(f"Received at: {email.get('received_at')}")
        print(f"Gmail ID: {email.get('gmail_id')}")
        print("-" * 50)
    
    # Now check for ALL emails with no time filter
    print("\nChecking for ALL recent emails (no time filter, limit 5)...")
    emails = reader.get_unprocessed_emails(since_timestamp=None)
    
    print(f'Found {len(emails)} total recent emails')
    
    # Print details of each email (up to 5)
    for email in emails[:5]:
        print(f"Subject: {email.get('subject')}")
        print(f"From: {email.get('sender_mail')}")
        print(f"Received at: {email.get('received_at')}")
        print(f"Gmail ID: {email.get('gmail_id')}")
        print("-" * 50)

if __name__ == "__main__":
    main()

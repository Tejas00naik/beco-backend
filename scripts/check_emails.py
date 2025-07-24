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
    
    # Get emails from the last 30 minutes
    print("Checking for recent emails...")
    start_time = datetime.now() - timedelta(minutes=30)
    emails = reader.get_unprocessed_emails(since_timestamp=start_time)
    
    print(f'Found {len(emails)} emails in the last 30 minutes')
    
    # Print details of each email
    for email in emails:
        print(f"Subject: {email.get('subject')}")
        print(f"From: {email.get('sender_mail')}")
        print(f"Received at: {email.get('received_at')}")
        print(f"Gmail ID: {email.get('gmail_id')}")
        print("-" * 50)

if __name__ == "__main__":
    main()

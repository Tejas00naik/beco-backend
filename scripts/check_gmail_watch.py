#!/usr/bin/env python3
from src.external_apis.gcp.gmail_reader import GmailReader
from src.repositories.gmail_watch_repository import GmailWatchRepository
from src.dao.firestore_dao import FirestoreDAO
import os
import json

def main():
    # Initialize the Gmail reader
    reader = GmailReader(
        credentials_path='secrets/email-client-secret.json',
        token_path='secrets/token.json',
        mailbox_id='paymentadvice'
    )
    
    # Initialize FirestoreDAO and GmailWatchRepository
    dao = FirestoreDAO()
    repo = GmailWatchRepository(dao=dao)
    
    print("Checking Gmail watch status...")
    
    # Check token file
    if os.path.exists('secrets/token.json'):
        with open('secrets/token.json', 'r') as f:
            token_data = json.load(f)
            print(f"Token file exists for: {token_data.get('client_id', 'Unknown')}")
            print(f"Scopes: {token_data.get('scopes', [])}")
    else:
        print("Token file does not exist!")
    
    # Check existing watch status in Firestore
    email_address = "paymentadvice@beco.co.in"
    watch_status = repo.get_latest_watch_status(email_address)
    
    if watch_status:
        print(f"Found watch status in Firestore:")
        print(f"  Email: {watch_status.email}")
        print(f"  Last refreshed: {watch_status.last_refresh_time}")
        print(f"  Expires at: {watch_status.expiry_time}")
        print(f"  Status: {'ACTIVE' if watch_status.is_active else 'INACTIVE'}")
        print(f"  History ID: {watch_status.history_id}")
    else:
        print(f"No watch status found for {email_address}")
    
    # Check watch status from Gmail API
    print("\nTrying to refresh watch (this will show current status)...")
    reader.check_and_refresh_watch(email_address=email_address, dao=dao)
    
    # Check again after refresh attempt
    watch_status = repo.get_latest_watch_status(email_address)
    if watch_status:
        print(f"\nUpdated watch status in Firestore:")
        print(f"  Email: {watch_status.email}")
        print(f"  Last refreshed: {watch_status.last_refresh_time}")
        print(f"  Expires at: {watch_status.expiry_time}")
        print(f"  Status: {'ACTIVE' if watch_status.is_active else 'INACTIVE'}")
        print(f"  History ID: {watch_status.history_id}")
    else:
        print(f"\nStill no watch status found after refresh attempt")

if __name__ == "__main__":
    main()

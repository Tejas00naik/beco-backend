#!/usr/bin/env python
# Script to reset the Gmail watch timestamp in Firestore

import asyncio
from src.repositories.firestore_dao import FirestoreDAO
from dotenv import load_dotenv
load_dotenv()
async def reset_gmail_watch_timestamp():
    """Reset the Gmail watch timestamp to force a refresh on next Cloud Function run"""
    print("Initializing Firestore DAO...")
    dao = FirestoreDAO()
    
    # Set timestamp to a date in the past to force refresh
    old_date = "2020-01-01T00:00:00Z"
    print(f"Setting gmail_watch_last_refresh timestamp to {old_date}")
    
    # Check and update the collection name for gmail_watch_last_refresh
    collection_name = dao._get_collection_name('app_config')
    doc_ref = dao.db.collection(collection_name).document('gmail_watch_last_refresh')
    await doc_ref.set({'timestamp': old_date})
    
    print("Gmail watch timestamp reset successfully")

if __name__ == "__main__":
    asyncio.run(reset_gmail_watch_timestamp())

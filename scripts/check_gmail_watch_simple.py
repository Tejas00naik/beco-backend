#!/usr/bin/env python3
from src.external_apis.gcp.gmail_reader import GmailReader
import os
import json
from datetime import datetime
import googleapiclient.discovery
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def main():
    # Check token file
    token_path = 'secrets/token.json'
    credentials_path = 'secrets/email-client-secret.json'
    email_address = "paymentadvice@beco.co.in"
    
    print("Checking Gmail token and watch status...")
    
    if os.path.exists(token_path):
        with open(token_path, 'r') as f:
            token_data = json.load(f)
            print(f"Token file exists for: {token_data.get('client_id', 'Unknown')}")
            print(f"Scopes: {token_data.get('scopes', [])}")
            print(f"Token type: {token_data.get('token_type', 'Unknown')}")
    else:
        print("Token file does not exist!")
        return
    
    # Initialize the Gmail reader
    reader = GmailReader(
        credentials_path=credentials_path,
        token_path=token_path,
        mailbox_id='paymentadvice'
    )
    
    # Load credentials directly to check watch status
    try:
        # Get credentials
        creds = Credentials.from_authorized_user_info(json.load(open(token_path)), 
                                                    ['https://www.googleapis.com/auth/gmail.readonly',
                                                     'https://www.googleapis.com/auth/gmail.modify',
                                                     'https://www.googleapis.com/auth/gmail.settings.basic'])
        
        # Create Gmail API service
        gmail_service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)
        
        # Try to get the current watch status (this doesn't exist in Gmail API)
        # but we can check if our credentials work by listing labels
        labels = gmail_service.users().labels().list(userId='me').execute()
        print(f"\nAuthentication successful! Found {len(labels.get('labels', []))} labels")
        
        # Check for any notifications topic in the Gmail account
        print("\nChecking for active watch...")
        try:
            # Try setting a new watch to see response
            topic_name = f"projects/vaulted-channel-462118-a5/topics/gmail-notifications"
            request = {
                'labelIds': ['INBOX'],
                'topicName': topic_name,
                'labelFilterBehavior': 'INCLUDE'
            }
            watch_response = gmail_service.users().watch(userId='me', body=request).execute()
            print("\nWatch set successfully!")
            print(f"History ID: {watch_response.get('historyId')}")
            print(f"Expiration: {watch_response.get('expiration')}")
            
            # Convert expiration from milliseconds to datetime
            if 'expiration' in watch_response:
                exp_ms = int(watch_response['expiration'])
                exp_date = datetime.fromtimestamp(exp_ms / 1000)
                print(f"Expires on: {exp_date}")
        except Exception as e:
            print(f"\nError setting watch: {str(e)}")
            
    except Exception as e:
        print(f"Error accessing Gmail API: {str(e)}")
        
if __name__ == "__main__":
    main()

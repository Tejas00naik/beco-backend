#!/usr/bin/env python3
"""
Script to get the latest Gmail message ID and history ID
"""

import os
import sys
import json
from google.oauth2.credentials import Credentials
import googleapiclient.discovery

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main():
    # Setup paths
    token_path = os.path.abspath('secrets/token.json')
    credentials_path = os.path.abspath('secrets/email-client-secret.json')
    
    print(f"Checking token at: {token_path}")
    print(f"Checking credentials at: {credentials_path}")
    
    if not os.path.exists(token_path):
        print("Token file does not exist!")
        return
        
    if not os.path.exists(credentials_path):
        print("Credentials file does not exist!")
        return
    
    # Load credentials
    with open(token_path, 'r') as f:
        creds = Credentials.from_authorized_user_info(json.load(f), 
                                                    ['https://www.googleapis.com/auth/gmail.readonly',
                                                     'https://www.googleapis.com/auth/gmail.modify'])
    
    # Create Gmail API service
    gmail_service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)
    
    # Get number of emails to retrieve from command line or default to 5
    import argparse
    parser = argparse.ArgumentParser(description='Get Gmail message information')
    parser.add_argument('-n', '--num-emails', type=int, default=5, help='Number of recent emails to retrieve')
    args = parser.parse_args()
    num_emails = args.num_emails
    
    print(f"\nRetrieving information for {num_emails} recent emails...\n")
    
    # Get recent messages
    results = gmail_service.users().messages().list(userId='me', maxResults=num_emails).execute()
    messages = results.get('messages', [])
    
    if not messages:
        print("No messages found.")
        return
    
    # Store the most recent history ID
    latest_history_id = None
        
    # Process each message
    for idx, message_info in enumerate(messages, 1):
        message_id = message_info['id']
        
        print(f"\n{'=' * 80}")
        print(f"EMAIL {idx}: Message ID: {message_id}")
        print(f"{'-' * 80}")
        
        try:
            # Get the full message with all details
            message = gmail_service.users().messages().get(userId='me', id=message_id, format='full').execute()
            history_id = message.get('historyId')
            
            # Save the most recent history ID (first message)
            if idx == 1:
                latest_history_id = history_id
                
            print(f"History ID: {history_id}")
            
            # Extract email details
            headers = {header['name']: header['value'] for header in message['payload']['headers']}
            
            # Print email metadata
            sender = headers.get('From', 'Unknown')
            subject = headers.get('Subject', 'No Subject')
            date = headers.get('Date', 'Unknown Date')
            
            print(f"Date: {date}")
            print(f"From: {sender}")
            print(f"Subject: {subject}")
            
            # Extract attachments
            attachments = []
            
            # Function to recursively extract attachments from message parts
            def extract_attachments(payload):
                if 'parts' in payload:
                    for part in payload['parts']:
                        if part.get('filename') and part['filename'].strip():
                            attachments.append({
                                'filename': part['filename'],
                                'mimeType': part.get('mimeType', 'unknown')
                            })
                        if 'parts' in part:
                            extract_attachments(part)
            
            # Check if message has parts and extract attachments
            if 'parts' in message['payload']:
                extract_attachments(message['payload'])
            
            # Print attachment information
            if attachments:
                print(f"Attachments:")
                for att_idx, attachment in enumerate(attachments, 1):
                    print(f"  {att_idx}. {attachment['filename']} ({attachment['mimeType']})")
            else:
                print("No attachments")
                
            # Print command for this specific email
            print(f"\nTest command for this email:")
            print(f"python cloud_function/test_local_v2.py --message-id {message_id} --history-id {history_id} --email paymentadvice@beco.co.in")
            
        except Exception as e:
            print(f"Error retrieving details for message {message_id}: {str(e)}")
    
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'-' * 80}")
    print(f"Latest History ID: {latest_history_id}")
    print("\nTo test with a specific email, use one of the test commands shown above.")
    print("Make sure to choose an email with a Payment Advice PDF attachment.")
    print("\nUsage examples:")
    print("1. Get information about 10 recent emails:")
    print("   python scripts/get_latest_gmail_info.py --num-emails 10")
    print("2. Get information about 3 recent emails:")
    print("   python scripts/get_latest_gmail_info.py -n 3")
    
if __name__ == "__main__":
    main()

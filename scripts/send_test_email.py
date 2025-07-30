#!/usr/bin/env python3
"""
Simple script to send a test email to the payment advice inbox.
This will trigger the Gmail → PubSub → Cloud Function pipeline.
"""

import os
import sys
import time
import argparse
import datetime
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.external_apis.gcp.gmail_reader import GmailReader

# Configure colorful output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_section(title: str):
    """Print a section header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 50}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD} {title} {Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 50}{Colors.ENDC}\n")

def print_success(msg: str):
    """Print a success message"""
    print(f"{Colors.GREEN}✓ {msg}{Colors.ENDC}")

def print_warning(msg: str):
    """Print a warning message"""
    print(f"{Colors.WARNING}⚠ {msg}{Colors.ENDC}")

def print_error(msg: str):
    """Print an error message"""
    print(f"{Colors.FAIL}✗ {msg}{Colors.ENDC}")

def print_info(msg: str):
    """Print an info message"""
    print(f"{Colors.BLUE}ℹ {msg}{Colors.ENDC}")

async def send_test_email(gmail_reader, subject=None, recipient=None, pdf_path=None):
    """Send a test email to trigger the PubSub → Cloud Function pipeline"""
    print_section("SENDING TEST EMAIL")
    
    if not subject:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        subject = f"Payment Advice Test - Single Email Processing - {timestamp}"
    
    if not recipient:
        # Use the authenticated user's email as recipient
        recipient = gmail_reader.get_authenticated_email()
        print_info(f"No recipient specified, using authenticated user: {recipient}")
    
    # Default PDF path if not specified
    if not pdf_path:
        pdf_path = "/Users/macbookpro/RECOCENT/beco-backend/Payment_Advice_2000060222 (1).PDF"
    
    # Create a test email with a PDF attachment
    try:
        # Create a simple message
        service = gmail_reader.service
        message = MIMEMultipart()
        message['to'] = recipient
        message['subject'] = subject
        
        # Email body
        body = f"""This is an automated test email sent at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This email was sent to verify the Gmail → PubSub → Cloud Function pipeline.

Please do not reply to this email."""
        message.attach(MIMEText(body))
        
        # Attach the specified PDF file
        if os.path.exists(pdf_path):
            # Attach PDF
            with open(pdf_path, "rb") as pdf_file:
                pdf_attachment = MIMEApplication(pdf_file.read(), _subtype="pdf")
                filename = os.path.basename(pdf_path)
                pdf_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
                message.attach(pdf_attachment)
                print_info(f"Attached PDF: {pdf_path}")
        else:
            print_error(f"PDF file not found: {pdf_path}")
            return None
        
        # Encode and send message
        import base64
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        send_message = service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
        message_id = send_message.get('id')
        print_success(f"Email sent successfully with Message ID: {message_id}")
        return message_id
        
    except Exception as e:
        print_error(f"Error sending test email: {str(e)}")
        import traceback
        print_error(traceback.format_exc())
        return None

async def main():
    """Main function to send test email"""
    parser = argparse.ArgumentParser(description="Send test email for Cloud Function pipeline")
    parser.add_argument("--client-secret", default="./secrets/email-client-secret.json", help="Path to Gmail client secret file")
    parser.add_argument("--recipient", default="paymentadvice@beco.co.in", help="Email recipient")
    parser.add_argument("--subject", default=None, help="Email subject line")
    parser.add_argument("--pdf", default="/Users/macbookpro/RECOCENT/beco-backend/Payment_Advice_2000060222 (1).PDF", help="Path to PDF attachment")
    args = parser.parse_args()
    
    # Normalize paths
    client_secret = os.path.abspath(args.client_secret) if args.client_secret else None
    pdf_path = os.path.abspath(args.pdf) if args.pdf else None
    
    # Initialize Gmail Reader
    try:
        # Find client_secret.json in the project directory if not specified
        if not client_secret:
            possible_paths = [
                "/Users/macbookpro/RECOCENT/beco-backend/secrets/email-client-secret.json",
                "/Users/macbookpro/RECOCENT/secrets/email-client-secret.json",
                "./secrets/email-client-secret.json"
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    client_secret = path
                    break
        
        token_path = "/Users/macbookpro/RECOCENT/beco-backend/token.json"
        
        gmail_reader = GmailReader(
            credentials_path=client_secret,
            token_path=token_path,
            mailbox_id="paymentadvice"
        )
        print_success("Gmail reader initialized")
    except Exception as e:
        print_error(f"Failed to initialize Gmail reader: {str(e)}")
        return
    
    # Send test email
    message_id = await send_test_email(
        gmail_reader,
        subject=args.subject,
        recipient=args.recipient,
        pdf_path=pdf_path
    )
    
    if message_id:
        print_success(f"Test email successfully sent with Message ID: {message_id}")
        print_info("Now watch the PubSub notifications and Cloud Function logs for processing.")
        print_info(f"You can monitor logs with: gcloud functions logs read --project=vaulted-channel-462118-a5 --region=asia-south1 process_email")
    else:
        print_error("Failed to send test email.")

if __name__ == "__main__":
    asyncio.run(main())

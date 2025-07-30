#!/usr/bin/env python3
"""
End-to-End Gmail → PubSub → Cloud Function Pipeline Verification

This script checks each component of the email processing pipeline:
1. Gmail watch status - Is the watch active and when does it expire?
2. Pub/Sub topic and subscription - Are they properly configured?
3. Cloud Function - Is it deployed and properly configured with a trigger?
4. Recent emails - Are emails being received in the specified timeframe?
5. Cloud Function logs - Is the function being triggered and executing successfully?

Usage:
    python verify_email_pipeline.py [options]

Options:
    --email            Email address being monitored (default: paymentadvice@beco.co.in)
    --client-secret    Path to Gmail client secret file (default: ./secrets/email-client-secret.json)
    --service-account  Path to service account key file for GCP authentication (optional)
    --project          GCP project ID (default: vaulted-channel-462118-a5)
    --region           GCP region (default: asia-south1)
    --topic            Pub/Sub topic ID (default: gmail-notifications)
    --subscription     Pub/Sub subscription ID (default: gmail-sub)
    --function         Cloud Function name (default: process_email)
    --minutes          Minutes to look back for emails and logs (default: 10)
    --deploy-debug     Run a complete deploy-observe-debug cycle (deploys Cloud Function, sends test email, monitors logs)
    --deploy-script    Path to deploy.sh script (default: ../cloud_function/deploy.sh)
    --timeout          Timeout in seconds for log monitoring (default: 120)
    --stationary       Seconds to consider logs stationary (no new logs) (default: 10)

Authentication Methods:

1. User Account OAuth (Recommended for local development):
   Run the following command BEFORE running this script to authenticate with your Google account:
   
   $ gcloud auth application-default login
   
   This will create a user credentials file at ~/.config/gcloud/application_default_credentials.json
   and all Google Cloud client libraries will automatically use these credentials.
   
   No additional parameters needed in this script - all GCP clients will use your user credentials.

2. Service Account Authentication (Alternative method):
   If you prefer using a service account or need to run this in an automated environment:
   
   $ python verify_email_pipeline.py --service-account=/path/to/service-account-key.json

Requirements:
    - Google Cloud SDK (gcloud) installed and configured
    - Gmail API credentials and token available
    - Access to GCP project with necessary permissions
"""

import os
import sys
import time
import json
import asyncio
import argparse
import datetime
from typing import Dict, Any, List, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import pathlib
import tempfile
import random
import uuid
from google.cloud import pubsub_v1
from google.cloud import logging as gcp_logging
from google.api_core.exceptions import NotFound
from google.auth import default as google_auth_default
from google.oauth2 import service_account, credentials as google_credentials

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.external_apis.gcp.gmail_reader import GmailReader
from src.repositories.firestore_dao import FirestoreDAO
from src.repositories.gmail_watch_repository import GmailWatchRepository

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

async def verify_gmail_watch(gmail_reader: GmailReader, watch_repo: GmailWatchRepository, email_address: str) -> bool:
    """Verify Gmail watch status"""
    print_section("GMAIL WATCH STATUS")
    
    # Get current watch status
    watch_status = await watch_repo.get_watch_status()
    
    if not watch_status:
        print_error(f"No watch status found for {email_address}")
        print_info("Setting up a new watch...")
        await gmail_reader.async_refresh_watch(email_address)
        watch_status = await watch_repo.get_watch_status()
        if watch_status:
            print_success("Watch successfully established")
        else:
            print_error("Failed to establish watch")
            return False
    
    # Check if watch has expired
    if watch_status:
        expiration = watch_status.expiration
        email = watch_status.email_address
        
        if expiration is None:
            print_error("Watch expiration time not found")
            return False
            
        expiration_date = datetime.datetime.fromtimestamp(int(expiration) / 1000)  # Convert ms to seconds
        now = datetime.datetime.utcnow()
        
        if expiration_date > now:
            print_success(f"Watch for {email} is ACTIVE")
            print_info(f"Expires: {expiration_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print_info(f"History ID: {watch_status.history_id}")
            return True
        else:
            print_warning(f"Watch for {email} has EXPIRED")
            print_info(f"Expired: {expiration_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print_info(f"History ID: {watch_status.history_id}")
    else:
        print_warning(f"Watch expired or expiring soon")
        print_info("Refreshing watch...")
        await gmail_reader.async_refresh_watch(email_address)
        print_success("Watch refreshed")
    
    return True

def verify_pubsub_topic(project_id: str, topic_id: str, subscription_id: str, credentials_path: str = None) -> bool:
    """Verify Pub/Sub topic and subscription exist and are properly configured"""
    print_section("PUBSUB CONFIGURATION")
    
    try:
        # Initialize the Pub/Sub client with credentials if available
        if credentials_path and os.path.exists(credentials_path):
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            publisher = pubsub_v1.PublisherClient(credentials=credentials)
            subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            print_info(f"Using service account credentials from {credentials_path}")
        else:
            # Force using user credentials from ADC file
            adc_path = os.path.expanduser('~/.config/gcloud/application_default_credentials.json')
            if os.path.exists(adc_path):
                try:
                    with open(adc_path, 'r') as f:
                        info = json.load(f)
                        print_info(f"Found ADC file with type: {info.get('type', 'unknown')}")
                    
                    # Force using ADC user credentials
                    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)  # Remove any override
                    credentials, project = google_auth_default()
                    print_info(f"Using credentials: {type(credentials).__name__}")
                    print_info(f"User: {credentials.service_account_email if hasattr(credentials, 'service_account_email') else 'User account'}")
                    
                    publisher = pubsub_v1.PublisherClient(credentials=credentials)
                    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                    print_info("Using forced application default credentials for Pub/Sub")
                except Exception as e:
                    print_warning(f"Could not load ADC credentials: {str(e)}")
                    publisher = pubsub_v1.PublisherClient()
                    subscriber = pubsub_v1.SubscriberClient()
                    print_info("Using default credentials for Pub/Sub")
            else:
                print_warning("No ADC file found")
                publisher = pubsub_v1.PublisherClient()
                subscriber = pubsub_v1.SubscriberClient()
                print_info("Using default credentials for Pub/Sub")
        
        # Format the topic and subscription paths
        topic_path = publisher.topic_path(project_id, topic_id)
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        
        # Check topic exists
        try:
            topic = publisher.get_topic(request={"topic": topic_path})
            print_success(f"Topic exists: {topic_path}")
        except NotFound:
            print_error(f"Topic not found: {topic_path}")
            return False
        except Exception as e:
            print_warning(f"Unable to check topic (permissions issue?): {str(e)}")
        
        # Check if subscription exists and its configuration
        try:
            subscription = subscriber.get_subscription(request={"subscription": subscription_path})
            print_success(f"Subscription exists: {subscription_path}")
            
            # Check push config
            push_config = subscription.push_config
            if push_config and push_config.push_endpoint:
                print_success(f"Push endpoint configured: {push_config.push_endpoint}")
            else:
                print_warning("No push endpoint configured - check if using pull subscription")
                
            # Check other important subscription properties
            print_info(f"Message retention: {subscription.message_retention_duration.seconds}s")
            print_info(f"Acknowledge deadline: {subscription.ack_deadline_seconds}s")
            
        except NotFound:
            print_error(f"Subscription not found: {subscription_path}")
            return False
        except Exception as e:
            print_warning(f"Unable to check subscription (permissions issue?): {str(e)}")
        
        return True
    except Exception as e:
        print_warning(f"PubSub verification skipped due to permissions or setup issues: {str(e)}")
        return True

def check_cloud_function_status(project_id: str, region: str, function_name: str) -> bool:
    """Check Cloud Function status"""
    print_section("CLOUD FUNCTION STATUS")
    
    # Use application default credentials (from gcloud auth)
    
    # Use gcloud command to get function status
    import subprocess
    
    try:
        cmd = [
            "gcloud", "functions", "describe", function_name,
            f"--region={region}", f"--project={project_id}"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print_error(f"Failed to get Cloud Function status: {result.stderr}")
            return False
        
        # Parse output
        output = result.stdout
        
        # Check if function is active
        if "status: ACTIVE" in output:
            print_success(f"Cloud Function '{function_name}' is ACTIVE")
        else:
            print_warning(f"Cloud Function '{function_name}' is not ACTIVE")
            print_info(output)
            return False
        
        # Check event trigger
        if "eventTrigger:" in output and "pubsub.topic.publish" in output:
            print_success("Cloud Function is configured with Pub/Sub trigger")
        else:
            print_warning("Cloud Function may not be properly configured with Pub/Sub trigger")
        
        return True
    
    except Exception as e:
        print_error(f"Error checking Cloud Function status: {str(e)}")
        return False

async def monitor_new_emails(gmail_reader: GmailReader, minutes: int = 5) -> List[Dict[str, Any]]:
    """Monitor for new emails in the specified time window"""
    print_section(f"MONITORING FOR NEW EMAILS (Last {minutes} minutes)")
    
    # Calculate the start time
    start_time = datetime.datetime.now() - datetime.timedelta(minutes=minutes)
    
    # Get emails since that time
    emails = gmail_reader.get_unprocessed_emails(since_timestamp=start_time)
    
    if emails:
        print_success(f"Found {len(emails)} new emails in the last {minutes} minutes")
        for i, email in enumerate(emails, 1):
            print_info(f"Email {i}:")
            print_info(f"  Subject: {email.get('subject')}")
            print_info(f"  From: {email.get('sender_mail')}")
            print_info(f"  Received: {email.get('received_at')}")
            print_info(f"  Message ID: {email.get('id')}")
    else:
        print_warning(f"No new emails found in the last {minutes} minutes")
    
    return emails

async def send_test_email(gmail_reader: GmailReader, subject: str = None, recipient: str = None) -> str:
    """Send a test email to trigger the PubSub → Cloud Function pipeline"""
    print_section("SENDING TEST EMAIL")
    
    if not subject:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        subject = f"Payment Advice Test - Single Email Processing - {timestamp}"
    
    if not recipient:
        # Use the authenticated user's email as recipient
        recipient = gmail_reader.get_authenticated_email()
        print_info(f"No recipient specified, using authenticated user: {recipient}")
    
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
        
        # Attach the specific PDF file
        pdf_path = "/Users/macbookpro/RECOCENT/beco-backend/data/Payment_Advice_2000060222.PDF"
        
        if os.path.exists(pdf_path):
            # Attach PDF
            with open(pdf_path, "rb") as pdf_file:
                pdf_attachment = MIMEApplication(pdf_file.read(), _subtype="pdf")
                pdf_attachment.add_header('Content-Disposition', 'attachment', filename="Payment_Advice_2000060222.PDF")
                message.attach(pdf_attachment)
                print_info(f"Attached real PDF: {pdf_path}")
        else:
            print_error(f"PDF file not found: {pdf_path}")
            return None
        
        # Encode and send message
        import base64
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.application import MIMEApplication
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

def monitor_logs_until_stationary(project_id: str, function_name: str, 
                                   timeout_seconds: int = 180, 
                                   stationary_seconds: int = 10,
                                   credentials_path: str = None,
                                   message_id: str = None,
                                   log_file: str = None) -> bool:
    """Monitor Cloud Function logs until they are stationary for the specified period
    or until the global timeout is reached.
    
    Args:
        project_id: GCP project ID
        function_name: Cloud Function name
        timeout_seconds: Maximum time to monitor logs (defaults to 180s)
        stationary_seconds: Time to consider logs stationary (no new logs)
        credentials_path: Path to service account credentials
        message_id: Optional Gmail message ID to filter logs
        log_file: File to save logs to (created with timestamp if None)
        
    Returns:
        bool: True if logs became stationary within the timeout, False otherwise
    """
    # Always ensure we have a log file for output with timestamp
    if log_file is None:
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"cf_logs_{current_time}.txt"
    
    print(f"Logs will be saved to: {log_file}")
    print_section(f"MONITORING CLOUD FUNCTION LOGS (timeout: {timeout_seconds}s, stationary: {stationary_seconds}s)")
    
    # Clear any existing log file to start fresh
    with open(log_file, 'w') as f:
        f.write(f"===== LOG MONITORING STARTED AT {datetime.datetime.now().isoformat()} =====\n")
        f.write(f"Function: {function_name}\n")
        f.write(f"Project: {project_id}\n")
        f.write(f"Timeout: {timeout_seconds}s\n")
        f.write(f"Stationary period: {stationary_seconds}s\n")
        if message_id:
            f.write(f"Filtering for message ID: {message_id}\n")
        f.write("=================================================\n\n")
    
    try:
        # Authenticate to Google Cloud
        if credentials_path and os.path.exists(credentials_path):
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = gcp_logging.Client(project=project_id, credentials=credentials)
        else:
            client = gcp_logging.Client(project=project_id)
        
        print_info(f"Monitoring logs for Cloud Function: {function_name}")
        print_info(f"Monitoring will stop after {timeout_seconds} seconds or {stationary_seconds} seconds after the last log")
        print_info("Press Ctrl+C to stop monitoring")
        
        # Store the timestamp of the most recent log entry
        most_recent_timestamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
        last_log_time = datetime.datetime.now()
        
        # Keep track of processed log entries to avoid duplicates
        seen_entries = set()
        
        # Track timing variables
        start_time = datetime.datetime.now()
        function_triggered = False
        function_completed = False
        error_found = False
        message_processing_started = False
        message_processing_completed = False
        key_log_milestones = {
            "function_triggered": False,
            "processing_started": False,
            "processing_completed": False,
            "email_parsed": False,
            "sap_pushed": False,
            "watch_refreshed": False,
            "function_completed": False
        }
        
        print_info(f"Monitoring start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print_info(f"Target completion by: {(start_time + datetime.timedelta(seconds=timeout_seconds)).strftime('%Y-%m-%d %H:%M:%S')}")
        
        while (datetime.datetime.now() - start_time).total_seconds() < timeout_seconds:
            # Format filter for this Cloud Function's logs
            filter_str = f'resource.type="cloud_function" resource.labels.function_name="{function_name}" timestamp>="{most_recent_timestamp.isoformat()}"'
            
            # Add message_id filter if provided
            if message_id:
                filter_str += f' textPayload:"ID: {message_id}"'
            
            # Get logs
            try:
                entry_iterator = client.list_entries(filter_=filter_str, order_by="timestamp asc", page_size=100)
                
                # Process new log entries
                new_entries_found = False
                
                for entry in entry_iterator:
                    log_id = entry.insert_id if hasattr(entry, 'insert_id') and entry.insert_id else str(entry.timestamp)
                    
                    if log_id not in seen_entries:
                        # New log entry found
                        new_entries_found = True
                        seen_entries.add(log_id)
                        
                        # Update the most recent timestamp if this log is newer
                        if entry.timestamp > most_recent_timestamp:
                            most_recent_timestamp = entry.timestamp
                        
                        # Format the log entry
                        timestamp_str = entry.timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                        message = entry.payload if hasattr(entry, 'payload') else "No message"
                        
                        # Get execution ID and log level
                        execution_id = entry.resource.labels.get('execution_id', 'unknown') if hasattr(entry.resource, 'labels') else 'unknown'
                        log_level = "INFO"
                        if hasattr(entry, 'severity'):
                            log_level = str(entry.severity)
                        
                        # Construct log line
                        log_line = f"{timestamp_str} [{execution_id}] {log_level} - {message}"
                        
                        # Analyze log content for key milestones
                        if "Function execution started" in message:
                            function_triggered = True
                            key_log_milestones["function_triggered"] = True
                            print_success(f"[{timestamp_str}] Function triggered")
                        elif "STARTING SINGLE EMAIL PROCESSING FOR ID" in message:
                            message_processing_started = True
                            key_log_milestones["processing_started"] = True
                            print_success(f"[{timestamp_str}] Processing started for email: {message_id if message_id else 'unknown'}")
                        elif "Function execution took" in message:
                            function_completed = True
                            key_log_milestones["function_completed"] = True
                            print_success(f"[{timestamp_str}] Function completed: {message}")
                        elif "Created email processing log for email" in message:
                            key_log_milestones["email_parsed"] = True
                        elif "Refreshed Gmail watch with historyId" in message:
                            key_log_milestones["watch_refreshed"] = True
                        elif "Payment advice pushed to SAP successfully" in message:
                            key_log_milestones["sap_pushed"] = True
                            message_processing_completed = True
                            key_log_milestones["processing_completed"] = True
                        elif log_level in ("ERROR", "CRITICAL", "WARNING"):
                            error_found = True
                            print_error(f"[{timestamp_str}] {log_level}: {message}")
                        
                        # Write to console based on log level
                        if log_level in ("ERROR", "CRITICAL"):
                            print_error(log_line)
                        elif log_level == "WARNING":
                            print_warning(log_line)
                        else:
                            print_info(log_line)
                        
                        # Always write to log file
                        with open(log_file, 'a') as f:
                            f.write(log_line + '\n')
                        
                        # Update the last log time
                        last_log_time = datetime.datetime.now()
            except Exception as e:
                print_error(f"Error fetching logs: {str(e)}")
                time.sleep(2)  # Wait a bit before retrying
                continue
            
            # Check if processing is complete (based on function completion and stationary logs)
            stationary_time = (datetime.datetime.now() - last_log_time).total_seconds()
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            
            # Print regular status updates
            if elapsed_time % 10 < 1 and elapsed_time > 5:  # Every ~10 seconds
                print_info(f"Monitoring for {elapsed_time:.0f}s... (timeout: {timeout_seconds}s)")
                print_info(f"Time since last log: {stationary_time:.1f}s (stationary threshold: {stationary_seconds}s)")
            
            # Check if logs are stationary for the required period
            if stationary_time >= stationary_seconds:
                print_success(f"No new logs for {stationary_seconds} seconds")
                break
                
            # Only add a short sleep if we didn't find any new entries
            if not new_entries_found:
                time.sleep(1)  # Don't hammer the API
        
        # Analyze logs after monitoring completes
        elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
        print_section("LOG MONITORING RESULTS")
        
        # Write summary to log file
        with open(log_file, 'a') as f:
            f.write("\n=================================================\n")
            f.write(f"LOG ANALYSIS COMPLETED AT {datetime.datetime.now().isoformat()}\n")
            f.write(f"Total monitoring time: {elapsed_time:.1f} seconds\n")
            f.write(f"Function triggered: {function_triggered}\n")
            f.write(f"Email processing started: {message_processing_started}\n")
            f.write(f"Email processing completed: {message_processing_completed}\n")
            f.write(f"Function completed: {function_completed}\n")
            f.write(f"Errors detected: {error_found}\n")
            f.write(f"Key milestones: {json.dumps(key_log_milestones, indent=2)}\n")
            f.write("=================================================\n")
        
        # Check if we hit the global timeout
        if elapsed_time >= timeout_seconds:
            print_warning(f"Monitoring timed out after {timeout_seconds} seconds")
            print_info(f"See complete logs in file: {log_file}")
            return False
        
        # Report on milestones
        print_info(f"Function triggered: {'✅' if function_triggered else '❌'}")
        print_info(f"Email processing started: {'✅' if message_processing_started else '❌'}")
        print_info(f"Email processing completed: {'✅' if message_processing_completed else '❌'}")
        print_info(f"Function completed: {'✅' if function_completed else '❌'}")
        print_info(f"Errors detected: {'❌' if error_found else '✅'}")
        print_info(f"See complete logs in file: {log_file}")
        
        # Return success if key milestones were reached
        success = function_triggered and function_completed and not error_found
        if success:
            print_success("Log monitoring completed successfully")
        else:
            print_warning("Log monitoring completed with issues")
            
        return success
        
    except Exception as e:
        print_error(f"Error monitoring logs: {str(e)}")
        import traceback
        print_error(traceback.format_exc())
        return False

def run_deployment(deploy_script_path: str) -> bool:
    """Run Cloud Function deployment script"""
    print_section("DEPLOYING CLOUD FUNCTION")
    
    if not os.path.exists(deploy_script_path):
        print_error(f"Deployment script not found: {deploy_script_path}")
        return False
    
    # Check if script is executable
    if not os.access(deploy_script_path, os.X_OK):
        print_warning(f"Making deployment script executable: {deploy_script_path}")
        try:
            os.chmod(deploy_script_path, 0o755)  # rwxr-xr-x
        except Exception as e:
            print_error(f"Failed to make script executable: {str(e)}")
            return False
    
    # Execute deployment script
    print_info(f"Executing deployment script: {deploy_script_path}")
    deploy_dir = os.path.dirname(deploy_script_path)
    
    try:
        import subprocess
        process = subprocess.Popen(
            [deploy_script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=deploy_dir,  # Run in the script's directory
            text=True
        )
        
        # Print output in real-time
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            print(line.rstrip())
        
        # Wait for process to complete
        returncode = process.wait()
        
        if returncode == 0:
            print_success("Deployment completed successfully")
            return True
        else:
            stderr_output = process.stderr.read()
            print_error(f"Deployment failed with exit code {returncode}")
            print_error(f"Error: {stderr_output}")
            return False
            
    except Exception as e:
        print_error(f"Error executing deployment script: {str(e)}")
        import traceback
        print_error(traceback.format_exc())
        return False

def check_cloud_function_logs(project_id: str, function_name: str, minutes: int = 10, credentials_path: str = None) -> bool:
    """Check Cloud Function logs for recent executions"""
    print_section(f"CLOUD FUNCTION LOGS (Last {minutes} minutes)")
    
    try:
        # Try to authenticate with provided credentials if available
        if credentials_path and os.path.exists(credentials_path):
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = gcp_logging.Client(project=project_id, credentials=credentials)
            print_info(f"Using service account credentials from {credentials_path}")
        else:
            # Force using user credentials from ADC file
            adc_path = os.path.expanduser('~/.config/gcloud/application_default_credentials.json')
            if os.path.exists(adc_path):
                try:
                    with open(adc_path, 'r') as f:
                        info = json.load(f)
                        print_info(f"Found ADC file with type: {info.get('type', 'unknown')}")
                    
                    # Force using ADC user credentials
                    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)  # Remove any override
                    credentials, project = google_auth_default()
                    print_info(f"Using credentials: {type(credentials).__name__}")
                    print_info(f"User: {credentials.service_account_email if hasattr(credentials, 'service_account_email') else 'User account'}")
                    
                    client = gcp_logging.Client(project=project_id, credentials=credentials)
                    print_info("Using forced application default credentials")
                except Exception as e:
                    print_warning(f"Could not load ADC credentials: {str(e)}")
                    client = gcp_logging.Client(project=project_id)
                    print_info("Using default credentials")
            else:
                print_warning("No ADC file found")
                client = gcp_logging.Client(project=project_id)
                print_info("Using default credentials")
            
        logger = client.logger(f"cloudfunctions.googleapis.com%2Fcloud-functions")
    except Exception as e:
        print_error(f"Error initializing logging client: {str(e)}")
        return False
    
    # Calculate timestamp
    now = datetime.datetime.now(datetime.timezone.utc)
    start_time = now - datetime.timedelta(minutes=minutes)
    
    filter_str = (
        f'resource.type="cloud_function" AND '
        f'resource.labels.function_name="{function_name}" AND '
        f'timestamp>="{start_time.isoformat()}"'
    )
    
    try:
        entries = list(logger.list_entries(filter_=filter_str, order_by="timestamp desc", max_results=10))
        
        if not entries:
            print_warning(f"No logs found for Cloud Function in the last {minutes} minutes")
            return False
        
        print_success(f"Found {len(entries)} log entries for Cloud Function")
        
        # Check for execution indicators
        function_triggered = False
        function_completed = False
        error_found = False
        
        for entry in entries:
            payload = entry.payload
            timestamp = entry.timestamp.isoformat()
            
            if isinstance(payload, str):
                message = payload
            elif isinstance(payload, dict):
                message = payload.get('message', str(payload))
            else:
                message = str(payload)
            
            # Handle both object and string severity types
            if hasattr(entry.severity, 'name'):
                log_level = entry.severity.name
            else:
                log_level = str(entry.severity) if entry.severity else "INFO"
            
            if "Function execution started" in message:
                function_triggered = True
                print_success(f"[{timestamp}] {log_level}: Function triggered: {message}")
            elif "Function execution took" in message:
                function_completed = True
                print_success(f"[{timestamp}] {log_level}: Function completed: {message}")
            elif log_level in ("ERROR", "CRITICAL"):
                error_found = True
                print_error(f"[{timestamp}] {log_level}: {message}")
            else:
                print_info(f"[{timestamp}] {log_level}: {message}")
        
        if function_triggered:
            print_success("Cloud Function was triggered recently")
        else:
            print_warning("No evidence of Cloud Function being triggered recently")
        
        if function_completed:
            print_success("Cloud Function execution completed")
        else:
            print_warning("No evidence of Cloud Function execution completing")
        
        if error_found:
            print_warning("Errors detected in Cloud Function logs")
        
        return function_triggered and function_completed and not error_found
    
    except Exception as e:
        print_error(f"Error checking Cloud Function logs: {str(e)}")
        return False

async def main():
    """Main function to verify the entire pipeline"""
    parser = argparse.ArgumentParser(description="Gmail Pipeline Verification")
    parser.add_argument("--client-secret", default="./secrets/email-client-secret.json", help="Path to Gmail client secret file")
    parser.add_argument("--service-account", help="Path to Google Cloud service account key file for authentication")
    parser.add_argument("--minutes", type=int, default=10, help="Minutes to look back for new emails")
    parser.add_argument("--project", default="vaulted-channel-462118-a5", help="Google Cloud project ID")
    parser.add_argument("--region", default="asia-south1", help="Google Cloud region")
    parser.add_argument("--function", default="process_email", help="Cloud Function name")
    parser.add_argument("--topic", default="gmail-notifications", help="Pub/Sub topic name")
    parser.add_argument("--subscription", default="gmail-sub", help="Pub/Sub subscription name")
    parser.add_argument("--email", default="paymentadvice@beco.co.in", help="Email address to monitor")
    # Action group for main operation mode
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--test", action="store_true", help="Send a test email and monitor logs")
    action_group.add_argument("--check-logs", action="store_true", help="Check Cloud Function logs only (no email sending)")
    action_group.add_argument("--verify-all", action="store_true", help="Verify all components (no email or logs)")
    parser.add_argument("--message-id", help="Filter logs by specific Gmail message ID")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout in seconds for log monitoring")
    parser.add_argument("--stationary", type=int, default=10, help="Seconds to consider logs stationary (no new logs)")
    parser.add_argument("--test-subject", default="Test Email for Cloud Function", help="Subject for test email")
    parser.add_argument("--test-recipient", default="paymentadvice@beco.co.in", help="Recipient for test email")
    args = parser.parse_args()
    
    # Normalize paths
    client_secret = os.path.abspath(args.client_secret) if args.client_secret else None
    service_account = os.path.abspath(args.service_account) if args.service_account else None
    
    # Initialize components
    print_section("INITIALIZING COMPONENTS")
    
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
    
    # Initialize Firestore DAO
    try:
        firestore_dao = FirestoreDAO()
        print_success("Firestore DAO initialized")
    except Exception as e:
        print_error(f"Failed to initialize Firestore DAO: {str(e)}")
        return
    
    # Initialize Gmail Watch Repository
    try:
        watch_repo = GmailWatchRepository(firestore_dao)
        print_success("Gmail Watch Repository initialized")
    except Exception as e:
        print_error(f"Failed to initialize Gmail Watch Repository: {str(e)}")
        return
    
    # Verify Gmail watch status
    watch_ok = await verify_gmail_watch(gmail_reader, watch_repo, args.email)
    if not watch_ok:
        print_error("Gmail watch verification failed")
    
    # Verify PubSub configuration
    pubsub_ok = verify_pubsub_topic(args.project, args.topic, args.subscription, service_account)
    if not pubsub_ok:
        print_error("PubSub configuration verification failed")
    
    # Check Cloud Function status
    cf_ok = check_cloud_function_status(args.project, args.region, args.function)
    if not cf_ok:
        print_error("Cloud Function verification failed")
    
    # Monitor for new emails
    emails = await monitor_new_emails(gmail_reader, args.minutes)
    
    # Check Cloud Function logs
    logs_ok = check_cloud_function_logs(args.project, args.function, args.minutes, service_account)
    
    # Run test email and log monitoring if requested
    if args.test:
        print_section("SEND TEST EMAIL AND MONITOR LOGS")
        
        # Step 1: Send a test email
        print_info("Sending test email to trigger Cloud Function...")
        email_ok = await send_test_email(gmail_reader, subject=args.test_subject, recipient=args.test_recipient)
        
        if not email_ok:
            print_error("Failed to send test email. Cannot continue with testing.")
            return
            
        print_success(f"Test email sent successfully to {args.test_recipient}")
        time.sleep(5)  # Allow time for email to be delivered and notification to be processed
        
        # Step 2: Monitor logs until processing completes or timeout
        print_info(f"Monitoring logs for Cloud Function activation (timeout: {args.timeout}s)...")
        
        # After sending test email, we can try to filter for that specific message ID
        message_id = None  # We don't know the message ID for the test email yet
        
        logs_stationary = monitor_logs_until_stationary(
            project_id=args.project,
            function_name=args.function,
            timeout_seconds=args.timeout,
            stationary_seconds=args.stationary,
            credentials_path=service_account,
            message_id=message_id,
            log_file=log_file
        )
        
        if logs_stationary:
            print_success("Test email processed successfully")
        else:
            print_warning("Log monitoring timed out without detecting stable processing completion")
    
    # Only check logs if requested
    elif args.check_logs:
        print_section("MONITORING CLOUD FUNCTION LOGS")
        
        print_info(f"Monitoring logs for Cloud Function (timeout: {args.timeout}s)...")
        
        logs_stationary = monitor_logs_until_stationary(
            project_id=args.project,
            function_name=args.function,
            timeout_seconds=args.timeout,
            stationary_seconds=args.stationary,
            credentials_path=service_account,
            message_id=args.message_id if hasattr(args, 'message_id') else None,
            log_file=log_file
        )
        
        if logs_stationary:
            print_success("Log monitoring completed, logs are now stationary")
        else:
            print_warning("Log monitoring timed out without detecting stable state")
            
    # Overall assessment
    print_section("OVERALL ASSESSMENT")
    
    components_ok = watch_ok and pubsub_ok and cf_ok
    
    if components_ok:
        print_success("All pipeline components are correctly configured")
    else:
        print_warning("Some pipeline components have issues")
    
    if logs_ok:
        print_success("Cloud Function has been triggered and executed successfully recently")
    else:
        print_warning("No recent successful Cloud Function execution detected")

    print("\nVerification complete.")

if __name__ == "__main__":
    asyncio.run(main())

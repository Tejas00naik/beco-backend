#!/usr/bin/env python
"""
Script to check data in Firestore to verify production data
"""

import asyncio
import os
import sys
from pathlib import Path

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from src.repositories.firestore_dao import FirestoreDAO

# Load environment variables
load_dotenv()

async def check_firestore_data():
    """Check data in the production Firestore database"""
    
    # Initialize the DAO with no prefix for production collections
    project_id = os.environ.get("FIRESTORE_PROJECT_ID")
    dao = FirestoreDAO(project_id=project_id, collection_prefix="")
    
    # Safe getter function for document fields
    def safe_get(doc, field, default="N/A"):
        if doc is None:
            return default
        value = doc.get(field)
        if value is None:
            return default
        return value
    
    # Format string safely
    def safe_format_str(value, max_len=40):
        if value is None or not isinstance(value, str):
            return "N/A"
        if len(value) > max_len:
            return f"{value[:max_len]}..."
        return value
        
    # Function to print dict contents with indentation
    def print_dict(d, indent=0):
        if not isinstance(d, dict):
            print(" " * indent + str(d))
            return
            
        for key, value in d.items():
            if isinstance(value, dict):
                print(" " * indent + f"{key}:")
                print_dict(value, indent + 4)
            else:
                print(" " * indent + f"{key}: {value}")
    
    # Check RECENT email logs
    print("\n=== RECENT EMAIL LOGS ===")
    email_logs = await dao.query_documents("email_log", limit=10, order_by="created_timestamp", desc=True)
    print(f"Found {len(email_logs)} recent email logs")
    
    # Look for our test email with Zepto/Kiranakart subject
    print("\n=== ZEPTO TEST EMAIL LOGS ===")
    found_test_email = False
    for doc in email_logs:
        email_id = safe_get(doc, 'email_id')
        subject = safe_format_str(safe_get(doc, 'subject'))
        message_id = safe_get(doc, 'message_id')
        
        if 'ZEPTO' in subject.upper() or 'KIRANAKART' in subject.upper() or message_id == '19852e39d49c87e2':
            found_test_email = True
            print(f"\nFound Zepto test email:")
            print(f"Email ID: {email_id}")
            print(f"Subject: {subject}")
            print(f"Message ID: {message_id}")
            print(f"Status: {safe_get(doc, 'status')}")
            
            # Check related details
            legal_entity_uuid = safe_get(doc, 'legal_entity_uuid')
            group_uuid = safe_get(doc, 'group_uuid')
            print(f"Legal Entity UUID: {legal_entity_uuid}")
            print(f"Group UUID: {group_uuid}")
            
            # Check if using correct group UUID (not fallback)
            if group_uuid and group_uuid != "00000000-0000-0000-0000-000000000000":
                print("✓ SUCCESS: Using correct group UUID (not fallback)")
            elif group_uuid:
                print("❌ ERROR: Using fallback group UUID")
            
            # Show error if any
            error = safe_get(doc, 'error_message')
            if error and error != "N/A":
                print(f"Error: {error}")
    
    if not found_test_email:
        print("Could not find our recent Zepto test email in the logs")
        
    # Show all recent email logs
    print("\n=== ALL RECENT EMAIL LOGS ===")
    for doc in email_logs:
        email_id = safe_get(doc, 'email_id')
        subject = safe_format_str(safe_get(doc, 'subject'))
        status = safe_get(doc, 'status')
        print(f"Email ID: {email_id}, Subject: {subject}, Status: {status}")
    
    # Check RECENT payment advices
    print("\n=== RECENT PAYMENT ADVICES ===")
    # Sort by created_timestamp in descending order to get most recent ones
    payment_advices = await dao.query_documents("payment_advice", limit=10, order_by="created_timestamp", desc=True)
    print(f"Found {len(payment_advices)} recent payment advices")
    
    # First, look for any Zepto/Kiranakart entries
    print("\n=== ZEPTO/KIRANAKART PAYMENT ADVICES ===")
    found_zepto = False
    for doc in payment_advices:
        payer = safe_get(doc, 'payer_name', '').upper()
        if 'ZEPTO' in payer or 'KIRANAKART' in payer:
            found_zepto = True
            print(f"\nFound Zepto/Kiranakart payment advice:")
            print(f"PA ID: {safe_get(doc, 'payment_advice_id')}")
            print(f"Payer: {payer}")
            print(f"Amount: {safe_get(doc, 'total_amount')}")
            print(f"Created: {safe_get(doc, 'created_timestamp')}")
            print(f"Legal Entity UUID: {safe_get(doc, 'legal_entity_uuid')}")
            print(f"Group UUID: {safe_get(doc, 'group_uuid')}")
            print(f"Email ID: {safe_get(doc, 'email_id')}")
            
            # Check if the group UUID is the fallback (all zeros)
            group_uuid = safe_get(doc, 'group_uuid')
            if group_uuid and group_uuid != "00000000-0000-0000-0000-000000000000":
                print("✓ SUCCESS: Using correct group UUID (not fallback)")
            else:
                print("❌ ERROR: Using fallback group UUID")
    
    if not found_zepto:
        print("No Zepto/Kiranakart payment advices found in recent records")
    
    # Show all recent payment advices for reference
    print("\n=== ALL RECENT PAYMENT ADVICES ===")
    for doc in payment_advices:
        pa_id = safe_get(doc, 'payment_advice_id')
        payer = safe_get(doc, 'payer_name')
        amount = safe_get(doc, 'total_amount')
        legal_entity = safe_get(doc, 'legal_entity_uuid')
        group_uuid = safe_get(doc, 'group_uuid')
        print(f"PA ID: {pa_id}, Payer: {payer}, Amount: {amount}")
        print(f"  Legal Entity: {legal_entity}, Group: {group_uuid}\n")
    
    # Check invoices with uniqueness constraint
    print("\n=== INVOICES ===")
    invoices = await dao.query_documents("invoice", limit=5)
    print(f"Found {len(invoices)} invoices")
    for doc in invoices:
        inv_num = safe_get(doc, 'invoice_number')
        amount = safe_get(doc, 'amount')
        print(f"Invoice Number: {inv_num}, Amount: {amount}")
    
    # Check settlements
    print("\n=== SETTLEMENTS ===")
    settlements = await dao.query_documents("settlement", limit=5)
    print(f"Found {len(settlements)} settlements")
    for doc in settlements:
        settlement_id = safe_get(doc, 'settlement_id')
        amount = safe_get(doc, 'amount')
        sap_id = safe_get(doc, 'sap_transaction_id')
        print(f"Settlement ID: {settlement_id}, Amount: {amount}, SAP ID: {sap_id}")
    
    # Check batch runs
    print("\n=== BATCH RUNS ===")
    batch_runs = await dao.query_documents("batch_run", limit=5)
    print(f"Found {len(batch_runs)} batch runs")
    for doc in batch_runs:
        run_id = safe_get(doc, 'run_id')
        status = safe_get(doc, 'status')
        emails = safe_get(doc, 'emails_processed')
        errors = safe_get(doc, 'errors')
        print(f"Run ID: {run_id}, Status: {status}, Emails: {emails}, Errors: {errors}")

async def main():
    """Entry point for the script"""
    try:
        await check_firestore_data()
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

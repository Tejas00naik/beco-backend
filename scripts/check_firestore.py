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
from models.firestore_dao import FirestoreDAO

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
    
    # Check email logs
    print("\n=== EMAIL LOGS ===")
    email_logs = await dao.query_documents("email_log", limit=5)
    print(f"Found {len(email_logs)} email logs")
    for doc in email_logs:
        email_id = safe_get(doc, 'email_id')
        subject = safe_format_str(safe_get(doc, 'subject'))
        print(f"Email ID: {email_id}, Subject: {subject}")
    
    # Check payment advices
    print("\n=== PAYMENT ADVICES ===")
    payment_advices = await dao.query_documents("payment_advice", limit=5)
    print(f"Found {len(payment_advices)} payment advices")
    for doc in payment_advices:
        pa_id = safe_get(doc, 'payment_advice_id')
        payer = safe_get(doc, 'payer_name')
        amount = safe_get(doc, 'total_amount')
        print(f"PA ID: {pa_id}, Payer: {payer}, Amount: {amount}")
    
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

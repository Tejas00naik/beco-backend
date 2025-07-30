#!/usr/bin/env python
import os
import sys
import json
import requests
import pandas as pd
from google.cloud import firestore
from datetime import datetime

# Initialize Firestore client
os.environ["FIRESTORE_PROJECT_ID"] = "vaulted-channel-462118-a5"
os.environ["FIRESTORE_DATABASE_ID"] = "beco-payment-advice-dev"

project_id = os.environ.get("FIRESTORE_PROJECT_ID")
database_id = os.environ.get("FIRESTORE_DATABASE_ID")

db = firestore.Client(project=project_id, database=database_id)

def get_latest_payment_advice():
    """Get the most recent payment advice with a SAP export URL."""
    advices = db.collection("payment_advice").order_by("updated_at", direction=firestore.Query.DESCENDING).limit(1).get()
    
    for advice in advices:
        advice_data = advice.to_dict()
        if "sap_export_url" in advice_data:
            return advice_data
    
    return None

def download_sap_export(url, output_path):
    """Download the SAP export file from the given URL."""
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        return True
    return False

def analyze_excel_file(file_path):
    """Analyze the Excel file to check for BP codes."""
    df = pd.read_excel(file_path)
    print("\n--- SAP Export Analysis ---")
    print(f"Total rows: {len(df)}")
    
    # Check if BP Code column exists
    bp_code_column = None
    for col in df.columns:
        if "BP" in col and "Code" in col:
            bp_code_column = col
            break
    
    if bp_code_column:
        print(f"BP Code column found: {bp_code_column}")
        bp_codes = df[bp_code_column].tolist()
        null_count = sum(1 for code in bp_codes if pd.isna(code))
        print(f"BP Codes: {bp_codes}")
        print(f"BP Codes null count: {null_count} out of {len(bp_codes)}")
        print(f"BP Codes present: {len(bp_codes) - null_count} out of {len(bp_codes)}")
    else:
        print("No BP Code column found in the Excel file.")
    
    # Print the first few rows
    print("\n--- First 5 rows of the SAP Export ---")
    print(df.head())

def main():
    print("Fetching the latest payment advice with SAP export URL...")
    advice = get_latest_payment_advice()
    
    if not advice:
        print("No payment advice with SAP export URL found.")
        return
    
    print(f"Found payment advice: {advice['payment_advice_uuid']}")
    print(f"Payment advice number: {advice['payment_advice_number']}")
    print(f"Legal entity UUID: {advice['legal_entity_uuid']}")
    
    if "sap_export_url" not in advice:
        print("No SAP export URL found in the payment advice.")
        return
    
    # Create a timestamp for the downloaded file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"sap_export_{timestamp}.xlsx"
    
    print(f"Downloading SAP export from URL...")
    if download_sap_export(advice["sap_export_url"], output_file):
        print(f"Downloaded SAP export to {output_file}")
        analyze_excel_file(output_file)
    else:
        print("Failed to download SAP export.")

if __name__ == "__main__":
    main()

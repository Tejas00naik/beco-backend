"""Test script for BatchWorkerV2 to process latest email.

This script processes just the latest email in the inbox to verify:
1. Correct legal entity detection
2. Correct group UUID identification
3. Proper use of factory pattern for Zepto LLM processing
"""

import os
import sys
import logging
import asyncio
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
from os.path import abspath, dirname

# Add project root to Python path
project_root = dirname(dirname(abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import BatchWorkerV2
from src.batch_worker.batch_worker_v2 import BatchWorkerV2

# Import configuration
from src.config import (
    TARGET_MAILBOX_ID,
    DEFAULT_GMAIL_CREDENTIALS_PATH
)


async def main():
    """Test BatchWorkerV2 with latest email."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Test BatchWorkerV2 with latest email")
    parser.add_argument(
        "--use-assistants-api", 
        action="store_true",
        help="Use OpenAI Assistants API for PDF processing instead of standard extraction"
    )
    args = parser.parse_args()
    
    # Set environment variable based on argument
    if args.use_assistants_api:
        logger.info("Using OpenAI Assistants API for PDF processing")
        os.environ["USE_ASSISTANTS_API"] = "True"
    else:
        logger.info("Using standard text extraction for PDF processing")
        os.environ["USE_ASSISTANTS_API"] = "False"
    
    logger.info("Starting BatchWorkerV2 test for latest email")
    
    # Create output directory for analysis files if it doesn't exist
    output_dir = os.path.join(project_root, "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate timestamp for unique output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Initialize BatchWorkerV2 for testing
    worker_v2 = BatchWorkerV2(
        is_test=True,  # Use test mode with dev_ collection prefix
        mailbox_id=TARGET_MAILBOX_ID,
        run_mode="incremental",
        use_gmail=True,  # Use Gmail to get real emails
        gmail_credentials_path=DEFAULT_GMAIL_CREDENTIALS_PATH
    )
    
    # Process just the most recent email
    success = await worker_v2.process_last_email()
    
    # Access the processed data to print and save both L2 and OP tables
    if hasattr(worker_v2, 'last_processed_output') and worker_v2.last_processed_output:
        llm_output = worker_v2.last_processed_output
        
        # Save full LLM output to file for analysis
        llm_output_file = os.path.join(output_dir, f"llm_output_{timestamp}.json")
        with open(llm_output_file, 'w') as f:
            json.dump(llm_output, f, indent=2, default=str)
        print(f"\nSaved full LLM output to: {llm_output_file}")
        
        # Save PDF text content if available
        if hasattr(worker_v2, 'last_pdf_text') and worker_v2.last_pdf_text:
            pdf_text_file = os.path.join(output_dir, f"pdf_text_{timestamp}.txt")
            with open(pdf_text_file, 'w') as f:
                f.write(worker_v2.last_pdf_text)
            print(f"Saved PDF text to: {pdf_text_file}")
        
        # Print the original L2 table (Body Table)
        print("\n\n===== L2 TABLE (LLM ORIGINAL OUTPUT) =====")
        if 'Body Table' in llm_output:
            body_table = llm_output['Body Table']
            for idx, row in enumerate(body_table):
                print(f"Row {idx+1}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")
                print()
                
            # Save L2 table to separate file
            l2_table_file = os.path.join(output_dir, f"l2_table_{timestamp}.json")
            with open(l2_table_file, 'w') as f:
                json.dump(body_table, f, indent=2, default=str)
            print(f"Saved L2 table to: {l2_table_file}")
        else:
            print("No Body Table found in LLM output")
        
        # Print the Meta Table
        print("\n===== META TABLE =====")
        if 'Meta Table' in llm_output:
            meta_table = llm_output['Meta Table']
            for key, value in meta_table.items():
                print(f"{key}: {value}")
                
            # Save Meta table to separate file
            meta_table_file = os.path.join(output_dir, f"meta_table_{timestamp}.json")
            with open(meta_table_file, 'w') as f:
                json.dump(meta_table, f, indent=2, default=str)
            print(f"Saved Meta table to: {meta_table_file}")
        else:
            print("No Meta Table found in LLM output")
        
        # Print the transformed OP table (paymentadvice_lines)
        print("\n\n===== OP TABLE (TRANSFORMED PAYMENTADVICE_LINES) =====")
        if 'paymentadvice_lines' in llm_output:
            payment_lines = llm_output['paymentadvice_lines']
            for idx, line in enumerate(payment_lines):
                print(f"Line {idx+1}:")
                for key, value in line.items():
                    # Highlight reference invoice numbers for easier comparison
                    if key == 'ref_invoice_no':
                        print(f"  {key}: >>>>{value}<<<<")
                    else:
                        print(f"  {key}: {value}")
                print()
            print(f"Total OP table entries: {len(payment_lines)}")
            
            # Special analysis for reference invoice numbers
            print("\n===== REFERENCE INVOICE NUMBER ANALYSIS =====")
            ref_invoice_nos = [line.get('ref_invoice_no', '') for line in payment_lines if line.get('ref_invoice_no')]
            if ref_invoice_nos:
                print(f"Found {len(ref_invoice_nos)} reference invoice numbers:")
                for i, ref in enumerate(ref_invoice_nos):
                    print(f"  {i+1}. {ref}")
                    
                # Check if any reference numbers contain comma (potentially indicating amount appended)
                comma_refs = [ref for ref in ref_invoice_nos if ',' in ref]
                if comma_refs:
                    print(f"\nWARNING: {len(comma_refs)} reference numbers contain commas (possible amount appended):")
                    for ref in comma_refs:
                        print(f"  - {ref}")
                else:
                    print("\nSUCCESS: No reference numbers contain commas - likely no amounts appended")
            else:
                print("No reference invoice numbers found in output")
            
            # Save OP table to separate file
            op_table_file = os.path.join(output_dir, f"op_table_{timestamp}.json")
            with open(op_table_file, 'w') as f:
                json.dump(payment_lines, f, indent=2, default=str)
            print(f"Saved OP table to: {op_table_file}")
        else:
            print("No paymentadvice_lines found in processed output")
    
    if success:
        logger.info("Successfully processed latest email with BatchWorkerV2")
    else:
        logger.error("Failed to process latest email with BatchWorkerV2")


if __name__ == "__main__":
    asyncio.run(main())

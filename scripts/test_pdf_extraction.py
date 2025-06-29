#!/usr/bin/env python3

"""
Script to test PDF text extraction and LLM processing with both email and attachment content.
"""

import os
import sys
import json
import argparse
import asyncio
import logging

# Add parent directory to path to import from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.llm_integration.extractor import LLMExtractor
from models.firestore_dao import FirestoreDAO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file using PyPDF2."""
    try:
        import PyPDF2
    except ImportError:
        logger.warning("PyPDF2 not installed. Installing now...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2"])
        import PyPDF2
    
    # Extract text from PDF
    logger.info(f"Extracting text from PDF: {pdf_path}")
    pdf_text = ""
    with open(pdf_path, "rb") as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            pdf_text += page.extract_text() + "\n\n"
    
    logger.info(f"Extracted {len(pdf_text)} characters from PDF")
    return pdf_text


async def test_extraction_with_both_inputs(pdf_path, email_path=None, output_path=None):
    """Test extraction with both PDF and email content."""
    # Check if OpenAI API key is set
    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY environment variable is not set!")
        print("Please set your OpenAI API key before running this script.")
        return
        
    # Extract text from PDF
    pdf_text = extract_text_from_pdf(pdf_path)
    print(f"PDF Text Content ({len(pdf_text)} chars): {pdf_text[:100]}...")
    
    # Read email content if provided
    email_text = ""
    if email_path:
        try:
            with open(email_path, "r", encoding="utf-8") as f:
                email_text = f.read()
            print(f"Email Text Content ({len(email_text)} chars): {email_text[:100]}...")
        except Exception as e:
            print(f"Error reading email file: {str(e)}")
    
    # Save the extracted text to files for inspection
    pdf_text_path = pdf_path + ".txt"
    with open(pdf_text_path, "w", encoding="utf-8") as f:
        f.write(pdf_text)
    print(f"PDF text content saved to {pdf_text_path}")
    
    # Initialize the LLM extractor
    dao = FirestoreDAO(collection_prefix="")
    extractor = LLMExtractor(dao=dao)
    
    # Construct attachment_data dict similar to how EmailProcessor would do it
    attachment_data = {
        "filename": os.path.basename(pdf_path),
        "content_type": "application/pdf",
        "size": os.path.getsize(pdf_path),
        "text_content": pdf_text
    }
    
    # Process with LLM
    print("\n1. Testing LLM extraction with attachment_data.text_content:")
    try:
        # 1. Test with the process_attachment_for_payment_advice method
        processed_output = await extractor.process_attachment_for_payment_advice(
            email_text_content=email_text,
            attachment_data=attachment_data
        )
        print("\nLLM Extraction Results (process_attachment_for_payment_advice):")
        if "paymentAdviceDate" in processed_output:
            print(f"  Payment Advice Date: {processed_output.get('paymentAdviceDate')}")
            print(f"  Payment Advice Number: {processed_output.get('paymentAdviceNumber')}")
            print(f"  Payer Legal Name: {processed_output.get('payersLegalName')}")
            print(f"  Payee Legal Name: {processed_output.get('payeesLegalName')}")
            print(f"  Invoice Table: {len(processed_output.get('invoiceTable', []))} entries")
            print(f"  Other Doc Table: {len(processed_output.get('otherDocTable', []))} entries")
            print(f"  Settlement Table: {len(processed_output.get('settlementTable', []))} entries")
        else:
            print("  No payment advice data returned")
        
        # 2. Test with the process_document method directly
        print("\n2. Testing with process_document method:")
        output = await extractor.process_document(
            document_text=pdf_text,
            email_body=email_text,
            group_uuid="group-amazon-12345"  # Explicitly set group UUID
        )
        print("\nLLM Extraction Results (process_document):")
        print(f"  Meta Table: {json.dumps(output.get('meta_table', {}), indent=2)}")
        print(f"  Invoice Table: {len(output.get('invoice_table', []))} entries")
        print(f"  Settlement Table: {len(output.get('settlement_table', []))} entries")
        print(f"  Reconciliation Statement: {len(output.get('reconciliation_statement', []))} entries")
        
        # Save the output if requested
        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2)
            print(f"\nFull output saved to {output_path}")
        
        return output
        
    except Exception as e:
        print(f"Error processing with LLM: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test PDF text extraction and LLM processing with email and attachment"
    )
    parser.add_argument("pdf_file", help="Path to the PDF file to process")
    parser.add_argument(
        "-e", "--email", 
        help="Path to a file containing email body text (optional)"
    )
    parser.add_argument(
        "-o", "--output", 
        help="Path to save the output JSON (optional)"
    )
    
    args = parser.parse_args()
    asyncio.run(test_extraction_with_both_inputs(args.pdf_file, args.email, args.output))


if __name__ == "__main__":
    main()

"""Test script for LLM extraction."""

import asyncio
import os
import sys
import argparse
import json
from typing import Dict, Any, Optional, Union
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
print(f"OpenAI API Key loaded: {'Yes' if os.environ.get('OPENAI_API_KEY') else 'No'}")
print(f"API Key prefix: {os.environ.get('OPENAI_API_KEY')[:4]}..." if os.environ.get('OPENAI_API_KEY') else "No API key found")

from models.firestore_dao import FirestoreDAO
from src.llm_integration import LLMExtractor

async def test_llm_extraction(input_file_path: str, output_file_path: str = None, email_body_path: str = None):
    """Test LLM extraction with a text file and output the results."""
    # Check if OpenAI API key is set
    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY environment variable is not set!")
        print("Please set your OpenAI API key before running this script.")
        return
        
    print(f"Testing LLM extraction with file: {input_file_path}")
    
    # Determine if we're dealing with a PDF or text file
    is_pdf = input_file_path.lower().endswith(".pdf")
    
    document_text = None
    if not is_pdf:
        # Read document text for non-PDF files
        try:
            with open(input_file_path, "r", encoding="utf-8") as file:
                document_text = file.read()
            print(f"Loaded text document with {len(document_text)} characters")
        except Exception as e:
            print(f"Error reading text file: {str(e)}")
            return
    else:
        print(f"Detected PDF file: {input_file_path}")
        
    # Initialize DAO and LLM extractor
    dao = FirestoreDAO(collection_prefix="")
    extractor = LLMExtractor(dao=dao)
    print("Using real LLMExtractor with OpenAI API (GPT-4-turbo)")
    
    # Read email body if provided
    email_body = None
    if email_body_path:
        try:
            with open(email_body_path, "r", encoding="utf-8") as file:
                email_body = file.read()
            print(f"Loaded email body with {len(email_body)} characters")
        except Exception as e:
            print(f"Warning: Could not load email body from {email_body_path}: {str(e)}")
    
    # Process the document
    print("Processing document with LLM extractor...")
    if is_pdf:
        result = await extractor.process_document(pdf_path=input_file_path, email_body=email_body)
    else:
        result = await extractor.process_document(document_text=document_text, email_body=email_body)
    
    # Get legal entity and group information
    legal_entity_uuid = await extractor.detect_legal_entity_from_output(result)
    group_uuid = await extractor.detect_group_from_output(result)
    
    print(f"\nLLM Extraction Results:")
    print(f"  Legal Entity UUID: {legal_entity_uuid}")
    print(f"  Group UUID: {group_uuid}")
    print(f"  Meta Table: {json.dumps(result.get('meta_table', {}), indent=2)}")
    print(f"  Invoice Table: {len(result.get('invoice_table', []))} entries")
    print(f"  Settlement Table: {len(result.get('settlement_table', []))} entries")
    print(f"  Reconciliation Statement: {len(result.get('reconciliation_statement', []))} entries")
    
    # Save output to file if specified
    if output_file_path:
        with open(output_file_path, "w", encoding="utf-8") as output_file:
            json.dump(result, output_file, indent=2)
        print(f"\nFull output saved to {output_file_path}")
        
    return result

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Test LLM extraction with a file (text or PDF)")
    parser.add_argument("input_file", help="Path to the file to process (text or PDF)")
    parser.add_argument(
        "-o", "--output", 
        help="Path to save the output JSON (optional)"
    )
    parser.add_argument(
        "-e", "--email",
        help="Path to a file containing email body text (optional)"
    )
    
    args = parser.parse_args()
    asyncio.run(test_llm_extraction(args.input_file, args.output, args.email))

if __name__ == "__main__":
    main()

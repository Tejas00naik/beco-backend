#!/usr/bin/env python3
"""
Utility script to extract text from PDF files for LLM processing.
"""

import argparse
import os
import PyPDF2

def extract_text_from_pdf(pdf_path):
    """Extract text content from a PDF file."""
    print(f"Extracting text from PDF: {pdf_path}")
    
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return None
        
    text = ""
    try:
        with open(pdf_path, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)
            num_pages = len(pdf_reader.pages)
            
            print(f"PDF has {num_pages} pages.")
            
            # Extract text from each page
            for i in range(num_pages):
                page = pdf_reader.pages[i]
                text += page.extract_text() + "\n\n"
                
            print(f"Extracted {len(text)} characters of text")
            return text
    except Exception as e:
        print(f"Error extracting text from PDF: {str(e)}")
        return None

def save_text_to_file(text, output_path):
    """Save extracted text to a file."""
    try:
        with open(output_path, "w", encoding="utf-8") as file:
            file.write(text)
        print(f"Text saved to: {output_path}")
        return True
    except Exception as e:
        print(f"Error saving text to file: {str(e)}")
        return False

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Extract text from PDF files")
    parser.add_argument("pdf_file", help="Path to the PDF file to extract text from")
    parser.add_argument(
        "-o", "--output", 
        help="Path to save the extracted text (default is same filename with .txt extension)"
    )
    
    args = parser.parse_args()
    
    # Set default output path if not provided
    output_path = args.output
    if not output_path:
        output_path = os.path.splitext(args.pdf_file)[0] + ".txt"
    
    # Extract and save text
    text = extract_text_from_pdf(args.pdf_file)
    if text:
        save_text_to_file(text, output_path)
        print(f"\nNext steps: Run the LLM extraction on the text file:")
        print(f"python scripts/test_llm_extraction.py {output_path} -e data/test_email_body.txt -o data/llm_output.json")

if __name__ == "__main__":
    main()

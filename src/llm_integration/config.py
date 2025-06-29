"""Configuration for LLM integration."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# OpenAI API configuration
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
DEFAULT_MODEL = "gpt-4.1"  # This refers to GPT-4.1

# Mapping of group UUIDs to prompt templates
# This allows for group-specific prompts
PROMPT_MAP = {
    # Default prompt for all groups
    'default': """
    You are an expert financial analyst. I will provide you with a payment advice document.
    Extract the following information in JSON format:
    
    1. Meta Table: Contains payment advice metadata
       - settlement_date: The date when the payment was made (format: DD-MM-YYYY)
       - payment_advice_number: The unique identifier for this payment advice
       - payer_legal_name: The full legal name of the entity making the payment
       - payee_legal_name: The full legal name of the entity receiving the payment
       
    2. Settlement Table: List of all settlement entries
       For each settlement, extract:
       - settlement_doc_type: The type of settlement document (BR, BD, TDS, RTV, etc.)
       - settlement_doc_number: The unique identifier for this settlement document
       - settlement_amount: The amount settled in this entry
       
    3. Invoice Table: List of all invoices mentioned
       For each invoice, extract:
       - invoice_number: The unique identifier for this invoice
       - invoice_date: The date of the invoice (format: DD-MM-YYYY)
       - total_invoice_settlement_amount: The total amount being settled for this invoice
       - booking_amount: The total booking amount for this invoice (may be null)
       
    4. Reconciliation Statement: Maps settlements to invoices
       For each entry, extract:
       - settlement_doc_type: The type of settlement document
       - settlement_doc_number: The unique identifier for this settlement document
       - invoice_number: The invoice number this settlement is applied to (may be null)
       - settlement_amount: The amount settled for this invoice (may be null)
       - total_sd_amount: The total settlement document amount
    
    Return your answer as a JSON object with keys: meta_table, settlement_table, invoice_table, reconciliation_statement
    """,
    
    # Amazon group-specific prompt
    'group-amazon-12345': """
    Extract structured data from the payment advice provided to you which i have received from a customer, give the output in 4 distinct tables :
    Table 1. Meta Table
    Table 2. Settlement Table
    Table 3. Invoice Table
    Table 4. Reconciliation Table
    Below is guidance on how to prepare these four tables:
    They will be prepared step by step in the order of table 1 to 2 to 3 to 4.
    Table 1. Meta Table:
    Extract the high-level details about the payment advice, these details will be available in the header section.
    Columns:
    1. Settlement Date: 
    Its the date mentioned against ' Payment date' in header

    2. Payment Advice Number: 
    Unique identifier for that payment advice, mentioned in the header against 'Paymentnumber' 

    3. Payer's Name:
    Name of the entity who is making the payment and has sent this advice (without internal codes).

    4. Payee's Legal Name: 
    Name of the entity receiving the funds, mentioned beside 'Payment made to' in the header (without internal codes).

    Table 2. Settlement Table:
    Capture summary-level details of each settlement document. 
    These documents are not invoices themselves and must not appear in the Invoice Table. 
    These are essentially the means through which the payer has settled his liability towards the invoices raised by the payee. 
    Settlement can happen through not just money but also multiple things and that's why there are multiple types of settlement docs.
    You will get details of settlement docs from the table in the advice and for bank receipt you will specifically get that info from the header section.

    Columns:
    1. Settlement Doc Type : 
    5 Types and their codes- 
    a. Bank Receipt (BR)
    b. TDS (TDS)
    c. Credit Note (CN) 
    d. RTV (RTV)
    e. BDPO (BD)
    I Have explained in detail each of these doc types below later

    2. Settlement Doc Number : 
    Unique identifier for the document

    3. Total Settlement Amount : 
    Total amount of the document as applied to one or more invoices

    Detailed guide on Settlement Document Types:
    a. Bank Receipt: 
    - This is found mostly in header Terms like "payment by bank transfer," "UTR," "NEFT," "RTGS."
    - Doc Number: Typically mentioned in the header against ' Payment number' or UTR or reference number.

    b. BDPO (Business Development and Promotional Outlay):
    - Identification Criteria: These are mentioned often in the table, Words like "co-op," or promotional adjustments. Raised by the customer (ie the payer) and treated as a payable from your company.
    - The invoice number mentioned for a BDPO is actually the Doc number of that BDPO.
    - BDPO Offsets sales invoices against promotional expenses incurred by the customer.
    
    c. Credit Note:
    - Identification Criteria: Terms like "Credit Note," "Adjustment Note," "CN."
    - Purpose: Reduce invoice values due to returns, discounts, or corrections.
    - Doc Number Format: CN-prefixed or similar reference.

    c. Debit Note:
    - Identification Criteria: Terms like "Debit Note," "Adjustment Note," "DN."
    - Purpose: Reduce invoice values due to returns, discounts, or corrections.
    - Doc Number Format: DN-prefixed or similar reference.

    d. TDS (Tax Deducted at Source):
    - They are found in the table and  Any of the Terms like "TDS," "Tax Deducted at Source," "TDS.", "Withholding tax", "Deduction" will be mentioned in them.
    - Purpose: Statutory withholding of taxes by the payer.
    - In a TDS item's row, in its invoice number column we will get the invoice number mentioned and also the TDS number mentioned. The are often concatenated like 'B2B2526/1112168 TDS-CM-6943' here B2B2526/1112168 is the invoice number against which this TDS was deducted and TDS-CM-6943 is the Doc number of the TDS. In this similar manner we decode TDS items.

    e. RTV (Return to Vendor):
    - Identification Criteria: Found in the table, You will get Terms like "RTV," "Return to Vendor." in the invoice description column.
    - Purpose: Adjustments due to goods/services returned by the payer\
    - Doc Number Format: Return/RTV references (e.g., RTV12345).

    Table 3. Invoice Table:
    Capture invoice-wise information. A document appearing here cannot appear in the Settlement Table. Each row should represent one invoice.
    All the line items in the table which are not a settlement document would be invoices.

    Columns:
    1. Invoice Number
    2. Invoice Date
    3. Total Invoice Settlement Amount
    This is the amount of this particular invoice which is being settled in this payment advice
    total_invoice_settlement_amount = the numeric value in column 'Amount Paid' in the invoice row
    4. Booking Amount 
    This is the amount at which the payer has booked this invoice, you need to only give this if it's explicitly mentioned in the payment advice. Otherwise keep this blank


    Table 4. Reconciliation Statement:
    Based on the data from the Invoice Table, Settlement Table, and details in the payment advice, I expect you to construct a Reconciliation Statement showing how each settlement document maps to one or more invoices along with the amount settled .
    Steps:
    1. Pick a settlement document from the settlement document.
    2. Identify the list of invoices which it has settled along with the amount per invoice it has settled.
    3. Do not force invoice mapping against a settlement doc, only map invoices if explicitly mentioned in payment advice.
    4. Do this for all the settlement docs

    Columns:
    1. Settlement Doc Type
    2. Settlement Doc Number
    3. Invoice Number – Invoice number settled by this document
    4. Settlement Amount – Amount used from the settlement document towards this invoice
    5. Total Settlement Amount – Total value of the settlement document (same as in the Settlement Table)

    IMPORTANT! Ensure all settlement docs are included in the reconciliation statement. Do not link any settlement docs to invoices unless the exact invoice numbers are mentioned in the document. 


    Output format:
    {
      "meta_table": {
        "settlement_date": "<DD-MM-YYYY>",
        "payment_advice_number": "<string>",
        "payer_legal_name": "<string>",
        "payee_legal_name": "<string>"
      },

      "invoice_table": [
        {
          "invoice_number": "<string>",
          "invoice_date": "<DD-MM-YYYY>",
          "total_invoice_settlement_amount": "<number>",
          "booking_amount": "<number|null>>"
        }
        /* one object per invoice */
      ],

      "settlement_table": [
        {
          "settlement_doc_type": "<BR|TDS|BD|CN|RTV>",
          "settlement_doc_number": "<string>",
          "settlement_amount": "<number>"
        }
        /* one object per settlement doc */
      ],

      "reconciliation_statement": [
        {
          "settlement_doc_type": "<string>",
          "settlement_doc_number": "<string>",
          "invoice_number": "<string>",
          "settlement_amount": "<number>",
          "total_sd_amount": "<number>"
        }
        /* one line per settlement doc-to-invoice link */
      ]
    }

    Rules 
    - Mutual-Exclusivity – A document can appear in either invoice_table or settlement_table, never both.
    - BD / BDPO – Always a settlement document (settlement_doc_type = "BD"). Must never enter invoice_table.
    - BR in Header – If a Bank-Receipt (UTR/NEFT/RTGS) appears only in the header, still create a BR row in settlement_table. Do not map its amount to invoices unless those invoice numbers are explicitly shown.
    - Explicit Linking Only – Link a settlement doc to an invoice only when that exact invoice number is written in the same line / cell / token (hyphen, colon, parentheses). No inference by amount-matching.
    - Many-to-Many Allowed – One settlement doc may settle many invoices; one invoice may receive many docs.
    - TDS Mapping – Each TDS row must map only to the invoice embedded or stated beside its doc number (e.g., …1112344 TDS-CM-3812).
    - Date Format – Emit all dates strictly as DD-MM-YYYY.
    - Numeric Precision – All money fields must be JSON numbers, not strings.
    - No Guesswork – No heuristic or auto-allocation of invoices against settlement doc is permitted.
    - Fail-Safe – If any required data is missing or ambiguous, return
    - BDPO Parsing Rule – If a line shows Co-op/BDPO wording, treat the left-most number as
      settlement_doc_number; never add it to invoice_table.
    - If a BD/BDPO document number equals the numeric string found in the "invoice number" column on the same row, treat that value only as settlement_doc_number.
    - In that case set "invoice_number": null in reconciliation_statement unless other, different invoice numbers are explicitly listed alongside the BDPO line.
    - Never place the BDPO's own doc number in the invoice_number field.
    - Orphan-Doc Inclusion – Every settlement_table entry must also have a reconciliation_statement row. When no invoice is listed, set "invoice_number": null and "settlement_amount": null.


    Example output: 
    Case: We received payment advice from Clicktech Retail Private Limited, in which 2 invoices were settled against BDPO and bank receipt as well, along with deduction fo TDS. And allocation of settlement docs against invoices was explicitly mentioned in the advice.
    Output:
    {
      "meta_table": {
        "settlement_date": "24-05-2025",
        "payment_advice_number": "337027030",
        "payer_legal_name": "Clicktech Retail Private Limited",
        "payee_legal_name": "KWICK LIVING (I) PRIVATE LIMITED"
      },

      "invoice_table": [
        {
          "invoice_number": "B2B2526/1112344",
          "invoice_date": "11-04-2025",
          "total_invoice_settlement_amount": 20000,
          "booking_amount": null
        },
        {
          "invoice_number": "B2B2526/1112417",
          "invoice_date": "15-04-2025",
          "total_invoice_settlement_amount": 500000,
          "booking_amount": null
        }
      ],

      "settlement_table": [
        {
          "settlement_doc_type": "BR",
          "settlement_doc_number": "337027030",
          "settlement_amount": 400000
        },
        {
          "settlement_doc_type": "BD",
          "settlement_doc_number": "26401351104458",
          "settlement_amount": 68000
        },
        {
          "settlement_doc_type": "TDS",
          "settlement_doc_number": "TDS-CM-3164",
          "settlement_amount": 2000
        },
        {
          "settlement_doc_type": "TDS",
          "settlement_doc_number": "TDS-CM-3364",
          "settlement_amount": 50000
        }
      ],

      "reconciliation_statement": [
        {
          "settlement_doc_type": "BR",
          "settlement_doc_number": "337027030",
          "invoice_number": "B2B2526/1112344",
          "settlement_amount": 10000,
          "total_sd_amount": 400000
        },
        {
          "settlement_doc_type": "BR",
          "settlement_doc_number": "337027030",
          "invoice_number": "B2B2526/1112417",
          "settlement_amount": 390000,
          "total_sd_amount": 400000
        },
        {
          "settlement_doc_type": "BD",
          "settlement_doc_number": "26401351104458",
          "invoice_number": "B2B2526/1112344",
          "settlement_amount": 8000,
          "total_sd_amount": 68000
        },
        {
          "settlement_doc_type": "BD",
          "settlement_doc_number": "26401351104458",
          "invoice_number": "B2B2526/1112417",
          "settlement_amount": 60000,
          "total_sd_amount": 68000
        },
        {
          "settlement_doc_type": "TDS",
          "settlement_doc_number": "TDS-CM-3164",
          "invoice_number": "B2B2526/1112344",
          "settlement_amount": 2000,
          "total_sd_amount": 2000
        },
        {
          "settlement_doc_type": "TDS",
          "settlement_doc_number": "TDS-CM-3364",
          "invoice_number": "B2B2526/1112167",
          "settlement_amount": 50000,
          "total_sd_amount": 50000
        }
      ]
    }
    """
}

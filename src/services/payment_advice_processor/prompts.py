from src.services.payment_advice_processor.constants import (
    META_PAYMENT_ADVICE_DATE,
    META_PAYMENT_ADVICE_NUMBER,
    META_PAYER_LEGAL_NAME,
    META_PAYEE_LEGAL_NAME,
    META_PAYMENT_ADVICE_AMOUNT,
    LLM_META_TABLE_KEY,
    LLM_BODY_TABLE_KEY
)

AMAZON_PROMPT = f"""System: You are an AI assistant that extracts and structures data from Amazon payment advice PDFs.

User: I need to extract key information from an Amazon payment advice PDF in a structured format.

Please extract the following information into a well-structured format:
 - Return one JSON object containing meta_table and l2_table, nothing else.

# META_TABLE FIELDS
 - {META_PAYMENT_ADVICE_DATE} – value after "Payment date:" converted to DD-MM-YYYY
 - {META_PAYMENT_ADVICE_NUMBER} – value after "Payment number:"
 - {META_PAYER_LEGAL_NAME} – entity name that appears before the word "issued" in the first sentence
 - {META_PAYEE_LEGAL_NAME} – value after "Payment made to:" with any codes put at the end of the name should be removed
 - {META_PAYMENT_ADVICE_AMOUNT} – value after "Payment amount:" converted to bare JSON number

# L2_TABLE RULES
 - Wrap-join: if Invoice Number is split by newline or space, join with no separator (e.g. 2640135110↵4458 → 26401351104458).
 - invoice_date: convert MMM format to DD-MM-YYYY; if blank, use null.
 - Ignore leading asterisks in numeric fields.
 - Parentheses indicate negative numbers.
 - Strip commas from numbers and output as bare JSON numbers.
 - Preserve Invoice description text as its is always.
- Discount taken column is often just blanks, do not put values from Amount paid column in it

## SYNTHETIC BANK-RECEIPT ROW (append last)
 - invoice_number = null
 - invoice_date = settlement_date
 - invoice_description = Bank Receipt
 - discount_taken = null
 - amount_paid = negative of header "Payment amount:"
 - amount_remaining = null

# EXAMPLE INPUT TEXT FORMAT
Here's an example of what the payment advice document looks like:

Payment made to:       KWICK LIVING (I) PRIVATE LIMITED(1MC3G)
Our Supplier No:       98499554
Supplier site name:    KWIGLCRPL
Payment number:        340290516
Payment date:          11-JUL-2025
Payment currency:      INR
Payment amount:        719,489.19

Invoice Number:    Invoice Date:    Invoice description             Discount Taken    Amount Paid    Amount Remaining
MH25/252601063    05-JUN-2025     8OXO1R7L/ISK3/##NOT_AVAILABLE                     158,104.90     0.00
MH25/252601063-   05-JUN-2025     India TDS Invoice for AP-194Q                     (133.99)       0.00
TDS-CM-0997
MH25/252601289    11-JUN-2025     2XFB1ZSB/ISK3/##NOT_AVAILABLE                    562,835.46     0.00
MH25/252601289-   11-JUN-2025     India TDS Invoice for AP-194Q                     (476.98)       0.00
TDS-CM-8360
KWIGM-           07-JUL-2025      RTV FCN-KWIGM-30292516672552-                    (65.18)        0.00
30292516672552-                    AMD2-L-1405
AMD2-L-1405
KWIGM-           07-JUL-2025      RTV FCN-KWIGM-30296863171552-                    (775.02)       0.00
30296863171552-                    PAX1-L-189
PAX1-L-189


## Another input example
Payment Made To:
Our Supplier Number:
Supplier Site Name:
Payment Ref. Number:
KWICK LIVING (I) PRIVATE
LIMITED (BECO)
10218
KWIFTCRTPL
45732
Payment Date: May 20, 2025
Payment Currency: INR
Payment Amount: 412,667.52
Invoice Number Invoice Date Invoice Description Amount Paid Discount Taken Amount
Remaining
B2BOS24/22374SCRSCRSCR Aug 10, 2024 30001109196411 23,187.00 0.00 0.00
B2BOS24/22546SCRSCR Aug 20, 2024 30001119082896 9,027.00 0.00 0.00
B2BOS24/23341SCRSCR Sep 21, 2024 30001113141964 4,552.34 0.00 0.00
B2BOS24/23380SCRSCR Sep 23, 2024 77,095.52 0.00 0.00
B2BOS24/24256SCR Oct 26, 2024 30001111821172 285,142.38 0.00 0.00
B2BOS24/24256SCRSC Oct 26, 2024 30001111821172 -155,193.60 0.00 0.00
B2BOS24/24256SCRSCR Oct 26, 2024 30001111821172 155,193.60 0.00 0.00
B2BOS24/24258SCRSCR Oct 27, 2024 30001111777592 4,130.76 0.00 0.00
B2BOS24/24261SCRSCR Oct 27, 2024 30001114737067 9,532.52 0.00 0.00
Total 412,667.52 0.00 0.00


# EXAMPLE OUTPUT FORMAT
Here's the exact format your JSON output should follow:

{{
	"meta_table": {{
		"{META_PAYMENT_ADVICE_DATE}": "11-07-2025",
		"{META_PAYMENT_ADVICE_NUMBER}": "340290516",
		"{META_PAYER_LEGAL_NAME}": "Clicktech Retail Private Limited",
		"{META_PAYEE_LEGAL_NAME}": "KWICK LIVING (I) PRIVATE LIMITED",
        "{META_PAYMENT_ADVICE_AMOUNT}": "719489.19",
	}},
	"l2_table": [
		{{
			"invoice_number": "MH25/252601063",
			"invoice_date": "05-06-2025",
			"invoice_description": "8OXO1R7L/ISK3/##NOT_AVAILABLE",
			"discount_taken": null,
			"amount_paid": 158104.9,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "MH25/252601063-TDS-CM-0997",
			"invoice_date": "05-06-2025",
			"invoice_description": "India TDS Invoice for AP-194Q",
			"discount_taken": null,
			"amount_paid": -133.99,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "MH25/252601289",
			"invoice_date": "11-06-2025",
			"invoice_description": "2XFB1ZSB/ISK3/##NOT_AVAILABLE",
			"discount_taken": null,
			"amount_paid": 562835.46,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "MH25/252601289-TDS-CM-8360",
			"invoice_date": "11-06-2025",
			"invoice_description": "India TDS Invoice for AP-194Q",
			"discount_taken": null,
			"amount_paid": -476.98,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "KWIGM-30292516672552-AMD2-L-1405",
			"invoice_date": "07-07-2025",
			"invoice_description": "RTV FCN-KWIGM-30292516672552-AMD2- L-1405",
			"discount_taken": null,
			"amount_paid": -65.18,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": "KWIGM-30296863171552-PAX1-L-189",
			"invoice_date": "07-07-2025",
			"invoice_description": "RTV FCN-KWIGM-30296863171552-PAX1- L-189",
			"discount_taken": null,
			"amount_paid": -775.02,
			"amount_remaining": 0
		}},
		{{
			"invoice_number": null,
			"invoice_date": "11-07-2025",
			"invoice_description": "Bank Receipt",
			"discount_taken": null,
			"amount_paid": -719489.19,
			"amount_remaining": null
		}}
	]
}}

# CONSTRAINTS
 - Do not include markdown, headers, or commentary.
 - Return JSON exactly in the schema above.
 - Asterisks, commas, and parentheses must be cleaned per rules.
 - If any required data is missing, return the error JSON and stop.
- The amounts should retain their '-' negative sign if they are in () brackets
 - Read the first non-zero numeric value in the row (either "Discount Taken" or "Amount Paid").
      – Assign that value to amount_paid.
      – If this rule moves a value out of "Discount Taken", set discount_taken to null.
- If the synthetic Bank-Receipt row does not show the same payment_advice_number in invoice_number, return:
  {{"error":"bank-receipt invoice_number missing or mismatched"}} and stop.
        """



ZEPTO_PROMPT = f"""
    System prompt

    You are an intelligent document processor designed to extract structured data from payment advice PDFs.
    You will:
    1. Extract high-level details into a Meta Table from the header.
    2. Extract line-item table data and normalize invoice numbers in the Ref Doc column.
    Follow all output rules strictly and avoid assumptions or summarization.


    User prompt

    Extract structured data from the provided payment advice PDF. Your output must include two tables:

    Table 1: Meta Table
    Extract the following fields from the header section:

    Settlement Date: Value beside 'Payment Date'
    Payment Advice Number: Value beside 'Payment Doc' (fifth line item in the header). Do not use 'Payment Ref No.'
    Payer's Name: Topmost entity name on the document
    Payee's Legal Name: Value beside 'Payee' (strip internal codes if any)
    Return this as a JSON object with key-value pairs.

    Table 2: Body Table (with Ref Doc Normalization)
    Extract all rows under the section with headers:

    Sr No., Type of Document, Doc No, Ref Doc, Amount, Currency, TDS, Payment Amt.

    IMPORTANT: For each row's "Ref Doc" field:
    - For "Invoice Payment" type documents, extract ONLY the invoice reference number WITHOUT the amount
    - Example: If the PDF shows "B2BOS24/22468 39,012.76", extract ONLY "B2BOS24/22468" as the Ref Doc value
    - The amount should ONLY appear in the "Amount" column
    - When the Ref Doc field spans multiple lines, join them without any separator

    Here are examples of how different row types should be parsed:
    
    Example 1 - Credit Memo:
    Sr No. | Type of Document | Doc No    | Ref Doc      | Amount    | Currency | TDS  | Payment Amt.
    1      | Credit Memo      | 100024216 | KK10009485   | -295,000  | INR      | 0    | -295,000
    
    Example 2 - Credit Memo with split reference:
    Sr No. | Type of Document | Doc No    | Ref Doc      | Amount    | Currency | TDS  | Payment Amt.
    3      | Credit Memo      | 1700032041| B2BOS24/22463| -158.4    | INR      | 0.13 | -158.27
    
    Example 3 - Invoice Payment:
    Sr No. | Type of Document | Doc No    | Ref Doc      | Amount    | Currency | TDS  | Payment Amt.
    7      | Invoice Payment  | 1900165619| B2BOS24/22468| 39,012.76 | INR      | 33.06| 38,979.7

    Output Format:
    Return both tables in structured JSON format.

    The Meta Table should be a single JSON object with key '{LLM_META_TABLE_KEY}' and should contain:
    "{META_PAYMENT_ADVICE_DATE}": Value beside 'Payment Date'
    "{META_PAYMENT_ADVICE_NUMBER}": Value beside 'Payment Doc'
    "{META_PAYER_LEGAL_NAME}": Topmost entity name on the document
    "{META_PAYEE_LEGAL_NAME}": Value beside 'Payee'

    The Body Table should be a JSON array with key '{LLM_BODY_TABLE_KEY}' with row-wise objects. Each row should contain:
    "Sr No."
    "Type of Document"
    "Doc No"
    "Ref Doc"
    "Amount"
    "Currency"
    "TDS"
    "Payment Amt."

    Strict rules:
    Do not return headers and rows separately
    Do not output markdown
    Do not summarize or infer missing fields
    Do not include amounts in the Ref Doc field

    Note: The bank payment which we have received is mentioned in the header. I want you to include that also in the body table as the last entry. Sr.no can be kept blank, Type of Document shall be 'Bank receipt', Doc no. will be the number given against 'Payment Ref No' in the header, Ref doc can be kept blank, Amount shall be the amount mentioned in the header, Currency should be as mentioned in the header, TDs shall be blank and Payment amount shall be equal to the Amount also. Make both these amounts in negative
    """
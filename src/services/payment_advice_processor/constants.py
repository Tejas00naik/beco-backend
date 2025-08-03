"""
Constants for LLM output formats.

This file centralizes all the key names and formats used in LLM prompt templates
and downstream processing to ensure consistency throughout the application.
"""

# Table keys for LLM output
LLM_META_TABLE_KEY = "meta_table"
LLM_BODY_TABLE_KEY = "body_table"
LLM_INVOICE_TABLE_KEY = "invoice_table"
LLM_SETTLEMENT_TABLE_KEY = "settlement_table"
LLM_OTHER_DOC_TABLE_KEY = "other_doc_table"
LLM_RECONCILIATION_STATEMENT_KEY = "reconciliation_statement"
LLM_PAYMENT_ADVICE_LINES_KEY = "paymentadvice_lines"

# Meta table field keys
META_PAYMENT_ADVICE_NUMBER = "payment_advice_number"
META_PAYMENT_ADVICE_DATE = "payment_advice_date"
META_PAYMENT_ADVICE_AMOUNT = "payment_advice_amount"
META_PAYMENT_AMOUNT = "payment_amount"  # Alternative key
META_SETTLEMENT_DATE = "settlement_date"  # Alternative key for payment_advice_date
META_PAYER_NAME = "payer_name"
META_PAYER_LEGAL_NAME = "payer_legal_name"  # Alternative key
META_PAYEE_NAME = "payee_name"
META_PAYEE_LEGAL_NAME = "payee_legal_name"  # Alternative key

# Entity and group identifiers
LLM_LEGAL_ENTITY_UUID_KEY = "legal_entity_uuid"
LLM_GROUP_UUID_KEY = "group_uuid"
LLM_GROUP_UUIDS_KEY = "group_uuids"

# Payment advice line fields
PA_LINE_BP_CODE = "bp_code"
PA_LINE_GL_CODE = "gl_code"
PA_LINE_ACCOUNT_TYPE = "account_type"
PA_LINE_CUSTOMER = "customer"
PA_LINE_DOC_TYPE = "doc_type"
PA_LINE_DOC_NUMBER = "doc_number"
PA_LINE_REF_INVOICE_NO = "ref_invoice_no"
PA_LINE_REF_1 = "ref_1"
PA_LINE_REF_2 = "ref_2"
PA_LINE_REF_3 = "ref_3"
PA_LINE_AMOUNT = "amount"
PA_LINE_DR_CR = "dr_cr"
PA_LINE_DR_AMT = "dr_amt"
PA_LINE_CR_AMT = "cr_amt"
PA_LINE_BRANCH_NAME = "branch_name"

# Camel case alternatives (for backward compatibility)
LLM_META_TABLE_KEY_CAMEL = "metaTable"
LLM_BODY_TABLE_KEY_CAMEL = "bodyTable"
LLM_INVOICE_TABLE_KEY_CAMEL = "invoiceTable"
LLM_SETTLEMENT_TABLE_KEY_CAMEL = "settlementTable"
LLM_OTHER_DOC_TABLE_KEY_CAMEL = "otherDocTable"
META_PAYMENT_ADVICE_NUMBER_CAMEL = "paymentAdviceNumber"
META_PAYMENT_ADVICE_DATE_CAMEL = "paymentAdviceDate"
META_PAYMENT_ADVICE_AMOUNT_CAMEL = "paymentAdviceAmount"
META_PAYER_LEGAL_NAME_CAMEL = "payersLegalName"
META_PAYEE_LEGAL_NAME_CAMEL = "payeesLegalName"


GROUP_UUIDS = {
    "amazon": "group-amazon-12345",
    "zepto": "group-zepto-67890",
    "hot": "group-hot-54321"
}

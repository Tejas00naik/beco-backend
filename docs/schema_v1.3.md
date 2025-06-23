# Transaction Log Schema v1.3

This document describes the schema for the transaction log system used in the BECO batch worker application.

## Changes in v1.3
- Added support for multiple group UUIDs per email log (`group_uuids: List[str]`)
- Added SAP transaction ID fields to Invoice, OtherDoc, and Settlement tables
- Enforced uniqueness constraints on invoice_number and other_doc_number at the application level

## Tables

### BatchRun

| Field        | Type                | Description                                    | Required | Notes                                     |
|--------------|---------------------|------------------------------------------------|----------|-------------------------------------------|
| run_id       | String (UUID)       | Unique identifier for the batch run            | Yes      | Primary key                               |
| start_ts     | Timestamp           | Timestamp when the batch run started           | Yes      |                                           |
| end_ts       | Timestamp           | Timestamp when the batch run completed         | No       | Null if run is still in progress          |
| status       | String (Enum)       | Current status of the batch run                | Yes      | Values: NEW, IN_PROGRESS, SUCCESS, FAILED, PARTIAL |
| emails_processed | Integer         | Count of emails processed in this run          | No       | Default: 0                                |
| errors       | Integer             | Count of errors encountered in this run        | No       | Default: 0                                |

### EmailLog

| Field         | Type                  | Description                                  | Required | Notes                                     |
|---------------|------------------------|----------------------------------------------|----------|-------------------------------------------|
| email_log_uuid | String (UUID)        | Unique identifier for the email log          | Yes      | Primary key                               |
| batch_run_id  | String (UUID)         | Reference to the batch run                   | Yes      | Foreign key to BatchRun                   |
| email_id      | String                | External identifier for the email            | Yes      | Should be unique across email source      |
| email_source  | String (Enum)         | Source of the email                          | Yes      | Values: GMAIL, OUTLOOK, etc.              |
| received_date | Timestamp             | Date when the email was received             | Yes      |                                           |
| subject       | String                | Email subject                                | Yes      |                                           |
| sender        | String                | Email sender address                         | Yes      |                                           |
| mailbox_id    | String                | Identifier for the mailbox                   | Yes      |                                           |
| group_uuids   | Array of String (UUID) | List of group UUIDs associated with email   | No       | Can belong to multiple groups             |
| created_at    | Timestamp             | When the record was created                  | Yes      |                                           |
| updated_at    | Timestamp             | When the record was last updated             | Yes      |                                           |

### EmailProcessingLog

| Field               | Type          | Description                                  | Required | Notes                                     |
|---------------------|---------------|----------------------------------------------|----------|-------------------------------------------|
| email_processing_uuid | String (UUID) | Unique identifier for processing log       | Yes      | Primary key                               |
| email_log_uuid      | String (UUID)  | Reference to the email log                  | Yes      | Foreign key to EmailLog                   |
| run_id              | String (UUID)  | Batch run reference                         | Yes      | Foreign key to BatchRun                   |
| processing_status   | String (Enum)  | Status of processing                        | Yes      | Values: SUCCESS, FAILED, PARTIAL          |
| error_message       | String         | Error message if processing failed          | No       | Null if successful                        |
| created_at          | Timestamp      | When the record was created                 | Yes      |                                           |
| updated_at          | Timestamp      | When the record was last updated            | Yes      |                                           |

### PaymentAdvice

| Field                 | Type          | Description                               | Required | Notes                                     |
|-----------------------|---------------|-------------------------------------------|----------|-------------------------------------------|
| payment_advice_uuid   | String (UUID)  | Unique identifier for payment advice     | Yes      | Primary key                               |
| email_log_uuid        | String (UUID)  | Reference to the email log               | Yes      | Foreign key to EmailLog                   |
| legal_entity_uuid     | String (UUID)  | Reference to the legal entity            | Yes      | Foreign key to LegalEntity                |
| payment_advice_number | String         | Payment advice number                    | Yes      |                                           |
| payment_advice_date   | Date           | Date of payment advice                   | Yes      |                                           |
| payment_advice_amount | Decimal        | Total amount in the payment advice       | Yes      |                                           |
| payment_advice_status | String (Enum)  | Status of payment advice                 | Yes      | Values: NEW, PROCESSING, COMPLETED, FAILED |
| payer_name            | String         | Name of the payer                        | No       |                                           |
| payee_name            | String         | Name of the payee                        | No       |                                           |
| created_at            | Timestamp      | When the record was created              | Yes      |                                           |
| updated_at            | Timestamp      | When the record was last updated         | Yes      |                                           |

### Invoice

| Field             | Type          | Description                                  | Required | Notes                                     |
|-------------------|---------------|----------------------------------------------|----------|-------------------------------------------|
| invoice_uuid      | String (UUID)  | Unique identifier for invoice                | Yes      | Primary key                               |
| payment_advice_uuid | String (UUID) | Reference to the payment advice             | Yes      | Foreign key to PaymentAdvice              |
| customer_uuid     | String (UUID)  | Reference to the customer                    | Yes      | Foreign key to Customer                   |
| invoice_number    | String         | Invoice number                              | Yes      | Must be unique (enforced at app level)    |
| invoice_date      | Date           | Date of invoice                             | Yes      |                                           |
| booking_amount    | Decimal        | Amount of the invoice                       | Yes      |                                           |
| invoice_status    | String (Enum)  | Status of invoice                           | Yes      | Values: OPEN, CLOSED, PARTIALLY_PAID      |
| sap_transaction_id | String        | SAP transaction ID for reconciliation        | No       | Set after successful SAP reconciliation   |
| created_at        | Timestamp      | When the record was created                 | Yes      |                                           |
| updated_at        | Timestamp      | When the record was last updated            | Yes      |                                           |

### OtherDoc

| Field             | Type          | Description                                  | Required | Notes                                     |
|-------------------|---------------|----------------------------------------------|----------|-------------------------------------------|
| other_doc_uuid    | String (UUID)  | Unique identifier for other document        | Yes      | Primary key                               |
| payment_advice_uuid | String (UUID) | Reference to the payment advice            | Yes      | Foreign key to PaymentAdvice              |
| customer_uuid     | String (UUID)  | Reference to the customer                   | Yes      | Foreign key to Customer                   |
| other_doc_number  | String         | Document number                             | Yes      | Must be unique (enforced at app level)    |
| other_doc_date    | Date           | Date of document                            | Yes      |                                           |
| other_doc_type    | String (Enum)  | Type of document                           | Yes      | Values: CREDIT_NOTE, DEBIT_NOTE, OTHER    |
| other_doc_amount  | Decimal        | Amount of the document                      | Yes      |                                           |
| sap_transaction_id | String        | SAP transaction ID for reconciliation        | No       | Set after successful SAP reconciliation   |
| created_at        | Timestamp      | When the record was created                 | Yes      |                                           |
| updated_at        | Timestamp      | When the record was last updated            | Yes      |                                           |

### Settlement

| Field             | Type          | Description                                  | Required | Notes                                     |
|-------------------|---------------|----------------------------------------------|----------|-------------------------------------------|
| settlement_uuid   | String (UUID)  | Unique identifier for settlement            | Yes      | Primary key                               |
| payment_advice_uuid | String (UUID) | Reference to the payment advice            | Yes      | Foreign key to PaymentAdvice              |
| customer_uuid     | String (UUID)  | Reference to the customer                   | Yes      | Foreign key to Customer                   |
| invoice_uuid      | String (UUID)  | Reference to the invoice                    | No       | Foreign key to Invoice, null for other_doc settlements |
| other_doc_uuid    | String (UUID)  | Reference to other document                | No       | Foreign key to OtherDoc, null for invoice settlements |
| settlement_date   | Date           | Date of settlement                         | Yes      |                                           |
| settlement_amount | Decimal        | Amount of the settlement                    | Yes      |                                           |
| settlement_status | String (Enum)  | Status of settlement                       | Yes      | Values: READY, PROCESSING, COMPLETED, FAILED |
| sap_transaction_id | String        | SAP transaction ID for reconciliation        | No       | Set after successful SAP reconciliation   |
| created_at        | Timestamp      | When the record was created                 | Yes      |                                           |
| updated_at        | Timestamp      | When the record was last updated            | Yes      |                                           |

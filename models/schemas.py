"""
Data models for Firestore collections based on the provided schema.

Core Master Data, Transaction Data, and Processing Metadata schemas
as defined in the project documentation.
"""
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from uuid import uuid4
import enum


# Enums
class PaymentAdviceStatus(str, enum.Enum):
    NEW = "new"
    READY = "ready"
    RECONCILED = "reconciled"
    FLAGGED = "flagged"


class InvoiceStatus(str, enum.Enum):
    OPEN = "open"
    RECONCILED = "reconciled"


class OtherDocType(str, enum.Enum):
    BR = "BR"  # Bank Receipt
    BDPO = "BDPO"  # Bank Draft/Payment Order
    DN = "DN"  # Debit Note
    CN = "CN"  # Credit Note
    RTV = "RTV"  # Return to Vendor
    TDS = "TDS"  # Tax Deducted at Source
    OTHER = "OTHER"  # Other document types


class SettlementStatus(str, enum.Enum):
    READY = "ready"
    PUSHED = "pushed"
    ERROR = "error"


class ProcessingStatus(str, enum.Enum):
    PARSED = "parsed"
    SAP_PUSHED = "sap_pushed"
    ERROR = "error"


class BatchRunStatus(str, enum.Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


# Base model with common fields
@dataclass
class BaseModel:
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


# Core Master-Data Schema models
@dataclass
class Group(BaseModel):
    group_uuid: str = field(default_factory=lambda: str(uuid4()))
    group_name: str = ""
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None
    group_created_at: datetime = field(default_factory=datetime.utcnow)
    group_updated_at: datetime = field(default_factory=datetime.utcnow)
    group_deleted_at: Optional[datetime] = None


@dataclass
class LegalEntity(BaseModel):
    legal_entity_uuid: str = field(default_factory=lambda: str(uuid4()))
    legal_entity_name: str = ""
    sap_id: Optional[str] = None
    sap_name: Optional[str] = None
    is_active: bool = True
    group_uuid: str = ""
    metadata: Optional[Dict[str, Any]] = None
    legal_entity_created_at: datetime = field(default_factory=datetime.utcnow)
    legal_entity_updated_at: datetime = field(default_factory=datetime.utcnow)
    legal_entity_deleted_at: Optional[datetime] = None


@dataclass
class Customer(BaseModel):
    customer_uuid: str = field(default_factory=lambda: str(uuid4()))
    customer_name: str = ""
    sap_customer_id: Optional[str] = None
    sap_customer_name: Optional[str] = None
    state: Optional[str] = None
    payment_term_in_days: int = 0
    is_active: bool = True
    legal_entity_uuid: str = ""
    metadata: Optional[Dict[str, Any]] = None
    customer_created_at: datetime = field(default_factory=datetime.utcnow)
    customer_updated_at: datetime = field(default_factory=datetime.utcnow)
    customer_deleted_at: Optional[datetime] = None


@dataclass
class Email(BaseModel):
    email_uuid: str = field(default_factory=lambda: str(uuid4()))
    email_address: str = ""
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None
    email_created_at: datetime = field(default_factory=datetime.utcnow)
    email_updated_at: datetime = field(default_factory=datetime.utcnow)
    email_deleted_at: Optional[datetime] = None


@dataclass
class Domain(BaseModel):
    domain_uuid: str = field(default_factory=lambda: str(uuid4()))
    domain_name: str = ""
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None
    domain_created_at: datetime = field(default_factory=datetime.utcnow)
    domain_updated_at: datetime = field(default_factory=datetime.utcnow)
    domain_deleted_at: Optional[datetime] = None


@dataclass
class CustEmailDomainMap(BaseModel):
    cust_email_domain_map_uuid: str = field(default_factory=lambda: str(uuid4()))
    customer_uuid: str = ""
    email_uuid: str = ""
    domain_uuid: str = ""
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    deleted_at: Optional[datetime] = None


# Transaction-Data Schema models
@dataclass
class EmailLog(BaseModel):
    email_log_uuid: str = field(default_factory=lambda: str(uuid4()))
    group_uuids: List[str] = field(default_factory=list)  # Multiple FK links to Group - derived from LLM-identified legal entities
    email_object_file_path: str = ""
    received_at: datetime = field(default_factory=datetime.utcnow)
    sender_mail: str = ""
    original_sender_mail: Optional[str] = None
    email_subject: Optional[str] = None
    mailbox_id: Optional[str] = None


@dataclass
class PaymentAdvice(BaseModel):
    payment_advice_uuid: str = field(default_factory=lambda: str(uuid4()))
    email_log_uuid: str = ""
    legal_entity_uuid: Optional[str] = None  # FK to LegalEntity - set by LLM per advice
    payment_advice_number: Optional[str] = None
    payment_advice_date: Optional[date] = None
    payment_advice_amount: Optional[float] = None
    payment_advice_status: PaymentAdviceStatus = PaymentAdviceStatus.NEW
    payer_name: Optional[str] = None
    payee_name: Optional[str] = None


@dataclass
class Invoice(BaseModel):
    invoice_uuid: str = field(default_factory=lambda: str(uuid4()))
    payment_advice_uuid: str = ""
    customer_uuid: Optional[str] = None  # FK to Customer - derived per line by LLM
    invoice_number: str = ""  # Should be unique within the system
    invoice_date: Optional[date] = None
    booking_amount: Optional[float] = None
    total_settlement_amount: Optional[float] = None  # Total amount settled against this invoice
    sap_transaction_id: Optional[str] = None  # SAP transaction ID after reconciliation
    invoice_status: InvoiceStatus = InvoiceStatus.OPEN


@dataclass
class OtherDoc(BaseModel):
    other_doc_uuid: str = field(default_factory=lambda: str(uuid4()))
    payment_advice_uuid: str = ""
    customer_uuid: Optional[str] = None  # FK to Customer - derived per line by LLM
    other_doc_number: str = ""  # Should be unique within the system
    other_doc_date: Optional[date] = None
    other_doc_type: OtherDocType = OtherDocType.OTHER
    other_doc_amount: Optional[float] = None
    sap_transaction_id: Optional[str] = None  # SAP transaction ID after reconciliation


@dataclass
class Settlement(BaseModel):
    settlement_uuid: str = field(default_factory=lambda: str(uuid4()))
    payment_advice_uuid: str = ""
    customer_uuid: Optional[str] = None  # FK to Customer - derived from invoice/other_doc
    invoice_uuid: Optional[str] = None  # FK to Invoice - exactly one of invoice_uuid or other_doc_uuid must be set
    other_doc_uuid: Optional[str] = None  # FK to OtherDoc - exactly one of invoice_uuid or other_doc_uuid must be set
    settlement_date: Optional[date] = None
    settlement_amount: Optional[float] = None
    settlement_status: SettlementStatus = SettlementStatus.READY

    def __post_init__(self):
        # Enforce that exactly one of invoice_uuid or other_doc_uuid must be set
        invoice_set = self.invoice_uuid is not None
        other_doc_set = self.other_doc_uuid is not None
        
        if not (invoice_set ^ other_doc_set):  # XOR operation
            raise ValueError("Exactly one of invoice_uuid or other_doc_uuid must be set")


# Processing-Metadata Schema models
@dataclass
class BatchRun(BaseModel):
    run_id: str = field(default_factory=lambda: str(uuid4()))
    start_ts: datetime = field(default_factory=datetime.utcnow)
    end_ts: Optional[datetime] = None
    status: BatchRunStatus = BatchRunStatus.SUCCESS
    emails_processed: int = 0
    errors: int = 0
    mailbox_id: Optional[str] = None
    run_mode: str = "incremental"  # Options: 'incremental', 'full_refresh'


@dataclass
class EmailProcessingLog(BaseModel):
    email_log_uuid: str = ""
    run_id: str = ""
    processing_status: ProcessingStatus = ProcessingStatus.PARSED
    sap_doc_num: Optional[str] = None
    error_msg: Optional[str] = None


@dataclass
class SapErrorDlq(BaseModel):
    dlq_id: str = field(default_factory=lambda: str(uuid4()))
    payment_advice_uuid: str = ""
    sap_payload: Dict[str, Any] = field(default_factory=dict)
    sap_response: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    next_retry_ts: Optional[datetime] = None

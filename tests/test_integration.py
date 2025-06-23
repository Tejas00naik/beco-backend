"""
Integration tests for the batch worker
These tests simulate the end-to-end data flow using real mock components
(but still mocking the Firestore access).
"""

import pytest
import asyncio
import os
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.main import BatchWorker
from src.mocks.email_reader import MockEmailReader
from src.mocks.llm_extractor import MockLLMExtractor
from src.mocks.sap_caller import MockSapCaller
from models.schemas import BatchRunStatus, Invoice, OtherDoc, InvoiceStatus, OtherDocType, Settlement, SettlementStatus


@pytest.fixture
def mock_firestore_dao():
    """Create a mock FirestoreDAO that tracks all operations."""
    mock_dao = AsyncMock()
    
    # Storage to track created documents
    mock_dao._documents = {}
    
    # Override add_document to track documents
    async def add_document(collection, doc_id, data):
        if collection not in mock_dao._documents:
            mock_dao._documents[collection] = {}
        mock_dao._documents[collection][doc_id] = data
        return doc_id
    
    # Override update_document to update tracked documents
    async def update_document(collection, doc_id, updates):
        if collection in mock_dao._documents and doc_id in mock_dao._documents[collection]:
            for key, value in updates.items():
                setattr(mock_dao._documents[collection][doc_id], key, value)
    
    # Override create_settlement to track settlements
    async def create_settlement(settlement):
        # Include customer_uuid in the settlement ID for uniqueness with the new schema
        settlement_id = f"{settlement.payment_advice_uuid}_{settlement.invoice_uuid or '_'}_{settlement.other_doc_uuid or '_'}_{settlement.customer_uuid}"
        if "settlement" not in mock_dao._documents:
            mock_dao._documents["settlement"] = {}
        mock_dao._documents["settlement"][settlement_id] = settlement
        return settlement_id
        
    # Helper methods to ensure all collection types are tracked
    async def create_email_log(email_log):
        return await add_document("email_log", email_log.email_log_uuid, email_log)
    
    async def create_payment_advice(payment_advice):
        return await add_document("payment_advice", payment_advice.payment_advice_uuid, payment_advice)
        
    async def create_invoice(invoice):
        return await add_document("invoice", invoice.invoice_uuid, invoice)
        
    async def create_other_doc(other_doc):
        return await add_document("other_doc", other_doc.other_doc_uuid, other_doc)
    
    async def create_batch_run(batch_run):
        return await add_document("batch_run", batch_run.run_id, batch_run)
        
    async def update_batch_run(run_id, updates):
        await update_document("batch_run", run_id, updates)
        
    async def create_email_processing_log(email_processing_log):
        doc_id = f"{email_processing_log.email_log_uuid}_{email_processing_log.run_id}"
        return await add_document("email_processing_log", doc_id, email_processing_log)
    
    # Assign mocked methods
    mock_dao.add_document = AsyncMock(side_effect=add_document)
    mock_dao.update_document = AsyncMock(side_effect=update_document)
    mock_dao.create_settlement = AsyncMock(side_effect=create_settlement)
    mock_dao.create_email_log = AsyncMock(side_effect=create_email_log)
    mock_dao.create_payment_advice = AsyncMock(side_effect=create_payment_advice)
    mock_dao.create_invoice = AsyncMock(side_effect=create_invoice)
    mock_dao.create_other_doc = AsyncMock(side_effect=create_other_doc)
    mock_dao.create_batch_run = AsyncMock(side_effect=create_batch_run)
    mock_dao.update_batch_run = AsyncMock(side_effect=update_batch_run)
    mock_dao.create_email_processing_log = AsyncMock(side_effect=create_email_processing_log)
    
    return mock_dao


@pytest.fixture
def integration_batch_worker(mock_firestore_dao):
    """Create a BatchWorker with real mock components but mock DAO."""
    with patch("os.environ") as mock_env:
        mock_env.get.return_value = "test-project-id"
        
        worker = BatchWorker(is_test=True)
        
        # Replace the DAO with our tracked mock
        worker.dao = mock_firestore_dao
        
        # Keep the real mock components
        worker.email_reader = MockEmailReader(max_emails=2, is_test=True)  # Limit to 2 emails for testing and ensure generation
        worker.llm_extractor = MockLLMExtractor()
        worker.sap_caller = MockSapCaller(failure_rate=0.2)  # 20% failure rate
        
        yield worker


@pytest.mark.asyncio
async def test_end_to_end_flow(integration_batch_worker):
    """Test the full end-to-end flow with real mock components."""
    worker = integration_batch_worker
    
    # Run the batch worker
    await worker.run()
    
    # Verify batch run was created and updated
    assert "batch_run" in worker.dao._documents
    assert len(worker.dao._documents["batch_run"]) == 1
    
    # Get the batch run
    batch_run_id = list(worker.dao._documents["batch_run"].keys())[0]
    batch_run = worker.dao._documents["batch_run"][batch_run_id]
    
    # Verify batch run has required fields
    assert batch_run.run_id == batch_run_id
    assert batch_run.start_ts is not None
    assert batch_run.end_ts is not None
    assert batch_run.status in [BatchRunStatus.SUCCESS, BatchRunStatus.PARTIAL, BatchRunStatus.FAILED]
    
    # Verify emails were processed
    assert "email_log" in worker.dao._documents
    assert len(worker.dao._documents["email_log"]) > 0
    
    # Verify email logs have group_uuids (updated schema v1.3)
    email_log = list(worker.dao._documents["email_log"].values())[0]
    assert hasattr(email_log, 'group_uuids')
    assert isinstance(email_log.group_uuids, list)
    
    # Verify email processing logs were created
    assert "email_processing_log" in worker.dao._documents
    assert len(worker.dao._documents["email_processing_log"]) > 0
    
    # Verify payment advices were created
    assert "payment_advice" in worker.dao._documents
    assert len(worker.dao._documents["payment_advice"]) > 0
    
    # Verify payment advices have legal_entity_uuid (new schema)
    payment_advice = list(worker.dao._documents["payment_advice"].values())[0]
    assert hasattr(payment_advice, 'legal_entity_uuid')
    
    # Verify invoices and settlements were created
    assert "invoice" in worker.dao._documents
    assert len(worker.dao._documents["invoice"]) > 0
    assert "settlement" in worker.dao._documents
    assert len(worker.dao._documents["settlement"]) > 0
    
    # Verify invoices have customer_uuid (new schema)
    invoice = list(worker.dao._documents["invoice"].values())[0]
    assert hasattr(invoice, 'customer_uuid')
    
    # Verify settlements have customer_uuid (new schema)
    settlement = list(worker.dao._documents["settlement"].values())[0]
    assert hasattr(settlement, 'customer_uuid')
    
    # Check relationships between entities
    # Get a payment advice
    payment_advice_id = list(worker.dao._documents["payment_advice"].keys())[0]
    payment_advice = worker.dao._documents["payment_advice"][payment_advice_id]
    
    # Find invoices for this payment advice
    invoices_for_pa = [
        inv for inv in worker.dao._documents["invoice"].values()
        if inv.payment_advice_uuid == payment_advice_id
    ]
    assert len(invoices_for_pa) > 0
    
    # Find settlements for this payment advice
    settlements_for_pa = [
        s for s in worker.dao._documents["settlement"].values()
        if s.payment_advice_uuid == payment_advice_id
    ]
    assert len(settlements_for_pa) > 0
    
    # Verify email logs are linked to email processing logs
    email_log_id = list(worker.dao._documents["email_log"].keys())[0]
    processing_logs_for_email = [
        log for log in worker.dao._documents["email_processing_log"].values()
        if log.email_log_uuid == email_log_id
    ]
    assert len(processing_logs_for_email) > 0


@pytest.mark.asyncio
async def test_uniqueness_constraints(integration_batch_worker):
    """Test that uniqueness constraints are enforced for invoice_number and other_doc_number."""
    worker = integration_batch_worker
    
    # Create a test invoice with a specific number
    test_invoice_number = "INV-TEST-UNIQUE-001"
    test_invoice1 = Invoice(
        invoice_uuid="test-invoice-uuid-1",
        payment_advice_uuid="test-payment-advice-uuid",
        customer_uuid="test-customer-uuid",
        invoice_number=test_invoice_number,
        invoice_date=datetime.utcnow(),
        booking_amount=1000.0,
        invoice_status=InvoiceStatus.OPEN,
        sap_transaction_id=None
    )
    
    # Create a test other document with a specific number
    test_doc_number = "DOC-TEST-UNIQUE-001"
    test_other_doc1 = OtherDoc(
        other_doc_uuid="test-other-doc-uuid-1",
        payment_advice_uuid="test-payment-advice-uuid",
        customer_uuid="test-customer-uuid",
        other_doc_number=test_doc_number,
        other_doc_date=datetime.utcnow(),
        other_doc_type=OtherDocType.OTHER,
        other_doc_amount=500.0,
        sap_transaction_id=None
    )
    
    # Add the first invoice and other doc
    await worker.dao.add_document("invoice", test_invoice1.invoice_uuid, test_invoice1)
    await worker.dao.add_document("other_doc", test_other_doc1.other_doc_uuid, test_other_doc1)
    
    # Verify they were added
    assert "invoice" in worker.dao._documents
    assert test_invoice1.invoice_uuid in worker.dao._documents["invoice"]
    assert "other_doc" in worker.dao._documents
    assert test_other_doc1.other_doc_uuid in worker.dao._documents["other_doc"]
    
    # Now try to create duplicates with the same numbers
    duplicate_invoice_exists = await worker.check_document_exists("invoice", "invoice_number", test_invoice_number)
    duplicate_doc_exists = await worker.check_document_exists("other_doc", "other_doc_number", test_doc_number)
    
    # Assert that duplicates are detected
    assert duplicate_invoice_exists is True, "Uniqueness check for invoice_number failed"
    assert duplicate_doc_exists is True, "Uniqueness check for other_doc_number failed"
    
    # Try to add duplicates through the process_payment_advice method
    # Create mock data for payment advice processing
    email_log_uuid = "test-email-log-uuid"
    pa_data = {
        "legal_entity_uuid": "test-legal-entity-uuid",
        "payment_advice_number": "PA-TEST-001",
        "payment_advice_date": datetime.utcnow(),
        "payment_advice_amount": 1500.0,
        "payer_name": "Test Payer",
        "payee_name": "Test Payee"
    }
    
    # Mock email data and transaction details
    email_data = {"email_id": "test-email-id"}
    
    # Patch the extract_transaction_details method to return our test data with duplicate invoice number
    original_extract = worker.llm_extractor.extract_transaction_details
    
    # Prepare mock data for duplicate test
    mock_invoice_data = [{
        "customer_uuid": "test-customer-uuid",
        "invoice_number": test_invoice_number,  # Duplicate invoice number
        "invoice_date": datetime.utcnow(),
        "booking_amount": 1000.0
    }]
    
    mock_other_doc_data = [{
        "customer_uuid": "test-customer-uuid",
        "other_doc_number": test_doc_number,  # Duplicate document number
        "other_doc_date": datetime.utcnow(),
        "other_doc_type": OtherDocType.OTHER,
        "other_doc_amount": 500.0
    }]
    
    mock_settlements_data = []
    
    # Define a regular (non-async) mock function
    def mock_extract(*args, **kwargs):
        return mock_invoice_data, mock_other_doc_data, mock_settlements_data
    
    # Set mock extractor
    worker.llm_extractor.extract_transaction_details = mock_extract
    
    try:
        # Process the payment advice with duplicate invoice and document numbers
        await worker.process_payment_advice(email_log_uuid, pa_data, email_data, 0)
        
        # Count invoices and other docs after processing
        invoice_count = len(worker.dao._documents["invoice"])
        other_doc_count = len(worker.dao._documents["other_doc"])
        
        # The counts should still be 1 for each since duplicates should be skipped
        assert invoice_count == 1, f"Expected 1 invoice, got {invoice_count}. Duplicate was not skipped."
        assert other_doc_count == 1, f"Expected 1 other doc, got {other_doc_count}. Duplicate was not skipped."
    finally:
        # Restore original extractor
        worker.llm_extractor.extract_transaction_details = original_extract


@pytest.mark.asyncio
async def test_empty_batch_run(integration_batch_worker):
    """Test a batch run with no new emails."""
    worker = integration_batch_worker
    
    # Setup a mock email reader that returns no emails
    class EmptyMockEmailReader(MockEmailReader):
        def get_unprocessed_emails(self, since_timestamp=None):
            return []
    
    # Replace the email reader with our empty one
    worker.email_reader = EmptyMockEmailReader()
    
    # Run the batch worker
    await worker.run()
    
    # Verify batch run was created and updated
    assert "batch_run" in worker.dao._documents
    assert len(worker.dao._documents["batch_run"]) == 1
    
    # Check that no emails were processed
    assert "email_log" not in worker.dao._documents or len(worker.dao._documents["email_log"]) == 0

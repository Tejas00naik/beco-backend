"""
Tests for the main orchestrator logic
"""

import pytest
import asyncio
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.main import BatchWorker
from models.schemas import (
    EmailLog, PaymentAdvice, Invoice, OtherDoc, Settlement,
    BatchRun, EmailProcessingLog, ProcessingStatus, BatchRunStatus
)


@pytest.fixture
def mock_components():
    """Mock all the components used by the BatchWorker."""
    with patch("src.main.FirestoreDAO") as mock_dao, \
         patch("src.main.MockEmailReader") as mock_email_reader, \
         patch("src.main.MockLLMExtractor") as mock_llm_extractor, \
         patch("src.main.MockSapCaller") as mock_sap_caller:
        
        # Setup the mocks
        dao_instance = AsyncMock()
        mock_dao.return_value = dao_instance
        
        email_reader_instance = MagicMock()
        mock_email_reader.return_value = email_reader_instance
        
        llm_extractor_instance = MagicMock()
        mock_llm_extractor.return_value = llm_extractor_instance
        
        sap_caller_instance = MagicMock()
        mock_sap_caller.return_value = sap_caller_instance
        
        yield {
            "dao": dao_instance,
            "email_reader": email_reader_instance,
            "llm_extractor": llm_extractor_instance,
            "sap_caller": sap_caller_instance
        }


@pytest.fixture
def mock_env():
    """Mock environment variables."""
    with patch("os.environ") as mock_env:
        mock_env.get.return_value = "test-project-id"
        yield mock_env


@pytest.fixture
def batch_worker(mock_components, mock_env):
    """Create a BatchWorker instance with mocked components."""
    worker = BatchWorker(is_test=True)
    
    # Replace the real components with mocks
    worker.dao = mock_components["dao"]
    worker.email_reader = mock_components["email_reader"]
    worker.llm_extractor = mock_components["llm_extractor"]
    worker.sap_caller = mock_components["sap_caller"]
    
    return worker


@pytest.mark.asyncio
async def test_start_batch_run(batch_worker):
    """Test starting a batch run."""
    run_id = await batch_worker.start_batch_run()
    
    # Verify a batch run was created
    assert run_id is not None
    assert batch_worker.batch_run is not None
    assert batch_worker.batch_run.run_id == run_id
    assert batch_worker.batch_run.status == BatchRunStatus.SUCCESS
    
    # Verify it was stored in Firestore
    batch_worker.dao.add_document.assert_called_once()
    args, kwargs = batch_worker.dao.add_document.call_args
    assert args[0] == "batch_run"
    assert args[1] == run_id


@pytest.mark.asyncio
async def test_process_email_success(batch_worker):
    """Test processing an email successfully."""
    # Make sure all AsyncMock methods return awaitable objects
    # Setup the mock data
    email_id = str(uuid.uuid4())
    batch_worker.batch_run = BatchRun(
        run_id=str(uuid.uuid4()),
        start_ts=datetime.utcnow(),
        status=BatchRunStatus.SUCCESS
    )
    
    email_data = {
        "email_id": email_id,
        "object_file_path": "/mock/path/email.eml",
        "received_at": datetime.utcnow().isoformat(),
        "sender_mail": "test@example.com",
        "original_sender_mail": None,
        "legal_entity_uuid": None,
        "customer_uuid": None,
        "subject": "Payment Advice",
        "body": "This is a payment advice"
    }
    
    # Mock LLM responses
    batch_worker.llm_extractor.extract_email_metadata.return_value = {
        "payer_name": "Test Payer",
        "payee_name": "Test Payee"
    }
    
    batch_worker.llm_extractor.extract_payment_advices.return_value = [
        {
            "payment_advice_number": "PA-12345",
            "payment_advice_date": "2025-06-21",
            "payment_advice_amount": 1000.0
        }
    ]
    
    batch_worker.llm_extractor.extract_transaction_details.return_value = (
        # Invoices
        [
            {
                "invoice_number": "INV-001",
                "invoice_date": "2025-06-01",
                "booking_amount": 1000.0
            }
        ],
        # Other docs
        [],
        # Settlements
        [
            {
                "settlement_date": "2025-06-21",
                "settlement_amount": 1000.0
            }
        ]
    )
    
    # Force add_document to return None so we can track calls
    batch_worker.dao.add_document = AsyncMock(return_value=None)
    batch_worker.dao.update_document = AsyncMock(return_value=None)
    batch_worker.dao.create_settlement = AsyncMock(return_value=None)
    
    # Mock SAP caller
    batch_worker.sap_caller.reconcile_payment.return_value = (True, {"success": True})
    
    # Process the email
    result = await batch_worker.process_email(email_data)
    
    # Verify result
    assert result is True
    assert batch_worker.emails_processed == 1
    assert batch_worker.errors == 0
    
    # Verify the dao calls
    assert batch_worker.dao.add_document.await_count >= 1  # At least one add_document call
    assert batch_worker.dao.create_settlement.await_count >= 1  # At least one create_settlement call


@pytest.mark.asyncio
async def test_process_email_failure(batch_worker):
    """Test handling errors while processing an email."""
    # Setup the mock data
    email_id = str(uuid.uuid4())
    batch_worker.batch_run = BatchRun(
        run_id=str(uuid.uuid4()),
        start_ts=datetime.utcnow(),
        status=BatchRunStatus.SUCCESS
    )
    
    email_data = {
        "email_id": email_id,
        "object_file_path": "/mock/path/email.eml",
        "received_at": datetime.utcnow().isoformat(),
        "sender_mail": "test@example.com",
        "subject": "Payment Advice",
        "body": "This is a payment advice"
    }
    
    # Make the DAO throw an exception
    batch_worker.dao.add_document.side_effect = Exception("Test error")
    
    # Process the email
    result = await batch_worker.process_email(email_data)
    
    # Verify result
    assert result is False
    assert batch_worker.emails_processed == 0
    assert batch_worker.errors == 1


@pytest.mark.asyncio
async def test_run_with_no_emails(batch_worker):
    """Test running a batch with no new emails."""
    # Mock no new emails
    batch_worker.email_reader.get_unprocessed_emails.return_value = []
    
    # Run the batch
    await batch_worker.run()
    
    # Verify batch run was started and finished
    assert batch_worker.dao.add_document.call_count == 1  # BatchRun creation
    assert batch_worker.dao.update_document.call_count == 1  # BatchRun update
    
    # Verify no emails were processed
    assert batch_worker.emails_processed == 0


@pytest.mark.asyncio
async def test_run_with_emails(batch_worker):
    """Test running a batch with new emails."""
    # Setup the batch run and mock emails
    email_id1 = str(uuid.uuid4())
    email_id2 = str(uuid.uuid4())
    
    batch_worker.email_reader.get_unprocessed_emails.return_value = [
        {
            "email_id": email_id1,
            "object_file_path": "/mock/path/email1.eml",
            "received_at": datetime.utcnow().isoformat(),
            "sender_mail": "test1@example.com",
            "subject": "Payment Advice 1"
        },
        {
            "email_id": email_id2,
            "object_file_path": "/mock/path/email2.eml",
            "received_at": datetime.utcnow().isoformat(),
            "sender_mail": "test2@example.com",
            "subject": "Payment Advice 2"
        }
    ]
    
    # Mock process_email to track calls and return success
    batch_worker.process_email = AsyncMock(return_value=True)
    
    # Run the batch
    await batch_worker.run()
    
    # Verify both emails were processed
    assert batch_worker.process_email.call_count == 2
    
    # Verify batch run was properly finished
    assert batch_worker.dao.update_document.call_count == 1  # BatchRun update
    
    # Verify emails were marked as processed
    batch_worker.email_reader.mark_as_processed.assert_called_once()
    args, _ = batch_worker.email_reader.mark_as_processed.call_args
    assert set(args[0]) == {email_id1, email_id2}

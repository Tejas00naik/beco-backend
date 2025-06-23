"""
Unit tests for the Firestore Data Access Object (DAO)
"""

import pytest
import asyncio
import uuid
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock, patch

from models.schemas import (
    EmailLog, PaymentAdvice, Invoice, OtherDoc, Settlement,
    BatchRun, EmailProcessingLog, ProcessingStatus, BatchRunStatus
)
from models.firestore_dao import FirestoreDAO


@pytest.fixture
def mock_firestore():
    """Mock the Firestore client."""
    with patch("models.firestore_dao.firestore") as mock:
        client_mock = MagicMock()
        mock.Client.return_value = client_mock
        yield mock, client_mock


@pytest.fixture
def dao(mock_firestore):
    """Create a DAO instance with mocked Firestore client."""
    _, _ = mock_firestore
    return FirestoreDAO(project_id="test-project", collection_prefix="dev_")


@pytest.mark.asyncio
async def test_add_document(dao, mock_firestore):
    """Test adding a document to Firestore."""
    _, client_mock = mock_firestore
    
    # Mock the document reference
    doc_ref_mock = MagicMock()
    # Set is an async operation, so it needs to be an AsyncMock
    doc_ref_mock.set = AsyncMock()
    collection_mock = MagicMock()
    collection_mock.document.return_value = doc_ref_mock
    client_mock.collection.return_value = collection_mock
    
    # Test data
    test_id = str(uuid.uuid4())
    test_data = BatchRun(
        run_id=test_id,
        start_ts=datetime.utcnow(),
        status=BatchRunStatus.SUCCESS
    )
    
    # Call the method
    await dao.add_document("batch_run", test_id, test_data)
    
    # Verify
    client_mock.collection.assert_called_once_with("dev_batch_run")
    collection_mock.document.assert_called_once_with(test_id)
    assert doc_ref_mock.set.await_count == 1  # Use await_count for AsyncMock
    
    # Verify the data passed to set is a dict (not the dataclass)
    args, _ = doc_ref_mock.set.call_args
    assert isinstance(args[0], dict)
    assert "run_id" in args[0]
    assert "start_ts" in args[0]
    assert "status" in args[0]


@pytest.mark.asyncio
async def test_update_document(dao, mock_firestore):
    """Test updating a document in Firestore."""
    _, client_mock = mock_firestore
    
    # Mock the document reference
    doc_ref_mock = MagicMock()
    # Update is an async operation, so it needs to be an AsyncMock
    doc_ref_mock.update = AsyncMock()
    collection_mock = MagicMock()
    collection_mock.document.return_value = doc_ref_mock
    client_mock.collection.return_value = collection_mock
    
    # Test data
    test_id = str(uuid.uuid4())
    updates = {"status": BatchRunStatus.FAILED}
    
    # Call the method
    await dao.update_document("batch_run", test_id, updates)
    
    # Verify
    client_mock.collection.assert_called_once_with("dev_batch_run")
    collection_mock.document.assert_called_once_with(test_id)
    assert doc_ref_mock.update.await_count == 1
    doc_ref_mock.update.assert_awaited_with(updates)


@pytest.mark.asyncio
async def test_create_settlement(dao, mock_firestore):
    """Test creating a settlement with deterministic ID."""
    _, client_mock = mock_firestore
    
    # Mock the document reference
    doc_ref_mock = MagicMock()
    # Set is an async operation, so it needs to be an AsyncMock
    doc_ref_mock.set = AsyncMock()
    collection_mock = MagicMock()
    collection_mock.document.return_value = doc_ref_mock
    client_mock.collection.return_value = collection_mock
    
    # Test data
    payment_advice_uuid = str(uuid.uuid4())
    invoice_uuid = str(uuid.uuid4())
    settlement = Settlement(
        payment_advice_uuid=payment_advice_uuid,
        invoice_uuid=invoice_uuid,
        other_doc_uuid=None,
        settlement_date=date(2025, 6, 1),
        settlement_amount=1000.0,
        settlement_status="ready"
    )
    
    # Call the method
    await dao.create_settlement(settlement)
    
    # Verify
    client_mock.collection.assert_called_once_with("dev_settlement")
    
    # Verify the document ID format is correct
    args, _ = collection_mock.document.call_args
    assert args[0].startswith(f"{payment_advice_uuid}_{invoice_uuid}")
    
    # Verify the set call was made
    doc_ref_mock.set.assert_called_once()

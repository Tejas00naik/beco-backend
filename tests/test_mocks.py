"""
Unit tests for the mock components
"""

import pytest
import random
from unittest import mock
from datetime import datetime

from src.mocks.email_reader import MockEmailReader
from src.mocks.llm_extractor import MockLLMExtractor
from src.mocks.sap_caller import MockSapCaller


def test_mock_email_reader():
    """Test the MockEmailReader functionality."""
    # Initialize with explicit mock data path to avoid side effects
    import tempfile
    import os
    temp_dir = tempfile.mkdtemp()
    reader = MockEmailReader(data_path=temp_dir, max_emails=3)
    
    # Force generation of mock emails by overriding random number generation
    original_randint = random.randint
    try:
        # Mock randint to always return max value for num_emails
        random.randint = lambda a, b: b if a == 1 and b == 3 else original_randint(a, b)
        
        # Get unprocessed emails
        emails = reader.get_unprocessed_emails()
        
        # Verify we got some emails
        assert isinstance(emails, list)
        assert len(emails) > 0
    finally:
        # Restore original function
        random.randint = original_randint
    
    # Check the structure of each email
    for email in emails:
        assert "email_id" in email
        assert "sender_mail" in email
        assert "received_at" in email
        assert "subject" in email
        assert "content" in email
        
    # Mark some emails as processed
    if emails:
        email_ids = [emails[0]["email_id"]]
        reader.mark_as_processed(email_ids)
        
        # Get new emails (should exclude the one we marked)
        new_emails = reader.get_unprocessed_emails()
        processed_ids = [e["email_id"] for e in new_emails]
        
        # Ensure the marked email is not in the new list
        assert email_ids[0] not in processed_ids


def test_mock_llm_extractor():
    """Test the MockLLMExtractor functionality."""
    extractor = MockLLMExtractor()
    
    # Create mock email data with payment advices
    invoice = {
        "invoice_number": "INV-001", 
        "invoice_date": "2025-06-01", 
        "booking_amount": 1000.00,
        "customer_uuid": "customer-123"  # Added customer_uuid to invoice
    }
    
    other_doc = {
        "other_doc_number": "CN-001",
        "other_doc_date": "2025-06-01",
        "other_doc_type": "credit_note",
        "other_doc_amount": 500.00,
        "customer_uuid": "customer-123"  # Added customer_uuid to other_doc
    }
    
    payment_advice = {
        "payment_advice_number": "PA-12345",
        "payment_advice_date": "2025-06-21",
        "payment_advice_amount": 1000.00,
        "payer_name": "Test Payer", 
        "payee_name": "Test Payee",
        "legal_entity_uuid": "legal-entity-123",  # Added legal_entity_uuid to payment_advice
        "invoices": [invoice],
        "other_docs": [other_doc]
    }
    
    mock_email = {
        "email_id": "test-id",
        "sender_mail": "test@example.com",
        "received_at": datetime.now().isoformat(),
        "subject": "Payment Advice #12345",
        "body": "This is a payment advice for invoice #INV-001.",
        "group_uuid": "group-123",  # Added group_uuid to email
        "payment_advices": [payment_advice]
    }
    
    # Extract metadata
    metadata = extractor.extract_email_metadata(mock_email)
    assert isinstance(metadata, dict)
    assert "payer_name" in metadata
    assert "payee_name" in metadata
    assert "group_uuid" in metadata  # Check for group_uuid in metadata
    assert "email_subject" in metadata  # Check for email_subject in metadata
    assert metadata["payer_name"] == "Test Payer"
    assert metadata["group_uuid"] == "group-123"  # Verify group_uuid value
    
    # Extract payment advices
    payment_advices = extractor.extract_payment_advices(mock_email)
    assert isinstance(payment_advices, list)
    assert len(payment_advices) > 0
    assert payment_advices[0]["payment_advice_number"] == "PA-12345"
    
    # Check payment advice structure
    for pa in payment_advices:
        assert "payment_advice_number" in pa
        assert "payment_advice_date" in pa
        assert "payment_advice_amount" in pa
        assert "legal_entity_uuid" in pa  # Check for legal_entity_uuid in payment_advice
        assert pa["legal_entity_uuid"] == "legal-entity-123"  # Verify legal_entity_uuid value
    
    # Extract transaction details
    invoices, other_docs, settlements = extractor.extract_transaction_details(mock_email, 0)
    
    # Check invoices
    assert isinstance(invoices, list)
    assert len(invoices) > 0
    for inv in invoices:
        assert "invoice_number" in inv
        assert "invoice_date" in inv
        assert "booking_amount" in inv
        assert "customer_uuid" in inv  # Check for customer_uuid in invoice
        assert inv["customer_uuid"] == "customer-123"  # Verify customer_uuid value
    
    # Check other docs
    assert isinstance(other_docs, list)
    for doc in other_docs:
        if doc:  # Some might be empty
            assert "other_doc_number" in doc
            assert "other_doc_date" in doc
            assert "other_doc_type" in doc
            assert "customer_uuid" in doc  # Check for customer_uuid in other_doc
            assert doc["customer_uuid"] == "customer-123"  # Verify customer_uuid value
    
    # Check settlements
    assert isinstance(settlements, list)
    assert len(settlements) > 0
    for settlement in settlements:
        assert "settlement_date" in settlement
        assert "settlement_amount" in settlement
        assert "customer_uuid" in settlement  # Check for customer_uuid in settlement
        assert settlement["customer_uuid"] == "customer-123"  # Verify customer_uuid value


def test_mock_sap_caller():
    """Test the MockSapCaller functionality."""
    sap_caller = MockSapCaller(failure_rate=0.0)  # Ensure success for testing
    
    # Create test data
    payment_advice = {
        "payment_advice_uuid": "test-uuid",
        "payment_advice_number": "PA-12345",
        "payment_advice_date": "2025-06-21",
        "payment_advice_amount": 1000.00
    }
    
    settlement = {
        "settlement_uuid": "test-settlement-uuid",
        "settlement_date": "2025-06-21",
        "settlement_amount": 1000.00,
        "invoice_uuid": "test-invoice-uuid",
        "other_doc_uuid": None
    }
    
    # Test successful reconciliation
    success, response = sap_caller.reconcile_payment(payment_advice, settlement)
    assert success is True
    assert isinstance(response, dict)
    assert "success" in response
    assert response["success"] is True
    
    # Test with forced failure
    sap_caller = MockSapCaller(failure_rate=1.0)  # 100% failure
    success, response = sap_caller.reconcile_payment(payment_advice, settlement)
    assert success is False
    assert "error" in response

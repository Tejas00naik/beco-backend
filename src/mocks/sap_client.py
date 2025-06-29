"""Mock SAP client for development and testing."""

import logging
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class MockSapClient:
    """
    A mock SAP client that simulates the behavior of SAP Service Layer API.
    """
    
    def __init__(self):
        """
        Initialize the mock SAP client with sample data.
        """
        # Sample BP (Business Partner) accounts - represents customers in SAP
        self.bp_accounts = {
            "BP001": {"name": "Amazon Seller Services", "legal_entity": "amazon-services-123456"},
            "BP002": {"name": "Amazon Development Center", "legal_entity": "amazon-development-123456"},
            "BP003": {"name": "Flipkart India Private Limited", "legal_entity": "flipkart-india-123456"},
            "BP004": {"name": "Myntra Designs", "legal_entity": "myntra-designs-123456"},
            "BP005": {"name": "Clicktech Retail Private Limited", "legal_entity": "amazon-clicktech-retail-123456"}
        }
        
        # Generate mock transaction data
        self.transactions = self._generate_mock_transactions()
        logger.info(f"Initialized mock SAP client with {len(self.bp_accounts)} BP accounts and {len(self.transactions)} transactions")
    
    def _generate_mock_transactions(self) -> List[Dict[str, Any]]:
        """
        Generate mock SAP transaction data for invoices and other documents.
        
        Returns:
            List of mock transactions
        """
        transactions = []
        bp_codes = list(self.bp_accounts.keys())
        
        # Generate 50 invoice transactions
        for i in range(1, 51):
            invoice_num = f"INV-{1000 + i}"
            bp_code = random.choice(bp_codes)
            
            # Create invoice transaction with fixed ID for consistency
            transactions.append({
                "transaction_id": f"SAP-INV-{i:08d}",  # Fixed ID pattern
                "document_number": invoice_num,
                "document_type": "invoice",
                "bp_code": bp_code,
                "bp_name": self.bp_accounts[bp_code]["name"],
                "legal_entity": self.bp_accounts[bp_code]["legal_entity"],
                "posting_date": "2025-06-01",  # Fixed date for consistency
                "amount": 10000 + (i * 100)  # Deterministic amount
            })
        
        # Generate other document transactions (TDS/BDPOs) with fixed patterns
        doc_prefixes = ["TDS-CM", "BDPO", "CN"]
        for i in range(1, 51):
            # Deterministic prefix selection
            prefix_idx = i % len(doc_prefixes)
            prefix = doc_prefixes[prefix_idx]
            doc_num = f"{prefix}-{2000 + i}"
            
            # Deterministic BP code selection
            bp_idx = i % len(bp_codes)
            bp_code = bp_codes[bp_idx]
            
            # Create other document transaction with fixed IDs
            transactions.append({
                "transaction_id": f"SAP-DOC-{i:08d}",  # Fixed ID pattern
                "document_number": doc_num,
                "document_type": "other_doc",
                "bp_code": bp_code,
                "bp_name": self.bp_accounts[bp_code]["name"],
                "legal_entity": self.bp_accounts[bp_code]["legal_entity"],
                "posting_date": "2025-06-01",  # Fixed date for consistency
                "amount": 1000 + (i * 50)  # Deterministic amount
            })
            
        # Add special transactions with fixed IDs that match the MockLLMExtractor document numbers
        # Directly corresponds to the 5 payment advice variations in MockLLMExtractor
        # Also includes Amazon post-processing special documents (INV-9999, TDS-CM-9999, BDPO-9999)
        special_transactions = [
            # First payment advice (file_hash == 0)
            {
                "transaction_id": "SAP-INV-10000001",
                "document_number": "INV-1234",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 35000.00
            },
            {
                "transaction_id": "SAP-INV-10000002",
                "document_number": "INV-5678",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 42000.00
            },
            {
                "transaction_id": "SAP-DOC-10000001",
                "document_number": "BDPO-12345",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 3500.00
            },
            {
                "transaction_id": "SAP-DOC-10000002",
                "document_number": "TDS-CM-1234",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 4200.00
            },
            
            # Second payment advice (file_hash == 1)
            {
                "transaction_id": "SAP-INV-10000003",
                "document_number": "INV-0708",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 32000.00
            },
            {
                "transaction_id": "SAP-INV-10000004",
                "document_number": "INV-0707",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 28500.00
            },
            {
                "transaction_id": "SAP-DOC-10000003",
                "document_number": "BDPO-30707",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 3200.00
            },
            {
                "transaction_id": "SAP-DOC-10000004",
                "document_number": "TDS-CM-0707",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 2850.00
            },
            
            # Third payment advice (file_hash == 2)
            {
                "transaction_id": "SAP-INV-10000005",
                "document_number": "INV-0447",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 27500.00
            },
            {
                "transaction_id": "SAP-INV-10000006",
                "document_number": "INV-0446",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 31200.00
            },
            {
                "transaction_id": "SAP-DOC-10000005",
                "document_number": "BDPO-00446",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 3120.00
            },
            {
                "transaction_id": "SAP-DOC-10000006",
                "document_number": "TDS-CM-0446",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 2750.00
            },
            
            # Fourth payment advice (file_hash == 3)
            {
                "transaction_id": "SAP-INV-10000007",
                "document_number": "INV-7452",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 41500.00
            },
            {
                "transaction_id": "SAP-INV-10000008",
                "document_number": "INV-7453",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 38200.00
            },
            {
                "transaction_id": "SAP-DOC-10000007",
                "document_number": "BDPO-97452",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 4150.00
            },
            {
                "transaction_id": "SAP-DOC-10000008",
                "document_number": "TDS-CM-7452",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 3820.00
            },
            
            # Fifth payment advice (file_hash == 4)
            {
                "transaction_id": "SAP-INV-10000009",
                "document_number": "INV-8581",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 29500.00
            },
            {
                "transaction_id": "SAP-INV-10000010",
                "document_number": "INV-8580",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 33700.00
            },
            {
                "transaction_id": "SAP-DOC-10000009",
                "document_number": "BDPO-98580",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 3370.00
            },
            {
                "transaction_id": "SAP-DOC-10000010",
                "document_number": "TDS-CM-8580",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 2950.00
            },
            # Amazon post-processing special documents
            {
                "transaction_id": "SAP-INV-10000099",
                "document_number": "INV-9999",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 25000.00
            },
            {
                "transaction_id": "SAP-DOC-10000099",
                "document_number": "TDS-CM-9999",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 2500.00
            },
            {
                "transaction_id": "SAP-DOC-10000098",
                "document_number": "BDPO-9999",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": "2025-06-01",
                "amount": 1250.00
            }
        ]
        
        transactions.extend(special_transactions)
        return transactions
    
    async def search_transactions(self, doc_number: str, doc_type: Optional[str] = None, 
                               date_from: Optional[datetime] = None, date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Search for transactions in SAP with the given document number.
        
        Args:
            doc_number: Document number to search for
            doc_type: Optional document type filter (invoice or other_doc)
            date_from: Optional start date for transaction search
            date_to: Optional end date for transaction search
            
        Returns:
            List of matching transactions
        """
        # Filter by document number (exact match)
        results = [t for t in self.transactions if t["document_number"] == doc_number]
        
        # Additional filters if provided
        if doc_type:
            results = [t for t in results if t["document_type"] == doc_type]
            
        if date_from:
            date_from_str = date_from.strftime("%Y-%m-%d")
            results = [t for t in results if t["posting_date"] >= date_from_str]
            
        if date_to:
            date_to_str = date_to.strftime("%Y-%m-%d")
            results = [t for t in results if t["posting_date"] <= date_to_str]
        
        # Log the search results
        logger.debug(f"SAP search for {doc_number} returned {len(results)} results")
        return results
    
    async def get_customer_by_bp_code(self, bp_code: str) -> Optional[Dict[str, Any]]:
        """
        Get customer details by BP code.
        
        Args:
            bp_code: Business Partner code in SAP
            
        Returns:
            Customer details or None if not found
        """
        if bp_code in self.bp_accounts:
            customer = self.bp_accounts[bp_code].copy()
            customer["bp_code"] = bp_code
            return customer
        return None
    
    async def call_reconciliation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mock reconciliation API call to SAP.
        
        Args:
            payload: Reconciliation payload
            
        Returns:
            API response from SAP
        """
        # Log the reconciliation call
        logger.info(f"Mock SAP reconciliation call with payload: {payload}")
        
        # Usually always succeeds in mock
        success = True
        
        if success:
            return {
                "success": True,
                "transaction_id": "SAP-RECON-12345678",
                "message": "Reconciliation successful"
            }
        else:
            return {
                "success": False,
                "error": "Mock SAP error: Service temporarily unavailable"
            }

    async def reconcile_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """Simulate reconciling a transaction in SAP
        
        Args:
            transaction_id: ID of the transaction to reconcile
            
        Returns:
            Dict with success status and message
        """
        logger.info(f"Mock SAP reconciliation for transaction {transaction_id}")
        # Always succeed for consistent testing
        return {
            "success": True,
            "transaction_id": "SAP-RECON-12345678",
            "message": "Reconciliation successful"
        }

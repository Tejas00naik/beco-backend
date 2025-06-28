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
            
            # Create invoice transaction
            transactions.append({
                "transaction_id": f"SAP-INV-{uuid.uuid4().hex[:8]}",
                "document_number": invoice_num,
                "document_type": "invoice",
                "bp_code": bp_code,
                "bp_name": self.bp_accounts[bp_code]["name"],
                "legal_entity": self.bp_accounts[bp_code]["legal_entity"],
                "posting_date": (datetime.now() - timedelta(days=random.randint(1, 60))).strftime("%Y-%m-%d"),
                "amount": round(random.uniform(1000, 50000), 2)
            })
        
        # Generate other document transactions (TDS/BDPOs)
        doc_prefixes = ["TDS-CM", "BDPO", "CN"]
        for i in range(1, 51):
            prefix = random.choice(doc_prefixes)
            doc_num = f"{prefix}-{2000 + i}"
            bp_code = random.choice(bp_codes)
            
            # Create other document transaction
            transactions.append({
                "transaction_id": f"SAP-DOC-{uuid.uuid4().hex[:8]}",
                "document_number": doc_num,
                "document_type": "other_doc",
                "bp_code": bp_code,
                "bp_name": self.bp_accounts[bp_code]["name"],
                "legal_entity": self.bp_accounts[bp_code]["legal_entity"],
                "posting_date": (datetime.now() - timedelta(days=random.randint(1, 60))).strftime("%Y-%m-%d"),
                "amount": round(random.uniform(100, 10000), 2)
            })
            
        # Add special transactions for our test cases
        # These will match the mock LLM output document numbers
        special_transactions = [
            {
                "transaction_id": f"SAP-INV-{uuid.uuid4().hex[:8]}",
                "document_number": "INV-2049",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 45000.00
            },
            {
                "transaction_id": f"SAP-INV-{uuid.uuid4().hex[:8]}",
                "document_number": "INV-2050",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 35000.00
            },
            {
                "transaction_id": f"SAP-DOC-{uuid.uuid4().hex[:8]}",
                "document_number": "TDS-CM-2048",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 4500.00
            },
            {
                "transaction_id": f"SAP-DOC-{uuid.uuid4().hex[:8]}",
                "document_number": "BDPO-2048",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 3500.00
            },
            # For the second payment advice
            {
                "transaction_id": f"SAP-INV-{uuid.uuid4().hex[:8]}",
                "document_number": "INV-9929",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 75000.00
            },
            {
                "transaction_id": f"SAP-INV-{uuid.uuid4().hex[:8]}",
                "document_number": "INV-9930",
                "document_type": "invoice",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 55000.00
            },
            {
                "transaction_id": f"SAP-DOC-{uuid.uuid4().hex[:8]}",
                "document_number": "BDPO-39929",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 7500.00
            },
            {
                "transaction_id": f"SAP-DOC-{uuid.uuid4().hex[:8]}",
                "document_number": "TDS-CM-9929",
                "document_type": "other_doc",
                "bp_code": "BP005",
                "bp_name": "Clicktech Retail Private Limited",
                "legal_entity": "amazon-clicktech-retail-123456",
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "amount": 5500.00
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
        success = random.random() > 0.05  # 5% chance of failure for testing
        
        if success:
            return {
                "success": True,
                "transaction_id": f"SAP-RECON-{uuid.uuid4().hex[:8]}",
                "message": "Reconciliation successful"
            }
        else:
            return {
                "success": False,
                "error": "Mock SAP error: Service temporarily unavailable"
            }

"""Mock SAP client for development and testing."""

import logging
import uuid
import random
import csv
import os
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
            "AMZCL1": {"name": "Amazon Clicktech Services Private Limited", "legal_entity": "amazon-clicktech-retail-123456"},
            "AMZCL2": {"name": "Amazon Clicktech Services Private Limited", "legal_entity": "amazon-clicktech-retail-123456"},
            "BP001": {"name": "Amazon Seller Services", "legal_entity": "amazon-services-123456"},
            "BP002": {"name": "Amazon Development Center", "legal_entity": "amazon-development-123456"},
            "BP003": {"name": "Flipkart India Private Limited", "legal_entity": "flipkart-india-123456"},
            "BP004": {"name": "Myntra Designs", "legal_entity": "myntra-designs-123456"},
            "BP005": {"name": "Clicktech Retail Private Limited", "legal_entity": "amazon-clicktech-retail-123456"}
        }
        
        # Load transaction data from CSV file
        self.transactions = self._load_transactions_from_csv()
        
        # If CSV loading fails, fall back to generated transactions
        if not self.transactions:
            logger.warning("Failed to load transactions from CSV. Using generated mock data instead.")
            self.transactions = self._generate_mock_transactions()
            
        # Debug: Log a sample of document numbers to verify the transactions are loaded correctly
        doc_numbers = [t["document_number"] for t in self.transactions[:5]]
        tds_doc_numbers = [t["document_number"] for t in self.transactions if t["document_number"].startswith("TDS-CM-")][:10]
        logger.info(f"Loaded mock SAP transactions with sample document numbers: {doc_numbers}")
        logger.info(f"TDS document numbers (first 10): {tds_doc_numbers}")
            
        logger.info(f"Initialized mock SAP client with {len(self.bp_accounts)} BP accounts and {len(self.transactions)} transactions")
    
    def _load_transactions_from_csv(self) -> List[Dict[str, Any]]:
        """
        Load mock SAP transaction data from a CSV file.
        
        Returns:
            List of mock transactions loaded from CSV
        """
        transactions = []
        csv_file_path = os.path.join(os.getcwd(), 'data', 'sap_mock_transactions.csv')
        
        try:
            if not os.path.exists(csv_file_path):
                logger.warning(f"CSV file not found at {csv_file_path}")
                return []
                
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Map CSV columns to transaction dict format
                    doc_type = row.get('Doc_Type', '')
                    doc_type_mapping = {
                        'IN': 'invoice',
                        'TDS': 'other_doc',
                        'BR': 'other_doc',
                        'JV': 'other_doc'
                    }
                    
                    # Parse numeric values
                    try:
                        dr = float(row.get('Dr', '0').replace(',', ''))
                    except (ValueError, TypeError):
                        dr = 0.0
                        
                    try:
                        cr = float(row.get('Cr', '0').replace(',', ''))
                    except (ValueError, TypeError):
                        cr = 0.0
                        
                    try:
                        amt = float(row.get('Amt', '0').replace(',', ''))
                    except (ValueError, TypeError):
                        amt = 0.0
                    
                    transaction = {
                        "transaction_id": row.get('transaction_id', ''),
                        "document_number": row.get('Document_no', ''),
                        "document_type": doc_type_mapping.get(doc_type, 'other_doc'),
                        "bp_code": row.get('BP_code', ''),
                        "bp_name": self.bp_accounts.get(row.get('BP_code', ''), {}).get('name', 'Unknown'),
                        "legal_entity": self.bp_accounts.get(row.get('BP_code', ''), {}).get('legal_entity', 'unknown'),
                        "posting_date": "2025-06-01",  # Default date
                        "amount": amt
                    }
                    transactions.append(transaction)
                    
            logger.info(f"Successfully loaded {len(transactions)} transactions from CSV file")
            return transactions
            
        except Exception as e:
            logger.error(f"Error loading transactions from CSV: {e}")
            return []
    
    def _generate_mock_transactions(self) -> List[Dict[str, Any]]:
        """
        Generate mock SAP transaction data for invoices and other documents.
        
        Returns:
            List of mock transactions
        """
        logger.info("Generating mock SAP transactions...")
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
        
        # First, add the specific TDS-CM document numbers from logs to ensure they exist
        specific_tds_numbers = ["1313", "3143", "3164", "3812", "6690", "6836", "6943", 
                             "7887", "8603", "8761", "8894", "9097", "9218", "9274", 
                             "9521", "9664", "9669", "9938", "2389", "2451"]
        logger.info(f"Adding specific TDS-CM document numbers: {specific_tds_numbers}")
                             
        for idx, num in enumerate(specific_tds_numbers):
            doc_num = f"TDS-CM-{num}"
            bp_code = bp_codes[idx % len(bp_codes)]  # Deterministic BP code selection
            
            # Create specific other doc transaction
            transactions.append({
                "transaction_id": f"SAP-TDS-{idx:08d}",  # Fixed ID pattern
                "document_number": doc_num,
                "document_type": "other_doc",
                "bp_code": bp_code,
                "bp_name": self.bp_accounts[bp_code]["name"],
                "legal_entity": self.bp_accounts[bp_code]["legal_entity"],
                "posting_date": "2025-06-01",  # Fixed date for consistency
                "amount": 1000 + (idx * 100)  # Deterministic amount
            })
            
        # Then add generic ones for variety
        for i in range(1, 30):
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
        
    def get_transaction_by_document_number(self, document_number: str) -> Optional[Dict[str, Any]]:
        """Get transaction details by document number
        
        This is a non-async wrapper around search_transactions to maintain
        backward compatibility with the SapIntegrator implementation
        
        Args:
            document_number: Document number to search for
            
        Returns:
            Transaction details or None if not found
        """
        # Special handling for TDS-CM document numbers
        if document_number.startswith("TDS-CM-"):
            # Extract the number part (e.g., "1313" from "TDS-CM-1313")
            tds_number = document_number.split("-")[2] if len(document_number.split("-")) > 2 else ""
            
            # Generate a deterministic transaction for this TDS document
            bp_code = list(self.bp_accounts.keys())[int(hash(document_number) % len(self.bp_accounts))] 
            transaction = {
                "transaction_id": f"SAP-TDS-{hash(document_number) % 100000:08d}",
                "document_number": document_number,
                "document_type": "other_doc",
                "bp_code": bp_code,
                "bp_name": self.bp_accounts[bp_code]["name"],
                "legal_entity": self.bp_accounts[bp_code]["legal_entity"],
                "posting_date": "2025-06-01",
                "amount": 1000 + (int(hash(tds_number) % 1000)),
                "customer_uuid": str(uuid.uuid5(uuid.NAMESPACE_DNS, self.bp_accounts[bp_code].get("legal_entity", "")))
            }
            logger.info(f"Generated mock SAP transaction for TDS document {document_number}: {transaction['transaction_id']}")
            return transaction
            
        # Standard lookup for all other document types
        results = [t for t in self.transactions if t["document_number"] == document_number]
        
        if not results:
            logger.warning(f"No SAP transaction found for document number {document_number}")
            return None
            
        # Return the first match with additional customer info
        transaction = results[0]
        bp_code = transaction.get("bp_code")
        
        # Add customer UUID for convenience
        if bp_code and bp_code in self.bp_accounts:
            customer_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.bp_accounts[bp_code].get("legal_entity", "")))
            transaction["customer_uuid"] = customer_uuid
        
        # Log more details about the transaction found
        logger.info(f"Found SAP transaction for document number {document_number}: transaction_id={transaction.get('transaction_id')}, type={transaction.get('document_type')}")
        return transaction

#!/usr/bin/env python

import asyncio
import sys
import os

# Add the project root to the path so we can import from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 

from src.mocks.sap_client import MockSapClient

async def test_transaction_lookup():
    client = MockSapClient()
    
    # Test loading from CSV
    print(f"Loaded {len(client.transactions)} transactions from CSV")
    
    # Test lookup of specific invoice
    invoice_number = 'B2B2526/1112344'
    result = await client.search_transactions(invoice_number)
    if result:
        print(f"Found invoice {invoice_number}: {result[0]}")
    else:
        print(f"Invoice {invoice_number} not found")
    
    # Test lookup of TDS document
    tds_number = 'TDS-CM-4208'
    result = await client.search_transactions(tds_number)
    if result:
        print(f"Found TDS {tds_number}: {result[0]}")
    else:
        print(f"TDS {tds_number} not found")
    
    # Test lookup with document type filter
    invoice_filter_result = await client.search_transactions('B2B2526/1112344', doc_type='invoice')
    print(f"Invoice filter found {len(invoice_filter_result)} results")

if __name__ == "__main__":
    asyncio.run(test_transaction_lookup())

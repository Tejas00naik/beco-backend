"""
Mock SAP B1 Internal Reconciliation Endpoint Caller

This module simulates calling an SAP B1 API endpoint for payment reconciliation.
In a real implementation, this would make actual API calls to SAP B1,
but for the proof of concept, we simulate successful and occasional failed responses.
"""

import json
import random
import time
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class MockSapCaller:
    """
    Mock implementation of an SAP B1 API caller.
    
    Simulates the behavior of calling the SAP B1 Internal Reconciliation endpoint,
    including occasional failures to test error handling.
    """
    
    def __init__(self, failure_rate: float = 0.1, latency_ms: int = 200):
        """
        Initialize the mock SAP caller.
        
        Args:
            failure_rate: Probability of a request failing (0.0 to 1.0)
            latency_ms: Simulated API latency in milliseconds
        """
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        logger.info(f"Initialized MockSapCaller (failure_rate={failure_rate}, latency_ms={latency_ms})")
    
    def reconcile_payment(self, payment_advice_data: Dict[str, Any], 
                         settlement_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Call the mock SAP B1 Internal Reconciliation endpoint.
        
        Args:
            payment_advice_data: Payment advice data
            settlement_data: Settlement data
            
        Returns:
            Tuple of (success, response_data)
        """
        payment_advice_id = payment_advice_data.get("payment_advice_uuid", "unknown")
        settlement_id = settlement_data.get("settlement_uuid", "unknown")
        
        logger.info(f"Calling SAP B1 for payment advice {payment_advice_id}, settlement {settlement_id}")
        
        # Prepare the payload that would be sent to SAP
        payload = {
            "reconciliation": {
                "payment_advice": {
                    "id": payment_advice_data.get("payment_advice_uuid"),
                    "number": payment_advice_data.get("payment_advice_number"),
                    "date": payment_advice_data.get("payment_advice_date"),
                    "amount": payment_advice_data.get("payment_advice_amount")
                },
                "settlement": {
                    "id": settlement_data.get("settlement_uuid"),
                    "date": settlement_data.get("settlement_date"),
                    "amount": settlement_data.get("settlement_amount"),
                    "invoice_id": settlement_data.get("invoice_uuid"),
                    "other_doc_id": settlement_data.get("other_doc_uuid")
                }
            }
        }
        
        # Simulate API latency
        time.sleep(self.latency_ms / 1000.0)
        
        # Randomly determine success/failure based on failure rate
        if random.random() < self.failure_rate:
            # Simulate a failure response
            error_codes = ["SAP001", "SAP002", "SAP003", "SAP004"]
            error_messages = [
                "Invalid document number",
                "Reconciliation failed due to amount mismatch",
                "Business partner not found",
                "Internal SAP error"
            ]
            
            error_idx = random.randint(0, len(error_codes) - 1)
            error_code = error_codes[error_idx]
            error_message = error_messages[error_idx]
            
            response = {
                "success": False,
                "error": {
                    "code": error_code,
                    "message": error_message
                },
                "timestamp": datetime.utcnow().isoformat(),
                "request_id": f"REQ-{random.randint(10000, 99999)}"
            }
            
            logger.warning(f"SAP call failed for payment advice {payment_advice_id}: {error_code} - {error_message}")
            return False, response
        
        # Simulate a successful response
        sap_doc_num = f"SAP-{random.randint(100000, 999999)}"
        response = {
            "success": True,
            "data": {
                "sap_document_number": sap_doc_num,
                "sap_transaction_id": f"TXN-{random.randint(10000000, 99999999)}",
                "reconciliation_date": datetime.utcnow().isoformat(),
                "status": "RECONCILED"
            },
            "timestamp": datetime.utcnow().isoformat(),
            "request_id": f"REQ-{random.randint(10000, 99999)}"
        }
        
        logger.info(f"SAP call succeeded for payment advice {payment_advice_id}, created document {sap_doc_num}")
        return True, response

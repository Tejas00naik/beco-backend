"""SAP integration functionality for the batch worker."""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Import models
from models.schemas import PaymentAdvice, Settlement, Invoice, InvoiceStatus, SettlementStatus

logger = logging.getLogger(__name__)


class SapIntegrator:
    """
    Handles SAP integration operations for the batch worker.
    """
    
    def __init__(self, dao, sap_caller):
        """
        Initialize the SAP integrator.
        
        Args:
            dao: Firestore DAO instance
            sap_caller: SAP caller instance for API calls
        """
        self.dao = dao
        self.sap_caller = sap_caller
    
    async def call_sap_reconciliation(self, payment_advice: PaymentAdvice, settlement: Settlement) -> bool:
        """
        Call the SAP B1 reconciliation endpoint for a settlement.
        
        Args:
            payment_advice: Payment advice data
            settlement: Settlement data
            
        Returns:
            True if the call was successful, False otherwise
        """
        try:
            # Get invoice that this settlement is for
            invoice_docs = await self.dao.query_documents(
                "invoice", 
                [("invoice_number", "==", settlement.invoice_number)]
            )
            
            if not invoice_docs or len(invoice_docs) == 0:
                logger.error(f"No invoice found for settlement {settlement.settlement_uuid} with invoice number {settlement.invoice_number}")
                return False
                
            invoice_doc = invoice_docs[0]  # Get the first matching invoice
            invoice = Invoice(**invoice_doc)  # Convert dict to Invoice object
            
            # Log the reconciliation call
            logger.info(f"Calling SAP reconciliation for settlement {settlement.settlement_uuid}, invoice {invoice.invoice_number}")
            
            # Prepare SAP request payload
            sap_payload = {
                "invoice_number": invoice.invoice_number,
                "invoice_date": invoice.invoice_date.isoformat() if invoice.invoice_date else None,
                "payment_advice_number": payment_advice.payment_advice_number,
                "payment_advice_date": payment_advice.payment_advice_date.isoformat() if payment_advice.payment_advice_date else None,
                "settlement_amount": settlement.settlement_amount,
                "payer_name": payment_advice.payer_name
            }
            
            # Call SAP API
            sap_result = await self.sap_caller.call_reconciliation(sap_payload)
            
            if sap_result.get("success"):
                # Update invoice with SAP transaction ID
                sap_transaction_id = sap_result.get("transaction_id")
                
                # Update invoice status and SAP transaction ID
                await self.dao.update_document("invoice", invoice.invoice_uuid, {
                    "invoice_status": InvoiceStatus.RECONCILED,
                    "sap_transaction_id": sap_transaction_id,
                    "updated_at": datetime.utcnow()
                })
                
                # Update settlement status
                await self.dao.update_document("settlement", settlement.settlement_uuid, {
                    "settlement_status": SettlementStatus.RECONCILED,
                    "sap_transaction_id": sap_transaction_id,
                    "updated_at": datetime.utcnow()
                })
                
                logger.info(f"Successfully reconciled settlement {settlement.settlement_uuid} with SAP transaction ID {sap_transaction_id}")
                return True
            else:
                # Update settlement status to ERROR
                await self.dao.update_document("settlement", settlement.settlement_uuid, {
                    "settlement_status": SettlementStatus.ERROR,
                    "error_msg": sap_result.get("error", "Unknown SAP error"),
                    "updated_at": datetime.utcnow()
                })
                
                logger.error(f"SAP reconciliation failed for settlement {settlement.settlement_uuid}: {sap_result.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"Error calling SAP for settlement {settlement.settlement_uuid}: {str(e)}")
            
            try:
                # Update settlement status to ERROR
                await self.dao.update_document("settlement", settlement.settlement_uuid, {
                    "settlement_status": SettlementStatus.ERROR,
                    "error_msg": str(e),
                    "updated_at": datetime.utcnow()
                })
            except Exception as update_error:
                logger.error(f"Failed to update settlement status: {str(update_error)}")
                
            return False

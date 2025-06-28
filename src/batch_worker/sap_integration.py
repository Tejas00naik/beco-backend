"""SAP integration functionality for the batch worker."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

# Import models
from models.schemas import (
    PaymentAdvice, Settlement, Invoice, OtherDoc,
    InvoiceStatus, SettlementStatus, OtherDocType
)

logger = logging.getLogger(__name__)


class SapIntegrator:
    """
    Handles SAP integration operations for the batch worker.
    """
    
    def __init__(self, dao, sap_client):
        """
        Initialize the SAP integrator.
        
        Args:
            dao: Firestore DAO instance
            sap_client: SAP client instance for API calls
        """
        self.dao = dao
        self.sap_client = sap_client
    
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
                [("invoice_uuid", "==", settlement.invoice_uuid)]
            )
            
            if not invoice_docs or len(invoice_docs) == 0:
                logger.error(f"No invoice found for settlement {settlement.settlement_uuid} with invoice UUID {settlement.invoice_uuid}")
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
            sap_result = await self.sap_client.call_reconciliation(sap_payload)
            
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
            
    async def enrich_documents_with_sap_data(self, payment_advice_uuid: str) -> Tuple[int, int]:
        """
        Search SAP for transaction IDs and BP account info for all invoices and other docs
        related to a payment advice, and update them in Firestore.
        
        Args:
            payment_advice_uuid: Payment advice UUID to process
            
        Returns:
            Tuple of (num_successful_updates, num_failed_updates)
        """
        try:
            # Get the payment advice
            payment_advice_doc = await self.dao.get_document("payment_advice", payment_advice_uuid)
            if not payment_advice_doc:
                logger.error(f"Payment advice {payment_advice_uuid} not found")
                return (0, 0)
                
            payment_advice = PaymentAdvice(**payment_advice_doc)
            
            # Get payment advice date for limiting SAP search timeframe
            pa_date = payment_advice.payment_advice_date
            # Set search window to 3 months before payment advice date
            date_from = pa_date - timedelta(days=90) if pa_date else datetime.now() - timedelta(days=90)
            date_to = datetime.now()
            
            # Get all invoices for this payment advice
            invoice_docs = await self.dao.query_documents(
                "invoice", 
                [("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            
            # Get all other docs for this payment advice
            other_doc_docs = await self.dao.query_documents(
                "other_doc", 
                [("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            
            # Initialize counters
            successful_updates = 0
            failed_updates = 0
            
            # Process invoices
            for invoice_doc in invoice_docs:
                invoice = Invoice(**invoice_doc)
                
                # If already has SAP transaction ID, skip
                if invoice.sap_transaction_id:
                    logger.debug(f"Invoice {invoice.invoice_uuid} already has SAP transaction ID {invoice.sap_transaction_id}")
                    continue
                    
                # Search SAP for this invoice number
                sap_results = await self.sap_client.search_transactions(
                    doc_number=invoice.invoice_number,
                    doc_type="invoice",
                    date_from=date_from,
                    date_to=date_to
                )
                
                if not sap_results:
                    logger.warning(f"No SAP transaction found for invoice {invoice.invoice_number}")
                    failed_updates += 1
                    continue
                    
                # Use the first result (should be only one for exact invoice number match)
                sap_transaction = sap_results[0]
                sap_transaction_id = sap_transaction["transaction_id"]
                bp_code = sap_transaction["bp_code"]
                
                # Get customer UUID from customer table based on SAP BP code
                customer_docs = await self.dao.query_documents(
                    "customer", 
                    [("sap_customer_id", "==", bp_code)]
                )
                
                customer_uuid = None
                if customer_docs and len(customer_docs) > 0:
                    customer_uuid = customer_docs[0]["customer_uuid"]
                    
                # Update invoice with SAP transaction ID and customer UUID
                update_data = {
                    "sap_transaction_id": sap_transaction_id,
                    "updated_at": datetime.utcnow()
                }
                
                if customer_uuid:
                    update_data["customer_uuid"] = customer_uuid
                    
                await self.dao.update_document("invoice", invoice.invoice_uuid, update_data)
                logger.info(f"Updated invoice {invoice.invoice_uuid} with SAP transaction ID {sap_transaction_id} and customer {customer_uuid}")
                successful_updates += 1
            
            # Process other docs
            for other_doc_doc in other_doc_docs:
                other_doc = OtherDoc(**other_doc_doc)
                
                # If already has SAP transaction ID, skip
                if other_doc.sap_transaction_id:
                    logger.debug(f"Other doc {other_doc.other_doc_uuid} already has SAP transaction ID {other_doc.sap_transaction_id}")
                    continue
                    
                # Search SAP for this other doc number
                sap_results = await self.sap_client.search_transactions(
                    doc_number=other_doc.other_doc_number,
                    doc_type="other_doc",
                    date_from=date_from,
                    date_to=date_to
                )
                
                if not sap_results:
                    logger.warning(f"No SAP transaction found for other doc {other_doc.other_doc_number}")
                    failed_updates += 1
                    continue
                    
                # Use the first result
                sap_transaction = sap_results[0]
                sap_transaction_id = sap_transaction["transaction_id"]
                bp_code = sap_transaction["bp_code"]
                
                # Get customer UUID from customer table based on SAP BP code
                customer_docs = await self.dao.query_documents(
                    "customer", 
                    [("sap_customer_id", "==", bp_code)]
                )
                
                customer_uuid = None
                if customer_docs and len(customer_docs) > 0:
                    customer_uuid = customer_docs[0]["customer_uuid"]
                    
                # Update other doc with SAP transaction ID and customer UUID
                update_data = {
                    "sap_transaction_id": sap_transaction_id,
                    "updated_at": datetime.utcnow()
                }
                
                if customer_uuid:
                    update_data["customer_uuid"] = customer_uuid
                    
                await self.dao.update_document("other_doc", other_doc.other_doc_uuid, update_data)
                logger.info(f"Updated other doc {other_doc.other_doc_uuid} with SAP transaction ID {sap_transaction_id} and customer {customer_uuid}")
                successful_updates += 1
                
            # Update payment advice status with results
            if successful_updates > 0 and failed_updates == 0:
                logger.info(f"Successfully enriched all documents for payment advice {payment_advice_uuid}")
            elif successful_updates > 0:
                logger.warning(f"Partially enriched documents for payment advice {payment_advice_uuid}: {successful_updates} successful, {failed_updates} failed")
            else:
                logger.error(f"Failed to enrich any documents for payment advice {payment_advice_uuid}")
                
            return (successful_updates, failed_updates)
            
        except Exception as e:
            logger.error(f"Error enriching documents for payment advice {payment_advice_uuid}: {str(e)}")
            return (0, 0)
            
    async def enrich_settlement_customer_data(self, payment_advice_uuid: str) -> Tuple[int, int]:
        """
        Update settlement customer_uuid based on linked invoice or other doc.
        This should be called after enrich_documents_with_sap_data.
        
        Args:
            payment_advice_uuid: Payment advice UUID to process
            
        Returns:
            Tuple of (num_successful_updates, num_failed_updates)
        """
        try:
            # Get all settlements for this payment advice
            settlement_docs = await self.dao.query_documents(
                "settlement", 
                [("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            
            if not settlement_docs:
                logger.warning(f"No settlements found for payment advice {payment_advice_uuid}")
                return (0, 0)
                
            successful_updates = 0
            failed_updates = 0
            
            for settlement_doc in settlement_docs:
                settlement = Settlement(**settlement_doc)
                
                # If already has customer UUID, skip
                if settlement.customer_uuid:
                    logger.debug(f"Settlement {settlement.settlement_uuid} already has customer UUID {settlement.customer_uuid}")
                    continue
                    
                customer_uuid = None
                
                # Try to get customer UUID from invoice
                if settlement.invoice_uuid:
                    invoice_doc = await self.dao.get_document("invoice", settlement.invoice_uuid)
                    if invoice_doc and "customer_uuid" in invoice_doc and invoice_doc["customer_uuid"]:
                        customer_uuid = invoice_doc["customer_uuid"]
                        
                # If not found, try to get from other doc
                if not customer_uuid and settlement.other_doc_uuid:
                    other_doc_doc = await self.dao.get_document("other_doc", settlement.other_doc_uuid)
                    if other_doc_doc and "customer_uuid" in other_doc_doc and other_doc_doc["customer_uuid"]:
                        customer_uuid = other_doc_doc["customer_uuid"]
                
                # Update settlement with customer UUID if found
                if customer_uuid:
                    await self.dao.update_document("settlement", settlement.settlement_uuid, {
                        "customer_uuid": customer_uuid,
                        "updated_at": datetime.utcnow()
                    })
                    logger.info(f"Updated settlement {settlement.settlement_uuid} with customer UUID {customer_uuid}")
                    successful_updates += 1
                else:
                    logger.warning(f"Could not find customer UUID for settlement {settlement.settlement_uuid}")
                    failed_updates += 1
                    
            return (successful_updates, failed_updates)
            
        except Exception as e:
            logger.error(f"Error updating settlement customer data for payment advice {payment_advice_uuid}: {str(e)}")
            return (0, 0)

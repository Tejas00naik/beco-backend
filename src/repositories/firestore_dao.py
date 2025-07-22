"""
Firestore Data Access Object for handling all database operations.

This module provides a unified interface for interacting with Firestore
collections based on the defined schemas. It handles all write operations
for the transaction and processing metadata tables.
"""

import os
import logging
from typing import Dict, Any, List, Optional, TypeVar, Generic, Type, Union
from datetime import datetime, date
from google.cloud import firestore
from google.cloud import firestore_v1
from google.cloud.firestore_v1 import AsyncClient
from dataclasses import asdict, is_dataclass

from src.models.schemas import (
    # Master Data
    Group, LegalEntity, Customer, Email, Domain, CustEmailDomainMap,
    
    # Transaction Data
    EmailLog, PaymentAdvice, Invoice, OtherDoc, Settlement, PaymentAdviceLine,
    
    # Processing Metadata
    BatchRun, EmailProcessingLog, SapErrorDlq
)

# Type variable for generic methods
T = TypeVar('T')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FirestoreDAO:
    """Data Access Object for Firestore operations."""

    def __init__(self, project_id: str = None, collection_prefix: str = "", database_id: str = None):
        """
        Initialize the Firestore DAO.
        
        Args:
            project_id: Optional Firestore project ID (defaults to env variable)
            collection_prefix: Optional prefix for collections (for testing)
            database_id: Optional Firestore database ID (defaults to env variable or 'default')
        """
        self.project_id = project_id or os.environ.get("FIRESTORE_PROJECT_ID")
        if not self.project_id:
            raise ValueError("Firestore project ID not provided and FIRESTORE_PROJECT_ID env variable not set")
        
        self.database_id = database_id or os.environ.get("FIRESTORE_DATABASE_ID", "beco-payment-advice-dev")
        
        self.db = AsyncClient(project=self.project_id, database=self.database_id)
        self.collection_prefix = collection_prefix
        logger.info(f"Initialized FirestoreDAO with project {self.project_id}, database {self.database_id}, prefix: '{collection_prefix}'")

    
    def _get_collection_name(self, name: str) -> str:
        """Get the full collection name with prefix."""
        return f"{self.collection_prefix}{name}"

    def _convert_to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert a dataclass object to a dictionary for Firestore."""
        if is_dataclass(obj):
            data_dict = asdict(obj)
            
            # Convert datetime and date objects to Firestore-compatible formats
            for key, value in data_dict.items():
                if isinstance(value, datetime):
                    data_dict[key] = firestore_v1.SERVER_TIMESTAMP if value is None else value
                elif isinstance(value, date):
                    # Convert date objects to datetime at midnight
                    data_dict[key] = datetime.combine(value, datetime.min.time())
            
            return data_dict
        elif isinstance(obj, dict):
            # Also process dictionary values for nested dates
            result = {}
            for key, value in obj.items():
                if isinstance(value, date) and not isinstance(value, datetime):
                    result[key] = datetime.combine(value, datetime.min.time())
                else:
                    result[key] = value
            return result
        else:
            raise TypeError(f"Object of type {type(obj)} is not supported for Firestore conversion")

    async def add_document(self, collection: str, document_id: str, data: Union[Dict[str, Any], Any]) -> str:
        """
        Add a document to a collection with a specific ID.
        
        Args:
            collection: Collection name
            document_id: Document ID
            data: Document data (dict or dataclass)
            
        Returns:
            Document ID
        """
        try:
            collection_ref = self.db.collection(self._get_collection_name(collection))
            data_dict = self._convert_to_dict(data)
            
            doc_ref = collection_ref.document(document_id)
            await doc_ref.set(data_dict)
            logger.info(f"Added document {document_id} to {collection}")
            return document_id
            
        except Exception as e:
            logger.error(f"Error adding document to {collection}: {str(e)}")
            raise

    async def update_document(self, collection: str, document_id: str, data: Dict[str, Any]) -> None:
        """
        Update an existing document.
        
        Args:
            collection: Collection name
            document_id: Document ID
            data: Updated fields
        """
        try:
            doc_ref = self.db.collection(self._get_collection_name(collection)).document(document_id)
            data_dict = self._convert_to_dict(data)
            
            # Add updated_at timestamp
            if 'updated_at' not in data_dict:
                data_dict['updated_at'] = datetime.utcnow()
                
            await doc_ref.update(data_dict)
            logger.info(f"Updated document {document_id} in {collection}")
            
        except Exception as e:
            logger.error(f"Error updating document {document_id} in {collection}: {str(e)}")
            raise

    async def get_document(self, collection: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a document by ID.
        
        Args:
            collection: Collection name
            document_id: Document ID
            
        Returns:
            Document data or None if not found
        """
        try:
            doc_ref = self.db.collection(self._get_collection_name(collection)).document(document_id)
            doc = await doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            else:
                logger.warning(f"Document {document_id} not found in {collection}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting document {document_id} from {collection}: {str(e)}")
            raise

    async def query_documents(self, collection: str, filters: List[tuple] = None, 
                              order_by: str = None, limit: int = None, desc: bool = False) -> List[Dict[str, Any]]:
        """
        Query documents with filters.
        
        Args:
            collection: Collection name
            filters: List of (field, operator, value) tuples
            order_by: Field to order by
            limit: Maximum number of results
            
        Returns:
            List of document dictionaries
        """
        try:
            query = self.db.collection(self._get_collection_name(collection))
            
            if filters:
                for field, op, value in filters:
                    query = query.where(field, op, value)
            
            if order_by:
                # Apply descending order if specified
                if desc:
                    query = query.order_by(order_by, direction=firestore.Query.DESCENDING)
                else:
                    query = query.order_by(order_by)
                
            if limit:
                query = query.limit(limit)
                
            # Properly handle async iteration of Firestore's AsyncStreamGenerator
            results = []
            async for doc in query.stream():
                results.append(doc.to_dict())
                
            logger.info(f"Query returned {len(results)} results from {collection}")
            return results
            
        except Exception as e:
            logger.error(f"Error querying {collection}: {str(e)}")
            raise

    async def delete_document(self, collection: str, document_id: str) -> None:
        """
        Delete a document by ID.
        
        Args:
            collection: Collection name
            document_id: Document ID
        """
        try:
            doc_ref = self.db.collection(self._get_collection_name(collection)).document(document_id)
            doc_ref.delete()
            logger.info(f"Deleted document {document_id} from {collection}")
            
        except Exception as e:
            logger.error(f"Error deleting document {document_id} from {collection}: {str(e)}")
            raise

    # Transaction data specific methods
    async def create_email_log(self, email_log: EmailLog) -> str:
        """Create a new email log entry."""
        return await self.add_document("email_log", email_log.email_log_uuid, email_log)
        
    async def create_payment_advice(self, payment_advice: PaymentAdvice) -> str:
        """Create a new payment advice."""
        return await self.add_document("payment_advice", payment_advice.payment_advice_uuid, payment_advice)
        
    async def create_invoice(self, invoice: Invoice) -> str:
        """Create a new invoice record."""
        return await self.add_document("invoice", invoice.invoice_uuid, invoice)
        
    async def create_other_doc(self, other_doc: OtherDoc) -> str:
        """Create a new other document record."""
        return await self.add_document("other_doc", other_doc.other_doc_uuid, other_doc)
        
    async def create_settlement(self, settlement: Settlement) -> str:
        """
        Create a new settlement record with deterministic ID.
        
        As per schema notes, creates a composite ID to enforce uniqueness:
        {payment_advice_uuid}_{invoice_uuid ?? "_"}_{other_doc_uuid ?? "_"}
        """
        # Generate deterministic ID based on foreign keys
        invoice_part = settlement.invoice_uuid or "_"
        other_doc_part = settlement.other_doc_uuid or "_"
        deterministic_id = f"{settlement.payment_advice_uuid}_{invoice_part}_{other_doc_part}"
        
        # Set the settlement UUID to this deterministic ID
        settlement.settlement_uuid = deterministic_id
        
        return await self.add_document("settlement", deterministic_id, settlement)

    # Processing metadata methods
    async def create_batch_run(self, batch_run: BatchRun) -> str:
        """Create a new batch run record."""
        return await self.add_document("batch_run", batch_run.run_id, batch_run)
        
    async def update_batch_run(self, run_id: str, updates: Dict[str, Any]) -> None:
        """Update a batch run with new data."""
        await self.update_document("batch_run", run_id, updates)
        
    async def create_email_processing_log(self, email_processing_log: EmailProcessingLog) -> str:
        """
        Create a new email processing log entry.
        
        Uses a deterministic ID combining email_log_uuid and run_id.
        """
        doc_id = f"{email_processing_log.email_log_uuid}_{email_processing_log.run_id}"
        return await self.add_document("email_processing_log", doc_id, email_processing_log)
        
    async def create_sap_error_dlq(self, sap_error: SapErrorDlq) -> str:
        """Create a new SAP error dead letter queue entry."""
        return await self.add_document("sap_error_dlq", sap_error.dlq_id, sap_error)
        
    async def create_payment_advice_line(self, payment_advice_line: PaymentAdviceLine) -> str:
        """Create a new payment advice line entry."""
        return await self.add_document("paymentadvice_lines", payment_advice_line.payment_advice_line_uuid, payment_advice_line)
        
    async def clear_mailbox_data(self, mailbox_id: str) -> None:
        """Delete all data for a specific mailbox_id for full refresh mode.
        
        This deletes all documents in collections where mailbox_id matches 
        including: email_log, payment_advice, invoice, other_doc, settlement,
        batch_run, and email_processing_log.
        
        Args:
            mailbox_id: The mailbox ID to clear data for
        """
        try:
            logger.info(f"Starting full refresh: Deleting all data for mailbox_id {mailbox_id}")
            
            # First clear all settlements (they have foreign keys)
            payment_advice_uuids = []
            email_log_uuids = []
            
            # Find all email logs with this mailbox_id
            email_logs = await self.query_documents(
                "email_log", filters=[("mailbox_id", "==", mailbox_id)]
            )
            
            for email_log in email_logs:
                email_log_uuids.append(email_log["email_log_uuid"])
                
                # Find payment advices for this email
                advices = await self.query_documents(
                    "payment_advice", filters=[("email_log_uuid", "==", email_log["email_log_uuid"])]
                )
                
                for advice in advices:
                    payment_advice_uuids.append(advice["payment_advice_uuid"])
                    
                    # Delete settlements for this payment advice
                    settlements = await self.query_documents(
                        "settlement", filters=[("payment_advice_uuid", "==", advice["payment_advice_uuid"])]
                    )
                    
                    for settlement in settlements:
                        await self.delete_document("settlement", settlement["settlement_uuid"])
                        logger.info(f"Deleted settlement {settlement['settlement_uuid']}")
                    
                    # Delete invoices for this payment advice
                    invoices = await self.query_documents(
                        "invoice", filters=[("payment_advice_uuid", "==", advice["payment_advice_uuid"])]
                    )
                    
                    for invoice in invoices:
                        await self.delete_document("invoice", invoice["invoice_uuid"])
                        logger.info(f"Deleted invoice {invoice['invoice_uuid']}")
                    
                    # Delete other docs for this payment advice
                    other_docs = await self.query_documents(
                        "other_doc", filters=[("payment_advice_uuid", "==", advice["payment_advice_uuid"])]
                    )
                    
                    for other_doc in other_docs:
                        await self.delete_document("other_doc", other_doc["other_doc_uuid"])
                        logger.info(f"Deleted other_doc {other_doc['other_doc_uuid']}")
                    
                    # Delete the payment advice
                    await self.delete_document("payment_advice", advice["payment_advice_uuid"])
                    logger.info(f"Deleted payment_advice {advice['payment_advice_uuid']}")
            
            # Delete all email processing logs for this mailbox
            for email_log_uuid in email_log_uuids:
                # Find all batch runs with processing logs for this email
                processing_logs = await self.query_documents(
                    "email_processing_log", filters=[("email_log_uuid", "==", email_log_uuid)]
                )
                
                for log in processing_logs:
                    await self.delete_document("email_processing_log", f"{log['email_log_uuid']}_{log['run_id']}")
                    logger.info(f"Deleted email_processing_log for email {log['email_log_uuid']}")
            
            # Delete all email logs
            for email_log_uuid in email_log_uuids:
                await self.delete_document("email_log", email_log_uuid)
                logger.info(f"Deleted email_log {email_log_uuid}")
            
            # Delete all batch runs for this mailbox
            batch_runs = await self.query_documents(
                "batch_run", filters=[("mailbox_id", "==", mailbox_id)]
            )
            
            for batch_run in batch_runs:
                await self.delete_document("batch_run", batch_run["run_id"])
                logger.info(f"Deleted batch_run {batch_run['run_id']}")
                
            logger.info(f"Completed full refresh: All data deleted for mailbox_id {mailbox_id}")
            
        except Exception as e:
            logger.error(f"Error clearing data for mailbox_id {mailbox_id}: {str(e)}")
            raise
        
    async def get_customer_by_email(self, email_address: str) -> Optional[Dict[str, Any]]:
        """
        Look up customer by sender email address.
        
        Implements the SQL-like join logic from the schema documentation:
        SELECT c.* FROM emails e
        JOIN cust_email_domain_map m USING(email_uuid)
        JOIN customers c USING(customer_uuid)
        WHERE e.email_address = :sender;
        """
        try:
            # First find the email record
            email_docs = await self.query_documents(
                "emails", 
                filters=[("email_address", "==", email_address.lower()), ("is_active", "==", True)]
            )
            
            if not email_docs:
                logger.info(f"No active email found for {email_address}")
                return None
                
            email_doc = email_docs[0]
            email_uuid = email_doc.get("email_uuid")
            
            # Next, find the mapping
            map_docs = await self.query_documents(
                "cust_email_domain_map",
                filters=[("email_uuid", "==", email_uuid), ("deleted_at", "==", None)]
            )
            
            if not map_docs:
                logger.info(f"No customer mapping found for email {email_address}")
                return None
                
            map_doc = map_docs[0]
            customer_uuid = map_doc.get("customer_uuid")
            
            # Finally, get the customer
            customer = await self.get_document("customers", customer_uuid)
            return customer
            
        except Exception as e:
            logger.error(f"Error looking up customer by email {email_address}: {str(e)}")
            raise

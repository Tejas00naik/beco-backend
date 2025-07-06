"""Other document processing functionality.

This module contains the OtherDocProcessor class which handles creating
other document records in Firestore.
"""

import logging
import uuid
from typing import Dict, Any, List

# Import models
from models.schemas import OtherDoc, OtherDocType

# Import helpers
from .utils import parse_date, parse_amount, check_document_exists

logger = logging.getLogger(__name__)


class OtherDocProcessor:
    """
    Handles other document record creation and processing.
    """
    
    def __init__(self, dao):
        """
        Initialize the other document processor.
        
        Args:
            dao: Firestore DAO instance
        """
        self.dao = dao
    
    async def process_other_docs(self, other_doc_table: List[Dict[str, Any]], payment_advice_uuid: str, payment_advice_date) -> Dict[str, int]:
        """
        Process other document table from LLM output and create OtherDoc records.
        
        Args:
            other_doc_table: List of other document records from LLM output
            payment_advice_uuid: UUID of the payment advice
            payment_advice_date: Date of the payment advice
            
        Returns:
            Dictionary with counts of created and skipped documents
        """
        results = {"created": 0, "skipped": 0}
        
        if not other_doc_table:
            logger.info("No other doc table found in LLM output, skipping other doc processing")
            return results
        
        logger.info(f"Processing {len(other_doc_table)} other documents for payment advice {payment_advice_uuid}")
        
        for doc_data in other_doc_table:
            try:
                # Extract other document fields
                other_doc_number = doc_data.get('otherDocNumber')
                if not other_doc_number:
                    logger.warning("Skipping other doc with missing document number")
                    results["skipped"] += 1
                    continue
                
                # Check if document already exists for this payment advice
                existing_doc = await check_document_exists(
                    self.dao, "other_doc",
                    {"payment_advice_uuid": payment_advice_uuid, "other_doc_number": other_doc_number}
                )
                
                if existing_doc:
                    logger.info(f"Other doc {other_doc_number} already exists for payment advice {payment_advice_uuid}, skipping")
                    results["skipped"] += 1
                    continue
                
                # Process document fields
                doc_date = parse_date(doc_data.get('otherDocDate'))
                doc_type_str = doc_data.get('otherDocType', '')
                doc_amount = parse_amount(doc_data.get('otherDocAmount'))
                
                # Determine document type
                if 'tds' in doc_type_str.lower():
                    doc_type = OtherDocType.TDS
                elif 'credit' in doc_type_str.lower() or 'cm' in doc_type_str.lower():
                    doc_type = OtherDocType.CN
                elif 'debit' in doc_type_str.lower() or 'dm' in doc_type_str.lower():
                    doc_type = OtherDocType.DN
                else:
                    doc_type = OtherDocType.OTHER
                
                # Create OtherDoc record
                other_doc_uuid = str(uuid.uuid4())
                other_doc = OtherDoc(
                    other_doc_uuid=other_doc_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    customer_uuid=None,  # Will be populated by SAP integration
                    other_doc_number=other_doc_number,
                    other_doc_date=doc_date,
                    other_doc_type=doc_type,
                    other_doc_amount=doc_amount,
                    sap_transaction_id=None  # Will be populated by SAP integration
                )
                
                # Add OtherDoc to Firestore
                await self.dao.add_document("other_doc", other_doc_uuid, other_doc.__dict__)
                logger.info(f"Created other doc {other_doc_uuid} with number {other_doc_number} and type {doc_type}")
                results["created"] += 1
                
            except Exception as e:
                logger.warning(f"Failed to create other doc: {str(e)}")
                results["skipped"] += 1
        
        return results
    
    async def get_other_doc_uuid_by_number(self, payment_advice_uuid: str, other_doc_number: str) -> str:
        """
        Get the UUID of an other doc by its number and payment advice UUID.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            other_doc_number: Document number to look up
            
        Returns:
            UUID of the other doc, or None if not found
        """
        try:
            query_results = await self.dao.query_documents(
                "other_doc",
                [
                    ("payment_advice_uuid", "==", payment_advice_uuid),
                    ("other_doc_number", "==", other_doc_number)
                ]
            )
            
            if query_results and len(query_results) > 0:
                return query_results[0]["other_doc_uuid"]
            return None
            
        except Exception as e:
            logger.error(f"Error looking up other doc UUID: {str(e)}")
            return None

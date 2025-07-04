"""Utility functions for LLM integration."""

import logging
from typing import Dict, Any, List, Optional

from models.firestore_dao import FirestoreDAO
from src.llm_integration.extractor import LLMExtractor

logger = logging.getLogger(__name__)

async def convert_llm_output_to_processor_format(llm_output: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert LLM output format to the format expected by the PaymentProcessor.
    
    Args:
        llm_output: The raw output from the LLM extractor
        
    Returns:
        Reformatted output suitable for PaymentProcessor
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Ensure we have valid input
    if not llm_output or not isinstance(llm_output, dict):
        logger.warning(f"Invalid LLM output format: {llm_output}")
        return {}
        
    processed_output = {}
    
    # Meta table -> metaTable fields
    if "meta_table" in llm_output and llm_output["meta_table"] is not None and isinstance(llm_output["meta_table"], dict):
        meta = llm_output["meta_table"]
        # Extract and add to processed output with correct camelCase keys
        processed_output["paymentAdviceDate"] = meta.get("settlement_date")  # keep DD-MMM-YYYY format
        processed_output["paymentAdviceNumber"] = meta.get("payment_advice_number")
        processed_output["payersLegalName"] = meta.get("payer_legal_name")
        processed_output["payeesLegalName"] = meta.get("payee_legal_name")
    else:
        logger.warning("meta_table is missing or invalid in LLM output")
        # Add default values to ensure the processor doesn't crash
        processed_output["paymentAdviceDate"] = None
        processed_output["paymentAdviceNumber"] = None
        processed_output["payersLegalName"] = None
        processed_output["payeesLegalName"] = None
    
    # Invoice table -> invoiceTable
    if "invoice_table" in llm_output and isinstance(llm_output["invoice_table"], list):
        invoiceTable = []
        for invoice in llm_output["invoice_table"]:
            if not isinstance(invoice, dict):
                logger.warning(f"Invalid invoice format in LLM output: {invoice}")
                continue
                
            invoice_entry = {
                "invoiceNumber": invoice.get("invoice_number"),
                "invoiceDate": invoice.get("invoice_date"),
                "bookingAmount": invoice.get("booking_amount"),
                "totalSettlementAmount": invoice.get("total_invoice_settlement_amount")
            }
            invoiceTable.append(invoice_entry)
        processed_output["invoiceTable"] = invoiceTable
    else:
        logger.warning("invoice_table is missing or invalid in LLM output")
        processed_output["invoiceTable"] = []
    
    # settlement_table -> otherDocTable (1 row = 1 settlement document)
    if "settlement_table" in llm_output and isinstance(llm_output["settlement_table"], list):
        otherDocTable = []
        for settlement in llm_output["settlement_table"]:
            if not isinstance(settlement, dict):
                logger.warning(f"Invalid settlement format in LLM output: {settlement}")
                continue
                
            doc_type = settlement.get("settlement_doc_type")
            amount = settlement.get("settlement_amount")
            
            # Negate amount for cash-in credit doc types (BR, TDS, CN)
            if amount is not None and doc_type in ["BR", "TDS", "CN"]:
                try:
                    amount = -abs(float(amount)) if amount != '' else None  # Ensure it's negative for credits
                except (ValueError, TypeError):
                    logger.warning(f"Invalid settlement amount: {amount}")
                    amount = None
                
            other_doc_entry = {
                "otherDocType": doc_type,
                "otherDocNumber": settlement.get("settlement_doc_number"),
                "otherDocAmount": amount
            }
            otherDocTable.append(other_doc_entry)
        processed_output["otherDocTable"] = otherDocTable
    else:
        logger.warning("settlement_table is missing or invalid in LLM output")
        processed_output["otherDocTable"] = []
    
    # reconciliation_statement -> settlementTable (links between settlement doc and invoice)
    if "reconciliation_statement" in llm_output and isinstance(llm_output["reconciliation_statement"], list):
        settlementTable = []
        for recon in llm_output["reconciliation_statement"]:
            if not isinstance(recon, dict):
                logger.warning(f"Invalid reconciliation format in LLM output: {recon}")
                continue
                
            # Skip entries without invoice_number - they're not valid links
            if not recon.get("invoice_number"):
                continue
                
            doc_type = recon.get("settlement_doc_type")
            amount = recon.get("settlement_amount")
            
            # Negate amount for cash-in credit doc types (BR, TDS, CN)
            if amount is not None and doc_type in ["BR", "TDS", "CN"]:
                try:
                    amount = -abs(float(amount)) if amount != '' else None  # Ensure it's negative for credits
                except (ValueError, TypeError):
                    logger.warning(f"Invalid settlement amount: {amount}")
                    amount = None
                
            settlement_entry = {
                # Doc type is not used in settlementTable as per mapping guide
                "settlementDocNumber": recon.get("settlement_doc_number"),
                "invoiceNumber": recon.get("invoice_number"),
                "settlementAmount": amount
            }
            settlementTable.append(settlement_entry)
        processed_output["settlementTable"] = settlementTable
    else:
        logger.warning("reconciliation_statement is missing or invalid in LLM output")
        processed_output["settlementTable"] = []
    
    return processed_output

async def process_attachment_with_llm(attachment_text: str, dao: FirestoreDAO) -> Dict[str, Any]:
    """
    Process an attachment text with the LLM and convert to the PaymentProcessor format.
    
    Args:
        attachment_text: The text extracted from the attachment
        dao: FirestoreDAO instance for group/legal entity lookups
        
    Returns:
        Processed payment advice data in the PaymentProcessor format
    """
    try:
        # Initialize LLM extractor
        extractor = LLMExtractor(dao=dao)
        
        # First pass: Process with default prompt to get basic structure
        llm_output = await extractor.process_document(attachment_text)
        
        # Detect group based on payer name in output
        group_uuid = await extractor.detect_group_from_output(llm_output)
        
        # Second pass with group-specific prompt if a group was detected
        if group_uuid:
            logger.info(f"Running second pass with group-specific prompt for group_uuid={group_uuid}")
            llm_output = await extractor.process_document(attachment_text, group_uuid=group_uuid)
        
        # Convert LLM output to PaymentProcessor format
        processor_format = await convert_llm_output_to_processor_format(llm_output)
        
        # Add detected legal entity/group information
        legal_entity_uuid = await extractor.detect_legal_entity_from_output(llm_output)
        processor_format["legal_entity_uuid"] = legal_entity_uuid
        processor_format["group_uuid"] = group_uuid
        
        return processor_format
        
    except Exception as e:
        logger.error(f"Error processing attachment with LLM: {str(e)}")
        raise

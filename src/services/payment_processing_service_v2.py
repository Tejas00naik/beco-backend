"""
Payment Processing Service V2 for Zepto-specific payment advice handling.
This service only processes payment advice lines and does not touch legacy invoice/settlement/other doc tables.
"""

import json
import logging
import uuid
import traceback
from typing import Dict, Any, List, Optional

from src.models.schemas import PaymentAdvice
from src.models.schemas import PaymentAdviceLine
from src.repositories.payment_advice_repository import PaymentAdviceRepository
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class PaymentProcessingServiceV2:
    """
    Service for processing payment advices from LLM outputs in BatchWorkerV2/Zepto flows.
    Only processes payment advice lines, not legacy invoice/settlement/other doc tables.
    """
    
    def __init__(self, payment_advice_repo: PaymentAdviceRepository, dao: Optional[FirestoreDAO] = None):
        """
        Initialize the PaymentProcessingServiceV2.
        
        Args:
            payment_advice_repo: Repository for payment advice operations
            dao: Firestore DAO for direct database operations
        """
        self.payment_advice_repo = payment_advice_repo
        self.dao = dao
        logger.info("Initialized PaymentProcessingServiceV2 for Zepto-specific processing")
    
    async def create_payment_advice(self, email_log_uuid: str, llm_output: Dict[str, Any], legal_entity_uuid: Optional[str] = None, group_uuids: Optional[List[str]] = None) -> str:
        """
        Create a payment advice from LLM output, only processing payment advice lines.
        
        Args:
            email_log_uuid: UUID of the email log entry
            llm_output: Output from the LLM processor
            legal_entity_uuid: Optional UUID of the legal entity associated with this payment advice
            group_uuids: Optional list of group UUIDs associated with this payment advice
            
        Returns:
            UUID of the created payment advice
        """
        logger.info(f"Processing payment advice for email_log_uuid: {email_log_uuid}")
        
        # Extract meta information for the payment advice
        meta_table = {}
        if "meta_table" in llm_output:
            meta_table = llm_output["meta_table"]
        elif "metaTable" in llm_output:
            meta_table = llm_output["metaTable"]
        elif "Meta Table" in llm_output:
            meta_table = llm_output["Meta Table"]
        
        # Extract payment advice info from meta table
        payment_advice_number = meta_table.get("payment_advice_number") or meta_table.get("Payment Advice Number")
        payment_advice_date = meta_table.get("payment_advice_date") or meta_table.get("Settlement Date")
        payer_name = meta_table.get("payer_legal_name") or meta_table.get("Payer's Name")
        payee_name = meta_table.get("payee_legal_name") or meta_table.get("Payee's Legal Name")
        
        logger.info(f"META EXTRACTION: payment_advice_number={payment_advice_number}, payment_advice_date={payment_advice_date}, payer_name={payer_name}, payee_name={payee_name}")
        
        # Generate a UUID for the payment advice if not provided
        payment_advice_uuid = llm_output.get("payment_advice_uuid")
        if not payment_advice_uuid:
            payment_advice_uuid = str(uuid.uuid4())
            llm_output["payment_advice_uuid"] = payment_advice_uuid
            logger.info(f"Generated payment_advice_uuid: {payment_advice_uuid}")
        
        # Calculate payment advice amount from payment advice lines if available
        payment_advice_amount = 0
        if "paymentadvice_lines" in llm_output and llm_output["paymentadvice_lines"]:
            # For Zepto, sum up all the amount values in paymentadvice_lines
            try:
                for line in llm_output["paymentadvice_lines"]:
                    amount = float(line.get("amount", 0))
                    payment_advice_amount += amount
            except Exception as e:
                logger.error(f"Error calculating payment advice amount: {str(e)}")
        
        # Create the payment advice object
        payment_advice = PaymentAdvice(
            payment_advice_uuid=payment_advice_uuid,
            email_log_uuid=email_log_uuid,
            legal_entity_uuid=legal_entity_uuid,  # Add the legal entity UUID parameter
            payment_advice_number=payment_advice_number,
            payment_advice_date=payment_advice_date,
            payment_advice_amount=payment_advice_amount,
            payer_name=payer_name,
            payee_name=payee_name
        )
        
        # Store group UUIDs in the EmailLog if provided
        if group_uuids and self.dao:
            try:
                # Fetch existing email log
                email_log_data = await self.dao.get_document("email_log", email_log_uuid)
                if email_log_data:
                    # Update group_uuids field
                    email_log_data["group_uuids"] = group_uuids
                    await self.dao.update_document("email_log", email_log_uuid, {"group_uuids": group_uuids})
                    logger.info(f"Updated email log {email_log_uuid} with group UUIDs: {group_uuids}")
            except Exception as e:
                logger.error(f"Error updating email log with group UUIDs: {str(e)}")
                # Continue processing even if this fails
        
        logger.info(f"PAYMENT ADVICE OBJECT: payment_advice_number={payment_advice.payment_advice_number}, payment_advice_date={payment_advice.payment_advice_date}, payment_advice_amount={payment_advice.payment_advice_amount}, payer_name={payment_advice.payer_name}, payee_name={payment_advice.payee_name}")
        
        # Save the payment advice to the repository
        await self.payment_advice_repo.create(payment_advice)
        logger.info(f"Created payment advice {payment_advice_uuid} for email log {email_log_uuid}")
        
        # Log full LLM output for debugging
        logger.info(f"FULL LLM OUTPUT: {json.dumps(llm_output, default=str)}")
        logger.info(f"LLM OUTPUT KEYS: {list(llm_output.keys())}")
        
        # Process payment advice lines if available
        if "paymentadvice_lines" in llm_output and llm_output["paymentadvice_lines"]:
            payment_advice_lines = llm_output["paymentadvice_lines"]
            logger.info(f"Found {len(payment_advice_lines)} payment advice lines to process")
            
            # Save payment advice lines to Firestore
            await self.save_payment_advice_lines(payment_advice_lines, payment_advice_uuid)
        else:
            logger.warning(f"No paymentadvice_lines found in LLM output for Zepto flow")
        
        return payment_advice_uuid
    
    async def save_payment_advice_lines(self, payment_advice_lines: List[Dict], payment_advice_uuid: str) -> int:
        """
        Save payment advice lines to Firestore.
        
        Args:
            payment_advice_lines: List of payment advice line dictionaries
            payment_advice_uuid: UUID of the parent payment advice
            
        Returns:
            Number of successfully saved payment advice lines
        """
        if not self.dao:
            logger.error("DAO not initialized - cannot save payment advice lines")
            return 0
        
        logger.info(f"Saving {len(payment_advice_lines)} payment advice lines for payment advice {payment_advice_uuid}")
        saved_count = 0
        
        for line in payment_advice_lines:
            try:
                # Create a unique UUID for this line
                line_uuid = str(uuid.uuid4())
                
                # Create PaymentAdviceLine object
                payment_advice_line = PaymentAdviceLine(
                    payment_advice_line_uuid=line_uuid,
                    payment_advice_uuid=payment_advice_uuid,
                    bp_code=line.get("bp_code"),
                    gl_code=line.get("gl_code"),
                    account_type=line.get("account_type"),
                    customer=line.get("customer"),
                    doc_type=line.get("doc_type"),
                    doc_number=line.get("doc_number"),
                    ref_invoice_no=line.get("ref_invoice_no"),
                    ref_1=line.get("ref_1"),
                    ref_2=line.get("ref_2"),
                    ref_3=line.get("ref_3"),
                    amount=line.get("amount"),
                    dr_cr=line.get("dr_cr"),
                    dr_amt=line.get("dr_amt"),
                    cr_amt=line.get("cr_amt"),
                    branch_name=line.get("branch_name") or "Maharashtra"  # Default to Maharashtra if not set
                )
                
                # Save to Firestore
                await self.dao.create_payment_advice_line(payment_advice_line)
                saved_count += 1
                logger.info(f"Saved payment advice line {line_uuid} to Firestore")
                
            except Exception as line_error:
                logger.error(f"Error saving payment advice line to Firestore: {str(line_error)}")
                logger.error(traceback.format_exc())
        
        logger.info(f"Successfully saved {saved_count} out of {len(payment_advice_lines)} payment advice lines to Firestore")
        return saved_count

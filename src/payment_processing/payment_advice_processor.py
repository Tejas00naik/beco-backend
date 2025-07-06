"""Payment advice processing functionality.

This module contains the PaymentAdviceProcessor class which handles creating
and updating payment advice records in Firestore.
"""

import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# Import models
from models.schemas import PaymentAdvice, PaymentAdviceStatus

# Import helpers
from .utils import parse_date, parse_amount

logger = logging.getLogger(__name__)


class PaymentAdviceProcessor:
    """
    Handles payment advice record creation and updates.
    """
    
    def __init__(self, dao, legal_entity_lookup):
        """
        Initialize the payment advice processor.
        
        Args:
            dao: Firestore DAO instance
            legal_entity_lookup: Legal entity lookup service
        """
        self.dao = dao
        self.legal_entity_lookup = legal_entity_lookup
    
    async def create_payment_advice(self, llm_output: Dict[str, Any], email_log_uuid: str, payment_advice_uuid: str) -> Dict[str, Any]:
        """
        TRACING: This method is called from src/payment_processing/payment_processor.py line 61
        """
        print(f"\n\n==== PAYMENT ADVICE PROCESSOR REACHED ====\nProcessing payment advice {payment_advice_uuid}\n\n")
        """
        Create a payment advice record from LLM output.
        
        Args:
            llm_output: The LLM extracted data
            email_log_uuid: UUID of the parent email log
            payment_advice_uuid: UUID to use for the payment advice
            
        Returns:
            The created payment advice record
        """
        try:
            # Get meta_table from LLM output - check both 'metaTable' and 'meta_table' keys
            meta_table = llm_output.get('metaTable') or llm_output.get('meta_table', {})
            
            # For debugging, log the full LLM output structure using print to ensure visibility
            print(f"\n[DEBUG] LLM output keys: {list(llm_output.keys())}")
            if 'metaTable' in llm_output:
                print(f"[DEBUG] metaTable exists with keys: {list(llm_output.get('metaTable', {}).keys())}")
                print(f"[DEBUG] metaTable contents: {llm_output.get('metaTable')}")
            if 'meta_table' in llm_output:
                print(f"[DEBUG] meta_table exists with keys: {list(llm_output.get('meta_table', {}).keys())}")
                print(f"[DEBUG] meta_table contents: {llm_output.get('meta_table')}")
                
            # Extract payer and payee names from LLM output - check multiple possible field names
            # First check top-level fields for backward compatibility
            payer_name = llm_output.get('payersLegalName') or llm_output.get('payer_name')
            payee_name = llm_output.get('payeesLegalName') or llm_output.get('payee_name')
            
            # If not found in top level, check meta_table
            if not payer_name:
                payer_name = meta_table.get('payersLegalName') or meta_table.get('payer_legal_name') or meta_table.get('payer_name')
            if not payee_name:
                payee_name = meta_table.get('payeesLegalName') or meta_table.get('payee_legal_name') or meta_table.get('payee_name')
            
            print(f"\n[META FIELD EXTRACTION] payer_name: '{payer_name}'")
            print(f"[META FIELD EXTRACTION] payee_name: '{payee_name}'")
            
            logger.info(f"Extracted payer name from LLM: '{payer_name}'")
            
            # Extract other payment advice fields - first try top level for backward compatibility
            payment_advice_number = llm_output.get('paymentAdviceNumber') or meta_table.get('paymentAdviceNumber')
            
            # Get date from top level or meta_table
            payment_advice_date_str = llm_output.get('paymentAdviceDate') or meta_table.get('paymentAdviceDate')
            payment_advice_date = parse_date(payment_advice_date_str)
            
            # Get amount from top level or meta_table
            payment_advice_amount_raw = llm_output.get('paymentAdviceAmount') or meta_table.get('paymentAdviceAmount')
            payment_advice_amount = parse_amount(payment_advice_amount_raw)
            
            print(f"[META FIELD EXTRACTION] payment_advice_number: '{payment_advice_number}'")
            print(f"[META FIELD EXTRACTION] payment_advice_date: '{payment_advice_date}'")
            print(f"[META FIELD EXTRACTION] payment_advice_amount: '{payment_advice_amount}'")
            
            # Log meta field values for debugging
            logger.info(f"[META_FIELDS] Extracted meta fields from LLM output:")
            logger.info(f"[META_FIELDS] payer_name: '{payer_name}'")
            logger.info(f"[META_FIELDS] payee_name: '{payee_name}'")
            logger.info(f"[META_FIELDS] payment_advice_number: '{payment_advice_number}'")
            logger.info(f"[META_FIELDS] payment_advice_date: '{payment_advice_date}'")
            logger.info(f"[META_FIELDS] payment_advice_amount: '{payment_advice_amount}'")
            
            # Check if the legal_entity_uuid and group_uuid are already in the llm_output
            # This would be the case if the two-step process was used in the EmailProcessor
            legal_entity_uuid = llm_output.get('legal_entity_uuid')
            group_uuid = llm_output.get('group_uuid')
            
            logger.info(f"[ENTITY_DETECTION] Using legal_entity_uuid={legal_entity_uuid}, group_uuid={group_uuid} from two-step detection")
            
            # If we don't have the legal entity from the two-step process, fall back to direct lookup
            if not legal_entity_uuid and payer_name:
                logger.info(f"[ENTITY_DETECTION] Fallback: lookup by payer name '{payer_name}'")
                try:
                    legal_entity_uuid = await self.legal_entity_lookup.lookup_legal_entity_uuid(payer_name)
                    logger.info(f"[ENTITY_DETECTION] Looked up legal entity UUID for payer '{payer_name}': {legal_entity_uuid}")
                    
                    if legal_entity_uuid and not group_uuid:
                        # We should already have group_uuid from the two-step process or the fallback lookup
                        # But just in case, ensure we have it if legal_entity_uuid is available
                        try:
                            legal_entity = await self.dao.get_document("legal_entity", legal_entity_uuid)
                            if legal_entity:
                                group_uuid = legal_entity.get("group_uuid")
                                logger.info(f"[GROUP_UUID_DEBUG] Found group_uuid {group_uuid} for legal entity {legal_entity_uuid}")
                        except Exception as e:
                            logger.error(f"[GROUP_UUID_DEBUG] Error fetching legal entity {legal_entity_uuid}: {str(e)}")
                except ValueError as e:
                    logger.warning(f"[ENTITY_DETECTION] Legal entity lookup error for '{payer_name}': {str(e)}")
            
            if not legal_entity_uuid and not group_uuid:
                logger.warning(f"[ENTITY_DETECTION] Failed to detect legal entity and group")
            
            # Create payment advice object
            payment_advice = {
                "payment_advice_uuid": payment_advice_uuid,
                "email_log_uuid": email_log_uuid,
                "legal_entity_uuid": legal_entity_uuid,
                "sap_transaction_id": None,  # Will be set during SAP enrichment
                "payment_advice_status": PaymentAdviceStatus.NEW,
                "created_at": datetime.now(timezone.utc),
                "payer_name": payer_name,
                "payee_name": payee_name,
                "payment_advice_date": payment_advice_date,
                "payment_advice_number": payment_advice_number,
                "payment_advice_amount": payment_advice_amount
            }
            
            # Log the complete payment advice record for debugging
            logger.info(f"[PAYMENT_ADVICE] Creating payment advice with fields:")
            for key, value in payment_advice.items():
                logger.info(f"[PAYMENT_ADVICE]   {key}: {value}")
            
            # Add Payment Advice to Firestore
            await self.dao.add_document("payment_advice", payment_advice_uuid, payment_advice)
            logger.info(f"Created payment advice {payment_advice_uuid} with status {payment_advice['payment_advice_status']}")
            
            # Log meta fields again after storage for confirmation
            logger.info(f"[META_FIELDS] Stored meta fields in payment advice {payment_advice_uuid}:")
            logger.info(f"[META_FIELDS] payer_name: '{payment_advice['payer_name']}'")
            logger.info(f"[META_FIELDS] payee_name: '{payment_advice['payee_name']}'")
            return payment_advice
            
        except Exception as e:
            logger.error(f"Failed to create payment advice: {str(e)}")
            return None
    
    async def parse_date(self, date_str):
        """Parse a date string into a datetime object."""
        logger.info(f"[DATE_PARSE] Attempting to parse date string: '{date_str}'")
        if not date_str:
            logger.info(f"[DATE_PARSE] Date string is empty, returning None")
            return None
    
    async def update_payment_advice_status(
        self,
        payment_advice_uuid: str,
        settlements_created: int,
        settlement_errors: int,
        total_settlements: int
    ) -> None:
        """
        Update payment advice status based on settlement processing results.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            settlements_created: Number of settlements successfully created
            settlement_errors: Number of settlements that failed to be created
            total_settlements: Total number of settlements that were attempted
        """
        try:
            if settlements_created > 0 and settlement_errors == 0 and total_settlements == settlements_created:
                # Only set to FETCHED if all settlements were processed successfully (no errors)
                await self.dao.update_document("payment_advice", payment_advice_uuid, {
                    "payment_advice_status": PaymentAdviceStatus.FETCHED
                })
                logger.info(f"Updated payment advice {payment_advice_uuid} status to FETCHED after processing all {settlements_created} settlements successfully")
            elif settlements_created > 0 and settlement_errors > 0:
                # If some settlements were created but others failed, mark as PARTIAL_FETCHED
                await self.dao.update_document("payment_advice", payment_advice_uuid, {
                    "payment_advice_status": PaymentAdviceStatus.PARTIAL_FETCHED
                })
                logger.warning(f"Updated payment advice {payment_advice_uuid} status to PARTIAL_FETCHED with {settlements_created} successful and {settlement_errors} failed settlements")
            elif settlement_errors > 0:
                # If all settlements failed (none created), mark as ERROR
                await self.dao.update_document("payment_advice", payment_advice_uuid, {
                    "payment_advice_status": PaymentAdviceStatus.ERROR
                })
                logger.warning(f"Updated payment advice {payment_advice_uuid} status to ERROR due to {settlement_errors} settlement errors with no successful settlements")
        except Exception as e:
            logger.error(f"Failed to update payment advice status: {str(e)}")

"""Service for enriching payment advice lines with account information."""

import logging
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict

from src.models.account import Account
from src.repositories.account_repository import AccountRepository
from src.repositories.firestore_dao import FirestoreDAO
from src.config import TDS_ACCOUNT_CODE

logger = logging.getLogger(__name__)

class AccountEnrichmentService:
    """Service for enriching payment advice lines with account information."""
    
    def __init__(self, dao: FirestoreDAO):
        """Initialize with Firestore DAO."""
        self.dao = dao
        self.account_repo = AccountRepository(dao)
    
    async def get_payment_advice_lines(self, payment_advice_uuid: str) -> List[Dict[str, Any]]:
        """
        Get all payment advice lines for a payment advice.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            List of payment advice line objects
        """
        try:
            # Query payment advice lines with the given payment advice UUID
            lines = await self.dao.query_documents(
                "paymentadvice_lines", 
                [("payment_advice_uuid", "==", payment_advice_uuid)]
            )
            
            logger.info(f"Found {len(lines)} payment advice lines for {payment_advice_uuid}")
            return lines
        except Exception as e:
            logger.error(f"Error getting payment advice lines for {payment_advice_uuid}: {str(e)}")
            return []

    async def get_payment_advice(self, payment_advice_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Get a payment advice by UUID.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            Payment advice object if found, None otherwise
        """
        try:
            # Get payment advice
            payment_advice = await self.dao.get_document("payment_advice", payment_advice_uuid)
            if not payment_advice:
                logger.error(f"Payment advice {payment_advice_uuid} not found")
                return None
                
            return payment_advice
        except Exception as e:
            logger.error(f"Error getting payment advice {payment_advice_uuid}: {str(e)}")
            return None
    
    async def categorize_lines(self, lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Categorize payment advice lines into BP and GL types.
        
        Args:
            lines: List of payment advice lines
            
        Returns:
            Tuple of (bp_lines, gl_lines)
        """
        bp_lines = []
        gl_lines = []
        
        for line in lines:
            account_type = line.get("account_type", "").upper()
            doc_type = line.get("doc_type", "").upper()
            
            # TDS entries are always GL type
            if doc_type == "TDS":
                line["account_type"] = "GL"
                gl_lines.append(line)
            elif account_type == "GL":
                gl_lines.append(line)
            else:
                # Default to BP type if not specified or not GL
                line["account_type"] = "BP"
                bp_lines.append(line)
                
        logger.info(f"Categorized lines: {len(bp_lines)} BP lines, {len(gl_lines)} GL lines")
        return bp_lines, gl_lines
    
    async def enrich_bp_lines(self, bp_lines: List[Dict[str, Any]], legal_entity_uuid: str) -> List[Dict[str, Any]]:
        """
        Enrich BP lines with BP code from account linked to legal entity.
        
        Args:
            bp_lines: List of payment advice lines with account_type = BP
            legal_entity_uuid: UUID of the legal entity
            
        Returns:
            List of enriched BP lines
        """
        if not bp_lines:
            logger.warning("No BP lines to enrich")
            return []
        
        logger.info(f"Starting BP line enrichment for legal entity {legal_entity_uuid}, found {len(bp_lines)} BP lines")
            
        # Check if legal entity exists
        legal_entity = await self.dao.get_document("legal_entity", legal_entity_uuid)
        if not legal_entity:
            logger.error(f"Legal entity not found with UUID: {legal_entity_uuid}")
        else:
            logger.info(f"Legal entity exists: {legal_entity.get('name')}")
        
        # Get the BP account for the legal entity (one query for all BP lines)
        logger.info(f"Looking up BP account for legal entity {legal_entity_uuid}")
        
        # First check for accounts directly
        accounts = await self.dao.query_documents(
            "account", 
            [
                ("legal_entity_uuid", "==", legal_entity_uuid),
                ("account_type", "==", "BP")
            ]
        )
        if accounts:
            logger.info(f"Direct query found {len(accounts)} BP accounts for legal entity {legal_entity_uuid}")
            for acct in accounts:
                logger.info(f"Account: {acct.get('account_uuid')}, SAP ID: {acct.get('sap_account_id')}")
        else:
            logger.warning(f"No accounts found directly for legal entity {legal_entity_uuid}")
        
        # Now try through the repository
        bp_account = await self.account_repo.get_bp_account_by_legal_entity(legal_entity_uuid)
        
        if not bp_account or not bp_account.sap_account_id:
            logger.warning(f"No BP account or SAP ID found for legal entity {legal_entity_uuid} via repository")
            return bp_lines
            
        bp_code = bp_account.sap_account_id
        logger.info(f"Found BP code {bp_code} for legal entity {legal_entity_uuid}")
        
        # Enrich all BP lines with the same BP code
        enriched_lines = []
        for line in bp_lines:
            line["bp_code"] = bp_code
            logger.info(f"Enriched line {line.get('payment_advice_line_uuid')} with BP code {bp_code}")
            enriched_lines.append(line)
            
        logger.info(f"Successfully enriched {len(enriched_lines)} BP lines with BP code {bp_code}")
        return enriched_lines
    
    async def enrich_gl_lines(self, gl_lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich GL lines with GL code (TDS account for TDS entries).
        
        Args:
            gl_lines: List of payment advice lines with account_type = GL
            
        Returns:
            List of enriched GL lines
        """
        if not gl_lines:
            return []
            
        # Group GL lines by doc_type
        doc_type_lines = defaultdict(list)
        for line in gl_lines:
            doc_type = line.get("doc_type", "").upper()
            doc_type_lines[doc_type].append(line)
            
        # Process TDS lines specifically
        tds_lines = doc_type_lines.get("TDS", [])
        if tds_lines:
            # Get TDS account (one query for all TDS lines)
            tds_account = await self.account_repo.get_tds_account()
            
            # Get TDS GL code - either from account or from config
            tds_gl_code = None
            if tds_account and tds_account.sap_account_id:
                tds_gl_code = tds_account.sap_account_id
                logger.info(f"Found TDS GL code {tds_gl_code} from TDS account")
            else:
                # Fallback to config if TDS account not found in database
                tds_gl_code = TDS_ACCOUNT_CODE
                logger.info(f"Using default TDS GL code {tds_gl_code} from config")
            
            # Enrich all TDS lines with the same GL code
            for line in tds_lines:
                line["gl_code"] = tds_gl_code
            
        # Return all GL lines (TDS and non-TDS)
        return gl_lines
            
    async def enrich_payment_advice_lines(self, payment_advice_uuid: str) -> bool:
        """
        Enrich payment advice lines with account information.
        
        Args:
            payment_advice_uuid: UUID of the payment advice
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get payment advice
            payment_advice = await self.get_payment_advice(payment_advice_uuid)
            if not payment_advice:
                return False
                
            # Get legal entity UUID
            legal_entity_uuid = payment_advice.get("legal_entity_uuid")
            if not legal_entity_uuid:
                logger.warning(f"Payment advice {payment_advice_uuid} has no legal entity UUID")
                return False
                
            # Get payment advice lines
            lines = await self.get_payment_advice_lines(payment_advice_uuid)
            if not lines:
                logger.warning(f"No payment advice lines found for {payment_advice_uuid}")
                return False
                
            # Categorize lines into BP and GL types
            bp_lines, gl_lines = await self.categorize_lines(lines)
            
            # Enrich BP lines with BP code
            enriched_bp_lines = await self.enrich_bp_lines(bp_lines, legal_entity_uuid)
            
            # Enrich GL lines with GL code
            enriched_gl_lines = await self.enrich_gl_lines(gl_lines)
            
            # Combine all enriched lines
            all_enriched_lines = enriched_bp_lines + enriched_gl_lines
            
            # Update each line in Firestore
            update_count = 0
            for line in all_enriched_lines:
                try:
                    line_uuid = line.get("payment_advice_line_uuid")
                    if not line_uuid:
                        logger.warning(f"Line has no UUID: {line}")
                        continue
                        
                    # Update specific fields only
                    updates = {
                        "account_type": line.get("account_type"),
                        "updated_at": line.get("updated_at"),
                        "sap_enrichment_status": "enriched"
                    }
                    
                    # Add BP code if present
                    if "bp_code" in line:
                        updates["bp_code"] = line["bp_code"]
                        logger.info(f"Adding BP code {line['bp_code']} to line {line_uuid}")
                    else:
                        logger.warning(f"No BP code found in line {line_uuid} (account type: {line.get('account_type')})")
                        
                    # Add GL code if present
                    if "gl_code" in line:
                        updates["gl_code"] = line["gl_code"]
                        
                    # Update line in Firestore
                    logger.info(f"Updating line {line_uuid} in Firestore with: {updates}")
                    await self.dao.update_document("paymentadvice_lines", line_uuid, updates)
                    logger.info(f"Successfully updated line {line_uuid} in Firestore")
                    update_count += 1
                    
                except Exception as e:
                    logger.error(f"Error updating line: {str(e)}")
                    
            logger.info(f"Successfully updated {update_count} out of {len(all_enriched_lines)} payment advice lines")
            return update_count > 0
            
        except Exception as e:
            logger.error(f"Error enriching payment advice lines for {payment_advice_uuid}: {str(e)}")
            return False

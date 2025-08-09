"""
Monitoring Service

This module provides functionality to update the monitoring dashboard
by joining email_log and payment_advice data and updating the Google Sheet.
"""

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.external_apis.gcp.sheets_service import SheetsService
from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class MonitoringService:
    """Service to update the monitoring dashboard with processed payment advices."""
    
    def __init__(self, dao: FirestoreDAO = None, sheets_service: SheetsService = None):
        """
        Initialize the monitoring service.
        
        Args:
            dao: Firestore Data Access Object
            sheets_service: Google Sheets service instance
        """
        self.dao = dao or FirestoreDAO()
        self.sheets_service = sheets_service or SheetsService()
        
    async def setup_monitoring_sheet(self):
        """Set up the monitoring sheet with headers and formatting."""
        return self.sheets_service.setup_monitoring_sheet()
        
    async def get_monitoring_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get joined data from email_log and payment_advice tables.
        
        Args:
            limit: Maximum number of records to retrieve
            
        Returns:
            List of dictionaries with joined data
        """
        try:
            # Get recent email logs
            email_logs = await self.dao.get_email_logs(limit=limit)
            
            if not email_logs:
                logger.info("No email logs found")
                return []
            
            # Get payment advices for each email log
            monitoring_entries = []
            
            for email_log in email_logs:
                email_log_uuid = email_log.get("email_log_uuid")
                
                # Get payment advices for this email log
                payment_advices = await self.dao.get_payment_advices_by_email_log(email_log_uuid)
                
                if not payment_advices:
                    # No payment advices for this email log yet
                    continue
                
                # Join data from email log and payment advices
                for payment_advice in payment_advices:
                    entry = {
                        "email_subject": email_log.get("subject", ""),
                        "sender": email_log.get("sender_mail", ""),
                        "received_at": email_log.get("received_at"),
                        "legal_entity_name": payment_advice.get("legal_entity_name", ""),
                        "payment_advice_uuid": payment_advice.get("payment_advice_uuid", ""),
                        "reference_numbers": ", ".join(payment_advice.get("reference_numbers", [])),
                        "amount": payment_advice.get("amount", 0),
                        "sap_export_status": payment_advice.get("sap_export_status", "Pending"),
                        "sap_export_url": payment_advice.get("sap_export_url", ""),
                        "processed_at": payment_advice.get("created_at"),
                        "payment_advice_date": payment_advice.get("payment_advice_date", "")
                    }
                    monitoring_entries.append(entry)
            
            logger.info(f"Retrieved {len(monitoring_entries)} monitoring entries")
            return monitoring_entries
            
        except Exception as e:
            logger.error(f"Error retrieving monitoring data: {str(e)}")
            return []
    
    async def update_monitoring_sheet(self, limit: int = 100) -> bool:
        """
        Update the monitoring sheet with latest data.
        
        Args:
            limit: Maximum number of records to retrieve
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            # Get the monitoring data
            entries = await self.get_monitoring_data(limit=limit)
            
            if not entries:
                logger.info("No entries to update in monitoring sheet")
                return False
            
            # Update the sheet
            success = self.sheets_service.add_monitoring_entries(entries)
            
            if success:
                logger.info(f"Successfully updated monitoring sheet with {len(entries)} entries")
            else:
                logger.error("Failed to update monitoring sheet")
                
            return success
            
        except Exception as e:
            logger.error(f"Error updating monitoring sheet: {str(e)}")
            return False
    
    async def update_after_batch_processing(self, email_log_uuid: str) -> bool:
        """
        Update monitoring sheet after batch processing is complete for an email.
        
        This method is meant to be called from the batch worker after processing
        is complete for an email.
        
        Args:
            email_log_uuid: UUID of the processed email log
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            # Get the email log
            email_log = await self.dao.get_email_log(email_log_uuid)
            
            logger.info(f"DEBUG - Retrieved email log: {json.dumps(email_log, default=str)}")
            
            if not email_log:
                logger.error(f"Email log {email_log_uuid} not found")
                return False
            
            # Get payment advices for this email log
            payment_advices = await self.dao.get_payment_advices_by_email_log(email_log_uuid)
            
            logger.info(f"DEBUG - Retrieved payment advices: {json.dumps(payment_advices, default=str)}")
            
            # Join data from email log and payment advices
            entries = []
            
            if not payment_advices:
                # Even if there are no payment advices, check if the sheet is empty
                # and initialize it with headers if needed
                logger.warning(f"No payment advices found for email log {email_log_uuid}")
                
                # Check if the sheet is empty and needs headers
                result = self.sheets_service.get_monitoring_entries()
                
                if not result:  # Sheet is empty or hasn't been initialized with headers
                    logger.info("Sheet appears empty or uninitialized, adding headers")
                    self.sheets_service.setup_monitoring_sheet()
                    logger.info("Successfully initialized monitoring sheet with headers")
                
                return True  # Return success even though we're not adding data rows
            for payment_advice in payment_advices:
                # Use email_subject instead of subject
                email_subject = email_log.get("email_subject", "") 
                
                # Map Firestore fields to monitoring sheet columns
                entry = {
                    "email_subject": email_subject,
                    "sender": email_log.get("sender_mail", ""),
                    "received_at": email_log.get("received_at"),
                    "legal_entity_name": payment_advice.get("legal_entity_name", payment_advice.get("payer_name", "")),
                    "payment_advice_uuid": payment_advice.get("payment_advice_uuid", ""),
                    "reference_numbers": payment_advice.get("payment_advice_number", ""),
                    "amount": payment_advice.get("payment_advice_amount", 0),
                    "payment_advice_date": payment_advice.get("payment_advice_date", ""),
                    "sap_export_status": payment_advice.get("payment_advice_status", "Pending"),
                    "sap_export_url": payment_advice.get("sap_export_url", ""),
                    "processed_at": payment_advice.get("created_at")
                }
                entries.append(entry)
                logger.info(f"DEBUG - Created entry for sheet: {json.dumps(entry, default=str)}")
            
            # Update the sheet
            logger.info(f"Updating monitoring sheet with {len(entries)} entries for email log {email_log_uuid}")
            
            # Log detailed entry information before sheet update
            for i, entry in enumerate(entries):
                pa_id = entry.get("payment_advice_uuid", "unknown")
                subject = entry.get("email_subject", "unknown")
                logger.info(f"Sheet update preparation: Entry {i+1}/{len(entries)}: PA_ID={pa_id}, Subject={subject}")
            
            success = self.sheets_service.add_monitoring_entries(entries)
            
            if success:
                logger.info(f"Successfully updated monitoring sheet for email log {email_log_uuid} with {len(entries)} entries")
            else:
                logger.error(f"Failed to update monitoring sheet for email log {email_log_uuid}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error updating monitoring sheet after batch processing: {str(e)}")
            return False

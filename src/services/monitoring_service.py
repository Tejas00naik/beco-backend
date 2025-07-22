"""
Monitoring Service

This module provides functionality to update the monitoring dashboard
by joining email_log and payment_advice data and updating the Google Sheet.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.external_apis.gcp.sheets_service import SheetsService
from src.data_access.firestore_dao import FirestoreDAO

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
                        "processed_at": payment_advice.get("created_at")
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
            
            if not email_log:
                logger.error(f"Email log {email_log_uuid} not found")
                return False
            
            # Get payment advices for this email log
            payment_advices = await self.dao.get_payment_advices_by_email_log(email_log_uuid)
            
            if not payment_advices:
                logger.warning(f"No payment advices found for email log {email_log_uuid}")
                return False
            
            # Join data from email log and payment advices
            entries = []
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
                    "processed_at": payment_advice.get("created_at")
                }
                entries.append(entry)
            
            # Update the sheet
            success = self.sheets_service.add_monitoring_entries(entries)
            
            if success:
                logger.info(f"Successfully updated monitoring sheet for email log {email_log_uuid}")
            else:
                logger.error(f"Failed to update monitoring sheet for email log {email_log_uuid}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error updating monitoring sheet after batch processing: {str(e)}")
            return False

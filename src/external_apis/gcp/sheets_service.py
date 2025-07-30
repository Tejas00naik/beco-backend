"""
Google Sheets Service

This module provides functionality to interact with Google Sheets API
for the monitoring dashboard of processed payment advices.
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.config import (
    SHEETS_CREDENTIALS_PATH,
    MONITORING_SHEET_ID,
    MONITORING_SHEET_RANGE
)

logger = logging.getLogger(__name__)

# Define OAuth2 scopes needed for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class SheetsService:
    """Google Sheets API integration for monitoring dashboard."""
    
    def __init__(
        self,
        credentials_path: str = SHEETS_CREDENTIALS_PATH,
        sheet_id: str = MONITORING_SHEET_ID,
        sheet_range: str = MONITORING_SHEET_RANGE
    ):
        """
        Initialize the Google Sheets service.
        
        Args:
            credentials_path: Path to service account JSON file
            sheet_id: ID of the Google Sheet to use
            sheet_range: Range to use within the sheet (e.g., 'A:J')
        """
        self.credentials_path = credentials_path
        self.sheet_id = sheet_id
        self.sheet_range = sheet_range
        self.service = None
        
        # Authenticate and create the Sheets API service
        self._authenticate()
        
        logger.info(f"Initialized SheetsService for sheet_id: {sheet_id}")
        
    def _authenticate(self):
        """Authenticate with Google Sheets API using service account."""
        try:
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=SCOPES
            )
            
            # Create the Sheets API service
            self.service = build('sheets', 'v4', credentials=creds)
            logger.info("Successfully authenticated with Google Sheets API")
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Sheets API: {str(e)}")
            raise
    
    def setup_monitoring_sheet(self):
        """
        Set up the monitoring sheet with headers and formatting.
        """
        try:
            # Define headers
            headers = [
                "Date", "Email Subject", "Sender", 
                "Legal Entity", "Payment Advice ID",
                "Reference Numbers", "Total Amount", 
                "SAP Export Status", "SAP Export Link", "Processed At"
            ]
            
            # Format as a values array for Sheets API
            values = [headers]
            
            # Clear any existing data and add headers
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.sheet_id,
                range=self.sheet_range,
            ).execute()
            
            # Add headers
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=f"A1:J1",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
            # Apply formatting (headers bold, freeze top row)
            requests = [
                # Make headers bold
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": 0,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 10
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "bold": True
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.bold"
                    }
                },
                # Freeze the top row
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": 0,
                            "gridProperties": {
                                "frozenRowCount": 1
                            }
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                },
                # Auto-resize columns
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": 0,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 10
                        }
                    }
                }
            ]
            
            # Execute formatting requests
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": requests}
            ).execute()
            
            logger.info("Monitoring sheet headers and formatting set up successfully")
            return True
            
        except HttpError as error:
            logger.error(f"Error setting up monitoring sheet: {error}")
            return False
    
    def add_monitoring_entries(self, entries: List[Dict[str, Any]]) -> bool:
        """
        Add new entries to the monitoring sheet.
        
        Args:
            entries: List of dictionaries containing the monitoring data
                    Expected keys: email_subject, sender, received_at,
                    legal_entity_name, payment_advice_uuid, reference_numbers,
                    amount, sap_export_status, sap_export_url, processed_at
        """
        try:
            # Format entries for sheet
            values = []
            
            for entry in entries:
                # Format the date for better readability
                received_date = entry.get("received_at")
                if isinstance(received_date, datetime):
                    formatted_date = received_date.strftime("%Y-%m-%d %H:%M")
                else:
                    formatted_date = str(received_date)
                    
                processed_date = entry.get("processed_at")
                if isinstance(processed_date, datetime):
                    formatted_processed_date = processed_date.strftime("%Y-%m-%d %H:%M")
                else:
                    formatted_processed_date = str(processed_date)
                
                # Create a row with all required columns
                row = [
                    formatted_date,
                    entry.get("email_subject", ""),
                    entry.get("sender", ""),
                    entry.get("legal_entity_name", ""),
                    entry.get("payment_advice_uuid", ""),
                    entry.get("reference_numbers", ""),
                    entry.get("amount", ""),
                    entry.get("sap_export_status", "Pending"),
                    # Format URL as a hyperlink formula
                    f'=HYPERLINK("{entry.get("sap_export_url", "")}", "Download")' if entry.get("sap_export_url") else "",
                    formatted_processed_date
                ]
                values.append(row)
            
            # Determine if sheet is empty and needs headers
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range="A:A"
            ).execute()
            
            existing_rows = result.get("values", [])
            is_empty = not existing_rows
            next_row = len(existing_rows) + 1
            
            logger.info(f"DEBUG - Sheet status check: is_empty={is_empty}, existing_rows={len(existing_rows)}, next_row={next_row}")
            
            # If sheet is empty, add headers first
            if is_empty:
                logger.info("Sheet is empty, adding headers first")
                self.setup_monitoring_sheet()
                next_row = 2  # After headers
                logger.info("Headers added to empty sheet, next_row set to 2")
            
            # Append new values to sheet
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=f"A{next_row}",
                valueInputOption="USER_ENTERED",  # Use USER_ENTERED to interpret the formulas
                body={"values": values}
            ).execute()
            
            # Enhanced logging with detailed row information
            end_row = next_row + len(values) - 1
            row_range = f"A{next_row}:J{end_row}"
            sheet_url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/edit#gid=0"
            
            for i, entry in enumerate(entries):
                row_num = next_row + i
                pa_id = entry.get("payment_advice_uuid", "unknown")
                logger.info(f"Sheet update: Row {row_num} updated with payment advice {pa_id}")
            return True
            
        except HttpError as error:
            logger.error(f"Error adding entries to monitoring sheet: {error}")
            return False
    
    def get_monitoring_entries(self) -> List[Dict[str, Any]]:
        """
        Get all entries from the monitoring sheet.
        
        Returns:
            List of dictionaries containing the monitoring data
        """
        try:
            # Get all values from the sheet
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=self.sheet_range
            ).execute()
            
            values = result.get("values", [])
            
            if not values:
                logger.info("No data found in monitoring sheet")
                return []
            
            # Get headers from first row
            headers = values[0]
            
            # Convert rows to dictionaries
            entries = []
            for row in values[1:]:
                # Pad row with empty strings if it's shorter than headers
                padded_row = row + [""] * (len(headers) - len(row))
                entry = dict(zip(headers, padded_row))
                entries.append(entry)
            
            logger.info(f"Retrieved {len(entries)} entries from monitoring sheet")
            return entries
            
        except HttpError as error:
            logger.error(f"Error getting entries from monitoring sheet: {error}")
            return []

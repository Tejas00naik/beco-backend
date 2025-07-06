"""Common parsing utility functions.

This module contains helper functions for parsing dates, amounts, and other data.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a date string into a datetime object.
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        datetime object or None if parsing fails
    """
    if not date_str:
        return None
    
    # Try different date formats
    date_formats = [
        '%Y-%m-%d',     # 2023-01-30
        '%d-%m-%Y',     # 30-01-2023
        '%d/%m/%Y',     # 30/01/2023
        '%m/%d/%Y',     # 01/30/2023
        '%d-%b-%Y',     # 30-Jan-2023
        '%d %b %Y',     # 30 Jan 2023
        '%d %B %Y',     # 30 January 2023
        '%B %d, %Y',    # January 30, 2023
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    
    # If all formats fail
    logger.warning(f"Could not parse date string: {date_str}")
    return None


def parse_amount(amount_value: Optional[Union[str, int, float]]) -> Optional[float]:
    """
    Parse an amount value into a float.
    
    Args:
        amount_value: Amount as string, int, or float
        
    Returns:
        float value or None if parsing fails
    """
    if amount_value is None:
        return None
        
    # If already a number, return as float
    if isinstance(amount_value, (int, float)):
        return float(amount_value)
    
    # If it's a string, clean and convert
    if isinstance(amount_value, str):
        try:
            # Remove currency symbols and commas
            clean_amount = amount_value.replace(',', '')
            clean_amount = ''.join(c for c in clean_amount if c.isdigit() or c in '.-')
            
            # Convert to float
            return float(clean_amount) if clean_amount else None
            
        except (ValueError, AttributeError):
            logger.warning(f"Could not parse amount: {amount_value}")
            return None
    
    logger.warning(f"Unknown amount type: {type(amount_value)}, value: {amount_value}")
    return None

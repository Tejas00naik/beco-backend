"""Helper functions for the batch worker."""

import logging
from datetime import datetime, date
from typing import Optional, Dict, Any, Union

logger = logging.getLogger(__name__)


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """
    Parse a date string into a Python date object.
    
    Args:
        date_str: String representation of a date
        
    Returns:
        Date object if parsing succeeds, None otherwise
    """
    if not date_str:
        return None
        
    try:
        # Try to parse date string in common formats
        formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        # If we get here, none of the formats matched
        logger.warning(f"Failed to parse date '{date_str}' with standard formats")
        return None
    except Exception as e:
        logger.warning(f"Failed to parse date '{date_str}': {str(e)}")
        return None


def parse_amount(amount: Optional[Union[str, float, int]]) -> Optional[float]:
    """
    Parse a currency amount into a float. Handles both string and numeric inputs.
    
    Args:
        amount: String or numeric representation of an amount
        
    Returns:
        Float value if parsing succeeds, None otherwise
    """
    if amount is None:
        return None
        
    # If already a numeric type, return as float
    if isinstance(amount, (int, float)):
        return float(amount)
        
    # String handling
    if isinstance(amount, str):
        try:
            # Remove currency symbols, commas, and other non-numeric characters
            # except for decimal point and negative sign
            clean_str = ''.join(c for c in amount if c.isdigit() or c in '.-')
            return float(clean_str)
        except Exception as e:
            logger.warning(f"Failed to parse amount '{amount}': {str(e)}")
            return None
    
    # If we get here, the type is unsupported
    logger.warning(f"Unsupported type for amount: {type(amount)}, value: {amount}")
    return None


async def check_document_exists(dao, collection: str, field: str, value: str) -> bool:
    """
    Check if a document with the given field value already exists in the collection.
    
    Args:
        dao: Firestore DAO instance
        collection: Collection name to check in
        field: Field name to check
        value: Value to check for
        
    Returns:
        True if document exists, False otherwise
    """
    if not value:
        return False
        
    try:
        # Query Firestore for documents with the specified field value
        docs = await dao.query_documents(collection, [(field, "==", value)])
        return len(docs) > 0
    except Exception as e:
        logger.warning(f"Error checking if document exists: {str(e)}")
        return False

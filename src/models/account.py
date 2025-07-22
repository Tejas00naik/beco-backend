"""Account model for Firestore collections."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import uuid4

@dataclass
class Account:
    """
    Account model for BP (Business Partner) and GL (General Ledger) accounts.
    Replaces the Customer model with more semantic naming.
    """
    account_uuid: str = field(default_factory=lambda: str(uuid4()))
    account_name: str = ""
    account_type: str = "BP"  # BP or GL
    sap_account_id: Optional[str] = None  # BP code for BP accounts, GL code for GL accounts
    sap_account_name: Optional[str] = None
    state: Optional[str] = None
    payment_term_in_days: int = 0
    is_active: bool = True
    legal_entity_uuid: Optional[str] = None  # Only applicable for BP accounts
    is_tds_account: bool = False  # Flag for TDS accounts
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    deleted_at: Optional[datetime] = None

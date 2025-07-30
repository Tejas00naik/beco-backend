"""
Legal Entity Lookup Service - Compatibility Layer.

This module provides backward compatibility for the original LegalEntityLookupService
by delegating to the new modular implementation.
"""

import logging
import os
import asyncio
from typing import Optional, Dict, Any, List

from src.repositories.firestore_dao import FirestoreDAO
from src.repositories.legal_entity_repository import LegalEntityRepository
from src.external_apis.llm.legal_entity_client import LegalEntityLLMClient
from src.services.legal_entity_service import LegalEntityService

logger = logging.getLogger(__name__)

# Define a default group UUID
DEFAULT_GROUP_UUID = "00000000-0000-0000-0000-000000000000"


class LegalEntityLookupService:
    """
    Backward compatibility wrapper for legal entity lookup operations.
    
    This maintains the same public API as the original LegalEntityLookupService
    but delegates to the new modular LegalEntityService implementation.
    """
    
    def __init__(self, dao=None, project_id=None, collection_prefix=""):
        """
        Initialize the legal entity lookup service.
        
        Args:
            dao: Optional FirestoreDAO instance. If not provided, a new one will be created.
            project_id: Firestore project ID. Required if dao is not provided.
            collection_prefix: Collection prefix for Firestore. Default is empty.
        """
        # Create the new service that we'll delegate to
        repository = LegalEntityRepository(dao)
        llm_client = LegalEntityLLMClient()
        self.service = LegalEntityService(repository, llm_client)
        logger.info("Initialized LegalEntityLookupService (compatibility wrapper)")

    async def detect_legal_entity(self, email_body: Optional[str] = None, document_text: Optional[str] = None) -> Dict[str, Any]:
        """
        Use LLM to detect the legal entity from email body and document text,
        then look up its UUID and group UUID.
        
        Delegates to the new service implementation.
        
        Args:
            email_body: Optional email body text
            document_text: Optional document text (from attachment)
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid
        """
        return await self.service.detect_legal_entity(email_body, document_text)

"""
Legal Entity Lookup Service - Compatibility Layer.

This module provides backward compatibility for the original LegalEntityLookupService
by delegating to the new modular implementation.
"""

import logging
import os
import asyncio
from typing import Optional, Dict, Any, List

from models.firestore_dao import FirestoreDAO
from src.repositories.legal_entity_repository import LegalEntityRepository
from src.llm_integration.legal_entity_client import LegalEntityLLMClient
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
    
    async def lookup_legal_entity_uuid(self, payer_name: str) -> Optional[str]:
        """
        Look up the legal entity UUID for a given payer name.
        
        Delegates to the new service implementation.
        
        Args:
            payer_name: The name of the payer company
            
        Returns:
            UUID string for the legal entity
        """
        return await self.service.lookup_legal_entity_uuid(payer_name)
    
    async def lookup_from_llm_output(self, llm_output: Dict[str, Any]) -> Optional[str]:
        """
        Look up legal entity UUID from LLM output data.
        
        Delegates to the new service implementation.
        
        Args:
            llm_output: Dictionary containing LLM output, including metaTable with payersLegalName
            
        Returns:
            UUID string for the legal entity
        """
        return await self.service.lookup_from_llm_output(llm_output)
    
    async def get_legal_entity_with_group(self, legal_entity_uuid: str) -> Dict[str, Any]:
        """
        Get legal entity with its group UUID.
        
        Delegates to the new service implementation.
        
        Args:
            legal_entity_uuid: The UUID of the legal entity
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid
        """
        return await self.service.get_legal_entity_with_group(legal_entity_uuid)
    
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
        
    # For backward compatibility - synchronous version
    def lookup_legal_entity_uuid_sync(self, payer_name: str) -> Optional[str]:
        """
        Synchronous version of lookup_legal_entity_uuid for backward compatibility.
        
        This method is maintained for backward compatibility with non-async code.
        It uses asyncio.run to run the async method in a new event loop.
        This is not ideal, but it's necessary for compatibility.
        
        Args:
            payer_name: Name of the payer to look up
            
        Returns:
            UUID of the legal entity if found, None otherwise
        """
        logger.warning("lookup_legal_entity_uuid_sync is deprecated, use async version instead")
        
        # Create a new event loop to run the async method
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # Create a new event loop if there isn't one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        return loop.run_until_complete(self.lookup_legal_entity_uuid(payer_name))

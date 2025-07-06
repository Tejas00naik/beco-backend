"""Business logic for legal entity operations."""

import logging
from typing import Dict, Any, Optional, List

from src.repositories.legal_entity_repository import LegalEntityRepository
from src.external_apis.llm.legal_entity_client import LegalEntityLLMClient

logger = logging.getLogger(__name__)

# Define a default group UUID
DEFAULT_GROUP_UUID = "00000000-0000-0000-0000-000000000000"


class LegalEntityService:
    """
    Service layer for legal entity operations.
    
    This class provides business logic for legal entity operations,
    connecting the repository layer with the LLM client.
    """
    
    def __init__(self, repository: LegalEntityRepository, llm_client: LegalEntityLLMClient):
        """
        Initialize the legal entity service.
        
        Args:
            repository: Legal entity repository for database operations
            llm_client: LLM client for entity detection
        """
        self.repository = repository
        self.llm_client = llm_client
        logger.info("Initialized LegalEntityService")
    
    async def lookup_legal_entity_uuid(self, payer_name: str) -> Optional[str]:
        """
        Look up the legal entity UUID for a given payer name.
        
        Args:
            payer_name: The name of the payer company
            
        Returns:
            UUID string for the legal entity if found, None otherwise
        """
        if not payer_name:
            logger.warning("Empty payer name provided for legal entity lookup")
            return None
            
        # Look up the legal entity by name
        entity = await self.repository.get_legal_entity_by_name(payer_name)
        if entity:
            legal_entity_uuid = entity.get("legal_entity_uuid")
            logger.info(f"Found legal entity UUID for '{payer_name}': {legal_entity_uuid}")
            return legal_entity_uuid
            
        logger.warning(f"No legal entity found for payer name: {payer_name}")
        return None
    
    async def lookup_from_llm_output(self, llm_output: Dict[str, Any]) -> Optional[str]:
        """
        Look up legal entity UUID from LLM output data.
        
        Args:
            llm_output: Dictionary containing LLM output, including metaTable with payersLegalName
            
        Returns:
            UUID string for the legal entity if found, None otherwise
        """
        if not llm_output or not isinstance(llm_output, dict):
            logger.warning("Invalid LLM output provided")
            return None
            
        # Try to extract payer name from LLM output
        meta_table = llm_output.get("metaTable", {})
        payer_name = meta_table.get("payersLegalName")
        
        if not payer_name:
            logger.warning("No payer name found in LLM output")
            return None
            
        # Look up the legal entity by name
        return await self.lookup_legal_entity_uuid(payer_name)
    
    async def get_legal_entity_with_group(self, legal_entity_uuid: str) -> Dict[str, Any]:
        """
        Get legal entity with its group UUID.
        
        Args:
            legal_entity_uuid: The UUID of the legal entity
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid
        """
        # Load entities if not already loaded
        if not self.repository._entities_loaded:
            await self.repository.fetch_all_legal_entities()
            
        # Find the entity with matching UUID
        for entity in self.repository._cache.values():
            if entity.get("legal_entity_uuid") == legal_entity_uuid:
                return {
                    "legal_entity_uuid": legal_entity_uuid,
                    "group_uuid": entity.get("group_uuid", DEFAULT_GROUP_UUID)
                }
                
        # Return default response if not found
        logger.warning(f"No group UUID found for legal entity UUID: {legal_entity_uuid}")
        return {
            "legal_entity_uuid": legal_entity_uuid,
            "group_uuid": DEFAULT_GROUP_UUID
        }
    
    async def detect_legal_entity(self, email_body: Optional[str] = None, document_text: Optional[str] = None) -> Dict[str, Any]:
        """
        Use LLM to detect the legal entity from email body and document text,
        then look up its UUID and group UUID.
        
        Args:
            email_body: Optional email body text
            document_text: Optional document text (from attachment)
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid
        """
        if not email_body and not document_text:
            logger.warning("No text provided for legal entity detection")
            return {
                "legal_entity_uuid": None, 
                "group_uuid": DEFAULT_GROUP_UUID
            }
            
        try:
            # Get all legal entities from repository
            all_entities = await self.repository.fetch_all_legal_entities()
            legal_entity_names = [entity.get("legal_entity_name") for entity in all_entities if entity.get("legal_entity_name")]
            
            # Call LLM to detect legal entity
            detected_name = await self.llm_client.detect_legal_entity(
                legal_entity_names=legal_entity_names,
                email_body=email_body,
                document_text=document_text
            )
            
            if detected_name and detected_name.upper() != "UNKNOWN":
                # Look up the detected entity in the repository
                entity = await self.repository.get_legal_entity_by_name(detected_name)
                if entity:
                    legal_entity_uuid = entity.get("legal_entity_uuid")
                    group_uuid = entity.get("group_uuid", DEFAULT_GROUP_UUID)
                    
                    logger.info(f"Detected legal entity: {detected_name}, UUID: {legal_entity_uuid}, Group: {group_uuid}")
                    return {
                        "legal_entity_uuid": legal_entity_uuid,
                        "group_uuid": group_uuid
                    }
            
            # If no entity detected or not found in repository
            logger.warning(f"Could not map detected entity '{detected_name}' to a known legal entity")
            return {
                "legal_entity_uuid": None,
                "group_uuid": DEFAULT_GROUP_UUID
            }
            
        except Exception as e:
            logger.error(f"Error during legal entity detection: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            return {
                "legal_entity_uuid": None,
                "group_uuid": DEFAULT_GROUP_UUID
            }

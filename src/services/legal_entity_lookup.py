"""
Legal Entity Lookup Service

This module provides an implementation of a legal entity lookup service
that first performs a direct lookup in the LegalEntity table,
and falls back to LLM-based lookup only if no match is found.
"""

import logging
import uuid
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any, List
import asyncio

from models.schemas import LegalEntity
from models.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class LegalEntityLookupService:
    """
    Service that handles legal entity lookup from payer name.
    
    This implementation follows a two-step process:
    1. First, attempt direct lookup in the LegalEntity table
    2. If not found, fall back to LLM-based lookup (currently mocked)
    """
    
    def __init__(self, dao=None, project_id=None, collection_prefix=""):
        """
        Initialize the legal entity lookup service.
        
        Args:
            dao: Optional FirestoreDAO instance. If not provided, a new one will be created.
            project_id: Firestore project ID. Required if dao is not provided.
            collection_prefix: Collection prefix for Firestore. Default is empty.
        """
        # Initialize DAO if not provided
        self.dao = dao or FirestoreDAO(project_id=project_id, collection_prefix=collection_prefix)
        logger.info("Initialized LegalEntityLookupService")
        
        # Cache of legal entities for quick lookup
        self.legal_entity_cache = {}
        self.cache_loaded = False
        
        # Sample predefined legal entities for testing (will be replaced by actual DB lookup)
        # In production, this would come from the LegalEntity table
        self.sample_legal_entities = [
            {"legal_entity_uuid": "acme-corp-legal-entity-12345", "legal_entity_name": "Acme Corp"},
            {"legal_entity_uuid": "globex-corp-legal-entity-67890", "legal_entity_name": "Globex Corporation"},
            {"legal_entity_uuid": "stark-ind-legal-entity-24680", "legal_entity_name": "Stark Industries"},
            {"legal_entity_uuid": "wayne-ent-legal-entity-13579", "legal_entity_name": "Wayne Enterprises"},
            {"legal_entity_uuid": "umbrella-corp-legal-entity-98765", "legal_entity_name": "Umbrella Corporation"},
            {"legal_entity_uuid": "beco-trading-85412", "legal_entity_name": "Beco Trading Ltd"},
            # Amazon's legal entity name for testing direct lookup
            {"legal_entity_uuid": "amazon-clicktech-retail-123456", "legal_entity_name": "Clicktech Retail Private Limited"}
        ]
    
    async def load_legal_entities(self) -> None:
        """
        Load all legal entities from the Firestore database into the cache.
        Since the LegalEntity table is small (~50 items), we can load all of them.
        """
        try:
            # Fetch from Firestore - properly await the async method
            legal_entities = await self.dao.query_documents("legal_entity")
            
            # Build cache for quick name-based lookup
            self.legal_entity_cache = {}
            for entity in legal_entities:
                name = entity.get("legal_entity_name", "").lower().strip()
                if name:
                    self.legal_entity_cache[name] = entity.get("legal_entity_uuid")
                    
            self.cache_loaded = True
            logger.info(f"Loaded {len(self.legal_entity_cache)} legal entities into cache")
            
            # If no entities found in Firestore, and we're in development/testing mode,
            # seed with the sample data for testing
            if len(self.legal_entity_cache) == 0:
                logger.warning("No legal entities found in Firestore, using sample data for testing")
                for entity in self.sample_legal_entities:
                    # Add sample entities to cache
                    name = entity.get("legal_entity_name", "").lower().strip()
                    if name:
                        self.legal_entity_cache[name] = entity.get("legal_entity_uuid")
                        
                    # Also add to Firestore for persistence - properly await the async method
                    await self.dao.add_document(
                        "legal_entity", 
                        entity.get("legal_entity_uuid"), 
                        {
                            "legal_entity_uuid": entity.get("legal_entity_uuid"),
                            "legal_entity_name": entity.get("legal_entity_name"),
                            "is_active": True,
                            "group_uuid": "",  # Default empty group UUID
                            "created_at": datetime.utcnow().isoformat(),
                            "updated_at": datetime.utcnow().isoformat()
                        }
                    )
                logger.info(f"Added {len(self.sample_legal_entities)} sample legal entities to Firestore")
            
        except Exception as e:
            logger.error(f"Failed to load legal entities: {str(e)}")
            # Initialize empty cache to avoid repeated failures
            self.legal_entity_cache = {}
            self.cache_loaded = True
    
    async def lookup_legal_entity_uuid(self, payer_name: str) -> str:
        """
        Look up the legal entity UUID for a given payer name.
        
        Performs a two-step lookup:
        1. Direct lookup in the LegalEntity table
        2. If not found, fall back to LLM-based lookup
        
        Args:
            payer_name: The name of the payer company
            
        Returns:
            UUID string for the legal entity
            
        Raises:
            ValueError: If payer_name is empty or if the legal entity is not registered
        """
        if not payer_name:
            raise ValueError("Cannot lookup legal entity for empty payer name")
            
        # Normalize payer name for consistent lookup
        normalized_name = payer_name.lower().strip()
        
        # Ensure cache is loaded
        if not self.cache_loaded:
            await self.load_legal_entities()
        
        # STEP 1: Check direct lookup in LegalEntity table (cache)
        if normalized_name in self.legal_entity_cache:
            uuid = self.legal_entity_cache[normalized_name]
            logger.info(f"Found legal entity UUID for '{payer_name}' via direct lookup: {uuid}")
            return uuid
            
        # STEP 2: Not found in direct lookup, fall back to LLM (mocked for now)
        logger.info(f"Legal entity '{payer_name}' not found in database, falling back to LLM lookup")
        
        # For now, generate a deterministic UUID based on the payer name
        # This simulates an LLM lookup that always returns the same UUID for the same name
        name_hash = hashlib.md5(normalized_name.encode()).hexdigest()[:8]
        entity_uuid = f"legal-entity-{name_hash}"
        
        # In a real implementation, check if the LLM returned "not registered"
        # For simulation, consider entities with hash ending in '00' as not registered
        if name_hash.endswith('00'):
            raise ValueError(f"Legal entity '{payer_name}' is not registered in the system")
        
        logger.info(f"Generated legal entity UUID for '{payer_name}' via LLM: {entity_uuid}")
        return entity_uuid
        
    async def lookup_from_llm_output(self, llm_output: Dict[str, Any]) -> str:
        """
        Look up legal entity UUID from LLM output data.
        
        Args:
            llm_output: Dictionary containing LLM output, including metaTable with payersLegalName
            
        Returns:
            UUID string for the legal entity
            
        Raises:
            ValueError: If payer name is missing or legal entity is not registered
        """
        # Extract payer name from LLM output
        meta_table = llm_output.get("metaTable", {})
        payer_name = meta_table.get("payersLegalName")
        
        if not payer_name:
            raise ValueError("No payer legal name found in LLM output")
        
        # Look up the legal entity UUID
        return await self.lookup_legal_entity_uuid(payer_name)
    
    # Synchronous version for backwards compatibility
    def lookup_legal_entity_uuid_sync(self, payer_name: str) -> str:
        """
        Synchronous version of lookup_legal_entity_uuid.
        For use in contexts where async/await is not available.
        
        Args:
            payer_name: The name of the payer company
            
        Returns:
            UUID string for the legal entity
            
        Raises:
            ValueError: If payer_name is empty or if the legal entity is not registered
        """
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.lookup_legal_entity_uuid(payer_name))
        finally:
            loop.close()

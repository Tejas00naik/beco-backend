"""Legal entity repository for database operations."""

import logging
from typing import List, Dict, Any, Optional

from src.repositories.firestore_dao import FirestoreDAO

logger = logging.getLogger(__name__)

class LegalEntityRepository:
    """Repository for legal entity database operations."""
    
    def __init__(self, dao: FirestoreDAO):
        """
        Initialize the repository.
        
        Args:
            dao: Firestore DAO for database operations
        """
        self.dao = dao
        self._cache = {}
        self._entities_loaded = False
        
    async def fetch_all_legal_entities(self) -> List[Dict[str, Any]]:
        """
        Fetch all legal entities from Firestore.
        
        Returns:
            List of legal entity objects
        """
        if not self.dao:
            logger.error("No DAO provided, cannot fetch legal entities")
            return []
            
        try:
            legal_entities = await self.dao.query_documents("legal_entity", [])
            logger.info(f"Fetched {len(legal_entities)} legal entities from Firestore")
            
            # Update cache
            self._cache = {}
            for entity in legal_entities:
                name = entity.get("legal_entity_name")
                if name:
                    # Store in cache with lowercase key for case-insensitive lookup
                    self._cache[name.lower()] = entity
                    
            self._entities_loaded = True
            return legal_entities
        except Exception as e:
            logger.error(f"Error fetching legal entities: {str(e)}")
            return []
    
    async def get_legal_entity_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get legal entity by name (case-insensitive).
        
        Args:
            name: Legal entity name to look up
            
        Returns:
            Legal entity object if found, None otherwise
        """
        # Load entities if not loaded
        if not self._entities_loaded:
            await self.fetch_all_legal_entities()
            
        # Normalize name for case-insensitive comparison
        normalized_name = name.lower() if name else ""
        
        if not normalized_name:
            logger.warning("Empty name provided to get_legal_entity_by_name")
            return None
            
        # Try exact match first
        entity = self._cache.get(normalized_name)
        if entity:
            logger.info(f"Found exact match for '{name}'")
            return entity
            
        return None

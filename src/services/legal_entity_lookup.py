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
import json
import aiohttp
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from the project root .env file
dotenv_path = Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / '.env'
load_dotenv(dotenv_path=dotenv_path, override=True)

from models.schemas import LegalEntity
from models.firestore_dao import FirestoreDAO
from src.llm_integration.config import LEGAL_ENTITY_DETECTION_PROMPT, DEFAULT_MODEL

# Import sample groups for debugging purposes
try:
    from scripts.seed_legal_entities import SAMPLE_GROUPS
    logger = logging.getLogger(__name__)
    logger.info("Successfully imported SAMPLE_GROUPS for debug logging")
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("Could not import SAMPLE_GROUPS, group name logging will be limited")
    SAMPLE_GROUPS = []

# Default group UUID when no match is found
DEFAULT_GROUP_UUID = "default-group-uuid-12345"

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
        
        logger.info(f"Group detection: Processing LLM output for legal entity lookup")
        logger.info(f"LLM meta table payer name: {payer_name}")
        
        if not payer_name:
            logger.warning("No payer legal name found in LLM output, using default group")
            logger.info(f"Using default group UUID: {DEFAULT_GROUP_UUID}")
            return DEFAULT_GROUP_UUID
        
        # Normalize payer name
        normalized_name = self._normalize_company_name(payer_name)
        logger.info(f"Group detection: Normalized payer name from '{payer_name}' to '{normalized_name}'")
        payer_name = normalized_name
        
        logger.info(f"Looking up group UUID for payer: {payer_name}")
        
        # Query Firestore for matching legal entity group
        legal_entities = await self.dao.query_documents("legal_entity_group", [])
        
        # Iterate through legal entities to find a match
        for entity in legal_entities:
            group_name = entity.get("group_name", "").lower().strip()
            if group_name in payer_name or payer_name in group_name:
                logger.info(f"Group detection: Found matching legal entity group:")
                logger.info(f"  Group name: {entity['group_name']}")
                logger.info(f"  Group UUID: {entity['legal_entity_group_uuid']}")
                logger.info(f"  Match type: {'Payer contains group' if group_name in payer_name else 'Group contains payer'}")
                return entity['legal_entity_group_uuid']
        
        # If no match found, return default group
        logger.warning(f"No matching legal entity group found for payer {payer_name}, using default group")
        logger.info(f"Using default group UUID: {DEFAULT_GROUP_UUID}")
        return DEFAULT_GROUP_UUID
    
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
            
    async def fetch_all_legal_entities(self) -> List[Dict[str, Any]]:
        """
        Fetch all legal entities from Firestore.
        
        Returns:
            List of legal entity dictionaries
        """
        if not self.cache_loaded:
            await self.load_legal_entities()
            
        legal_entities = await self.dao.query_documents("legal_entity")
        
        # If no entities found in Firestore and in development/testing mode,
        # use the sample data
        if len(legal_entities) == 0:
            logger.warning("No legal entities found in Firestore, using sample data for testing")
            legal_entities = self.sample_legal_entities
            
        return legal_entities
        
    async def detect_legal_entity_with_llm(self, email_body: Optional[str] = None, document_text: Optional[str] = None) -> Dict[str, Any]:
        """
        Use LLM to detect the legal entity from email body and document text.
        
        Args:
            email_body: Optional email body text
            document_text: Optional document text (from attachment)
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid if found, or defaults
        """
        # Fetch all legal entities to provide as context
        legal_entities = await self.fetch_all_legal_entities()
        
        # Create a simple list of legal entity names for the prompt
        legal_entity_names = [entity.get("legal_entity_name") for entity in legal_entities if entity.get("legal_entity_name")]
        
        # Log all available legal entity names
        logger.info(f"Available legal entity names for LLM detection: {legal_entity_names}")
        
        # Format the legal entity list for the prompt
        legal_entity_list = "\n".join([f"- {name}" for name in legal_entity_names])
        
        # Format the prompt with the legal entity list
        prompt = LEGAL_ENTITY_DETECTION_PROMPT.format(legal_entity_list=legal_entity_list)
        
        # Combine email body and document text if both are provided
        combined_text = ""
        if email_body:
            combined_text += f"EMAIL BODY:\n{email_body}\n\n"
        if document_text:
            combined_text += f"DOCUMENT TEXT:\n{document_text}"
            
        if not combined_text:
            logger.warning("No text provided for legal entity detection")
            return {
                "legal_entity_uuid": None,
                "group_uuid": DEFAULT_GROUP_UUID
            }
        
        try:
            # Call OpenAI API directly for this simple task
            api_key = os.environ.get('OPENAI_API_KEY')
            if not api_key:
                logger.error("OpenAI API key not found in environment variables")
                return {
                    "legal_entity_uuid": None,
                    "group_uuid": DEFAULT_GROUP_UUID
                }
                
            # Debug API key to ensure it's not a placeholder
            key_prefix = api_key[:4] if len(api_key) > 4 else api_key
            key_suffix = api_key[-4:] if len(api_key) > 4 else ""
            logger.info(f"Using OpenAI API key: {key_prefix}...{key_suffix}")
            
            # Ensure API key isn't a placeholder like 'your-api-key-here'
            if 'your-' in api_key or 'api-key' in api_key or api_key.startswith('sk-') is False:
                logger.error(f"API key appears to be a placeholder: {key_prefix}...")
                logger.error("Please set a valid OpenAI API key in the .env file")
                return {
                    "legal_entity_uuid": None,
                    "group_uuid": DEFAULT_GROUP_UUID
                }
                
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                payload = {
                    "model": DEFAULT_MODEL,
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": combined_text}
                    ],
                    "temperature": 0.1  # Low temperature for more deterministic results
                }
                
                # Log payload details (excluding full text for brevity)
                logger.info(f"Calling LLM for legal entity detection with temperature: {payload['temperature']}")
                logger.info(f"Prompt context contains {len(legal_entity_names)} legal entity names")
                
                # Log a limited portion of the user text for debugging
                user_text_sample = combined_text[:200] + "..." if len(combined_text) > 200 else combined_text
                logger.info(f"Sample of text sent to LLM: {user_text_sample}")
                
                async with session.post(
                    "https://api.openai.com/v1/chat/completions", 
                    headers=headers, 
                    json=payload
                ) as response:
                    response_data = await response.json()
                    logger.info(f"LLM API response status: {response.status}")
                    
                    # Log usage statistics if available
                    if "usage" in response_data:
                        usage = response_data["usage"]
                        logger.info(f"LLM API usage: {usage}")
                    
                    if response.status != 200:
                        logger.error(f"Error calling OpenAI API: {response_data}")
                        return {
                            "legal_entity_uuid": None,
                            "group_uuid": DEFAULT_GROUP_UUID
                        }
                        
                    # Extract the detected legal entity name from the response
                    detected_name = response_data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                    logger.info(f"LLM detected legal entity: '{detected_name}'")
                    
                    # Log full response for debugging if needed
                    if "choices" in response_data:
                        choices_data = response_data["choices"]
                        finish_reason = choices_data[0].get("finish_reason") if choices_data else "unknown"
                        logger.info(f"LLM finish_reason: {finish_reason}")
                    
                    # Handle the case where LLM couldn't detect the entity
                    if detected_name == "UNKNOWN" or not detected_name:
                        logger.warning("LLM couldn't detect legal entity")
                        return {
                            "legal_entity_uuid": None,
                            "group_uuid": DEFAULT_GROUP_UUID
                        }
                    
                    # Try to find the detected legal entity in our list
                    match_found = False
                    for entity in legal_entities:
                        entity_name = entity.get("legal_entity_name")
                        if entity_name == detected_name:
                            match_found = True
                            legal_entity_uuid = entity.get("legal_entity_uuid")
                            group_uuid = entity.get("group_uuid")
                            
                            logger.info(f"Found exact matching legal entity: {legal_entity_uuid}")
                            logger.info(f"Legal entity name match: '{entity_name}' == '{detected_name}'")
                            logger.info(f"Group UUID for legal entity: {group_uuid}")
                            
                            # Also log the group name if we can find it
                            for group in SAMPLE_GROUPS if "SAMPLE_GROUPS" in globals() else []:
                                if group.get("group_uuid") == group_uuid:
                                    logger.info(f"Group name: {group.get('group_name')}")
                                    break
                            
                            return {
                                "legal_entity_uuid": legal_entity_uuid,
                                "group_uuid": group_uuid
                            }
            
            # If no match was found
            logger.warning(f"No matching legal entity found for '{detected_name}'")
            return {
                "legal_entity_uuid": None,
                "group_uuid": DEFAULT_GROUP_UUID
            }
                
        except Exception as e:
            logger.error(f"Error during legal entity detection: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            return {
                "legal_entity_uuid": None,
                "group_uuid": DEFAULT_GROUP_UUID
            }

"""Legal entity detection module using LLM."""

import logging
import os
import json
from typing import Dict, Any, Optional, List
import aiohttp

# Import environment variables
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Simple prompt for legal entity detection
LEGAL_ENTITY_DETECTION_PROMPT = """
You are a legal entity detection system. Your task is to identify which legal entity from the provided list is mentioned in the input text.

The text may contain an email body and/or a document text (like a PDF or attachment).

Available legal entities:
{legal_entity_list}

Rules:
1. Return EXACTLY ONE legal entity name from the list above that best matches the entity mentioned in the text.
2. The match should be the full, exact name as listed above.
3. If you cannot confidently match any entity from the list, return "UNKNOWN".
4. Do NOT return any explanation, reasoning, or additional text - ONLY the matching entity name or "UNKNOWN".

For example, if the text mentions "payment from XYZ Corp" and "XYZ Corporation" is in the list, return "XYZ Corporation".
"""

class LegalEntityDetector:
    """
    Detects legal entities from text using LLM.
    This is a dedicated module for the first step of the two-step LLM process.
    """
    
    def __init__(self, dao=None):
        """
        Initialize the legal entity detector.
        
        Args:
            dao: Firestore DAO instance for legal entity lookups
        """
        self.dao = dao
        self.default_model = os.environ.get('OPENAI_MODEL', 'gpt-4.1')
        
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
            return legal_entities
        except Exception as e:
            logger.error(f"Error fetching legal entities: {str(e)}")
            return []
            
    async def detect_legal_entity_with_llm(self, email_body: Optional[str] = None, document_text: Optional[str] = None) -> Dict[str, Any]:
        """
        Use LLM to detect the legal entity from email body and document text.
        
        Args:
            email_body: Optional email body text
            document_text: Optional document text (from attachment)
            
        Returns:
            Dictionary with legal_entity_uuid and group_uuid if found, or defaults
        """
        # Constants
        DEFAULT_GROUP_UUID = "00000000-0000-0000-0000-000000000000"
        DEFAULT_MODEL = "gpt-4.1"
        
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

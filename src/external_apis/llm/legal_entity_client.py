"""LLM client for legal entity detection."""

import logging
import os
import json
from typing import Dict, Any, Optional, List
import aiohttp

logger = logging.getLogger(__name__)

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

class LegalEntityLLMClient:
    """Client for calling LLM API for legal entity detection."""
    
    def __init__(self):
        """Initialize the LLM client."""
        self.api_key = os.environ.get("OPENAI_API_KEY")
        self.default_model = os.environ.get("OPENAI_MODEL", "gpt-4.1")
        
    async def detect_legal_entity(
        self, 
        legal_entity_names: List[str], 
        email_body: Optional[str] = None, 
        document_text: Optional[str] = None
    ) -> str:
        """
        Detect legal entity from text using LLM.
        
        Args:
            legal_entity_names: List of legal entity names to match against
            email_body: Optional email body text
            document_text: Optional document text (from attachment)
            
        Returns:
            Detected legal entity name or "UNKNOWN"
        """
        if not self.api_key:
            logger.error("No OpenAI API key provided")
            return "UNKNOWN"
            
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
            logger.error("No text provided for legal entity detection")
            return "UNKNOWN"
            
        try:
            # Prepare the API call
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # Payload for the API call
            payload = {
                "model": self.default_model,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": combined_text}
                ],
                "temperature": 0,  # Use low temperature for deterministic output
                "max_tokens": 50   # Limit token usage
            }
            
            # Log request details for debugging
            logger.info(f"Using model: {self.default_model} for legal entity detection")
            logger.info(f"Prompt context contains {len(legal_entity_names)} legal entity names")
            
            # Log a limited portion of the user text for debugging
            user_text_sample = combined_text[:200] + "..." if len(combined_text) > 200 else combined_text
            logger.info(f"Sample of text sent to LLM: {user_text_sample}")
            
            async with aiohttp.ClientSession() as session:
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
                        return "UNKNOWN"
                        
                    # Extract the detected legal entity name from the response
                    detected_name = response_data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                    logger.info(f"LLM detected legal entity: '{detected_name}'")
                    
                    # Log full response for debugging if needed
                    if "choices" in response_data:
                        choices_data = response_data["choices"]
                        finish_reason = choices_data[0].get("finish_reason") if choices_data else "unknown"
                        logger.info(f"LLM finish_reason: {finish_reason}")
                    
                    # Return the detected name or "UNKNOWN"
                    return detected_name
                
        except Exception as e:
            logger.error(f"Error during legal entity detection: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            return "UNKNOWN"

"""LLM Client for OpenAI API interactions."""

import logging
import os
from typing import Dict, Any, List, Optional, Union
from dotenv import load_dotenv, find_dotenv

from openai import OpenAI
from openai.types.chat import ChatCompletionMessage
from openai.types.completion_usage import CompletionUsage
from src.external_apis.llm.config import OPENAI_API_KEY, DEFAULT_MODEL

logger = logging.getLogger(__name__)


class LLMClient:
    """
    A focused client that only handles communication with the OpenAI API.
    This class is responsible for:
    1. Initializing the OpenAI client
    2. Calling the chat API
    3. Handling and logging token usage
    
    It does NOT handle any extraction or processing logic.
    """
    
    def __init__(self, model: str = DEFAULT_MODEL):
        """
        Initialize the LLM client.
        
        Args:
            model: The model to use for chat completions
        """
        # Initialize OpenAI client
        # env_path = os.path.join(os.getcwd(), "secret.env")
        # print("using env path: ", env_path)
        print("env path exists: ", find_dotenv())
        load_dotenv(find_dotenv(), override=True)
        logger.info(f"Loading OpenAI API key from environment for model {model}...")
        
        # Get API key directly from .env file for maximum reliability
        # Use the key directly from config or as a fallback try the environment variable
        openai_api_key = OPENAI_API_KEY
        if not openai_api_key:
            # As a fallback, try getting directly from environment
            openai_api_key = os.environ.get("OPENAI_API_KEY")
            
        if not openai_api_key:
            logger.error("OpenAI API key not found in environment variables!")
            raise ValueError("OpenAI API key not found in environment variables!")
            
        # Set up the OpenAI client
        self.client = OpenAI(api_key=openai_api_key)
        self.model = model
        logger.info(f"LLMClient initialized with model {self.model}")
    
    async def call_chat_api(
        self, 
        system_prompt: str, 
        user_content: str, 
        temperature: float = 0.0,
        timeout: float = 90.0
    ) -> Dict[str, Any]:
        """
        Call the OpenAI Chat Completions API.
        
        Args:
            system_prompt: The system prompt to use
            user_content: The user content to send
            temperature: The temperature for response generation (0.0 for deterministic)
            timeout: Timeout in seconds for the API call
            
        Returns:
            Dictionary containing:
                - 'response_text': The text response from the API
                - 'usage': Token usage statistics if available
        """
        logger.info(f"Calling {self.model} with system prompt {len(system_prompt)} chars and user content {len(user_content)} chars")
        
        # Estimate token count (rough approximation: 1 token ≈ 4 characters for English text)
        system_tokens = len(system_prompt) / 4
        user_tokens = len(user_content) / 4
        total_tokens = system_tokens + user_tokens
        
        logger.info(f"Estimated token counts - System: {system_tokens:.0f}, User: {user_tokens:.0f}, Total: {total_tokens:.0f}")
        
        # Check if likely to exceed token limits
        if total_tokens > 128000:  # GPT-4 Turbo max context window
            logger.warning(f"⚠️ POTENTIAL TOKEN LIMIT ISSUE: Estimated tokens ({total_tokens:.0f}) may exceed model context limit")
        
        result = {
            "response_text": "",
            "usage": None
        }
        
        try:
            # Call the API with timeout
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                temperature=temperature,
                timeout=timeout
            )
            
            # Extract the response text
            result["response_text"] = response.choices[0].message.content
            logger.info(f"Got response with {len(result['response_text'])} chars")
            
            # Log and store token usage if available
            if hasattr(response, 'usage') and response.usage:
                usage = response.usage
                logger.info(f"Actual token usage - Prompt: {usage.prompt_tokens}, "
                            f"Completion: {usage.completion_tokens}, "
                            f"Total: {usage.total_tokens}")
                result["usage"] = {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                }
                
        except Exception as e:
            # Enhanced error logging with specific error types
            error_type = type(e).__name__
            error_msg = str(e).lower()
            
            logger.error(f"Error calling OpenAI API ({error_type}): {str(e)}")
            
            # Check for specific error conditions
            if any(token_err in error_msg for token_err in ["maximum context length", "token limit", "tokens in prompt"]):
                logger.error(f"⚠️ TOKEN LIMIT EXCEEDED: Document is too large for {self.model}. "
                           f"Estimated tokens: {total_tokens:.0f}")
            elif "rate limit" in error_msg:
                logger.error(f"⚠️ RATE LIMIT: OpenAI API rate limit reached")
            elif "timeout" in error_msg:
                logger.error(f"⚠️ TIMEOUT: Request timed out after {timeout} seconds. Document may be too large or complex")
            
            # Include full traceback for detailed debugging
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Re-raise the error
            raise
            
        return result

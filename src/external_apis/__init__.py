"""External API clients for third-party services."""

# Import all client classes for easy access
from .llm import LegalEntityLLMClient, LLMExtractor

__all__ = ['LegalEntityLLMClient', 'LLMExtractor']

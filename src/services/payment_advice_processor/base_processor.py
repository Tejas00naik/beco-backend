"""Base class for group-specific processors."""

from abc import ABC, abstractmethod
from typing import Dict, Any


class GroupProcessor(ABC):
    """Abstract base class for group-specific processing logic."""
    
    @abstractmethod
    def process_payment_advice(self, attachment_text: str, email_body: str, attachment_obj: Dict[str, Any], attachment_file_format: str) -> Dict[str, Any]:
        """
        Process the payment advice.
        
        Args:
            attachment_text: Text content of the attachment
            email_body: Email body text
            attachment_obj: Dictionary with attachment metadata
            attachment_file_format: Format of the attachment file
            
        Returns:
            Processed payment advice dictionary
        """
        pass
    
    @abstractmethod
    def get_prompt_template(self) -> str:
        """Get the group-specific prompt template."""
        pass
    
    def post_process_output(self, processed_output: Dict[str, Any]) -> Dict[str, Any]:
        """Post-process the LLM output."""
        return processed_output
    
    def get_group_name(self) -> str:
        """Get the name of the group."""
        return self.__class__.__name__.replace("GroupProcessor", "")

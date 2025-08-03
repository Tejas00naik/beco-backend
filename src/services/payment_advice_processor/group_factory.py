"""Group-specific factory pattern for LLM extraction and processing."""

import logging
from typing import Dict, Any
from src.services.payment_advice_processor.base_processor import GroupProcessor
from src.services.payment_advice_processor.constants import GROUP_UUIDS

logger = logging.getLogger(__name__)

class DefaultGroupProcessor(GroupProcessor):
    """Default group processor when no specific group is identified."""
    
    def process_payment_advice(self, attachment_text: str, email_body: str, attachment_obj: Dict[str, Any]) -> Dict[str, Any]:
        """Process the payment advice."""
        return None
        
    def get_prompt_template(self) -> str:
        """Get the group-specific prompt template."""
        return """You are an AI assistant that extracts data from payment advice documents.
        Please extract the key information and format it as a JSON object."""
        
    def get_group_name(self) -> str:
        """Get the name of this group processor."""
        return "Default"

class GroupProcessorFactory:
    """Factory class for creating group-specific processors."""
    
    @classmethod
    def get_processor(cls, group_uuid: str) -> GroupProcessor:
        """
        Get the appropriate processor for the given group UUID.
        
        Args:
            group_uuid: The group UUID to get a processor for
            
        Returns:
            An instance of the appropriate GroupProcessor
        """
        # Import at runtime to avoid circular imports
        from src.services.payment_advice_processor.amazon import AmazonGroupProcessor
        from src.services.payment_advice_processor.zepto import ZeptoGroupProcessor
        from src.services.payment_advice_processor.blinkit_hot import HOTGroupProcessor
        
        # Create processor map dynamically
        processor_map = {
            GROUP_UUIDS["amazon"]: AmazonGroupProcessor,
            GROUP_UUIDS["zepto"]: ZeptoGroupProcessor,
            GROUP_UUIDS["hot"]: HOTGroupProcessor,
        }
        
        if not group_uuid or group_uuid not in processor_map:
            logger.warning(f"No processor found for group_uuid={group_uuid}, using default")
            return DefaultGroupProcessor()

        processor_class = processor_map[group_uuid]
        logger.info(f"Using {processor_class.__name__} for group_uuid={group_uuid}")
        return processor_class()
    
    @classmethod
    def register_processor(cls, group_uuid: str, processor_class: type) -> None:
        """
        Register a processor for a specific group UUID.
        
        Args:
            group_uuid: The group UUID
            processor_class: The processor class to register
        """
        # This method is not needed with the dynamic approach
        pass

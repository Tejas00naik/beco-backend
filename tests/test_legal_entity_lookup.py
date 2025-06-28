"""
Tests for the LegalEntity Lookup Service

These tests validate that the legal entity lookup service correctly performs:
1. Direct lookup from the LegalEntity table
2. Fallback to LLM-based lookup if not found in the table
3. Error handling for unregistered entities
"""

import unittest
import asyncio
from unittest.mock import patch, MagicMock

from src.services.legal_entity_lookup import LegalEntityLookupService


class TestLegalEntityLookup(unittest.TestCase):
    """Test cases for the Legal Entity Lookup Service."""

    def setUp(self):
        """Set up the test environment."""
        # Create a legal entity lookup service with mocked DAO
        self.mock_dao = MagicMock()
        self.lookup_service = LegalEntityLookupService(dao=self.mock_dao)
        
        # Ensure the test starts with a clean cache
        self.lookup_service.legal_entity_cache = {}
        self.lookup_service.cache_loaded = False
        
        # Sample test data
        self.test_legal_entities = [
            {"legal_entity_uuid": "amazon-clicktech-retail-123456", "legal_entity_name": "Clicktech Retail Private Limited"},
            {"legal_entity_uuid": "acme-corp-legal-entity-12345", "legal_entity_name": "Acme Corp"},
        ]
        
        # Update the sample data in the service
        self.lookup_service.sample_legal_entities = self.test_legal_entities

    def test_load_legal_entities(self):
        """Test that legal entities are loaded into cache correctly."""
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self.lookup_service.load_legal_entities())
        finally:
            loop.close()
        
        # Verify that the cache is correctly populated
        self.assertTrue(self.lookup_service.cache_loaded)
        self.assertEqual(len(self.lookup_service.legal_entity_cache), 2)
        self.assertEqual(
            self.lookup_service.legal_entity_cache["clicktech retail private limited"], 
            "amazon-clicktech-retail-123456"
        )

    def test_direct_lookup_success(self):
        """Test successful direct lookup in the LegalEntity table."""
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                self.lookup_service.lookup_legal_entity_uuid("Clicktech Retail Private Limited")
            )
        finally:
            loop.close()
        
        # Verify the correct UUID is returned
        self.assertEqual(result, "amazon-clicktech-retail-123456")

    def test_direct_lookup_case_insensitive(self):
        """Test that direct lookup is case-insensitive."""
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                self.lookup_service.lookup_legal_entity_uuid("clicktech RETAIL private LIMITED")
            )
        finally:
            loop.close()
        
        # Verify the correct UUID is returned despite different casing
        self.assertEqual(result, "amazon-clicktech-retail-123456")

    def test_llm_fallback_lookup(self):
        """Test fallback to LLM-based lookup when entity not found in table."""
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                self.lookup_service.lookup_legal_entity_uuid("Unknown Company Ltd")
            )
        finally:
            loop.close()
        
        # Verify a deterministic UUID is generated based on the name
        self.assertTrue(result.startswith("legal-entity-"))
        self.assertNotEqual(result, "amazon-clicktech-retail-123456")

    def test_empty_payer_name(self):
        """Test that empty payer name raises ValueError."""
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        with self.assertRaises(ValueError):
            try:
                loop.run_until_complete(self.lookup_service.lookup_legal_entity_uuid(""))
            finally:
                loop.close()

    def test_sync_lookup_method(self):
        """Test the synchronous lookup method."""
        # Call the synchronous version
        result = self.lookup_service.lookup_legal_entity_uuid_sync("Clicktech Retail Private Limited")
        
        # Verify the correct UUID is returned
        self.assertEqual(result, "amazon-clicktech-retail-123456")

    def test_lookup_from_llm_output(self):
        """Test extracting and looking up legal entity from LLM output."""
        # Mock LLM output with payer name
        llm_output = {
            "metaTable": {
                "payersLegalName": "Clicktech Retail Private Limited",
                "paymentAdviceNumber": "PA-12345",
                "paymentDate": "2023-01-01"
            }
        }
        
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                self.lookup_service.lookup_from_llm_output(llm_output)
            )
        finally:
            loop.close()
        
        # Verify the correct UUID is returned
        self.assertEqual(result, "amazon-clicktech-retail-123456")

    def test_llm_output_missing_payer(self):
        """Test that missing payer name in LLM output raises ValueError."""
        # Mock LLM output without payer name
        llm_output = {
            "metaTable": {
                "paymentAdviceNumber": "PA-12345",
                "paymentDate": "2023-01-01"
            }
        }
        
        # Run the async method in a new event loop
        loop = asyncio.new_event_loop()
        with self.assertRaises(ValueError):
            try:
                loop.run_until_complete(
                    self.lookup_service.lookup_from_llm_output(llm_output)
                )
            finally:
                loop.close()


if __name__ == "__main__":
    unittest.main()

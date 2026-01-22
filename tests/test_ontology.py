"""Unit tests for ontology module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from src.dbxmetagen.ontology import (
    OntologyConfig,
    OntologyLoader,
    EntityDefinition,
    EntityDiscoverer,
    OntologyBuilder,
    build_ontology
)


class TestOntologyConfig:
    """Tests for OntologyConfig."""
    
    def test_fully_qualified_entities(self):
        config = OntologyConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_entities == "test_catalog.test_schema.ontology_entities"
    
    def test_fully_qualified_metrics(self):
        config = OntologyConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_metrics == "test_catalog.test_schema.ontology_metrics"


class TestOntologyLoader:
    """Tests for OntologyLoader."""
    
    def test_default_config(self):
        config = OntologyLoader._default_config()
        assert "version" in config
        assert "entities" in config
        assert "relationships" in config
    
    def test_get_entity_definitions_empty(self):
        config = {"entities": {"definitions": {}}}
        entities = OntologyLoader.get_entity_definitions(config)
        assert entities == []
    
    def test_get_entity_definitions_with_data(self):
        config = {
            "entities": {
                "definitions": {
                    "Customer": {
                        "description": "Customer entity",
                        "keywords": ["customer", "user"],
                        "typical_attributes": ["id", "name"]
                    }
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        assert len(entities) == 1
        assert entities[0].name == "Customer"
        assert entities[0].description == "Customer entity"
        assert "customer" in entities[0].keywords


class TestEntityDefinition:
    """Tests for EntityDefinition."""
    
    def test_creation(self):
        entity = EntityDefinition(
            name="Customer",
            description="A customer",
            keywords=["customer"],
            typical_attributes=["id"]
        )
        assert entity.name == "Customer"
        assert entity.description == "A customer"
    
    def test_default_lists(self):
        entity = EntityDefinition(name="Test", description="Test")
        assert entity.keywords == []
        assert entity.typical_attributes == []


class TestEntityDiscoverer:
    """Tests for EntityDiscoverer."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return OntologyConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    def test_calculate_match_confidence_exact_match(self):
        ontology_config = {
            "entities": {
                "discovery_confidence_threshold": 0.5,
                "definitions": {}
            }
        }
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        
        entity_def = EntityDefinition(
            name="Customer",
            description="Customer entity",
            keywords=["customer", "user"],
            typical_attributes=["id", "name"]
        )
        
        confidence = discoverer._calculate_match_confidence(
            "customer_master",
            "this table contains customer data with id and name",
            entity_def
        )
        assert confidence > 0.5


class TestOntologyBuilder:
    """Tests for OntologyBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return OntologyConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            return OntologyBuilder(mock_spark, config)
    
    def test_create_entities_table(self, builder, mock_spark):
        builder.create_entities_table()
        mock_spark.sql.assert_called()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "ontology_entities" in call_arg
    
    def test_create_metrics_table(self, builder, mock_spark):
        builder.create_metrics_table()
        mock_spark.sql.assert_called()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "ontology_metrics" in call_arg


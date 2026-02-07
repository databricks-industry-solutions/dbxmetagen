"""Unit tests for ontology module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from dbxmetagen.ontology import (
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
    
    def test_fully_qualified_kb(self):
        config = OntologyConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_kb == "test_catalog.test_schema.table_knowledge_base"


class TestOntologyLoader:
    """Tests for OntologyLoader."""
    
    def test_default_config(self):
        config = OntologyLoader._default_config()
        assert "version" in config
        assert "entities" in config
        assert "relationships" in config
    
    def test_default_config_has_auto_discover(self):
        config = OntologyLoader._default_config()
        assert config["entities"]["auto_discover"] is True
    
    def test_default_config_has_lowered_threshold(self):
        """Default threshold should be 0.4 (lowered from 0.7)."""
        config = OntologyLoader._default_config()
        assert config["entities"]["discovery_confidence_threshold"] == 0.4
    
    def test_default_config_has_embedded_definitions(self):
        """Default config should include embedded entity definitions."""
        config = OntologyLoader._default_config()
        definitions = config["entities"]["definitions"]
        assert len(definitions) > 0
        assert "Customer" in definitions
        assert "Product" in definitions
        assert "DataTable" in definitions  # Fallback type
    
    def test_get_entity_definitions_uses_defaults_when_empty(self):
        """When config has empty definitions, should use embedded defaults."""
        config = {"entities": {"definitions": {}}}
        entities = OntologyLoader.get_entity_definitions(config)
        # Should NOT be empty - should use embedded defaults
        assert len(entities) > 0
    
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
    
    def test_get_entity_definitions_multiple(self):
        config = {
            "entities": {
                "definitions": {
                    "Customer": {"description": "Customer", "keywords": ["customer"]},
                    "Product": {"description": "Product", "keywords": ["product"]},
                    "Patient": {"description": "Patient", "keywords": ["patient"]}
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        assert len(entities) == 3
        entity_names = [e.name for e in entities]
        assert "Customer" in entity_names
        assert "Product" in entity_names
        assert "Patient" in entity_names


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
    
    def test_keywords_are_list(self):
        entity = EntityDefinition(
            name="Test",
            description="Test",
            keywords=["a", "b", "c"]
        )
        assert isinstance(entity.keywords, list)
        assert len(entity.keywords) == 3


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
        
        # New API: name_variations, original_name, comment, entity_def
        name_variations = discoverer._normalize_name("customer_master")
        confidence = discoverer._calculate_match_confidence(
            name_variations,
            "customer_master",
            "this table contains customer data with id and name",
            entity_def
        )
        assert confidence > 0.5
    
    def test_calculate_match_confidence_no_match(self):
        ontology_config = {"entities": {"definitions": {}}}
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        
        entity_def = EntityDefinition(
            name="Customer",
            description="Customer entity",
            keywords=["customer", "user"],
            typical_attributes=["id", "name"]
        )
        
        name_variations = discoverer._normalize_name("inventory_table")
        confidence = discoverer._calculate_match_confidence(
            name_variations,
            "inventory_table",
            "this table contains product inventory data",
            entity_def
        )
        assert confidence < 0.3
    
    def test_calculate_match_confidence_partial_match(self):
        ontology_config = {"entities": {"definitions": {}}}
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        
        entity_def = EntityDefinition(
            name="Product",
            description="Product entity",
            keywords=["product", "item", "sku"],
            typical_attributes=["id", "name", "price"]
        )
        
        # Table name doesn't match but comment has "product"
        name_variations = discoverer._normalize_name("inventory_items")
        confidence = discoverer._calculate_match_confidence(
            name_variations,
            "inventory_items",
            "this table stores product data",
            entity_def
        )
        # Should have some confidence but not full
        assert 0 < confidence < 1.0
    
    def test_ai_model_constant_defined(self):
        """AI model constant should be defined for AI discovery."""
        assert hasattr(EntityDiscoverer, 'AI_MODEL')
        assert EntityDiscoverer.AI_MODEL == "databricks-gpt-oss-120b"
    
    def test_normalize_name_handles_snake_case(self):
        """_normalize_name should handle snake_case variations."""
        ontology_config = {"entities": {"definitions": {}}}
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        
        variations = discoverer._normalize_name("customer_orders")
        assert "customer_orders" in variations
        assert "customer orders" in variations
        assert "customerorders" in variations
    
    def test_normalize_name_handles_plurals(self):
        """_normalize_name should generate singular/plural variations."""
        ontology_config = {"entities": {"definitions": {}}}
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        
        variations = discoverer._normalize_name("customers")
        assert "customers" in variations
        assert "customer" in variations  # Singular
    
    def test_normalize_name_empty_string(self):
        """_normalize_name should handle empty strings."""
        ontology_config = {"entities": {"definitions": {}}}
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        
        variations = discoverer._normalize_name("")
        assert variations == []


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
    
    def test_create_entities_table_has_required_columns(self, builder, mock_spark):
        builder.create_entities_table()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "entity_id" in call_arg
        assert "entity_name" in call_arg
        assert "entity_type" in call_arg
        assert "confidence" in call_arg
        assert "auto_discovered" in call_arg
    
    def test_create_metrics_table(self, builder, mock_spark):
        builder.create_metrics_table()
        mock_spark.sql.assert_called()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "ontology_metrics" in call_arg
    
    def test_create_metrics_table_has_stub_columns(self, builder, mock_spark):
        """Metrics table should have columns for future UC metric views."""
        builder.create_metrics_table()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "sql_definition" in call_arg
        assert "uc_view_name" in call_arg


class TestBuildOntology:
    """Tests for build_ontology function."""
    
    @patch('src.dbxmetagen.ontology.OntologyBuilder')
    def test_creates_builder_with_correct_config(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"entities_discovered": 5, "entity_types": 3}
        mock_builder_class.return_value = mock_builder
        
        mock_spark = MagicMock()
        build_ontology(mock_spark, "my_cat", "my_sch")
        
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('src.dbxmetagen.ontology.OntologyBuilder')
    def test_passes_config_path(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {}
        mock_builder_class.return_value = mock_builder
        
        build_ontology(MagicMock(), "cat", "sch", config_path="custom/path.yaml")
        
        config = mock_builder_class.call_args[0][1]
        assert config.config_path == "custom/path.yaml"
    
    @patch('src.dbxmetagen.ontology.OntologyBuilder')
    def test_returns_run_result(self, mock_builder_class):
        expected = {"entities_discovered": 10, "entity_types": 4, "edges_added": 10}
        mock_builder = MagicMock()
        mock_builder.run.return_value = expected
        mock_builder_class.return_value = mock_builder
        
        result = build_ontology(MagicMock(), "cat", "sch")
        assert result == expected


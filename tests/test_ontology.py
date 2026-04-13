"""Unit tests for ontology module."""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from dbxmetagen.ontology import (
    OntologyConfig,
    OntologyLoader,
    EntityDefinition,
    EntityDiscoverer,
    EntityClassificationResult,
    OntologyBuilder,
    EdgeCatalog,
    EdgeCatalogEntry,
    PropertyDefinition,
    build_ontology,
    _enforce_entity_value,
    _resolve_ontology_config_path,
    _DEFAULT_ONTOLOGY_CONFIG_PATH,
    DEFAULT_CLASSIFICATION_MODEL,
    DOMAIN_ENTITY_AFFINITY,
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
        assert "Person" in definitions
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
        return MagicMock()
    
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
        assert confidence > 0.4
    
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
    
    def test_default_classification_model(self):
        """Default classification model should be claude-sonnet-4-6."""
        assert DEFAULT_CLASSIFICATION_MODEL == "databricks-claude-sonnet-4-6"

    def test_model_endpoint_from_config(self):
        """Discoverer should read classification_model from validation config."""
        ontology_config = {
            "entities": {"definitions": {}},
            "validation": {"classification_model": "my-custom-model"},
        }
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        assert discoverer._model_endpoint == "my-custom-model"

    def test_model_endpoint_default(self):
        """Discoverer should fall back to DEFAULT_CLASSIFICATION_MODEL."""
        ontology_config = {"entities": {"definitions": {}}}
        discoverer = EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)
        assert discoverer._model_endpoint == DEFAULT_CLASSIFICATION_MODEL

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
        return MagicMock()
    
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
        mock_spark.table.return_value.schema.fields = []
        builder.create_entities_table()
        mock_spark.sql.assert_called()
        all_sql = [c[0][0] for c in mock_spark.sql.call_args_list]
        ddl = all_sql[0]
        assert "ontology_entities" in ddl
        assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_create_entities_table_has_required_columns(self, builder, mock_spark):
        mock_spark.table.return_value.schema.fields = []
        builder.create_entities_table()
        ddl = mock_spark.sql.call_args_list[0][0][0]
        assert "entity_id" in ddl
        assert "entity_name" in ddl
        assert "entity_type" in ddl
        assert "confidence" in ddl
        assert "auto_discovered" in ddl

    def test_create_entities_table_runs_confidence_cleanup(self, builder, mock_spark):
        mock_spark.table.return_value.schema.fields = []
        builder.create_entities_table()
        all_sql = [c[0][0] for c in mock_spark.sql.call_args_list]
        cleanup_sqls = [s for s in all_sql if "GREATEST" in s and "LEAST" in s]
        assert len(cleanup_sqls) == 1
        assert "confidence < 0.0 OR confidence > 1.0" in cleanup_sqls[0]

    def test_create_entities_table_cleanup_tolerates_failure(self, builder, mock_spark):
        mock_spark.table.return_value.schema.fields = []

        def side_effect(sql):
            if "GREATEST" in sql:
                raise Exception("table is empty")
            return MagicMock()

        mock_spark.sql.side_effect = side_effect
        builder.create_entities_table()
    
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


class TestStoreEntitiesConfidenceClamping:
    """Tests that _store_entities clamps confidence to [0, 1]."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def _extract_rows(self, builder):
        return builder.spark.createDataFrame.call_args[0][0]

    def test_negative_confidence_clamped_to_zero(self, builder):
        entities = [{
            "entity_id": "e1", "entity_type": "Patient",
            "source_tables": ["t1"], "confidence": -0.5,
        }]
        builder._store_entities(entities)
        rows = self._extract_rows(builder)
        assert rows[0][7] == 0.0  # confidence field

    def test_confidence_above_one_clamped(self, builder):
        entities = [{
            "entity_id": "e2", "entity_type": "Order",
            "source_tables": ["t2"], "confidence": 1.5,
        }]
        builder._store_entities(entities)
        rows = self._extract_rows(builder)
        assert rows[0][7] == 1.0

    def test_valid_confidence_unchanged(self, builder):
        entities = [{
            "entity_id": "e3", "entity_type": "Product",
            "source_tables": ["t3"], "confidence": 0.75,
        }]
        builder._store_entities(entities)
        rows = self._extract_rows(builder)
        assert rows[0][7] == 0.75

    def test_discovery_confidence_clamped(self, builder):
        entities = [{
            "entity_id": "e4", "entity_type": "Event",
            "source_tables": ["t4"], "confidence": 0.8,
            "discovery_confidence": -0.3,
        }]
        builder._store_entities(entities)
        rows = self._extract_rows(builder)
        assert rows[0][8] == 0.0  # discovery_confidence field


class TestBuildOntology:
    """Tests for build_ontology function."""
    
    @patch('dbxmetagen.ontology.OntologyBuilder')
    def test_creates_builder_with_correct_config(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"entities_discovered": 5, "entity_types": 3}
        mock_builder_class.return_value = mock_builder
        
        mock_spark = MagicMock()
        build_ontology(mock_spark, "my_cat", "my_sch")
        
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('dbxmetagen.ontology.OntologyBuilder')
    def test_passes_config_path(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {}
        mock_builder_class.return_value = mock_builder
        
        build_ontology(MagicMock(), "cat", "sch", config_path="custom/path.yaml")
        
        config = mock_builder_class.call_args[0][1]
        assert config.config_path == "custom/path.yaml"
    
    @patch('dbxmetagen.ontology.OntologyBuilder')
    def test_returns_run_result(self, mock_builder_class):
        expected = {"entities_discovered": 10, "entity_types": 4, "edges_added": 10}
        mock_builder = MagicMock()
        mock_builder.run.return_value = expected
        mock_builder_class.return_value = mock_builder
        
        result = build_ontology(MagicMock(), "cat", "sch")
        assert result == expected


class TestEnforceEntityValue:
    """Tests for _enforce_entity_value."""

    def test_exact_match(self):
        val, exact = _enforce_entity_value("Patient", ["Patient", "Provider"])
        assert val == "Patient"
        assert exact is True

    def test_case_insensitive_match(self):
        val, exact = _enforce_entity_value("patient", ["Patient", "Provider"])
        assert val == "Patient"
        assert exact is True

    def test_partial_match(self):
        val, exact = _enforce_entity_value("patient_record", ["Patient", "Provider"])
        assert val == "Patient"
        assert exact is False

    def test_no_match_returns_fallback(self):
        val, exact = _enforce_entity_value("xyz_unknown", ["Patient", "Provider"])
        assert val == "DataTable"
        assert exact is False

    def test_custom_fallback(self):
        val, exact = _enforce_entity_value("xyz", ["Patient"], fallback="Other")
        assert val == "Other"


class TestEntityClassificationResult:
    """Tests for the Pydantic structured output model."""

    def test_basic_creation(self):
        r = EntityClassificationResult(
            entity_type="Patient", confidence=0.9, reasoning="matches keywords"
        )
        assert r.entity_type == "Patient"
        assert r.secondary_entity_type is None
        assert r.recommended_entity is None

    def test_multi_entity(self):
        r = EntityClassificationResult(
            entity_type="Patient",
            secondary_entity_type="Encounter",
            confidence=0.85,
            reasoning="relationship table",
        )
        assert r.secondary_entity_type == "Encounter"

    def test_recommended_entity(self):
        r = EntityClassificationResult(
            entity_type="DataTable",
            confidence=0.3,
            recommended_entity="ClinicalTrial",
            reasoning="low confidence",
        )
        assert r.recommended_entity == "ClinicalTrial"


class TestDomainEntityAffinity:
    """Tests for the domain-to-entity affinity map."""

    def test_healthcare_has_patient(self):
        assert "Patient" in DOMAIN_ENTITY_AFFINITY["healthcare"]

    def test_finance_has_transaction(self):
        assert "Transaction" in DOMAIN_ENTITY_AFFINITY["finance"]


class TestKeywordPrefilter:
    """Tests for EntityDiscoverer._keyword_prefilter."""

    def _make_discoverer(self):
        ontology_config = OntologyLoader._default_config()
        return EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)

    def test_returns_candidates(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("patient_records", "medical records", "healthcare")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_domain_boosts_relevant(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("data_table", "", "healthcare", top_n=5)
        healthcare_entities = DOMAIN_ENTITY_AFFINITY["healthcare"]
        boosted = [r for r in result if r in healthcare_entities]
        assert len(boosted) > 0

    def test_top_n_limits_results(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("something", "", "unknown", top_n=3)
        assert len(result) == 3


class TestDeduplicateEntities:
    """Tests for EntityDiscoverer.deduplicate_entities."""

    def test_merges_same_table_entity(self):
        entities = [
            {
                "entity_id": "1", "entity_type": "Patient", "entity_name": "Patient",
                "source_tables": ["t1"], "source_columns": ["col_a"],
                "attributes": {"granularity": "column", "discovery_method": "keyword"},
                "confidence": 0.7, "validation_notes": None,
            },
            {
                "entity_id": "2", "entity_type": "Patient", "entity_name": "Patient",
                "source_tables": ["t1"], "source_columns": ["col_b"],
                "attributes": {"granularity": "column", "discovery_method": "ai"},
                "confidence": 0.9, "validation_notes": "high conf",
            },
        ]
        result = EntityDiscoverer.deduplicate_entities(entities)
        assert len(result) == 1
        assert result[0]["confidence"] == 0.9
        assert set(result[0]["source_columns"]) == {"col_a", "col_b"}

    def test_keeps_different_entity_types(self):
        entities = [
            {
                "entity_id": "1", "entity_type": "Patient", "entity_name": "Patient",
                "source_tables": ["t1"], "source_columns": [],
                "attributes": {"granularity": "table"}, "confidence": 0.8,
                "validation_notes": None,
            },
            {
                "entity_id": "2", "entity_type": "Encounter", "entity_name": "Encounter",
                "source_tables": ["t1"], "source_columns": [],
                "attributes": {"granularity": "table"}, "confidence": 0.7,
                "validation_notes": None,
            },
        ]
        result = EntityDiscoverer.deduplicate_entities(entities)
        assert len(result) == 2


# ==============================================================================
# New tests for two-layer ontology improvements
# ==============================================================================


class TestEntityDefinitionRelationships:
    """Tests for EntityDefinition.relationships field."""

    def test_default_relationships_empty(self):
        entity = EntityDefinition(name="Test", description="Test")
        assert entity.relationships == {}

    def test_relationships_set_on_creation(self):
        rels = {
            "treated_by": {"target": "Provider", "cardinality": "many-to-many"},
            "has_encounter": {"target": "Encounter", "cardinality": "one-to-many"},
        }
        entity = EntityDefinition(
            name="Patient", description="A patient", relationships=rels
        )
        assert len(entity.relationships) == 2
        assert entity.relationships["treated_by"]["target"] == "Provider"
        assert entity.relationships["has_encounter"]["cardinality"] == "one-to-many"


class TestRelationshipsParsing:
    """Tests for OntologyLoader.get_entity_definitions relationship parsing."""

    def test_new_relationships_format(self):
        config = {
            "entities": {
                "definitions": {
                    "Patient": {
                        "description": "Patient entity",
                        "keywords": ["patient"],
                        "relationships": {
                            "treated_by": {"target": "Provider", "cardinality": "many-to-many"},
                            "has_encounter": {"target": "Encounter", "cardinality": "one-to-many"},
                        },
                    }
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        assert len(entities) == 1
        p = entities[0]
        assert len(p.relationships) == 2
        assert p.relationships["treated_by"]["target"] == "Provider"
        assert p.relationships["has_encounter"]["cardinality"] == "one-to-many"

    def test_old_typical_relationships_backward_compat(self):
        config = {
            "entities": {
                "definitions": {
                    "Person": {
                        "description": "A person",
                        "keywords": ["person"],
                        "typical_relationships": ["owns", "contains"],
                    }
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        p = entities[0]
        assert "owns" in p.relationships
        assert "contains" in p.relationships
        assert p.relationships["owns"] == {}

    def test_new_format_takes_precedence(self):
        config = {
            "entities": {
                "definitions": {
                    "Order": {
                        "description": "An order",
                        "keywords": ["order"],
                        "typical_relationships": ["references"],
                        "relationships": {
                            "placed_by": {"target": "Customer", "cardinality": "many-to-one"},
                        },
                    }
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        o = entities[0]
        assert "placed_by" in o.relationships
        assert "references" not in o.relationships

    def test_no_relationships_field(self):
        config = {
            "entities": {
                "definitions": {
                    "Ref": {"description": "Lookup", "keywords": ["ref"]}
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        assert entities[0].relationships == {}

    def test_empty_relationships_map(self):
        config = {
            "entities": {
                "definitions": {
                    "Ref": {
                        "description": "Lookup",
                        "keywords": ["ref"],
                        "relationships": {},
                    }
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        assert entities[0].relationships == {}


class TestBestAttributeForColumn:
    """Tests for EntityDiscoverer._best_attribute_for_column."""

    def test_exact_match(self):
        edef = EntityDefinition(
            name="Patient", description="Patient",
            typical_attributes=["id", "mrn", "name", "dob"]
        )
        assert EntityDiscoverer._best_attribute_for_column("mrn", edef) == "mrn"

    def test_prefix_stripped(self):
        edef = EntityDefinition(
            name="Patient", description="Patient",
            typical_attributes=["id", "mrn", "name", "dob"]
        )
        assert EntityDiscoverer._best_attribute_for_column("patient_mrn", edef) == "mrn"

    def test_partial_match(self):
        edef = EntityDefinition(
            name="Order", description="Order",
            typical_attributes=["id", "customer_id", "total", "status"]
        )
        result = EntityDiscoverer._best_attribute_for_column("order_status", edef)
        assert result == "status"

    def test_no_match_returns_stripped_name(self):
        edef = EntityDefinition(
            name="Patient", description="Patient",
            typical_attributes=["id", "mrn"]
        )
        result = EntityDiscoverer._best_attribute_for_column("patient_custom_field", edef)
        assert result == "custom_field"

    def test_no_prefix_no_match(self):
        edef = EntityDefinition(
            name="Patient", description="Patient",
            typical_attributes=["id", "mrn"]
        )
        result = EntityDiscoverer._best_attribute_for_column("random_col", edef)
        assert result == "random_col"


class TestDeclaredRelationshipLookup:
    """Tests for discover_inter_entity_relationships declared relationship lookup logic."""

    def _make_builder(self, entity_defs, ontology_config=None):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = ontology_config or OntologyLoader._default_config()
            builder = OntologyBuilder(mock_spark, config)
        builder.discoverer.entity_definitions = entity_defs
        return builder, mock_spark

    def test_declared_rel_used_over_references(self):
        """When a declared relationship matches (src_type, dst_type), use it."""
        edefs = [
            EntityDefinition(
                name="Patient", description="Patient", keywords=["patient"],
                relationships={"treated_by": {"target": "Provider", "cardinality": "many-to-many"}},
            ),
            EntityDefinition(name="Provider", description="Provider", keywords=["provider"]),
        ]
        builder, mock_spark = self._make_builder(edefs)

        ent_row1 = MagicMock()
        ent_row1.entity_id, ent_row1.entity_name, ent_row1.entity_type = "e1", "Patient", "Patient"
        ent_row1.source_tables = ["cat.sch.patients"]
        ent_row2 = MagicMock()
        ent_row2.entity_id, ent_row2.entity_name, ent_row2.entity_type = "e2", "Provider", "Provider"
        ent_row2.source_tables = ["cat.sch.providers"]

        fk_row = MagicMock()
        fk_row.src_table, fk_row.dst_table = "cat.sch.patients", "cat.sch.providers"

        sql_results = [
            MagicMock(collect=MagicMock(return_value=[ent_row1, ent_row2])),
            MagicMock(collect=MagicMock(return_value=[fk_row])),
            MagicMock(),  # DELETE FROM edges
            MagicMock(),  # INSERT INTO edges (_insert_edges_safe)
        ]
        mock_spark.sql.side_effect = sql_results

        result = builder.discover_inter_entity_relationships()
        assert result["edges_added"] == 1
        df = mock_spark.createDataFrame.call_args
        if df:
            rows = df[0][0]
            assert rows[0][2] == "treated_by"

    def test_undiscovered_declared_reported(self):
        """Declared relationships with no matching FK should be reported."""
        edefs = [
            EntityDefinition(
                name="Patient", description="Patient",
                relationships={
                    "treated_by": {"target": "Provider", "cardinality": "many-to-many"},
                    "has_condition": {"target": "Condition", "cardinality": "one-to-many"},
                },
            ),
            EntityDefinition(name="Provider", description="Provider"),
            EntityDefinition(name="Condition", description="Condition"),
        ]
        builder, mock_spark = self._make_builder(edefs)

        # No entities discovered, no FKs
        mock_spark.sql.return_value.collect.return_value = []

        result = builder.discover_inter_entity_relationships()
        assert len(result["undiscovered_declared"]) == 2
        names = {u["relationship"] for u in result["undiscovered_declared"]}
        assert "treated_by" in names
        assert "has_condition" in names

    def test_fallback_to_references(self):
        """When no declaration matches, fall back to 'references'."""
        edefs = [
            EntityDefinition(name="Event", description="Event"),
            EntityDefinition(name="Ref", description="Reference"),
        ]
        builder, mock_spark = self._make_builder(edefs)

        ent_row1 = MagicMock()
        ent_row1.entity_id, ent_row1.entity_name, ent_row1.entity_type = "e1", "Event", "Event"
        ent_row1.source_tables = ["tbl_a"]
        ent_row2 = MagicMock()
        ent_row2.entity_id, ent_row2.entity_name, ent_row2.entity_type = "e2", "Ref", "Ref"
        ent_row2.source_tables = ["tbl_b"]

        fk_row = MagicMock()
        fk_row.src_table, fk_row.dst_table = "tbl_a", "tbl_b"

        sql_results = [
            MagicMock(collect=MagicMock(return_value=[ent_row1, ent_row2])),
            MagicMock(collect=MagicMock(return_value=[fk_row])),
            MagicMock(),  # DELETE FROM edges
            MagicMock(),  # INSERT INTO edges (_insert_edges_safe)
        ]
        mock_spark.sql.side_effect = sql_results

        result = builder.discover_inter_entity_relationships()
        assert result["edges_added"] == 1
        df = mock_spark.createDataFrame.call_args
        if df:
            rows = df[0][0]
            assert rows[0][2] == "references"


class TestBundleYAMLRelationships:
    """Tests that bundle YAML files contain structured relationships."""

    @pytest.fixture(params=["healthcare", "general", "retail_cpg", "financial_services"])
    def bundle_name(self, request):
        return request.param

    def test_bundle_has_relationships(self, bundle_name):
        """Every entity in every bundle should have a relationships key."""
        import os, yaml
        bundle_dir = os.path.join(
            os.path.dirname(__file__), "..", "configurations", "ontology_bundles"
        )
        path = os.path.join(bundle_dir, f"{bundle_name}.yaml")
        with open(path) as f:
            data = yaml.safe_load(f)
        definitions = data.get("ontology", {}).get("entities", {}).get("definitions", {})
        assert len(definitions) > 0, f"No definitions in {bundle_name}"
        for name, defn in definitions.items():
            assert "relationships" in defn, (
                f"Entity '{name}' in bundle '{bundle_name}' missing relationships key"
            )
            assert isinstance(defn["relationships"], dict), (
                f"Entity '{name}' relationships should be a dict"
            )

    def test_relationship_values_have_target(self, bundle_name):
        """Non-empty relationships should declare a target entity."""
        import os, yaml
        bundle_dir = os.path.join(
            os.path.dirname(__file__), "..", "configurations", "ontology_bundles"
        )
        path = os.path.join(bundle_dir, f"{bundle_name}.yaml")
        with open(path) as f:
            data = yaml.safe_load(f)
        definitions = data["ontology"]["entities"]["definitions"]
        for name, defn in definitions.items():
            for rel_name, rel_info in defn.get("relationships", {}).items():
                assert isinstance(rel_info, dict), (
                    f"{name}.{rel_name} should be a dict with target/cardinality"
                )
                assert "target" in rel_info, (
                    f"{name}.{rel_name} missing 'target'"
                )
                assert "cardinality" in rel_info, (
                    f"{name}.{rel_name} missing 'cardinality'"
                )


# ======================================================================
# _store_entities MERGE update + _purge_stale_bundle_entities
# ======================================================================


class TestStoreEntitiesMergeUpdate:
    """Tests that _store_entities emits a MERGE with WHEN MATCHED THEN UPDATE."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def test_merge_contains_when_matched(self, builder):
        entities = [{"entity_id": "e1", "entity_type": "T", "source_tables": ["t1"], "confidence": 0.8}]
        builder._store_entities(entities)
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        merge_sql = [s for s in sql_calls if "MERGE INTO" in s]
        assert len(merge_sql) == 1
        assert "WHEN MATCHED" in merge_sql[0]
        assert "auto_discovered = TRUE" in merge_sql[0]
        assert "validated = FALSE" in merge_sql[0]

    def test_merge_does_not_overwrite_validated(self, builder):
        """WHEN MATCHED guard ensures validated entities are never overwritten."""
        entities = [{"entity_id": "e1", "entity_type": "T", "source_tables": ["t1"], "confidence": 0.9}]
        builder._store_entities(entities)
        merge_sql = [c[0][0] for c in builder.spark.sql.call_args_list if "MERGE" in c[0][0]][0]
        assert "validated = FALSE" in merge_sql

    def test_merge_updates_expected_columns(self, builder):
        entities = [{"entity_id": "e1", "entity_type": "T", "source_tables": ["t1"], "confidence": 0.5}]
        builder._store_entities(entities)
        merge_sql = [c[0][0] for c in builder.spark.sql.call_args_list if "MERGE" in c[0][0]][0]
        for col in ("confidence", "source_columns", "attributes", "column_bindings", "bundle_version", "updated_at"):
            assert f"target.{col} = source.{col}" in merge_sql


class TestPurgeStaleBundleEntities:
    """Tests for _purge_stale_bundle_entities."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", ontology_bundle="general")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def test_purge_emits_delete_sql(self, builder):
        mock_result = MagicMock()
        mock_result.first.return_value = [3]
        builder.spark.sql.return_value = mock_result

        builder._purge_stale_bundle_entities("general")
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        delete_sqls = [s for s in sql_calls if "DELETE FROM" in s]
        assert len(delete_sqls) == 1
        assert "ontology_bundle != 'general'" in delete_sqls[0]
        assert "auto_discovered = TRUE" in delete_sqls[0]
        assert "validated = FALSE" in delete_sqls[0]

    def test_purge_skips_when_no_bundle(self, builder):
        count = builder._purge_stale_bundle_entities("")
        assert count == 0
        builder.spark.sql.assert_not_called()

    def test_purge_preserves_validated_entities(self, builder):
        mock_result = MagicMock()
        mock_result.first.return_value = [0]
        builder.spark.sql.return_value = mock_result

        builder._purge_stale_bundle_entities("general")
        sql = builder.spark.sql.call_args[0][0]
        assert "validated = FALSE" in sql

    def test_discover_and_store_calls_purge_when_bundle_set(self, builder):
        builder.discoverer.discover_entities_from_tables = MagicMock(return_value=[])
        builder.discover_and_store_entities()
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        delete_sqls = [s for s in sql_calls if "DELETE FROM" in s]
        assert len(delete_sqls) == 1

    def test_discover_and_store_skips_purge_when_no_bundle(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", ontology_bundle=None)
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            builder = OntologyBuilder(mock_spark, config)
        builder.discoverer.discover_entities_from_tables = MagicMock(return_value=[])
        builder.discover_and_store_entities()
        sql_calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        delete_sqls = [s for s in sql_calls if "DELETE FROM" in s]
        assert len(delete_sqls) == 0


# ======================================================================
# Bundle version format alignment and mismatch warning
# ======================================================================


class TestBundleVersionFormat:
    """OntologyBuilder._get_bundle_version must match EntityDiscoverer format."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", ontology_bundle="fhir_r4")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            cfg = OntologyLoader._default_config()
            cfg.setdefault("metadata", {})["version"] = "2.0"
            mock_load.return_value = cfg
            b = OntologyBuilder(mock_spark, config)
        return b

    @pytest.fixture
    def discoverer(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", ontology_bundle="fhir_r4")
        ontology_config = OntologyLoader._default_config()
        ontology_config.setdefault("metadata", {})["version"] = "2.0"
        return EntityDiscoverer(mock_spark, config, ontology_config)

    def test_builder_includes_bundle_name(self, builder):
        bv = builder._get_bundle_version()
        assert bv.startswith("fhir_r4:")

    def test_builder_and_discoverer_formats_match(self, builder, discoverer):
        assert builder._get_bundle_version() == discoverer._get_bundle_version()

    def test_default_bundle_fallback(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", ontology_bundle="")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        assert b._get_bundle_version().startswith("default:")


class TestBundleSwitchWarning:
    """run() should warn when entities from a different bundle exist."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", ontology_bundle="omop_cdm")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def test_warns_on_bundle_switch(self, builder, caplog):
        import logging

        call_count = [0]

        def sql_side_effect(query):
            call_count[0] += 1
            result = MagicMock()
            if "MAX(bundle_version)" in query:
                row = MagicMock()
                row.v = "fhir_r4:1.0"
                result.collect.return_value = [row]
                return result
            if "DISTINCT ontology_bundle" in query:
                row = MagicMock()
                row.ontology_bundle = "fhir_r4"
                result.collect.return_value = [row]
                return result
            # All subsequent calls: abort the run early
            raise StopIteration("short-circuit")

        builder.spark.sql.side_effect = sql_side_effect

        with caplog.at_level(logging.WARNING, logger="dbxmetagen.ontology"):
            try:
                builder.run()
            except StopIteration:
                pass

        assert any("bundle(s)" in m and "omop_cdm" in m for m in caplog.messages)

    def test_no_warning_when_same_bundle(self, builder, caplog):
        import logging

        builder.config = OntologyConfig(
            catalog_name="cat", schema_name="sch", ontology_bundle="fhir_r4"
        )

        def sql_side_effect(query):
            result = MagicMock()
            if "MAX(bundle_version)" in query:
                row = MagicMock()
                row.v = "fhir_r4:1.0"
                result.collect.return_value = [row]
                return result
            if "DISTINCT ontology_bundle" in query:
                row = MagicMock()
                row.ontology_bundle = "fhir_r4"
                result.collect.return_value = [row]
                return result
            raise StopIteration("short-circuit")

        builder.spark.sql.side_effect = sql_side_effect

        with caplog.at_level(logging.WARNING, logger="dbxmetagen.ontology"):
            try:
                builder.run()
            except StopIteration:
                pass

        assert not any("bundle(s)" in m for m in caplog.messages)

    def test_no_warning_on_empty_table(self, builder, caplog):
        import logging

        def sql_side_effect(query):
            result = MagicMock()
            if "MAX(bundle_version)" in query:
                row = MagicMock()
                row.v = None
                result.collect.return_value = [row]
                return result
            if "DISTINCT ontology_bundle" in query:
                result.collect.return_value = []
                return result
            raise StopIteration("short-circuit")

        builder.spark.sql.side_effect = sql_side_effect

        with caplog.at_level(logging.WARNING, logger="dbxmetagen.ontology"):
            try:
                builder.run()
            except StopIteration:
                pass

        assert not any("bundle(s)" in m for m in caplog.messages)


# ======================================================================
# _heuristic_classify improvements
# ======================================================================


class TestHeuristicClassifyImprovements:
    """Tests for self-referencing PK detection and downgraded _id/_key confidence."""

    @pytest.fixture
    def classifier(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            builder = OntologyBuilder(mock_spark, config)
        return builder

    def test_self_referencing_pk_order_id_on_orders(self, classifier):
        """order_id on cat.sch.orders -> primary_key, not object_property."""
        role, method, conf = classifier._heuristic_classify(
            "order_id", "BIGINT", False, None, "", table_name="cat.sch.orders",
        )
        assert role == "primary_key"
        assert conf >= 0.80

    def test_self_referencing_pk_customer_key_on_customers(self, classifier):
        """customer_key on cat.sch.customers -> primary_key."""
        role, method, conf = classifier._heuristic_classify(
            "customer_key", "BIGINT", False, None, "", table_name="cat.sch.customers",
        )
        assert role == "primary_key"

    def test_foreign_key_customer_id_on_orders(self, classifier):
        """customer_id on cat.sch.orders -> object_property (not self-referencing)."""
        role, method, conf = classifier._heuristic_classify(
            "customer_id", "BIGINT", False, None, "", table_name="cat.sch.orders",
        )
        assert role == "object_property"
        assert conf == 0.55
        assert method == "heuristic_weak"

    def test_ambiguous_id_column_low_confidence(self, classifier):
        """Generic _id column without linked_entity gets low confidence."""
        role, method, conf = classifier._heuristic_classify(
            "region_id", "INT", False, None, "", table_name="cat.sch.orders",
        )
        assert role == "object_property"
        assert conf == 0.55
        assert method == "heuristic_weak"

    def test_linked_entity_still_high_confidence(self, classifier):
        """Column with linked_entity should remain object_property at 0.80."""
        role, method, conf = classifier._heuristic_classify(
            "provider_id", "BIGINT", False, "Provider", "", table_name="cat.sch.encounters",
        )
        assert role == "object_property"
        assert conf == 0.80
        assert method == "heuristic_strong"

    def test_explicit_pk_column_still_primary_key(self, classifier):
        """Column already in entity source_columns (is_pk_column=True) -> primary_key."""
        role, method, conf = classifier._heuristic_classify(
            "patient_id", "BIGINT", True, None, "", table_name="cat.sch.encounters",
        )
        assert role == "primary_key"
        assert conf == 0.85

    def test_no_table_name_falls_back_to_object_property(self, classifier):
        """Without table_name, can't detect self-referencing PK."""
        role, method, conf = classifier._heuristic_classify(
            "order_id", "BIGINT", False, None, "",
        )
        assert role == "object_property"
        assert conf == 0.55

    def test_plural_table_strip(self, classifier):
        """Plural table name: patient_id on patients -> primary_key."""
        role, _, _ = classifier._heuristic_classify(
            "patient_id", "BIGINT", False, None, "", table_name="cat.sch.patients",
        )
        assert role == "primary_key"


class TestHeuristicNeverReturnsLink:
    """Belt-and-suspenders: verify _heuristic_classify never returns 'link'."""

    @pytest.fixture
    def classifier(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            builder = OntologyBuilder(mock_spark, config)
        return builder

    _REPRESENTATIVE_COLUMNS = [
        ("order_id", "BIGINT", False, None, "", "cat.sch.orders"),
        ("customer_id", "BIGINT", False, None, "", "cat.sch.orders"),
        ("patient_id", "BIGINT", True, None, "", "cat.sch.encounters"),
        ("provider_id", "BIGINT", False, "Provider", "", "cat.sch.encounters"),
        ("name", "STRING", False, None, "", "cat.sch.customers"),
        ("amount", "DECIMAL", False, None, "", "cat.sch.transactions"),
        ("created_at", "TIMESTAMP", False, None, "", "cat.sch.logs"),
        ("country_code", "STRING", False, None, "", "cat.sch.addresses"),
        ("is_active", "BOOLEAN", False, None, "", "cat.sch.users"),
        ("ssn", "STRING", False, None, "PII", "cat.sch.patients"),
        ("icd_code", "STRING", False, None, "", "cat.sch.diagnoses"),
        ("etl_timestamp", "TIMESTAMP", False, None, "", "cat.sch.raw"),
        ("notes", "STRING", False, None, "", "cat.sch.encounters"),
        ("year", "INT", False, None, "", "cat.sch.dim_time"),
        ("unknown_field", "STRUCT", False, None, "", "cat.sch.misc"),
    ]

    @pytest.mark.parametrize("col,dtype,is_pk,linked,cls_type,tbl", _REPRESENTATIVE_COLUMNS)
    def test_never_returns_link(self, classifier, col, dtype, is_pk, linked, cls_type, tbl):
        role, method, conf = classifier._heuristic_classify(col, dtype, is_pk, linked, cls_type, table_name=tbl)
        assert role != "link", f"_heuristic_classify returned 'link' for {col} on {tbl}"


class TestEdgeCatalogInverse:
    """Tests for EdgeCatalog inverse lookup and validation."""

    def test_get_inverse_returns_name(self):
        entries = {
            "placed_by": EdgeCatalogEntry(name="placed_by", inverse="placed", domain="Transaction", range="Person"),
        }
        catalog = EdgeCatalog(entries)
        assert catalog.get_inverse("placed_by") == "placed"

    def test_get_inverse_returns_none_when_missing(self):
        entries = {
            "references": EdgeCatalogEntry(name="references"),
        }
        catalog = EdgeCatalog(entries)
        assert catalog.get_inverse("references") is None

    def test_get_inverse_unknown_edge(self):
        catalog = EdgeCatalog({})
        assert catalog.get_inverse("nonexistent") is None

    def test_validate_edge_domain_range(self):
        entries = {
            "placed_by": EdgeCatalogEntry(name="placed_by", inverse="placed", domain="Transaction", range="Person"),
        }
        catalog = EdgeCatalog(entries)
        valid, _ = catalog.validate("placed_by", "Transaction", "Person")
        assert valid
        valid2, msg = catalog.validate("placed_by", "Person", "Transaction")
        assert not valid2

    def test_find_edge_by_domain_range(self):
        entries = {
            "belongs_to": EdgeCatalogEntry(name="belongs_to", domain="Product", range="Organization"),
        }
        catalog = EdgeCatalog(entries)
        match = catalog.find_edge("Product", "Organization")
        assert match is not None
        assert match.name == "belongs_to"
        assert catalog.find_edge("Organization", "Product") is None


class TestResolveEdgeName:
    """Tests for _resolve_edge_name with bundle property edge definitions."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            builder = OntologyBuilder(mock_spark, config)

        edef = EntityDefinition(
            name="Transaction",
            description="A business transaction",
            parent=None,
            keywords=["order"],
            properties=[
                PropertyDefinition(
                    name="placed_by",
                    kind="object_property",
                    role="object_property",
                    typical_attributes=["customer_id", "buyer_id"],
                    edge="placed_by",
                    target_entity="Person",
                ),
            ],
            relationships={"contains": {"target": "Product"}},
        )
        builder.discoverer._entity_def_map = {"Transaction": edef}
        builder.discoverer._edge_catalog = EdgeCatalog({
            "placed_by": EdgeCatalogEntry(
                name="placed_by", inverse="placed",
                domain="Transaction", range="Person",
            ),
            "contains": EdgeCatalogEntry(
                name="contains", inverse="contained_in",
                domain="Transaction", range="Product",
            ),
        })
        return builder

    def test_bundle_property_edge_match(self, builder):
        """Column matching bundle property with edge -> returns that edge name."""
        name = builder._resolve_edge_name("Transaction", "Person", "customer_id")
        assert name == "placed_by"

    def test_legacy_relationship_block(self, builder):
        """No column match but legacy relationships block has target -> returns that name."""
        name = builder._resolve_edge_name("Transaction", "Product", "product_id")
        assert name == "contains"

    def test_fallback_to_references(self, builder):
        """No match at all -> returns 'references'."""
        name = builder._resolve_edge_name("Transaction", "Location", "location_id")
        assert name == "references"


class TestBuildBundlePropertyIndex:
    """Tests for _build_bundle_property_index bundle-match tier."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            builder = OntologyBuilder(mock_spark, config)

        edef = EntityDefinition(
            name="Transaction",
            description="A business transaction",
            parent=None,
            keywords=["order"],
            properties=[
                PropertyDefinition(
                    name="amount", kind="data_property", role="measure",
                    typical_attributes=["total_amount", "order_amount", "amount"],
                ),
                PropertyDefinition(
                    name="placed_by", kind="object_property", role="object_property",
                    typical_attributes=["customer_id", "buyer_id"],
                    edge="placed_by", target_entity="Person",
                ),
            ],
            relationships={},
        )
        builder.discoverer._entity_def_map = {"Transaction": edef}
        return builder

    def test_index_maps_typical_attributes(self, builder):
        index = builder._build_bundle_property_index("Transaction")
        assert "total_amount" in index
        assert index["total_amount"][0] == "measure"
        assert "customer_id" in index
        assert index["customer_id"][0] == "object_property"
        assert index["customer_id"][2] == "placed_by"

    def test_index_empty_for_unknown_entity(self, builder):
        assert builder._build_bundle_property_index("Unknown") == {}

    def test_index_case_insensitive(self, builder):
        index = builder._build_bundle_property_index("Transaction")
        assert "total_amount" in index
        assert "TOTAL_AMOUNT" not in index


# ======================================================================
# Extended _enforce_entity_value tests (parity with domain _enforce_value)
# ======================================================================

ALL_ENTITY_TYPES = [
    "Person", "Organization", "Product", "Transaction", "Location",
    "Event", "Reference", "Metric", "Document", "Patient", "Provider",
    "Encounter", "Condition", "Procedure", "Medication", "Observation",
    "Claim", "Coverage", "DataTable",
]


class TestEnforceEntityValueExtended:
    """Adversarial and parametrized tests mirroring domain classifier depth."""

    # -- Exact match for every default entity type --
    @pytest.mark.parametrize("entity", ALL_ENTITY_TYPES)
    def test_exact_match_all_types(self, entity):
        val, exact = _enforce_entity_value(entity, ALL_ENTITY_TYPES)
        assert val == entity and exact is True

    # -- Case variations --
    @pytest.mark.parametrize("entity,predicted", [
        ("Patient", "PATIENT"),
        ("Patient", "patient"),
        ("Patient", "pATIENT"),
        ("Encounter", "encounter"),
        ("Encounter", "ENCOUNTER"),
        ("Organization", "organization"),
        ("Organization", "ORGANIZATION"),
        ("Transaction", "transaction"),
        ("Observation", "observation"),
        ("Medication", "MEDICATION"),
        ("Coverage", "coverage"),
    ])
    def test_case_variations(self, entity, predicted):
        val, exact = _enforce_entity_value(predicted, ALL_ENTITY_TYPES)
        assert val == entity and exact is True

    # -- LLM-realistic extended outputs (substring containment) --
    @pytest.mark.parametrize("predicted,expected", [
        ("Patient_Record", "Patient"),
        ("Patient_Demographics", "Patient"),
        ("Medical_Encounter", "Encounter"),
        ("Encounter_Visit", "Encounter"),
        ("Insurance_Claim", "Claim"),
        ("Claim_Submission", "Claim"),
        ("Lab_Observation", "Observation"),
        ("Observation_Result", "Observation"),
        ("Drug_Medication", "Medication"),
        ("Medication_Order", "Medication"),
        ("Healthcare_Provider", "Provider"),
        ("Provider_Practitioner", "Provider"),
        ("Business_Transaction", "Transaction"),
        ("Transaction_Record", "Transaction"),
        ("Product_Catalog", "Product"),
        ("Reference_Lookup", "Reference"),
        ("Metric_KPI", "Metric"),
        ("Document_Content", "Document"),
        ("Coverage_Plan", "Coverage"),
        ("Location_Address", "Location"),
        ("System_Event", "Event"),
        ("Surgical_Procedure", "Procedure"),
        ("Diagnosis_Condition", "Condition"),
    ])
    def test_llm_extended_names(self, predicted, expected):
        val, exact = _enforce_entity_value(predicted, ALL_ENTITY_TYPES)
        assert val == expected
        assert exact is False

    # -- Substring ambiguity: both entity names present in predicted string --
    @pytest.mark.parametrize("predicted", [
        "Provider_Organization",
        "Event_Location",
        "Product_Transaction",
        "Patient_Encounter",
        "Condition_Procedure",
    ])
    def test_ambiguous_substring_does_not_crash(self, predicted):
        """Ambiguous inputs must return a valid entity, never crash or fallback."""
        val, exact = _enforce_entity_value(predicted, ALL_ENTITY_TYPES)
        assert val in ALL_ENTITY_TYPES
        assert val != "DataTable"
        assert exact is False

    # -- Reverse containment (predicted is a prefix/substring of allowed) --
    @pytest.mark.parametrize("predicted,expected", [
        ("Med", "Medication"),
        ("Proc", "Procedure"),
        ("Obs", "Observation"),
        ("Cov", "Coverage"),
        ("Trans", "Transaction"),
        ("Org", "Organization"),
        ("Cond", "Condition"),
        ("Loc", "Location"),
        ("Ref", "Reference"),
        ("Prod", "Product"),
    ])
    def test_reverse_containment(self, predicted, expected):
        val, exact = _enforce_entity_value(predicted, ALL_ENTITY_TYPES)
        assert val == expected
        assert exact is False

    def test_reverse_containment_enc_is_ambiguous(self):
        """'enc' is a substring of both 'Reference' and 'Encounter';
        iteration order determines the winner. Verify it doesn't crash
        and returns a valid entity (not the fallback)."""
        val, exact = _enforce_entity_value("Enc", ALL_ENTITY_TYPES)
        assert val in ("Reference", "Encounter")
        assert exact is False

    # -- Fallback to DataTable for garbage inputs --
    @pytest.mark.parametrize("garbage", [
        "xyzzy_unknown",
        "12345",
        "definitely_not_an_entity",
        "banana_split_sundae",
        "asdfghjkl",
        "the quick brown fox",
    ])
    def test_garbage_returns_fallback(self, garbage):
        val, exact = _enforce_entity_value(garbage, ALL_ENTITY_TYPES)
        assert val == "DataTable"
        assert exact is False

    def test_empty_string_matches_first_via_substring(self):
        """Empty string is a substring of every allowed value, so the first
        entity wins via iteration order. This documents actual behavior."""
        val, exact = _enforce_entity_value("", ALL_ENTITY_TYPES)
        assert val in ALL_ENTITY_TYPES
        assert exact is False

    def test_whitespace_matches_first_via_substring(self):
        """Whitespace-only strips to empty, same as above."""
        val, exact = _enforce_entity_value("   ", ALL_ENTITY_TYPES)
        assert val in ALL_ENTITY_TYPES
        assert exact is False

    def test_custom_fallback(self):
        val, exact = _enforce_entity_value("xyzzy", ALL_ENTITY_TYPES, fallback="Other")
        assert val == "Other"
        assert exact is False

    def test_empty_allowed_list_returns_fallback(self):
        val, exact = _enforce_entity_value("Patient", [])
        assert val == "DataTable"
        assert exact is False

    def test_return_types_always_str_bool(self):
        """Contract: always returns (str, bool), never None."""
        val, exact = _enforce_entity_value("anything", ALL_ENTITY_TYPES)
        assert isinstance(val, str)
        assert isinstance(exact, bool)


# ======================================================================
# Negative tests: _enforce_entity_value never returns a wrong type
# ======================================================================


class TestEnforceEntityValueNeverReturnsWrongType:
    """Garbage strings must always map to the fallback, never a real entity."""

    ALLOWED = [e for e in ALL_ENTITY_TYPES if e != "DataTable"]

    @pytest.mark.parametrize("garbage", [
        "zebra_crossing_data",
        "foobar_baz_qux",
        "aaaabbbbcccc",
        "___---!!!",
        "lorem ipsum dolor sit amet",
        "1234567890",
        "SELECT * FROM",
        "null",
        "undefined",
        "NaN",
    ])
    def test_garbage_never_matches_real_entity(self, garbage):
        val, exact = _enforce_entity_value(garbage, self.ALLOWED)
        assert val == "DataTable", f"'{garbage}' should not match any entity, got '{val}'"
        assert exact is False


# ======================================================================
# Extended _keyword_prefilter tests
# ======================================================================


class TestKeywordPrefilterExtended:
    """Deeper coverage for entity prefiltering logic."""

    def _make_discoverer(self):
        ontology_config = OntologyLoader._default_config()
        return EntityDiscoverer(MagicMock(), MagicMock(), ontology_config)

    def test_patient_encounters_includes_patient(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("patient_encounters", "admission records", "healthcare")
        assert "Patient" in result

    def test_patient_encounters_includes_encounter(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("patient_encounters", "admission records", "healthcare")
        assert "Encounter" in result

    def test_lab_results_includes_observation(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("lab_results", "laboratory test results", "healthcare")
        assert "Observation" in result

    def test_claim_submissions_includes_claim(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("claim_submissions", "insurance claims", "healthcare")
        assert "Claim" in result

    def test_finance_domain_transactions(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("daily_transactions", "payment records", "finance")
        assert "Transaction" in result

    def test_unknown_domain_keyword_match(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("customer_orders", "order history", "unknown")
        entity_names = set(result)
        assert "Transaction" in entity_names or "Person" in entity_names

    def test_top_n_small_still_finds_strong_signal(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("patient_records", "medical records", "healthcare", top_n=2)
        assert len(result) == 2
        assert "Patient" in result

    def test_empty_inputs_do_not_crash(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("", "", "unknown", top_n=3)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_medication_table_includes_medication(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("prescription_drugs", "pharmacy medication orders", "healthcare")
        assert "Medication" in result

    def test_provider_table_includes_provider(self):
        d = self._make_discoverer()
        result = d._keyword_prefilter("physician_directory", "provider npi registry", "healthcare")
        assert "Provider" in result


# ======================================================================
# DEFAULT_CLASSIFICATION_MODEL consolidation verification
# ======================================================================


class TestClassificationModelConsolidation:
    """Verify the constant is the single source of truth across modules."""

    def test_config_and_ontology_share_same_constant(self):
        from dbxmetagen.config import DEFAULT_CLASSIFICATION_MODEL as CONFIG_MODEL
        from dbxmetagen.ontology import DEFAULT_CLASSIFICATION_MODEL as ONTOLOGY_MODEL
        assert CONFIG_MODEL == ONTOLOGY_MODEL

    def test_constant_is_claude_sonnet(self):
        from dbxmetagen.config import DEFAULT_CLASSIFICATION_MODEL
        assert DEFAULT_CLASSIFICATION_MODEL == "databricks-claude-sonnet-4-6"


# ======================================================================
# Enrich graph nodes with ontology data
# ======================================================================


class TestEnrichTableNodesWithOntology:
    """Tests for _enrich_table_nodes_with_ontology."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def test_issues_merge_into_graph_nodes(self, builder):
        mock_result = MagicMock()
        mock_result.collect.return_value = [MagicMock(cnt=5)]
        builder.spark.sql.return_value = mock_result
        count = builder._enrich_table_nodes_with_ontology()
        all_sql = [c[0][0] for c in builder.spark.sql.call_args_list]
        merge_sql = all_sql[0]
        assert "MERGE INTO" in merge_sql
        assert "graph_nodes" in merge_sql
        assert "ontology_id" in merge_sql
        assert "ontology_type" in merge_sql
        assert "node_type = 'table'" in merge_sql
        assert count == 5

    def test_filters_primary_entities_only(self, builder):
        mock_result = MagicMock()
        mock_result.collect.return_value = [MagicMock(cnt=0)]
        builder.spark.sql.return_value = mock_result
        builder._enrich_table_nodes_with_ontology()
        merge_sql = builder.spark.sql.call_args_list[0][0][0]
        assert "'primary'" in merge_sql

    def test_returns_zero_on_failure(self, builder):
        builder.spark.sql.side_effect = Exception("table not found")
        assert builder._enrich_table_nodes_with_ontology() == 0

    def test_uses_correct_table_names(self, builder):
        mock_result = MagicMock()
        mock_result.collect.return_value = [MagicMock(cnt=0)]
        builder.spark.sql.return_value = mock_result
        builder._enrich_table_nodes_with_ontology()
        merge_sql = builder.spark.sql.call_args_list[0][0][0]
        assert "cat.sch.graph_nodes" in merge_sql
        assert "cat.sch.ontology_entities" in merge_sql


class TestAddSameEntityTypeEdges:
    """Tests for _add_same_entity_type_edges."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def test_no_pairs_returns_zero(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        builder.spark.sql.return_value = mock_df
        assert builder._add_same_entity_type_edges() == 0

    def test_self_join_uses_less_than_to_dedupe(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        builder.spark.sql.return_value = mock_df
        builder._add_same_entity_type_edges()
        join_sql = builder.spark.sql.call_args_list[0][0][0]
        assert "a.table_name < b.table_name" in join_sql

    def test_calls_insert_edges_safe_when_pairs_exist(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 3
        mock_df.select.return_value = mock_df
        builder.spark.sql.return_value = mock_df
        with patch.object(builder, '_insert_edges_safe') as mock_insert:
            builder._add_same_entity_type_edges()
            mock_insert.assert_called_once()
            args = mock_insert.call_args[0]
            assert "graph_edges" in args[1]

    def test_returns_zero_on_failure(self, builder):
        builder.spark.sql.side_effect = Exception("boom")
        assert builder._add_same_entity_type_edges() == 0


class TestClearOntologyEdgesIncludesSameEntityType:
    """Verify _clear_ontology_edges removes same_entity_type edges."""

    @pytest.fixture
    def builder(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch")
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            b = OntologyBuilder(mock_spark, config)
        return b

    def test_delete_includes_same_entity_type(self, builder):
        builder._clear_ontology_edges()
        delete_sql = builder.spark.sql.call_args[0][0]
        assert "same_entity_type" in delete_sql


# ======================================================================
# OntologyValidator -- batched column validation
# ======================================================================

from pyspark.sql.utils import AnalysisException
from dbxmetagen.ontology_validator import (
    OntologyValidator,
    OntologyValidatorConfig,
    MAX_ENTITIES_PER_PROMPT,
    RETRY_ENTITIES_PER_PROMPT,
    CIRCUIT_BREAKER_THRESHOLD,
)


class TestParseAiArray:
    """Tests for _parse_ai_array truncation-resilient parser."""

    def test_parses_clean_array(self):
        text = '[{"entity_id": "a", "is_valid": true}, {"entity_id": "b", "is_valid": false}]'
        items, missing = OntologyValidator._parse_ai_array(text, ["a", "b"])
        assert len(items) == 2
        assert missing == []

    def test_handles_markdown_fences(self):
        text = '```json\n[{"entity_id": "x", "is_valid": true}]\n```'
        items, missing = OntologyValidator._parse_ai_array(text, ["x"])
        assert len(items) == 1
        assert missing == []

    def test_recovers_truncated_array(self):
        text = '[{"entity_id": "a", "is_valid": true}, {"entity_id": "b", "is_val'
        items, missing = OntologyValidator._parse_ai_array(text, ["a", "b"])
        assert len(items) == 1
        assert items[0]["entity_id"] == "a"
        assert "b" in missing

    def test_empty_response_returns_all_missing(self):
        items, missing = OntologyValidator._parse_ai_array("", ["a", "b"])
        assert items == []
        assert set(missing) == {"a", "b"}

    def test_no_bracket_returns_all_missing(self):
        items, missing = OntologyValidator._parse_ai_array("No JSON here", ["x"])
        assert items == []
        assert missing == ["x"]

    def test_tracks_missing_entity_ids(self):
        text = '[{"entity_id": "a", "is_valid": true}]'
        items, missing = OntologyValidator._parse_ai_array(text, ["a", "b", "c"])
        assert len(items) == 1
        assert set(missing) == {"b", "c"}

    def test_total_parse_failure_returns_all_missing(self):
        text = '[not valid json at all}'
        items, missing = OntologyValidator._parse_ai_array(text, ["a", "b"])
        assert items == []
        assert set(missing) == {"a", "b"}


class TestBuildEntitySection:

    def test_with_column_bindings(self):
        entity = {
            "entity_id": "abc",
            "entity_type": "Patient",
            "confidence": 0.85,
            "discovery_method": "keyword",
            "column_bindings": [
                {"attribute_name": "identifier", "bound_table": "t", "bound_column": "patient_id"},
            ],
            "source_columns": ["patient_id"],
        }
        section = OntologyValidator._build_entity_section(entity)
        assert "Patient" in section
        assert "patient_id" in section
        assert "Patient.identifier" in section

    def test_without_bindings_uses_source_columns(self):
        entity = {
            "entity_id": "abc",
            "entity_type": "Address",
            "confidence": 0.7,
            "discovery_method": "ai_batch",
            "column_bindings": [],
            "source_columns": ["addr_line1", "addr_city"],
        }
        section = OntologyValidator._build_entity_section(entity)
        assert "addr_line1" in section
        assert "addr_city" in section


class TestBuildBatchPrompt:

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_multi_table_prompt(self, validator):
        entities = [
            {
                "entity_id": "e1", "entity_type": "Patient", "table_name": "t1",
                "table_comment": "Patient records", "domain": "Healthcare",
                "source_columns": ["pid"], "column_bindings": [],
                "confidence": 0.9, "discovery_method": "keyword",
            },
            {
                "entity_id": "e2", "entity_type": "Claim", "table_name": "t2",
                "table_comment": "Claims data", "domain": "Healthcare",
                "source_columns": ["claim_id"], "column_bindings": [],
                "confidence": 0.7, "discovery_method": "ai_batch",
            },
        ]
        prompt = validator._build_batch_prompt(entities)
        assert "t1" in prompt
        assert "t2" in prompt
        assert "Patient" in prompt
        assert "Claim" in prompt
        assert "JSON array" in prompt


class TestRunAiQueryBatch:

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_empty_input_returns_empty(self, validator):
        s, f = validator._run_ai_query_batch([])
        assert s == []
        assert f == []

    def test_circuit_breaker_all_null(self, validator):
        mock_row = MagicMock()
        mock_row.response = None
        mock_row.entity_ids = ["a"]
        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_row]
        validator.spark.sql.return_value = mock_df
        validator.spark.createDataFrame.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="all .* AI_QUERY calls returned errors"):
            validator._run_ai_query_batch([{
                "chunk_id": "0", "entity_ids": ["a"], "prompt_text": "test"
            }])

    def test_circuit_breaker_majority_null(self, validator):
        row_ok = MagicMock()
        row_ok.response = '[{"entity_id":"a","is_valid":true}]'
        row_ok.entity_ids = ["a"]
        row_fail1 = MagicMock()
        row_fail1.response = None
        row_fail1.entity_ids = ["b"]
        row_fail2 = MagicMock()
        row_fail2.response = None
        row_fail2.entity_ids = ["c"]

        mock_df = MagicMock()
        mock_df.collect.return_value = [row_ok, row_fail1, row_fail2]
        validator.spark.sql.return_value = mock_df
        validator.spark.createDataFrame.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="model serving issue"):
            validator._run_ai_query_batch([
                {"chunk_id": "0", "entity_ids": ["a"], "prompt_text": "t1"},
                {"chunk_id": "1", "entity_ids": ["b"], "prompt_text": "t2"},
                {"chunk_id": "2", "entity_ids": ["c"], "prompt_text": "t3"},
            ])

    def test_partial_failure_below_threshold(self, validator):
        row_ok1 = MagicMock()
        row_ok1.response = '[{"entity_id":"a","is_valid":true}]'
        row_ok1.entity_ids = ["a"]
        row_ok2 = MagicMock()
        row_ok2.response = '[{"entity_id":"b","is_valid":true}]'
        row_ok2.entity_ids = ["b"]
        row_fail = MagicMock()
        row_fail.response = None
        row_fail.entity_ids = ["c"]

        mock_df = MagicMock()
        mock_df.collect.return_value = [row_ok1, row_ok2, row_fail]
        validator.spark.sql.return_value = mock_df
        validator.spark.createDataFrame.return_value = MagicMock()

        successes, failures = validator._run_ai_query_batch([
            {"chunk_id": "0", "entity_ids": ["a"], "prompt_text": "t1"},
            {"chunk_id": "1", "entity_ids": ["b"], "prompt_text": "t2"},
            {"chunk_id": "2", "entity_ids": ["c"], "prompt_text": "t3"},
        ])
        assert len(successes) == 2
        assert len(failures) == 1


class TestBatchUpdateEntityValidation:

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_empty_results(self, validator):
        assert validator.batch_update_entity_validation([]) == 0

    def test_merge_sql_executed(self, validator):
        results = [{
            "entity_id": "e1",
            "validation_result": {"is_valid": True, "confidence_adjustment": 0.1, "reasoning": "ok"},
        }]
        validator.spark.createDataFrame.return_value = MagicMock()
        count = validator.batch_update_entity_validation(results)
        assert count == 1
        merge_sql = validator.spark.sql.call_args[0][0]
        assert "MERGE INTO" in merge_sql
        assert "cat.sch.ontology_entities" in merge_sql

    def test_falls_back_to_per_entity_on_merge_failure(self, validator):
        results = [{
            "entity_id": "e1",
            "validation_result": {"is_valid": True, "confidence_adjustment": 0, "reasoning": "ok"},
        }]
        validator.spark.createDataFrame.return_value = MagicMock()
        validator.spark.sql.side_effect = [Exception("MERGE failed"), None]
        with patch.object(validator, 'update_entity_validation', return_value=1) as mock_update:
            count = validator.batch_update_entity_validation(results)
            mock_update.assert_called_once()
            assert count == 1


class TestRunWithValidateColumns:

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        v = OntologyValidator(mock_spark, config)
        return v

    def test_columns_skipped_by_default(self, validator):
        with patch.object(validator, 'validate_table_entities_batched', return_value=([], [])) as mock_tbl, \
             patch.object(validator, 'batch_update_entity_validation', return_value=0), \
             patch.object(validator, 'check_consistency', return_value=[]), \
             patch.object(validator, 'check_coverage', return_value=[]), \
             patch.object(validator, 'validate_relationships', return_value=[]), \
             patch.object(validator, 'prune_invalid_edges', return_value=0), \
             patch.object(validator, 'generate_ontology_recommendations', return_value={}):
            result = validator.run()
            mock_tbl.assert_called_once()
            assert result["column_entities_validated"] == 0

    def test_columns_validated_when_opted_in(self, validator):
        with patch.object(validator, 'validate_table_entities_batched', return_value=([], [])) as mock_tbl, \
             patch.object(validator, 'validate_column_entities_batched', return_value=([], [])) as mock_col, \
             patch.object(validator, 'batch_update_entity_validation', return_value=0), \
             patch.object(validator, 'check_consistency', return_value=[]), \
             patch.object(validator, 'check_coverage', return_value=[]), \
             patch.object(validator, 'validate_relationships', return_value=[]), \
             patch.object(validator, 'prune_invalid_edges', return_value=0), \
             patch.object(validator, 'generate_ontology_recommendations', return_value={}):
            result = validator.run(validate_columns=True)
            mock_tbl.assert_called_once()
            mock_col.assert_called_once()


class TestContradictionInBatchedPath:
    """Verify _detect_contradiction is applied to batched column validation results."""

    def test_contradiction_flips_is_valid(self):
        result = {
            "entity_id": "e1",
            "is_valid": True,
            "confidence_adjustment": 0.1,
            "reasoning": "This aligns with a Medication entity",
            "suggested_type": "Medication",
        }
        checked = OntologyValidator._detect_contradiction(result, "Patient")
        assert checked["is_valid"] is False
        assert checked["confidence_adjustment"] <= -0.3

    def test_no_contradiction_when_types_match(self):
        result = {
            "entity_id": "e1",
            "is_valid": True,
            "confidence_adjustment": 0.2,
            "reasoning": "Correct classification",
            "suggested_type": "Patient",
        }
        checked = OntologyValidator._detect_contradiction(result, "Patient")
        assert checked["is_valid"] is True
        assert checked["confidence_adjustment"] == 0.2

    def test_no_contradiction_without_suggested_type(self):
        result = {
            "entity_id": "e1",
            "is_valid": True,
            "confidence_adjustment": 0.0,
            "reasoning": "looks fine",
        }
        checked = OntologyValidator._detect_contradiction(result, "Patient")
        assert checked["is_valid"] is True


class TestColumnBindingsFallback:
    """Verify column_bindings fallback when column doesn't exist in table."""

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_fallback_on_exception(self, validator):
        """When first SQL raises, second query without column_bindings is used."""
        mock_row = MagicMock()
        mock_row.entity_id = "e1"
        mock_row.entity_type = "Patient"
        mock_row.table_name = "t1"
        mock_row.source_columns = ["col1"]
        mock_row.confidence = 0.8
        mock_row.discovery_method = "keyword"
        mock_row.table_comment = "a table"
        mock_row.domain = "healthcare"

        call_count = [0]
        def sql_side_effect(query):
            call_count[0] += 1
            if call_count[0] == 1:
                raise AnalysisException("column_bindings not found")
            mock_df = MagicMock()
            mock_df.collect.return_value = [mock_row]
            return mock_df

        validator.spark.sql.side_effect = sql_side_effect

        with patch.object(validator, '_run_ai_query_batch', return_value=([], [])):
            results, unvalidated = validator.validate_column_entities_batched()

        assert call_count[0] == 2
        assert len(unvalidated) == 0 or len(results) == 0

    def test_no_fallback_when_column_exists(self, validator):
        """Normal path: first SQL succeeds, no fallback needed."""
        mock_row = MagicMock()
        mock_row.entity_id = "e1"
        mock_row.entity_type = "Patient"
        mock_row.table_name = "t1"
        mock_row.source_columns = ["col1"]
        mock_row.column_bindings = None
        mock_row.confidence = 0.8
        mock_row.discovery_method = "keyword"
        mock_row.table_comment = "a table"
        mock_row.domain = "healthcare"

        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_row]
        validator.spark.sql.return_value = mock_df

        with patch.object(validator, '_run_ai_query_batch', return_value=([], [])):
            results, unvalidated = validator.validate_column_entities_batched()

        validator.spark.sql.assert_called_once()


class TestTableBatchValidation:
    """Tests for validate_table_entities_batched and related methods."""

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_build_table_entity_section(self):
        entity = {
            "entity_id": "e1",
            "entity_name": "Patient",
            "entity_type": "Person",
            "confidence": 0.9,
            "description": "A patient record",
            "source_tables": ["schema.patients"],
            "table_metadata": "  Comment: Patient info\n  Domain: healthcare",
            "column_metadata": "  - patient_id (INT)\n  - name (STRING)",
        }
        section = OntologyValidator._build_table_entity_section(entity)
        assert "Entity (id: e1):" in section
        assert "Name: Patient" in section
        assert "Type: Person" in section
        assert "A patient record" in section
        assert "schema.patients" in section
        assert "patient_id" in section

    def test_validate_table_entities_batched_empty(self, validator):
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        validator.spark.sql.return_value = mock_df

        results, unvalidated = validator.validate_table_entities_batched()
        assert results == []
        assert unvalidated == []

    def test_validate_table_entities_batched_basic(self, validator):
        entity_row = MagicMock()
        entity_row.entity_id = "e1"
        entity_row.entity_name = "Patient"
        entity_row.entity_type = "Person"
        entity_row.description = "A patient"
        entity_row.source_tables = ["t1"]
        entity_row.source_columns = ["col1"]
        entity_row.confidence = 0.8
        entity_row.table_comment = "table about patients"
        entity_row.domain = "healthcare"

        col_row = MagicMock()
        col_row.table_name = "t1"
        col_row.column_name = "col1"
        col_row.data_type = "STRING"
        col_row.classification = None
        col_row.comment = "patient name"

        call_count = [0]
        def sql_side_effect(query):
            call_count[0] += 1
            mock_df = MagicMock()
            if call_count[0] == 1:
                mock_df.collect.return_value = [entity_row]
            elif call_count[0] == 2:
                mock_df.collect.return_value = [col_row]
            else:
                mock_df.collect.return_value = []
            return mock_df

        validator.spark.sql.side_effect = sql_side_effect

        success_row = MagicMock()
        success_row.chunk_id = "0"
        success_row.entity_ids = ["e1"]
        success_row.response = json.dumps([{
            "entity_id": "e1",
            "is_valid": True,
            "confidence_adjustment": 0.1,
            "reasoning": "Correct",
            "suggested_type": "",
        }])

        with patch.object(validator, '_run_ai_query_batch', return_value=([success_row], [])):
            results, unvalidated = validator.validate_table_entities_batched()

        assert len(results) == 1
        assert results[0]["entity_id"] == "e1"
        assert results[0]["validation_result"]["is_valid"] is True
        assert unvalidated == []

    def test_run_uses_batched_table_validation(self, validator):
        with patch.object(validator, 'validate_table_entities_batched', return_value=([], [])) as mock_tbl, \
             patch.object(validator, 'batch_update_entity_validation', return_value=0) as mock_update, \
             patch.object(validator, 'check_consistency', return_value=[]), \
             patch.object(validator, 'check_coverage', return_value=[]), \
             patch.object(validator, 'validate_relationships', return_value=[]), \
             patch.object(validator, 'prune_invalid_edges', return_value=0), \
             patch.object(validator, 'generate_ontology_recommendations', return_value={}):
            result = validator.run()
            mock_tbl.assert_called_once()
            mock_update.assert_called()
            assert "table_entities_validated" in result


class TestPruneInvalidEdges:
    """Tests for prune_invalid_edges."""

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_prune_runs_delete(self, validator):
        call_count = [0]
        def sql_side_effect(query):
            call_count[0] += 1
            mock_df = MagicMock()
            if "COUNT" in query and call_count[0] == 1:
                mock_df.collect.return_value = [MagicMock(cnt=10)]
            elif "DELETE" in query:
                mock_df.collect.return_value = []
            elif "COUNT" in query:
                mock_df.collect.return_value = [MagicMock(cnt=7)]
            return mock_df

        validator.spark.sql.side_effect = sql_side_effect
        pruned = validator.prune_invalid_edges()
        assert pruned == 3

    def test_prune_returns_zero_when_no_invalid(self, validator):
        call_count = [0]
        def sql_side_effect(query):
            call_count[0] += 1
            mock_df = MagicMock()
            mock_df.collect.return_value = [MagicMock(cnt=5)]
            return mock_df

        validator.spark.sql.side_effect = sql_side_effect
        pruned = validator.prune_invalid_edges()
        assert pruned == 0

    def test_prune_handles_missing_table(self, validator):
        validator.spark.sql.side_effect = Exception("table not found")
        pruned = validator.prune_invalid_edges()
        assert pruned == 0

    def test_run_calls_prune_and_includes_in_result(self, validator):
        with patch.object(validator, 'validate_table_entities_batched', return_value=([], [])), \
             patch.object(validator, 'batch_update_entity_validation', return_value=0), \
             patch.object(validator, 'check_consistency', return_value=[]), \
             patch.object(validator, 'check_coverage', return_value=[]), \
             patch.object(validator, 'validate_relationships', return_value=[]), \
             patch.object(validator, 'prune_invalid_edges', return_value=5) as mock_prune, \
             patch.object(validator, 'generate_ontology_recommendations', return_value={}):
            result = validator.run()
            mock_prune.assert_called_once()
            assert result["pruned_edges"] == 5


class TestStoreEntitiesNonIncremental:
    """Tests that _store_entities resets validated state when incremental=False."""

    @pytest.fixture
    def builder_incremental(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", incremental=True)
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            return OntologyBuilder(mock_spark, config)

    @pytest.fixture
    def builder_non_incremental(self):
        mock_spark = MagicMock()
        config = OntologyConfig(catalog_name="cat", schema_name="sch", incremental=False)
        with patch.object(OntologyLoader, 'load_config') as mock_load:
            mock_load.return_value = OntologyLoader._default_config()
            return OntologyBuilder(mock_spark, config)

    def _get_merge_sql(self, builder):
        entities = [{"entity_id": "e1", "entity_type": "Patient", "source_tables": ["t1"], "confidence": 0.9}]
        builder._store_entities(entities)
        all_sql = [c[0][0] for c in builder.spark.sql.call_args_list]
        merge_sqls = [s for s in all_sql if "MERGE INTO" in s]
        assert len(merge_sqls) == 1
        return merge_sqls[0]

    def test_incremental_merge_requires_validated_false(self, builder_incremental):
        sql = self._get_merge_sql(builder_incremental)
        assert "target.validated = FALSE THEN UPDATE" in sql
        assert "target.validated = FALSE," not in sql.split("THEN UPDATE")[1]

    def test_non_incremental_merge_drops_validated_guard(self, builder_non_incremental):
        sql = self._get_merge_sql(builder_non_incremental)
        assert "AND target.validated = FALSE THEN UPDATE" not in sql

    def test_non_incremental_merge_resets_validated(self, builder_non_incremental):
        sql = self._get_merge_sql(builder_non_incremental)
        after_update = sql.split("THEN UPDATE SET")[1]
        assert "target.validated = FALSE" in after_update
        assert "target.validation_notes = NULL" in after_update


class TestForceRevalidate:
    """Tests for the force_revalidate parameter in OntologyValidator.run()."""

    @pytest.fixture
    def validator(self):
        mock_spark = MagicMock()
        config = OntologyValidatorConfig(catalog_name="cat", schema_name="sch")
        return OntologyValidator(mock_spark, config)

    def test_force_revalidate_runs_reset_update(self, validator):
        with patch.object(validator, 'validate_table_entities_batched', return_value=([], [])), \
             patch.object(validator, 'batch_update_entity_validation', return_value=0), \
             patch.object(validator, 'check_consistency', return_value=[]), \
             patch.object(validator, 'check_coverage', return_value=[]), \
             patch.object(validator, 'validate_relationships', return_value=[]), \
             patch.object(validator, 'prune_invalid_edges', return_value=0), \
             patch.object(validator, 'generate_ontology_recommendations', return_value={}):
            validator.run(force_revalidate=True)
            all_sql = [c[0][0] for c in validator.spark.sql.call_args_list]
            reset_sqls = [s for s in all_sql if "UPDATE" in s and "validated = FALSE" in s and "validated = TRUE" in s]
            assert len(reset_sqls) == 1
            assert "auto_discovered = TRUE" in reset_sqls[0]

    def test_force_revalidate_false_skips_reset(self, validator):
        with patch.object(validator, 'validate_table_entities_batched', return_value=([], [])), \
             patch.object(validator, 'batch_update_entity_validation', return_value=0), \
             patch.object(validator, 'check_consistency', return_value=[]), \
             patch.object(validator, 'check_coverage', return_value=[]), \
             patch.object(validator, 'validate_relationships', return_value=[]), \
             patch.object(validator, 'prune_invalid_edges', return_value=0), \
             patch.object(validator, 'generate_ontology_recommendations', return_value={}):
            validator.run(force_revalidate=False)
            all_sql = [c[0][0] for c in validator.spark.sql.call_args_list]
            reset_sqls = [s for s in all_sql if "UPDATE" in s and "SET validated = FALSE" in s and "WHERE" in s]
            assert len(reset_sqls) == 0

    def test_validate_ontology_passes_force_revalidate(self):
        with patch('dbxmetagen.ontology_validator.OntologyValidator') as MockClass:
            mock_instance = MagicMock()
            mock_instance.run.return_value = {"entities_validated": 0}
            MockClass.return_value = mock_instance
            from dbxmetagen.ontology_validator import validate_ontology
            validate_ontology(
                spark=MagicMock(), catalog_name="c", schema_name="s",
                force_revalidate=True,
            )
            mock_instance.run.assert_called_once_with(
                validate_columns=False, force_revalidate=True
            )


class TestEmitBundleEdges:
    """Tests for OntologyBuilder.emit_bundle_edges()."""

    @pytest.fixture
    def builder(self, mock_spark):
        config = MagicMock()
        config.fully_qualified_entities = "cat.sch.ontology_entities"
        config.fully_qualified_relationships = "cat.sch.ontology_relationships"
        config.catalog_name = "cat"
        config.schema_name = "sch"

        mock_loader = MagicMock()
        mock_loader.has_tier_indexes = True
        mock_loader.get_edges_tier1.return_value = [
            {"name": "treats", "domain": "Medication", "range": "Condition", "cardinality": "one-to-many"},
            {"name": "prescribes", "domain": "Practitioner", "range": "Medication", "cardinality": "one-to-many"},
            {"name": "has_encounter", "domain": "Patient", "range": "Encounter", "cardinality": "one-to-many"},
        ]

        with patch('dbxmetagen.ontology.OntologyLoader') as MockOntLoader, \
             patch('dbxmetagen.ontology.EntityDiscoverer') as MockDisc, \
             patch('dbxmetagen.ontology.load_domain_entity_affinity', return_value={}):
            MockOntLoader.load_config.return_value = {"ontology": {"version": "1.0"}}
            disc_instance = MockDisc.return_value
            disc_instance._get_index_loader.return_value = mock_loader
            disc_instance.entity_definitions = []

            from dbxmetagen.ontology import OntologyBuilder
            b = OntologyBuilder(mock_spark, config)
            b.discoverer = disc_instance
            yield b

    def test_emits_edges_for_matching_entity_pairs(self, builder):
        Row = type("Row", (), {"__init__": lambda self, **kw: self.__dict__.update(kw)})
        discovered = [Row(entity_type="Medication"), Row(entity_type="Condition"), Row(entity_type="Patient")]
        builder.spark.sql.return_value.collect.return_value = discovered
        builder.spark.createDataFrame = MagicMock()
        builder.spark.catalog = MagicMock()
        builder.discoverer._get_index_loader.return_value._load.return_value = {}

        result = builder.emit_bundle_edges()
        assert result == 1
        df_data = builder.spark.createDataFrame.call_args[0][0]
        assert len(df_data) == 1
        row = df_data[0]
        assert row["src_entity_type"] == "Medication"
        assert row["dst_entity_type"] == "Condition"
        assert row["relationship_name"] == "treats"
        assert row["cardinality"] == "one-to-many"
        assert row["source"] == "bundle"
        assert row["confidence"] == 0.8
        assert row["evidence_column"] is None
        assert row["evidence_table"] is None
        assert "relationship_id" in row

        merge_calls = [c for c in builder.spark.sql.call_args_list if "MERGE INTO" in str(c)]
        assert len(merge_calls) == 1

    def test_skips_when_no_loader(self, builder):
        builder.discoverer._get_index_loader.return_value = None
        assert builder.emit_bundle_edges() == 0

    def test_skips_when_no_discovered_entities(self, builder):
        builder.spark.sql.return_value.collect.return_value = []
        builder.discoverer._get_index_loader.return_value._load.return_value = {}
        assert builder.emit_bundle_edges() == 0

    def test_reference_resolution_resolves_from_property_name(self, builder):
        """Edge with range='Reference' resolved via property name suffix."""
        from dbxmetagen.ontology import OntologyBuilder

        loader = builder.discoverer._get_index_loader.return_value
        loader.get_edges_tier1.return_value = [
            {"name": "Patient.managingOrganization", "domain": "Patient", "range": "Reference", "cardinality": "one-to-one"},
        ]
        loader._load.return_value = {}

        Row = type("Row", (), {"__init__": lambda self, **kw: self.__dict__.update(kw)})
        builder.spark.sql.return_value.collect.return_value = [
            Row(entity_type="Patient"), Row(entity_type="Organization"),
        ]
        builder.spark.createDataFrame = MagicMock()
        builder.spark.catalog = MagicMock()

        result = builder.emit_bundle_edges()
        assert result == 1
        df_data = builder.spark.createDataFrame.call_args[0][0]
        row = df_data[0]
        assert row["src_entity_type"] == "Patient"
        assert row["dst_entity_type"] == "Organization"
        assert row["relationship_name"] == "Patient.managingOrganization"
        assert row["cardinality"] == "one-to-one"
        assert row["source"] == "bundle"

    def test_multi_range_fallback(self, builder):
        """Edge with non-discovered primary range falls back to tier-2 ranges list."""
        loader = builder.discoverer._get_index_loader.return_value
        loader.get_edges_tier1.return_value = [
            {"name": "subject", "domain": "Observation", "range": "Resource", "cardinality": "one-to-one"},
        ]
        loader._load.return_value = {
            "subject": {"name": "subject", "domain": "Observation", "range": "Resource",
                        "ranges": ["Resource", "Patient", "Group"]},
        }

        Row = type("Row", (), {"__init__": lambda self, **kw: self.__dict__.update(kw)})
        builder.spark.sql.return_value.collect.return_value = [
            Row(entity_type="Observation"), Row(entity_type="Patient"),
        ]
        builder.spark.createDataFrame = MagicMock()
        builder.spark.catalog = MagicMock()

        result = builder.emit_bundle_edges()
        assert result == 1
        df_data = builder.spark.createDataFrame.call_args[0][0]
        assert df_data[0]["dst_entity_type"] == "Patient"

    def test_reference_resolution_skips_ambiguous(self, builder):
        """Reference resolution skips when multiple entity types match the suffix."""
        from dbxmetagen.ontology import OntologyBuilder

        # "suborganization" ends with both "organization" and "suborganization"
        result = OntologyBuilder._resolve_reference_target(
            "Patient.subOrganization", {"organization": "Organization", "suborganization": "SubOrganization"}
        )
        assert result is None


class TestBuildTiersEnrichedSchema:
    """Verify build_tiers outputs label on entities and cardinality on edges."""

    def test_tier1_has_label_and_edge_has_cardinality(self):
        import json as _json
        import tempfile
        from pathlib import Path
        from dbxmetagen.ontology_bundle_indexes import build_tiers

        entities = {
            "Patient": {
                "description": "Patient record",
                "label": "Patient Resource",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Patient",
                "parents": [],
                "outgoing_edges": [{"name": "hasEncounter", "uri": "", "range": "Encounter", "ranges": [], "inverse": None}],
                "keywords": ["patient"],
                "synonyms": [],
                "typical_attributes": [],
                "business_questions": [],
                "relationships": {"hasEncounter": {"target": "Encounter", "cardinality": "one-to-many"}},
                "properties": {},
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            build_tiers(entities, Path(tmp))
            t1 = _json.loads((Path(tmp) / "entities_tier1.json").read_text())
            assert t1[0]["label"] == "Patient Resource"
            e1 = _json.loads((Path(tmp) / "edges_tier1.json").read_text())
            assert e1[0]["cardinality"] == "one-to-many"

    def test_edge_tiers_include_label_and_facet(self):
        import json as _json
        import tempfile
        from pathlib import Path
        import yaml
        from dbxmetagen.ontology_bundle_indexes import build_tiers

        entities = {
            "Encounter": {
                "description": "An encounter",
                "label": "Encounter",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Encounter",
                "parents": [],
                "outgoing_edges": [{
                    "name": "subject", "uri": "", "range": "Patient",
                    "ranges": ["Patient"], "inverse": None,
                    "label": "Subject", "facet": "who",
                }],
                "keywords": [],
                "synonyms": [],
                "typical_attributes": [],
                "business_questions": [],
                "relationships": {"subject": {"target": "Patient", "cardinality": "one-to-one"}},
                "properties": {},
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            build_tiers(entities, Path(tmp))
            e1 = _json.loads((Path(tmp) / "edges_tier1.json").read_text())
            assert e1[0]["label"] == "Subject"
            assert e1[0]["facet"] == "who"
            e3 = _json.loads((Path(tmp) / "edges_tier3.json").read_text())
            assert e3["subject"]["facet"] == "who"
            assert e3["subject"]["category"] == "who"

            e1_yaml = yaml.safe_load((Path(tmp) / "edges_tier1.yaml").read_text())
            assert e1_yaml[0]["label"] == "Subject"
            assert e1_yaml[0]["facet"] == "who"


class TestDualFormatLoader:
    """Verify OntologyIndexLoader prefers JSON over YAML."""

    def test_loads_json_when_both_exist(self):
        import json as _json
        import tempfile
        from pathlib import Path
        import yaml
        from dbxmetagen.ontology_index import OntologyIndexLoader

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            yaml.dump([{"name": "A", "description": "from yaml"}], (d / "entities_tier1.yaml").open("w"))
            _json.dump([{"name": "B", "description": "from json"}], (d / "entities_tier1.json").open("w"))
            loader = OntologyIndexLoader(base_dir=str(d))
            assert loader.has_tier_indexes
            result = loader.get_entities_tier1()
            assert result[0]["name"] == "B"

    def test_falls_back_to_yaml(self):
        import tempfile
        from pathlib import Path
        import yaml
        from dbxmetagen.ontology_index import OntologyIndexLoader

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            yaml.dump([{"name": "C", "description": "yaml only"}], (d / "entities_tier1.yaml").open("w"))
            loader = OntologyIndexLoader(base_dir=str(d))
            assert loader.has_tier_indexes
            result = loader.get_entities_tier1()
            assert result[0]["name"] == "C"

    def test_edge_tier_prefers_json(self):
        import json as _json
        import tempfile
        from pathlib import Path
        import yaml
        from dbxmetagen.ontology_index import OntologyIndexLoader

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            yaml.dump([{"name": "E", "description": "ent"}], (d / "entities_tier1.yaml").open("w"))
            yaml.dump([{"name": "old_edge", "domain": "A", "range": "B"}],
                      (d / "edges_tier1.yaml").open("w"))
            _json.dump([{"name": "new_edge", "domain": "X", "range": "Y"}],
                       (d / "edges_tier1.json").open("w"))
            loader = OntologyIndexLoader(base_dir=str(d))
            edges = loader.get_edges_tier1()
            assert edges[0]["name"] == "new_edge"


class TestEntitiesFromBundleFidelity:
    """North-star tests: curated bundle metadata should flow through the pipeline."""

    def test_bundle_relationship_label_propagates_to_outgoing_edges(self):
        """Curated bundle labels on relationships should survive into outgoing_edges.

        Currently entities_from_bundle() builds outgoing_edges with only name, uri,
        range, inverse. The 'label' field from a bundle's relationship definition is
        silently dropped. When curated bundles provide display labels (which they
        should for non-OWL ontologies like general, financial_services, retail_cpg),
        those labels must flow into outgoing_edges so build_tiers() can propagate
        them to edge tier files for LLM context.
        """
        import tempfile
        from pathlib import Path
        import yaml
        from dbxmetagen.ontology_bundle_indexes import entities_from_bundle

        bundle = {
            "ontology": {
                "entities": {"definitions": {
                    "Patient": {
                        "description": "A patient",
                        "keywords": ["patient"],
                        "relationships": {
                            "managingOrganization": {
                                "target": "Organization",
                                "cardinality": "one-to-one",
                                "label": "Managing Organization",
                            }
                        },
                    }
                }},
                "edge_catalog": {},
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test_bundle.yaml"
            yaml.dump(bundle, p.open("w"), default_flow_style=False)
            entities = entities_from_bundle(p)

        edge = entities["Patient"]["outgoing_edges"][0]
        assert edge.get("label") == "Managing Organization"


class TestEdgeTierSemanticCompleteness:
    """North-star tests: W5 semantic annotations should survive into edge tier files."""

    def test_sub_property_of_preserved_in_edge_tier2(self):
        """W5 sub-property-of hierarchy should survive into edges_tier2 for LLM context.

        Currently build_tiers() copies label and facet from outgoing_edges into
        all_edges, but does NOT copy sub_property_of. This means the W5 semantic
        classification (e.g. ["who.focus"]) extracted by _extract_single_class()
        is lost when building tier-2 edge files. Downstream consumers (like LLM
        prompts in predict_edge()) that read tier-2/3 edges never see the W5 path.
        """
        import json as _json
        import tempfile
        from pathlib import Path
        from dbxmetagen.ontology_bundle_indexes import build_tiers

        entities = {
            "Encounter": {
                "description": "An encounter",
                "label": "Encounter",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Encounter",
                "parents": [],
                "outgoing_edges": [{
                    "name": "subject", "uri": "", "range": "Patient",
                    "ranges": ["Patient"], "inverse": None,
                    "label": "Subject", "facet": "who",
                    "sub_property_of": ["who.focus"],
                }],
                "keywords": [],
                "synonyms": [],
                "typical_attributes": [],
                "business_questions": [],
                "relationships": {"subject": {"target": "Patient", "cardinality": "one-to-one"}},
                "properties": {},
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            build_tiers(entities, Path(tmp))
            e2 = _json.loads((Path(tmp) / "edges_tier2.json").read_text())
            assert e2["subject"]["sub_property_of"] == ["who.focus"]


class TestEntitiesFromBundleStringRelationship:
    """entities_from_bundle must handle shorthand string relationships."""

    def test_string_relationship_produces_entity_with_target(self):
        import yaml as _yaml
        from dbxmetagen.ontology_bundle_indexes import entities_from_bundle
        bundle = {
            "ontology": {
                "entities": {
                    "definitions": {
                        "Patient": {
                            "description": "A patient",
                            "relationships": {
                                "managingOrganization": "Organization",
                            },
                        },
                    },
                },
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "bundle.yaml"
            p.write_text(_yaml.dump(bundle))
            result = entities_from_bundle(p)
        pat = result["Patient"]
        edge_names = [e["name"] for e in pat["outgoing_edges"]]
        assert "managingOrganization" in edge_names
        edge = next(e for e in pat["outgoing_edges"] if e["name"] == "managingOrganization")
        assert edge["range"] == "Organization"


class TestParseTierFileJsonFallback:
    """_parse_tier_file must fall back to YAML sibling on corrupt JSON."""

    def test_corrupt_json_falls_back_to_yaml(self):
        import yaml as _yaml
        from dbxmetagen.ontology_index import _parse_tier_file
        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "entities_tier1.json"
            yaml_path = Path(tmp) / "entities_tier1.yaml"
            json_path.write_text("{corrupt json!!!", encoding="utf-8")
            yaml_path.write_text(_yaml.dump({"Patient": {"desc": "ok"}}), encoding="utf-8")
            result = _parse_tier_file(json_path)
        assert result == {"Patient": {"desc": "ok"}}

    def test_corrupt_json_no_yaml_sibling_raises(self):
        from dbxmetagen.ontology_index import _parse_tier_file
        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "entities_tier1.json"
            json_path.write_text("{corrupt json!!!", encoding="utf-8")
            with pytest.raises(json.JSONDecodeError):
                _parse_tier_file(json_path)


class TestGetUriCaseInsensitive:
    """get_uri must perform case-insensitive entity name lookup."""

    def test_lowercase_input_matches_titlecase_key(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader.__new__(OntologyIndexLoader)
        loader._cache = {}
        loader.bundle_dir = None
        uri_data = {"Patient": "http://hl7.org/fhir/Patient"}
        with patch.object(loader, "_load", return_value=uri_data):
            assert loader.get_uri("patient") == "http://hl7.org/fhir/Patient"

    def test_exact_case_still_works(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader.__new__(OntologyIndexLoader)
        loader._cache = {}
        loader.bundle_dir = None
        uri_data = {"Patient": "http://hl7.org/fhir/Patient"}
        with patch.object(loader, "_load", return_value=uri_data):
            assert loader.get_uri("Patient") == "http://hl7.org/fhir/Patient"


class TestResolveOntologyConfigPath:
    """Tests for _resolve_ontology_config_path auto-derive logic."""

    def test_default_config_derives_from_bundle(self):
        result = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "general",
        )
        assert result == "configurations/ontology_bundles/general.yaml"

    def test_explicit_override_wins(self):
        result = _resolve_ontology_config_path(
            "custom/path.yaml", "general",
        )
        assert result == "custom/path.yaml"

    def test_empty_bundle_keeps_default(self):
        result = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "",
        )
        assert result == _DEFAULT_ONTOLOGY_CONFIG_PATH

    def test_healthcare_bundle_derives_correctly(self):
        result = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "healthcare",
        )
        assert result == "configurations/ontology_bundles/healthcare.yaml"

    def test_formal_bundle_derives_correctly(self):
        result = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "fhir_r4",
        )
        assert result == "configurations/ontology_bundles/fhir_r4.yaml"


class TestOntologyConfigPathDerivation:
    """Integration tests verifying that OntologyLoader.load_config
    succeeds with derived bundle paths for real bundle files."""

    def test_general_bundle_loads_general_entities(self):
        path = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "general",
        )
        config = OntologyLoader.load_config(path)
        defs = config.get("entities", {}).get("definitions", {})
        assert "Person" in defs
        assert "Organization" in defs
        assert "Patient" not in defs

    def test_healthcare_bundle_loads_healthcare_entities(self):
        path = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "healthcare",
        )
        config = OntologyLoader.load_config(path)
        defs = config.get("entities", {}).get("definitions", {})
        assert "Patient" in defs
        assert "Provider" in defs

    def test_fhir_bundle_loads_with_validation(self):
        path = _resolve_ontology_config_path(
            _DEFAULT_ONTOLOGY_CONFIG_PATH, "fhir_r4",
        )
        config = OntologyLoader.load_config(path)
        defs = config.get("entities", {}).get("definitions", {})
        assert len(defs) > 50
        validation = config.get("validation", {})
        assert validation.get("ai_validation_enabled") is True


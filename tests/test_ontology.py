"""Unit tests for ontology module."""

import pytest
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


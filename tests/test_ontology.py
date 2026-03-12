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

        # Mock entity rows
        ent_row1 = MagicMock()
        ent_row1.entity_id, ent_row1.entity_name, ent_row1.entity_type = "e1", "Patient", "Patient"
        ent_row1.source_tables = ["cat.sch.patients"]
        ent_row2 = MagicMock()
        ent_row2.entity_id, ent_row2.entity_name, ent_row2.entity_type = "e2", "Provider", "Provider"
        ent_row2.source_tables = ["cat.sch.providers"]

        # Mock FK row
        fk_row = MagicMock()
        fk_row.src_table, fk_row.dst_table = "cat.sch.patients", "cat.sch.providers"

        sql_results = [
            MagicMock(collect=MagicMock(return_value=[ent_row1, ent_row2])),
            MagicMock(collect=MagicMock(return_value=[fk_row])),
        ]
        mock_spark.sql.side_effect = sql_results

        result = builder.discover_inter_entity_relationships()
        assert result["edges_added"] == 1
        write_call = mock_spark.sql.return_value.write.mode.return_value.saveAsTable
        # Verify the edge was written with treated_by (not references)
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


"""Tests for ontology_roles canonical registry and property generation."""

import copy
import pytest
import yaml
from pathlib import Path

from dbxmetagen.ontology_roles import (
    PROPERTY_ROLES,
    VALID_ROLE_NAMES,
    infer_role_from_column_name,
    resolve_fk_target,
    property_roles_for_yaml,
)


class TestCanonicalRoles:
    def test_eleven_roles_defined(self):
        assert len(PROPERTY_ROLES) == 11
        assert "pii" not in PROPERTY_ROLES

    def test_valid_role_names_matches_keys(self):
        assert VALID_ROLE_NAMES == frozenset(PROPERTY_ROLES.keys())

    def test_all_roles_have_required_fields(self):
        for role, meta in PROPERTY_ROLES.items():
            assert "description" in meta, f"{role} missing description"
            assert "kind" in meta, f"{role} missing kind"
            assert "semantic_role" in meta, f"{role} missing semantic_role"

    def test_kind_values_valid(self):
        for role, meta in PROPERTY_ROLES.items():
            assert meta["kind"] in ("data_property", "object_property"), f"{role} bad kind"

    def test_object_property_has_object_kind(self):
        assert PROPERTY_ROLES["object_property"]["kind"] == "object_property"

    def test_no_pii_role(self):
        assert "pii" not in PROPERTY_ROLES


class TestInferRole:
    @pytest.fixture
    def entities(self):
        return frozenset({"Patient", "Encounter", "Provider", "Claim", "Organization"})

    def test_pk_by_entity_id(self, entities):
        assert infer_role_from_column_name("patient_id", "Patient", entities) == "primary_key"

    def test_pk_bare_id(self, entities):
        assert infer_role_from_column_name("id", "Patient", entities) == "primary_key"

    def test_fk_to_known_entity(self, entities):
        assert infer_role_from_column_name("encounter_id", "Claim", entities) == "object_property"

    def test_fk_to_unknown_entity(self, entities):
        assert infer_role_from_column_name("widget_id", "Claim", entities) == "object_property"

    def test_label_column_email(self, entities):
        assert infer_role_from_column_name("email", "Patient", entities) == "label"

    def test_business_key_column(self, entities):
        assert infer_role_from_column_name("mrn", "Patient", entities) == "business_key"

    def test_temporal_suffix(self, entities):
        assert infer_role_from_column_name("admission_date", "Encounter", entities) == "temporal"

    def test_measure_suffix(self, entities):
        assert infer_role_from_column_name("total_amount", "Claim", entities) == "measure"

    def test_dimension_suffix(self, entities):
        assert infer_role_from_column_name("claim_status", "Claim", entities) == "dimension"

    def test_geo_column(self, entities):
        assert infer_role_from_column_name("city", "Patient", entities) == "geographic"

    def test_audit_column(self, entities):
        assert infer_role_from_column_name("etl_timestamp", "Patient", entities) == "audit"

    def test_label_column(self, entities):
        assert infer_role_from_column_name("description", "Claim", entities) == "label"

    def test_unknown_returns_none(self, entities):
        assert infer_role_from_column_name("foobar", "Patient", entities) is None


class TestResolveFkTarget:
    def test_resolves_exact(self):
        assert resolve_fk_target("patient_id", frozenset({"Patient"})) == "Patient"

    def test_resolves_plural(self):
        assert resolve_fk_target("claim_id", frozenset({"Claims"})) == "Claims"

    def test_no_match(self):
        assert resolve_fk_target("widget_id", frozenset({"Patient"})) is None

    def test_non_id_column(self):
        assert resolve_fk_target("city", frozenset({"Patient"})) is None


class TestPropertyRolesForYaml:
    def test_returns_dict(self):
        result = property_roles_for_yaml()
        assert isinstance(result, dict)
        assert len(result) == 11

    def test_has_maps_to_kind(self):
        result = property_roles_for_yaml()
        for role, meta in result.items():
            assert "maps_to_kind" in meta, f"{role} missing maps_to_kind"
            assert "description" in meta


class TestPropertyGenerator:
    """Test the generate_bundle_properties module (import from scripts)."""

    @pytest.fixture
    def generator_module(self):
        import sys
        scripts_dir = str(Path(__file__).parent.parent / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        import generate_bundle_properties as mod
        return mod

    def test_curated_strategy_healthcare(self, generator_module):
        bundle_path = (
            Path(__file__).parent.parent / "configurations" / "ontology_bundles" / "healthcare.yaml"
        )
        if not bundle_path.exists():
            pytest.skip("healthcare.yaml not available")
        raw = yaml.safe_load(bundle_path.read_text())
        bundle_copy = copy.deepcopy(raw)
        ent_count, new_props, preserved = generator_module.process_bundle(bundle_copy, "healthcare")
        assert ent_count > 0
        assert new_props >= 0
        defs = bundle_copy["ontology"]["entities"]["definitions"]
        for ent_name, defn in defs.items():
            props = defn.get("properties", {})
            if isinstance(props, dict):
                for pname, pinfo in props.items():
                    assert pinfo.get("role") in VALID_ROLE_NAMES, (
                        f"{ent_name}.{pname} has invalid role {pinfo.get('role')}"
                    )

    def test_owl_strategy_omop(self, generator_module):
        bundle_path = (
            Path(__file__).parent.parent / "configurations" / "ontology_bundles" / "omop_cdm.yaml"
        )
        if not bundle_path.exists():
            pytest.skip("omop_cdm.yaml not available")
        raw = yaml.safe_load(bundle_path.read_text())
        # Strip existing properties to test generation
        for defn in raw.get("ontology", {}).get("entities", {}).get("definitions", {}).values():
            defn.pop("properties", None)
        bundle_copy = copy.deepcopy(raw)
        ent_count, new_props, preserved = generator_module.process_bundle(bundle_copy, "omop_cdm")
        assert ent_count > 0
        assert new_props > 0
        defs = bundle_copy["ontology"]["entities"]["definitions"]
        has_obj = any(
            p.get("role") == "object_property"
            for d in defs.values()
            for p in d.get("properties", {}).values()
            if isinstance(d.get("properties"), dict)
        )
        assert has_obj, "OMOP should have object_property entries"

    def test_detect_source_formal(self, generator_module):
        assert generator_module.detect_source({"metadata": {"name": "FHIR R4", "bundle_type": "formal_ontology"}}) == "fhir_r4"
        assert generator_module.detect_source({"metadata": {"name": "OMOP CDM", "bundle_type": "formal_ontology"}}) == "omop_cdm"
        assert generator_module.detect_source({"metadata": {"name": "Schema.org", "bundle_type": "formal_ontology"}}) == "schema_org"

    def test_detect_source_curated(self, generator_module):
        assert generator_module.detect_source({"metadata": {"name": "Healthcare"}}) == "curated"

    def test_preserves_existing_properties(self, generator_module):
        bundle = {
            "metadata": {"name": "Test"},
            "ontology": {
                "entities": {
                    "definitions": {
                        "Widget": {
                            "description": "A widget",
                            "typical_attributes": ["widget_id", "name", "created_date"],
                            "properties": {
                                "widget_id": {
                                    "kind": "data_property",
                                    "role": "primary_key",
                                    "typical_attributes": ["widget_id", "wid"],
                                },
                            },
                        }
                    }
                },
            },
        }
        _, _, preserved = generator_module.process_bundle(bundle, "test")
        assert preserved == 1
        props = bundle["ontology"]["entities"]["definitions"]["Widget"]["properties"]
        assert "wid" in props["widget_id"]["typical_attributes"]

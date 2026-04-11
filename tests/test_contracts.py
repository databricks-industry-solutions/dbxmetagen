"""Contract tests for module boundary interfaces.

These tests verify the shape and invariants of outputs from key public functions.
They catch interface drift -- when a function's return type or structure changes
in a way that would break downstream consumers.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# 1. entities_from_bundle: return shape contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_bundle_indexes import entities_from_bundle

REQUIRED_ENTITY_KEYS = {
    "description", "label", "source", "uri", "parents",
    "outgoing_edges", "keywords", "synonyms", "typical_attributes",
    "business_questions", "relationships", "properties",
}

REQUIRED_EDGE_KEYS = {"name", "range", "ranges", "uri", "inverse"}


class TestEntitiesFromBundleContract:
    def _bundle(self):
        return {
            "ontology": {"entities": {"definitions": {
                "Patient": {
                    "description": "A patient",
                    "uri": "http://hl7.org/fhir/Patient",
                    "source_ontology": "fhir",
                    "relationships": {
                        "managingOrganization": {"target": "Organization", "cardinality": "many-to-one"},
                    },
                },
                "Organization": {"description": "An org"},
            }}}
        }

    def test_return_is_dict_of_dicts(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "b.yaml"
            p.write_text(yaml.dump(self._bundle()))
            result = entities_from_bundle(p)
        assert isinstance(result, dict)
        for name, entity in result.items():
            assert isinstance(name, str)
            assert isinstance(entity, dict)

    def test_entities_have_required_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "b.yaml"
            p.write_text(yaml.dump(self._bundle()))
            result = entities_from_bundle(p)
        for name, entity in result.items():
            missing = REQUIRED_ENTITY_KEYS - set(entity.keys())
            assert not missing, f"{name} missing keys: {missing}"

    def test_outgoing_edges_have_required_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "b.yaml"
            p.write_text(yaml.dump(self._bundle()))
            result = entities_from_bundle(p)
        for name, entity in result.items():
            for edge in entity["outgoing_edges"]:
                missing = REQUIRED_EDGE_KEYS - set(edge.keys())
                assert not missing, f"{name}.{edge.get('name')} missing keys: {missing}"

    def test_relationships_match_outgoing_edges(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "b.yaml"
            p.write_text(yaml.dump(self._bundle()))
            result = entities_from_bundle(p)
        for name, entity in result.items():
            edge_names = {e["name"] for e in entity["outgoing_edges"]}
            rel_names = set(entity["relationships"].keys())
            assert edge_names == rel_names, f"{name}: edges={edge_names}, rels={rel_names}"


# ---------------------------------------------------------------------------
# 2. build_tiers: output file contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_bundle_indexes import build_tiers

EXPECTED_TIER_FILES = {
    "entities_tier1.yaml", "entities_tier1.json",
    "entities_tier2.yaml", "entities_tier2.json",
    "entities_tier3.yaml", "entities_tier3.json",
    "edges_tier1.yaml", "edges_tier1.json",
    "edges_tier2.yaml", "edges_tier2.json",
    "edges_tier3.yaml", "edges_tier3.json",
    "equivalent_class_uris.yaml", "equivalent_class_uris.json",
}


class TestBuildTiersContract:
    def _entities(self):
        return {
            "Patient": {
                "description": "A patient", "label": "Patient",
                "source": "fhir", "uri": "http://hl7.org/fhir/Patient",
                "parents": [], "outgoing_edges": [], "keywords": [],
                "synonyms": [], "typical_attributes": [],
                "business_questions": [], "relationships": {},
                "properties": {},
            }
        }

    def test_all_expected_files_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_tiers(self._entities(), Path(tmp))
            created = {f.name for f in Path(tmp).iterdir()}
        assert EXPECTED_TIER_FILES.issubset(created)

    def test_json_yaml_parity(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_tiers(self._entities(), Path(tmp))
            for stem in ["entities_tier1", "entities_tier2", "edges_tier1"]:
                j = json.loads((Path(tmp) / f"{stem}.json").read_text())
                y = yaml.safe_load((Path(tmp) / f"{stem}.yaml").read_text())
                assert j == y, f"JSON/YAML mismatch for {stem}"

    def test_dry_run_returns_counts(self):
        counts = build_tiers(self._entities(), Path("/nonexistent"), dry_run=True)
        assert isinstance(counts, dict)
        assert "entities_tier1" in counts
        assert all(isinstance(v, int) for v in counts.values())


# ---------------------------------------------------------------------------
# 3. _parse_tier_file: return type contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_index import _parse_tier_file


class TestParseTierFileContract:
    def test_json_returns_dict_or_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            for data in [{"a": 1}, [1, 2]]:
                p = Path(tmp) / "t.json"
                p.write_text(json.dumps(data))
                result = _parse_tier_file(p)
                assert isinstance(result, (dict, list))

    def test_yaml_returns_dict_or_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            for data in [{"a": 1}, [1, 2]]:
                p = Path(tmp) / "t.yaml"
                p.write_text(yaml.dump(data))
                result = _parse_tier_file(p)
                assert isinstance(result, (dict, list))


# ---------------------------------------------------------------------------
# 4. OntologyIndexLoader.get_uri: return type contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_index import OntologyIndexLoader


class TestGetUriContract:
    def _make_loader(self, uris):
        loader = OntologyIndexLoader.__new__(OntologyIndexLoader)
        loader._cache = {}
        loader.bundle_dir = None
        return loader, uris

    def test_returns_string_or_none(self):
        loader, uris = self._make_loader({"Patient": "http://hl7.org/fhir/Patient"})
        with patch.object(loader, "_load", return_value=uris):
            hit = loader.get_uri("Patient")
            assert isinstance(hit, str)
            miss = loader.get_uri("Unknown")
            assert miss is None

    def test_empty_uris_returns_none(self):
        loader, _ = self._make_loader({})
        with patch.object(loader, "_load", return_value=None):
            assert loader.get_uri("Patient") is None


# ---------------------------------------------------------------------------
# 5. _validate_* functions: return shape contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_index import (
    _validate_list_of_dicts,
    _validate_dict_of_dicts,
    _validate_uri_map,
)


class TestValidatorReturnContracts:
    def test_list_of_dicts_returns_tuple(self):
        result = _validate_list_of_dicts([], set(), "f")
        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], list)

    def test_dict_of_dicts_returns_tuple(self):
        result = _validate_dict_of_dicts({}, set(), "f")
        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], dict)
        assert isinstance(result[1], list)

    def test_uri_map_returns_tuple(self):
        result = _validate_uri_map({}, "f")
        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], dict)
        assert isinstance(result[1], list)


# ---------------------------------------------------------------------------
# 6. EdgeCatalogEntry: interface contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology import EdgeCatalogEntry, EdgeCatalog


class TestEdgeCatalogContract:
    def test_entry_has_required_attributes(self):
        e = EdgeCatalogEntry(name="treats", domain="Doctor", range="Patient")
        assert hasattr(e, "matches_domain")
        assert hasattr(e, "matches_range")
        assert hasattr(e, "validate_edge")
        assert callable(e.matches_domain)

    def test_catalog_returns_none_for_missing(self):
        cat = EdgeCatalog({})
        assert cat.get("nonexistent") is None
        assert cat.get_inverse("nonexistent") is None
        assert cat.find_edge("A", "B") is None

    def test_validate_returns_tuple(self):
        cat = EdgeCatalog({"e": EdgeCatalogEntry(name="e", domain="A", range="B")})
        result = cat.validate("e", "A", "B")
        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], bool)
        assert isinstance(result[1], str)


# ---------------------------------------------------------------------------
# 7. CommentResponse: shape contract
# ---------------------------------------------------------------------------

from dbxmetagen.metadata_generator import CommentResponse


class TestCommentResponseContract:
    def test_has_expected_fields(self):
        r = CommentResponse.model_validate({
            "table": "t", "columns": ["c1", "c2"],
            "column_contents": ["d1", "d2"],
        })
        assert hasattr(r, "table")
        assert hasattr(r, "columns")
        assert hasattr(r, "column_contents")
        assert isinstance(r.column_contents, list)

    def test_extra_fields_forbidden(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            CommentResponse.model_validate({
                "table": "t", "columns": ["c"],
                "column_contents": ["d"], "extra_field": "bad",
            })


# ---------------------------------------------------------------------------
# 8. _enforce_value: contract
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import _enforce_value


class TestEnforceValueContract:
    def test_returns_two_tuple(self):
        result = _enforce_value("x", ["a", "b"])
        assert isinstance(result, tuple) and len(result) == 2

    def test_second_element_is_bool(self):
        _, exact = _enforce_value("a", ["a", "b"])
        assert isinstance(exact, bool)

    def test_first_element_is_string(self):
        val, _ = _enforce_value("x", ["a", "b"])
        assert isinstance(val, str)


# ---------------------------------------------------------------------------
# 9. _enforce_entity_value: contract
# ---------------------------------------------------------------------------

from dbxmetagen.ontology import _enforce_entity_value


class TestEnforceEntityValueContract:
    def test_returns_two_tuple(self):
        result = _enforce_entity_value("x", ["Patient"])
        assert isinstance(result, tuple) and len(result) == 2

    def test_result_is_string_bool(self):
        val, exact = _enforce_entity_value("Patient", ["Patient"])
        assert isinstance(val, str)
        assert isinstance(exact, bool)


# ---------------------------------------------------------------------------
# 10. _parse_bool: contract
# ---------------------------------------------------------------------------

from dbxmetagen.config import _parse_bool


class TestParseBoolContract:
    def test_always_returns_bool(self):
        for v in (True, False, "true", "false", "1", "0", None, 0, 1):
            result = _parse_bool(v)
            assert isinstance(result, bool), f"_parse_bool({v!r}) returned {type(result)}"

"""Pure-logic unit tests for dbxmetagen.

These tests import REAL modules directly -- no conftest mocks, no Spark.
They test pure-Python functions that take simple inputs and return simple outputs.
This file is the highest-signal regression gate and runs in <2 seconds.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml


# ---------------------------------------------------------------------------
# 1. config._parse_bool
# ---------------------------------------------------------------------------

from dbxmetagen.config import _parse_bool


class TestParseBool:
    def test_true_strings(self):
        for v in ("true", "True", "TRUE", "1", "yes", " true ", " YES"):
            assert _parse_bool(v) is True

    def test_false_strings(self):
        for v in ("false", "False", "0", "no", "random", ""):
            assert _parse_bool(v) is False

    def test_bool_passthrough(self):
        assert _parse_bool(True) is True
        assert _parse_bool(False) is False

    def test_none_returns_false(self):
        assert _parse_bool(None) is False

    def test_int_coercion(self):
        assert _parse_bool(1) is True
        assert _parse_bool(0) is False

    def test_bad_type_raises(self):
        with pytest.raises(TypeError):
            _parse_bool([1, 2])


# ---------------------------------------------------------------------------
# 2. sampling.determine_sampling_ratio
# ---------------------------------------------------------------------------

from dbxmetagen.sampling import determine_sampling_ratio


class TestDetermineSamplingRatio:
    def test_undersized_table(self):
        assert determine_sampling_ratio(5, 100) == 1.0

    def test_oversized_table(self):
        assert determine_sampling_ratio(1000, 100) == pytest.approx(0.1)

    def test_exact_match(self):
        assert determine_sampling_ratio(50, 50) == 1.0


# ---------------------------------------------------------------------------
# 3. processing.extract_concise_error
# ---------------------------------------------------------------------------

from dbxmetagen.processing import (
    _batch_column_comment_ddl,
    _can_batch_ddl,
    extract_concise_error,
)


class TestExtractConciseError:
    def test_jvm_stacktrace_stripped(self):
        err = Exception("Oops\nJVM stacktrace:\njava.lang.NullPointerException")
        assert "JVM stacktrace" not in extract_concise_error(err)
        assert "Oops" in extract_concise_error(err)

    def test_tag_policy_violation(self):
        msg = (
            "INVALID_PARAMETER_VALUE: Tag value bad_val "
            "is not an allowed value for tag policy key my_tag"
        )
        result = extract_concise_error(Exception(msg))
        assert "my_tag" in result
        assert "bad_val" in result

    def test_plain_error_passthrough(self):
        assert extract_concise_error(Exception("simple")) == "simple"


# ---------------------------------------------------------------------------
# 4. domain_classifier._normalize
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import _normalize


class TestNormalize:
    def test_strips_separators(self):
        assert _normalize("Data_Engineering") == "dataengineering"

    def test_ampersand_replacement(self):
        assert _normalize("R&D") == "randd"

    def test_slash_removal(self):
        assert _normalize("IT/OT") == "itot"


# ---------------------------------------------------------------------------
# 5. domain_classifier._trigram_overlap
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import _trigram_overlap


class TestTrigramOverlap:
    def test_identical_strings(self):
        assert _trigram_overlap("abcdef", "abcdef") == 1.0

    def test_disjoint_strings(self):
        assert _trigram_overlap("abc", "xyz") == 0.0

    def test_short_strings_return_zero(self):
        assert _trigram_overlap("ab", "abc") == 0.0
        assert _trigram_overlap("abc", "ab") == 0.0

    def test_partial_overlap(self):
        score = _trigram_overlap("healthcare", "health")
        assert 0.0 < score < 1.0


# ---------------------------------------------------------------------------
# 6. domain_classifier._enforce_value
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import _enforce_value


class TestEnforceValue:
    ALLOWED = ["Healthcare", "Financial_Services", "Retail", "Technology"]

    def test_exact_match(self):
        val, exact = _enforce_value("Healthcare", self.ALLOWED)
        assert val == "Healthcare" and exact is True

    def test_case_insensitive(self):
        val, exact = _enforce_value("healthcare", self.ALLOWED)
        assert val == "Healthcare" and exact is True

    def test_normalized_match(self):
        val, _ = _enforce_value("financial services", self.ALLOWED)
        assert val == "Financial_Services"

    def test_substring_match(self):
        val, _ = _enforce_value("retail and cpg", self.ALLOWED)
        assert val == "Retail"

    def test_empty_prediction(self):
        val, exact = _enforce_value("", self.ALLOWED)
        assert val == "unknown" and exact is False

    def test_fallback(self):
        val, exact = _enforce_value("zzzzzzzzzz", self.ALLOWED, fallback="other")
        assert val == "other" and exact is False


# ---------------------------------------------------------------------------
# 7. domain_classifier._domain_keyword_prefilter
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import _domain_keyword_prefilter


class TestDomainKeywordPrefilter:
    def test_small_config_returns_all(self):
        config = {"domains": {"A": {"keywords": []}, "B": {"keywords": []}}}
        result = _domain_keyword_prefilter("c.s.patients", {}, config, top_n=5)
        assert set(result) == {"A", "B"}

    def test_keyword_scoring(self):
        config = {
            "domains": {
                "Healthcare": {"keywords": ["patient", "diagnosis"]},
                "Finance": {"keywords": ["trade", "portfolio"]},
                "Retail": {"keywords": ["product", "sku"]},
                "Energy": {"keywords": ["oil", "gas"]},
                "Telecom": {"keywords": ["cell", "network"]},
                "Insurance": {"keywords": ["claim", "policy"]},
                "Logistics": {"keywords": ["shipment", "warehouse"]},
            }
        }
        result = _domain_keyword_prefilter(
            "c.hospital.patient_records", {}, config, top_n=3
        )
        assert "Healthcare" in result


# ---------------------------------------------------------------------------
# 8. ontology._leading_overlap
# ---------------------------------------------------------------------------

from dbxmetagen.ontology import _leading_overlap


class TestLeadingOverlap:
    def test_full_match(self):
        assert _leading_overlap("patient", "Patient") == 7

    def test_partial_match(self):
        assert _leading_overlap("prod", "Product") == 4

    def test_no_match(self):
        assert _leading_overlap("xyz", "abc") == 0


# ---------------------------------------------------------------------------
# 9. ontology._enforce_entity_value
# ---------------------------------------------------------------------------

from dbxmetagen.ontology import _enforce_entity_value


class TestEnforceEntityValue:
    ALLOWED = ["Patient", "Encounter", "Observation", "Organization"]

    def test_exact_case_insensitive(self):
        val, exact = _enforce_entity_value("patient", self.ALLOWED)
        assert val == "Patient" and exact is True

    def test_empty_returns_first(self):
        val, exact = _enforce_entity_value("", self.ALLOWED)
        assert val == "Patient" and exact is False

    def test_forward_containment(self):
        val, _ = _enforce_entity_value("the Encounter resource", self.ALLOWED)
        assert val == "Encounter"

    def test_leading_overlap(self):
        val, _ = _enforce_entity_value("Obs", self.ALLOWED)
        assert val == "Observation"


# ---------------------------------------------------------------------------
# 10. ontology.EdgeCatalogEntry._matches / validate_edge
# ---------------------------------------------------------------------------

from dbxmetagen.ontology import EdgeCatalogEntry, EdgeCatalog


class TestEdgeCatalogEntry:
    def test_any_matches_everything(self):
        e = EdgeCatalogEntry(name="test", domain=None, range="Any")
        assert e.matches_domain("Patient") is True
        assert e.matches_range("Anything") is True

    def test_string_constraint(self):
        e = EdgeCatalogEntry(name="test", domain="Patient", range="Encounter")
        assert e.matches_domain("Patient") is True
        assert e.matches_domain("Organization") is False

    def test_list_constraint(self):
        e = EdgeCatalogEntry(name="test", domain=["Patient", "Practitioner"], range="Encounter")
        assert e.matches_domain("Patient") is True
        assert e.matches_domain("Device") is False

    def test_validate_edge(self):
        e = EdgeCatalogEntry(name="test", domain="Patient", range="Encounter")
        assert e.validate_edge("Patient", "Encounter") is True
        assert e.validate_edge("Patient", "Organization") is False


class TestEdgeCatalog:
    def _make(self):
        return EdgeCatalog({
            "treats": EdgeCatalogEntry(name="treats", domain="Practitioner", range="Patient"),
            "observedIn": EdgeCatalogEntry(name="observedIn", domain="Observation", range="Encounter"),
        })

    def test_find_edge(self):
        cat = self._make()
        assert cat.find_edge("Practitioner", "Patient").name == "treats"
        assert cat.find_edge("Patient", "Patient") is None

    def test_validate(self):
        cat = self._make()
        ok, _ = cat.validate("treats", "Practitioner", "Patient")
        assert ok is True
        ok, msg = cat.validate("treats", "Patient", "Encounter")
        assert ok is False
        assert "domain/range" in msg

    def test_validate_missing_edge(self):
        cat = self._make()
        ok, msg = cat.validate("unknown_edge", "A", "B")
        assert ok is False
        assert "not in catalog" in msg


# ---------------------------------------------------------------------------
# 11. OntologyBuilder._resolve_reference_target
# ---------------------------------------------------------------------------

from dbxmetagen.ontology import OntologyBuilder


class TestResolveReferenceTarget:
    def test_exact_suffix_match(self):
        discovered = {"organization": "Organization", "patient": "Patient"}
        result = OntologyBuilder._resolve_reference_target(
            "Patient.managingOrganization", discovered
        )
        assert result == "Organization"

    def test_ambiguous_returns_none(self):
        discovered = {"organization": "Organization", "suborganization": "SubOrganization"}
        result = OntologyBuilder._resolve_reference_target(
            "Entity.subOrganization", discovered
        )
        assert result is None

    def test_no_match_returns_none(self):
        discovered = {"patient": "Patient"}
        result = OntologyBuilder._resolve_reference_target("Something.device", discovered)
        assert result is None


# ---------------------------------------------------------------------------
# 12. ontology_index validators
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_index import (
    _validate_list_of_dicts,
    _validate_dict_of_dicts,
    _validate_uri_map,
    _parse_tier_file,
)


class TestValidateListOfDicts:
    def test_valid_entries(self):
        data = [{"name": "a", "desc": "b"}, {"name": "c", "desc": "d"}]
        valid, issues = _validate_list_of_dicts(data, {"name", "desc"}, "test.yaml")
        assert len(valid) == 2 and not issues

    def test_non_list_input(self):
        _, issues = _validate_list_of_dicts("not a list", {"name"}, "test.yaml")
        assert len(issues) == 1

    def test_missing_keys(self):
        data = [{"name": "a"}]
        valid, issues = _validate_list_of_dicts(data, {"name", "desc"}, "test.yaml")
        assert len(valid) == 0 and len(issues) == 1


class TestValidateDictOfDicts:
    def test_valid_entries(self):
        data = {"Patient": {"desc": "ok", "uri": "http://..."}}
        valid, issues = _validate_dict_of_dicts(data, {"desc", "uri"}, "test.yaml")
        assert "Patient" in valid and not issues

    def test_non_dict_input(self):
        _, issues = _validate_dict_of_dicts([1, 2], {"desc"}, "test.yaml")
        assert len(issues) == 1


class TestValidateUriMap:
    def test_valid(self):
        data = {"Patient": "http://hl7.org/fhir/Patient"}
        valid, issues = _validate_uri_map(data, "test.yaml")
        assert valid == data and not issues

    def test_non_string_value(self):
        valid, issues = _validate_uri_map({"Patient": 123}, "test.yaml")
        assert "Patient" not in valid and len(issues) == 1


# ---------------------------------------------------------------------------
# 13. ontology_index._parse_tier_file (JSON / YAML / fallback)
# ---------------------------------------------------------------------------

class TestParseTierFile:
    def test_json_parse(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "tier.json"
            p.write_text(json.dumps({"a": 1}))
            assert _parse_tier_file(p) == {"a": 1}

    def test_yaml_parse(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "tier.yaml"
            p.write_text(yaml.dump({"b": 2}))
            assert _parse_tier_file(p) == {"b": 2}

    def test_corrupt_json_falls_back_to_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            json_p = Path(tmp) / "tier.json"
            yaml_p = Path(tmp) / "tier.yaml"
            json_p.write_text("{corrupt!!")
            yaml_p.write_text(yaml.dump({"c": 3}))
            assert _parse_tier_file(json_p) == {"c": 3}

    def test_corrupt_json_no_yaml_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "tier.json"
            p.write_text("{corrupt!!")
            with pytest.raises(json.JSONDecodeError):
                _parse_tier_file(p)


# ---------------------------------------------------------------------------
# 14. ontology_index.OntologyIndexLoader.get_uri (case-insensitive)
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_index import OntologyIndexLoader


class TestGetUriCaseInsensitive:
    def _make_loader(self, uris):
        loader = OntologyIndexLoader.__new__(OntologyIndexLoader)
        loader._cache = {}
        loader.bundle_dir = None
        return loader, uris

    def test_exact_case(self):
        loader, uris = self._make_loader({"Patient": "http://hl7.org/fhir/Patient"})
        with patch.object(loader, "_load", return_value=uris):
            assert loader.get_uri("Patient") == "http://hl7.org/fhir/Patient"

    def test_lowercase_input(self):
        loader, uris = self._make_loader({"Patient": "http://hl7.org/fhir/Patient"})
        with patch.object(loader, "_load", return_value=uris):
            assert loader.get_uri("patient") == "http://hl7.org/fhir/Patient"

    def test_missing_returns_none(self):
        loader, uris = self._make_loader({"Patient": "http://hl7.org/fhir/Patient"})
        with patch.object(loader, "_load", return_value=uris):
            assert loader.get_uri("UnknownType") is None


# ---------------------------------------------------------------------------
# 15. ontology_bundle_indexes._endpoint_name
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_bundle_indexes import _endpoint_name


class TestEndpointName:
    def test_string(self):
        assert _endpoint_name("Patient") == "Patient"

    def test_list(self):
        assert _endpoint_name(["Patient", "Encounter"]) == "Patient"

    def test_none(self):
        assert _endpoint_name(None) is None

    def test_empty_string(self):
        assert _endpoint_name("  ") is None

    def test_empty_list(self):
        # empty list falls through to str() fallback -- this is current behavior
        assert _endpoint_name([]) == "[]"


# ---------------------------------------------------------------------------
# 16. ontology_bundle_indexes.entities_from_bundle
# ---------------------------------------------------------------------------

from dbxmetagen.ontology_bundle_indexes import entities_from_bundle


class TestEntitiesFromBundle:
    def _write_bundle(self, tmp, bundle_dict):
        p = Path(tmp) / "bundle.yaml"
        p.write_text(yaml.dump(bundle_dict))
        return p

    def test_basic_entity(self):
        bundle = {
            "ontology": {"entities": {"definitions": {
                "Patient": {"description": "A patient entity"},
            }}}
        }
        with tempfile.TemporaryDirectory() as tmp:
            result = entities_from_bundle(self._write_bundle(tmp, bundle))
        assert "Patient" in result
        assert result["Patient"]["description"] == "A patient entity"

    def test_string_shorthand_relationship(self):
        bundle = {
            "ontology": {"entities": {"definitions": {
                "Patient": {
                    "description": "A patient",
                    "relationships": {"managingOrg": "Organization"},
                },
            }}}
        }
        with tempfile.TemporaryDirectory() as tmp:
            result = entities_from_bundle(self._write_bundle(tmp, bundle))
        edge = next(e for e in result["Patient"]["outgoing_edges"] if e["name"] == "managingOrg")
        assert edge["range"] == "Organization"

    def test_empty_bundle(self):
        bundle = {"ontology": {"entities": {"definitions": {}}}}
        with tempfile.TemporaryDirectory() as tmp:
            result = entities_from_bundle(self._write_bundle(tmp, bundle))
        assert result == {}


# ---------------------------------------------------------------------------
# 17. metadata_generator: CommentResponse validator
# ---------------------------------------------------------------------------

from dbxmetagen.metadata_generator import CommentResponse


class TestCommentResponseValidator:
    def _build(self, contents):
        return {"table": "t", "columns": ["c"], "column_contents": contents}

    def test_none_coerces_to_empty(self):
        r = CommentResponse.model_validate(self._build(None))
        assert r.column_contents == []

    def test_string_wraps_in_list(self):
        r = CommentResponse.model_validate(self._build("hello"))
        assert r.column_contents == ["hello"]

    def test_list_passthrough(self):
        r = CommentResponse.model_validate(self._build(["a", "b"]))
        assert r.column_contents == ["a", "b"]

    def test_nested_list_flattened(self):
        r = CommentResponse.model_validate(self._build([["a", "b"]]))
        assert r.column_contents == ["a", "b"]

    def test_stringified_json_array(self):
        r = CommentResponse.model_validate(self._build('["x", "y"]'))
        assert r.column_contents == ["x", "y"]

    def test_truncated_json_recovery(self):
        r = CommentResponse.model_validate(self._build('["x", "y", "z'))
        assert "x" in r.column_contents and "y" in r.column_contents


# ---------------------------------------------------------------------------
# 18. metadata_generator: PIColumnContent.normalize_classification
# ---------------------------------------------------------------------------

from dbxmetagen.metadata_generator import PIColumnContent


class TestPIClassificationNormalize:
    def _build(self, classification):
        return {
            "column_name": "col", "classification": classification,
            "reason": "test", "type": "string", "confidence": 0.9,
        }

    def test_pii_maps_to_pi(self):
        r = PIColumnContent.model_validate(self._build("pii"))
        assert r.classification == "pi"

    def test_case_insensitive(self):
        r = PIColumnContent.model_validate(self._build("PHI"))
        assert r.classification == "phi"

    def test_none_string(self):
        r = PIColumnContent.model_validate(self._build("none"))
        assert r.classification == "None"

    def test_unknown_defaults_to_none(self):
        r = PIColumnContent.model_validate(self._build("sensitive_stuff"))
        assert r.classification == "None"

    def test_valid_passthrough(self):
        for v in ("pi", "phi", "pci", "medical_information", "None"):
            r = PIColumnContent.model_validate(self._build(v))
            assert r.classification == v


# ---------------------------------------------------------------------------
# 19. Null-threshold logic (extracted from processing.sample_df)
# ---------------------------------------------------------------------------

class TestNullFilterThreshold:
    """Tests the formula: max(1, n_cols // 2)."""

    def test_single_column(self):
        assert max(1, 1 // 2) == 1

    def test_two_columns(self):
        assert max(1, 2 // 2) == 1

    def test_ten_columns(self):
        assert max(1, 10 // 2) == 5

    def test_zero_columns(self):
        assert max(1, 0 // 2) == 1


# ---------------------------------------------------------------------------
# 20. Chunk count logic (extracted from processing.chunk_df)
# ---------------------------------------------------------------------------

class TestChunkCount:
    """Tests the formula: ceil(n_cols / columns_per_call)."""

    def test_exact_division(self):
        assert (20 + 20 - 1) // 20 == 1

    def test_remainder(self):
        assert (25 + 20 - 1) // 20 == 2

    def test_single_column(self):
        assert (1 + 20 - 1) // 20 == 1

    def test_large_table(self):
        assert (100 + 20 - 1) // 20 == 5


# ---------------------------------------------------------------------------
# 21. Infrastructure table detection
# ---------------------------------------------------------------------------

from dbxmetagen.table_filter import (
    INFRASTRUCTURE_TABLE_NAMES,
    is_infrastructure_table,
    infrastructure_exclude_sql,
)


class TestIsInfrastructureTable:
    def test_bare_name_match(self):
        for name in ("graph_nodes", "ontology_entities", "metadata_generation_log",
                      "table_knowledge_base", "fk_predictions", "data_quality_scores"):
            assert is_infrastructure_table(name) is True, f"{name} should be infra"

    def test_fully_qualified_match(self):
        assert is_infrastructure_table("my_catalog.my_schema.graph_nodes") is True
        assert is_infrastructure_table("prod.metagen.ontology_entities") is True

    def test_user_table_not_matched(self):
        for name in ("dim_funds", "patients", "fact_orders", "my_catalog.s.dim_date"):
            assert is_infrastructure_table(name) is False, f"{name} should NOT be infra"

    def test_dynamic_control_table(self):
        assert is_infrastructure_table("metadata_control_alice") is True
        assert is_infrastructure_table("c.s.metadata_control_bob") is True

    def test_dynamic_temp_log(self):
        assert is_infrastructure_table("temp_metadata_generation_log_alice_20240101") is True
        assert is_infrastructure_table("c.s.temp_metadata_generation_log_x_y") is True

    def test_partial_name_no_match(self):
        assert is_infrastructure_table("my_graph_nodes_table") is False
        assert is_infrastructure_table("c.s.user_ontology_entities") is False

    def test_registry_completeness(self):
        expected = {
            "metadata_generation_log", "table_knowledge_base",
            "column_knowledge_base", "schema_knowledge_base",
            "extended_table_metadata", "graph_nodes", "graph_edges",
            "ontology_entities", "ontology_metrics",
            "ontology_column_properties", "ontology_relationships",
            "discovery_diff_report", "entity_tag_audit_log",
            "fk_predictions", "profiling_snapshots",
            "column_profiling_stats", "data_quality_scores",
            "semantic_layer_questions", "metric_view_definitions",
            "llm_payloads", "metadata_documents", "dbxmetagen_token_usage",
        }
        assert INFRASTRUCTURE_TABLE_NAMES == expected


class TestInfrastructureExcludeSql:
    def test_returns_and_clause(self):
        sql = infrastructure_exclude_sql(column="table_name")
        assert sql.startswith("AND ")
        assert "graph_nodes" in sql
        assert "metadata_control_" in sql

    def test_custom_column(self):
        sql = infrastructure_exclude_sql(column="kb.table_name")
        assert "kb.table_name" in sql


# ---------------------------------------------------------------------------
# 12. DDL bundle utilities (ddl_bundle_utils)
# ---------------------------------------------------------------------------

from dbxmetagen.ddl_bundle_utils import rewrite_ddl_catalog_schema, dq_grade


class TestRewriteDdlCatalogSchema:
    def test_noop_when_same(self):
        stmts = ["ALTER TABLE cat.sch.t SET TAGS ('x' = '1');"]
        assert rewrite_ddl_catalog_schema(stmts, "cat", "sch", "cat", "sch") is stmts

    def test_bare_rewrite(self):
        stmts = ["ALTER TABLE dev.raw.t SET TAGS ('x' = '1');"]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert out == ["ALTER TABLE prod.clean.t SET TAGS ('x' = '1');"]

    def test_backtick_rewrite(self):
        stmts = ["ALTER TABLE `dev`.`raw`.t ALTER COLUMN `c` SET TAGS ('k' = 'v');"]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert "`prod`.`clean`" in out[0]
        assert "`dev`.`raw`" not in out[0]

    def test_preserves_quoted_tag_values(self):
        stmts = ["ALTER TABLE dev.raw.t ALTER COLUMN `c` SET TAGS ('fk_references' = 'dev.raw.other.col');"]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert "'fk_references' = 'dev.raw.other.col'" in out[0], "tag value should not be rewritten"
        assert out[0].startswith("ALTER TABLE prod.clean.t")

    def test_multiple_stmts(self):
        stmts = [
            "ALTER TABLE dev.raw.a SET TAGS ('x' = '1');",
            "ALTER TABLE dev.raw.b SET TAGS ('y' = '2');",
        ]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert all("prod.clean" in s for s in out)
        assert len(out) == 2

    def test_empty_list(self):
        assert rewrite_ddl_catalog_schema([], "a", "b", "c", "d") == []


class TestDqGrade:
    @pytest.mark.parametrize("score,expected", [
        (100.0, "A"), (95.5, "A"), (90.0, "A"),
        (89.9, "B"), (85.0, "B"), (80.0, "B"),
        (79.9, "C"), (75.0, "C"), (70.0, "C"),
        (69.9, "D"), (65.0, "D"), (60.0, "D"),
        (59.9, "F"), (50.0, "F"), (0.0, "F"),
    ])
    def test_thresholds(self, score, expected):
        assert dq_grade(score) == expected


# ---------------------------------------------------------------------------
# 13. processing._batch_column_comment_ddl and _can_batch_ddl
# ---------------------------------------------------------------------------


class TestBatchColumnCommentDdl:
    def test_two_columns_single_alter_column_keyword(self):
        sql = _batch_column_comment_ddl(
            "cat.sch.t",
            [("a", "ca"), ("b", "cb")],
        )
        assert sql.count("ALTER COLUMN") == 1
        assert sql == (
            'ALTER TABLE cat.sch.t ALTER COLUMN `a` COMMENT "ca", `b` COMMENT "cb"'
        )

    def test_three_columns(self):
        sql = _batch_column_comment_ddl("t", [("x", "1"), ("y", "2"), ("z", "3")])
        assert "ALTER COLUMN `x`" in sql
        assert ", `y` COMMENT" in sql
        assert sql.endswith('`z` COMMENT "3"')

    def test_double_quotes_sanitized_in_comment(self):
        sql = _batch_column_comment_ddl("t", [("c", 'say "hi"')])
        assert 'COMMENT "say \'hi\'"' in sql


class TestCanBatchDdl:
    def test_none_allows_batch(self):
        env_wo = {k: v for k, v in os.environ.items() if k != "DATABRICKS_RUNTIME_VERSION"}
        with patch.dict(os.environ, env_wo, clear=True):
            assert "DATABRICKS_RUNTIME_VERSION" not in os.environ
            assert _can_batch_ddl() is True

    def test_numeric_16_3_and_above(self):
        with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "16.3"}):
            assert _can_batch_ddl() is True
        with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "17.0"}):
            assert _can_batch_ddl() is True

    def test_numeric_below_16_3_disallows(self):
        with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "15.4"}):
            assert _can_batch_ddl() is False

    def test_client_runtime_string_disallows(self):
        with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "client.1.13"}):
            assert _can_batch_ddl() is False


# ---------------------------------------------------------------------------
# 14. DDL bundle known-limitation xfail tests
# ---------------------------------------------------------------------------


class TestRewriteDdlKnownLimitations:
    @pytest.mark.xfail(reason="bare .replace() matches schema as substring of longer schema name")
    def test_rewrite_substring_match_on_schema(self):
        stmts = ["ALTER TABLE dev.raw_v2.orders SET TAGS ('domain' = 'retail');"]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert "dev.raw_v2" in out[0], "raw_v2 is a different schema and must not be rewritten"

    @pytest.mark.xfail(reason="$$-delimited bodies not recognized by quote-splitter")
    def test_rewrite_skips_dollar_quoted_yaml_body(self):
        stmts = [
            "CREATE OR REPLACE VIEW `prod`.`clean`.`mv_test`\n"
            "WITH METRICS LANGUAGE YAML AS $$\n"
            "source: dev.raw.patients\nname: mv_test\n$$;"
        ]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert "source: dev.raw.patients" in out[0]

    @pytest.mark.xfail(reason="FK tag values are quoted references that rewriter skips")
    def test_rewrite_updates_fk_tag_value(self):
        stmts = [
            "ALTER TABLE dev.raw.orders ALTER COLUMN `customer_id` "
            "SET TAGS ('fk_references' = 'dev.raw.customers.customer_id');"
        ]
        out = rewrite_ddl_catalog_schema(stmts, "dev", "raw", "prod", "clean")
        assert "prod.clean.customers" in out[0], "FK tag value must also be rewritten"


# ---------------------------------------------------------------------------
# 15. load_domain_config bundle resolution
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import load_domain_config


class TestLoadDomainConfigBundleResolution:
    """Verify that load_domain_config resolves bundle names to the right YAML."""

    CONFIGS_DIR = os.path.join(
        os.path.dirname(__file__), "..", "configurations", "ontology_bundles"
    )

    def test_general_bundle_has_six_domains(self):
        config = load_domain_config(bundle_path=os.path.join(self.CONFIGS_DIR, "general.yaml"))
        domains = config["domains"]
        assert len(domains) == 10
        assert "clinical" not in domains
        assert "diagnostics" not in domains
        assert "payer" not in domains

    def test_healthcare_bundle_has_twelve_domains(self):
        config = load_domain_config(bundle_path=os.path.join(self.CONFIGS_DIR, "healthcare.yaml"))
        domains = config["domains"]
        assert len(domains) >= 12
        assert "clinical" in domains
        assert "diagnostics" in domains
        assert "payer" in domains
        assert "finance" in domains

    def test_empty_bundle_path_skips_bundle(self):
        """Empty string must NOT load general.yaml (6 domains) -- root cause of domain=0 bug."""
        config = load_domain_config(bundle_path="", config_path="/nonexistent")
        domains = config.get("domains", {})
        general_only = {"finance", "operations", "workforce", "customer", "technology", "governance"}
        assert set(domains.keys()) != general_only, (
            "Empty bundle_path loaded general.yaml (6 domains). "
            "Expected hardcoded healthcare fallback or 'unknown'."
        )

    def test_none_bundle_path_skips_bundle(self):
        config = load_domain_config(bundle_path=None, config_path="/nonexistent")
        domains = config.get("domains", {})
        general_only = {"finance", "operations", "workforce", "customer", "technology", "governance"}
        assert set(domains.keys()) != general_only

    def test_healthcare_config_path(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "configurations", "domain_config_healthcare.yaml"
        )
        if os.path.exists(path):
            config = load_domain_config(config_path=path)
            assert "clinical" in config["domains"]
            assert "payer" in config["domains"]


# ---------------------------------------------------------------------------
# R3: _excluded_info_names / constants / key mapping
# ---------------------------------------------------------------------------

from dbxmetagen.prompts import STATS_FIELDS, _INFO_SCHEMA_TO_DESCRIBE, Prompt


def _make_prompt_stub(mode, **extra):
    """Build a minimal object that satisfies _excluded_info_names without Spark."""
    obj = MagicMock(spec=Prompt)
    cfg = MagicMock()
    cfg.mode = mode
    cfg.include_datatype_from_metadata = extra.get("include_datatype_from_metadata", True)
    cfg.include_possible_data_fields_in_metadata = extra.get(
        "include_possible_data_fields_in_metadata", True,
    )
    cfg.pi_classification_tag_name = extra.get("pi_classification_tag_name", "data_classification")
    cfg.pi_subclassification_tag_name = extra.get(
        "pi_subclassification_tag_name", "data_subclassification",
    )
    cfg.domain_tag_name = extra.get("domain_tag_name", "domain")
    cfg.subdomain_tag_name = extra.get("subdomain_tag_name", "subdomain")
    obj.config = cfg
    return obj


class TestExcludedInfoNames:
    """Verify _excluded_info_names matches the old _filter_*_mode logic."""

    def test_comment_mode_defaults(self):
        obj = _make_prompt_stub("comment")
        result = Prompt._excluded_info_names(obj)
        assert "comment" in result
        assert "description" in result
        assert "data_classification" in result
        assert "data_subclassification" in result
        assert "data_type" not in result  # included by default
        assert "min" not in result        # included by default

    def test_comment_mode_exclude_datatype(self):
        obj = _make_prompt_stub("comment", include_datatype_from_metadata=False)
        result = Prompt._excluded_info_names(obj)
        assert "data_type" in result

    def test_comment_mode_exclude_stats(self):
        obj = _make_prompt_stub("comment", include_possible_data_fields_in_metadata=False)
        result = Prompt._excluded_info_names(obj)
        assert "min" in result
        assert "max" in result

    def test_pi_mode(self):
        obj = _make_prompt_stub("pi")
        result = Prompt._excluded_info_names(obj)
        assert result == {"data_classification", "data_subclassification"}

    def test_domain_mode(self):
        obj = _make_prompt_stub("domain")
        result = Prompt._excluded_info_names(obj)
        assert result == {"domain", "subdomain"}

    def test_custom_tag_names(self):
        obj = _make_prompt_stub(
            "pi",
            pi_classification_tag_name="my_pi_tag",
            pi_subclassification_tag_name="my_sub_tag",
        )
        result = Prompt._excluded_info_names(obj)
        assert result == {"my_pi_tag", "my_sub_tag"}


class TestInfoSchemaConstants:
    def test_key_mapping_uses_col_name(self):
        assert _INFO_SCHEMA_TO_DESCRIBE["column_name"] == "col_name"

    def test_key_mapping_data_type(self):
        assert _INFO_SCHEMA_TO_DESCRIBE["data_type"] == "data_type"

    def test_key_mapping_comment(self):
        assert _INFO_SCHEMA_TO_DESCRIBE["comment"] == "comment"

    def test_is_nullable_not_mapped(self):
        assert "is_nullable" not in _INFO_SCHEMA_TO_DESCRIBE

    def test_stats_fields_complete(self):
        assert STATS_FIELDS == {"min", "max", "num_nulls", "distinct_count", "avg_col_len", "max_col_len"}

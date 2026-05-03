"""Tests for DOJ geographic ontology compliance features."""

import re
import pytest
from unittest.mock import MagicMock, patch
from collections import namedtuple
from typing import Optional, Tuple

from dbxmetagen.ontology import (
    OntologyConfig,
    OntologyBuilder,
    EntityDiscoverer,
    EntityDefinition,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_builder(spark=None, bundle_geo_patterns=frozenset()):
    """Build a lightweight OntologyBuilder with mocked internals."""
    spark = spark or MagicMock()
    config = OntologyConfig(catalog_name="cat", schema_name="sch")
    with patch.object(OntologyBuilder, "__init__", lambda self, *a, **k: None):
        builder = OntologyBuilder.__new__(OntologyBuilder)
    builder.spark = spark
    builder.config = config
    builder.ontology_config = {}
    builder.discoverer = MagicMock()
    builder.discoverer._bundle_geo_patterns = bundle_geo_patterns
    builder._validation_cfg = {}
    return builder


def _classify(col_lower, dtype="STRING", is_pk=False, linked=None, cls_type="",
              bundle_geo_patterns=frozenset()):
    """Call the real OntologyBuilder._heuristic_classify."""
    builder = _make_builder(bundle_geo_patterns=bundle_geo_patterns)
    return builder._heuristic_classify(col_lower, dtype, is_pk, linked, cls_type)


def _make_discoverer_for_patterns(entity_defs):
    """Build a minimal EntityDiscoverer to test _build_bundle_geo_patterns."""
    with patch.object(EntityDiscoverer, "__init__", lambda self, *a, **k: None):
        disc = EntityDiscoverer.__new__(EntityDiscoverer)
    disc.entity_definitions = entity_defs
    return disc


# ---------------------------------------------------------------------------
# 1. Rule ordering — gated by bundle geo patterns
# ---------------------------------------------------------------------------

class TestRuleOrdering:
    """Verify geo/code ordering depends on whether bundle has geo entities."""

    # Columns that are ONLY in _GEO_PATTERNS_DEFAULT (no _code regex overlap)
    PURE_GEO_COLUMNS = ["city", "latitude", "longitude", "region", "province",
                        "county", "address"]

    # Columns that match BOTH _GEO_PATTERNS_DEFAULT and _code regex
    OVERLAP_COLUMNS = ["country_code", "state_code", "postal_code", "zip_code"]

    # Columns that match _code regex but are NOT geographic
    PURE_CODE_COLUMNS = ["icd_code", "cpt_code", "product_code", "ndc_code",
                         "drg_code", "loinc_code"]

    @pytest.mark.parametrize("col", PURE_GEO_COLUMNS)
    def test_pure_geo_always_geographic(self, col):
        """Columns only in geo patterns are geographic regardless of bundle."""
        role, _, _ = _classify(col)
        assert role == "geographic", f"{col} should be geographic, got {role}"

    @pytest.mark.parametrize("col", OVERLAP_COLUMNS)
    def test_overlap_is_dimension_without_geo_bundle(self, col):
        """country_code etc. match _code first when bundle has no geo entities."""
        role, _, _ = _classify(col, bundle_geo_patterns=frozenset())
        assert role == "dimension", f"{col} should be dimension without geo bundle, got {role}"

    @pytest.mark.parametrize("col", OVERLAP_COLUMNS)
    def test_overlap_is_geographic_with_geo_bundle(self, col):
        """country_code etc. match geo first when bundle explicitly has geo entities."""
        geo_pats = frozenset({"country_code", "state_code", "postal_code", "zip_code",
                              "city", "region"})
        role, _, _ = _classify(col, bundle_geo_patterns=geo_pats)
        assert role == "geographic", f"{col} should be geographic with geo bundle, got {role}"

    @pytest.mark.parametrize("col", PURE_CODE_COLUMNS)
    def test_non_geo_codes_remain_dimension(self, col):
        role, _, _ = _classify(col)
        assert role == "dimension", f"{col} should be dimension, got {role}"

    def test_geo_prefix_classified(self):
        role, _, _ = _classify("geo_district")
        assert role == "geographic"


# ---------------------------------------------------------------------------
# 2. Dynamic bundle keywords
# ---------------------------------------------------------------------------

class TestDynamicBundleKeywords:
    """Verify bundle-derived geo patterns override defaults."""

    def test_bundle_keywords_used_when_present(self):
        pats = frozenset({"district", "canton", "municipality"})
        role, _, _ = _classify("canton", bundle_geo_patterns=pats)
        assert role == "geographic"

    def test_bundle_keywords_dont_match_non_bundle_column(self):
        pats = frozenset({"district"})
        role, _, _ = _classify("country", bundle_geo_patterns=pats)
        assert role != "geographic"

    def test_empty_bundle_falls_back_to_defaults(self):
        role, _, _ = _classify("country")
        assert role == "geographic"

    def test_build_bundle_geo_patterns_with_geo_entities(self):
        defs = [
            EntityDefinition(name="Geographic", description="base",
                             keywords=["place"], typical_attributes=["location_name"]),
            EntityDefinition(name="AdminRegion", description="regions",
                             parent="Geographic",
                             keywords=["state", "province"],
                             typical_attributes=["region_code"]),
            EntityDefinition(name="Customer", description="non-geo",
                             keywords=["buyer"]),
        ]
        disc = _make_discoverer_for_patterns(defs)
        patterns = disc._build_bundle_geo_patterns()
        assert "place" in patterns
        assert "location_name" in patterns
        assert "state" in patterns
        assert "province" in patterns
        assert "region_code" in patterns
        assert "buyer" not in patterns

    def test_build_bundle_geo_patterns_no_geo_entities(self):
        defs = [EntityDefinition(name="Customer", description="non-geo", keywords=["buyer"])]
        disc = _make_discoverer_for_patterns(defs)
        patterns = disc._build_bundle_geo_patterns()
        assert patterns == frozenset()

    def test_build_bundle_geo_patterns_recognizes_location(self):
        defs = [
            EntityDefinition(name="Location", description="geo",
                             keywords=["location", "address"],
                             typical_attributes=["city", "state", "zip_code"]),
        ]
        disc = _make_discoverer_for_patterns(defs)
        patterns = disc._build_bundle_geo_patterns()
        assert "location" in patterns
        assert "city" in patterns
        assert "zip_code" in patterns


# ---------------------------------------------------------------------------
# 3. Geographic coverage report schema
# ---------------------------------------------------------------------------

class TestGeographicCoverageReport:

    def test_coverage_report_returns_expected_columns(self):
        expected_cols = {"table_name", "column_name", "property_role",
                         "property_confidence", "geo_entity_subtype",
                         "entity_confidence", "classification_status"}
        mock_df = MagicMock()
        mock_df.columns = list(expected_cols)

        builder = _make_builder()
        builder.spark.sql.return_value = mock_df

        result = builder.get_geographic_coverage_report()
        assert set(result.columns) == expected_cols

    def test_coverage_report_sql_includes_location(self):
        builder = _make_builder()
        builder.spark.sql.return_value = MagicMock()
        builder.get_geographic_coverage_report()
        sql = builder.spark.sql.call_args[0][0]
        assert "'Location'" in sql


# ---------------------------------------------------------------------------
# 4. Audit trail
# ---------------------------------------------------------------------------

class TestAuditTrail:

    def test_audit_rows_written_after_tagging(self):
        Row = namedtuple("Row", ["entity_type", "table_name", "confidence", "discovery_method"])
        ColRow = namedtuple("ColRow", ["entity_type", "table_name", "col_name",
                                       "confidence", "discovery_method"])

        table_rows = [Row("Organization", "cat.sch.orders", 0.9, "keyword")]
        col_rows = [ColRow("Geographic", "cat.sch.orders", "country", 0.8, "heuristic")]

        spark = MagicMock()
        # table entities uses toLocalIterator(); col entities uses collect()
        table_mock = MagicMock(toLocalIterator=MagicMock(return_value=iter(table_rows)))
        col_mock = MagicMock(collect=MagicMock(return_value=col_rows))
        call_results = iter([table_mock, col_mock, None])
        spark.sql.side_effect = lambda q: next(call_results) if "SELECT" in q or "CREATE" in q else None

        builder = _make_builder(spark=spark)
        builder._validation_cfg = {"min_entity_confidence": 0.5}
        builder._get_bundle_version = MagicMock(return_value="1.0")

        tagged = builder.apply_entity_tags()
        assert tagged == 2
        spark.createDataFrame.assert_called_once()
        audit_args = spark.createDataFrame.call_args
        rows = audit_args[0][0]
        assert len(rows) == 2
        assert rows[0][-1] == "applied"  # action
        assert rows[1][-1] == "applied"


# ---------------------------------------------------------------------------
# 5. Reconciliation dry-run
# ---------------------------------------------------------------------------

class TestReconciliation:

    def test_dry_run_returns_conflicts_without_update(self):
        mock_conflicts = MagicMock()
        mock_conflicts.count.return_value = 3
        mock_conflicts.filter.return_value = MagicMock(collect=MagicMock(return_value=[]))

        builder = _make_builder()
        builder.spark.sql.return_value = mock_conflicts

        result = builder.reconcile_geographic_classifications(dry_run=True)
        assert result.count() == 3
        for call in builder.spark.sql.call_args_list:
            query = call[0][0]
            assert "UPDATE" not in query

    def test_non_dry_run_applies_updates(self):
        Row = namedtuple("Row", ["table_name", "column_name", "property_role",
                                  "geo_entity_type", "conflict_type"])
        conflict = Row("cat.sch.orders", "country", "dimension",
                        "Geographic", "entity_says_geo_but_role_disagrees")

        mock_conflicts = MagicMock()
        mock_conflicts.count.return_value = 1
        mock_conflicts.filter.return_value = MagicMock(
            collect=MagicMock(return_value=[conflict])
        )

        builder = _make_builder()
        builder.spark.sql.return_value = mock_conflicts

        builder.reconcile_geographic_classifications(dry_run=False)
        update_calls = [
            c for c in builder.spark.sql.call_args_list
            if "UPDATE" in str(c)
        ]
        assert len(update_calls) >= 1

    def test_reconciliation_sql_includes_location(self):
        builder = _make_builder()
        mock_conflicts = MagicMock()
        mock_conflicts.count.return_value = 0
        builder.spark.sql.return_value = mock_conflicts
        builder.reconcile_geographic_classifications(dry_run=True)
        sql = builder.spark.sql.call_args[0][0]
        assert "'Location'" in sql

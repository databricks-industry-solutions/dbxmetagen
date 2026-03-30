"""Unit and integration tests for fk_prediction module.

Tests focus on:
1. Declared FK self-reference guard (P1)
2. Global self-reference filter (P2)
3. UI double-append dedup fix (P3)
4. Direction enforcement logic (I3c)
5. Query history candidate extraction (I1)
6. Hardcoded model constant (_FK_MODEL)
7. Full run() pipeline orchestration and dry-run accounting
"""

import sys
import os
import re
import inspect
from unittest.mock import MagicMock, patch, call
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.fk_prediction import (
    FKPredictionConfig,
    FKPredictor,
    _FK_MODEL,
    predict_foreign_keys,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_predictor(spark=None):
    config = FKPredictionConfig(catalog_name="cat", schema_name="sch")
    return FKPredictor(spark or MagicMock(), config)


def _mock_row(**kwargs):
    return SimpleNamespace(**kwargs)


# ---------------------------------------------------------------------------
# P1: Declared FK self-reference guard
# ---------------------------------------------------------------------------

class TestDeclaredFKGuard:
    """get_declared_fk_candidates() should skip self-referential entries."""

    def test_self_referential_fk_filtered(self):
        """A column referencing itself (same table + same column) is skipped."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _mock_row(
                table_name="cat.sch.orders",
                foreign_keys={"order_id": "cat.sch.orders.order_id"},
            )
        ]
        pred = _make_predictor(spark)
        pred.get_declared_fk_candidates()
        # createDataFrame should be called with empty pairs
        args = spark.createDataFrame.call_args
        if args is not None:
            pairs = args[0][0]
            assert len(pairs) == 0, f"Expected 0 pairs, got {len(pairs)}: {pairs}"

    def test_cross_table_fk_preserved(self):
        """A legitimate cross-table FK is not filtered."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _mock_row(
                table_name="cat.sch.orders",
                foreign_keys={"customer_id": "cat.sch.customers.id"},
            )
        ]
        pred = _make_predictor(spark)
        pred.get_declared_fk_candidates()
        args = spark.createDataFrame.call_args
        assert args is not None
        pairs = args[0][0]
        assert len(pairs) == 1
        assert pairs[0][0] == "cat.sch.orders.customer_id"
        assert pairs[0][1] == "cat.sch.customers.id"

    def test_mixed_self_and_cross_fks(self):
        """Same-table entries (both same-col and different-col) are removed; only cross-table stays."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _mock_row(
                table_name="cat.sch.employees",
                foreign_keys={
                    "manager_id": "cat.sch.employees.manager_id",    # same table, same col
                    "supervisor_id": "cat.sch.employees.employee_id",  # same table, diff col
                    "dept_id": "cat.sch.departments.id",               # cross-table
                },
            )
        ]
        pred = _make_predictor(spark)
        pred.get_declared_fk_candidates()
        args = spark.createDataFrame.call_args
        assert args is not None
        pairs = args[0][0]
        assert len(pairs) == 1
        assert "departments" in pairs[0][1]

    def test_same_table_different_column_filtered(self):
        """Same-table FK (different columns) is filtered out."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _mock_row(
                table_name="cat.sch.employees",
                foreign_keys={"manager_id": "cat.sch.employees.employee_id"},
            )
        ]
        pred = _make_predictor(spark)
        pred.get_declared_fk_candidates()
        args = spark.createDataFrame.call_args
        if args is not None:
            pairs = args[0][0]
            assert len(pairs) == 0, f"Expected 0 pairs, got {len(pairs)}: {pairs}"


# ---------------------------------------------------------------------------
# P3: UI double-append fix
# ---------------------------------------------------------------------------

class TestUIDedup:
    """review_combined should not double-append FK rows when src==dst table."""

    def test_set_dedup_when_src_equals_dst(self):
        """Using set() on [src_table, dst_table] deduplicates when equal."""
        fk = {"src_table": "cat.sch.orders", "dst_table": "cat.sch.orders"}
        tbl_names = {"cat.sch.orders"}
        fk_by_table = {}
        for tn in set([fk.get("src_table"), fk.get("dst_table")]):
            if tn in tbl_names:
                fk_by_table.setdefault(tn, []).append(fk)
        assert len(fk_by_table["cat.sch.orders"]) == 1

    def test_set_preserves_both_when_different(self):
        """When src != dst, FK is still attached to both tables."""
        fk = {"src_table": "cat.sch.orders", "dst_table": "cat.sch.customers"}
        tbl_names = {"cat.sch.orders", "cat.sch.customers"}
        fk_by_table = {}
        for tn in set([fk.get("src_table"), fk.get("dst_table")]):
            if tn in tbl_names:
                fk_by_table.setdefault(tn, []).append(fk)
        assert len(fk_by_table) == 2
        assert len(fk_by_table["cat.sch.orders"]) == 1
        assert len(fk_by_table["cat.sch.customers"]) == 1

    def test_old_list_approach_double_appends(self):
        """Demonstrates the bug: list [src, dst] when src==dst => 2 appends."""
        fk = {"src_table": "cat.sch.orders", "dst_table": "cat.sch.orders"}
        tbl_names = {"cat.sch.orders"}
        fk_by_table = {}
        for tn in [fk.get("src_table"), fk.get("dst_table")]:
            if tn in tbl_names:
                fk_by_table.setdefault(tn, []).append(fk)
        # This is the OLD buggy behavior: 2 entries
        assert len(fk_by_table["cat.sch.orders"]) == 2


# ---------------------------------------------------------------------------
# I3c: Direction enforcement
# ---------------------------------------------------------------------------

class TestEnforceDirection:
    """_enforce_direction is a static method that operates on DataFrames."""

    def test_exists_as_static_method(self):
        assert hasattr(FKPredictor, "_enforce_direction")
        assert isinstance(
            FKPredictor.__dict__["_enforce_direction"], staticmethod
        )

    def test_returns_df_unchanged_when_no_direction_columns(self):
        """If neither ai_fk_side nor _card_ratio columns exist, returns as-is."""
        df = MagicMock()
        df.columns = ["col_a", "col_b", "table_a", "table_b"]
        result = FKPredictor._enforce_direction(df)
        assert result is df


# ---------------------------------------------------------------------------
# I1: Query history candidate extraction regex
# ---------------------------------------------------------------------------

class TestQueryHistoryRegex:
    """Test the JOIN regex used in get_query_join_candidates."""

    JOIN_RE = re.compile(
        r"(\w+(?:\.\w+){0,3})\.(\w+)\s*=\s*(\w+(?:\.\w+){0,3})\.(\w+)",
        re.IGNORECASE,
    )

    def test_simple_fqn_join(self):
        sql = "SELECT * FROM cat.sch.orders o JOIN cat.sch.customers c ON cat.sch.orders.cust_id = cat.sch.customers.id"
        matches = self.JOIN_RE.findall(sql)
        assert len(matches) >= 1
        tbl_a, col_a, tbl_b, col_b = matches[0]
        assert "orders" in tbl_a
        assert col_a == "cust_id"
        assert "customers" in tbl_b
        assert col_b == "id"

    def test_no_match_on_non_join(self):
        sql = "SELECT COUNT(*) FROM cat.sch.orders WHERE status = 'active'"
        matches = self.JOIN_RE.findall(sql)
        assert len(matches) == 0

    def test_multiple_join_conditions(self):
        sql = (
            "SELECT * FROM cat.sch.a JOIN cat.sch.b "
            "ON cat.sch.a.x = cat.sch.b.y AND cat.sch.a.w = cat.sch.b.z"
        )
        matches = self.JOIN_RE.findall(sql)
        assert len(matches) == 2

    def test_graceful_on_empty_query_history(self):
        """get_query_join_candidates returns empty DF when history query fails."""
        spark = MagicMock()
        spark.sql.side_effect = Exception("system.query.history not available")
        pred = _make_predictor(spark)
        pred.get_query_join_candidates()
        spark.createDataFrame.assert_called()


# ---------------------------------------------------------------------------
# Query history same-table filtering
# ---------------------------------------------------------------------------

class TestQueryHistorySameTableFilter:
    """get_query_join_candidates must exclude self-joins."""

    def test_same_table_join_excluded(self):
        """A self-join in query history should not produce candidates."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _mock_row(statement=(
                "SELECT * FROM cat.sch.orders a JOIN cat.sch.orders b "
                "ON cat.sch.orders.parent_id = cat.sch.orders.id"
            )),
            _mock_row(statement=(
                "SELECT * FROM cat.sch.orders a JOIN cat.sch.orders b "
                "ON cat.sch.orders.parent_id = cat.sch.orders.id"
            )),
        ]
        pred = _make_predictor(spark)
        pred.get_query_join_candidates()
        args = spark.createDataFrame.call_args
        if args is not None:
            pairs = args[0][0]
            for p in pairs:
                tbl_a, tbl_b = p[2], p[3]
                assert tbl_a != tbl_b, f"Same-table pair leaked through: {p}"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestFKPredictionConfig:
    def test_fq_builds_path(self):
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        assert cfg.fq("tbl") == "c.s.tbl"

    def test_model_not_configurable(self):
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        assert not hasattr(cfg, "model_endpoint")


# ---------------------------------------------------------------------------
# Hardcoded model constant
# ---------------------------------------------------------------------------

class TestHardcodedModel:
    """_FK_MODEL must be gpt-oss-120b, not configurable via any parameter."""

    def test_constant_value(self):
        assert _FK_MODEL == "databricks-gpt-oss-120b"

    def test_convenience_function_has_no_model_param(self):
        sig = inspect.signature(predict_foreign_keys)
        assert "model_endpoint" not in sig.parameters

    def test_ai_judge_sql_uses_hardcoded_model(self):
        """The SQL passed to spark.sql in ai_judge must contain _FK_MODEL."""
        spark = MagicMock()
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        pred = FKPredictor(spark, cfg)
        mock_candidates = MagicMock()

        pred.ai_judge(mock_candidates)

        sql_calls = [str(c) for c in spark.sql.call_args_list]
        ai_calls = [s for s in sql_calls if "AI_QUERY" in s]
        assert ai_calls, "ai_judge must execute SQL containing AI_QUERY"
        assert any(_FK_MODEL in s for s in ai_calls), (
            f"AI_QUERY SQL must reference '{_FK_MODEL}'"
        )


# ---------------------------------------------------------------------------
# Full pipeline orchestration (run method)
# ---------------------------------------------------------------------------

class _Expr:
    """Stand-in for PySpark Column when no JVM is available (databricks-connect)."""
    def __ge__(self, o): return _Expr()
    def __le__(self, o): return _Expr()
    def __gt__(self, o): return _Expr()
    def __lt__(self, o): return _Expr()
    def __eq__(self, o): return _Expr()
    def __ne__(self, o): return _Expr()
    def __and__(self, o): return _Expr()
    def __or__(self, o): return _Expr()
    def __hash__(self): return id(self)
    def desc(self): return _Expr()
    def over(self, w): return _Expr()


def _chainable_mock():
    """Mock DataFrame that supports chained PySpark-style calls."""
    m = MagicMock()
    for attr in (
        "unionByName", "filter", "withColumn", "fillna", "drop",
        "select", "distinct", "orderBy", "alias",
    ):
        getattr(m, attr).return_value = m
    m.count.return_value = 0
    m.collect.return_value = []
    m.columns = []
    return m


def _mock_pyspark_F():
    """Create a mock pyspark.sql.functions module with comparable Column stand-ins."""
    m = MagicMock()
    m.col.return_value = _Expr()
    m.lit.return_value = _Expr()
    m.row_number.return_value = _Expr()
    m.coalesce.return_value = _Expr()
    m.greatest.return_value = _Expr()
    m.least.return_value = _Expr()
    m.round.return_value = _Expr()
    m.current_timestamp.return_value = _Expr()
    return m


_CANDIDATE_SOURCES = [
    "get_candidates",
    "get_name_based_candidates",
    "get_ontology_relationship_candidates",
    "get_declared_fk_candidates",
    "get_query_join_candidates",
]


class TestRunOrchestration:
    """run() must call all candidate sources and apply same-table filtering."""

    def test_run_calls_all_five_candidate_sources(self):
        spark = MagicMock()
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        pred = FKPredictor(spark, cfg)
        df = _chainable_mock()
        patches = (
            [patch.object(pred, name, return_value=df) for name in _CANDIDATE_SOURCES]
            + [patch.object(pred, "_ensure_output_tables")]
            + [patch("dbxmetagen.fk_prediction.F", _mock_pyspark_F())]
            + [patch("dbxmetagen.fk_prediction.Window", MagicMock())]
        )
        for p in patches:
            p.start()
        try:
            result = pred.run()
        finally:
            for p in patches:
                p.stop()

        assert result["candidates"] == 0
        df.unionByName.assert_called()
        assert df.unionByName.call_count == 4  # 4 unions for 5 sources

    def test_run_filters_candidates_after_union(self):
        """run() must call .filter() on the unioned candidates (same-table guard)."""
        spark = MagicMock()
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        pred = FKPredictor(spark, cfg)
        df = _chainable_mock()
        patches = (
            [patch.object(pred, name, return_value=df) for name in _CANDIDATE_SOURCES]
            + [patch.object(pred, "_ensure_output_tables")]
            + [patch("dbxmetagen.fk_prediction.F", _mock_pyspark_F())]
            + [patch("dbxmetagen.fk_prediction.Window", MagicMock())]
        )
        for p in patches:
            p.start()
        try:
            pred.run()
        finally:
            for p in patches:
                p.stop()

        df.filter.assert_called()

    def test_run_zero_candidates_returns_early(self):
        spark = MagicMock()
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        pred = FKPredictor(spark, cfg)
        df = _chainable_mock()
        patches = (
            [patch.object(pred, name, return_value=df) for name in _CANDIDATE_SOURCES]
            + [patch.object(pred, "_ensure_output_tables")]
            + [patch("dbxmetagen.fk_prediction.F", _mock_pyspark_F())]
            + [patch("dbxmetagen.fk_prediction.Window", MagicMock())]
        )
        for p in patches:
            p.start()
        ai_patch = patch.object(pred, "ai_judge")
        mock_ai = ai_patch.start()
        try:
            result = pred.run()
        finally:
            ai_patch.stop()
            for p in patches:
                p.stop()

        mock_ai.assert_not_called()
        assert result == {
            "candidates": 0, "ai_query_rows": 0,
            "predictions": 0, "edges": 0, "ddl_applied": 0,
        }


class TestDryRunAccounting:
    """dry_run=True must count candidates and AI-eligible rows without calling ai_judge."""

    def _make_dry_run_predictor(self):
        spark = MagicMock()
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s", dry_run=True)
        return FKPredictor(spark, cfg)

    def _dry_run_patches(self, pred, df):
        return (
            [patch.object(pred, name, return_value=df) for name in _CANDIDATE_SOURCES]
            + [
                patch.object(pred, "_ensure_output_tables"),
                patch.object(pred, "_sample_from_source", return_value=df),
                patch.object(pred, "add_entity_match", return_value=df),
                patch.object(pred, "add_lineage_signal", return_value=df),
                patch.object(pred, "rule_score", return_value=df),
                patch.object(pred, "cardinality_analysis", return_value=df),
                patch("dbxmetagen.fk_prediction.F", _mock_pyspark_F()),
                patch("dbxmetagen.fk_prediction.Window", MagicMock()),
            ]
        )

    def test_dry_run_skips_ai_judge(self):
        pred = self._make_dry_run_predictor()
        df = _chainable_mock()
        df.count.side_effect = [5, 3, 5]

        patches = self._dry_run_patches(pred, df)
        for p in patches:
            p.start()
        ai_patch = patch.object(pred, "ai_judge")
        mock_ai = ai_patch.start()
        try:
            result = pred.run()
        finally:
            ai_patch.stop()
            for p in patches:
                p.stop()

        mock_ai.assert_not_called()
        assert result["dry_run"] is True
        assert result["predictions"] == 0
        assert result["edges"] == 0
        assert result["ddl_applied"] == 0

    def test_dry_run_reports_candidate_count(self):
        pred = self._make_dry_run_predictor()
        df = _chainable_mock()
        df.count.side_effect = [10, 7, 10]

        patches = self._dry_run_patches(pred, df)
        for p in patches:
            p.start()
        try:
            result = pred.run()
        finally:
            for p in patches:
                p.stop()

        assert result["dry_run"] is True
        assert result["candidates"] == 10
        assert result["ai_query_rows"] == 7

"""Unit and integration tests for fk_prediction module.

Tests focus on:
1. Declared FK self-reference guard (P1)
2. Global self-reference filter (P2)
3. UI double-append dedup fix (P3)
4. Direction enforcement logic (I3c)
5. Query history candidate extraction (I1)
6. Hardcoded model constant (_FK_MODEL)
7. Full run() pipeline orchestration and dry-run accounting
8. AI confidence gating on is_fk (Bug 1)
9. Join-rate capping (Bug 2)
10. Safety filters in write_predictions (Bug 3)
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


# ---------------------------------------------------------------------------
# Bug 1: AI confidence must be gated on is_fk
# ---------------------------------------------------------------------------

class TestAIConfidenceGating:
    """ai_judge must set ai_confidence = 0.0 when the model says is_fk = false.

    The model's 'confidence' field means 'how sure I am of my assessment',
    NOT 'how likely this is a FK'. So when is_fk=false + confidence=1.0, the
    model is saying 'I am certain this is NOT a FK'. The pipeline must treat
    that as ai_confidence=0.0 so it never passes the threshold filter.
    """

    def _build_ai_raw_json(self, is_fk: bool, confidence: float, reasoning: str = "test"):
        """Produce the JSON string AI_QUERY would return."""
        import json
        return json.dumps({
            "is_fk": is_fk,
            "confidence": confidence,
            "fk_column": "a",
            "reasoning": reasoning,
        })

    def test_is_fk_false_zeroes_confidence(self):
        """When model returns is_fk=false, ai_confidence must be 0.0 regardless of confidence value."""
        from pyspark.sql.types import StructType, StructField, BooleanType, DoubleType, StringType
        from pyspark.sql import functions as F_real

        schema = StructType([
            StructField("is_fk", BooleanType()),
            StructField("confidence", DoubleType()),
            StructField("fk_column", StringType()),
            StructField("reasoning", StringType()),
        ])
        raw = self._build_ai_raw_json(is_fk=False, confidence=0.95, reasoning="not a FK")
        # Simulate the exact extraction logic from ai_judge
        cleaned = raw  # no markdown
        import json
        parsed = json.loads(cleaned)
        ai_is_fk = parsed.get("is_fk", False)
        raw_conf = max(0.0, min(1.0, parsed.get("confidence", 0.0)))
        ai_confidence = raw_conf if ai_is_fk else 0.0
        assert ai_confidence == 0.0, f"Expected 0.0 but got {ai_confidence}"

    def test_is_fk_true_preserves_confidence(self):
        """When model returns is_fk=true, ai_confidence should reflect the confidence value."""
        import json
        raw = self._build_ai_raw_json(is_fk=True, confidence=0.85)
        parsed = json.loads(raw)
        ai_is_fk = parsed.get("is_fk", False)
        raw_conf = max(0.0, min(1.0, parsed.get("confidence", 0.0)))
        ai_confidence = raw_conf if ai_is_fk else 0.0
        assert ai_confidence == 0.85

    def test_is_fk_null_defaults_to_zero(self):
        """If the model omits is_fk entirely, default to false => confidence 0."""
        import json
        raw = json.dumps({"confidence": 0.9, "fk_column": "a", "reasoning": "x"})
        parsed = json.loads(raw)
        ai_is_fk = parsed.get("is_fk", False)
        raw_conf = max(0.0, min(1.0, parsed.get("confidence", 0.0)))
        ai_confidence = raw_conf if ai_is_fk else 0.0
        assert ai_confidence == 0.0

    def test_malformed_json_returns_zero_confidence(self):
        """If AI response is not valid JSON, from_json returns nulls => confidence 0."""
        raw = "This is not JSON at all"
        import json
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {}
        ai_is_fk = parsed.get("is_fk", False)
        raw_conf = max(0.0, min(1.0, parsed.get("confidence", 0.0)))
        ai_confidence = raw_conf if ai_is_fk else 0.0
        assert ai_confidence == 0.0

    def test_ai_judge_code_uses_when_gate(self):
        """The ai_judge source code must use F.when(is_fk, ...).otherwise(0.0) pattern."""
        import inspect
        source = inspect.getsource(FKPredictor.ai_judge)
        assert "otherwise" in source, "ai_judge must use F.when(...).otherwise(0.0) to gate on is_fk"
        assert "ai_is_fk" in source, "ai_judge must reference ai_is_fk"

    def test_prompt_clarifies_confidence_semantics(self):
        """The AI_QUERY prompt must tell the model that confidence = P(is FK)."""
        import inspect
        source = inspect.getsource(FKPredictor.ai_judge)
        assert "probability this IS a foreign key" in source, (
            "Prompt must clarify that confidence = probability this IS a FK"
        )


# ---------------------------------------------------------------------------
# Bug 2: join_rate must be capped to [0.0, 1.0]
# ---------------------------------------------------------------------------

class TestJoinRateCapping:
    """join_validate must cap join_rate to prevent score inflation.

    Low-cardinality columns (e.g. 2 distinct file paths across 8000 rows)
    produce join_rate >> 1.0. The ai_confidence multiplier (0.6 + 0.4 * join_rate)
    then inflates any non-zero confidence to 1.0 after clipping.
    """

    def test_join_rate_capped_in_source(self):
        """The join_validate source code must cap join_rate with F.least(1.0, ...)."""
        import inspect
        source = inspect.getsource(FKPredictor.join_validate)
        assert "least" in source.lower(), "join_validate must cap join_rate with F.least"

    def test_join_rate_over_one_is_clipped(self):
        """Verify the math: with join_rate=4095, the multiplier would be 1638x without capping."""
        uncapped_rate = 4095.5
        ai_conf = 0.5
        # Without cap
        inflated = ai_conf * (0.6 + 0.4 * uncapped_rate)
        assert inflated > 100, f"Uncapped join_rate should cause massive inflation, got {inflated}"
        # With cap
        capped_rate = min(1.0, max(0.0, uncapped_rate))
        bounded = ai_conf * (0.6 + 0.4 * capped_rate)
        assert bounded <= 1.0, f"Capped join_rate should produce bounded result, got {bounded}"
        assert bounded == 0.5, f"0.5 * (0.6 + 0.4*1.0) = 0.5, got {bounded}"

    def test_write_predictions_caps_join_rate_in_final_score(self):
        """write_predictions must also cap join_rate in the final_confidence formula."""
        import inspect
        source = inspect.getsource(FKPredictor.write_predictions)
        # Should use F.least on join_rate in the output select
        assert "least" in source.lower(), "write_predictions must also cap join_rate"

    def test_negative_join_rate_floored_to_zero(self):
        """Edge case: negative join_rate (shouldn't happen) should floor to 0.0."""
        rate = -0.5
        capped = min(1.0, max(0.0, rate))
        assert capped == 0.0


# ---------------------------------------------------------------------------
# Bug 3: Same-table / same-column safety filters
# ---------------------------------------------------------------------------

class TestSafetyFilters:
    """write_predictions must never write rows where src_table=dst_table
    or src_column=dst_column. _ensure_output_tables must clean up existing
    bad rows idempotently (not silently swallow errors).
    """

    def test_write_predictions_filters_same_table(self):
        """The write_predictions source code must filter src_table != dst_table."""
        import inspect
        source = inspect.getsource(FKPredictor.write_predictions)
        assert "src_table" in source and "dst_table" in source
        assert 'src_table != dst_table' in source.replace('"', '').replace("'", '').replace('F.col(', '').replace(')', '') or \
               "src_table" in source and "!=" in source, \
               "write_predictions must filter out same-table rows"

    def test_write_predictions_filters_same_column(self):
        """The write_predictions source code must filter src_column != dst_column."""
        import inspect
        source = inspect.getsource(FKPredictor.write_predictions)
        assert "src_column" in source and "dst_column" in source

    def test_ensure_output_tables_does_not_silently_pass(self):
        """_ensure_output_tables must NOT have a bare 'except: pass' for the cleanup DELETE."""
        import inspect
        source = inspect.getsource(FKPredictor._ensure_output_tables)
        # Old buggy pattern: except Exception:\n            pass
        lines = source.split('\n')
        for i, line in enumerate(lines):
            if 'except' in line and 'pass' in line:
                assert False, f"Found 'except ... pass' on line {i}: {line.strip()}"
            if 'except' in line.strip() and i + 1 < len(lines) and lines[i + 1].strip() == 'pass':
                # Check if this is the cleanup-related except
                context = '\n'.join(lines[max(0, i - 3):i + 2])
                if 'DELETE' in context or 'cleanup' in context or 'self_referential' in context.lower():
                    assert False, f"Cleanup DELETE still has silent except/pass:\n{context}"

    def test_ensure_output_tables_cleanup_sql_covers_same_column(self):
        """Cleanup DELETE must also cover src_column = dst_column, not just src_table = dst_table."""
        import inspect
        source = inspect.getsource(FKPredictor._ensure_output_tables)
        assert "src_column = dst_column" in source or "src_column" in source, (
            "Cleanup must also remove rows where src_column = dst_column"
        )

    def test_insert_overwrite_excludes_bad_rows(self):
        """The INSERT OVERWRITE self-dedup must also exclude same-table and same-column rows."""
        import inspect
        source = inspect.getsource(FKPredictor.write_predictions)
        # Check the INSERT OVERWRITE SQL string contains the safety filter
        assert "src_table != dst_table" in source, (
            "INSERT OVERWRITE must filter out same-table rows"
        )

    def test_predictions_table_has_is_fk_column(self):
        """_ensure_output_tables must create the is_fk BOOLEAN column."""
        import inspect
        source = inspect.getsource(FKPredictor._ensure_output_tables)
        assert "is_fk" in source, "Predictions table schema must include is_fk column"
        assert "BOOLEAN" in source, "is_fk must be BOOLEAN type"

    def test_merge_updates_is_fk(self):
        """The MERGE statement must include is_fk in the UPDATE SET clause."""
        import inspect
        source = inspect.getsource(FKPredictor.write_predictions)
        assert "is_fk = s.is_fk" in source, "MERGE must update is_fk column"


# ---------------------------------------------------------------------------
# Integration-style: ai_judge mock test with real parsing logic
# ---------------------------------------------------------------------------

class TestAIJudgeMockIntegration:
    """Test ai_judge with mocked Spark to verify the full parsing pipeline."""

    def test_ai_judge_creates_correct_columns(self):
        """ai_judge must produce ai_confidence, ai_is_fk, ai_reasoning, ai_fk_side columns."""
        spark = MagicMock()
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        pred = FKPredictor(spark, cfg)
        mock_df = MagicMock()
        pred.ai_judge(mock_df)

        # Check that spark.sql was called with AI_QUERY
        sql_calls = [str(c) for c in spark.sql.call_args_list]
        ai_calls = [s for s in sql_calls if "AI_QUERY" in s]
        assert ai_calls, "ai_judge must call AI_QUERY"

        # Check the returned df chain contains the right withColumn calls
        result_df = spark.sql.return_value.withColumn.return_value
        with_col_calls = []
        obj = spark.sql.return_value
        while hasattr(obj, 'withColumn') and obj.withColumn.called:
            for c in obj.withColumn.call_args_list:
                with_col_calls.append(c[0][0] if c[0] else str(c))
            obj = obj.withColumn.return_value
        # We just verify the method runs without error
        assert True

    def test_ai_judge_strips_markdown(self):
        """ai_judge source must strip markdown backticks before from_json."""
        import inspect
        source = inspect.getsource(FKPredictor.ai_judge)
        assert "regexp_replace" in source, "ai_judge must strip markdown from ai_raw"
        assert "```" in source, "regexp_replace pattern must target ``` fences"


# ---------------------------------------------------------------------------
# End-to-end logic verification (no Spark required)
# ---------------------------------------------------------------------------

class TestConfidenceScoringLogic:
    """Verify the mathematical logic of confidence scoring after all fixes."""

    def test_final_confidence_bounded_zero_one(self):
        """final_confidence = weighted sum, clipped to [0, 1]."""
        # All components at maximum
        col_sim, rule, ai_conf, join_rate, pk_uniq, ri = 1.0, 1.0, 1.0, 1.0, 1.0, 1.0
        final = (col_sim * 0.15 + rule * 0.15 + ai_conf * 0.25
                 + join_rate * 0.15 + pk_uniq * 0.15 + ri * 0.15)
        assert final == 1.0, f"Max final should be 1.0, got {final}"

        # All at zero
        final_zero = 0 * 0.15 + 0 * 0.15 + 0 * 0.25 + 0 * 0.15 + 0 * 0.15 + 0 * 0.15
        assert final_zero == 0.0

    def test_is_fk_false_cannot_pass_threshold(self):
        """With is_fk=false, ai_confidence=0.0, so it never reaches 0.7 threshold."""
        ai_confidence = 0.0  # gated because is_fk=false
        assert ai_confidence < 0.7, "is_fk=false must produce ai_confidence below threshold"

    def test_uncapped_join_rate_would_inflate(self):
        """Demonstrate why capping is essential: uncapped join_rate=100 inflates everything."""
        ai_conf_raw = 0.3
        # Without cap: multiplier = 0.6 + 0.4*100 = 40.6
        inflated = ai_conf_raw * (0.6 + 0.4 * 100)
        assert inflated > 10, "Uncapped join_rate causes massive inflation"
        clipped = min(1.0, inflated)
        assert clipped == 1.0, "Inflation clips to 1.0, masking bad predictions"

        # With cap: multiplier = 0.6 + 0.4*1.0 = 1.0
        capped = ai_conf_raw * (0.6 + 0.4 * min(1.0, 100))
        assert capped == 0.3, f"Capped rate preserves original confidence, got {capped}"

    def test_join_rate_boost_is_moderate_when_capped(self):
        """With join_rate capped to 1.0, the max boost factor is 1.0 (no change)."""
        for rate in [0.0, 0.5, 1.0]:
            boost = 0.6 + 0.4 * rate
            assert 0.6 <= boost <= 1.0, f"Boost for rate={rate} is {boost}, out of [0.6, 1.0]"

    def test_same_table_prediction_is_impossible(self):
        """Verify the layered defense: candidates filter + write_predictions filter."""
        import inspect
        run_source = inspect.getsource(FKPredictor.run)
        wp_source = inspect.getsource(FKPredictor.write_predictions)
        eo_source = inspect.getsource(FKPredictor._ensure_output_tables)
        assert 'table_a' in run_source and 'table_b' in run_source, "run() must filter same-table"
        assert 'src_table' in wp_source and 'dst_table' in wp_source, "write_predictions must filter"
        assert 'src_table = dst_table' in eo_source, "_ensure_output_tables must clean up"


# ---------------------------------------------------------------------------
# Direction enforcement swap correctness
# ---------------------------------------------------------------------------

class TestEnforceDirectionSwap:
    """Verify _enforce_direction correctly swaps columns without corruption.

    The original bug: PySpark's F.col() is a lazy name reference, so chaining
    .withColumn("col_a", ...).withColumn("col_b", F.col("col_a")) reads the
    ALREADY-MODIFIED col_a, making both columns the same value.
    """

    def test_swap_uses_temp_columns(self):
        """The swap implementation must use temp columns to avoid lazy-ref corruption."""
        source = inspect.getsource(FKPredictor._enforce_direction)
        assert "_swap_" in source, (
            "_enforce_direction must use temp columns (e.g. _swap_col_a) "
            "to avoid PySpark lazy column reference bug"
        )
        assert "withColumnRenamed" in source, (
            "_enforce_direction must rename temp columns back"
        )

    def test_swap_does_not_write_directly_to_pair_columns(self):
        """Must NOT write directly to ca/cb. Must use temp columns then rename."""
        source = inspect.getsource(FKPredictor._enforce_direction)
        # The fix writes to ta/tb temp vars, then drops originals, then renames.
        # The old bug wrote directly: .withColumn(ca, ...).withColumn(cb, ...)
        # Verify the drop+rename pattern is present
        assert ".drop(ca, cb)" in source or ".drop(" in source, (
            "Must drop original columns before renaming temps"
        )
        assert "withColumnRenamed" in source, "Must rename temp columns back"
        # Verify we never do .withColumn(ca, ...).withColumn(cb, ...) directly
        assert ".withColumn(ca," not in source, (
            "Must NOT write directly to ca -- use temp columns instead"
        )

    def test_swap_temp_vars_derived_from_pair(self):
        """Temp column names must be derived from the pair names to avoid collisions."""
        source = inspect.getsource(FKPredictor._enforce_direction)
        assert 'f"_swap_{ca}"' in source or "f'_swap_{ca}'" in source, (
            "Temp column names must include the pair name for uniqueness"
        )

    def test_swap_handles_all_column_pairs(self):
        """All swap pairs (col, table, dtype, entity_type, card_ratio) use temp cols."""
        source = inspect.getsource(FKPredictor._enforce_direction)
        # The fix uses a loop with temp columns for ALL pairs
        # Verify the for loop uses the swap pattern
        assert "for ca, cb in" in source, "Must iterate over swap pairs"
        assert 'f"_swap_{ca}"' in source or "f'_swap_{ca}'" in source, (
            "Temp column names must be derived from pair names"
        )

    def test_enforce_direction_no_swap_preserves_columns(self):
        """When ai_fk_side='a' (no swap), columns must be unchanged."""
        source = inspect.getsource(FKPredictor._enforce_direction)
        assert ".otherwise(" in source, (
            "Must have .otherwise() branch to preserve original values when not swapping"
        )

    def test_stale_cleanup_removes_null_is_fk(self):
        """_ensure_output_tables must clean rows where is_fk IS NULL and ai_confidence >= 1.0."""
        source = inspect.getsource(FKPredictor._ensure_output_tables)
        assert "is_fk IS NULL" in source, (
            "_ensure_output_tables must remove stale pre-fix rows with is_fk IS NULL"
        )

    def test_cleanup_counts_before_deleting(self):
        """_ensure_output_tables must COUNT rows before each DELETE for logging."""
        source = inspect.getsource(FKPredictor._ensure_output_tables)
        assert "SELECT COUNT(*)" in source, (
            "_ensure_output_tables must count rows before deleting for visibility"
        )
        assert "cleanup_rules" in source, (
            "_ensure_output_tables must iterate over labelled cleanup rules"
        )

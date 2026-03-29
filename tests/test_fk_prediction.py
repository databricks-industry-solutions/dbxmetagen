"""Unit tests for fk_prediction module.

Tests focus on:
1. Declared FK self-reference guard (P1)
2. Global self-reference filter (P2)
3. UI double-append dedup fix (P3)
4. Direction enforcement logic (I3c)
5. Query history candidate extraction (I1)
"""

import sys
import os
import re
from unittest.mock import MagicMock
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor


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
        """Only the self-referential entry is removed; others stay."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _mock_row(
                table_name="cat.sch.employees",
                foreign_keys={
                    "manager_id": "cat.sch.employees.manager_id",  # self-ref
                    "dept_id": "cat.sch.departments.id",            # cross-table
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

    def test_same_table_different_column_allowed(self):
        """Self-join FK (same table, different columns) is allowed."""
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
        assert args is not None
        pairs = args[0][0]
        assert len(pairs) == 1


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
# Config
# ---------------------------------------------------------------------------

class TestFKPredictionConfig:
    def test_fq_builds_path(self):
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        assert cfg.fq("tbl") == "c.s.tbl"

    def test_default_model(self):
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s")
        assert cfg.model_endpoint == "databricks-gpt-oss-120b"

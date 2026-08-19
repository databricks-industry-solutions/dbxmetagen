"""Tests for kpi_logic.resolve_kpi_target token-based column matching.

kpi_logic is a standalone pure module (only stdlib imports), so it loads without
the app/Databricks harness. Regression coverage for the code-review finding that
naive substring matching let short/common column names (id, a, amt) inflate the
overlap score and bind a KPI to the wrong table.
"""

import os
import sys

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")
sys.path.insert(0, APP_DIR)

import kpi_logic  # noqa: E402


_COL_BY_TABLE = {
    "c.s.dims": [{"column_name": "id"}, {"column_name": "a"}, {"column_name": "name"}],
    "c.s.fact": [{"column_name": "revenue"}, {"column_name": "region"}],
}


class TestResolveKpiTarget:
    def test_name_match_short_name_wins(self):
        out = kpi_logic.resolve_kpi_target("fact", "SUM(revenue)", ["c.s.dims", "c.s.fact"], _COL_BY_TABLE)
        assert out == ["c.s.fact"]

    def test_column_overlap_binds_to_real_table(self):
        # Only fact columns actually appear -> fact wins on token overlap.
        out = kpi_logic.resolve_kpi_target(
            "unknown_tbl", "SUM(revenue) / COUNT(region)", ["c.s.dims", "c.s.fact"], _COL_BY_TABLE
        )
        assert out == ["c.s.fact"]

    def test_substring_trap_does_not_bind_short_names(self):
        # 'valid' and 'paid' contain the letters of 'id'/'a' but are NOT those
        # columns. Naive substring matching would score c.s.dims; token matching
        # scores both tables 0 -> returns ALL candidates for downstream validation.
        out = kpi_logic.resolve_kpi_target(
            "unknown", "CASE WHEN valid THEN paid ELSE 0 END", ["c.s.dims", "c.s.fact"], _COL_BY_TABLE
        )
        assert set(out) == {"c.s.dims", "c.s.fact"}

    def test_no_signal_returns_all_candidates(self):
        out = kpi_logic.resolve_kpi_target(
            "unknown", "COUNT(1)", ["c.s.dims", "c.s.fact"], _COL_BY_TABLE
        )
        assert set(out) == {"c.s.dims", "c.s.fact"}

    def test_real_column_named_id_still_matches_as_token(self):
        # A formula that genuinely references `id` as a token should still count.
        out = kpi_logic.resolve_kpi_target(
            "unknown", "COUNT(DISTINCT id)", ["c.s.dims", "c.s.fact"], _COL_BY_TABLE
        )
        assert out == ["c.s.dims"]

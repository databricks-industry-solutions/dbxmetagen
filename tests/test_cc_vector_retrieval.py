"""Tests for the opt-in 'vector' customer-context retrieval (scope-prefiltered HYBRID VS).

Covers the pure scope math, dedupe, the success path (VS hits + patterns merged), and the
fail-open fallback to a bounded per-table SQL read when the index is unavailable.
"""

import dbxmetagen.customer_context as cc


# --- pure helpers ----------------------------------------------------------

def test_exact_scopes_three_levels():
    assert cc._exact_scopes("cat.sch.tbl") == ["cat", "cat.sch", "cat.sch.tbl"]


def test_exact_scopes_partial_names():
    assert cc._exact_scopes("cat.sch") == ["cat", "cat.sch"]
    assert cc._exact_scopes("cat") == ["cat"]
    assert cc._exact_scopes("") == []


def test_dedupe_rows_collapses_identical():
    rows = [
        {"scope": "a", "scope_type": "catalog", "context_text": "x"},
        {"scope": "a", "scope_type": "catalog", "context_text": "x"},
        {"scope": "a", "scope_type": "catalog", "context_text": "y"},
    ]
    assert len(cc._dedupe_rows(rows)) == 2


def test_context_vs_index_name_env(monkeypatch):
    # Default suffix is single-sourced; the name is catalog.schema.<suffix>.
    assert cc.context_vs_index_name("c", "s").endswith(".customer_context_vs_index")
    assert cc.context_vs_index_name("c", "s").startswith("c.s.")


# --- fake spark for the bounded fallback -----------------------------------

class _FakeRow:
    def __init__(self, d):
        self._d = d

    def asDict(self):
        return dict(self._d)


class _FakeDF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _FakeSpark:
    def __init__(self, rows):
        self._rows = rows
        self.last_sql = None

    def sql(self, q):
        self.last_sql = q
        return _FakeDF(self._rows)


# --- success path ----------------------------------------------------------

def test_vector_success_merges_vs_hits_and_patterns(monkeypatch):
    monkeypatch.setattr(cc, "_query_context_vs_index", lambda *a, **k: [
        {"scope": "cat.sch.tbl", "scope_type": "table", "context_text": "table level ctx",
         "context_label": "", "priority": 0},
    ])
    patterns = [
        {"scope": "cat.sch.*", "scope_type": "pattern", "context_text": "pattern level ctx",
         "context_label": "", "priority": 0},
        {"scope": "other.*", "scope_type": "pattern", "context_text": "unrelated",
         "context_label": "", "priority": 0},
    ]
    out = cc.resolve_customer_context_vector(
        spark=None, catalog="cat", schema="sch", full_table_name="cat.sch.tbl",
        signature="cat.sch.tbl col_a col_b", top_k=8, pattern_cache=patterns,
    )
    assert "table level ctx" in out
    assert "pattern level ctx" in out        # matching pattern merged in
    assert "unrelated" not in out            # non-matching pattern excluded


# --- fail-open fallback ----------------------------------------------------

def test_vector_falls_back_to_bounded_read_on_vs_error(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("index not ready")
    monkeypatch.setattr(cc, "_query_context_vs_index", _boom)

    spark = _FakeSpark([
        _FakeRow({"scope": "cat.sch", "scope_type": "schema", "context_text": "schema ctx",
                  "context_label": "", "priority": 0}),
    ])
    out = cc.resolve_customer_context_vector(
        spark=spark, catalog="cat", schema="sch", full_table_name="cat.sch.tbl",
        signature="cat.sch.tbl", top_k=8, pattern_cache=[],
    )
    assert "schema ctx" in out
    # The fallback read is BOUNDED (scope IN (...) / patterns), never a whole-table scan.
    assert "scope IN (" in spark.last_sql
    assert "active = TRUE" in spark.last_sql


def test_vector_empty_when_nothing_matches(monkeypatch):
    monkeypatch.setattr(cc, "_query_context_vs_index", lambda *a, **k: [])
    out = cc.resolve_customer_context_vector(
        spark=None, catalog="cat", schema="sch", full_table_name="cat.sch.tbl",
        signature="cat.sch.tbl", top_k=8, pattern_cache=[],
    )
    assert out == ""

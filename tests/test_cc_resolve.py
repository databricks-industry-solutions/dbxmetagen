"""
Unit tests for the unified customer-context "write -> flag -> resolve" model (api_server):

- A create ALWAYS appends (content-keyed) and never returns a blocking {conflict} payload --
  so a scope accumulating >1 active entry is derivably "needs disambiguation".
- 'replace' / 'combine' are the resolve-panel apply actions (collapse to one canonical row).
- combine-preview merges the scope's entries WITHOUT writing (feeds the editable proposal).

App-side tests: `api_server` import is heavy (FastAPI + Databricks SDK), so this file is
--ignore'd from the core suite (see pyproject.toml addopts) and run separately. Import is
guarded with importorskip so it degrades to a skip where the app deps are unavailable.

    .venv/bin/python -m pytest tests/test_cc_resolve.py -v

Every SQL/LLM helper is monkeypatched, so no warehouse or model endpoint is touched.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app"))
api_server = pytest.importorskip("api_server")

CustomerContextRequest = api_server.CustomerContextRequest
CombinePreviewRequest = api_server.CombinePreviewRequest


@pytest.fixture
def cc(monkeypatch):
    """Stub out every SQL/LLM touchpoint and record the write/delete calls."""
    calls = {"writes": [], "deletes": [], "combined": []}

    monkeypatch.setattr(api_server, "_ensure_customer_context_table", lambda: None)

    def fake_write(ctx_id, scope, scope_type, context_text, context_label, priority, now):
        calls["writes"].append({"ctx_id": ctx_id, "scope": scope, "scope_type": scope_type,
                                "context_text": context_text, "context_label": context_label,
                                "priority": priority})

    def fake_delete(ids, now):
        calls["deletes"].append(list(ids))

    def fake_combine(existing_texts, incoming_text):
        calls["combined"].append((list(existing_texts), incoming_text))
        return "MERGED SUMMARY"

    monkeypatch.setattr(api_server, "_cc_write_entry", fake_write)
    monkeypatch.setattr(api_server, "_cc_soft_delete_ids", fake_delete)
    monkeypatch.setattr(api_server, "_cc_combine_texts", fake_combine)
    return calls


def _active(*texts):
    """Build fake _cc_active_at_scope rows."""
    return [{"context_id": f"id{i}", "scope": "cat.sch", "scope_type": "schema",
             "context_text": t, "context_label": f"lbl{i}", "priority": 0,
             "word_count": len(t.split())} for i, t in enumerate(texts)]


def _set_existing(monkeypatch, rows):
    monkeypatch.setattr(api_server, "_cc_active_at_scope", lambda scope, exclude_id="": rows)


# --- create always appends, never blocks ------------------------------------

def test_create_on_occupied_scope_appends_and_flags(cc, monkeypatch):
    _set_existing(monkeypatch, _active("existing note"))
    req = CustomerContextRequest(scope="cat.sch", scope_type="schema",
                                 context_text="a new note for the schema")
    out = api_server.upsert_customer_context(req)
    assert "conflict" not in out                      # never blocks
    assert out["resolution"] == "append"
    assert out["needs_disambiguation"] is True
    assert cc["deletes"] == []                         # nothing overwritten
    assert len(cc["writes"]) == 1
    # content-keyed id (append), NOT the scope-keyed canonical id
    assert cc["writes"][0]["ctx_id"] == api_server._cc_content_id("cat.sch", "a new note for the schema")


def test_create_on_empty_scope_appends_unflagged(cc, monkeypatch):
    _set_existing(monkeypatch, [])
    req = CustomerContextRequest(scope="cat.sch", scope_type="schema", context_text="first note")
    out = api_server.upsert_customer_context(req)
    assert out["resolution"] == "append"
    assert out["needs_disambiguation"] is False
    assert cc["deletes"] == []


def test_detect_is_alias_for_append(cc, monkeypatch):
    """Legacy clients that still send on_conflict='detect' must append, not block."""
    _set_existing(monkeypatch, _active("existing"))
    req = CustomerContextRequest(scope="cat.sch", scope_type="schema",
                                 context_text="incoming", on_conflict="detect")
    out = api_server.upsert_customer_context(req)
    assert "conflict" not in out
    assert out["resolution"] == "append"


# --- resolve actions collapse the scope -------------------------------------

def test_replace_collapses_to_canonical_row(cc, monkeypatch):
    _set_existing(monkeypatch, _active("one", "two"))
    req = CustomerContextRequest(scope="cat.sch", scope_type="schema",
                                 context_text="the canonical text", on_conflict="replace")
    out = api_server.upsert_customer_context(req)
    assert out["resolution"] == "replace"
    assert cc["deletes"] == [["id0", "id1"]]           # originals retired
    assert cc["writes"][0]["ctx_id"] == api_server._cc_scope_id("cat.sch")  # scope-keyed canonical


def test_combine_merges_and_retires_originals(cc, monkeypatch):
    _set_existing(monkeypatch, _active("one", "two"))
    req = CustomerContextRequest(scope="cat.sch", scope_type="schema",
                                 context_text="incoming", on_conflict="combine")
    out = api_server.upsert_customer_context(req)
    assert out["resolution"] == "combine"
    assert out["merged_from"] == 2
    assert cc["combined"] == [(["one", "two"], "incoming")]
    assert cc["writes"][0]["context_text"] == "MERGED SUMMARY"
    assert cc["deletes"] == [["id0", "id1"]]


# --- combine-preview: merge WITHOUT writing ---------------------------------

def test_combine_preview_merges_without_writing(cc, monkeypatch):
    _set_existing(monkeypatch, _active("alpha fact", "beta fact"))
    out = api_server.combine_preview_customer_context(
        CombinePreviewRequest(scope="cat.sch", scope_type="schema"))
    assert out["merged_text"] == "MERGED SUMMARY"
    assert out["entry_count"] == 2
    assert len(out["sources"]) == 2
    assert cc["writes"] == []                          # preview never writes
    assert cc["deletes"] == []


def test_combine_preview_single_entry_is_verbatim(cc, monkeypatch):
    _set_existing(monkeypatch, _active("only one"))
    out = api_server.combine_preview_customer_context(
        CombinePreviewRequest(scope="cat.sch", scope_type="schema"))
    assert out["merged_text"] == "only one"
    assert cc["combined"] == []                        # no LLM call for a single entry
    assert cc["writes"] == []


def test_combine_preview_empty_scope_404(cc, monkeypatch):
    _set_existing(monkeypatch, [])
    with pytest.raises(api_server.HTTPException) as ei:
        api_server.combine_preview_customer_context(
            CombinePreviewRequest(scope="cat.sch", scope_type="schema"))
    assert ei.value.status_code == 404

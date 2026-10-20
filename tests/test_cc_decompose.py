"""
Unit tests for the customer-context DECOMPOSE pure logic (api_server):
- _cc_validate_decomposed: snaps model-proposed entries to REAL scopes, coerces the rest
- _cc_build_decompose_prompt: grounds the prompt in the real table list

App-side tests: `api_server` import is heavy (FastAPI + Databricks SDK), so this file is
--ignore'd from the core suite (see pyproject.toml addopts) and run separately. Import is
guarded with importorskip so it degrades to a skip where the app deps are unavailable.

    .venv/bin/python -m pytest tests/test_cc_decompose.py -v
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app"))
api_server = pytest.importorskip("api_server")

_validate = api_server._cc_validate_decomposed
_build_prompt = api_server._cc_build_decompose_prompt

ALLOWED = ["cat.sch.orders", "cat.sch.customers", "cat.sch.line_items"]


# --- _cc_validate_decomposed -------------------------------------------------

def test_valid_table_entry_passes_through():
    out = _validate(
        [{"context_text": "Orders are billing events.", "target_scope": "cat.sch.orders",
          "scope_type": "table", "confidence": 0.9}],
        ALLOWED, "cat", "sch", None)
    assert len(out) == 1
    assert out[0]["scope"] == "cat.sch.orders"
    assert out[0]["scope_type"] == "table"
    assert out[0]["coerced"] is False


def test_table_match_is_case_insensitive_and_snaps_to_canonical():
    out = _validate(
        [{"context_text": "x", "target_scope": "CAT.SCH.Orders", "scope_type": "table"}],
        ALLOWED, "cat", "sch", None)
    assert out[0]["scope"] == "cat.sch.orders"   # canonical casing from the allowed list
    assert out[0]["coerced"] is False


def test_hallucinated_table_is_coerced_to_fallback():
    # Model invents a table not in the estate -> must NOT pass through as a table scope.
    out = _validate(
        [{"context_text": "x", "target_scope": "cat.sch.ghost_table", "scope_type": "table"}],
        ALLOWED, "cat", "sch", "orders")
    assert out[0]["coerced"] is True
    assert out[0]["scope"] == "cat.sch.orders"   # fell back to the caller's table scope
    assert out[0]["scope_type"] == "table"


def test_fallback_is_schema_when_no_table_given():
    out = _validate(
        [{"context_text": "x", "target_scope": "cat.sch.ghost", "scope_type": "table"}],
        ALLOWED, "cat", "sch", None)
    assert out[0]["scope"] == "cat.sch"
    assert out[0]["scope_type"] == "schema"


def test_fallback_is_catalog_when_no_schema_given():
    out = _validate(
        [{"context_text": "x", "target_scope": "whatever", "scope_type": "table"}],
        [], "cat", None, None)
    assert out[0]["scope"] == "cat"
    assert out[0]["scope_type"] == "catalog"


def test_schema_scope_must_match_callers_schema():
    out = _validate(
        [{"context_text": "cross-cutting", "target_scope": "cat.other", "scope_type": "schema"}],
        ALLOWED, "cat", "sch", None)
    assert out[0]["coerced"] is True            # cat.other != cat.sch -> coerced
    assert out[0]["scope"] == "cat.sch"


def test_valid_schema_and_catalog_scopes_pass():
    out = _validate(
        [{"context_text": "a", "target_scope": "cat.sch", "scope_type": "schema"},
         {"context_text": "b", "target_scope": "cat", "scope_type": "catalog"}],
        ALLOWED, "cat", "sch", None)
    assert [e["scope"] for e in out] == ["cat.sch", "cat"]
    assert all(e["coerced"] is False for e in out)


def test_empty_text_and_non_dicts_dropped():
    out = _validate(
        [{"context_text": "   ", "target_scope": "cat.sch.orders", "scope_type": "table"},
         "garbage", None,
         {"context_text": "keep", "target_scope": "cat.sch.orders", "scope_type": "table"}],
        ALLOWED, "cat", "sch", None)
    assert len(out) == 1 and out[0]["context_text"] == "keep"


def test_context_text_truncated_to_word_cap():
    long = " ".join(["w"] * (api_server._CC_DECOMPOSE_WORDS + 50))
    out = _validate(
        [{"context_text": long, "target_scope": "cat.sch.orders", "scope_type": "table"}],
        ALLOWED, "cat", "sch", None)
    assert len(out[0]["context_text"].split()) == api_server._CC_DECOMPOSE_WORDS


def test_bad_confidence_becomes_none():
    out = _validate(
        [{"context_text": "x", "target_scope": "cat.sch.orders", "scope_type": "table",
          "confidence": "not-a-number"}],
        ALLOWED, "cat", "sch", None)
    assert out[0]["confidence"] is None


def test_entry_cap_enforced():
    many = [{"context_text": f"c{i}", "target_scope": "cat.sch.orders", "scope_type": "table"}
            for i in range(api_server._CC_DECOMPOSE_MAX_ENTRIES + 25)]
    out = _validate(many, ALLOWED, "cat", "sch", None)
    assert len(out) == api_server._CC_DECOMPOSE_MAX_ENTRIES


# --- _cc_build_decompose_prompt ----------------------------------------------

def test_prompt_lists_allowed_tables_and_forbids_invention():
    p = _build_prompt("DOC BODY", "a sentence of scope", ALLOWED, "cat", "sch", False)
    for t in ALLOWED:
        assert t in p
    assert "Never invent" in p
    assert "a sentence of scope" in p
    assert "DOC BODY" in p


def test_prompt_reflects_embedded_scope_flag():
    p_yes = _build_prompt("d", "s", ALLOWED, "cat", "sch", True)
    p_no = _build_prompt("d", "s", ALLOWED, "cat", "sch", False)
    assert "references specific" in p_yes
    assert "does NOT clearly name" in p_no

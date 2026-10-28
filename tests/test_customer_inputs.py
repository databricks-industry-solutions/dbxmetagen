"""
Unit tests for app-managed customer inputs:
- acronym overlay onto acro_content (customer_context.merge_acronyms_into_acro_content)
- uploaded column descriptions layered under the file CSV (overrides loader)

Pure-function coverage; no Spark/Databricks connection needed.
"""

import json
import os
import tempfile
from types import SimpleNamespace

import pytest

from dbxmetagen.customer_context import (
    merge_acronyms_into_acro_content,
    _acro_scope_matches,
    _acro_source_scopes,
)
import dbxmetagen.overrides as overrides
from dbxmetagen.overrides import (
    load_override_data_for_table,
    get_override_column_set,
    _customer_overrides_for_table,
)


# --- acronyms ---------------------------------------------------------------

# Scope is matched against the SOURCE tables being documented (config.table_names),
# so tests pass a source-table FQN list. 'cat.sch.t' -> catalog 'cat', schema 'cat.sch'.
_SRC = ["cat.sch.t"]


def test_merge_acronyms_no_scoped_rows_returns_base_unchanged():
    base = '{"CD": "clinical data"}'
    out = merge_acronyms_into_acro_content(base, [], _SRC)
    assert out is base  # identity: untouched when nothing matches


def test_merge_acronyms_non_matching_scope_returns_base():
    base = {"CD": "clinical data"}
    rows = [{"scope": "other.schema", "scope_type": "schema",
             "abbreviation": "QE", "expansion": "quality event", "notes": ""}]
    out = merge_acronyms_into_acro_content(base, rows, _SRC)
    assert out is base


def test_merge_acronyms_overlays_on_json_string_base():
    base = '{"CD": "clinical data"}'
    rows = [{"scope": "cat.sch", "scope_type": "schema",
             "abbreviation": "QE", "expansion": "quality event", "notes": ""}]
    out = merge_acronyms_into_acro_content(base, rows, _SRC)
    assert out == {"CD": "clinical data", "QE": "quality event"}


def test_merge_acronyms_schema_scope_matches_source_table_schema():
    # The bug fix: a schema-scoped acronym on the DATA schema (not the output schema)
    # must now apply to a source table in that schema.
    rows = [{"scope": "cat.sch", "scope_type": "schema",
             "abbreviation": "QE", "expansion": "quality event", "notes": ""}]
    out = merge_acronyms_into_acro_content({}, rows, ["cat.sch.patients"])
    assert out["QE"] == "quality event"


def test_merge_acronyms_schema_scope_beats_catalog_scope():
    rows = [
        {"scope": "cat", "scope_type": "catalog", "abbreviation": "QE", "expansion": "catalog-level", "notes": ""},
        {"scope": "cat.sch", "scope_type": "schema", "abbreviation": "QE", "expansion": "schema-level", "notes": ""},
    ]
    out = merge_acronyms_into_acro_content({}, rows, _SRC)
    assert out["QE"] == "schema-level"


def test_merge_acronyms_folds_notes_into_expansion():
    rows = [{"scope": "cat", "scope_type": "catalog",
             "abbreviation": "CD", "expansion": "clinical data", "notes": "Artifact"}]
    out = merge_acronyms_into_acro_content({}, rows, _SRC)
    assert out["CD"] == "clinical data (Artifact)"


def test_merge_acronyms_malformed_base_falls_back_to_scoped_only():
    out = merge_acronyms_into_acro_content("not json at all", [
        {"scope": "cat", "scope_type": "catalog", "abbreviation": "CD", "expansion": "clinical data", "notes": ""},
    ], _SRC)
    assert out == {"CD": "clinical data"}


def test_merge_acronyms_multi_schema_run_matches_each_source_schema():
    # A run spanning two source schemas: each schema-scoped acronym applies.
    rows = [
        {"scope": "cat.a", "scope_type": "schema", "abbreviation": "AA", "expansion": "alpha", "notes": ""},
        {"scope": "cat.b", "scope_type": "schema", "abbreviation": "BB", "expansion": "bravo", "notes": ""},
        {"scope": "cat.c", "scope_type": "schema", "abbreviation": "CC", "expansion": "charlie", "notes": ""},
    ]
    out = merge_acronyms_into_acro_content({}, rows, ["cat.a.t1", "cat.b.t2"])
    assert out == {"AA": "alpha", "BB": "bravo"}  # cat.c not in scope


def test_acro_source_scopes_handles_wildcards():
    cats, schemas = _acro_source_scopes(["cat.sch.*", "cat.other.tbl"])
    assert cats == {"cat"}
    assert schemas == {"cat.sch", "cat.other"}


def test_acro_scope_matches():
    cats, schemas = {"cat"}, {"cat.sch"}
    assert _acro_scope_matches({"scope_type": "catalog", "scope": "cat"}, cats, schemas)
    assert _acro_scope_matches({"scope_type": "schema", "scope": "cat.sch"}, cats, schemas)
    assert not _acro_scope_matches({"scope_type": "schema", "scope": "cat"}, cats, schemas)
    assert not _acro_scope_matches({"scope_type": "pattern", "scope": "x"}, cats, schemas)


# --- uploaded description overrides -----------------------------------------

@pytest.fixture(autouse=True)
def _reset_override_cache():
    overrides._customer_overrides_cache = []
    yield
    overrides._customer_overrides_cache = []


def _cfg(mode="comment"):
    return SimpleNamespace(mode=mode)


def test_customer_overrides_for_table_matches_exact_and_schema_wide():
    overrides._customer_overrides_cache = [
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "t1", "column_name": "c1", "comment": "exact"},
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "", "column_name": "c2", "comment": "schema-wide"},
        {"catalog_name": "cat", "schema_name": "other", "table_name": "t1", "column_name": "c3", "comment": "nope"},
    ]
    out = _customer_overrides_for_table("cat.sch.t1")
    assert out == {"c1": "exact", "c2": "schema-wide"}


def test_uploaded_override_applied_when_no_csv():
    overrides._customer_overrides_cache = [
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "t1", "column_name": "c1", "comment": "from upload"},
    ]
    # Non-existent CSV path -> only the uploaded override contributes.
    result = load_override_data_for_table("does_not_exist.csv", _cfg("comment"), "cat.sch.t1")
    assert result == {"c1": {"comment": "from upload"}}


def test_file_csv_wins_over_uploaded_override():
    overrides._customer_overrides_cache = [
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "t1", "column_name": "c1", "comment": "from upload"},
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "t1", "column_name": "c2", "comment": "upload only"},
    ]
    with tempfile.TemporaryDirectory() as d:
        csv_path = os.path.join(d, "metadata_overrides.csv")
        with open(csv_path, "w") as fh:
            fh.write("catalog,schema,table,column,comment\n")
            fh.write("cat,sch,t1,c1,from file\n")
        result = load_override_data_for_table(csv_path, _cfg("comment"), "cat.sch.t1")
    assert result["c1"] == {"comment": "from file"}       # file wins
    assert result["c2"] == {"comment": "upload only"}      # upload fills the gap


def test_uploaded_override_columns_excluded_from_llm_call():
    overrides._customer_overrides_cache = [
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "t1", "column_name": "c1", "comment": "x"},
    ]
    cols = get_override_column_set("does_not_exist.csv", _cfg("comment"), "cat.sch.t1")
    assert "c1" in cols


def test_uploaded_override_ignored_in_pi_mode():
    overrides._customer_overrides_cache = [
        {"catalog_name": "cat", "schema_name": "sch", "table_name": "t1", "column_name": "c1", "comment": "x"},
    ]
    assert load_override_data_for_table("does_not_exist.csv", _cfg("pi"), "cat.sch.t1") == {}
    assert get_override_column_set("does_not_exist.csv", _cfg("pi"), "cat.sch.t1") == set()

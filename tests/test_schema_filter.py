"""Tests for schema_filter_pattern feature in wildcard expansion."""

import sys
import os
from unittest.mock import patch, MagicMock

import pytest

# Re-use conftest stubs (conftest.py is auto-loaded by pytest)
from conftest import install_processing_stubs, uninstall_processing_stubs


@pytest.fixture(autouse=True)
def _processing_stubs():
    """Install/uninstall processing stubs around each test."""
    saved = install_processing_stubs()
    yield
    uninstall_processing_stubs(saved)


def _import_processing():
    """Import processing module after stubs are in place."""
    from dbxmetagen import processing
    return processing


def _setup_spark_mock(table_rows=None):
    """Configure the globally-mocked SparkSession to return given rows.

    Returns the mock spark instance so callers can assert on sql() calls.
    """
    mock_spark = MagicMock()
    if table_rows is None:
        table_rows = []
    mock_spark.sql.return_value.collect.return_value = table_rows
    # Patch the global mock so SparkSession.builder.getOrCreate() returns our mock
    spark_mod = sys.modules["pyspark.sql"]
    spark_mod.SparkSession.builder.getOrCreate.return_value = mock_spark
    return mock_spark


def _make_table_row(table_name="t1", table_type="MANAGED", data_source_format="DELTA"):
    row = MagicMock()
    row.table_name = table_name
    row.table_type = table_type
    row.data_source_format = data_source_format
    return row


# --- get_tables_in_schema filter tests ---

class TestGetTablesInSchemaFilter:
    """Test that schema_filter_pattern is applied BEFORE the SQL query."""

    def test_matching_schema_queries_tables(self):
        proc = _import_processing()
        mock_spark = _setup_spark_mock([_make_table_row("patients")])

        result = proc.get_tables_in_schema("cat", "gold_layer", schema_filter_pattern="^gold")
        assert result == ["cat.gold_layer.patients"]
        mock_spark.sql.assert_called_once()

    def test_non_matching_schema_returns_empty(self):
        proc = _import_processing()
        mock_spark = _setup_spark_mock([_make_table_row()])

        result = proc.get_tables_in_schema("cat", "bronze_raw", schema_filter_pattern="^gold")
        assert result == []
        # SQL was NOT called — filtered before query
        mock_spark.sql.assert_not_called()

    def test_case_insensitive_match(self):
        proc = _import_processing()
        _setup_spark_mock([_make_table_row()])

        result = proc.get_tables_in_schema("cat", "GOLD_layer", schema_filter_pattern="^gold")
        assert len(result) == 1

    def test_invalid_regex_includes_all(self):
        proc = _import_processing()
        mock_spark = _setup_spark_mock([_make_table_row()])

        # Invalid regex — should NOT exclude, just warn
        result = proc.get_tables_in_schema("cat", "any_schema", schema_filter_pattern="[invalid")
        assert len(result) == 1
        mock_spark.sql.assert_called_once()

    def test_no_filter_includes_all(self):
        proc = _import_processing()
        _setup_spark_mock([_make_table_row()])

        result = proc.get_tables_in_schema("cat", "anything", schema_filter_pattern=None)
        assert len(result) == 1

    def test_empty_string_filter_includes_all(self):
        proc = _import_processing()
        _setup_spark_mock([_make_table_row()])

        # Empty string should behave like no filter
        result = proc.get_tables_in_schema("cat", "anything", schema_filter_pattern="")
        assert len(result) == 1


# --- expand_schema_wildcards filter tests ---

class TestExpandSchemaWildcardsFilter:
    """Test that schema_filter_pattern is passed through expand_schema_wildcards."""

    def test_wildcard_with_matching_filter(self):
        proc = _import_processing()
        with patch.object(proc, "get_tables_in_schema", return_value=["cat.gold.t1", "cat.gold.t2"]) as mock_get:
            result = proc.expand_schema_wildcards(["cat.gold.*"], schema_filter_pattern="^gold")
            assert result == ["cat.gold.t1", "cat.gold.t2"]
            mock_get.assert_called_once_with("cat", "gold", "^gold")

    def test_wildcard_with_non_matching_filter(self):
        proc = _import_processing()
        with patch.object(proc, "get_tables_in_schema", return_value=[]) as mock_get:
            result = proc.expand_schema_wildcards(["cat.bronze.*"], schema_filter_pattern="^gold")
            assert result == []
            mock_get.assert_called_once_with("cat", "bronze", "^gold")

    def test_explicit_table_bypasses_filter(self):
        """Explicit table names (no wildcard) should never be filtered."""
        proc = _import_processing()
        with patch.object(proc, "get_tables_in_schema") as mock_get:
            result = proc.expand_schema_wildcards(
                ["cat.bronze.specific_table"],
                schema_filter_pattern="^gold"
            )
            assert result == ["cat.bronze.specific_table"]
            mock_get.assert_not_called()

    def test_mixed_wildcards_and_explicit(self):
        proc = _import_processing()
        with patch.object(proc, "get_tables_in_schema", return_value=["cat.gold.t1"]) as mock_get:
            result = proc.expand_schema_wildcards(
                ["cat.gold.*", "cat.bronze.explicit_table"],
                schema_filter_pattern="^gold"
            )
            assert "cat.gold.t1" in result
            assert "cat.bronze.explicit_table" in result
            assert len(result) == 2

    def test_no_filter_backward_compat(self):
        proc = _import_processing()
        with patch.object(proc, "get_tables_in_schema", return_value=["cat.s.t1"]) as mock_get:
            result = proc.expand_schema_wildcards(["cat.s.*"])
            assert result == ["cat.s.t1"]
            mock_get.assert_called_once_with("cat", "s", None)

    def test_negative_lookahead_pattern(self):
        """Verify negative lookahead works for excluding staging/temp schemas."""
        proc = _import_processing()
        calls = []

        def fake_get(cat, schema, filt=None):
            calls.append(schema)
            import re
            if filt and not re.search(filt, schema, re.IGNORECASE):
                return []
            return [f"{cat}.{schema}.t1"]

        with patch.object(proc, "get_tables_in_schema", side_effect=fake_get):
            result = proc.expand_schema_wildcards(
                ["cat.gold.*", "cat.staging.*", "cat.curated.*"],
                schema_filter_pattern="^(?!staging|temp)"
            )
            assert "cat.gold.t1" in result
            assert "cat.curated.t1" in result


# --- load_table_names_from_csv filter passthrough test ---

class TestLoadTableNamesFromCsvFilter:
    """Test that schema_filter_pattern is passed through to expand_schema_wildcards."""

    def test_csv_filter_passed_to_expand(self, tmp_path):
        proc = _import_processing()
        csv_file = tmp_path / "tables.csv"
        csv_file.write_text("table_name\ncat.gold.*\ncat.bronze.*\n")

        with patch.object(proc, "expand_schema_wildcards", return_value=["cat.gold.t1"]) as mock_expand:
            result = proc.load_table_names_from_csv(str(csv_file), schema_filter_pattern="^gold")
            assert result == ["cat.gold.t1"]
            # Verify the filter was passed positionally to expand_schema_wildcards
            mock_expand.assert_called_once()
            assert mock_expand.call_args[0][1] == "^gold"

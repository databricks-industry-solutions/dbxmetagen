"""Unit tests for override CSV parsing, validation, and pre-LLM exclusion logic.

Tests cover:
- _validate_csv_field_counts: field count mismatches, quoting edge cases
- _table_matches: wildcard and partial matching
- get_override_column_set: mode-aware column exclusion
- load_override_data_for_table: data extraction per mode
"""

import os
import tempfile
import pytest
from unittest.mock import MagicMock

from dbxmetagen.overrides import (
    _validate_csv_field_counts,
    _table_matches,
    get_override_column_set,
    load_override_data_for_table,
    _is_blank,
    _csv_cache,
)
from dbxmetagen.config import MetadataConfig


def _write_csv(content: str) -> str:
    """Write CSV content to a temp file and return its path."""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
    f.write(content)
    f.close()
    return f.name


def _config(mode: str = "comment", csv_path: str = "x.csv") -> MetadataConfig:
    return MetadataConfig(
        skip_yaml_loading=True,
        mode=mode,
        allow_manual_override=True,
        override_csv_path=csv_path,
    )


@pytest.fixture(autouse=True)
def clear_csv_cache():
    """Clear the override CSV cache between tests."""
    _csv_cache.clear()
    yield
    _csv_cache.clear()


# ---------------------------------------------------------------------------
# _validate_csv_field_counts
# ---------------------------------------------------------------------------
class TestValidateCSVFieldCounts:
    def test_valid_csv_passes(self, tmp_path):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            "cat,sch,tbl,col,My comment,,\n"
        )
        try:
            _validate_csv_field_counts(p)  # should not raise
        finally:
            os.unlink(p)

    def test_valid_csv_with_empty_fields(self, tmp_path):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
        )
        try:
            _validate_csv_field_counts(p)
        finally:
            os.unlink(p)

    def test_comma_inside_quoted_field_passes(self):
        p = _write_csv(
            'catalog,schema,table,column,comment,classification,type\n'
            ',,,ssn,"Sales data, including returns",pi,pii\n'
        )
        try:
            _validate_csv_field_counts(p)
        finally:
            os.unlink(p)

    def test_escaped_double_quotes_inside_field_passes(self):
        p = _write_csv(
            'catalog,schema,table,column,comment,classification,type\n'
            ',,,id,"Order tracking, including ""returns"" and refunds",,\n'
        )
        try:
            _validate_csv_field_counts(p)
        finally:
            os.unlink(p)

    def test_mismatched_field_count_raises(self):
        # Unquoted comma splits the field — 8 fields instead of 7
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,id,Sales data, including returns,,\n"
        )
        try:
            with pytest.raises(ValueError, match="line 2 has 8 fields"):
                _validate_csv_field_counts(p)
        finally:
            os.unlink(p)

    def test_broken_quoting_no_outer_quotes_raises(self):
        # What actually happens when an editor strips CSV quoting:
        # the outer quotes are removed, leaving bare commas and quotes
        p = _write_csv(
            'catalog,schema,table,column,comment,classification,type\n'
            ',,,id,Order tracking, including "returns" and refunds,,\n'
        )
        try:
            with pytest.raises(ValueError, match="has .* fields"):
                _validate_csv_field_counts(p)
        finally:
            os.unlink(p)

    def test_multiple_rows_validates_all(self):
        # Row 2 is fine, row 3 has mismatched fields
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
            ",,,id,bad comma, here,,\n"
        )
        try:
            with pytest.raises(ValueError, match="line 3"):
                _validate_csv_field_counts(p)
        finally:
            os.unlink(p)


# ---------------------------------------------------------------------------
# _table_matches
# ---------------------------------------------------------------------------
class TestTableMatches:
    def test_all_blank_matches_any_table(self):
        row = {"catalog": None, "schema": None, "table": None}
        assert _table_matches(row, "cat.sch.tbl") is True

    def test_matching_table_only(self):
        row = {"catalog": None, "schema": None, "table": "my_table"}
        assert _table_matches(row, "cat.sch.my_table") is True

    def test_non_matching_table(self):
        row = {"catalog": None, "schema": None, "table": "other_table"}
        assert _table_matches(row, "cat.sch.my_table") is False

    def test_matching_schema_and_table(self):
        row = {"catalog": None, "schema": "my_schema", "table": "my_table"}
        assert _table_matches(row, "cat.my_schema.my_table") is True

    def test_non_matching_schema(self):
        row = {"catalog": None, "schema": "wrong_schema", "table": "my_table"}
        assert _table_matches(row, "cat.my_schema.my_table") is False

    def test_full_match(self):
        row = {"catalog": "cat", "schema": "sch", "table": "tbl"}
        assert _table_matches(row, "cat.sch.tbl") is True

    def test_case_insensitive(self):
        row = {"catalog": None, "schema": None, "table": "My_Table"}
        assert _table_matches(row, "cat.sch.my_table") is True

    def test_non_fqn_returns_true(self):
        # Non-FQN table names bypass matching (len(parts) != 3).
        # In practice table_names are always FQN, so this is a fallback.
        row = {"catalog": "cat", "schema": "sch", "table": "tbl"}
        assert _table_matches(row, "just_a_name") is True

    def test_catalog_mismatch_rejects(self):
        row = {"catalog": "prod", "schema": None, "table": "my_table"}
        assert _table_matches(row, "dev.sch.my_table") is False


# ---------------------------------------------------------------------------
# get_override_column_set
# ---------------------------------------------------------------------------
class TestGetOverrideColumnSet:
    def test_comment_mode_returns_columns_with_comment_override(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,Sensitive SSN field,pi,pii\n"
            ",,,id,,,\n"
        )
        try:
            cfg = _config("comment", p)
            cols = get_override_column_set(p, cfg, "cat.sch.tbl")
            assert cols == {"ssn"}  # ssn has comment, id does not
        finally:
            os.unlink(p)

    def test_pi_mode_returns_columns_with_classification_override(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
            ",,,id,Some comment,,\n"
        )
        try:
            cfg = _config("pi", p)
            cols = get_override_column_set(p, cfg, "cat.sch.tbl")
            assert cols == {"ssn"}  # ssn has classification, id only has comment
        finally:
            os.unlink(p)

    def test_domain_mode_returns_empty(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,comment,pi,pii\n"
        )
        try:
            cfg = _config("domain", p)
            cols = get_override_column_set(p, cfg, "cat.sch.tbl")
            assert cols == set()
        finally:
            os.unlink(p)

    def test_table_level_row_excluded(self):
        """Table-level overrides (no column) should not appear in column set."""
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,my_table,,Table-level comment,,\n"
            ",,my_table,col1,Column comment,,\n"
        )
        try:
            cfg = _config("comment", p)
            cols = get_override_column_set(p, cfg, "cat.sch.my_table")
            assert cols == {"col1"}
        finally:
            os.unlink(p)

    def test_wildcard_ssn_matches_any_table(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
        )
        try:
            cfg = _config("pi", p)
            assert get_override_column_set(p, cfg, "cat.sch.table_a") == {"ssn"}
            _csv_cache.clear()
            assert get_override_column_set(p, cfg, "cat.sch.table_b") == {"ssn"}
        finally:
            os.unlink(p)

    def test_table_scoped_override_only_matches_that_table(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,tpn_orders,amount,Revenue,,\n"
        )
        try:
            cfg = _config("comment", p)
            assert get_override_column_set(p, cfg, "cat.sch.tpn_orders") == {"amount"}
            _csv_cache.clear()
            assert get_override_column_set(p, cfg, "cat.sch.other_table") == set()
        finally:
            os.unlink(p)

    def test_missing_file_returns_empty(self):
        cfg = _config("comment", "/nonexistent/path.csv")
        assert get_override_column_set("/nonexistent/path.csv", cfg, "cat.sch.tbl") == set()


# ---------------------------------------------------------------------------
# load_override_data_for_table
# ---------------------------------------------------------------------------
class TestLoadOverrideDataForTable:
    def test_comment_mode_loads_comment_values(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,Sensitive SSN field,pi,pii\n"
        )
        try:
            cfg = _config("comment", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tbl")
            assert "ssn" in data
            assert data["ssn"]["comment"] == "Sensitive SSN field"
        finally:
            os.unlink(p)

    def test_pi_mode_loads_classification_values(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
        )
        try:
            cfg = _config("pi", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tbl")
            assert "ssn" in data
            assert data["ssn"]["classification"] == "pi"
            assert data["ssn"]["type"] == "pii"
        finally:
            os.unlink(p)

    def test_comment_mode_skips_rows_without_comment(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
        )
        try:
            cfg = _config("comment", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tbl")
            assert data == {}  # no comment value, so nothing for comment mode
        finally:
            os.unlink(p)

    def test_pi_mode_skips_rows_without_classification(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,col1,Some comment,,\n"
        )
        try:
            cfg = _config("pi", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tbl")
            assert data == {}
        finally:
            os.unlink(p)

    def test_domain_mode_returns_empty(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,comment,pi,pii\n"
        )
        try:
            cfg = _config("domain", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tbl")
            assert data == {}
        finally:
            os.unlink(p)

    def test_quoted_comma_in_comment_parsed_correctly(self):
        p = _write_csv(
            'catalog,schema,table,column,comment,classification,type\n'
            ',,tpn_orders,amount,"Sales data, including returns",,\n'
        )
        try:
            cfg = _config("comment", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tpn_orders")
            assert "amount" in data
            assert data["amount"]["comment"] == "Sales data, including returns"
        finally:
            os.unlink(p)

    def test_escaped_double_quotes_in_comment_parsed_correctly(self):
        p = _write_csv(
            'catalog,schema,table,column,comment,classification,type\n'
            ',,tpn_orders,amount,"Order tracking, including ""returns"" and refunds",,\n'
        )
        try:
            cfg = _config("comment", p)
            data = load_override_data_for_table(p, cfg, "cat.sch.tpn_orders")
            assert "amount" in data
            assert data["amount"]["comment"] == 'Order tracking, including "returns" and refunds'
        finally:
            os.unlink(p)

    def test_multiple_tables_and_wildcards(self):
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
            ",,orders,amount,Revenue,,\n"
        )
        try:
            cfg = _config("pi", p)
            # ssn wildcard matches any table
            data = load_override_data_for_table(p, cfg, "cat.sch.customers")
            assert "ssn" in data
            assert "amount" not in data

            _csv_cache.clear()
            # orders table gets amount (table-scoped) in comment mode
            cfg2 = _config("comment", p)
            data2 = load_override_data_for_table(p, cfg2, "cat.sch.orders")
            assert "amount" in data2
        finally:
            os.unlink(p)

    def test_column_with_both_comment_and_pi_overrides(self):
        """A column with both comment and PI overrides should be excluded in BOTH modes."""
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,Sensitive SSN field,pi,pii\n"
        )
        try:
            # Comment mode: ssn has a comment override → should be excluded from LLM
            cfg_c = _config("comment", p)
            assert get_override_column_set(p, cfg_c, "cat.sch.tbl") == {"ssn"}
            data_c = load_override_data_for_table(p, cfg_c, "cat.sch.tbl")
            assert data_c["ssn"]["comment"] == "Sensitive SSN field"

            _csv_cache.clear()

            # PI mode: ssn has classification override → should also be excluded
            cfg_p = _config("pi", p)
            assert get_override_column_set(p, cfg_p, "cat.sch.tbl") == {"ssn"}
            data_p = load_override_data_for_table(p, cfg_p, "cat.sch.tbl")
            assert data_p["ssn"]["classification"] == "pi"
            assert data_p["ssn"]["type"] == "pii"
        finally:
            os.unlink(p)

    def test_pi_only_override_does_not_exclude_from_comment_mode(self):
        """A column with PI override but NO comment should NOT be excluded from comment LLM.

        This is intended: without a comment replacement value, the column would
        have no comment at all. The LLM should still generate one.
        """
        p = _write_csv(
            "catalog,schema,table,column,comment,classification,type\n"
            ",,,ssn,,pi,pii\n"
        )
        try:
            cfg = _config("comment", p)
            cols = get_override_column_set(p, cfg, "cat.sch.tbl")
            assert cols == set()  # NOT excluded — no comment replacement available
            data = load_override_data_for_table(p, cfg, "cat.sch.tbl")
            assert data == {}  # nothing to inject in comment mode
        finally:
            os.unlink(p)

    def test_real_world_csv_from_user(self):
        """Test the exact CSV format a user would create for overrides."""
        p = _write_csv(
            'catalog,schema,table,column,comment,classification,type\n'
            ',,,ssn,"Sales data, including returns",pi,pii\n'
            ',,,id,"Order tracking, including ""returns"" and refunds",,\n'
            ',,tpn_orders,tpn_order_amount,"Sales data, including returns",,\n'
            ',,tpn_orders,,"Order tracking, including ""returns"" and refunds",,\n'
        )
        try:
            # Validation passes (no field count errors)
            _validate_csv_field_counts(p)

            # PI mode: ssn wildcard matches any table
            cfg_pi = _config("pi", p)
            cols = get_override_column_set(p, cfg_pi, "cat.sch.any_table")
            assert "ssn" in cols
            assert "id" not in cols  # id row has no classification/type

            _csv_cache.clear()

            # Comment mode: both ssn and id have comment overrides (wildcard)
            cfg_c = _config("comment", p)
            cols_c = get_override_column_set(p, cfg_c, "cat.sch.any_table")
            assert cols_c == {"ssn", "id"}

            _csv_cache.clear()

            # Comment mode on tpn_orders: gets wildcard ssn+id PLUS table-scoped tpn_order_amount
            cols_tpn = get_override_column_set(p, cfg_c, "cat.sch.tpn_orders")
            assert "ssn" in cols_tpn
            assert "id" in cols_tpn
            assert "tpn_order_amount" in cols_tpn

            _csv_cache.clear()

            # Verify the escaped quotes parse correctly
            data = load_override_data_for_table(p, cfg_c, "cat.sch.any_table")
            assert data["id"]["comment"] == 'Order tracking, including "returns" and refunds'
            assert data["ssn"]["comment"] == "Sales data, including returns"
        finally:
            os.unlink(p)

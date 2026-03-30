"""
Unit and integration tests for metadata override logic.

Covers:
- _is_blank helper
- build_condition pattern matching
- apply_overrides_with_loop for PI, comment (column + table-level), domain modes
- override_metadata_from_csv CSV loading and file-not-found handling
- process_and_add_ddl wiring (both column_df and table_df get overrides)
"""

import os
import sys
import math
import tempfile
import pytest
from unittest.mock import MagicMock, patch, call

from dbxmetagen.config import MetadataConfig
from dbxmetagen.overrides import (
    _is_blank,
    _csv_cache,
    build_condition,
    apply_overrides_with_loop,
    override_metadata_from_csv,
    get_override_column_set,
    load_override_data_for_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(mode="comment", **overrides):
    defaults = dict(
        skip_yaml_loading=True,
        catalog_name="cat",
        schema_name="sch",
        mode=mode,
        allow_manual_override=True,
        override_csv_path="metadata_overrides.csv",
    )
    defaults.update(overrides)
    return MetadataConfig(**defaults)


def _mock_df(match_count=None):
    """Return a MagicMock DataFrame whose withColumn returns itself.

    Args:
        match_count: If set, df.filter(...).count() returns this value.
                     None means default MagicMock behavior (truthy, != 0).
    """
    df = MagicMock()
    df.withColumn.return_value = df
    if match_count is not None:
        df.filter.return_value.count.return_value = match_count
    return df


# ===========================================================================
# TestIsBlank
# ===========================================================================

class TestIsBlank:
    def test_none_is_blank(self):
        assert _is_blank(None) is True

    def test_nan_is_blank(self):
        assert _is_blank(float("nan")) is True

    def test_math_nan_is_blank(self):
        assert _is_blank(math.nan) is True

    def test_empty_string_is_blank(self):
        assert _is_blank("") is True

    def test_string_value_is_not_blank(self):
        assert _is_blank("pii") is False

    def test_zero_is_not_blank(self):
        assert _is_blank(0) is False

    def test_false_is_not_blank(self):
        assert _is_blank(False) is False


# ===========================================================================
# TestBuildCondition
# ===========================================================================

class TestBuildCondition:
    """Tests for build_condition dynamic condition building.

    Since PySpark col/reduce are mocked, we verify the function doesn't raise
    for valid patterns and does raise only when no fields are provided.
    """

    def test_only_column(self):
        df = _mock_df()
        result = build_condition(df, None, "ssn", None, None)
        assert result is not None

    def test_empty_strings_treated_as_none(self):
        df = _mock_df()
        result = build_condition(df, "", "ssn", "", "")
        assert result is not None

    def test_all_params(self):
        df = _mock_df()
        result = build_condition(df, "my_table", "my_col", "my_schema", "my_catalog")
        assert result is not None

    def test_table_level_all_three(self):
        df = _mock_df()
        result = build_condition(df, "my_table", None, "my_schema", "my_catalog")
        assert result is not None

    def test_table_and_column(self):
        df = _mock_df()
        result = build_condition(df, "my_table", "my_col", None, None)
        assert result is not None

    def test_schema_and_column(self):
        df = _mock_df()
        result = build_condition(df, None, "my_col", "my_schema", None)
        assert result is not None

    def test_schema_table_column(self):
        df = _mock_df()
        result = build_condition(df, "my_table", "my_col", "my_schema", None)
        assert result is not None

    def test_table_only(self):
        df = _mock_df()
        result = build_condition(df, "my_table", None, None, None)
        assert result is not None

    def test_schema_and_table(self):
        df = _mock_df()
        result = build_condition(df, "my_table", None, "my_schema", None)
        assert result is not None

    def test_catalog_only(self):
        df = _mock_df()
        result = build_condition(df, None, None, None, "my_catalog")
        assert result is not None

    def test_raises_for_no_params_at_all(self):
        df = _mock_df()
        with pytest.raises(ValueError, match="No match fields provided"):
            build_condition(df, None, None, None, None)

    def test_raises_for_all_empty_strings(self):
        df = _mock_df()
        with pytest.raises(ValueError, match="No match fields provided"):
            build_condition(df, "", None, "", "")

    def test_case_insensitive_column(self):
        df = _mock_df()
        result = build_condition(df, None, "SSN", None, None)
        assert result is not None

    def test_case_insensitive_all_params(self):
        df = _mock_df()
        result = build_condition(df, "My_Table", "My_Col", "My_Schema", "My_Catalog")
        assert result is not None


# ===========================================================================
# TestApplyOverridesWithLoop
# ===========================================================================

class TestApplyOverridesWithLoop:

    def test_empty_csv_dict_returns_df_unchanged(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        result = apply_overrides_with_loop(df, [], config)
        assert result is df
        df.withColumn.assert_not_called()

    def test_none_csv_dict_returns_df_unchanged(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        result = apply_overrides_with_loop(df, None, config)
        assert result is df

    # --- PI mode ---

    def test_pi_applies_classification_and_type(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        csv_dict = [{"column": "ssn", "classification": "pi", "type": "pii"}]

        result = apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 2
        col_names = [c.args[0] for c in df.withColumn.call_args_list]
        assert "classification" in col_names
        assert "type" in col_names

    def test_pi_skips_row_with_no_column(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        csv_dict = [{"column": None, "classification": "pi", "type": "pii"}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_pi_skips_row_with_blank_overrides(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        csv_dict = [{"column": "ssn", "classification": None, "type": None}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_pi_applies_only_classification_when_type_blank(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        csv_dict = [{"column": "ssn", "classification": "pi", "type": None}]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "classification"

    # --- Comment mode (column-level) ---

    def test_comment_column_level_applies_column_content(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{"column": "name", "comment": "Person's full name"}]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "column_content"

    def test_comment_skips_blank_comment(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{"column": "name", "comment": None}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_comment_skips_empty_string_comment(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{"column": "name", "comment": ""}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    # --- Comment mode with partial params ---

    def test_comment_column_with_table_only(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{"table": "orders", "column": "cust_id", "comment": "Customer ID"}]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "column_content"

    def test_comment_column_with_schema_and_table(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{
            "schema": "sch", "table": "orders",
            "column": "cust_id", "comment": "Customer ID",
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "column_content"

    # --- Comment mode (table-level) ---

    def test_comment_table_level_applies_column_content(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{
            "catalog": "cat", "schema": "sch", "table": "orders",
            "column": None, "comment": "Order tracking table",
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "column_content"

    def test_comment_table_level_with_table_only(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{"table": "orders", "column": None, "comment": "Order table"}]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "column_content"

    def test_comment_table_level_skips_when_no_table(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [{"column": None, "table": None, "comment": "Some comment"}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    # --- Domain mode ---

    def test_domain_applies_domain_and_subdomain(self):
        df = _mock_df()
        config = _make_config(mode="domain")
        csv_dict = [{
            "catalog": "cat", "schema": "sch", "table": "orders",
            "domain": "Sales", "subdomain": "Order Management",
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 2
        col_names = [c.args[0] for c in df.withColumn.call_args_list]
        assert "domain" in col_names
        assert "subdomain" in col_names

    def test_domain_skips_when_no_table(self):
        df = _mock_df()
        config = _make_config(mode="domain")
        csv_dict = [{"table": None, "domain": "Sales", "subdomain": None}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_domain_skips_blank_overrides(self):
        df = _mock_df()
        config = _make_config(mode="domain")
        csv_dict = [{
            "catalog": "cat", "schema": "sch", "table": "orders",
            "domain": None, "subdomain": None,
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_domain_applies_only_domain_when_subdomain_blank(self):
        df = _mock_df()
        config = _make_config(mode="domain")
        csv_dict = [{
            "catalog": "cat", "schema": "sch", "table": "orders",
            "domain": "Sales", "subdomain": None,
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "domain"

    # --- Invalid mode ---

    def test_invalid_mode_raises(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        config.mode = "invalid_mode"
        csv_dict = [{"column": "x", "comment": "y"}]

        with pytest.raises(ValueError, match="Invalid mode"):
            apply_overrides_with_loop(df, csv_dict, config)

    # --- PI mode: partial overrides ---

    def test_pi_applies_only_type_when_classification_blank(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        csv_dict = [{"column": "ssn", "classification": None, "type": "pii"}]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "type"

    # --- Domain mode: partial overrides ---

    def test_domain_applies_only_subdomain_when_domain_blank(self):
        df = _mock_df()
        config = _make_config(mode="domain")
        csv_dict = [{
            "catalog": "cat", "schema": "sch", "table": "orders",
            "domain": None, "subdomain": "Order Management",
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 1
        assert df.withColumn.call_args_list[0].args[0] == "subdomain"

    # --- Comment mode: multiple overrides ---

    def test_comment_multiple_overrides_in_csv(self):
        df = _mock_df()
        config = _make_config(mode="comment")
        csv_dict = [
            {"column": "ssn", "comment": "Social Security Number"},
            {"column": "name", "comment": "Full name"},
            {"column": "age", "comment": ""},  # blank -> skipped
        ]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 2
        for c in df.withColumn.call_args_list:
            assert c.args[0] == "column_content"

    # --- Zero-match path (critical: tests that _count_matches==0 prevents withColumn) ---

    def test_pi_zero_match_skips_override(self):
        df = _mock_df(match_count=0)
        config = _make_config(mode="pi")
        csv_dict = [{"column": "nonexistent_col", "classification": "pi", "type": "pii"}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_comment_column_zero_match_skips_override(self):
        df = _mock_df(match_count=0)
        config = _make_config(mode="comment")
        csv_dict = [{"column": "nonexistent_col", "comment": "Some comment"}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_comment_table_level_zero_match_skips_override(self):
        df = _mock_df(match_count=0)
        config = _make_config(mode="comment")
        csv_dict = [{"table": "nonexistent_table", "column": None, "comment": "Table comment"}]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_domain_zero_match_skips_override(self):
        df = _mock_df(match_count=0)
        config = _make_config(mode="domain")
        csv_dict = [{
            "catalog": "cat", "schema": "sch", "table": "nonexistent",
            "domain": "Sales", "subdomain": "Order Management",
        }]

        apply_overrides_with_loop(df, csv_dict, config)
        df.withColumn.assert_not_called()

    def test_pi_mix_of_matching_and_nonmatching_rows(self):
        """When some CSV rows match and some don't, only matching ones produce withColumn."""
        df = MagicMock()
        df.withColumn.return_value = df
        # First call to filter().count() returns 1, second returns 0
        df.filter.return_value.count.side_effect = [1, 0]

        config = _make_config(mode="pi")
        csv_dict = [
            {"column": "ssn", "classification": "pi", "type": "pii"},
            {"column": "nonexistent", "classification": "pi", "type": "pii"},
        ]

        apply_overrides_with_loop(df, csv_dict, config)
        assert df.withColumn.call_count == 2  # 1 matched row x 2 columns

    # --- Multiple rows ---

    def test_pi_multiple_rows_counted(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        csv_dict = [
            {"column": "ssn", "classification": "pi", "type": "pii"},
            {"column": "name", "classification": "pi", "type": "pii"},
            {"column": None, "classification": "pi", "type": "pii"},  # skipped
        ]

        apply_overrides_with_loop(df, csv_dict, config)
        # 2 applied rows x 2 withColumn calls each = 4
        assert df.withColumn.call_count == 4


# ===========================================================================
# TestOverrideMetadataFromCSV
# ===========================================================================

class TestOverrideMetadataFromCSV:

    def test_empty_path_returns_df_unchanged(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        result = override_metadata_from_csv(df, "", config)
        assert result is df

    def test_none_path_returns_df_unchanged(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        result = override_metadata_from_csv(df, None, config)
        assert result is df

    def test_nonexistent_file_returns_df_unchanged(self):
        df = _mock_df()
        config = _make_config(mode="pi")
        result = override_metadata_from_csv(df, "/nonexistent/path.csv", config)
        assert result is df

    @patch("dbxmetagen.overrides.apply_overrides_with_loop")
    def test_valid_csv_calls_apply_overrides(self, mock_apply):
        mock_apply.return_value = _mock_df()
        df = _mock_df()
        config = _make_config(mode="pi")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,comment,classification,type\n")
            f.write(",,,ssn,,pi,pii\n")
            tmp = f.name

        try:
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 1
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            override_metadata_from_csv(df, tmp, config)
            mock_apply.assert_called_once()
            call_args = mock_apply.call_args
            assert call_args.args[0] is df
            assert len(call_args.args[1]) == 1
            assert call_args.args[2] is config
        finally:
            os.unlink(tmp)

    @patch("dbxmetagen.overrides.apply_overrides_with_loop")
    def test_df_label_passed_through_to_apply(self, mock_apply):
        mock_apply.return_value = _mock_df()
        df = _mock_df()
        config = _make_config(mode="comment")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,comment,classification,type\n")
            f.write(",,,ssn,Some comment,,\n")
            tmp = f.name

        try:
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 1
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            override_metadata_from_csv(df, tmp, config, df_label="column_df")
            assert mock_apply.call_args.kwargs.get("df_label") == "column_df"
        finally:
            os.unlink(tmp)

    def test_empty_csv_returns_df_unchanged(self):
        df = _mock_df()
        config = _make_config(mode="pi")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,comment,classification,type\n")
            tmp = f.name

        try:
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 0
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            result = override_metadata_from_csv(df, tmp, config)
            assert result is df
        finally:
            os.unlink(tmp)

    def test_large_csv_raises(self):
        df = _mock_df()
        config = _make_config(mode="pi")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,comment,classification,type\n")
            f.write(",,,ssn,,pi,pii\n")
            tmp = f.name

        try:
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 20000
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            with pytest.raises(ValueError, match="too large"):
                override_metadata_from_csv(df, tmp, config)
        finally:
            os.unlink(tmp)

    @patch("dbxmetagen.overrides.apply_overrides_with_loop")
    def test_blank_cells_parsed_as_blank_not_string_nan(self, mock_apply):
        """Blank CSV cells must be treated as blank (None or NaN), never string 'nan'."""
        mock_apply.return_value = _mock_df()
        df = _mock_df()
        config = _make_config(mode="pi")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,comment,classification,type\n")
            f.write(",,,ssn,,pi,pii\n")
            f.write(",,,tpn,some,pi,pii\n")
            tmp = f.name

        try:
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 2
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            override_metadata_from_csv(df, tmp, config)
            csv_dict = mock_apply.call_args.args[1]
            for row in csv_dict:
                assert _is_blank(row["catalog"]), f"Expected blank, got {row['catalog']!r}"
                assert _is_blank(row["schema"]), f"Expected blank, got {row['schema']!r}"
                assert _is_blank(row["table"]), f"Expected blank, got {row['table']!r}"
            assert csv_dict[0]["column"] == "ssn"
            assert _is_blank(csv_dict[0]["comment"])
            assert csv_dict[1]["column"] == "tpn"
            assert csv_dict[1]["comment"] == "some"
        finally:
            os.unlink(tmp)

    @patch("dbxmetagen.overrides.apply_overrides_with_loop")
    def test_literal_nan_and_none_preserved(self, mock_apply):
        """Literal 'nan' / 'None' typed in a cell are kept as real strings."""
        mock_apply.return_value = _mock_df()
        df = _mock_df()
        config = _make_config(mode="pi")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,comment,classification,type\n")
            f.write("nan,None,,nan_col,None,pi,pii\n")
            tmp = f.name

        try:
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 1
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            override_metadata_from_csv(df, tmp, config)
            row = mock_apply.call_args.args[1][0]
            assert row["catalog"] == "nan", f"Literal 'nan' should be preserved, got {row['catalog']!r}"
            assert row["schema"] == "None", f"Literal 'None' should be preserved, got {row['schema']!r}"
            assert _is_blank(row["table"])
            assert row["column"] == "nan_col"
            assert row["comment"] == "None"
        finally:
            os.unlink(tmp)


# ===========================================================================
# TestProcessAndAddDdlOverrideWiring (integration)
# ===========================================================================

from conftest import install_processing_stubs, uninstall_processing_stubs


class TestProcessAndAddDdlOverrideWiring:
    """Verify that process_and_add_ddl calls override_metadata_from_csv
    on both column_df and table_df."""

    @classmethod
    def setup_class(cls):
        cls._saved = install_processing_stubs()
        import dbxmetagen.processing as pm
        cls._processing_mod = pm

    @classmethod
    def teardown_class(cls):
        uninstall_processing_stubs(cls._saved)

    def _make_full_config(self, mode="comment", allow_override=True, csv_path="overrides.csv"):
        return _make_config(
            mode=mode,
            allow_manual_override=allow_override,
            override_csv_path=csv_path,
        )

    def test_override_column_df_only_in_comment_mode(self):
        """In comment mode, process_and_add_ddl only overrides column_df.
        table_df override happens inside add_ddl_to_dfs after summarization."""
        pm = self._processing_mod
        column_df = _mock_df()
        table_df = _mock_df()
        config = self._make_full_config(mode="comment")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(column_df, table_df)),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c, **kw: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")

            assert mock_ov.call_count == 1
            assert mock_ov.call_args_list[0].args[0] is column_df
            assert mock_ov.call_args_list[0].kwargs.get("df_label") == "column_df"

    def test_override_both_dfs_in_pi_mode(self):
        """In PI mode, both column_df and table_df are overridden in process_and_add_ddl."""
        pm = self._processing_mod
        column_df = _mock_df()
        table_df = _mock_df()
        config = self._make_full_config(mode="pi")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(column_df, table_df)),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c, **kw: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")

            assert mock_ov.call_count == 2
            assert mock_ov.call_args_list[0].kwargs.get("df_label") == "column_df"
            assert mock_ov.call_args_list[1].kwargs.get("df_label") == "table_df"

    def test_override_not_called_when_disabled(self):
        pm = self._processing_mod
        config = self._make_full_config(allow_override=False)

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(_mock_df(), _mock_df())),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv") as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")
            mock_ov.assert_not_called()

    def test_override_skips_none_dfs(self):
        pm = self._processing_mod
        config = self._make_full_config(mode="pi")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(_mock_df(), None)),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c, **kw: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")
            assert mock_ov.call_count == 1
            assert mock_ov.call_args_list[0].args[0] is not None

    def test_override_passes_correct_csv_path(self):
        pm = self._processing_mod
        config = self._make_full_config(csv_path="/custom/overrides.csv", mode="pi")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(_mock_df(), _mock_df())),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c, **kw: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")
            for c in mock_ov.call_args_list:
                assert c.args[1] == "/custom/overrides.csv"

    def test_override_both_dfs_in_domain_mode(self):
        """In domain mode, both column_df and table_df are overridden in process_and_add_ddl."""
        pm = self._processing_mod
        column_df = _mock_df()
        table_df = _mock_df()
        config = self._make_full_config(mode="domain")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(column_df, table_df)),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c, **kw: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")

            assert mock_ov.call_count == 2
            assert mock_ov.call_args_list[0].kwargs.get("df_label") == "column_df"
            assert mock_ov.call_args_list[1].kwargs.get("df_label") == "table_df"

    def test_both_none_dfs_returns_empty_dict(self):
        pm = self._processing_mod
        config = self._make_full_config()

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(None, None)),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv") as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            result = pm.process_and_add_ddl(config, "cat.sch.my_table")
            mock_ov.assert_not_called()
            assert "_skip_reason" in result
            assert len(result) == 1


class TestAddDdlToDfsOverrideOrdering:
    """Verify that add_ddl_to_dfs calls override_metadata_from_csv on table_df
    AFTER summarize_table_content in comment mode."""

    @classmethod
    def setup_class(cls):
        cls._saved = install_processing_stubs()
        import dbxmetagen.processing as pm
        cls._processing_mod = pm

    @classmethod
    def teardown_class(cls):
        uninstall_processing_stubs(cls._saved)

    def test_comment_mode_table_override_after_summarization(self):
        """In comment mode, override_metadata_from_csv is called on table_df
        AFTER summarize_table_content, so the override is not destroyed."""
        pm = self._processing_mod
        table_df = _mock_df()
        column_df = _mock_df()
        column_df.columns = ["column_content"]

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
            apply_ddl=False,
        )

        call_order = []

        def track_summarize(df, cfg, name):
            call_order.append("summarize_table_content")
            return df

        def track_split(df):
            call_order.append("split_name_for_df")
            return df

        def track_override(df, path, cfg, **kw):
            call_order.append("override_metadata_from_csv")
            return df

        with (
            patch.object(pm, "summarize_table_content", side_effect=track_summarize),
            patch.object(pm, "split_name_for_df", side_effect=track_split),
            patch.object(pm, "override_metadata_from_csv", side_effect=track_override) as mock_ov,
            patch.object(pm, "add_ddl_to_table_comment_df", return_value=MagicMock()),
            patch.object(pm, "add_ddl_to_column_comment_df", return_value=MagicMock()),
        ):
            pm.add_ddl_to_dfs(config, table_df, column_df, "cat.sch.my_table")

            mock_ov.assert_called_once()
            assert mock_ov.call_args.kwargs.get("df_label") == "table_df"

            assert call_order == [
                "summarize_table_content",
                "split_name_for_df",
                "override_metadata_from_csv",
            ]

    def test_comment_mode_no_table_override_when_disabled(self):
        """When allow_manual_override is False, add_ddl_to_dfs should NOT call override."""
        pm = self._processing_mod
        table_df = _mock_df()
        column_df = _mock_df()
        column_df.columns = ["column_content"]

        config = _make_config(
            mode="comment",
            allow_manual_override=False,
            apply_ddl=False,
        )

        with (
            patch.object(pm, "summarize_table_content", side_effect=lambda df, cfg, name: df),
            patch.object(pm, "split_name_for_df", side_effect=lambda df: df),
            patch.object(pm, "override_metadata_from_csv") as mock_ov,
            patch.object(pm, "add_ddl_to_table_comment_df", return_value=MagicMock()),
            patch.object(pm, "add_ddl_to_column_comment_df", return_value=MagicMock()),
        ):
            pm.add_ddl_to_dfs(config, table_df, column_df, "cat.sch.my_table")
            mock_ov.assert_not_called()

    def test_pi_mode_no_table_override_in_add_ddl(self):
        """In PI mode, add_ddl_to_dfs should NOT call override_metadata_from_csv
        (it's handled in process_and_add_ddl instead)."""
        pm = self._processing_mod
        column_df = _mock_df()
        table_df = _mock_df()

        config = _make_config(
            mode="pi",
            allow_manual_override=True,
            apply_ddl=False,
        )

        with (
            patch.object(pm, "add_column_ddl_to_pi_df", return_value=MagicMock()),
            patch.object(pm, "create_pi_table_df", return_value=None),
            patch.object(pm, "override_metadata_from_csv") as mock_ov,
        ):
            pm.add_ddl_to_dfs(config, table_df, column_df, "cat.sch.my_table")
            mock_ov.assert_not_called()


# ===========================================================================
# TestCSVParsingRobustness
# ===========================================================================

class TestCSVParsingRobustness:
    """Test that override CSV parsing handles commas, quotes, unicode, and
    misformed rows correctly after the skipinitialspace + validation fixes."""

    HEADER = "catalog,schema,table,column,comment,classification,type\n"

    def _write_csv(self, *data_lines):
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(self.HEADER)
        for line in data_lines:
            f.write(line + "\n")
        f.close()
        return f.name

    def _run_csv(self, tmp, mode="comment"):
        """Call override_metadata_from_csv with a mock Spark, return the csv_dict
        passed to apply_overrides_with_loop."""
        df = _mock_df()
        config = _make_config(mode=mode)
        with patch("dbxmetagen.overrides.apply_overrides_with_loop") as mock_apply:
            mock_apply.return_value = df
            mock_spark_df = MagicMock()
            mock_spark_df.count.return_value = 1
            mock_spark = MagicMock()
            mock_spark.createDataFrame.return_value = mock_spark_df
            pyspark_sql = sys.modules["pyspark.sql"]
            pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark

            override_metadata_from_csv(df, tmp, config)
            return mock_apply.call_args.args[1]

    # --- Commas ---

    def test_comment_with_comma_quoted(self):
        tmp = self._write_csv(',,,ssn,"He said hello, goodbye",,')
        try:
            rows = self._run_csv(tmp)
            assert rows[0]["comment"] == "He said hello, goodbye"
        finally:
            os.unlink(tmp)

    def test_comment_with_comma_space_before_quote(self):
        """Leading space + quoted field should parse correctly with skipinitialspace."""
        tmp = self._write_csv(',,,ssn, "Sales data, including returns",,')
        try:
            rows = self._run_csv(tmp)
            assert rows[0]["comment"] == "Sales data, including returns"
        finally:
            os.unlink(tmp)

    # --- Quotes ---

    def test_comment_with_embedded_double_quotes(self):
        tmp = self._write_csv(',,,ssn,"He said ""hello"" to me",,')
        try:
            rows = self._run_csv(tmp)
            assert rows[0]["comment"] == 'He said "hello" to me'
        finally:
            os.unlink(tmp)

    def test_comment_with_single_quotes(self):
        tmp = self._write_csv(",,,ssn,it's a test,,")
        try:
            rows = self._run_csv(tmp)
            assert rows[0]["comment"] == "it's a test"
        finally:
            os.unlink(tmp)

    # --- Special characters ---

    def test_comment_with_parentheses_and_semicolons(self):
        tmp = self._write_csv(",,,ssn,(a; b),,")
        try:
            rows = self._run_csv(tmp)
            assert rows[0]["comment"] == "(a; b)"
        finally:
            os.unlink(tmp)

    def test_unicode_characters_in_comment(self):
        tmp = self._write_csv(",,,name,Nom complet de l'utilisateur,,")
        try:
            rows = self._run_csv(tmp)
            assert "complet" in rows[0]["comment"]
        finally:
            os.unlink(tmp)

    # --- Table-level comment ---

    def test_table_level_comment_with_comma(self):
        tmp = self._write_csv(',,orders,,"Order tracking, including returns",,')
        try:
            rows = self._run_csv(tmp)
            assert rows[0]["comment"] == "Order tracking, including returns"
            assert _is_blank(rows[0]["column"])
            assert rows[0]["table"] == "orders"
        finally:
            os.unlink(tmp)

    # --- Misparse detection ---

    def test_unquoted_comma_raises(self):
        """An unquoted comma that creates extra columns should raise ValueError."""
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(self.HEADER)
        # 8 fields instead of 7 -- the unquoted comma in comment splits it
        f.write(",,,ssn,hello, world,,\n")
        f.close()
        try:
            with pytest.raises(ValueError, match="fields"):
                self._run_csv(f.name)
        finally:
            os.unlink(f.name)

    def test_unknown_column_warns_but_does_not_raise(self):
        """A truly unknown column should trigger a warning but not an error."""
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write("catalog,schema,table,column,comment,classification,type,bogus_col\n")
        f.write(",,,ssn,A comment,,,whatever\n")
        f.close()
        try:
            with patch("dbxmetagen.overrides.logger") as mock_logger:
                rows = self._run_csv(f.name)
                assert rows[0]["comment"] == "A comment"
                mock_logger.warning.assert_called_once()
                warn_msg = mock_logger.warning.call_args[0][0]
                assert "unrecognized columns" in warn_msg
        finally:
            os.unlink(f.name)

    def test_known_extra_column_does_not_warn(self):
        """Known optional columns like domain should not trigger a warning."""
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write("catalog,schema,table,column,comment,classification,type,domain\n")
        f.write(",,,ssn,A comment,,,finance\n")
        f.close()
        try:
            with patch("dbxmetagen.overrides.logger") as mock_logger:
                rows = self._run_csv(f.name)
                assert rows[0]["comment"] == "A comment"
                assert rows[0]["domain"] == "finance"
                mock_logger.warning.assert_not_called()
        finally:
            os.unlink(f.name)


# ===========================================================================
# TestGetOverrideColumnSet
# ===========================================================================

class TestGetOverrideColumnSet:
    HEADER = "catalog,schema,table,column,comment,classification,type\n"

    def _write_csv(self, *data_lines):
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(self.HEADER)
        for line in data_lines:
            f.write(line + "\n")
        f.close()
        return f.name

    def setup_method(self):
        _csv_cache.clear()

    def test_comment_mode_returns_columns_with_comments(self):
        tmp = self._write_csv(
            "cat,sch,orders,cust_id,Customer ID,,",
            "cat,sch,orders,order_date,Order date,,",
            "cat,sch,orders,amount,,,",  # blank comment -> excluded
        )
        try:
            config = _make_config(mode="comment")
            result = get_override_column_set(tmp, config, "cat.sch.orders")
            assert result == {"cust_id", "order_date"}
        finally:
            os.unlink(tmp)

    def test_pi_mode_returns_columns_with_classification_or_type(self):
        tmp = self._write_csv(
            "cat,sch,orders,ssn,,pi,pii",
            "cat,sch,orders,name,,pi,",  # type blank, classification present
            "cat,sch,orders,age,,,",  # both blank -> excluded
        )
        try:
            config = _make_config(mode="pi")
            result = get_override_column_set(tmp, config, "cat.sch.orders")
            assert result == {"ssn", "name"}
        finally:
            os.unlink(tmp)

    def test_domain_mode_returns_empty_set(self):
        tmp = self._write_csv("cat,sch,orders,cust_id,Customer ID,,")
        try:
            config = _make_config(mode="domain")
            result = get_override_column_set(tmp, config, "cat.sch.orders")
            assert result == set()
        finally:
            os.unlink(tmp)

    def test_filters_by_table_name(self):
        tmp = self._write_csv(
            "cat,sch,orders,cust_id,Customer ID,,",
            "cat,sch,products,name,Product name,,",
        )
        try:
            config = _make_config(mode="comment")
            result = get_override_column_set(tmp, config, "cat.sch.orders")
            assert result == {"cust_id"}
        finally:
            os.unlink(tmp)

    def test_wildcard_no_table_matches_all(self):
        """CSV rows with no table specified should match any table."""
        tmp = self._write_csv(",,,ssn,,pi,pii")
        try:
            config = _make_config(mode="pi")
            result = get_override_column_set(tmp, config, "cat.sch.orders")
            assert result == {"ssn"}
        finally:
            os.unlink(tmp)

    def test_csv_not_found_returns_empty(self):
        config = _make_config(mode="comment")
        result = get_override_column_set("/nonexistent/path.csv", config, "cat.sch.t")
        assert result == set()

    def test_none_path_returns_empty(self):
        config = _make_config(mode="comment")
        result = get_override_column_set(None, config, "cat.sch.t")
        assert result == set()

    def test_table_level_rows_ignored(self):
        """Rows with no column name should not be included."""
        tmp = self._write_csv("cat,sch,orders,,Table-level comment,,")
        try:
            config = _make_config(mode="comment")
            result = get_override_column_set(tmp, config, "cat.sch.orders")
            assert result == set()
        finally:
            os.unlink(tmp)


# ===========================================================================
# TestLoadOverrideDataForTable
# ===========================================================================

class TestLoadOverrideDataForTable:
    HEADER = "catalog,schema,table,column,comment,classification,type\n"

    def _write_csv(self, *data_lines):
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(self.HEADER)
        for line in data_lines:
            f.write(line + "\n")
        f.close()
        return f.name

    def setup_method(self):
        _csv_cache.clear()

    def test_comment_mode_returns_column_data(self):
        tmp = self._write_csv("cat,sch,orders,cust_id,Customer ID,,")
        try:
            config = _make_config(mode="comment")
            result = load_override_data_for_table(tmp, config, "cat.sch.orders")
            assert result == {"cust_id": {"comment": "Customer ID"}}
        finally:
            os.unlink(tmp)

    def test_pi_mode_returns_classification_and_type(self):
        tmp = self._write_csv("cat,sch,orders,ssn,,pi,pii")
        try:
            config = _make_config(mode="pi")
            result = load_override_data_for_table(tmp, config, "cat.sch.orders")
            assert result == {"ssn": {"classification": "pi", "type": "pii"}}
        finally:
            os.unlink(tmp)

    def test_pi_mode_missing_type_defaults_to_none(self):
        tmp = self._write_csv("cat,sch,orders,ssn,,pi,")
        try:
            config = _make_config(mode="pi")
            result = load_override_data_for_table(tmp, config, "cat.sch.orders")
            assert result == {"ssn": {"classification": "pi", "type": "None"}}
        finally:
            os.unlink(tmp)

    def test_domain_mode_returns_empty(self):
        tmp = self._write_csv("cat,sch,orders,cust_id,Customer ID,,")
        try:
            config = _make_config(mode="domain")
            result = load_override_data_for_table(tmp, config, "cat.sch.orders")
            assert result == {}
        finally:
            os.unlink(tmp)

    def test_filters_by_table(self):
        tmp = self._write_csv(
            "cat,sch,orders,cust_id,Customer ID,,",
            "cat,sch,products,name,Product name,,",
        )
        try:
            config = _make_config(mode="comment")
            result = load_override_data_for_table(tmp, config, "cat.sch.orders")
            assert "cust_id" in result
            assert "name" not in result
        finally:
            os.unlink(tmp)

    def test_csv_not_found_returns_empty(self):
        config = _make_config(mode="comment")
        result = load_override_data_for_table("/nonexistent.csv", config, "cat.sch.t")
        assert result == {}

    def test_blank_override_values_skipped(self):
        tmp = self._write_csv("cat,sch,orders,amount,,,")
        try:
            config = _make_config(mode="comment")
            result = load_override_data_for_table(tmp, config, "cat.sch.orders")
            assert result == {}
        finally:
            os.unlink(tmp)


# ===========================================================================
# TestPreLLMExclusionWiring
# ===========================================================================

class TestPreLLMExclusionWiring:
    """Verify the pre-LLM column exclusion and synthetic row injection
    in get_generated_metadata_data_aware and review_and_generate_metadata."""

    @classmethod
    def setup_class(cls):
        cls._saved = install_processing_stubs()
        import dbxmetagen.processing as pm
        cls._processing_mod = pm

    @classmethod
    def teardown_class(cls):
        uninstall_processing_stubs(cls._saved)

    def test_override_columns_excluded_from_llm_chunks(self):
        """Overridden columns should be dropped from the DataFrame before chunking."""
        pm = self._processing_mod
        mock_df = MagicMock()
        mock_df.columns = ["col_a", "col_b", "col_c"]
        mock_df.count.return_value = 10
        mock_df.select.return_value = mock_df

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
            sample_size=0,
        )

        with (
            patch.object(pm, "read_table_with_type_conversion", return_value=mock_df),
            patch.object(pm, "_is_metric_view", return_value=False),
            patch.object(pm, "get_override_column_set", return_value={"col_b"}),
            patch.object(pm, "chunk_df", return_value=[]) as mock_chunk,
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            SparkSession.builder.getOrCreate.return_value = mock_spark

            result = pm.get_generated_metadata_data_aware(mock_spark, config, "cat.sch.t")
            mock_df.select.assert_called_once_with(["col_a", "col_c"])
            assert result == []

    def test_all_columns_overridden_returns_empty(self):
        """When all columns have overrides, return empty list (0 LLM calls)."""
        pm = self._processing_mod
        mock_df = MagicMock()
        mock_df.columns = ["col_a", "col_b"]
        mock_df.count.return_value = 10

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
            sample_size=0,
        )

        with (
            patch.object(pm, "read_table_with_type_conversion", return_value=mock_df),
            patch.object(pm, "_is_metric_view", return_value=False),
            patch.object(pm, "get_override_column_set", return_value={"col_a", "col_b"}),
            patch.object(pm, "chunk_df") as mock_chunk,
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            SparkSession.builder.getOrCreate.return_value = mock_spark

            result = pm.get_generated_metadata_data_aware(mock_spark, config, "cat.sch.t")
            assert result == []
            mock_chunk.assert_not_called()

    def test_no_overrides_passes_all_columns(self):
        """When no overrides exist, the full DataFrame goes to chunk_df."""
        pm = self._processing_mod
        mock_df = MagicMock()
        mock_df.columns = ["col_a", "col_b"]
        mock_df.count.return_value = 10

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
            sample_size=0,
        )

        with (
            patch.object(pm, "read_table_with_type_conversion", return_value=mock_df),
            patch.object(pm, "_is_metric_view", return_value=False),
            patch.object(pm, "get_override_column_set", return_value=set()),
            patch.object(pm, "chunk_df", return_value=[]) as mock_chunk,
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.get_generated_metadata_data_aware(mock_spark, config, "cat.sch.t")
            mock_df.select.assert_not_called()
            mock_chunk.assert_called_once()

    @staticmethod
    def _make_field(field_name):
        """Create a mock schema field with a proper .name attribute."""
        f = MagicMock()
        f.name = field_name
        return f

    def test_synthetic_rows_injected_in_review(self):
        """review_and_generate_metadata should inject synthetic rows for overridden columns."""
        pm = self._processing_mod
        mock_column_df = MagicMock()
        mock_table_df = MagicMock()

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
        )

        override_data = {"cust_id": {"comment": "Customer ID"}}

        with (
            patch.object(pm, "get_generated_metadata", return_value=[]),
            patch.object(pm, "replace_catalog_name", return_value="tok.sch.t"),
            patch.object(pm, "load_override_data_for_table", return_value=override_data),
            patch.object(pm, "append_override_row", wraps=pm.append_override_row) as mock_append,
            patch.object(pm, "rows_to_df", side_effect=[mock_column_df, mock_table_df]),
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            mock_schema = MagicMock()
            mock_schema.fields = [self._make_field("cust_id")]
            mock_spark.table.return_value.schema = mock_schema
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.review_and_generate_metadata(config, "cat.sch.t")
            mock_append.assert_called_once()
            call_args = mock_append.call_args
            assert call_args.args[3] == "cust_id"  # col_name
            assert call_args.args[4] == {"comment": "Customer ID"}  # values

    def test_mixed_override_and_llm_columns(self):
        """10 cols, 3 overridden -> LLM sees 7, output has synthetic rows for 3."""
        pm = self._processing_mod

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
        )

        # Simulate 7 LLM-generated rows via a fake append_column_rows
        def fake_append_column_rows(cfg, rows, ftn, resp, tok):
            from pyspark.sql import Row
            for i in range(7):
                rows.append(Row(table=ftn, tokenized_table=tok, ddl_type="column",
                                column_name=f"col_{i}", column_content=f"content_{i}"))
            return rows

        override_data = {
            "col_7": {"comment": "Override 7"},
            "col_8": {"comment": "Override 8"},
            "col_9": {"comment": "Override 9"},
        }

        mock_column_df = MagicMock()
        mock_table_df = MagicMock()

        with (
            patch.object(pm, "get_generated_metadata", return_value=[MagicMock()]),
            patch.object(pm, "replace_catalog_name", return_value="tok.sch.t"),
            patch.object(pm, "append_table_row", side_effect=lambda r, *a: r),
            patch.object(pm, "append_column_rows", side_effect=fake_append_column_rows),
            patch.object(pm, "load_override_data_for_table", return_value=override_data),
            patch.object(pm, "rows_to_df", side_effect=[mock_column_df, mock_table_df]),
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            mock_schema = MagicMock()
            mock_schema.fields = [self._make_field(f"col_{i}") for i in range(10)]
            mock_spark.table.return_value.schema = mock_schema
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.review_and_generate_metadata(config, "cat.sch.t")
            column_rows = pm.rows_to_df.call_args_list[0].args[0]
            assert len(column_rows) == 10  # 7 from LLM + 3 synthetic

    def test_override_disabled_no_exclusion(self):
        """When allow_manual_override is False, no exclusion or injection happens."""
        pm = self._processing_mod
        mock_df = MagicMock()
        mock_df.columns = ["col_a", "col_b"]
        mock_df.count.return_value = 10

        config = _make_config(
            mode="comment",
            allow_manual_override=False,
            override_csv_path="overrides.csv",
            sample_size=0,
        )

        with (
            patch.object(pm, "read_table_with_type_conversion", return_value=mock_df),
            patch.object(pm, "_is_metric_view", return_value=False),
            patch.object(pm, "get_override_column_set") as mock_get_cols,
            patch.object(pm, "chunk_df", return_value=[]),
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.get_generated_metadata_data_aware(mock_spark, config, "cat.sch.t")
            mock_get_cols.assert_not_called()

    def test_pi_mode_exclusion_and_synthetic_rows(self):
        """PI mode: overridden columns excluded from LLM, synthetic rows injected."""
        pm = self._processing_mod

        config = _make_config(
            mode="pi",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
        )

        def fake_append_column_rows(cfg, rows, ftn, resp, tok):
            from pyspark.sql import Row
            for i in range(3):
                rows.append(Row(table=ftn, tokenized_table=tok, ddl_type="column",
                                column_name=f"col_{i}", classification="None",
                                type="None", confidence=0.9, presidio_results=None))
            return rows

        override_data = {
            "ssn": {"classification": "pi", "type": "pii"},
            "dob": {"classification": "pi", "type": "None"},
        }

        mock_column_df = MagicMock()
        mock_table_df = MagicMock()

        with (
            patch.object(pm, "get_generated_metadata", return_value=[MagicMock()]),
            patch.object(pm, "replace_catalog_name", return_value="tok.sch.t"),
            patch.object(pm, "append_column_rows", side_effect=fake_append_column_rows),
            patch.object(pm, "load_override_data_for_table", return_value=override_data),
            patch.object(pm, "append_override_row", wraps=pm.append_override_row) as mock_append,
            patch.object(pm, "rows_to_df", side_effect=[mock_column_df, mock_table_df]),
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            mock_schema = MagicMock()
            mock_schema.fields = [
                self._make_field("col_0"), self._make_field("col_1"),
                self._make_field("col_2"), self._make_field("ssn"),
                self._make_field("dob"),
            ]
            mock_spark.table.return_value.schema = mock_schema
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.review_and_generate_metadata(config, "cat.sch.t")
            column_rows = pm.rows_to_df.call_args_list[0].args[0]
            assert len(column_rows) == 5  # 3 LLM + 2 synthetic

            # Verify append_override_row was called with correct PI data
            assert mock_append.call_count == 2
            call_cols = {c.args[3] for c in mock_append.call_args_list}
            assert call_cols == {"ssn", "dob"}
            ssn_call = next(c for c in mock_append.call_args_list if c.args[3] == "ssn")
            assert ssn_call.args[4] == {"classification": "pi", "type": "pii"}

    def test_case_insensitive_column_exclusion(self):
        """CSV has 'customer_id' but source table has 'Customer_ID' -- still excluded."""
        pm = self._processing_mod
        mock_df = MagicMock()
        mock_df.columns = ["Customer_ID", "Amount", "Order_Date"]
        mock_df.count.return_value = 10
        mock_df.select.return_value = mock_df

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
            sample_size=0,
        )

        with (
            patch.object(pm, "read_table_with_type_conversion", return_value=mock_df),
            patch.object(pm, "_is_metric_view", return_value=False),
            patch.object(pm, "get_override_column_set", return_value={"customer_id"}),
            patch.object(pm, "chunk_df", return_value=[]),
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.get_generated_metadata_data_aware(mock_spark, config, "cat.sch.t")
            mock_df.select.assert_called_once_with(["Amount", "Order_Date"])

    def test_case_insensitive_source_validation(self):
        """Override col 'customer_id' should match source schema 'Customer_ID'."""
        pm = self._processing_mod
        mock_column_df = MagicMock()
        mock_table_df = MagicMock()

        config = _make_config(
            mode="comment",
            allow_manual_override=True,
            override_csv_path="overrides.csv",
        )

        override_data = {"customer_id": {"comment": "The customer identifier"}}

        with (
            patch.object(pm, "get_generated_metadata", return_value=[]),
            patch.object(pm, "replace_catalog_name", return_value="tok.sch.t"),
            patch.object(pm, "load_override_data_for_table", return_value=override_data),
            patch.object(pm, "append_override_row", wraps=pm.append_override_row) as mock_append,
            patch.object(pm, "rows_to_df", side_effect=[mock_column_df, mock_table_df]),
        ):
            from pyspark.sql import SparkSession
            mock_spark = MagicMock()
            mock_schema = MagicMock()
            mock_schema.fields = [self._make_field("Customer_ID")]
            mock_spark.table.return_value.schema = mock_schema
            SparkSession.builder.getOrCreate.return_value = mock_spark

            pm.review_and_generate_metadata(config, "cat.sch.t")
            mock_append.assert_called_once()


# ===========================================================================
# TestAppendOverrideRow
# ===========================================================================

class _FakeRow:
    """Lightweight Row substitute that stores kwargs as real attributes."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestAppendOverrideRow:
    """Direct validation of append_override_row Row field names and values."""

    @classmethod
    def setup_class(cls):
        cls._saved = install_processing_stubs()
        import dbxmetagen.processing as pm
        cls._processing_mod = pm

    @classmethod
    def teardown_class(cls):
        uninstall_processing_stubs(cls._saved)

    def _run(self, mode, col_name, values):
        """Call append_override_row with a real Row substitute."""
        pm = self._processing_mod
        config = _make_config(mode=mode)
        rows = []
        with patch.object(pm, "Row", _FakeRow):
            result = pm.append_override_row(
                rows, "cat.sch.t", "tok.sch.t", col_name, values, config,
            )
        return result

    def test_comment_mode_row_schema(self):
        result = self._run("comment", "my_col", {"comment": "My description"})
        assert len(result) == 1
        row = result[0]
        assert row.table == "cat.sch.t"
        assert row.tokenized_table == "tok.sch.t"
        assert row.ddl_type == "column"
        assert row.column_name == "my_col"
        assert row.column_content == "My description"

    def test_pi_mode_row_schema(self):
        result = self._run("pi", "ssn", {"classification": "pi", "type": "pii"})
        assert len(result) == 1
        row = result[0]
        assert row.table == "cat.sch.t"
        assert row.tokenized_table == "tok.sch.t"
        assert row.ddl_type == "column"
        assert row.column_name == "ssn"
        assert row.classification == "pi"
        assert row.type == "pii"
        assert row.confidence == 1.0
        assert row.presidio_results is None

    def test_pi_mode_defaults_missing_fields(self):
        result = self._run("pi", "col_x", {})
        assert len(result) == 1
        row = result[0]
        assert row.classification == "None"
        assert row.type == "None"

    def test_domain_mode_is_noop(self):
        result = self._run("domain", "col_x", {"comment": "ignored"})
        assert len(result) == 0

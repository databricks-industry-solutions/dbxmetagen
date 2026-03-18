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
    build_condition,
    apply_overrides_with_loop,
    override_metadata_from_csv,
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


def _mock_df():
    """Return a MagicMock DataFrame whose withColumn returns itself."""
    df = MagicMock()
    df.withColumn.return_value = df
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

    def test_empty_string_is_not_blank(self):
        assert _is_blank("") is False

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
    """Tests for build_condition pattern matching.

    Since PySpark col/reduce are mocked, we verify the function doesn't raise
    for valid patterns and does raise for invalid ones.
    """

    def test_pattern1_only_column(self):
        df = _mock_df()
        result = build_condition(df, None, "ssn", None, None)
        assert result is not None

    def test_pattern1_empty_strings_treated_as_none(self):
        df = _mock_df()
        result = build_condition(df, "", "ssn", "", "")
        assert result is not None

    def test_pattern2_all_params(self):
        df = _mock_df()
        result = build_condition(df, "my_table", "my_col", "my_schema", "my_catalog")
        assert result is not None

    def test_pattern3_table_level(self):
        df = _mock_df()
        result = build_condition(df, "my_table", None, "my_schema", "my_catalog")
        assert result is not None

    def test_raises_for_partial_params_table_no_catalog(self):
        df = _mock_df()
        with pytest.raises(ValueError, match="Unsupported parameter combination"):
            build_condition(df, "my_table", "my_col", None, None)

    def test_raises_for_partial_params_schema_no_catalog(self):
        df = _mock_df()
        with pytest.raises(ValueError, match="Unsupported parameter combination"):
            build_condition(df, None, "my_col", "my_schema", None)

    def test_error_includes_param_values(self):
        df = _mock_df()
        with pytest.raises(ValueError, match="table='my_table'"):
            build_condition(df, "my_table", "my_col", None, None)

    def test_raises_for_no_params_at_all(self):
        df = _mock_df()
        with pytest.raises(ValueError, match="Unsupported parameter combination"):
            build_condition(df, None, None, None, None)


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

    def test_override_called_on_both_dfs_comment_mode(self):
        pm = self._processing_mod
        column_df = _mock_df()
        table_df = _mock_df()
        config = self._make_full_config(mode="comment")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(column_df, table_df)),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")

            assert mock_ov.call_count == 2
            assert mock_ov.call_args_list[0].args[0] is column_df
            assert mock_ov.call_args_list[1].args[0] is table_df

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
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")
            assert mock_ov.call_count == 1
            assert mock_ov.call_args_list[0].args[0] is not None

    def test_override_passes_correct_csv_path(self):
        pm = self._processing_mod
        config = self._make_full_config(csv_path="/custom/overrides.csv")

        with (
            patch.object(pm, "review_and_generate_metadata", return_value=(_mock_df(), _mock_df())),
            patch.object(pm, "split_and_hardcode_df", side_effect=lambda df, cfg: df),
            patch.object(pm, "override_metadata_from_csv", side_effect=lambda df, p, c: df) as mock_ov,
            patch.object(pm, "add_ddl_to_dfs", return_value={"r": MagicMock()}),
        ):
            pm.process_and_add_ddl(config, "cat.sch.my_table")
            for c in mock_ov.call_args_list:
                assert c.args[1] == "/custom/overrides.csv"

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
            assert result == {}

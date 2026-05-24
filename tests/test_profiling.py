"""Unit tests for profiling module."""

import pytest
import json
from unittest.mock import MagicMock, patch, call
from pyspark.sql import SparkSession

from dbxmetagen.profiling import (
    ProfilingConfig,
    ProfilingBuilder,
    run_profiling
)


# Since pyspark.sql.types is mocked globally by conftest, we need real-ish
# stand-ins for schema iteration and isinstance checks in profiling.py.
class _NumericType:
    pass

class _IntType(_NumericType):
    pass

class _DoubleType(_NumericType):
    pass

class _StringType:
    pass

class _BoolType:
    pass

class _TimestampType:
    pass

class _ArrayType:
    pass

class _MapType:
    pass

class _StructType:
    pass


# All type patches needed for profiling methods that use isinstance
_TYPE_PATCHES = {
    "dbxmetagen.profiling.NumericType": _NumericType,
    "dbxmetagen.profiling.SparkStringType": _StringType,
    "dbxmetagen.profiling.SparkBooleanType": _BoolType,
    "dbxmetagen.profiling.DateType": type("_DateType", (), {}),
    "dbxmetagen.profiling.SparkTimestampType": _TimestampType,
    "dbxmetagen.profiling.SparkArrayType": _ArrayType,
    "dbxmetagen.profiling.SparkMapType": _MapType,
    "dbxmetagen.profiling.StructType": _StructType,
}


def _patch_types(fn):
    """Apply all type patches for profiling isinstance checks."""
    for target, replacement in reversed(list(_TYPE_PATCHES.items())):
        fn = patch(target, replacement)(fn)
    return fn


class _MockField:
    def __init__(self, name, dataType):
        self.name = name
        self.dataType = dataType


class _MockSchema:
    def __init__(self, fields):
        self.fields = fields


def _make_schema(col_defs):
    """Create a mock schema from a list of (name, type_instance) tuples."""
    return _MockSchema([_MockField(n, t) for n, t in col_defs])


class TestProfilingConfig:
    """Tests for ProfilingConfig."""

    def test_fully_qualified_snapshots(self):
        config = ProfilingConfig(catalog_name="test_catalog", schema_name="test_schema")
        assert config.fully_qualified_snapshots == "test_catalog.test_schema.profiling_snapshots"

    def test_fully_qualified_column_stats(self):
        config = ProfilingConfig(catalog_name="test_catalog", schema_name="test_schema")
        assert config.fully_qualified_column_stats == "test_catalog.test_schema.column_profiling_stats"

    def test_custom_table_names(self):
        config = ProfilingConfig(
            catalog_name="cat", schema_name="sch",
            snapshots_table="custom_snapshots", column_stats_table="custom_stats"
        )
        assert config.fully_qualified_snapshots == "cat.sch.custom_snapshots"
        assert config.fully_qualified_column_stats == "cat.sch.custom_stats"


class TestProfilingBuilder:
    """Tests for ProfilingBuilder."""

    @pytest.fixture
    def mock_spark(self):
        return MagicMock()

    @pytest.fixture
    def config(self):
        return ProfilingConfig(catalog_name="test_catalog", schema_name="test_schema")

    @pytest.fixture
    def builder(self, mock_spark, config):
        return ProfilingBuilder(mock_spark, config)

    def test_create_snapshots_table(self, builder, mock_spark):
        builder.create_snapshots_table()
        assert mock_spark.sql.call_count >= 1
        ddl = mock_spark.sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "profiling_snapshots" in ddl
        assert "row_count" in ddl

    def test_create_column_stats_table(self, builder, mock_spark):
        builder.create_column_stats_table()
        mock_spark.sql.assert_called()
        call_arg = mock_spark.sql.call_args_list[0][0][0]
        assert "column_profiling_stats" in call_arg
        assert "null_rate" in call_arg
        assert "distinct_count" in call_arg

    def test_column_stats_ddl_includes_new_metrics(self, builder, mock_spark):
        builder.create_column_stats_table()
        call_arg = mock_spark.sql.call_args_list[0][0][0]
        assert "cardinality_ratio" in call_arg
        assert "empty_string_count" in call_arg
        assert "empty_string_rate" in call_arg

    def test_snapshot_schema_has_required_fields(self, builder, mock_spark):
        builder.create_snapshots_table()
        ddl = mock_spark.sql.call_args_list[0][0][0]
        for col_name in ["snapshot_id", "table_name", "row_count", "table_size_bytes", "num_files", "column_stats"]:
            assert col_name in ddl, f"Missing column {col_name} in snapshots DDL"

    def test_column_stats_schema_has_required_fields(self, builder, mock_spark):
        builder.create_column_stats_table()
        ddl = mock_spark.sql.call_args_list[0][0][0]
        for col_name in ["stat_id", "column_name", "null_count", "null_rate",
                         "distinct_count", "cardinality_ratio", "percentiles",
                         "empty_string_count", "empty_string_rate"]:
            assert col_name in ddl, f"Missing column {col_name} in column_stats DDL"

    def test_column_stats_schema_has_new_universal_fields(self, builder, mock_spark):
        builder.create_column_stats_table()
        ddl = mock_spark.sql.call_args_list[0][0][0]
        for col_name in ["data_type", "sample_values", "mode_value", "mode_frequency",
                         "entropy", "is_unique_candidate", "value_distribution",
                         "pattern_detected", "has_numeric_stats", "has_string_stats"]:
            assert col_name in ddl, f"Missing column {col_name} in column_stats DDL"

    def test_uuid_pattern_detection(self, builder):
        assert builder.UUID_PATTERN.match("123e4567-e89b-12d3-a456-426614174000")
        assert not builder.UUID_PATTERN.match("not-a-uuid")

    def test_email_pattern_detection(self, builder):
        assert builder.EMAIL_PATTERN.match("test@example.com")
        assert not builder.EMAIL_PATTERN.match("not-an-email")

    def test_date_pattern_detection(self, builder):
        assert builder.DATE_PATTERN.match("2024-01-15")
        assert builder.DATE_PATTERN.match("2024/01/15")
        assert not builder.DATE_PATTERN.match("January 15, 2024")


class TestDeltaSinglePass:
    """Tests for _profile_table_delta single-pass SQL generation."""

    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        schema = _make_schema([
            ("id", _IntType()),
            ("name", _StringType()),
            ("amount", _DoubleType()),
        ])
        mock_df = MagicMock()
        mock_df.schema = schema
        spark.table.return_value = mock_df
        return spark

    @pytest.fixture
    def config(self):
        return ProfilingConfig(catalog_name="cat", schema_name="sch")

    @pytest.fixture
    def builder(self, mock_spark, config):
        return ProfilingBuilder(mock_spark, config)

    def _setup_sql_responses(self, mock_spark, row_count=1000):
        """Set up mock SQL responses for the delta profiling calls."""
        agg_row = MagicMock()
        agg_data = {
            "_row_count": row_count,
            "id__non_null": 1000, "id__null_count": 0, "id__distinct": 950,
            "id__min": "1", "id__max": "1000", "id__mean": 500.0, "id__stddev": 288.0,
            "id__pcts": [250.0, 500.0, 750.0, 950.0, 990.0],
            "name__non_null": 980, "name__null_count": 20, "name__distinct": 500,
            "name__min": "Alice", "name__max": "Zoe",
            "name__min_len": 3, "name__max_len": 20, "name__avg_len": 8.5, "name__empty": 5,
            "amount__non_null": 990, "amount__null_count": 10, "amount__distinct": 800,
            "amount__min": "0.5", "amount__max": "9999.99", "amount__mean": 150.5,
            "amount__stddev": 42.3,
            "amount__pcts": [25.0, 75.0, 200.0, 500.0, 900.0],
        }
        agg_row.__getitem__ = lambda self, k: agg_data.get(k)
        agg_row.get = lambda k, d=None: agg_data.get(k, d)

        sample_row = MagicMock()
        sample_data = {"id": "1", "name": "Alice", "amount": "100.5"}
        sample_row.__getitem__ = lambda self, k: sample_data.get(k)

        detail_row = MagicMock()
        detail_row.sizeInBytes = 1024000
        detail_row.numFiles = 4
        detail_row.lastModified = None

        mode_row1 = MagicMock()
        mode_row1.__getitem__ = lambda self, k: {"col_name": "name", "val": "Alice", "cnt": 50}.get(k)
        mode_row2 = MagicMock()
        mode_row2.__getitem__ = lambda self, k: {"col_name": "name", "val": "Bob", "cnt": 30}.get(k)

        def sql_side_effect(query):
            result = MagicMock()
            q = query.strip()
            if "_row_count" in q and "COUNT(*)" in q:
                result.collect.return_value = [agg_row]
            elif "LIMIT 100" in q and "CAST" in q:
                result.collect.return_value = [sample_row]
            elif "DESCRIBE DETAIL" in q:
                result.collect.return_value = [detail_row]
            elif "UNION ALL" in q:
                result.collect.return_value = [mode_row1, mode_row2]
            elif "ROW_NUMBER" in q:
                result.collect.return_value = []
            else:
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect

    @_patch_types
    def test_delta_generates_single_pass_sql(self, builder, mock_spark):
        """Delta path should generate one SQL with all column aggregates."""
        self._setup_sql_responses(mock_spark)

        result = builder._profile_table_delta("cat.sch.my_table")

        assert result is not None
        assert result["row_count"] == 1000
        assert result["column_count"] == 3
        assert len(result["column_stat_records"]) == 3

        first_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "COUNT(*) AS `_row_count`" in first_sql
        assert "APPROX_COUNT_DISTINCT" in first_sql
        assert "PERCENTILE_APPROX" in first_sql
        assert "AVG(" in first_sql
        assert "STDDEV_SAMP(" in first_sql

    @_patch_types
    def test_delta_numeric_stats_populated(self, builder, mock_spark):
        """Numeric columns should have mean, stddev, percentiles from single pass."""
        self._setup_sql_responses(mock_spark)

        result = builder._profile_table_delta("cat.sch.my_table")
        id_rec = next(r for r in result["column_stat_records"] if r["column_name"] == "id")

        assert id_rec["has_numeric_stats"] is True
        assert id_rec["mean_value"] == 500.0
        assert id_rec["stddev_value"] == 288.0
        assert id_rec["percentiles"]["p50"] == 500.0

    @_patch_types
    def test_delta_string_stats_populated(self, builder, mock_spark):
        """String columns should have length stats and pattern detection."""
        self._setup_sql_responses(mock_spark)

        result = builder._profile_table_delta("cat.sch.my_table")
        name_rec = next(r for r in result["column_stat_records"] if r["column_name"] == "name")

        assert name_rec["has_string_stats"] is True
        assert name_rec["min_length"] == 3
        assert name_rec["max_length"] == 20
        assert name_rec["avg_length"] == 8.5
        assert name_rec["empty_string_count"] == 5

    @_patch_types
    def test_delta_mode_frequency_batched(self, builder, mock_spark):
        """Mode/frequency should use a batched UNION ALL query."""
        self._setup_sql_responses(mock_spark)

        builder._profile_table_delta("cat.sch.my_table")

        union_calls = [
            c for c in mock_spark.sql.call_args_list
            if "UNION ALL" in c[0][0]
        ]
        assert len(union_calls) == 1
        assert "GROUP BY" in union_calls[0][0][0]

    @_patch_types
    def test_delta_sample_single_limit_query(self, builder, mock_spark):
        """Sample values should come from a single LIMIT 100 query."""
        self._setup_sql_responses(mock_spark)

        builder._profile_table_delta("cat.sch.my_table")

        limit_calls = [
            c for c in mock_spark.sql.call_args_list
            if "LIMIT 100" in c[0][0] and "CAST" in c[0][0]
        ]
        assert len(limit_calls) == 1


class TestFederatedSinglePass:
    """Tests for _profile_table_federated pushdown-safe SQL."""

    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        schema = _make_schema([
            ("patient_id", _IntType()),
            ("diagnosis", _StringType()),
        ])
        mock_df = MagicMock()
        mock_df.schema = schema
        spark.table.return_value = mock_df
        return spark

    @pytest.fixture
    def config(self):
        return ProfilingConfig(catalog_name="fed_cat", schema_name="fed_sch")

    @pytest.fixture
    def builder(self, mock_spark, config):
        return ProfilingBuilder(mock_spark, config)

    def _setup_federated_responses(self, mock_spark, row_count=5000):
        agg_data = {
            "_row_count": row_count,
            "patient_id__non_null": 5000,
            "patient_id__min": "1", "patient_id__max": "5000",
            "patient_id__mean": 2500.0,
            "diagnosis__non_null": 4800,
            "diagnosis__min": "A01", "diagnosis__max": "Z99",
        }
        agg_row = MagicMock()
        agg_row.__getitem__ = lambda self, k: agg_data.get(k)
        agg_row.get = lambda k, d=None: agg_data.get(k, d)

        sample_data = {"patient_id": "42", "diagnosis": "A01.1"}
        sample_row = MagicMock()
        sample_row.__getitem__ = lambda self, k: sample_data.get(k)

        def sql_side_effect(query):
            result = MagicMock()
            q = query.strip()
            if "_row_count" in q:
                result.collect.return_value = [agg_row]
            elif "LIMIT 100" in q:
                result.collect.return_value = [sample_row]
            else:
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect

    @_patch_types
    def test_federated_skips_approx_count_distinct(self, builder, mock_spark):
        """Federated path should NOT use APPROX_COUNT_DISTINCT."""
        self._setup_federated_responses(mock_spark)

        builder._profile_table_federated("fed_cat.fed_sch.patients")

        first_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "APPROX_COUNT_DISTINCT" not in first_sql

    @_patch_types
    def test_federated_skips_percentile_approx(self, builder, mock_spark):
        """Federated path should NOT use PERCENTILE_APPROX."""
        self._setup_federated_responses(mock_spark)

        builder._profile_table_federated("fed_cat.fed_sch.patients")

        first_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "PERCENTILE_APPROX" not in first_sql

    @_patch_types
    def test_federated_skips_mode_frequency(self, builder, mock_spark):
        """Federated path should NOT run GROUP BY mode/frequency query."""
        self._setup_federated_responses(mock_spark)

        builder._profile_table_federated("fed_cat.fed_sch.patients")

        all_sqls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert not any("UNION ALL" in s for s in all_sqls)
        assert not any("GROUP BY" in s for s in all_sqls)

    @_patch_types
    def test_federated_uses_only_guaranteed_pushdown_aggregates(self, builder, mock_spark):
        """Federated path should only use bare-column COUNT, MIN, MAX, AVG."""
        self._setup_federated_responses(mock_spark)

        builder._profile_table_federated("fed_cat.fed_sch.patients")

        first_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "COUNT(*)" in first_sql
        assert "COUNT(" in first_sql
        assert "MIN(" in first_sql
        assert "MAX(" in first_sql
        assert "AVG(" in first_sql
        # These must NOT be in the pushed SQL (not guaranteed pushdown)
        assert "STDDEV_SAMP(" not in first_sql
        assert "LENGTH(" not in first_sql

    @_patch_types
    def test_federated_computes_null_from_subtraction(self, builder, mock_spark):
        """Federated path derives null_count = row_count - non_null."""
        self._setup_federated_responses(mock_spark)

        result = builder._profile_table_federated("fed_cat.fed_sch.patients")

        diag_rec = next(r for r in result["column_stat_records"] if r["column_name"] == "diagnosis")
        assert diag_rec["null_count"] == 200  # 5000 - 4800

    @_patch_types
    def test_federated_no_describe_detail(self, builder, mock_spark):
        """Federated path should not call DESCRIBE DETAIL."""
        self._setup_federated_responses(mock_spark)

        result = builder._profile_table_federated("fed_cat.fed_sch.patients")

        all_sqls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert not any("DESCRIBE DETAIL" in s for s in all_sqls)
        assert result["table_size_bytes"] is None
        assert result["num_files"] is None

    @_patch_types
    def test_federated_distinct_count_is_zero(self, builder, mock_spark):
        """Federated path should report distinct_count=0 (skipped)."""
        self._setup_federated_responses(mock_spark)

        result = builder._profile_table_federated("fed_cat.fed_sch.patients")

        for rec in result["column_stat_records"]:
            assert rec["distinct_count"] == 0


class TestProfileTableDispatch:
    """Tests for profile_table routing logic."""

    @pytest.fixture
    def builder(self):
        spark = MagicMock()
        config = ProfilingConfig(catalog_name="c", schema_name="s")
        return ProfilingBuilder(spark, config)

    def test_dispatch_delta_by_default(self, builder):
        with patch.object(builder, "_profile_table_delta", return_value={"test": True}) as m:
            result = builder.profile_table("t")
            m.assert_called_once_with("t", {})
            assert result == {"test": True}

    def test_dispatch_federated_when_flag_set(self, builder):
        with patch.object(builder, "_profile_table_federated", return_value={"fed": True}) as m:
            result = builder.profile_table("t", federation_mode=True)
            m.assert_called_once_with("t", {})
            assert result == {"fed": True}

    def test_dispatch_raises_on_error(self, builder):
        with patch.object(builder, "_profile_table_delta", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                builder.profile_table("t")


class TestParallelExecution:
    """Tests for ThreadPoolExecutor-based parallel run."""

    @patch('dbxmetagen.profiling.ProfilingBuilder.write_snapshot')
    @patch('dbxmetagen.profiling.ProfilingBuilder.profile_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder._load_all_drift_baselines')
    @patch('dbxmetagen.profiling.ProfilingBuilder.get_tables_to_profile')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_column_stats_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_snapshots_table')
    def test_run_uses_threadpool(self, mock_create_snap, mock_create_col,
                                 mock_get_tables, mock_load_drift, mock_profile, mock_write):
        mock_get_tables.return_value = ["t1", "t2", "t3"]
        mock_load_drift.return_value = {}
        mock_profile.return_value = {"snapshot_id": "x", "table_name": "t", "column_stat_records": []}

        spark = MagicMock()
        config = ProfilingConfig(catalog_name="c", schema_name="s")
        builder = ProfilingBuilder(spark, config)
        result = builder.run(federation_mode=False)

        assert result["tables_profiled"] == 3
        assert result["tables_failed"] == 0
        assert mock_profile.call_count == 3
        assert mock_write.call_count == 3

    @patch('dbxmetagen.profiling.ProfilingBuilder.write_snapshot')
    @patch('dbxmetagen.profiling.ProfilingBuilder.profile_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder._load_all_drift_baselines')
    @patch('dbxmetagen.profiling.ProfilingBuilder.get_tables_to_profile')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_column_stats_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_snapshots_table')
    def test_run_federation_mode_passes_flag(self, mock_create_snap, mock_create_col,
                                             mock_get_tables, mock_load_drift,
                                             mock_profile, mock_write):
        mock_get_tables.return_value = ["t1"]
        mock_load_drift.return_value = {}
        mock_profile.return_value = {"snapshot_id": "x", "table_name": "t1", "column_stat_records": []}

        spark = MagicMock()
        config = ProfilingConfig(catalog_name="c", schema_name="s")
        builder = ProfilingBuilder(spark, config)
        builder.run(federation_mode=True)

        mock_profile.assert_called_once_with("t1", True, {})

    @patch('dbxmetagen.profiling.ProfilingBuilder.write_snapshot')
    @patch('dbxmetagen.profiling.ProfilingBuilder.profile_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder._load_all_drift_baselines')
    @patch('dbxmetagen.profiling.ProfilingBuilder.get_tables_to_profile')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_column_stats_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_snapshots_table')
    def test_run_raises_on_failures(self, mock_create_snap, mock_create_col,
                                    mock_get_tables, mock_load_drift, mock_profile, mock_write):
        mock_get_tables.return_value = ["t1", "t2"]
        mock_load_drift.return_value = {}
        mock_profile.side_effect = [
            {"snapshot_id": "x", "table_name": "t1", "column_stat_records": []},
            RuntimeError("t2 broke"),
        ]

        spark = MagicMock()
        config = ProfilingConfig(catalog_name="c", schema_name="s")
        builder = ProfilingBuilder(spark, config)
        with pytest.raises(RuntimeError, match="1/2 tables"):
            builder.run()

    @patch('dbxmetagen.profiling.ProfilingBuilder.write_snapshot')
    @patch('dbxmetagen.profiling.ProfilingBuilder.profile_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder._load_all_drift_baselines')
    @patch('dbxmetagen.profiling.ProfilingBuilder.get_tables_to_profile')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_column_stats_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_snapshots_table')
    def test_run_tolerates_failures_when_raise_off(self, mock_create_snap, mock_create_col,
                                                   mock_get_tables, mock_load_drift, mock_profile, mock_write):
        mock_get_tables.return_value = ["t1", "t2"]
        mock_load_drift.return_value = {}
        mock_profile.side_effect = [
            {"snapshot_id": "x", "table_name": "t1", "column_stat_records": []},
            RuntimeError("t2 broke"),
        ]

        spark = MagicMock()
        config = ProfilingConfig(catalog_name="c", schema_name="s")
        builder = ProfilingBuilder(spark, config)
        result = builder.run(raise_on_error=False)

        assert result["tables_profiled"] == 1
        assert result["tables_failed"] == 1

    @patch('dbxmetagen.profiling.ProfilingBuilder.write_snapshot')
    @patch('dbxmetagen.profiling.ProfilingBuilder.profile_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder.get_tables_to_profile')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_column_stats_table')
    @patch('dbxmetagen.profiling.ProfilingBuilder.create_snapshots_table')
    def test_run_empty_table_list(self, mock_create_snap, mock_create_col,
                                  mock_get_tables, mock_profile, mock_write):
        mock_get_tables.return_value = []

        spark = MagicMock()
        config = ProfilingConfig(catalog_name="c", schema_name="s")
        builder = ProfilingBuilder(spark, config)
        result = builder.run()

        assert result["tables_profiled"] == 0
        assert result["total_tables"] == 0
        mock_profile.assert_not_called()


class TestRunProfiling:
    """Tests for run_profiling function."""

    @patch('dbxmetagen.profiling.ProfilingBuilder')
    def test_creates_builder_with_correct_config(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"tables_profiled": 5, "tables_failed": 0}
        mock_builder_class.return_value = mock_builder

        mock_spark = MagicMock()
        run_profiling(mock_spark, "my_cat", "my_sch")

        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"

    @patch('dbxmetagen.profiling.ProfilingBuilder')
    def test_passes_max_tables_to_run(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"tables_profiled": 10}
        mock_builder_class.return_value = mock_builder

        run_profiling(MagicMock(), "cat", "sch", max_tables=10)
        mock_builder.run.assert_called_once_with(10, federation_mode=False, raise_on_error=True)

    @patch('dbxmetagen.profiling.ProfilingBuilder')
    def test_returns_run_result(self, mock_builder_class):
        expected = {"tables_profiled": 5, "tables_failed": 1, "total_tables": 6}
        mock_builder = MagicMock()
        mock_builder.run.return_value = expected
        mock_builder_class.return_value = mock_builder

        result = run_profiling(MagicMock(), "cat", "sch")
        assert result == expected

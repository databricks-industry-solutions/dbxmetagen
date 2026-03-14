"""Unit tests for profiling module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from dbxmetagen.profiling import (
    ProfilingConfig,
    ProfilingBuilder,
    run_profiling
)


class TestProfilingConfig:
    """Tests for ProfilingConfig."""
    
    def test_fully_qualified_snapshots(self):
        config = ProfilingConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_snapshots == "test_catalog.test_schema.profiling_snapshots"
    
    def test_fully_qualified_column_stats(self):
        config = ProfilingConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_column_stats == "test_catalog.test_schema.column_profiling_stats"
    
    def test_custom_table_names(self):
        config = ProfilingConfig(
            catalog_name="cat",
            schema_name="sch",
            snapshots_table="custom_snapshots",
            column_stats_table="custom_stats"
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
        return ProfilingConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        return ProfilingBuilder(mock_spark, config)
    
    def test_create_snapshots_table(self, builder, mock_spark):
        builder.create_snapshots_table()
        mock_spark.sql.assert_called_once()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_arg
        assert "profiling_snapshots" in call_arg
        assert "row_count" in call_arg
    
    def test_create_column_stats_table(self, builder, mock_spark):
        builder.create_column_stats_table()
        mock_spark.sql.assert_called()
        call_arg = mock_spark.sql.call_args_list[0][0][0]
        assert "column_profiling_stats" in call_arg
        assert "null_rate" in call_arg
        assert "distinct_count" in call_arg
    
    def test_column_stats_ddl_includes_new_metrics(self, builder, mock_spark):
        """DDL should include cardinality_ratio and empty_string metrics."""
        builder.create_column_stats_table()
        call_arg = mock_spark.sql.call_args_list[0][0][0]
        assert "cardinality_ratio" in call_arg
        assert "empty_string_count" in call_arg
        assert "empty_string_rate" in call_arg
    
    def test_snapshot_schema_has_required_fields(self, builder):
        """Snapshot schema should have all required fields."""
        field_names = [f.name for f in builder.SNAPSHOT_SCHEMA.fields]
        assert "snapshot_id" in field_names
        assert "table_name" in field_names
        assert "row_count" in field_names
        assert "table_size_bytes" in field_names
        assert "num_files" in field_names
        assert "column_stats" in field_names
    
    def test_column_stats_schema_has_required_fields(self, builder):
        """Column stats schema should have all required fields."""
        field_names = [f.name for f in builder.COLUMN_STATS_SCHEMA.fields]
        assert "stat_id" in field_names
        assert "column_name" in field_names
        assert "null_count" in field_names
        assert "null_rate" in field_names
        assert "distinct_count" in field_names
        assert "cardinality_ratio" in field_names
        assert "percentiles" in field_names
        assert "empty_string_count" in field_names
        assert "empty_string_rate" in field_names
    
    def test_column_stats_schema_has_new_universal_fields(self, builder):
        """Column stats schema should include new universal metrics."""
        field_names = [f.name for f in builder.COLUMN_STATS_SCHEMA.fields]
        # Universal metrics for all columns
        assert "data_type" in field_names
        assert "sample_values" in field_names
        assert "mode_value" in field_names
        assert "mode_frequency" in field_names
        assert "entropy" in field_names
        assert "is_unique_candidate" in field_names
        assert "value_distribution" in field_names
        assert "pattern_detected" in field_names
        assert "has_numeric_stats" in field_names
        assert "has_string_stats" in field_names
    
    def test_uuid_pattern_detection(self, builder):
        """UUID pattern should be detected correctly."""
        assert builder.UUID_PATTERN.match("123e4567-e89b-12d3-a456-426614174000")
        assert not builder.UUID_PATTERN.match("not-a-uuid")
    
    def test_email_pattern_detection(self, builder):
        """Email pattern should be detected correctly."""
        assert builder.EMAIL_PATTERN.match("test@example.com")
        assert not builder.EMAIL_PATTERN.match("not-an-email")
    
    def test_date_pattern_detection(self, builder):
        """Date pattern should be detected correctly."""
        assert builder.DATE_PATTERN.match("2024-01-15")
        assert builder.DATE_PATTERN.match("2024/01/15")
        assert not builder.DATE_PATTERN.match("January 15, 2024")


class TestRunProfiling:
    """Tests for run_profiling function."""
    
    @patch('src.dbxmetagen.profiling.ProfilingBuilder')
    def test_creates_builder_with_correct_config(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"tables_profiled": 5, "tables_failed": 0}
        mock_builder_class.return_value = mock_builder
        
        mock_spark = MagicMock()
        run_profiling(mock_spark, "my_cat", "my_sch")
        
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('src.dbxmetagen.profiling.ProfilingBuilder')
    def test_passes_max_tables_to_run(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"tables_profiled": 10}
        mock_builder_class.return_value = mock_builder
        
        run_profiling(MagicMock(), "cat", "sch", max_tables=10)
        mock_builder.run.assert_called_once_with(10)
    
    @patch('src.dbxmetagen.profiling.ProfilingBuilder')
    def test_returns_run_result(self, mock_builder_class):
        expected = {"tables_profiled": 5, "tables_failed": 1, "total_tables": 6}
        mock_builder = MagicMock()
        mock_builder.run.return_value = expected
        mock_builder_class.return_value = mock_builder
        
        result = run_profiling(MagicMock(), "cat", "sch")
        assert result == expected


"""Unit tests for profiling module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from src.dbxmetagen.profiling import (
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


class TestProfilingBuilder:
    """Tests for ProfilingBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
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
        mock_spark.sql.assert_called_once()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "column_profiling_stats" in call_arg
        assert "null_rate" in call_arg
        assert "distinct_count" in call_arg


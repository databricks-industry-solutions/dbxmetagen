"""Unit tests for extended_metadata module."""

import pytest
from unittest.mock import MagicMock, patch, call
from pyspark.sql import SparkSession

from dbxmetagen.extended_metadata import (
    ExtendedMetadataConfig,
    ExtendedMetadataBuilder,
    extract_extended_metadata
)


class TestExtendedMetadataConfig:
    """Tests for ExtendedMetadataConfig."""
    
    def test_fully_qualified_source(self):
        config = ExtendedMetadataConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_source == "test_catalog.test_schema.table_knowledge_base"
    
    def test_fully_qualified_target(self):
        config = ExtendedMetadataConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_target == "test_catalog.test_schema.extended_table_metadata"


class TestExtendedMetadataBuilder:
    """Tests for ExtendedMetadataBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock()
    
    @pytest.fixture
    def config(self):
        return ExtendedMetadataConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        return ExtendedMetadataBuilder(mock_spark, config)
    
    @patch("dbxmetagen.processing.add_column_if_not_exists")
    def test_create_target_table(self, mock_add_col, builder, mock_spark):
        builder.create_target_table()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_arg
        assert "extended_table_metadata" in call_arg
        assert "data_source_format STRING" in call_arg
        assert "connection_name STRING" in call_arg
        assert "connection_type STRING" in call_arg
        assert "connection_url STRING" in call_arg
        assert mock_add_col.call_count == 4
    
    def test_get_tables_to_process_empty(self, builder, mock_spark):
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df
        
        result = builder.get_tables_to_process()
        assert result == []


class TestResolveCatalogConnections:
    """Tests for _resolve_catalog_connections."""

    @pytest.fixture
    def mock_spark(self):
        return MagicMock()

    @pytest.fixture
    def builder(self, mock_spark):
        config = ExtendedMetadataConfig(catalog_name="c", schema_name="s")
        return ExtendedMetadataBuilder(mock_spark, config)

    def test_foreign_catalog_resolves(self, builder, mock_spark):
        describe_row = MagicMock()
        describe_row.__getitem__ = lambda self, k: {
            "info_name": {"info_name": "Catalog Type", "info_value": "Foreign"}.get(k, ""),
            "info_value": {"info_name": "Catalog Type", "info_value": "Foreign"}.get(k, ""),
        }.get(k, "")

        cat_type_row = MagicMock()
        cat_type_row.__getitem__ = lambda s, k: {"info_name": "Catalog Type", "info_value": "Foreign"}[k]
        conn_name_row = MagicMock()
        conn_name_row.__getitem__ = lambda s, k: {"info_name": "Connection Name", "info_value": "my_redshift"}[k]

        conn_result_row = MagicMock()
        conn_result_row.__getitem__ = lambda s, k: {
            "connection_name": "my_redshift",
            "connection_type": "REDSHIFT",
            "url": "jdbc://cluster.us-east-1.redshift.amazonaws.com:5439/",
        }[k]

        describe_df = MagicMock()
        describe_df.collect.return_value = [cat_type_row, conn_name_row]

        conn_df = MagicMock()
        conn_df.collect.return_value = [conn_result_row]

        mock_spark.sql.side_effect = [describe_df, conn_df]

        result = builder._resolve_catalog_connections(["fedcat"])
        assert result["fedcat"]["connection_name"] == "my_redshift"
        assert result["fedcat"]["connection_type"] == "REDSHIFT"
        assert "redshift" in result["fedcat"]["connection_url"]

    def test_non_foreign_catalog_returns_empty(self, builder, mock_spark):
        row = MagicMock()
        row.__getitem__ = lambda s, k: {"info_name": "Catalog Type", "info_value": "Managed"}[k]
        describe_df = MagicMock()
        describe_df.collect.return_value = [row]
        mock_spark.sql.return_value = describe_df

        result = builder._resolve_catalog_connections(["managed_cat"])
        assert result == {}

    def test_permission_failure_graceful(self, builder, mock_spark):
        mock_spark.sql.side_effect = Exception("ACCESS_DENIED")
        result = builder._resolve_catalog_connections(["restricted_cat"])
        assert result == {}


class TestBuildStagedUpdatesNewColumns:
    """Verify new connection columns appear in staged DataFrame."""

    @pytest.fixture
    def mock_spark(self):
        return MagicMock()

    @pytest.fixture
    def builder(self, mock_spark):
        config = ExtendedMetadataConfig(catalog_name="c", schema_name="s")
        return ExtendedMetadataBuilder(mock_spark, config)

    @patch.object(ExtendedMetadataBuilder, "get_tables_to_process")
    @patch.object(ExtendedMetadataBuilder, "extract_basic_table_info")
    @patch.object(ExtendedMetadataBuilder, "extract_column_counts")
    @patch.object(ExtendedMetadataBuilder, "extract_lineage")
    @patch.object(ExtendedMetadataBuilder, "extract_constraints")
    @patch.object(ExtendedMetadataBuilder, "extract_table_properties")
    @patch.object(ExtendedMetadataBuilder, "_resolve_catalog_connections")
    def test_connection_columns_joined(
        self, mock_resolve, mock_props, mock_constraints, mock_lineage,
        mock_cols, mock_basic, mock_tables, builder, mock_spark
    ):
        mock_tables.return_value = ["cat.schema.tbl"]
        mock_resolve.return_value = {
            "cat": {"connection_name": "my_conn", "connection_type": "SNOWFLAKE", "connection_url": "jdbc://snow.example.com"}
        }

        mock_df = MagicMock()
        mock_df.join.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_spark.createDataFrame.return_value = mock_df
        mock_basic.return_value = mock_df
        mock_cols.return_value = mock_df
        mock_lineage.return_value = mock_df
        mock_constraints.return_value = mock_df
        mock_props.return_value = mock_df

        builder.build_staged_updates()

        mock_resolve.assert_called_once_with(["cat"])
        create_calls = mock_spark.createDataFrame.call_args_list
        conn_call = [c for c in create_calls if len(c[0]) == 2 and "connection_name" in str(c[0][1])]
        assert len(conn_call) == 1
        data = conn_call[0][0][0]
        assert data == [("cat.schema.tbl", "my_conn", "SNOWFLAKE", "jdbc://snow.example.com")]


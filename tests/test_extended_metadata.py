"""Unit tests for extended_metadata module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from src.dbxmetagen.extended_metadata import (
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
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return ExtendedMetadataConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        return ExtendedMetadataBuilder(mock_spark, config)
    
    def test_create_target_table(self, builder, mock_spark):
        builder.create_target_table()
        mock_spark.sql.assert_called_once()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_arg
        assert "extended_table_metadata" in call_arg
        assert "upstream_tables" in call_arg
        assert "downstream_tables" in call_arg
    
    def test_get_tables_to_process_empty(self, builder, mock_spark):
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df
        
        result = builder.get_tables_to_process()
        assert result == []


"""Unit tests for column_knowledge_base module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

from src.dbxmetagen.column_knowledge_base import (
    ColumnKnowledgeBaseConfig,
    ColumnKnowledgeBaseBuilder,
    build_column_knowledge_base
)


class TestColumnKnowledgeBaseConfig:
    """Tests for ColumnKnowledgeBaseConfig."""
    
    def test_fully_qualified_source(self):
        config = ColumnKnowledgeBaseConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_source == "test_catalog.test_schema.metadata_generation_log"
    
    def test_fully_qualified_target(self):
        config = ColumnKnowledgeBaseConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_target == "test_catalog.test_schema.column_knowledge_base"
    
    def test_custom_tables(self):
        config = ColumnKnowledgeBaseConfig(
            catalog_name="cat",
            schema_name="sch",
            source_table="custom_source",
            target_table="custom_target"
        )
        assert config.fully_qualified_source == "cat.sch.custom_source"
        assert config.fully_qualified_target == "cat.sch.custom_target"


class TestColumnKnowledgeBaseBuilder:
    """Tests for ColumnKnowledgeBaseBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return ColumnKnowledgeBaseConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        return ColumnKnowledgeBaseBuilder(mock_spark, config)
    
    def test_create_target_table(self, builder, mock_spark):
        builder.create_target_table()
        mock_spark.sql.assert_called_once()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_arg
        assert "column_knowledge_base" in call_arg
        assert "column_id STRING NOT NULL" in call_arg
    
    def test_extract_column_comments_filters_correctly(self, builder, mock_spark):
        # Create mock DataFrame
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        
        result = builder.extract_column_comments(mock_df)
        
        # Verify filter was called
        mock_df.filter.assert_called()
    
    def test_extract_pi_data_filters_correctly(self, builder, mock_spark):
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        
        result = builder.extract_pi_data(mock_df)
        
        mock_df.filter.assert_called()


class TestBuildColumnKnowledgeBase:
    """Tests for build_column_knowledge_base function."""
    
    def test_creates_config_correctly(self):
        with patch('src.dbxmetagen.column_knowledge_base.ColumnKnowledgeBaseBuilder') as MockBuilder:
            mock_builder = MagicMock()
            mock_builder.run.return_value = {"staged_count": 10, "total_records": 10}
            MockBuilder.return_value = mock_builder
            
            mock_spark = MagicMock()
            result = build_column_knowledge_base(mock_spark, "cat", "sch")
            
            MockBuilder.assert_called_once()
            config = MockBuilder.call_args[0][1]
            assert config.catalog_name == "cat"
            assert config.schema_name == "sch"


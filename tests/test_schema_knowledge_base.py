"""Unit tests for schema_knowledge_base module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from src.dbxmetagen.schema_knowledge_base import (
    SchemaKnowledgeBaseConfig,
    SchemaKnowledgeBaseBuilder,
    SchemaSummarizer,
    build_schema_knowledge_base
)


class TestSchemaKnowledgeBaseConfig:
    """Tests for SchemaKnowledgeBaseConfig."""
    
    def test_fully_qualified_source(self):
        config = SchemaKnowledgeBaseConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_source == "test_catalog.test_schema.table_knowledge_base"
    
    def test_fully_qualified_target(self):
        config = SchemaKnowledgeBaseConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_target == "test_catalog.test_schema.schema_knowledge_base"


class TestSchemaSummarizer:
    """Tests for SchemaSummarizer."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    def test_summarize_schema_with_no_tables(self, mock_spark):
        summarizer = SchemaSummarizer(mock_spark, "test-model")
        
        mock_df = MagicMock()
        mock_df.select.return_value.collect.return_value = []
        
        result = summarizer.summarize_schema("cat.schema", mock_df)
        assert result is None


class TestSchemaKnowledgeBaseBuilder:
    """Tests for SchemaKnowledgeBaseBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return SchemaKnowledgeBaseConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        return SchemaKnowledgeBaseBuilder(mock_spark, config)
    
    def test_create_target_table(self, builder, mock_spark):
        builder.create_target_table()
        mock_spark.sql.assert_called_once()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_arg
        assert "schema_knowledge_base" in call_arg


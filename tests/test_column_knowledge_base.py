"""Unit tests for column_knowledge_base module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

from dbxmetagen.column_knowledge_base import (
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
        return MagicMock()
    
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


class TestColumnMergeSQLGeneration:
    """Test that column KB MERGE respects review_updated_at."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.mock_spark.sql.return_value.collect.return_value = [{"cnt": 0}]
        self.config = ColumnKnowledgeBaseConfig(
            catalog_name="test_cat", schema_name="test_sch"
        )

    def _get_merge_sql(self):
        builder = ColumnKnowledgeBaseBuilder(self.mock_spark, self.config)
        mock_df = MagicMock()
        builder.merge_to_target(mock_df)
        for c in self.mock_spark.sql.call_args_list:
            sql = c[0][0]
            if "MERGE INTO" in sql:
                return sql
        return None

    def test_merge_preserves_reviewed_comment(self):
        merge_sql = self._get_merge_sql()
        assert merge_sql is not None
        assert "target.review_updated_at IS NOT NULL" in merge_sql
        assert "target.review_updated_at > source.updated_at" in merge_sql
        assert "THEN target.comment" in merge_sql

    def test_merge_preserves_reviewed_classification(self):
        merge_sql = self._get_merge_sql()
        assert merge_sql is not None
        assert "THEN target.classification" in merge_sql

    def test_merge_preserves_reviewed_classification_type(self):
        merge_sql = self._get_merge_sql()
        assert merge_sql is not None
        assert "THEN target.classification_type" in merge_sql

    def test_merge_preserves_reviewed_confidence(self):
        merge_sql = self._get_merge_sql()
        assert merge_sql is not None
        assert "THEN target.confidence" in merge_sql


class TestInformationSchemaScoping:
    """information_schema reads must be scoped to the corpus, not whole-catalog."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.config = ColumnKnowledgeBaseConfig(
            catalog_name="test_cat", schema_name="test_sch"
        )
        self.builder = ColumnKnowledgeBaseBuilder(self.mock_spark, self.config)

    def test_info_schema_query_is_schema_scoped(self):
        """Each information_schema query must filter by BOTH catalog and schema
        (the old code filtered by catalog only, scanning every schema)."""
        # source_df.select(...).where(...).distinct().collect() -> one (cat, sch) pair
        pair_row = MagicMock(cat="cat_a", sch="sch_a")
        chained = MagicMock()
        chained.select.return_value = chained
        chained.where.return_value = chained
        chained.distinct.return_value = chained
        chained.collect.return_value = [pair_row]

        self.builder.get_column_metadata_from_information_schema(chained)

        info_queries = [
            c[0][0] for c in self.mock_spark.sql.call_args_list
            if "information_schema.columns" in c[0][0]
        ]
        assert info_queries, "expected an information_schema query"
        for q in info_queries:
            assert "table_schema" in q, "query must be scoped by schema, not catalog-only"
            assert "table_catalog" in q
            # join key must be lowercased to match source_df.table_name = LOWER(`table`)
            assert "LOWER(CONCAT" in q
            # CRITICAL: predicate columns must NOT be wrapped in LOWER() -- that
            # defeats information_schema predicate pushdown and is 10x slower than
            # the whole-catalog scan it replaces (measured on DMVM). Verified fix.
            assert "LOWER(table_catalog)" not in q, "LOWER() on predicate defeats pushdown"
            assert "LOWER(table_schema)" not in q, "LOWER() on predicate defeats pushdown"

    def test_no_whole_catalog_discovery_query(self):
        """Must not derive catalogs from an unfiltered scan of the whole log."""
        pair_row = MagicMock(cat="cat_a", sch="sch_a")
        chained = MagicMock()
        chained.select.return_value = chained
        chained.where.return_value = chained
        chained.distinct.return_value = chained
        chained.collect.return_value = [pair_row]

        self.builder.get_column_metadata_from_information_schema(chained)

        # scope comes from the passed-in (already-filtered) source_df, not a
        # SELECT DISTINCT ... FROM metadata_generation_log
        for c in self.mock_spark.sql.call_args_list:
            sql = c[0][0]
            if "metadata_generation_log" in sql:
                assert "SELECT DISTINCT" not in sql.upper() or "information_schema" in sql


class TestBuildColumnKnowledgeBase:
    """Tests for build_column_knowledge_base function."""
    
    def test_creates_config_correctly(self):
        with patch('dbxmetagen.column_knowledge_base.ColumnKnowledgeBaseBuilder') as MockBuilder:
            mock_builder = MagicMock()
            mock_builder.run.return_value = {"staged_count": 10, "total_records": 10}
            MockBuilder.return_value = mock_builder
            
            mock_spark = MagicMock()
            result = build_column_knowledge_base(mock_spark, "cat", "sch")
            
            MockBuilder.assert_called_once()
            config = MockBuilder.call_args[0][1]
            assert config.catalog_name == "cat"
            assert config.schema_name == "sch"


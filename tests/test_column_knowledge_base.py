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
    
class TestColumnMergeSQLGeneration:
    """The single build_merge_sql statement must preserve steward-win semantics."""

    def setup_method(self):
        self.config = ColumnKnowledgeBaseConfig(
            catalog_name="test_cat", schema_name="test_sch"
        )
        self.builder = ColumnKnowledgeBaseBuilder(MagicMock(), self.config)
        self.merge_sql = self.builder.build_merge_sql([("cat_a", "sch_a")])

    def test_is_single_merge_statement(self):
        # One declarative MERGE -- not a multi-step pipeline. Exactly one MERGE
        # and exactly one read of the (slow) system info_schema view.
        assert self.merge_sql.count("MERGE INTO") == 1
        assert self.merge_sql.count("system.information_schema.columns") == 1
        # no temp-table materialization machinery
        assert "temp_ckb" not in self.merge_sql

    def test_latest_wins_via_row_number(self):
        # comment + PI are "most recent per column" via ROW_NUMBER windows
        assert "ROW_NUMBER() OVER" in self.merge_sql
        assert "metadata_type = 'comment'" in self.merge_sql
        assert "metadata_type = 'pi'" in self.merge_sql

    def test_merge_preserves_reviewed_comment(self):
        assert "target.review_updated_at IS NOT NULL" in self.merge_sql
        assert "target.review_updated_at > source.updated_at" in self.merge_sql
        assert "THEN target.comment" in self.merge_sql

    def test_merge_preserves_reviewed_classification(self):
        assert "THEN target.classification" in self.merge_sql

    def test_merge_preserves_reviewed_classification_type(self):
        assert "THEN target.classification_type" in self.merge_sql

    def test_merge_preserves_reviewed_confidence(self):
        assert "THEN target.confidence" in self.merge_sql


class TestInformationSchemaScoping:
    """information_schema reads must be scoped to the corpus, not whole-catalog."""

    def setup_method(self):
        self.config = ColumnKnowledgeBaseConfig(
            catalog_name="test_cat", schema_name="test_sch"
        )
        self.builder = ColumnKnowledgeBaseBuilder(MagicMock(), self.config)

    def test_info_schema_is_schema_scoped_and_pushdown_safe(self):
        """The info_schema read must filter by BOTH catalog and schema, join on a
        lowercased key, and keep predicate columns BARE (LOWER() on a predicate
        column defeats pushdown -- measured 10x slower)."""
        q = self.builder.build_merge_sql([("cat_a", "sch_a")])
        assert "table_schema IN (" in q, "must scope by schema, not catalog-only"
        assert "table_catalog = 'cat_a'" in q
        assert "LOWER(CONCAT" in q, "join key lowercased to match LOWER(`table`)"
        assert "LOWER(table_catalog)" not in q, "LOWER() on predicate defeats pushdown"
        assert "LOWER(table_schema)" not in q, "LOWER() on predicate defeats pushdown"

    def test_empty_corpus_yields_no_unbounded_scan(self):
        """No pairs -> a false predicate, never an unfiltered info_schema scan."""
        q = self.builder.build_merge_sql([])
        assert "1=0" in q

    def test_schemas_batched_into_one_query_per_catalog(self):
        """Many schemas in one catalog must become ONE `table_schema IN (...)`
        group, not one query per schema. system.information_schema.columns has a
        large fixed per-query cost, so per-schema queries multiply runtime
        (measured: 11 schemas = ~52s vs ~6s for a single IN-list query)."""
        q = self.builder.build_merge_sql([
            ("cat_a", "sch_a"), ("cat_a", "sch_b"), ("cat_a", "sch_c"),
        ])
        # exactly one info_schema read, one IN-list, all three schemas present
        assert q.count("system.information_schema.columns") == 1
        assert q.count("table_schema IN (") == 1
        for s in ("sch_a", "sch_b", "sch_c"):
            assert f"'{s}'" in q


class TestInformationSchemaFallback:
    """An information_schema failure degrades to NULL types, never fails the KB."""

    def _builder(self, sql_side_effect):
        spark = MagicMock()
        spark.sql.side_effect = sql_side_effect
        cfg = ColumnKnowledgeBaseConfig(catalog_name="c", schema_name="s")
        b = ColumnKnowledgeBaseBuilder(spark, cfg)
        b.create_target_table = MagicMock()
        b._corpus_catalog_schema_pairs = MagicMock(return_value=[("cat_a", "sch_a")])
        return b, spark

    @staticmethod
    def _count_result():
        m = MagicMock()
        m.collect.return_value = [{"cnt": 3}]
        return m

    def test_no_types_variant_never_touches_information_schema(self):
        b = ColumnKnowledgeBaseBuilder(MagicMock(), ColumnKnowledgeBaseConfig(catalog_name="c", schema_name="s"))
        q = b.build_merge_sql([("cat_a", "sch_a")], include_types=False)
        assert "information_schema" not in q
        # same CTE name and columns, so the rest of the MERGE is unchanged
        assert "types AS (" in q and "LEFT JOIN types USING (table_name, column_name)" in q
        assert "COALESCE(source.data_type, target.data_type)" in q

    def test_info_schema_failure_retries_once_without_types(self):
        merges = []

        def sql(q):
            if q.lstrip().startswith("MERGE"):
                merges.append(q)
                if "system.information_schema.columns" in q:
                    raise Exception("PERMISSION_DENIED: system.information_schema")
            return self._count_result()

        b, _ = self._builder(sql)
        result = b.run()
        assert len(merges) == 2
        assert "system.information_schema.columns" in merges[0]
        assert "information_schema" not in merges[1]
        assert result["types_included"] is False

    def test_error_not_caused_by_info_schema_still_raises(self):
        def sql(q):
            if q.lstrip().startswith("MERGE"):
                raise Exception("TABLE_OR_VIEW_NOT_FOUND: metadata_generation_log")
            return self._count_result()

        b, spark = self._builder(sql)
        with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
            b.run()
        assert sum(1 for c in spark.sql.call_args_list if c[0][0].lstrip().startswith("MERGE")) == 2

    def test_concurrency_conflict_retries_with_types_kept(self, monkeypatch):
        import dbxmetagen.column_knowledge_base as ckb
        monkeypatch.setattr(ckb.time, "sleep", lambda s: None)
        merges = []

        def sql(q):
            if q.lstrip().startswith("MERGE"):
                merges.append(q)
                if len(merges) == 1:
                    raise Exception("DELTA_CONCURRENT_APPEND: conflicting commit")
            return self._count_result()

        b, _ = self._builder(sql)
        assert b.run()["types_included"] is True
        assert len(merges) == 2 and all("system.information_schema.columns" in q for q in merges)


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


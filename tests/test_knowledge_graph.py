"""Unit tests for knowledge_graph module.

Tests focus on:
1. Pure function logic (security level computation)
2. SQL generation correctness (DDL, MERGE)
3. Configuration behavior
4. Edge building logic
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.knowledge_graph import (
    KnowledgeGraphConfig,
    compute_security_level,
    KnowledgeGraphBuilder,
    build_knowledge_graph,
)


class TestComputeSecurityLevel:
    """Test security level computation - determines data sensitivity."""

    def test_phi_takes_precedence_over_pii(self):
        """PHI is highest security level, even if PII is also true."""
        assert compute_security_level(has_pii=True, has_phi=True) == "PHI"

    def test_phi_without_pii_returns_phi(self):
        """PHI alone returns PHI (edge case - PHI implies PII)."""
        assert compute_security_level(has_pii=False, has_phi=True) == "PHI"

    def test_pii_without_phi_returns_pii(self):
        """PII without PHI returns PII."""
        assert compute_security_level(has_pii=True, has_phi=False) == "PII"

    def test_neither_pii_nor_phi_returns_public(self):
        """No sensitive data classification returns PUBLIC."""
        assert compute_security_level(has_pii=False, has_phi=False) == "PUBLIC"


class TestKnowledgeGraphConfig:
    """Test configuration dataclass behavior."""

    def test_fully_qualified_paths_built_correctly(self):
        """Config should build correct fully qualified table paths."""
        config = KnowledgeGraphConfig(
            catalog_name="prod",
            schema_name="metadata"
        )
        assert config.fully_qualified_source == "prod.metadata.table_knowledge_base"
        assert config.fully_qualified_nodes == "prod.metadata.graph_nodes"
        assert config.fully_qualified_edges == "prod.metadata.graph_edges"

    def test_custom_table_names_override_defaults(self):
        """Custom table names should override defaults."""
        config = KnowledgeGraphConfig(
            catalog_name="cat",
            schema_name="sch",
            source_table="my_kb",
            nodes_table="my_nodes",
            edges_table="my_edges"
        )
        assert config.fully_qualified_source == "cat.sch.my_kb"
        assert config.fully_qualified_nodes == "cat.sch.my_nodes"
        assert config.fully_qualified_edges == "cat.sch.my_edges"

    def test_default_table_names(self):
        """Default table names should be set correctly."""
        config = KnowledgeGraphConfig(
            catalog_name="c",
            schema_name="s"
        )
        assert config.source_table == "table_knowledge_base"
        assert config.nodes_table == "graph_nodes"
        assert config.edges_table == "graph_edges"


class TestCreateNodesDDL:
    """Test node table DDL generation."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.config = KnowledgeGraphConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )

    def test_nodes_ddl_includes_required_columns(self):
        """Nodes DDL must include all required columns for GraphFrames."""
        builder = KnowledgeGraphBuilder(self.mock_spark, self.config)
        builder.create_nodes_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        
        # GraphFrames requires 'id' column
        assert "id STRING NOT NULL" in ddl
        # Other required columns (note: `schema` is escaped because it's a reserved word)
        assert "table_name STRING" in ddl
        assert "catalog STRING" in ddl
        assert "`schema` STRING" in ddl  # Escaped reserved word
        assert "domain STRING" in ddl
        assert "subdomain STRING" in ddl
        assert "security_level STRING" in ddl
        assert "has_pii BOOLEAN" in ddl
        assert "has_phi BOOLEAN" in ddl

    def test_nodes_ddl_uses_correct_table_name(self):
        """DDL should create table at the configured location."""
        builder = KnowledgeGraphBuilder(self.mock_spark, self.config)
        builder.create_nodes_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        assert "test_cat.test_sch.graph_nodes" in ddl


class TestCreateEdgesDDL:
    """Test edge table DDL generation."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.config = KnowledgeGraphConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )

    def test_edges_ddl_includes_graphframes_columns(self):
        """Edges DDL must include src, dst columns required by GraphFrames."""
        builder = KnowledgeGraphBuilder(self.mock_spark, self.config)
        builder.create_edges_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        
        # GraphFrames requires 'src' and 'dst' columns
        assert "src STRING NOT NULL" in ddl
        assert "dst STRING NOT NULL" in ddl
        assert "relationship STRING NOT NULL" in ddl
        assert "weight DOUBLE" in ddl

    def test_edges_ddl_uses_correct_table_name(self):
        """DDL should create table at the configured location."""
        builder = KnowledgeGraphBuilder(self.mock_spark, self.config)
        builder.create_edges_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        assert "test_cat.test_sch.graph_edges" in ddl


class TestNodesMergeSQL:
    """Test that nodes MERGE SQL is generated correctly."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.mock_spark.sql.return_value.collect.return_value = [{"cnt": 0}]
        self.config = KnowledgeGraphConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )

    def _get_merge_sql(self):
        """Helper to extract MERGE SQL from mock calls."""
        builder = KnowledgeGraphBuilder(self.mock_spark, self.config)
        mock_df = MagicMock()
        builder.merge_nodes(mock_df)
        
        for call in self.mock_spark.sql.call_args_list:
            sql = call[0][0]
            if "MERGE INTO" in sql and "graph_nodes" in sql:
                return sql
        return None

    def test_nodes_merge_matches_on_id(self):
        """Nodes MERGE should match on id (the GraphFrames key)."""
        merge_sql = self._get_merge_sql()
        assert "ON target.id = source.id" in merge_sql

    def test_nodes_merge_uses_coalesce_for_nullable(self):
        """MERGE should use COALESCE to preserve existing values."""
        merge_sql = self._get_merge_sql()
        assert "COALESCE(source.domain, target.domain)" in merge_sql
        assert "COALESCE(source.subdomain, target.subdomain)" in merge_sql
        assert "COALESCE(source.comment, target.comment)" in merge_sql

    def test_nodes_merge_uses_or_for_booleans(self):
        """Boolean flags should use OR logic."""
        merge_sql = self._get_merge_sql()
        assert "source.has_pii OR target.has_pii" in merge_sql
        assert "source.has_phi OR target.has_phi" in merge_sql


class TestEdgesMergeSQL:
    """Test that edges MERGE SQL is generated correctly."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.mock_spark.sql.return_value.collect.return_value = [{"cnt": 0}]
        self.config = KnowledgeGraphConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )

    def _get_merge_sql(self):
        """Helper to extract MERGE SQL from mock calls."""
        builder = KnowledgeGraphBuilder(self.mock_spark, self.config)
        mock_df = MagicMock()
        builder.merge_edges(mock_df)
        
        for call in self.mock_spark.sql.call_args_list:
            sql = call[0][0]
            if "MERGE INTO" in sql and "graph_edges" in sql:
                return sql
        return None

    def test_edges_merge_uses_composite_key(self):
        """Edges MERGE should match on (src, dst, relationship) composite key."""
        merge_sql = self._get_merge_sql()
        assert "target.src = source.src" in merge_sql
        assert "target.dst = source.dst" in merge_sql
        assert "target.relationship = source.relationship" in merge_sql

    def test_edges_merge_updates_weight_and_timestamp(self):
        """MERGE should update weight and timestamp on match."""
        merge_sql = self._get_merge_sql()
        assert "target.weight = source.weight" in merge_sql
        assert "target.updated_at = source.updated_at" in merge_sql


class TestRelationshipTypes:
    """Test that all expected relationship types are defined."""

    def test_all_relationship_types_defined(self):
        """Builder should define all required relationship types."""
        expected_types = [
            "same_domain",
            "same_subdomain",
            "same_catalog",
            "same_schema",
            "same_security_level"
        ]
        assert KnowledgeGraphBuilder.RELATIONSHIP_TYPES == expected_types


class TestBuildKnowledgeGraphFunction:
    """Test the convenience function."""

    @patch("src.dbxmetagen.knowledge_graph.KnowledgeGraphBuilder")
    def test_function_creates_correct_config(self, mock_builder_class):
        """Function should create config with provided catalog/schema."""
        mock_builder = MagicMock()
        mock_builder.run.return_value = {
            "staged_nodes": 0, 
            "staged_edges": 0,
            "total_nodes": 0,
            "total_edges": 0
        }
        mock_builder_class.return_value = mock_builder
        
        build_knowledge_graph(
            spark=MagicMock(),
            catalog_name="my_catalog",
            schema_name="my_schema"
        )
        
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_catalog"
        assert config.schema_name == "my_schema"

    @patch("src.dbxmetagen.knowledge_graph.KnowledgeGraphBuilder")
    def test_function_returns_run_result(self, mock_builder_class):
        """Function should return the result from builder.run()."""
        mock_builder = MagicMock()
        expected_result = {
            "staged_nodes": 10, 
            "staged_edges": 50,
            "total_nodes": 10,
            "total_edges": 50
        }
        mock_builder.run.return_value = expected_result
        mock_builder_class.return_value = mock_builder
        
        result = build_knowledge_graph(
            spark=MagicMock(),
            catalog_name="cat",
            schema_name="sch"
        )
        
        assert result == expected_result


class TestEdgeBuildingLogicDocumentation:
    """
    Document edge building logic tested at integration level.
    
    The build_edges_for_attribute method uses PySpark DataFrame operations
    that require an active SparkContext, so detailed testing is done in
    the integration test (test_11_knowledge_base.py).
    
    Key behaviors verified in integration tests:
    1. Edges only built between nodes with non-null attribute values
    2. Self-loops avoided (src == dst filtered out)
    3. Duplicate edges avoided (only src < dst kept)
    4. All relationship types created correctly
    """

    def test_method_exists(self):
        """Verify build_edges_for_attribute method exists."""
        mock_spark = MagicMock()
        config = KnowledgeGraphConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )
        builder = KnowledgeGraphBuilder(mock_spark, config)
        assert hasattr(builder, "build_edges_for_attribute")
        assert callable(builder.build_edges_for_attribute)

    def test_build_all_edges_method_exists(self):
        """Verify build_all_edges_df method exists."""
        mock_spark = MagicMock()
        config = KnowledgeGraphConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )
        builder = KnowledgeGraphBuilder(mock_spark, config)
        assert hasattr(builder, "build_all_edges_df")
        assert callable(builder.build_all_edges_df)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


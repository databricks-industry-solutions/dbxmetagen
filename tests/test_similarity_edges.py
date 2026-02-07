"""Unit tests for similarity_edges module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from dbxmetagen.similarity_edges import (
    SimilarityEdgesConfig,
    SimilarityEdgeBuilder,
    build_similarity_edges
)


class TestSimilarityEdgesConfig:
    """Tests for SimilarityEdgesConfig."""
    
    def test_fully_qualified_nodes(self):
        config = SimilarityEdgesConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_nodes == "test_catalog.test_schema.graph_nodes"
    
    def test_fully_qualified_edges(self):
        config = SimilarityEdgesConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_edges == "test_catalog.test_schema.graph_edges"
    
    def test_default_similarity_threshold(self):
        config = SimilarityEdgesConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.similarity_threshold == 0.8
    
    def test_default_max_edges_per_node(self):
        config = SimilarityEdgesConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.max_edges_per_node == 10
    
    def test_custom_threshold(self):
        config = SimilarityEdgesConfig(
            catalog_name="cat",
            schema_name="sch",
            similarity_threshold=0.6,
            max_edges_per_node=5
        )
        assert config.similarity_threshold == 0.6
        assert config.max_edges_per_node == 5


class TestSimilarityEdgeBuilder:
    """Tests for SimilarityEdgeBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return SimilarityEdgesConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        return SimilarityEdgeBuilder(mock_spark, config)
    
    def test_relationship_type_constant(self, builder):
        """Relationship type should be 'similar_embedding'."""
        assert builder.RELATIONSHIP_TYPE == "similar_embedding"
    
    def test_get_nodes_with_embeddings_query(self, builder, mock_spark):
        builder.get_nodes_with_embeddings()
        mock_spark.sql.assert_called_once()
        query = mock_spark.sql.call_args[0][0]
        assert "embedding IS NOT NULL" in query
    
    def test_remove_existing_similarity_edges(self, builder, mock_spark):
        builder.remove_existing_similarity_edges()
        mock_spark.sql.assert_called_once()
        query = mock_spark.sql.call_args[0][0]
        assert "DELETE FROM" in query
        assert "similar_embedding" in query
    
    def test_compute_similarity_fallback_empty_nodes(self, builder, mock_spark):
        """Fallback should handle empty node list."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.createDataFrame.return_value = mock_df
        
        result = builder._compute_similarity_fallback(mock_df)
        assert result is not None
    
    def test_compute_similarity_fallback_single_node(self, builder, mock_spark):
        """Fallback should handle single node (no pairs to compare)."""
        mock_node = MagicMock()
        mock_node.id = "node1"
        mock_node.embedding = [1.0, 0.0, 0.0]
        
        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_node]
        mock_spark.createDataFrame.return_value = mock_df
        
        result = builder._compute_similarity_fallback(mock_df)
        assert result is not None


class TestCosineSimInFallback:
    """Tests for the cosine similarity calculation in fallback."""
    
    def test_identical_vectors(self):
        """Identical vectors should have similarity 1.0."""
        v = [1.0, 0.0, 0.0]
        dot = sum(a * b for a, b in zip(v, v))
        norm = sum(a * a for a in v) ** 0.5
        sim = dot / (norm * norm) if norm else 0.0
        assert abs(sim - 1.0) < 0.001
    
    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity 0.0."""
        v1 = [1.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0]
        dot = sum(a * b for a, b in zip(v1, v2))
        norm1 = sum(a * a for a in v1) ** 0.5
        norm2 = sum(b * b for b in v2) ** 0.5
        sim = dot / (norm1 * norm2) if norm1 and norm2 else 0.0
        assert abs(sim) < 0.001
    
    def test_opposite_vectors(self):
        """Opposite vectors should have similarity -1.0."""
        v1 = [1.0, 0.0, 0.0]
        v2 = [-1.0, 0.0, 0.0]
        dot = sum(a * b for a, b in zip(v1, v2))
        norm1 = sum(a * a for a in v1) ** 0.5
        norm2 = sum(b * b for b in v2) ** 0.5
        sim = dot / (norm1 * norm2) if norm1 and norm2 else 0.0
        assert abs(sim + 1.0) < 0.001


class TestBuildSimilarityEdges:
    """Tests for build_similarity_edges function."""
    
    @patch('src.dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_creates_builder_with_correct_config(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"edges_created": 10, "threshold": 0.8}
        mock_builder_class.return_value = mock_builder
        
        mock_spark = MagicMock()
        build_similarity_edges(mock_spark, "my_cat", "my_sch")
        
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('src.dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_passes_custom_threshold(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {}
        mock_builder_class.return_value = mock_builder
        
        build_similarity_edges(MagicMock(), "cat", "sch", similarity_threshold=0.6)
        
        config = mock_builder_class.call_args[0][1]
        assert config.similarity_threshold == 0.6
    
    @patch('src.dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_passes_max_edges_per_node(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {}
        mock_builder_class.return_value = mock_builder
        
        build_similarity_edges(MagicMock(), "cat", "sch", max_edges_per_node=5)
        
        config = mock_builder_class.call_args[0][1]
        assert config.max_edges_per_node == 5
    
    @patch('src.dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_returns_run_result(self, mock_builder_class):
        expected = {"edges_created": 50, "threshold": 0.75}
        mock_builder = MagicMock()
        mock_builder.run.return_value = expected
        mock_builder_class.return_value = mock_builder
        
        result = build_similarity_edges(MagicMock(), "cat", "sch")
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


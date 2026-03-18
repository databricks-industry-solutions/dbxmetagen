"""Unit tests for embeddings module."""

import pytest
import math
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from dbxmetagen.embeddings import (
    EmbeddingConfig,
    EmbeddingGenerator,
    generate_embeddings,
    find_similar_nodes
)


class TestEmbeddingConfig:
    """Tests for EmbeddingConfig."""
    
    def test_fully_qualified_nodes(self):
        config = EmbeddingConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_nodes == "test_catalog.test_schema.graph_nodes"
    
    def test_default_model(self):
        config = EmbeddingConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.model == "databricks-bge-large-en"
    
    def test_default_similarity_threshold(self):
        config = EmbeddingConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.similarity_threshold == 0.8
    
    def test_custom_similarity_threshold(self):
        config = EmbeddingConfig(
            catalog_name="cat",
            schema_name="sch",
            similarity_threshold=0.6
        )
        assert config.similarity_threshold == 0.6
    
    def test_custom_model(self):
        config = EmbeddingConfig(
            catalog_name="cat",
            schema_name="sch",
            model="custom-model"
        )
        assert config.model == "custom-model"


class TestEmbeddingGenerator:
    """Tests for EmbeddingGenerator."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock()
    
    @pytest.fixture
    def config(self):
        return EmbeddingConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def generator(self, mock_spark, config):
        return EmbeddingGenerator(mock_spark, config)
    
    def test_compute_cosine_similarity_identical(self, generator):
        vec = [1.0, 0.0, 0.0]
        sim = generator.compute_cosine_similarity(vec, vec)
        assert abs(sim - 1.0) < 0.001
    
    def test_compute_cosine_similarity_orthogonal(self, generator):
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.0, 1.0, 0.0]
        sim = generator.compute_cosine_similarity(vec1, vec2)
        assert abs(sim) < 0.001
    
    def test_compute_cosine_similarity_opposite(self, generator):
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [-1.0, 0.0, 0.0]
        sim = generator.compute_cosine_similarity(vec1, vec2)
        assert abs(sim + 1.0) < 0.001
    
    def test_compute_cosine_similarity_45_degrees(self, generator):
        vec1 = [1.0, 0.0]
        vec2 = [1.0, 1.0]
        sim = generator.compute_cosine_similarity(vec1, vec2)
        # cos(45 degrees) = sqrt(2)/2 ≈ 0.707
        expected = 1.0 / math.sqrt(2)
        assert abs(sim - expected) < 0.01
    
    def test_compute_cosine_similarity_empty(self, generator):
        sim = generator.compute_cosine_similarity([], [])
        assert sim == 0.0
    
    def test_compute_cosine_similarity_mismatched_length(self, generator):
        vec1 = [1.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        sim = generator.compute_cosine_similarity(vec1, vec2)
        assert sim == 0.0
    
    def test_compute_cosine_similarity_zero_vector(self, generator):
        vec1 = [0.0, 0.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        sim = generator.compute_cosine_similarity(vec1, vec2)
        assert sim == 0.0


class TestGenerateEmbeddings:
    """Tests for generate_embeddings function."""
    
    @patch('dbxmetagen.embeddings.EmbeddingGenerator')
    def test_creates_generator_with_correct_config(self, mock_gen_class):
        mock_gen = MagicMock()
        mock_gen.run.return_value = {"nodes_embedded": 10, "total_candidates": 10}
        mock_gen_class.return_value = mock_gen
        
        mock_spark = MagicMock()
        generate_embeddings(mock_spark, "my_cat", "my_sch")
        
        config = mock_gen_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('dbxmetagen.embeddings.EmbeddingGenerator')
    def test_returns_run_result(self, mock_gen_class):
        expected = {"nodes_embedded": 15, "total_candidates": 20}
        mock_gen = MagicMock()
        mock_gen.run.return_value = expected
        mock_gen_class.return_value = mock_gen
        
        result = generate_embeddings(MagicMock(), "cat", "sch")
        assert result == expected


class TestFindSimilarNodes:
    """Tests for find_similar_nodes function."""
    
    @patch('dbxmetagen.embeddings.EmbeddingGenerator')
    def test_returns_dataframe(self, mock_gen_class):
        mock_gen = MagicMock()
        expected_df = MagicMock()
        mock_gen.find_similar_nodes.return_value = expected_df
        mock_gen_class.return_value = mock_gen
        
        result = find_similar_nodes(MagicMock(), "cat", "sch", similarity_threshold=0.7)
        
        mock_gen.find_similar_nodes.assert_called_once()
        assert result == expected_df


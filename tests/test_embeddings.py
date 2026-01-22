"""Unit tests for embeddings module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from src.dbxmetagen.embeddings import (
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


class TestEmbeddingGenerator:
    """Tests for EmbeddingGenerator."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
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
    
    def test_compute_cosine_similarity_empty(self, generator):
        sim = generator.compute_cosine_similarity([], [])
        assert sim == 0.0
    
    def test_compute_cosine_similarity_mismatched_length(self, generator):
        vec1 = [1.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        sim = generator.compute_cosine_similarity(vec1, vec2)
        assert sim == 0.0


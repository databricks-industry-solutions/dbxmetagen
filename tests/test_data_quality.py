"""Unit tests for data_quality module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from src.dbxmetagen.data_quality import (
    DataQualityConfig,
    DataQualityScorer,
    compute_data_quality
)


class TestDataQualityConfig:
    """Tests for DataQualityConfig."""
    
    def test_fully_qualified_scores(self):
        config = DataQualityConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_scores == "test_catalog.test_schema.data_quality_scores"
    
    def test_default_thresholds(self):
        config = DataQualityConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.freshness_threshold_days == 7
        assert config.null_rate_threshold == 0.5


class TestDataQualityScorer:
    """Tests for DataQualityScorer."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock(spec=SparkSession)
    
    @pytest.fixture
    def config(self):
        return DataQualityConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def scorer(self, mock_spark, config):
        return DataQualityScorer(mock_spark, config)
    
    def test_create_scores_table(self, scorer, mock_spark):
        scorer.create_scores_table()
        mock_spark.sql.assert_called_once()
        call_arg = mock_spark.sql.call_args[0][0]
        assert "data_quality_scores" in call_arg
        assert "completeness_score" in call_arg
        assert "overall_score" in call_arg
    
    def test_compute_freshness_score_fresh_data(self, scorer):
        # Data modified today should have high score
        last_modified = datetime.utcnow()
        score, issues = scorer.compute_freshness_score(last_modified)
        assert score == 100.0
        assert len(issues) == 0
    
    def test_compute_freshness_score_stale_data(self, scorer):
        # Data modified 30 days ago should have lower score
        last_modified = datetime.utcnow() - timedelta(days=30)
        score, issues = scorer.compute_freshness_score(last_modified)
        assert score < 100.0
        assert len(issues) > 0
    
    def test_compute_freshness_score_no_modified_time(self, scorer):
        score, issues = scorer.compute_freshness_score(None)
        assert score == 50.0
        assert "Could not determine" in issues[0]
    
    def test_dimension_weights_sum_to_one(self, scorer):
        total = sum(scorer.DIMENSION_WEIGHTS.values())
        assert abs(total - 1.0) < 0.001


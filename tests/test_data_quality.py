"""Unit tests for data_quality module."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from dbxmetagen.data_quality import (
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
    
    def test_custom_thresholds(self):
        config = DataQualityConfig(
            catalog_name="cat",
            schema_name="sch",
            freshness_threshold_days=14,
            null_rate_threshold=0.3
        )
        assert config.freshness_threshold_days == 14
        assert config.null_rate_threshold == 0.3
    
    def test_fully_qualified_kb(self):
        """Knowledge base reference should be available for metadata scoring."""
        config = DataQualityConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_kb == "test_catalog.test_schema.table_knowledge_base"


class TestDataQualityScorer:
    """Tests for DataQualityScorer."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock()
    
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
        mock_spark.sql.assert_called()
        call_arg = mock_spark.sql.call_args_list[0][0][0]
        assert "data_quality_scores" in call_arg
        assert "completeness_score" in call_arg
        assert "overall_score" in call_arg
    
    def test_compute_freshness_score_fresh_data(self, scorer):
        """Data modified today should have high score."""
        last_modified = datetime.utcnow()
        score, issues, calculated = scorer.compute_freshness_score(last_modified)
        assert score == 100.0
        assert len(issues) == 0
        assert calculated is True
    
    def test_compute_freshness_score_at_threshold(self, scorer):
        """Data at exactly the threshold should still be 100%."""
        last_modified = datetime.utcnow() - timedelta(days=7)
        score, issues, calculated = scorer.compute_freshness_score(last_modified)
        assert score == 100.0
        assert calculated is True
    
    def test_compute_freshness_score_stale_data(self, scorer):
        """Data modified 30 days ago should have lower score."""
        last_modified = datetime.utcnow() - timedelta(days=30)
        score, issues, calculated = scorer.compute_freshness_score(last_modified)
        assert score < 100.0
        assert len(issues) > 0
        assert calculated is True
    
    def test_compute_freshness_score_very_stale_bottoms_at_minimum(self, scorer):
        """Very old data should not go below minimum (20)."""
        last_modified = datetime.utcnow() - timedelta(days=365)
        score, issues, calculated = scorer.compute_freshness_score(last_modified)
        assert score >= 20.0  # Gentler decay - minimum is 20
    
    def test_compute_freshness_score_no_modified_time(self, scorer):
        """None last_modified should return default score and indicate not calculated."""
        score, issues, calculated = scorer.compute_freshness_score(None)
        assert score == 60.0  # Default for unknown
        assert "Could not determine" in issues[0]
        assert calculated is False
    
    def test_dimension_weights_sum_to_one(self, scorer):
        total = sum(scorer.DIMENSION_WEIGHTS.values())
        assert abs(total - 1.0) < 0.001
    
    def test_all_dimensions_have_weight(self, scorer):
        """Ensure all dimension weights are defined and positive."""
        expected_dimensions = ["completeness", "uniqueness", "freshness", "consistency", "metadata"]
        for dim in expected_dimensions:
            assert dim in scorer.DIMENSION_WEIGHTS
            assert scorer.DIMENSION_WEIGHTS[dim] > 0
    
    def test_metadata_dimension_exists(self, scorer):
        """New metadata dimension should be present."""
        assert "metadata" in scorer.DIMENSION_WEIGHTS
        assert scorer.DIMENSION_WEIGHTS["metadata"] > 0
    
    def test_scores_schema_has_all_required_fields(self, scorer):
        """Scores DDL should include all score dimension columns."""
        scorer.create_scores_table()
        ddl = scorer.spark.sql.call_args_list[0][0][0]
        for col_name in ["completeness_score", "uniqueness_score", "freshness_score",
                         "consistency_score", "metadata_score", "overall_score",
                         "quality_issues", "dimensions_calculated"]:
            assert col_name in ddl, f"Missing column {col_name} in scores DDL"
    
    def test_pd_safe_mean_returns_default_for_missing_column(self, scorer):
        """_pd_safe_mean should return default when column is missing."""
        import pandas as pd
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        result = scorer._pd_safe_mean(df, "missing_col", 99.0)
        assert result == 99.0

    def test_pd_safe_mean_computes_average(self, scorer):
        """_pd_safe_mean should return mean when column exists."""
        import pandas as pd
        df = pd.DataFrame({"null_rate": [0.1, 0.3, 0.5]})
        result = scorer._pd_safe_mean(df, "null_rate", 0.0)
        assert abs(result - 0.3) < 1e-9


class TestComputeDataQuality:
    """Tests for compute_data_quality function."""
    
    @patch('dbxmetagen.data_quality.DataQualityScorer')
    def test_creates_scorer_with_correct_config(self, mock_scorer_class):
        mock_scorer = MagicMock()
        mock_scorer.run.return_value = {"tables_scored": 5, "average_score": 85.0}
        mock_scorer_class.return_value = mock_scorer
        
        mock_spark = MagicMock()
        result = compute_data_quality(mock_spark, "my_cat", "my_sch")
        
        config = mock_scorer_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('dbxmetagen.data_quality.DataQualityScorer')
    def test_returns_run_result(self, mock_scorer_class):
        expected = {"tables_scored": 10, "average_score": 90.0, "low_quality_tables": 1}
        mock_scorer = MagicMock()
        mock_scorer.run.return_value = expected
        mock_scorer_class.return_value = mock_scorer
        
        result = compute_data_quality(MagicMock(), "cat", "sch")
        assert result == expected

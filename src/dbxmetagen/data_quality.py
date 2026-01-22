"""
Data Quality module for computing quality scores.

Calculates completeness, uniqueness, freshness, and consistency scores
based on profiling data and system table metrics.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class DataQualityConfig:
    """Configuration for data quality scoring."""
    catalog_name: str
    schema_name: str
    profiling_table: str = "profiling_snapshots"
    column_stats_table: str = "column_profiling_stats"
    scores_table: str = "data_quality_scores"
    # Thresholds for scoring
    freshness_threshold_days: int = 7
    null_rate_threshold: float = 0.5
    
    @property
    def fully_qualified_profiling(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.profiling_table}"
    
    @property
    def fully_qualified_column_stats(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_stats_table}"
    
    @property
    def fully_qualified_scores(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.scores_table}"


class DataQualityScorer:
    """
    Scorer for computing data quality metrics.
    
    Dimensions:
    - Completeness: Based on null rates across columns
    - Uniqueness: Based on distinct value ratios
    - Freshness: Based on last modified time
    - Consistency: Based on data type conformance and constraint violations
    """
    
    # Weights for overall score calculation
    DIMENSION_WEIGHTS = {
        "completeness": 0.3,
        "uniqueness": 0.2,
        "freshness": 0.3,
        "consistency": 0.2
    }
    
    def __init__(self, spark: SparkSession, config: DataQualityConfig):
        self.spark = spark
        self.config = config
    
    def create_scores_table(self) -> None:
        """Create the data quality scores table."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_scores} (
            score_id STRING NOT NULL,
            table_name STRING NOT NULL,
            snapshot_id STRING,
            completeness_score DOUBLE,
            uniqueness_score DOUBLE,
            freshness_score DOUBLE,
            consistency_score DOUBLE,
            overall_score DOUBLE,
            quality_issues ARRAY<STRING>,
            created_at TIMESTAMP
        )
        COMMENT 'Data quality scores by dimension with issues'
        """
        self.spark.sql(ddl)
        logger.info(f"Quality scores table {self.config.fully_qualified_scores} ready")
    
    def get_latest_snapshots(self) -> DataFrame:
        """Get most recent snapshot per table."""
        return self.spark.sql(f"""
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY snapshot_time DESC) as rn
                FROM {self.config.fully_qualified_profiling}
            )
            SELECT * FROM ranked WHERE rn = 1
        """)
    
    def get_column_stats_for_snapshot(self, snapshot_id: str) -> DataFrame:
        """Get column stats for a specific snapshot."""
        return self.spark.sql(f"""
            SELECT * FROM {self.config.fully_qualified_column_stats}
            WHERE snapshot_id = '{snapshot_id}'
        """)
    
    def compute_completeness_score(self, column_stats: DataFrame) -> tuple:
        """
        Compute completeness score based on null rates.
        
        Score = 100 - (average null rate * 100)
        """
        issues = []
        
        if column_stats.count() == 0:
            return 100.0, issues
        
        stats = column_stats.select(
            F.avg("null_rate").alias("avg_null_rate"),
            F.max("null_rate").alias("max_null_rate")
        ).collect()[0]
        
        avg_null_rate = stats["avg_null_rate"] or 0
        max_null_rate = stats["max_null_rate"] or 0
        
        score = max(0, 100 - (avg_null_rate * 100))
        
        # Identify columns with high null rates
        if max_null_rate > self.config.null_rate_threshold:
            high_null_cols = column_stats.filter(
                F.col("null_rate") > self.config.null_rate_threshold
            ).select("column_name", "null_rate").collect()
            
            for col in high_null_cols[:5]:  # Limit reported issues
                issues.append(f"High null rate in {col.column_name}: {col.null_rate:.1%}")
        
        return score, issues
    
    def compute_uniqueness_score(self, column_stats: DataFrame, row_count: int) -> tuple:
        """
        Compute uniqueness score based on distinct value ratios.
        
        Considers the ratio of distinct values to total values.
        """
        issues = []
        
        if column_stats.count() == 0 or row_count == 0:
            return 100.0, issues
        
        # Calculate uniqueness ratio for each column
        stats = column_stats.withColumn(
            "uniqueness_ratio",
            F.col("distinct_count") / F.lit(row_count)
        ).select(F.avg("uniqueness_ratio").alias("avg_uniqueness")).collect()[0]
        
        avg_uniqueness = stats["avg_uniqueness"] or 0
        
        # Score based on having reasonable uniqueness
        # Very low uniqueness might indicate data issues
        score = min(100, avg_uniqueness * 100 * 2)  # Scale up since most columns won't be 100% unique
        
        # Check for potential duplicates (columns that should be unique but aren't)
        low_unique = column_stats.filter(
            (F.col("distinct_count") < row_count * 0.1) & 
            (F.col("distinct_count") > 1)
        ).count()
        
        if low_unique > column_stats.count() * 0.5:
            issues.append(f"Many columns have low uniqueness ({low_unique} columns)")
        
        return score, issues
    
    def compute_freshness_score(self, last_modified: Optional[datetime]) -> tuple:
        """
        Compute freshness score based on last modified time.
        
        Score decreases as data ages beyond threshold.
        """
        issues = []
        
        if last_modified is None:
            return 50.0, ["Could not determine last modification time"]
        
        now = datetime.utcnow()
        age_days = (now - last_modified).days
        
        if age_days <= self.config.freshness_threshold_days:
            score = 100.0
        else:
            # Linear decay after threshold
            decay = min(100, (age_days - self.config.freshness_threshold_days) * 5)
            score = max(0, 100 - decay)
            issues.append(f"Data is {age_days} days old (threshold: {self.config.freshness_threshold_days})")
        
        return score, issues
    
    def compute_consistency_score(self, column_stats: DataFrame) -> tuple:
        """
        Compute consistency score based on data patterns.
        
        Checks for anomalies like sudden changes in distinct counts.
        """
        issues = []
        score = 100.0
        
        if column_stats.count() == 0:
            return score, issues
        
        # Check for large drift in distinct counts
        drift_cols = column_stats.filter(
            F.abs(F.col("distinct_count_change_pct")) > 50
        ).select("column_name", "distinct_count_change_pct").collect()
        
        if drift_cols:
            drift_penalty = min(50, len(drift_cols) * 10)
            score = max(0, 100 - drift_penalty)
            for col in drift_cols[:3]:
                issues.append(
                    f"Large change in {col.column_name}: {col.distinct_count_change_pct:.1f}%"
                )
        
        return score, issues
    
    def score_table(self, snapshot_row) -> Dict[str, Any]:
        """Compute all quality scores for a table snapshot."""
        score_id = str(uuid.uuid4())
        table_name = snapshot_row.table_name
        snapshot_id = snapshot_row.snapshot_id
        row_count = snapshot_row.row_count or 0
        last_modified = snapshot_row.last_modified
        
        # Get column stats
        column_stats = self.get_column_stats_for_snapshot(snapshot_id)
        
        all_issues = []
        
        # Compute dimension scores
        completeness, comp_issues = self.compute_completeness_score(column_stats)
        all_issues.extend(comp_issues)
        
        uniqueness, uniq_issues = self.compute_uniqueness_score(column_stats, row_count)
        all_issues.extend(uniq_issues)
        
        freshness, fresh_issues = self.compute_freshness_score(last_modified)
        all_issues.extend(fresh_issues)
        
        consistency, cons_issues = self.compute_consistency_score(column_stats)
        all_issues.extend(cons_issues)
        
        # Calculate weighted overall score
        overall = (
            completeness * self.DIMENSION_WEIGHTS["completeness"] +
            uniqueness * self.DIMENSION_WEIGHTS["uniqueness"] +
            freshness * self.DIMENSION_WEIGHTS["freshness"] +
            consistency * self.DIMENSION_WEIGHTS["consistency"]
        )
        
        return {
            "score_id": score_id,
            "table_name": table_name,
            "snapshot_id": snapshot_id,
            "completeness_score": completeness,
            "uniqueness_score": uniqueness,
            "freshness_score": freshness,
            "consistency_score": consistency,
            "overall_score": overall,
            "quality_issues": all_issues
        }
    
    def write_scores(self, scores: List[Dict[str, Any]]) -> None:
        """Write quality scores to the table."""
        if not scores:
            return
        
        df = self.spark.createDataFrame(scores)
        df = df.withColumn("created_at", F.current_timestamp())
        df.write.mode("append").saveAsTable(self.config.fully_qualified_scores)
    
    def run(self) -> Dict[str, Any]:
        """Execute the data quality scoring pipeline."""
        logger.info("Starting data quality scoring")
        
        self.create_scores_table()
        
        snapshots = self.get_latest_snapshots()
        snapshot_list = snapshots.collect()
        
        logger.info(f"Scoring {len(snapshot_list)} tables")
        
        scores = []
        for snapshot_row in snapshot_list:
            try:
                score = self.score_table(snapshot_row)
                scores.append(score)
            except Exception as e:
                logger.warning(f"Could not score {snapshot_row.table_name}: {e}")
        
        self.write_scores(scores)
        
        # Summary statistics
        if scores:
            avg_overall = sum(s["overall_score"] for s in scores) / len(scores)
            low_quality = sum(1 for s in scores if s["overall_score"] < 70)
        else:
            avg_overall = 0
            low_quality = 0
        
        logger.info(f"Scoring complete. Avg score: {avg_overall:.1f}, Low quality tables: {low_quality}")
        
        return {
            "tables_scored": len(scores),
            "average_score": avg_overall,
            "low_quality_tables": low_quality
        }


def compute_data_quality(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str
) -> Dict[str, Any]:
    """
    Convenience function to compute data quality scores.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        
    Returns:
        Dict with execution statistics
    """
    config = DataQualityConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    scorer = DataQualityScorer(spark, config)
    return scorer.run()


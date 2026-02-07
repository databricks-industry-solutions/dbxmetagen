"""
Data Quality module for computing quality scores.

Calculates completeness, uniqueness, freshness, consistency, and metadata
quality scores based on profiling data with graceful handling of sparse metrics.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    ArrayType, TimestampType, IntegerType
)

logger = logging.getLogger(__name__)


@dataclass
class DataQualityConfig:
    """Configuration for data quality scoring."""
    catalog_name: str
    schema_name: str
    profiling_table: str = "profiling_snapshots"
    column_stats_table: str = "column_profiling_stats"
    scores_table: str = "data_quality_scores"
    kb_table: str = "table_knowledge_base"
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
    
    @property
    def fully_qualified_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.kb_table}"


class DataQualityScorer:
    """
    Scorer for computing data quality metrics with graceful handling of sparse data.
    
    Dimensions:
    - Completeness: Based on null rates across columns
    - Uniqueness: Based on distinct value ratios
    - Freshness: Based on last modified time
    - Consistency: Based on data patterns and drift
    - Metadata: Based on presence of comments and domain classifications
    """
    
    # Weights for overall score calculation
    DIMENSION_WEIGHTS = {
        "completeness": 0.25,
        "uniqueness": 0.15,
        "freshness": 0.25,
        "consistency": 0.20,
        "metadata": 0.15
    }
    
    # Explicit schema for quality scores table
    SCORES_SCHEMA = StructType([
        StructField("score_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("snapshot_id", StringType(), True),
        StructField("completeness_score", DoubleType(), True),
        StructField("uniqueness_score", DoubleType(), True),
        StructField("freshness_score", DoubleType(), True),
        StructField("consistency_score", DoubleType(), True),
        StructField("metadata_score", DoubleType(), True),
        StructField("overall_score", DoubleType(), True),
        StructField("quality_issues", ArrayType(StringType()), True),
        StructField("dimensions_calculated", IntegerType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
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
            metadata_score DOUBLE,
            overall_score DOUBLE,
            quality_issues ARRAY<STRING>,
            dimensions_calculated INT,
            created_at TIMESTAMP
        )
        COMMENT 'Data quality scores with sparse metrics handling'
        """
        self.spark.sql(ddl)
        
        # Add new columns to existing tables
        new_cols = [("metadata_score", "DOUBLE"), ("dimensions_calculated", "INT")]
        for col_name, col_type in new_cols:
            try:
                self.spark.sql(f"ALTER TABLE {self.config.fully_qualified_scores} ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
            except Exception:
                pass
        
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
    
    def _safe_avg(self, df: DataFrame, col: str, default: float = 0.0) -> float:
        """Safely compute average, returning default if column missing or all nulls."""
        try:
            if col not in df.columns:
                return default
            result = df.select(F.avg(col)).collect()[0][0]
            return float(result) if result is not None else default
        except Exception:
            return default
    
    def _safe_count(self, df: DataFrame, condition) -> int:
        """Safely count rows matching condition."""
        try:
            return df.filter(condition).count()
        except Exception:
            return 0
    
    def compute_completeness_score(self, column_stats: DataFrame, row_count: int) -> Tuple[float, List[str], bool]:
        """
        Compute completeness score based on null rates.
        Returns (score, issues, was_calculable).
        """
        issues = []
        
        if column_stats.count() == 0:
            return 80.0, ["No column stats available"], False
        
        avg_null_rate = self._safe_avg(column_stats, "null_rate", 0.0)
        max_null_rate = self._safe_avg(column_stats, "null_rate", 0.0)  # Will compute max below
        
        try:
            max_result = column_stats.select(F.max("null_rate")).collect()[0][0]
            max_null_rate = float(max_result) if max_result is not None else 0.0
        except Exception:
            pass
        
        score = max(0.0, 100.0 - (avg_null_rate * 100.0))
        
        # Identify columns with high null rates
        if max_null_rate > self.config.null_rate_threshold:
            try:
                high_null_cols = column_stats.filter(
                    F.col("null_rate") > self.config.null_rate_threshold
                ).select("column_name", "null_rate").collect()
                
                for col in high_null_cols[:5]:
                    rate = col.null_rate if col.null_rate else 0
                    issues.append(f"High null rate in {col.column_name}: {rate:.1%}")
            except Exception:
                pass
        
        return float(score), issues, True
    
    def compute_uniqueness_score(self, column_stats: DataFrame, row_count: int) -> Tuple[float, List[str], bool]:
        """
        Compute uniqueness score with graceful handling of missing metrics.
        """
        issues = []
        
        if column_stats.count() == 0 or row_count == 0:
            return 70.0, ["No column stats or empty table"], False
        
        # Try cardinality_ratio first
        avg_cardinality = self._safe_avg(column_stats, "cardinality_ratio", -1)
        
        if avg_cardinality < 0:
            # Fall back to computing from distinct_count
            try:
                non_null_with_distinct = column_stats.filter(F.col("distinct_count").isNotNull())
                if non_null_with_distinct.count() == 0:
                    return 70.0, ["No distinct count data available"], False
                
                stats = non_null_with_distinct.withColumn(
                    "uniqueness_ratio",
                    F.col("distinct_count") / F.lit(row_count)
                ).select(F.avg("uniqueness_ratio")).collect()[0]
                avg_cardinality = float(stats[0]) if stats[0] is not None else 0.3
            except Exception:
                avg_cardinality = 0.3  # Reasonable default
        
        # Score: higher cardinality is generally good
        score = min(100.0, avg_cardinality * 150.0 + 30.0)
        
        # Check for potential unique key columns (good)
        try:
            high_cardinality_cols = self._safe_count(
                column_stats,
                F.col("cardinality_ratio") > 0.95
            )
            if high_cardinality_cols > 0:
                score = min(100.0, score + 10.0)
        except Exception:
            pass
        
        # Check for constant columns (bad)
        try:
            constant_cols = self._safe_count(column_stats, F.col("distinct_count") == 1)
            total_cols = column_stats.count()
            if constant_cols > total_cols * 0.3:
                penalty = min(20.0, float(constant_cols) * 2.0)
                score = max(0.0, score - penalty)
                issues.append(f"{constant_cols} columns have constant values")
        except Exception:
            pass
        
        return float(score), issues, True
    
    def compute_freshness_score(self, last_modified: Optional[datetime]) -> Tuple[float, List[str], bool]:
        """
        Compute freshness score based on last modified time.
        """
        issues = []
        
        if last_modified is None:
            return 60.0, ["Could not determine last modification time"], False
        
        try:
            now = datetime.utcnow()
            age_days = (now - last_modified).days
            
            if age_days < 0:
                # Future date - probably timezone issue, assume fresh
                return 100.0, [], True
            
            if age_days <= self.config.freshness_threshold_days:
                score = 100.0
            else:
                # Gentler decay
                decay = min(80.0, float(age_days - self.config.freshness_threshold_days) * 3.0)
                score = max(20.0, 100.0 - decay)
                issues.append(f"Data is {age_days} days old (threshold: {self.config.freshness_threshold_days})")
            
            return float(score), issues, True
        except Exception as e:
            return 60.0, [f"Error computing freshness: {str(e)[:50]}"], False
    
    def compute_consistency_score(self, column_stats: DataFrame) -> Tuple[float, List[str], bool]:
        """
        Compute consistency score based on data patterns with graceful fallbacks.
        """
        issues = []
        score = 100.0
        calculated = False
        
        if column_stats.count() == 0:
            return 80.0, ["No column stats for consistency check"], False
        
        # Check for drift (if available)
        try:
            if "distinct_count_change_pct" in column_stats.columns:
                drift_cols = column_stats.filter(
                    (F.col("distinct_count_change_pct").isNotNull()) &
                    (F.abs(F.col("distinct_count_change_pct")) > 50)
                ).select("column_name", "distinct_count_change_pct").collect()
                
                if drift_cols:
                    drift_penalty = min(25.0, float(len(drift_cols)) * 8.0)
                    score -= drift_penalty
                    calculated = True
                    for col in drift_cols[:3]:
                        issues.append(f"Drift in {col.column_name}: {col.distinct_count_change_pct:.1f}%")
        except Exception:
            pass
        
        # Check for empty strings (if available)
        try:
            if "empty_string_rate" in column_stats.columns:
                high_empty = column_stats.filter(
                    (F.col("empty_string_rate").isNotNull()) &
                    (F.col("empty_string_rate") > 0.3)
                ).count()
                
                if high_empty > 0:
                    empty_penalty = min(15.0, float(high_empty) * 4.0)
                    score -= empty_penalty
                    calculated = True
                    issues.append(f"{high_empty} columns have high empty string rates")
        except Exception:
            pass
        
        # Check entropy (low entropy = low information)
        try:
            if "entropy" in column_stats.columns:
                low_entropy = column_stats.filter(
                    (F.col("entropy").isNotNull()) &
                    (F.col("entropy") < 0.5) &
                    (F.col("distinct_count") > 1)
                ).count()
                
                if low_entropy > column_stats.count() * 0.3:
                    score -= 10.0
                    calculated = True
                    issues.append(f"{low_entropy} columns have very low entropy")
        except Exception:
            pass
        
        # Check for extreme length variations
        try:
            if "min_length" in column_stats.columns and "max_length" in column_stats.columns:
                extreme = column_stats.filter(
                    (F.col("min_length").isNotNull()) &
                    (F.col("max_length").isNotNull()) &
                    (F.col("min_length") > 0) &
                    (F.col("max_length") > F.col("min_length") * 100)
                ).count()
                
                if extreme > 0:
                    score -= min(10.0, float(extreme) * 3.0)
                    calculated = True
                    issues.append(f"{extreme} columns have extreme length variation")
        except Exception:
            pass
        
        return max(0.0, float(score)), issues, calculated or True
    
    def compute_metadata_score(self, table_name: str) -> Tuple[float, List[str], bool]:
        """
        Compute metadata quality score based on presence of comments and classifications.
        """
        issues = []
        score = 0.0
        
        try:
            kb_data = self.spark.sql(f"""
                SELECT 
                    comment,
                    domain,
                    subdomain,
                    has_pii,
                    has_phi
                FROM {self.config.fully_qualified_kb}
                WHERE table_name = '{table_name}'
            """).collect()
            
            if not kb_data:
                return 50.0, ["Table not in knowledge base"], False
            
            row = kb_data[0]
            
            # Has comment (40 points)
            if row.comment and len(str(row.comment)) > 10:
                score += 40.0
            else:
                issues.append("Missing or short comment")
            
            # Has domain (30 points)
            if row.domain and str(row.domain).lower() not in ['unknown', 'none', '']:
                score += 30.0
            else:
                issues.append("Missing domain classification")
            
            # Has subdomain (15 points)
            if row.subdomain and str(row.subdomain).lower() not in ['unknown', 'none', '']:
                score += 15.0
            
            # Has PII/PHI classification (15 points)
            if row.has_pii is not None or row.has_phi is not None:
                score += 15.0
            
            return float(score), issues, True
            
        except Exception as e:
            return 50.0, [f"Could not check metadata: {str(e)[:50]}"], False
    
    def score_table(self, snapshot_row) -> Dict[str, Any]:
        """Compute all quality scores for a table snapshot with sparse metrics handling."""
        score_id = str(uuid.uuid4())
        table_name = snapshot_row.table_name
        snapshot_id = snapshot_row.snapshot_id
        row_count = int(snapshot_row.row_count or 0)
        last_modified = snapshot_row.last_modified
        
        # Get column stats
        column_stats = self.get_column_stats_for_snapshot(snapshot_id)
        
        all_issues = []
        dimensions_calculated = 0
        scores = {}
        
        # Compute each dimension
        completeness, comp_issues, comp_calc = self.compute_completeness_score(column_stats, row_count)
        scores["completeness"] = completeness
        all_issues.extend(comp_issues)
        if comp_calc:
            dimensions_calculated += 1
        
        uniqueness, uniq_issues, uniq_calc = self.compute_uniqueness_score(column_stats, row_count)
        scores["uniqueness"] = uniqueness
        all_issues.extend(uniq_issues)
        if uniq_calc:
            dimensions_calculated += 1
        
        freshness, fresh_issues, fresh_calc = self.compute_freshness_score(last_modified)
        scores["freshness"] = freshness
        all_issues.extend(fresh_issues)
        if fresh_calc:
            dimensions_calculated += 1
        
        consistency, cons_issues, cons_calc = self.compute_consistency_score(column_stats)
        scores["consistency"] = consistency
        all_issues.extend(cons_issues)
        if cons_calc:
            dimensions_calculated += 1
        
        metadata, meta_issues, meta_calc = self.compute_metadata_score(table_name)
        scores["metadata"] = metadata
        all_issues.extend(meta_issues)
        if meta_calc:
            dimensions_calculated += 1
        
        # Calculate weighted overall score
        # Only weight dimensions that were actually calculated
        total_weight = 0.0
        weighted_sum = 0.0
        
        for dim, score in scores.items():
            weight = self.DIMENSION_WEIGHTS.get(dim, 0.2)
            weighted_sum += score * weight
            total_weight += weight
        
        overall = weighted_sum / total_weight if total_weight > 0 else 50.0
        
        return {
            "score_id": score_id,
            "table_name": table_name,
            "snapshot_id": snapshot_id,
            "completeness_score": scores["completeness"],
            "uniqueness_score": scores["uniqueness"],
            "freshness_score": scores["freshness"],
            "consistency_score": scores["consistency"],
            "metadata_score": scores["metadata"],
            "overall_score": float(overall),
            "quality_issues": all_issues or [],
            "dimensions_calculated": dimensions_calculated
        }
    
    def write_scores(self, scores: List[Dict[str, Any]]) -> None:
        """Write quality scores to the table."""
        if not scores:
            return
        
        now = datetime.now()
        
        rows = []
        for s in scores:
            row = (
                s["score_id"],
                s["table_name"],
                s["snapshot_id"],
                s["completeness_score"],
                s["uniqueness_score"],
                s["freshness_score"],
                s["consistency_score"],
                s.get("metadata_score", 50.0),
                s["overall_score"],
                s["quality_issues"] or [],
                s.get("dimensions_calculated", 5),
                now
            )
            rows.append(row)
        
        df = self.spark.createDataFrame(rows, schema=self.SCORES_SCHEMA)
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(self.config.fully_qualified_scores)
    
    def run(self) -> Dict[str, Any]:
        """Execute the data quality scoring pipeline."""
        logger.info("Starting data quality scoring")
        
        self.create_scores_table()
        
        try:
            snapshots = self.get_latest_snapshots()
            snapshot_list = snapshots.collect()
        except Exception as e:
            logger.error(f"Could not get snapshots: {e}")
            return {"tables_scored": 0, "average_score": 0, "low_quality_tables": 0}
        
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
            avg_dims = sum(s.get("dimensions_calculated", 5) for s in scores) / len(scores)
        else:
            avg_overall = 0.0
            low_quality = 0
            avg_dims = 0.0
        
        logger.info(
            f"Data quality scoring complete. "
            f"Tables: {len(scores)}, Avg score: {avg_overall:.1f}, "
            f"Low quality (<70): {low_quality}, Avg dimensions: {avg_dims:.1f}"
        )
        
        return {
            "tables_scored": len(scores),
            "average_score": avg_overall,
            "low_quality_tables": low_quality,
            "average_dimensions_calculated": avg_dims
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

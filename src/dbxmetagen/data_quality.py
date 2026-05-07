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

import pandas as pd
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
        from dbxmetagen.processing import add_column_if_not_exists
        new_cols = [("metadata_score", "DOUBLE"), ("dimensions_calculated", "INT")]
        for col_name, col_type in new_cols:
            add_column_if_not_exists(self.spark, self.config.fully_qualified_scores, col_name, col_type)
        
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
    
    # ------------------------------------------------------------------
    # Pandas-based scoring (one toPandas() per table, zero extra Spark actions)
    # ------------------------------------------------------------------

    @staticmethod
    def _pd_safe_mean(df: pd.DataFrame, col: str, default: float = 0.0) -> float:
        if col not in df.columns:
            return default
        vals = df[col].dropna()
        return float(vals.mean()) if len(vals) > 0 else default

    def compute_completeness_score(self, cs: pd.DataFrame, row_count: int) -> Tuple[float, List[str], bool]:
        issues: List[str] = []
        if cs.empty:
            return 80.0, ["No column stats available"], False

        avg_null = self._pd_safe_mean(cs, "null_rate")
        score = max(0.0, 100.0 - avg_null * 100.0)

        if "null_rate" in cs.columns:
            high = cs[cs["null_rate"] > self.config.null_rate_threshold]
            for _, r in high.head(5).iterrows():
                rate = r.get("null_rate", 0) or 0
                issues.append(f"High null rate in {r.get('column_name', '?')}: {rate:.1%}")

        return float(score), issues, True

    def compute_uniqueness_score(self, cs: pd.DataFrame, row_count: int) -> Tuple[float, List[str], bool]:
        issues: List[str] = []
        if cs.empty or row_count == 0:
            return 70.0, ["No column stats or empty table"], False

        avg_card = self._pd_safe_mean(cs, "cardinality_ratio", -1)
        if avg_card < 0:
            dc = cs["distinct_count"].dropna() if "distinct_count" in cs.columns else pd.Series(dtype=float)
            if dc.empty:
                return 70.0, ["No distinct count data available"], False
            avg_card = float((dc / row_count).mean())

        score = min(100.0, avg_card * 150.0 + 30.0)

        if "cardinality_ratio" in cs.columns and (cs["cardinality_ratio"] > 0.95).any():
            score = min(100.0, score + 10.0)

        if "distinct_count" in cs.columns:
            constant = int((cs["distinct_count"] == 1).sum())
            if constant > len(cs) * 0.3:
                score = max(0.0, score - min(20.0, float(constant) * 2.0))
                issues.append(f"{constant} columns have constant values")

        return float(score), issues, True

    def compute_freshness_score(self, last_modified: Optional[datetime]) -> Tuple[float, List[str], bool]:
        issues: List[str] = []
        if last_modified is None:
            return 60.0, ["Could not determine last modification time"], False
        try:
            age_days = (datetime.utcnow() - last_modified).days
            if age_days < 0:
                return 100.0, [], True
            if age_days <= self.config.freshness_threshold_days:
                return 100.0, [], True
            decay = min(80.0, float(age_days - self.config.freshness_threshold_days) * 3.0)
            score = max(20.0, 100.0 - decay)
            issues.append(f"Data is {age_days} days old (threshold: {self.config.freshness_threshold_days})")
            return float(score), issues, True
        except Exception as e:
            return 60.0, [f"Error computing freshness: {str(e)[:50]}"], False

    def compute_consistency_score(self, cs: pd.DataFrame) -> Tuple[float, List[str], bool]:
        issues: List[str] = []
        score = 100.0
        calculated = False
        if cs.empty:
            return 80.0, ["No column stats for consistency check"], False

        if "distinct_count_change_pct" in cs.columns:
            drift = cs[cs["distinct_count_change_pct"].notna() & (cs["distinct_count_change_pct"].abs() > 50)]
            if not drift.empty:
                score -= min(25.0, float(len(drift)) * 8.0)
                calculated = True
                for _, r in drift.head(3).iterrows():
                    issues.append(f"Drift in {r.get('column_name', '?')}: {r['distinct_count_change_pct']:.1f}%")

        if "empty_string_rate" in cs.columns:
            high_empty = int((cs["empty_string_rate"].fillna(0) > 0.3).sum())
            if high_empty > 0:
                score -= min(15.0, float(high_empty) * 4.0)
                calculated = True
                issues.append(f"{high_empty} columns have high empty string rates")

        if "entropy" in cs.columns and "distinct_count" in cs.columns:
            low_ent = cs[cs["entropy"].notna() & (cs["entropy"] < 0.5) & (cs["distinct_count"] > 1)]
            if len(low_ent) > len(cs) * 0.3:
                score -= 10.0
                calculated = True
                issues.append(f"{len(low_ent)} columns have very low entropy")

        if "min_length" in cs.columns and "max_length" in cs.columns:
            extreme = cs[
                cs["min_length"].notna() & cs["max_length"].notna()
                & (cs["min_length"] > 0) & (cs["max_length"] > cs["min_length"] * 100)
            ]
            if not extreme.empty:
                score -= min(10.0, float(len(extreme)) * 3.0)
                calculated = True
                issues.append(f"{len(extreme)} columns have extreme length variation")

        return max(0.0, float(score)), issues, calculated or True

    def compute_metadata_score(self, table_name: str) -> Tuple[float, List[str], bool]:
        issues: List[str] = []
        score = 0.0
        try:
            kb_data = self.spark.sql(f"""
                SELECT comment, domain, subdomain, has_pii, has_phi
                FROM {self.config.fully_qualified_kb}
                WHERE table_name = '{table_name}'
            """).collect()
            if not kb_data:
                return 50.0, ["Table not in knowledge base"], False
            row = kb_data[0]
            if row.comment and len(str(row.comment)) > 10:
                score += 40.0
            else:
                issues.append("Missing or short comment")
            if row.domain and str(row.domain).lower() not in ['unknown', 'none', '']:
                score += 30.0
            else:
                issues.append("Missing domain classification")
            if row.subdomain and str(row.subdomain).lower() not in ['unknown', 'none', '']:
                score += 15.0
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

        cs = self.get_column_stats_for_snapshot(snapshot_id).toPandas()

        all_issues: List[str] = []
        dimensions_calculated = 0
        scores: Dict[str, float] = {}

        completeness, comp_issues, comp_calc = self.compute_completeness_score(cs, row_count)
        scores["completeness"] = completeness
        all_issues.extend(comp_issues)
        if comp_calc:
            dimensions_calculated += 1

        uniqueness, uniq_issues, uniq_calc = self.compute_uniqueness_score(cs, row_count)
        scores["uniqueness"] = uniqueness
        all_issues.extend(uniq_issues)
        if uniq_calc:
            dimensions_calculated += 1

        freshness, fresh_issues, fresh_calc = self.compute_freshness_score(last_modified)
        scores["freshness"] = freshness
        all_issues.extend(fresh_issues)
        if fresh_calc:
            dimensions_calculated += 1

        consistency, cons_issues, cons_calc = self.compute_consistency_score(cs)
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
        # APPEND: Delta append into fully_qualified_scores with mergeSchema=true; adds score_id PK rows plus table_name,
        # snapshot_id FK to profiling snapshots, per-dimension scores (completeness through metadata), overall_score,
        # quality_issues array, dimensions_calculated count, created_at timestamp for this batch write.
        # WHY: Builds a longitudinal quality ledger so dashboards and downstream gates can compare tables and runs
        # without overwriting prior scoring epochs tied to snapshot_id.
        # TRADEOFFS: Append-only preserves history and simplifies idempotent re-runs versus MERGE snapshots (avoids
        # complex keys) but grows storage and requires consumers to pick latest snapshot/time; mergeSchema tolerates
        # additive schema drift at the cost of weaker compile-time contract enforcement.
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

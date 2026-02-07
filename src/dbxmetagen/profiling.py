"""
Profiling module for capturing table and column statistics.

Generates profiling snapshots with row counts, column statistics,
pattern detection, and drift metrics for anomaly analysis.
"""

import logging
import math
import re
import json
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import uuid
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, StringType as SparkStringType, BooleanType as SparkBooleanType,
    DateType, TimestampType as SparkTimestampType, ArrayType as SparkArrayType,
    MapType as SparkMapType, StructType, StructField, StringType, LongType, 
    DoubleType, IntegerType, TimestampType, MapType, BooleanType
)

logger = logging.getLogger(__name__)


@dataclass 
class ProfilingConfig:
    """Configuration for profiling ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    snapshots_table: str = "profiling_snapshots"
    column_stats_table: str = "column_profiling_stats"
    
    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_snapshots(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.snapshots_table}"
    
    @property
    def fully_qualified_column_stats(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_stats_table}"


class ProfilingBuilder:
    """
    Builder for generating profiling snapshots and column statistics.
    
    Captures point-in-time statistics for drift detection and anomaly analysis.
    Now includes universal metrics, pattern detection, and frequency analysis.
    """
    
    # Pattern regexes for string analysis
    UUID_PATTERN = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    DATE_PATTERN = re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}')
    NUMERIC_ID_PATTERN = re.compile(r'^\d+$')
    
    # Explicit schema for snapshots table
    SNAPSHOT_SCHEMA = StructType([
        StructField("snapshot_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("snapshot_time", TimestampType(), True),
        StructField("row_count", LongType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("column_stats", MapType(StringType(), StringType()), True),
        StructField("table_size_bytes", LongType(), True),
        StructField("num_files", LongType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
    # Explicit schema for column stats table - extended with new fields
    COLUMN_STATS_SCHEMA = StructType([
        StructField("stat_id", StringType(), False),
        StructField("snapshot_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("null_count", LongType(), True),
        StructField("null_rate", DoubleType(), True),
        StructField("distinct_count", LongType(), True),
        StructField("cardinality_ratio", DoubleType(), True),
        StructField("is_unique_candidate", BooleanType(), True),
        StructField("min_value", StringType(), True),
        StructField("max_value", StringType(), True),
        StructField("mean_value", DoubleType(), True),
        StructField("stddev_value", DoubleType(), True),
        StructField("percentiles", MapType(StringType(), DoubleType()), True),
        StructField("min_length", IntegerType(), True),
        StructField("max_length", IntegerType(), True),
        StructField("avg_length", DoubleType(), True),
        StructField("empty_string_count", LongType(), True),
        StructField("empty_string_rate", DoubleType(), True),
        StructField("mode_value", StringType(), True),
        StructField("mode_frequency", LongType(), True),
        StructField("entropy", DoubleType(), True),
        StructField("sample_values", StringType(), True),
        StructField("value_distribution", StringType(), True),
        StructField("pattern_detected", StringType(), True),
        StructField("has_numeric_stats", BooleanType(), True),
        StructField("has_string_stats", BooleanType(), True),
        StructField("previous_distinct_count", LongType(), True),
        StructField("distinct_count_change_pct", DoubleType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
    def __init__(self, spark: SparkSession, config: ProfilingConfig):
        self.spark = spark
        self.config = config
        # Track profiling stats for summary
        self._stats = {"numeric_cols": 0, "string_cols": 0, "other_cols": 0, "total_cols": 0}
    
    def create_snapshots_table(self) -> None:
        """Create the profiling snapshots table."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_snapshots} (
            snapshot_id STRING NOT NULL,
            table_name STRING NOT NULL,
            snapshot_time TIMESTAMP,
            row_count BIGINT,
            column_count INT,
            column_stats MAP<STRING, STRING>,
            table_size_bytes BIGINT,
            num_files BIGINT,
            last_modified TIMESTAMP,
            created_at TIMESTAMP
        )
        COMMENT 'Point-in-time profiling snapshots for tables'
        """
        self.spark.sql(ddl)
        
        # Add column_count if missing
        try:
            self.spark.sql(f"ALTER TABLE {self.config.fully_qualified_snapshots} ADD COLUMN IF NOT EXISTS column_count INT")
        except Exception:
            pass
        
        logger.info(f"Snapshots table {self.config.fully_qualified_snapshots} ready")
    
    def create_column_stats_table(self) -> None:
        """Create the column profiling stats table with all new fields."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_column_stats} (
            stat_id STRING NOT NULL,
            snapshot_id STRING,
            table_name STRING,
            column_name STRING,
            data_type STRING,
            null_count BIGINT,
            null_rate DOUBLE,
            distinct_count BIGINT,
            cardinality_ratio DOUBLE,
            is_unique_candidate BOOLEAN,
            min_value STRING,
            max_value STRING,
            mean_value DOUBLE,
            stddev_value DOUBLE,
            percentiles MAP<STRING, DOUBLE>,
            min_length INT,
            max_length INT,
            avg_length DOUBLE,
            empty_string_count BIGINT,
            empty_string_rate DOUBLE,
            mode_value STRING,
            mode_frequency BIGINT,
            entropy DOUBLE,
            sample_values STRING,
            value_distribution STRING,
            pattern_detected STRING,
            has_numeric_stats BOOLEAN,
            has_string_stats BOOLEAN,
            previous_distinct_count BIGINT,
            distinct_count_change_pct DOUBLE,
            created_at TIMESTAMP
        )
        COMMENT 'Column-level profiling statistics with pattern detection and frequency analysis'
        """
        self.spark.sql(ddl)
        
        # Add new columns to existing tables (for migration)
        new_columns = [
            ("data_type", "STRING"),
            ("is_unique_candidate", "BOOLEAN"),
            ("mode_value", "STRING"),
            ("mode_frequency", "BIGINT"),
            ("entropy", "DOUBLE"),
            ("sample_values", "STRING"),
            ("value_distribution", "STRING"),
            ("pattern_detected", "STRING"),
            ("has_numeric_stats", "BOOLEAN"),
            ("has_string_stats", "BOOLEAN"),
            ("cardinality_ratio", "DOUBLE"),
            ("empty_string_count", "BIGINT"),
            ("empty_string_rate", "DOUBLE")
        ]
        for col_name, col_type in new_columns:
            try:
                self.spark.sql(f"ALTER TABLE {self.config.fully_qualified_column_stats} ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
            except Exception:
                pass
        
        logger.info(f"Column stats table {self.config.fully_qualified_column_stats} ready")
    
    def get_tables_to_profile(self) -> List[str]:
        """Get list of tables from knowledge base to profile."""
        df = self.spark.sql(f"""
            SELECT DISTINCT table_name 
            FROM {self.config.fully_qualified_source}
            WHERE table_name IS NOT NULL
        """)
        return [row.table_name for row in df.collect()]
    
    def _get_data_type_string(self, dtype) -> str:
        """Convert Spark data type to readable string."""
        type_name = type(dtype).__name__
        if isinstance(dtype, SparkArrayType):
            return f"array<{self._get_data_type_string(dtype.elementType)}>"
        elif isinstance(dtype, SparkMapType):
            return f"map<{self._get_data_type_string(dtype.keyType)},{self._get_data_type_string(dtype.valueType)}>"
        elif isinstance(dtype, StructType):
            return "struct"
        else:
            return type_name.replace("Type", "").lower()
    
    def _compute_entropy(self, value_counts: Dict[str, int], total: int) -> float:
        """Compute Shannon entropy for a distribution."""
        if total <= 0:
            return 0.0
        entropy = 0.0
        for count in value_counts.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        return entropy
    
    def _detect_pattern(self, sample_values: List[str]) -> str:
        """Detect common patterns in string values."""
        if not sample_values:
            return "unknown"
        
        patterns = {"uuid": 0, "email": 0, "date": 0, "numeric_id": 0, "other": 0}
        
        for val in sample_values:
            if val is None:
                continue
            val_str = str(val).strip()
            if self.UUID_PATTERN.match(val_str):
                patterns["uuid"] += 1
            elif self.EMAIL_PATTERN.match(val_str):
                patterns["email"] += 1
            elif self.DATE_PATTERN.match(val_str):
                patterns["date"] += 1
            elif self.NUMERIC_ID_PATTERN.match(val_str):
                patterns["numeric_id"] += 1
            else:
                patterns["other"] += 1
        
        # Return the dominant pattern (>50% of samples)
        total = len([v for v in sample_values if v is not None])
        if total == 0:
            return "empty"
        
        for pattern, count in patterns.items():
            if count > total * 0.5:
                return pattern
        
        return "mixed"
    
    def profile_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Generate profiling snapshot for a single table."""
        try:
            snapshot_id = str(uuid.uuid4())
            df = self.spark.table(table_name)
            row_count = df.count()
            column_count = len(df.columns)
            
            # Get table details if Delta
            table_size = None
            num_files = None
            last_modified = None
            
            try:
                detail_df = self.spark.sql(f"DESCRIBE DETAIL {table_name}")
                detail = detail_df.collect()[0]
                table_size = getattr(detail, 'sizeInBytes', None)
                num_files = getattr(detail, 'numFiles', None)
                last_modified = getattr(detail, 'lastModified', None)
            except Exception as e:
                logger.debug(f"Could not get table details for {table_name}: {e}")
            
            # Profile columns
            column_stats = {}
            column_stat_records = []
            
            for col_name in df.columns[:50]:  # Limit columns for performance
                try:
                    col_stats = self._profile_column(df, col_name, snapshot_id, table_name, row_count)
                    if col_stats:
                        # Create summary string for column_stats map
                        summary = f"dist:{col_stats.get('distinct_count', 0)},null:{col_stats.get('null_rate', 0):.2f}"
                        column_stats[col_name] = summary
                        column_stat_records.append(col_stats)
                except Exception as e:
                    logger.debug(f"Could not profile column {col_name}: {e}")
            
            return {
                "snapshot_id": snapshot_id,
                "table_name": table_name,
                "row_count": row_count,
                "column_count": column_count,
                "column_stats": column_stats,
                "table_size_bytes": table_size,
                "num_files": num_files,
                "last_modified": last_modified,
                "column_stat_records": column_stat_records
            }
            
        except Exception as e:
            logger.warning(f"Could not profile table {table_name}: {e}")
            return None
    
    def _profile_column(
        self, 
        df: DataFrame, 
        col_name: str, 
        snapshot_id: str,
        table_name: str,
        row_count: int
    ) -> Dict[str, Any]:
        """Profile a single column with universal and type-specific metrics."""
        stat_id = str(uuid.uuid4())
        col = F.col(f"`{col_name}`")
        col_type = df.schema[col_name].dataType
        data_type_str = self._get_data_type_string(col_type)
        
        # Track column type for summary
        self._stats["total_cols"] += 1
        
        # === UNIVERSAL METRICS (computed for ALL columns) ===
        basic_stats = df.select(
            F.count(col).alias("non_null_count"),
            F.sum(F.when(col.isNull(), 1).otherwise(0)).alias("null_count"),
            F.approx_count_distinct(col).alias("distinct_count")
        ).collect()[0]
        
        null_count = int(basic_stats["null_count"] or 0)
        non_null_count = int(basic_stats["non_null_count"] or 0)
        distinct_count = int(basic_stats["distinct_count"] or 0)
        
        # Null rate
        null_rate = float(null_count / row_count) if row_count > 0 else 0.0
        
        # Cardinality ratio
        cardinality_ratio = float(distinct_count / non_null_count) if non_null_count > 0 else 0.0
        
        # Is unique candidate (high cardinality, low nulls)
        is_unique = (cardinality_ratio > 0.95 and null_rate < 0.01 and row_count > 10)
        
        # Initialize result with sensible defaults (not NULL)
        result = {
            "stat_id": stat_id,
            "snapshot_id": snapshot_id,
            "table_name": table_name,
            "column_name": col_name,
            "data_type": data_type_str,
            "null_count": null_count,
            "null_rate": null_rate,
            "distinct_count": distinct_count,
            "cardinality_ratio": cardinality_ratio,
            "is_unique_candidate": is_unique,
            "min_value": "",  # Empty string instead of None
            "max_value": "",
            "mean_value": 0.0,
            "stddev_value": 0.0,
            "percentiles": {},
            "min_length": 0,
            "max_length": 0,
            "avg_length": 0.0,
            "empty_string_count": 0,
            "empty_string_rate": 0.0,
            "mode_value": "",
            "mode_frequency": 0,
            "entropy": 0.0,
            "sample_values": "[]",
            "value_distribution": "{}",
            "pattern_detected": "unknown",
            "has_numeric_stats": False,
            "has_string_stats": False,
            "previous_distinct_count": None,
            "distinct_count_change_pct": None
        }
        
        # === SAMPLE VALUES (for all column types) ===
        try:
            sample_rows = df.select(col.cast("string")).filter(col.isNotNull()).limit(100).collect()
            sample_values = [row[0] for row in sample_rows[:5] if row[0] is not None]
            result["sample_values"] = json.dumps(sample_values[:5])
        except Exception:
            sample_values = []
        
        # === MODE and FREQUENCY (for low-cardinality columns) ===
        if distinct_count > 0 and distinct_count <= 1000:
            try:
                mode_df = df.groupBy(col.cast("string").alias("val")).count().orderBy(F.desc("count")).limit(10)
                mode_rows = mode_df.collect()
                
                if mode_rows:
                    result["mode_value"] = str(mode_rows[0]["val"])[:100] if mode_rows[0]["val"] else ""
                    result["mode_frequency"] = int(mode_rows[0]["count"])
                    
                    # Value distribution (top 10)
                    dist = {str(r["val"])[:50]: int(r["count"]) for r in mode_rows}
                    result["value_distribution"] = json.dumps(dist)
                    
                    # Entropy
                    value_counts = {str(r["val"]): int(r["count"]) for r in mode_rows}
                    result["entropy"] = round(self._compute_entropy(value_counts, non_null_count), 4)
            except Exception as e:
                logger.debug(f"Could not compute mode for {col_name}: {e}")
        
        # === TYPE-SPECIFIC METRICS ===
        
        # Numeric stats
        if isinstance(col_type, NumericType):
            self._stats["numeric_cols"] += 1
            result["has_numeric_stats"] = True
            try:
                numeric_stats = df.select(
                    F.min(col).cast("string").alias("min_value"),
                    F.max(col).cast("string").alias("max_value"),
                    F.mean(col).alias("mean_value"),
                    F.stddev(col).alias("stddev_value")
                ).collect()[0]
                
                result["min_value"] = numeric_stats["min_value"] or ""
                result["max_value"] = numeric_stats["max_value"] or ""
                mean_val = numeric_stats["mean_value"]
                result["mean_value"] = float(mean_val) if mean_val is not None else 0.0
                stddev_val = numeric_stats["stddev_value"]
                result["stddev_value"] = float(stddev_val) if stddev_val is not None else 0.0
                
                # Percentiles
                try:
                    percentile_values = df.stat.approxQuantile(col_name, [0.25, 0.5, 0.75, 0.95, 0.99], 0.01)
                    if percentile_values and len(percentile_values) == 5:
                        result["percentiles"] = {
                            "p25": float(percentile_values[0]),
                            "p50": float(percentile_values[1]),
                            "p75": float(percentile_values[2]),
                            "p95": float(percentile_values[3]),
                            "p99": float(percentile_values[4])
                        }
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"Could not compute numeric stats for {col_name}: {e}")
        
        # String stats
        elif isinstance(col_type, SparkStringType):
            self._stats["string_cols"] += 1
            result["has_string_stats"] = True
            try:
                string_stats = df.select(
                    F.min(F.length(col)).alias("min_length"),
                    F.max(F.length(col)).alias("max_length"),
                    F.avg(F.length(col)).alias("avg_length"),
                    F.min(col).alias("min_value"),
                    F.max(col).alias("max_value"),
                    F.sum(F.when((col == "") | (F.trim(col) == ""), 1).otherwise(0)).alias("empty_count")
                ).collect()[0]
                
                min_len = string_stats["min_length"]
                result["min_length"] = int(min_len) if min_len is not None else 0
                max_len = string_stats["max_length"]
                result["max_length"] = int(max_len) if max_len is not None else 0
                avg_len = string_stats["avg_length"]
                result["avg_length"] = float(avg_len) if avg_len is not None else 0.0
                result["min_value"] = str(string_stats["min_value"])[:100] if string_stats["min_value"] else ""
                result["max_value"] = str(string_stats["max_value"])[:100] if string_stats["max_value"] else ""
                
                # Empty string metrics
                empty_count = int(string_stats["empty_count"] or 0)
                result["empty_string_count"] = empty_count
                result["empty_string_rate"] = float(empty_count / row_count) if row_count > 0 else 0.0
                
                # Pattern detection
                result["pattern_detected"] = self._detect_pattern(sample_values)
                
            except Exception as e:
                logger.debug(f"Could not compute string stats for {col_name}: {e}")
        
        # Boolean stats
        elif isinstance(col_type, SparkBooleanType):
            self._stats["other_cols"] += 1
            try:
                bool_stats = df.groupBy(col).count().collect()
                dist = {str(r[0]): int(r[1]) for r in bool_stats}
                result["value_distribution"] = json.dumps(dist)
                result["pattern_detected"] = "boolean"
            except Exception:
                pass
        
        # Date/Timestamp stats
        elif isinstance(col_type, (DateType, SparkTimestampType)):
            self._stats["other_cols"] += 1
            try:
                date_stats = df.select(
                    F.min(col).cast("string").alias("min_value"),
                    F.max(col).cast("string").alias("max_value")
                ).collect()[0]
                result["min_value"] = date_stats["min_value"] or ""
                result["max_value"] = date_stats["max_value"] or ""
                result["pattern_detected"] = "datetime"
            except Exception:
                pass
        else:
            self._stats["other_cols"] += 1
        
        # === DRIFT DETECTION ===
        try:
            prev_stats = self.spark.sql(f"""
                SELECT distinct_count
                FROM {self.config.fully_qualified_column_stats}
                WHERE table_name = '{table_name}' AND column_name = '{col_name}'
                ORDER BY created_at DESC
                LIMIT 1
            """).collect()
            
            if prev_stats and prev_stats[0]["distinct_count"] is not None:
                prev_distinct = int(prev_stats[0]["distinct_count"])
                result["previous_distinct_count"] = prev_distinct
                if prev_distinct > 0:
                    result["distinct_count_change_pct"] = float(
                        (distinct_count - prev_distinct) / prev_distinct * 100
                    )
        except Exception:
            pass
        
        return result
    
    def write_snapshot(self, snapshot_data: Dict[str, Any]) -> None:
        """Write snapshot and column stats to tables."""
        now = datetime.now()
        
        # Cast values to match table schema types
        num_files = snapshot_data["num_files"]
        num_files = int(num_files) if num_files is not None else None
        table_size = snapshot_data["table_size_bytes"]
        table_size = int(table_size) if table_size is not None else None
        row_count = snapshot_data["row_count"]
        row_count = int(row_count) if row_count is not None else None
        column_count = snapshot_data.get("column_count")
        column_count = int(column_count) if column_count is not None else None
        
        # Ensure column_stats is a dict (not None)
        column_stats = snapshot_data.get("column_stats") or {}
        
        # Write snapshot record with explicit schema
        snapshot_row = (
            snapshot_data["snapshot_id"],
            snapshot_data["table_name"],
            now,
            row_count,
            column_count,
            column_stats,
            table_size,
            num_files,
            snapshot_data["last_modified"],
            now
        )
        snapshot_df = self.spark.createDataFrame([snapshot_row], schema=self.SNAPSHOT_SCHEMA)
        snapshot_df.write.mode("append").option("mergeSchema", "true").saveAsTable(self.config.fully_qualified_snapshots)
        
        # Write column stats with explicit schema
        if snapshot_data.get("column_stat_records"):
            stats_rows = []
            for record in snapshot_data["column_stat_records"]:
                # Ensure all fields have valid defaults
                percentiles = record.get("percentiles") or {}
                
                stats_row = (
                    record["stat_id"],
                    record["snapshot_id"],
                    record["table_name"],
                    record["column_name"],
                    record.get("data_type", ""),
                    record["null_count"],
                    record["null_rate"],
                    record["distinct_count"],
                    record.get("cardinality_ratio", 0.0),
                    record.get("is_unique_candidate", False),
                    record.get("min_value", ""),
                    record.get("max_value", ""),
                    record.get("mean_value", 0.0),
                    record.get("stddev_value", 0.0),
                    percentiles,
                    record.get("min_length", 0),
                    record.get("max_length", 0),
                    record.get("avg_length", 0.0),
                    record.get("empty_string_count", 0),
                    record.get("empty_string_rate", 0.0),
                    record.get("mode_value", ""),
                    record.get("mode_frequency", 0),
                    record.get("entropy", 0.0),
                    record.get("sample_values", "[]"),
                    record.get("value_distribution", "{}"),
                    record.get("pattern_detected", "unknown"),
                    record.get("has_numeric_stats", False),
                    record.get("has_string_stats", False),
                    record.get("previous_distinct_count"),
                    record.get("distinct_count_change_pct"),
                    now
                )
                stats_rows.append(stats_row)
            
            stats_df = self.spark.createDataFrame(stats_rows, schema=self.COLUMN_STATS_SCHEMA)
            stats_df.write.mode("append").option("mergeSchema", "true").saveAsTable(self.config.fully_qualified_column_stats)
    
    def run(self, max_tables: int = None) -> Dict[str, Any]:
        """Execute the profiling pipeline."""
        logger.info("Starting profiling pipeline")
        self._stats = {"numeric_cols": 0, "string_cols": 0, "other_cols": 0, "total_cols": 0}
        
        self.create_snapshots_table()
        self.create_column_stats_table()
        
        tables = self.get_tables_to_profile()
        if max_tables:
            tables = tables[:max_tables]
        
        logger.info(f"Profiling {len(tables)} tables")
        
        successful = 0
        failed = 0
        
        for table_name in tables:
            snapshot = self.profile_table(table_name)
            if snapshot:
                self.write_snapshot(snapshot)
                successful += 1
            else:
                failed += 1
        
        # Summary logging
        logger.info(
            f"Profiling complete. Tables: {successful} success, {failed} failed. "
            f"Columns: {self._stats['total_cols']} total "
            f"({self._stats['numeric_cols']} numeric, {self._stats['string_cols']} string, "
            f"{self._stats['other_cols']} other)"
        )
        
        return {
            "tables_profiled": successful,
            "tables_failed": failed,
            "total_tables": len(tables),
            "columns_profiled": self._stats["total_cols"],
            "numeric_columns": self._stats["numeric_cols"],
            "string_columns": self._stats["string_cols"],
            "other_columns": self._stats["other_cols"]
        }


def run_profiling(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    max_tables: int = None
) -> Dict[str, Any]:
    """
    Convenience function to run profiling.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        max_tables: Maximum tables to profile
        
    Returns:
        Dict with execution statistics
    """
    config = ProfilingConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = ProfilingBuilder(spark, config)
    return builder.run(max_tables)

"""
Profiling module for capturing table and column statistics.

Generates profiling snapshots with row counts, column statistics,
and drift detection metrics for anomaly analysis.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType as SparkStringType

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
    """
    
    def __init__(self, spark: SparkSession, config: ProfilingConfig):
        self.spark = spark
        self.config = config
    
    def create_snapshots_table(self) -> None:
        """Create the profiling snapshots table."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_snapshots} (
            snapshot_id STRING NOT NULL,
            table_name STRING NOT NULL,
            snapshot_time TIMESTAMP,
            row_count BIGINT,
            column_stats MAP<STRING, STRING>,
            table_size_bytes BIGINT,
            num_files INT,
            last_modified TIMESTAMP,
            created_at TIMESTAMP
        )
        COMMENT 'Point-in-time profiling snapshots for tables'
        """
        self.spark.sql(ddl)
        logger.info(f"Snapshots table {self.config.fully_qualified_snapshots} ready")
    
    def create_column_stats_table(self) -> None:
        """Create the column profiling stats table."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_column_stats} (
            stat_id STRING NOT NULL,
            snapshot_id STRING,
            table_name STRING,
            column_name STRING,
            null_count BIGINT,
            null_rate DOUBLE,
            distinct_count BIGINT,
            min_value STRING,
            max_value STRING,
            mean_value DOUBLE,
            stddev_value DOUBLE,
            percentiles MAP<STRING, DOUBLE>,
            min_length INT,
            max_length INT,
            avg_length DOUBLE,
            previous_distinct_count BIGINT,
            distinct_count_change_pct DOUBLE,
            created_at TIMESTAMP
        )
        COMMENT 'Column-level profiling statistics with drift metrics'
        """
        self.spark.sql(ddl)
        logger.info(f"Column stats table {self.config.fully_qualified_column_stats} ready")
    
    def get_tables_to_profile(self) -> List[str]:
        """Get list of tables from knowledge base to profile."""
        df = self.spark.sql(f"""
            SELECT DISTINCT table_name 
            FROM {self.config.fully_qualified_source}
            WHERE table_name IS NOT NULL
        """)
        return [row.table_name for row in df.collect()]
    
    def profile_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Generate profiling snapshot for a single table.
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            Dict with snapshot data or None if profiling fails
        """
        try:
            snapshot_id = str(uuid.uuid4())
            
            # Get basic counts
            df = self.spark.table(table_name)
            row_count = df.count()
            
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
                    col_stats = self._profile_column(df, col_name, snapshot_id, table_name)
                    if col_stats:
                        column_stats[col_name] = str(col_stats.get("distinct_count", 0))
                        column_stat_records.append(col_stats)
                except Exception as e:
                    logger.debug(f"Could not profile column {col_name}: {e}")
            
            return {
                "snapshot_id": snapshot_id,
                "table_name": table_name,
                "row_count": row_count,
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
        table_name: str
    ) -> Dict[str, Any]:
        """Profile a single column."""
        stat_id = str(uuid.uuid4())
        col = F.col(f"`{col_name}`")
        
        # Basic stats
        stats_df = df.select(
            F.count(col).alias("non_null_count"),
            F.sum(F.when(col.isNull(), 1).otherwise(0)).alias("null_count"),
            F.approx_count_distinct(col).alias("distinct_count")
        ).collect()[0]
        
        row_count = df.count()
        null_count = stats_df["null_count"] or 0
        null_rate = null_count / row_count if row_count > 0 else 0
        distinct_count = stats_df["distinct_count"] or 0
        
        result = {
            "stat_id": stat_id,
            "snapshot_id": snapshot_id,
            "table_name": table_name,
            "column_name": col_name,
            "null_count": null_count,
            "null_rate": null_rate,
            "distinct_count": distinct_count,
            "min_value": None,
            "max_value": None,
            "mean_value": None,
            "stddev_value": None,
            "percentiles": None,
            "min_length": None,
            "max_length": None,
            "avg_length": None,
            "previous_distinct_count": None,
            "distinct_count_change_pct": None
        }
        
        # Get column type
        col_type = df.schema[col_name].dataType
        
        # Numeric stats
        if isinstance(col_type, NumericType):
            try:
                numeric_stats = df.select(
                    F.min(col).cast("string").alias("min_value"),
                    F.max(col).cast("string").alias("max_value"),
                    F.mean(col).alias("mean_value"),
                    F.stddev(col).alias("stddev_value")
                ).collect()[0]
                
                result["min_value"] = numeric_stats["min_value"]
                result["max_value"] = numeric_stats["max_value"]
                result["mean_value"] = numeric_stats["mean_value"]
                result["stddev_value"] = numeric_stats["stddev_value"]
                
                # Percentiles
                try:
                    percentile_values = df.stat.approxQuantile(
                        col_name, [0.25, 0.5, 0.75, 0.95, 0.99], 0.01
                    )
                    result["percentiles"] = {
                        "p25": percentile_values[0],
                        "p50": percentile_values[1],
                        "p75": percentile_values[2],
                        "p95": percentile_values[3],
                        "p99": percentile_values[4]
                    }
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"Could not compute numeric stats for {col_name}: {e}")
        
        # String stats
        elif isinstance(col_type, SparkStringType):
            try:
                string_stats = df.select(
                    F.min(F.length(col)).alias("min_length"),
                    F.max(F.length(col)).alias("max_length"),
                    F.avg(F.length(col)).alias("avg_length"),
                    F.min(col).alias("min_value"),
                    F.max(col).alias("max_value")
                ).collect()[0]
                
                result["min_length"] = string_stats["min_length"]
                result["max_length"] = string_stats["max_length"]
                result["avg_length"] = string_stats["avg_length"]
                result["min_value"] = str(string_stats["min_value"])[:100] if string_stats["min_value"] else None
                result["max_value"] = str(string_stats["max_value"])[:100] if string_stats["max_value"] else None
            except Exception as e:
                logger.debug(f"Could not compute string stats for {col_name}: {e}")
        
        # Get previous distinct count for drift detection
        try:
            prev_stats = self.spark.sql(f"""
                SELECT distinct_count
                FROM {self.config.fully_qualified_column_stats}
                WHERE table_name = '{table_name}' AND column_name = '{col_name}'
                ORDER BY created_at DESC
                LIMIT 1
            """).collect()
            
            if prev_stats:
                prev_distinct = prev_stats[0]["distinct_count"]
                result["previous_distinct_count"] = prev_distinct
                if prev_distinct and prev_distinct > 0:
                    result["distinct_count_change_pct"] = (
                        (distinct_count - prev_distinct) / prev_distinct * 100
                    )
        except Exception:
            pass
        
        return result
    
    def write_snapshot(self, snapshot_data: Dict[str, Any]) -> None:
        """Write snapshot and column stats to tables."""
        # Write snapshot record
        snapshot_df = self.spark.createDataFrame([{
            "snapshot_id": snapshot_data["snapshot_id"],
            "table_name": snapshot_data["table_name"],
            "snapshot_time": F.current_timestamp(),
            "row_count": snapshot_data["row_count"],
            "column_stats": snapshot_data["column_stats"],
            "table_size_bytes": snapshot_data["table_size_bytes"],
            "num_files": snapshot_data["num_files"],
            "last_modified": snapshot_data["last_modified"],
            "created_at": F.current_timestamp()
        }])
        
        snapshot_df.write.mode("append").saveAsTable(self.config.fully_qualified_snapshots)
        
        # Write column stats
        if snapshot_data.get("column_stat_records"):
            for record in snapshot_data["column_stat_records"]:
                record["created_at"] = F.current_timestamp()
            
            stats_df = self.spark.createDataFrame(snapshot_data["column_stat_records"])
            stats_df = stats_df.withColumn("created_at", F.current_timestamp())
            stats_df.write.mode("append").saveAsTable(self.config.fully_qualified_column_stats)
    
    def run(self, max_tables: int = None) -> Dict[str, Any]:
        """
        Execute the profiling pipeline.
        
        Args:
            max_tables: Maximum number of tables to profile (None = all)
        """
        logger.info("Starting profiling pipeline")
        
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
        
        logger.info(f"Profiling complete. Success: {successful}, Failed: {failed}")
        
        return {
            "tables_profiled": successful,
            "tables_failed": failed,
            "total_tables": len(tables)
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


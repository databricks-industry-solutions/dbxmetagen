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

from dbxmetagen.table_filter import table_filter_sql

logger = logging.getLogger(__name__)


@dataclass 
class ProfilingConfig:
    """Configuration for profiling ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    snapshots_table: str = "profiling_snapshots"
    column_stats_table: str = "column_profiling_stats"
    incremental: bool = True
    table_names: list[str] | None = None
    
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
        from dbxmetagen.processing import add_column_if_not_exists
        add_column_if_not_exists(self.spark, self.config.fully_qualified_snapshots, "column_count", "INT")
        
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
        from dbxmetagen.processing import add_column_if_not_exists
        for col_name, col_type in new_columns:
            add_column_if_not_exists(self.spark, self.config.fully_qualified_column_stats, col_name, col_type)
        
        logger.info(f"Column stats table {self.config.fully_qualified_column_stats} ready")
    
    def get_tables_to_profile(self) -> List[str]:
        """Get list of tables from knowledge base to profile.
        When incremental, only returns tables changed since last profiled."""
        tf = table_filter_sql(self.config.table_names or [], column="kb.table_name")
        tf_tbl = table_filter_sql(self.config.table_names or [], column="table_name")
        if self.config.incremental:
            try:
                df = self.spark.sql(f"""
                    SELECT DISTINCT kb.table_name
                    FROM {self.config.fully_qualified_source} kb
                    LEFT JOIN (
                        SELECT table_name, MAX(snapshot_time) AS last_profiled
                        FROM {self.config.fully_qualified_snapshots}
                        GROUP BY table_name
                    ) p ON kb.table_name = p.table_name
                    WHERE kb.table_name IS NOT NULL
                      AND (p.last_profiled IS NULL OR kb.updated_at > p.last_profiled)
                      {tf}
                """)
                tables = [row.table_name for row in df.collect()]
                total = self.spark.sql(f"SELECT COUNT(DISTINCT table_name) AS n FROM {self.config.fully_qualified_source} WHERE table_name IS NOT NULL {tf_tbl}").collect()[0].n
                logger.info(f"Incremental mode: {len(tables)} tables need re-profiling out of {total}")
                return tables
            except Exception as e:
                logger.warning(f"Incremental filtering failed ({e}), falling back to full refresh")
        df = self.spark.sql(f"""
            SELECT DISTINCT table_name 
            FROM {self.config.fully_qualified_source}
            WHERE table_name IS NOT NULL {tf_tbl}
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
    
    # ------------------------------------------------------------------
    # Dispatch: route to delta or federated profiling path
    # ------------------------------------------------------------------

    def profile_table(
        self, table_name: str, federation_mode: bool = False,
        drift_baselines: Optional[Dict[str, Dict[str, int]]] = None
    ) -> Dict[str, Any]:
        """Generate profiling snapshot for a single table.

        Routes to _profile_table_delta (single-pass with APPROX_COUNT_DISTINCT,
        PERCENTILE_APPROX, mode/frequency) or _profile_table_federated
        (pushdown-safe aggregates only).

        Raises on failure so callers get actionable error messages.
        """
        baselines = drift_baselines or {}
        if federation_mode:
            return self._profile_table_federated(table_name, baselines)
        return self._profile_table_delta(table_name, baselines)

    # ------------------------------------------------------------------
    # Delta path: single-pass SQL with full stats
    # ------------------------------------------------------------------

    def _profile_table_delta(self, table_name: str, drift_baselines: Dict[str, Dict[str, int]] = None) -> Optional[Dict[str, Any]]:
        snapshot_id = str(uuid.uuid4())
        df = self.spark.table(table_name)
        schema = df.schema
        columns = [f.name for f in schema.fields[:50]]
        column_count = len(schema.fields)

        # Classify columns by type
        numeric_cols, string_cols, bool_cols, date_cols, other_cols = [], [], [], [], []
        non_orderable_cols: set = set()
        col_type_map: Dict[str, Any] = {}
        for f in schema.fields[:50]:
            col_type_map[f.name] = f.dataType
            if isinstance(f.dataType, NumericType):
                numeric_cols.append(f.name)
            elif isinstance(f.dataType, SparkStringType):
                string_cols.append(f.name)
            elif isinstance(f.dataType, SparkBooleanType):
                bool_cols.append(f.name)
            elif isinstance(f.dataType, (DateType, SparkTimestampType)):
                date_cols.append(f.name)
            else:
                other_cols.append(f.name)
                if isinstance(f.dataType, (SparkMapType, SparkArrayType, StructType)):
                    non_orderable_cols.add(f.name)

        # --- Pass 1: single SQL with all aggregates ---
        agg_parts = ["COUNT(*) AS `_row_count`"]
        for c in columns:
            qc = f"`{c}`"
            agg_parts.append(f"COUNT({qc}) AS `{c}__non_null`")
            agg_parts.append(f"SUM(CASE WHEN {qc} IS NULL THEN 1 ELSE 0 END) AS `{c}__null_count`")
            if c not in non_orderable_cols:
                agg_parts.append(f"APPROX_COUNT_DISTINCT({qc}) AS `{c}__distinct`")
                agg_parts.append(f"CAST(MIN({qc}) AS STRING) AS `{c}__min`")
                agg_parts.append(f"CAST(MAX({qc}) AS STRING) AS `{c}__max`")

        for c in numeric_cols:
            qc = f"`{c}`"
            agg_parts.append(f"AVG({qc}) AS `{c}__mean`")
            agg_parts.append(f"STDDEV_SAMP({qc}) AS `{c}__stddev`")
            agg_parts.append(
                f"PERCENTILE_APPROX({qc}, ARRAY(0.25, 0.5, 0.75, 0.95, 0.99)) AS `{c}__pcts`"
            )

        for c in string_cols:
            qc = f"`{c}`"
            agg_parts.append(f"MIN(LENGTH({qc})) AS `{c}__min_len`")
            agg_parts.append(f"MAX(LENGTH({qc})) AS `{c}__max_len`")
            agg_parts.append(f"AVG(LENGTH({qc})) AS `{c}__avg_len`")
            agg_parts.append(
                f"SUM(CASE WHEN {qc} = '' OR TRIM({qc}) = '' THEN 1 ELSE 0 END) AS `{c}__empty`"
            )

        for c in date_cols:
            qc = f"`{c}`"
            agg_parts.append(f"CAST(MIN({qc}) AS STRING) AS `{c}__dt_min`")
            agg_parts.append(f"CAST(MAX({qc}) AS STRING) AS `{c}__dt_max`")

        sql = f"SELECT {', '.join(agg_parts)} FROM {table_name}"
        row = self.spark.sql(sql).collect()[0]
        row_count = int(row["_row_count"])

        # DESCRIBE DETAIL (instant for Delta)
        table_size, num_files, last_modified = None, None, None
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            table_size = getattr(detail, "sizeInBytes", None)
            num_files = getattr(detail, "numFiles", None)
            last_modified = getattr(detail, "lastModified", None)
        except Exception:
            pass

        # --- Pass 2: sample values (single LIMIT query) ---
        sample_map = self._fetch_samples(table_name, columns)

        # --- Parse pass-1 results into per-column records ---
        column_stats_summary: Dict[str, str] = {}
        column_stat_records: List[Dict[str, Any]] = []
        low_card_cols: List[str] = []

        for c in columns:
            non_null = int(row[f"{c}__non_null"] or 0)
            null_count = int(row[f"{c}__null_count"] or 0)
            is_non_orderable = c in non_orderable_cols
            distinct_count = 0 if is_non_orderable else int(row[f"{c}__distinct"] or 0)
            null_rate = float(null_count / row_count) if row_count > 0 else 0.0
            cardinality_ratio = float(distinct_count / non_null) if non_null > 0 else 0.0
            is_unique = cardinality_ratio > 0.95 and null_rate < 0.01 and row_count > 10

            dtype = col_type_map[c]
            data_type_str = self._get_data_type_string(dtype)
            is_numeric = isinstance(dtype, NumericType)
            is_string = isinstance(dtype, SparkStringType)
            is_bool = isinstance(dtype, SparkBooleanType)
            is_date = isinstance(dtype, (DateType, SparkTimestampType))

            self._stats["total_cols"] += 1
            if is_numeric:
                self._stats["numeric_cols"] += 1
            elif is_string:
                self._stats["string_cols"] += 1
            else:
                self._stats["other_cols"] += 1

            rec: Dict[str, Any] = {
                "stat_id": str(uuid.uuid4()),
                "snapshot_id": snapshot_id,
                "table_name": table_name,
                "column_name": c,
                "data_type": data_type_str,
                "null_count": null_count,
                "null_rate": null_rate,
                "distinct_count": distinct_count,
                "cardinality_ratio": cardinality_ratio,
                "is_unique_candidate": is_unique,
                "min_value": "" if is_non_orderable else str(row[f"{c}__min"] or "")[:100],
                "max_value": "" if is_non_orderable else str(row[f"{c}__max"] or "")[:100],
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
                "sample_values": json.dumps(sample_map.get(c, [])[:5]),
                "value_distribution": "{}",
                "pattern_detected": "unknown",
                "has_numeric_stats": is_numeric,
                "has_string_stats": is_string,
                "previous_distinct_count": None,
                "distinct_count_change_pct": None,
            }

            if is_numeric:
                mean_v = row[f"{c}__mean"]
                stddev_v = row[f"{c}__stddev"]
                rec["mean_value"] = float(mean_v) if mean_v is not None else 0.0
                rec["stddev_value"] = float(stddev_v) if stddev_v is not None else 0.0
                pcts = row[f"{c}__pcts"]
                if pcts and len(pcts) == 5:
                    rec["percentiles"] = {
                        "p25": float(pcts[0]), "p50": float(pcts[1]),
                        "p75": float(pcts[2]), "p95": float(pcts[3]), "p99": float(pcts[4]),
                    }

            if is_string:
                ml = row[f"{c}__min_len"]
                rec["min_length"] = int(ml) if ml is not None else 0
                xl = row[f"{c}__max_len"]
                rec["max_length"] = int(xl) if xl is not None else 0
                al = row[f"{c}__avg_len"]
                rec["avg_length"] = float(al) if al is not None else 0.0
                ec = row[f"{c}__empty"]
                rec["empty_string_count"] = int(ec) if ec is not None else 0
                rec["empty_string_rate"] = float(rec["empty_string_count"] / row_count) if row_count > 0 else 0.0
                rec["pattern_detected"] = self._detect_pattern(sample_map.get(c, []))

            if is_date:
                rec["min_value"] = str(row[f"{c}__dt_min"] or "")
                rec["max_value"] = str(row[f"{c}__dt_max"] or "")
                rec["pattern_detected"] = "datetime"

            if is_bool:
                rec["pattern_detected"] = "boolean"

            if 0 < distinct_count <= 1000:
                low_card_cols.append(c)

            column_stats_summary[c] = f"dist:{distinct_count},null:{null_rate:.2f}"
            column_stat_records.append(rec)

        # --- Pass 3: mode/frequency for low-cardinality columns (batched UNION ALL) ---
        if low_card_cols:
            self._fill_mode_frequency(table_name, low_card_cols, column_stat_records, row_count)

        # --- Drift detection (from pre-loaded baselines) ---
        self._apply_drift(table_name, column_stat_records, drift_baselines or {})

        return {
            "snapshot_id": snapshot_id,
            "table_name": table_name,
            "row_count": row_count,
            "column_count": column_count,
            "column_stats": column_stats_summary,
            "table_size_bytes": table_size,
            "num_files": num_files,
            "last_modified": last_modified,
            "column_stat_records": column_stat_records,
        }

    # ------------------------------------------------------------------
    # Federated path: pushdown-safe aggregates only
    # ------------------------------------------------------------------

    def _profile_table_federated(self, table_name: str, drift_baselines: Dict[str, Dict[str, int]] = None) -> Optional[Dict[str, Any]]:
        snapshot_id = str(uuid.uuid4())
        df = self.spark.table(table_name)
        schema = df.schema
        columns = [f.name for f in schema.fields[:50]]
        column_count = len(schema.fields)

        numeric_cols, string_cols = [], []
        non_orderable_cols: set = set()
        col_type_map: Dict[str, Any] = {}
        for f in schema.fields[:50]:
            col_type_map[f.name] = f.dataType
            if isinstance(f.dataType, NumericType):
                numeric_cols.append(f.name)
            elif isinstance(f.dataType, SparkStringType):
                string_cols.append(f.name)
            elif isinstance(f.dataType, (SparkMapType, SparkArrayType, StructType)):
                non_orderable_cols.add(f.name)

        # Guaranteed-pushdown aggregates only: bare-column COUNT/MIN/MAX/AVG.
        # STDDEV_SAMP and AGG(EXPR(col)) like MIN(LENGTH(col)) are NOT guaranteed
        # to push down across all LF connectors. Those stats are derived from
        # the LIMIT-based sample pull below instead.
        # Skip MIN/MAX for non-orderable types (MAP, ARRAY, STRUCT).
        agg_parts = ["COUNT(*) AS `_row_count`"]
        for c in columns:
            qc = f"`{c}`"
            agg_parts.append(f"COUNT({qc}) AS `{c}__non_null`")
            if c not in non_orderable_cols:
                agg_parts.append(f"CAST(MIN({qc}) AS STRING) AS `{c}__min`")
                agg_parts.append(f"CAST(MAX({qc}) AS STRING) AS `{c}__max`")

        for c in numeric_cols:
            qc = f"`{c}`"
            agg_parts.append(f"AVG({qc}) AS `{c}__mean`")

        sql = f"SELECT {', '.join(agg_parts)} FROM {table_name}"
        row = self.spark.sql(sql).collect()[0]
        row_count = int(row["_row_count"])

        # Sample values via LIMIT (pushes down everywhere)
        sample_map = self._fetch_samples(table_name, columns)

        column_stats_summary: Dict[str, str] = {}
        column_stat_records: List[Dict[str, Any]] = []

        for c in columns:
            non_null = int(row[f"{c}__non_null"] or 0)
            null_count = row_count - non_null
            null_rate = float(null_count / row_count) if row_count > 0 else 0.0
            is_non_orderable = c in non_orderable_cols

            dtype = col_type_map[c]
            data_type_str = self._get_data_type_string(dtype)
            is_numeric = isinstance(dtype, NumericType)
            is_string = isinstance(dtype, SparkStringType)

            self._stats["total_cols"] += 1
            if is_numeric:
                self._stats["numeric_cols"] += 1
            elif is_string:
                self._stats["string_cols"] += 1
            else:
                self._stats["other_cols"] += 1

            rec: Dict[str, Any] = {
                "stat_id": str(uuid.uuid4()),
                "snapshot_id": snapshot_id,
                "table_name": table_name,
                "column_name": c,
                "data_type": data_type_str,
                "null_count": null_count,
                "null_rate": null_rate,
                "distinct_count": 0,
                "cardinality_ratio": 0.0,
                "is_unique_candidate": False,
                "min_value": "" if is_non_orderable else str(row[f"{c}__min"] or "")[:100],
                "max_value": "" if is_non_orderable else str(row[f"{c}__max"] or "")[:100],
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
                "sample_values": json.dumps(sample_map.get(c, [])[:5]),
                "value_distribution": "{}",
                "pattern_detected": self._detect_pattern(sample_map.get(c, [])) if is_string else "unknown",
                "has_numeric_stats": is_numeric,
                "has_string_stats": is_string,
                "previous_distinct_count": None,
                "distinct_count_change_pct": None,
            }

            if is_numeric:
                mean_v = row[f"{c}__mean"]
                rec["mean_value"] = float(mean_v) if mean_v is not None else 0.0
                samples = sample_map.get(c, [])
                nums = []
                for s in samples:
                    try:
                        nums.append(float(s))
                    except (ValueError, TypeError):
                        pass
                if len(nums) > 1:
                    m = sum(nums) / len(nums)
                    var = sum((x - m) ** 2 for x in nums) / (len(nums) - 1)
                    rec["stddev_value"] = var ** 0.5

            if is_string:
                samples = sample_map.get(c, [])
                if samples:
                    lengths = [len(s) for s in samples]
                    rec["min_length"] = min(lengths)
                    rec["max_length"] = max(lengths)
                    rec["avg_length"] = sum(lengths) / len(lengths)

            column_stats_summary[c] = f"dist:0,null:{null_rate:.2f}"
            column_stat_records.append(rec)

        self._apply_drift(table_name, column_stat_records, drift_baselines or {})

        return {
            "snapshot_id": snapshot_id,
            "table_name": table_name,
            "row_count": row_count,
            "column_count": column_count,
            "column_stats": column_stats_summary,
            "table_size_bytes": None,
            "num_files": None,
            "last_modified": None,
            "column_stat_records": column_stat_records,
        }

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _fetch_samples(self, table_name: str, columns: List[str]) -> Dict[str, List[str]]:
        """Single LIMIT query to get sample values for all columns."""
        try:
            sel = ", ".join(f"CAST(`{c}` AS STRING) AS `{c}`" for c in columns)
            sample_rows = self.spark.sql(f"SELECT {sel} FROM {table_name} LIMIT 100").collect()
            result: Dict[str, List[str]] = {c: [] for c in columns}
            for r in sample_rows:
                for c in columns:
                    v = r[c]
                    if v is not None and len(result[c]) < 5:
                        result[c].append(v)
            return result
        except Exception as e:
            logger.debug(f"Sample fetch failed for {table_name}: {e}")
            return {}

    def _fill_mode_frequency(
        self, table_name: str, low_card_cols: List[str],
        records: List[Dict[str, Any]], row_count: int
    ) -> None:
        """Batched UNION ALL query for mode/frequency (Delta only)."""
        try:
            parts = []
            for c in low_card_cols:
                qc = f"`{c}`"
                parts.append(
                    f"SELECT '{c}' AS col_name, CAST({qc} AS STRING) AS val, COUNT(*) AS cnt "
                    f"FROM {table_name} GROUP BY {qc}"
                )
            sql = " UNION ALL ".join(parts)
            mode_rows = self.spark.sql(sql).collect()

            # Group by column
            col_groups: Dict[str, List] = {}
            for r in mode_rows:
                col_groups.setdefault(r["col_name"], []).append((r["val"], int(r["cnt"])))

            rec_map = {r["column_name"]: r for r in records}
            for col_name, vals in col_groups.items():
                vals.sort(key=lambda x: -x[1])
                top10 = vals[:10]
                rec = rec_map.get(col_name)
                if not rec or not top10:
                    continue
                rec["mode_value"] = str(top10[0][0])[:100] if top10[0][0] else ""
                rec["mode_frequency"] = top10[0][1]
                dist = {str(v)[:50]: cnt for v, cnt in top10}
                rec["value_distribution"] = json.dumps(dist)
                non_null = row_count - rec["null_count"]
                value_counts = {str(v): cnt for v, cnt in top10}
                rec["entropy"] = round(self._compute_entropy(value_counts, non_null), 4)
        except Exception as e:
            logger.debug(f"Mode frequency batch failed for {table_name}: {e}")

    def _load_all_drift_baselines(self, tables: List[str]) -> Dict[str, Dict[str, int]]:
        """Single upfront query to load previous distinct counts for all tables."""
        try:
            prev_rows = self.spark.sql(f"""
                SELECT table_name, column_name, distinct_count
                FROM (
                    SELECT table_name, column_name, distinct_count,
                           ROW_NUMBER() OVER (
                               PARTITION BY table_name, column_name ORDER BY created_at DESC
                           ) AS rn
                    FROM {self.config.fully_qualified_column_stats}
                    WHERE table_name IN ({','.join(f"'{t}'" for t in tables)})
                ) t WHERE rn = 1
            """).collect()
            result: Dict[str, Dict[str, int]] = {}
            for r in prev_rows:
                result.setdefault(r["table_name"], {})[r["column_name"]] = r["distinct_count"]
            return result
        except Exception:
            return {}

    def _apply_drift(
        self, table_name: str, records: List[Dict[str, Any]],
        drift_baselines: Dict[str, Dict[str, int]]
    ) -> None:
        """Apply pre-loaded drift baselines to column records."""
        prev_map = drift_baselines.get(table_name, {})
        for rec in records:
            prev = prev_map.get(rec["column_name"])
            if prev is not None:
                rec["previous_distinct_count"] = int(prev)
                if prev > 0:
                    rec["distinct_count_change_pct"] = float(
                        (rec["distinct_count"] - prev) / prev * 100
                    )
    
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
        # Dedup guard: delete any prior snapshot with the same snapshot_id (retry protection)
        sid = snapshot_data["snapshot_id"]
        try:
            self.spark.sql(
                f"DELETE FROM {self.config.fully_qualified_snapshots} WHERE snapshot_id = '{sid}'"
            )
        except Exception as e:
            logger.warning("Dedup DELETE failed for snapshots, proceeding with append: %s", e)
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
            # Dedup guard: delete any prior column stats with the same snapshot_id
            try:
                self.spark.sql(
                    f"DELETE FROM {self.config.fully_qualified_column_stats} WHERE snapshot_id = '{sid}'"
                )
            except Exception as e:
                logger.warning("Dedup DELETE failed for column_stats, proceeding with append: %s", e)
            stats_df.write.mode("append").option("mergeSchema", "true").saveAsTable(self.config.fully_qualified_column_stats)
    
    def run(self, max_tables: int = None, federation_mode: bool = False, raise_on_error: bool = True) -> Dict[str, Any]:
        """Execute the profiling pipeline with cross-table parallelism.

        Args:
            raise_on_error: If True (default), raise RuntimeError when any table
                fails to profile. Set False for pipeline resilience (partial results).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info("Starting profiling pipeline")
        self._stats = {"numeric_cols": 0, "string_cols": 0, "other_cols": 0, "total_cols": 0}

        self.create_snapshots_table()
        self.create_column_stats_table()

        tables = self.get_tables_to_profile()
        if max_tables:
            tables = tables[:max_tables]

        if not tables:
            logger.info("No tables to profile")
            return {
                "tables_profiled": 0, "tables_failed": 0, "total_tables": 0,
                "columns_profiled": 0, "numeric_columns": 0,
                "string_columns": 0, "other_columns": 0,
            }

        logger.info(f"Profiling {len(tables)} tables (federation_mode={federation_mode})")

        drift_baselines = self._load_all_drift_baselines(tables)

        # Patterns indicating the table is unreadable (VS indexes, unsupported formats)
        # These are skipped rather than counted as failures.
        _SKIP_PATTERNS = ("DATA_SOURCE_NOT_FOUND", "unsupported.DefaultSource")

        successful = 0
        failed = 0
        skipped = 0
        errors: list[str] = []
        max_workers = min(4 if federation_mode else 8, len(tables))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self.profile_table, t, federation_mode, drift_baselines): t
                for t in tables
            }
            for future in as_completed(futures):
                tbl = futures[future]
                try:
                    snapshot = future.result()
                    self.write_snapshot(snapshot)
                    successful += 1
                except Exception as e:
                    err_str = str(e)
                    if any(p in err_str for p in _SKIP_PATTERNS):
                        logger.warning(f"Skipping unreadable table {tbl}: {err_str[:120]}")
                        skipped += 1
                    else:
                        logger.error(f"Profiling failed for {tbl}: {e}")
                        errors.append(f"{tbl}: {e}")
                        failed += 1

        logger.info(
            f"Profiling complete. Tables: {successful} success, {failed} failed, {skipped} skipped. "
            f"Columns: {self._stats['total_cols']} total "
            f"({self._stats['numeric_cols']} numeric, {self._stats['string_cols']} string, "
            f"{self._stats['other_cols']} other)"
        )

        if failed > 0 and raise_on_error:
            raise RuntimeError(
                f"Profiling failed for {failed}/{len(tables)} tables:\n" +
                "\n".join(f"  - {e}" for e in errors)
            )

        return {
            "tables_profiled": successful,
            "tables_failed": failed,
            "tables_skipped": skipped,
            "total_tables": len(tables),
            "columns_profiled": self._stats["total_cols"],
            "numeric_columns": self._stats["numeric_cols"],
            "string_columns": self._stats["string_cols"],
            "other_columns": self._stats["other_cols"],
        }


def run_profiling(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    max_tables: int = None,
    incremental: bool = True,
    table_names: list[str] | None = None,
    federation_mode: bool = False,
    raise_on_error: bool = True,
) -> Dict[str, Any]:
    """
    Convenience function to run profiling.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        max_tables: Maximum tables to profile
        incremental: Only profile tables changed since last snapshot
        federation_mode: Use pushdown-safe profiling for federated catalogs
        raise_on_error: If True (default), raise on any table failure.
            Set False for pipeline resilience (partial results).
        
    Returns:
        Dict with execution statistics
    """
    from dbxmetagen.processing import _check_federation_guard
    _check_federation_guard(spark, catalog_name, schema_name, table_names, federation_mode)

    config = ProfilingConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        incremental=incremental,
        table_names=table_names,
    )
    builder = ProfilingBuilder(spark, config)
    return builder.run(max_tables, federation_mode=federation_mode, raise_on_error=raise_on_error)

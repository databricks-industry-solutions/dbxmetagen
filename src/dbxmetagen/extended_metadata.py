"""
Extended Metadata Extraction module.

Extracts comprehensive metadata from system tables including lineage,
constraints, data types, governance policies, and table properties.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from dbxmetagen.table_filter import table_filter_sql

logger = logging.getLogger(__name__)


@dataclass
class ExtendedMetadataConfig:
    """Configuration for extended metadata extraction."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    target_table: str = "extended_table_metadata"
    incremental: bool = True
    table_names: list[str] | None = None
    federation_mode: bool = False
    metadata_companion_path: str = ""

    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_target(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.target_table}"


class ExtendedMetadataBuilder:
    """
    Builder class for extracting extended metadata from system tables.
    
    Queries various system tables to gather lineage, constraints, governance
    policies, and other extended metadata for tables in the knowledge base.
    """
    
    def __init__(self, spark: SparkSession, config: ExtendedMetadataConfig):
        self.spark = spark
        self.config = config
    
    def create_target_table(self) -> None:
        """Create the target table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_target} (
            table_name STRING NOT NULL,
            table_type STRING,
            table_owner STRING,
            created TIMESTAMP,
            last_altered TIMESTAMP,
            column_count INT,
            upstream_tables ARRAY<STRING>,
            downstream_tables ARRAY<STRING>,
            primary_key_columns ARRAY<STRING>,
            foreign_keys MAP<STRING, STRING>,
            clustering_columns ARRAY<STRING>,
            partition_columns ARRAY<STRING>,
            row_filter_policy STRING,
            column_mask_policies MAP<STRING, STRING>,
            table_size_bytes BIGINT,
            num_files INT,
            data_source_format STRING,
            connection_name STRING,
            connection_type STRING,
            connection_url STRING,
            updated_at TIMESTAMP
        )
        COMMENT 'Extended table metadata from system tables'
        """
        self.spark.sql(ddl)

        from dbxmetagen.processing import add_column_if_not_exists
        for col, typ in [
            ("data_source_format", "STRING"),
            ("connection_name", "STRING"),
            ("connection_type", "STRING"),
            ("connection_url", "STRING"),
        ]:
            add_column_if_not_exists(self.spark, self.config.fully_qualified_target, col, typ)

        logger.info(f"Target table {self.config.fully_qualified_target} ready")
    
    def get_tables_to_process(self) -> List[str]:
        """Get list of tables from knowledge base to extract metadata for.
        When incremental, only returns tables changed since last extraction."""
        tn = self.config.table_names or []
        f_kb = table_filter_sql(tn, "kb.table_name")
        f_tbl = table_filter_sql(tn, "table_name")
        if self.config.incremental:
            try:
                df = self.spark.sql(f"""
                    SELECT DISTINCT kb.table_name
                    FROM {self.config.fully_qualified_source} kb
                    LEFT JOIN {self.config.fully_qualified_target} ext ON kb.table_name = ext.table_name
                    WHERE kb.table_name IS NOT NULL
                      AND (ext.updated_at IS NULL OR kb.updated_at > ext.updated_at)
                      {f_kb}
                """)
                tables = [row.table_name for row in df.collect()]
                total = self.spark.sql(f"SELECT COUNT(DISTINCT table_name) AS n FROM {self.config.fully_qualified_source} WHERE table_name IS NOT NULL {f_tbl}").collect()[0].n
                logger.info(f"Incremental mode: {len(tables)} tables need extended metadata out of {total}")
                return tables
            except Exception as e:
                logger.warning(f"Incremental filtering failed ({e}), falling back to full refresh")
        df = self.spark.sql(f"""
            SELECT DISTINCT table_name 
            FROM {self.config.fully_qualified_source}
            WHERE table_name IS NOT NULL
              {f_tbl}
        """)
        return [row.table_name for row in df.collect()]
    
    def _resolve_catalog_connections(self, catalogs: List[str]) -> Dict[str, Dict[str, str]]:
        """For each catalog, get its connection info (if foreign).

        Returns: {catalog_name: {connection_name, connection_type, connection_url}}
        """
        result: Dict[str, Dict[str, str]] = {}
        foreign_connection_names = []

        for catalog in catalogs:
            try:
                rows = self.spark.sql(f"DESCRIBE CATALOG EXTENDED {catalog}").collect()
                info = {r["info_name"]: r["info_value"] for r in rows}
                if info.get("Catalog Type", "").lower() == "foreign":
                    conn_name = info.get("Connection Name", "")
                    if conn_name:
                        result[catalog] = {"connection_name": conn_name, "connection_type": None, "connection_url": None}
                        foreign_connection_names.append(conn_name)
            except Exception as e:
                logger.debug(f"Could not describe catalog {catalog}: {e}")

        if foreign_connection_names:
            try:
                names_in = ",".join(f"'{n}'" for n in foreign_connection_names)
                conn_rows = self.spark.sql(f"""
                    SELECT connection_name, connection_type, url
                    FROM system.information_schema.connections
                    WHERE connection_name IN ({names_in})
                """).collect()
                conn_map = {r["connection_name"]: (r["connection_type"], r["url"]) for r in conn_rows}
                for cat_info in result.values():
                    cn = cat_info["connection_name"]
                    if cn in conn_map:
                        cat_info["connection_type"] = conn_map[cn][0]
                        cat_info["connection_url"] = conn_map[cn][1]
            except Exception as e:
                logger.debug(f"Could not query connections: {e}")

        return result

    def extract_basic_table_info(self, tables: List[str]) -> DataFrame:
        """Extract basic table info from information_schema.tables."""
        if not tables:
            return self.spark.createDataFrame([], self._get_basic_schema())
        
        all_dfs = []
        catalogs = set(t.split(".")[0] for t in tables if "." in t)
        
        for catalog in catalogs:
            try:
                df = self.spark.sql(f"""
                    SELECT 
                        CONCAT(table_catalog, '.', table_schema, '.', table_name) as table_name,
                        table_type,
                        table_owner,
                        created,
                        last_altered,
                        data_source_format
                    FROM system.information_schema.tables
                    WHERE table_catalog = '{catalog}'
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Could not query information_schema for catalog {catalog}: {e}")
        
        if all_dfs:
            from functools import reduce
            combined = reduce(lambda a, b: a.union(b), all_dfs)
            tables_df = self.spark.createDataFrame([(t,) for t in tables], ["filter_table"])
            return combined.join(tables_df, combined.table_name == tables_df.filter_table, "inner").drop("filter_table")
        
        return self.spark.createDataFrame([], self._get_basic_schema())
    
    def _get_basic_schema(self) -> str:
        return "table_name STRING, table_type STRING, table_owner STRING, created TIMESTAMP, last_altered TIMESTAMP, data_source_format STRING"
    
    def extract_column_counts(self, tables: List[str]) -> DataFrame:
        """Extract column counts from information_schema.columns."""
        if not tables:
            return self.spark.createDataFrame([], "table_name STRING, column_count INT")
        
        all_dfs = []
        catalogs = set(t.split(".")[0] for t in tables if "." in t)
        
        for catalog in catalogs:
            try:
                df = self.spark.sql(f"""
                    SELECT 
                        CONCAT(table_catalog, '.', table_schema, '.', table_name) as table_name,
                        COUNT(*) as column_count
                    FROM system.information_schema.columns
                    WHERE table_catalog = '{catalog}'
                    GROUP BY table_catalog, table_schema, table_name
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Could not query columns for catalog {catalog}: {e}")
        
        if all_dfs:
            from functools import reduce
            combined = reduce(lambda a, b: a.union(b), all_dfs)
            tables_df = self.spark.createDataFrame([(t,) for t in tables], ["filter_table"])
            return combined.join(tables_df, combined.table_name == tables_df.filter_table, "inner").drop("filter_table")
        
        return self.spark.createDataFrame([], "table_name STRING, column_count INT")
    
    def extract_lineage(self, tables: List[str]) -> DataFrame:
        """Extract lineage from system.access.table_lineage."""
        if not tables:
            return self.spark.createDataFrame([], "table_name STRING, upstream_tables ARRAY<STRING>, downstream_tables ARRAY<STRING>")
        
        try:
            # Get upstream (source) tables
            upstream_df = self.spark.sql("""
                SELECT 
                    CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name) as table_name,
                    COLLECT_SET(CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name)) as upstream_tables
                FROM system.access.table_lineage
                WHERE source_table_name IS NOT NULL
                GROUP BY target_table_catalog, target_table_schema, target_table_name
            """)
            
            # Get downstream (target) tables
            downstream_df = self.spark.sql("""
                SELECT 
                    CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name) as table_name,
                    COLLECT_SET(CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name)) as downstream_tables
                FROM system.access.table_lineage
                WHERE target_table_name IS NOT NULL
                GROUP BY source_table_catalog, source_table_schema, source_table_name
            """)
            
            # Filter to our tables and join
            tables_df = self.spark.createDataFrame([(t,) for t in tables], ["filter_table"])
            
            upstream_filtered = upstream_df.join(
                tables_df, upstream_df.table_name == tables_df.filter_table, "inner"
            ).drop("filter_table")
            
            downstream_filtered = downstream_df.join(
                tables_df, downstream_df.table_name == tables_df.filter_table, "inner"
            ).drop("filter_table")
            
            # Full outer join
            result = upstream_filtered.join(downstream_filtered, "table_name", "full_outer")
            return result
            
        except Exception as e:
            msg = (
                f"[dbxmetagen] Could not query lineage: {e}\n"
                "  You may not have access to system.access.table_lineage.\n"
                "  Continuing without lineage data."
            )
            logger.warning(msg)
            print(msg)
            return self.spark.createDataFrame([], "table_name STRING, upstream_tables ARRAY<STRING>, downstream_tables ARRAY<STRING>")
    
    def _load_fk_references(self, catalog: str) -> dict:
        """Try referential_constraints + key_column_usage to map FK cols to referenced tables."""
        ref_map: dict = {}
        try:
            rows = self.spark.sql(f"""
                SELECT
                    CONCAT(rc.constraint_catalog, '.', rc.constraint_schema, '.', kcu.table_name) AS src_table,
                    kcu.column_name AS src_col,
                    CONCAT(rc.unique_constraint_catalog, '.', rc.unique_constraint_schema, '.',
                           kcu2.table_name, '.', kcu2.column_name) AS ref_target
                FROM system.information_schema.referential_constraints rc
                JOIN system.information_schema.key_column_usage kcu
                    ON rc.constraint_catalog = kcu.constraint_catalog
                    AND rc.constraint_schema = kcu.constraint_schema
                    AND rc.constraint_name = kcu.constraint_name
                JOIN system.information_schema.key_column_usage kcu2
                    ON rc.unique_constraint_catalog = kcu2.constraint_catalog
                    AND rc.unique_constraint_schema = kcu2.constraint_schema
                    AND rc.unique_constraint_name = kcu2.constraint_name
                    AND kcu.ordinal_position = kcu2.ordinal_position
                WHERE rc.constraint_catalog = '{catalog}'
            """).collect()
            for r in rows:
                ref_map.setdefault(r.src_table, {})[r.src_col] = r.ref_target
        except Exception as e:
            logger.debug("referential_constraints unavailable for %s: %s", catalog, e)
        return ref_map

    def _load_companion_constraints(self, tables: List[str]) -> dict:
        path = (self.config.metadata_companion_path or "").strip()
        if not path:
            return {}
        from dbxmetagen.metadata_import import build_companion_constraints
        try:
            return build_companion_constraints(path, tables)
        except Exception as e:
            logger.warning("Could not parse companion metadata at %s: %s", path, e)
            return {}

    def _constraints_to_dataframe(self, tables: List[str], merged: dict) -> DataFrame:
        from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
        empty = self.spark.createDataFrame(
            [], "table_name STRING, primary_key_columns ARRAY<STRING>, foreign_keys MAP<STRING, STRING>"
        )
        rows = []
        for t in tables:
            pks, fks = merged.get(t, ([], {}))
            if not pks and not fks:
                continue
            rows.append((t, pks or None, fks or None))
        if not rows:
            return empty
        schema = StructType([
            StructField("table_name", StringType()),
            StructField("primary_key_columns", ArrayType(StringType())),
            StructField("foreign_keys", MapType(StringType(), StringType())),
        ])
        return self.spark.createDataFrame(rows, schema)

    def extract_constraints(self, tables: List[str]) -> DataFrame:
        """Extract primary key and foreign key constraints."""
        if not tables:
            return self.spark.createDataFrame([], "table_name STRING, primary_key_columns ARRAY<STRING>, foreign_keys MAP<STRING, STRING>")

        uc_by_table: dict = {}
        all_dfs = []
        catalogs = set(t.split(".")[0] for t in tables if "." in t)

        fk_ref_maps: dict = {}
        for catalog in catalogs:
            fk_ref_maps[catalog] = self._load_fk_references(catalog)
            try:
                df = self.spark.sql(f"""
                    SELECT 
                        CONCAT(t.constraint_catalog, '.', t.constraint_schema, '.', t.table_name) as table_name,
                        t.constraint_type,
                        k.column_name
                    FROM system.information_schema.table_constraints t
                    LEFT JOIN system.information_schema.key_column_usage k
                        ON t.constraint_catalog = k.constraint_catalog 
                        AND t.constraint_schema = k.constraint_schema 
                        AND t.constraint_name = k.constraint_name
                    WHERE t.constraint_catalog = '{catalog}'
                      AND t.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Could not query constraints for catalog {catalog}: {e}")

        if all_dfs:
            from functools import reduce
            combined = reduce(lambda a, b: a.union(b), all_dfs)

            pk_by_table: dict = {}
            for r in combined.filter(F.col("constraint_type") == "PRIMARY KEY").collect():
                if r.table_name and r.column_name:
                    pk_by_table.setdefault(r.table_name, set()).add(r.column_name)

            fk_by_table: dict = {}
            for r in combined.filter(F.col("constraint_type") == "FOREIGN KEY").collect():
                tname, col = r.table_name, r.column_name
                if not tname or not col:
                    continue
                cat = tname.split(".")[0]
                ref_target = fk_ref_maps.get(cat, {}).get(tname, {}).get(col, "")
                fk_by_table.setdefault(tname, {})[col] = ref_target

            for tname in set(pk_by_table) | set(fk_by_table):
                uc_by_table[tname] = (
                    sorted(pk_by_table.get(tname, [])),
                    dict(fk_by_table.get(tname, {})),
                )

        from dbxmetagen.metadata_import import merge_constraint_maps
        companion = self._load_companion_constraints(tables)
        merged = merge_constraint_maps(uc_by_table, companion)
        return self._constraints_to_dataframe(tables, merged)
    
    def extract_table_properties(self, tables: List[str]) -> DataFrame:
        """Extract clustering and partition info using DESCRIBE DETAIL."""
        empty_schema = "table_name STRING, clustering_columns ARRAY<STRING>, partition_columns ARRAY<STRING>, table_size_bytes BIGINT, num_files INT"
        if not tables:
            return self.spark.createDataFrame([], empty_schema)

        if self.config.federation_mode:
            logger.info("Federation mode: skipping DESCRIBE DETAIL (not supported on federated tables)")
            return self.spark.createDataFrame([(t, None, None, None, None) for t in tables], empty_schema)

        results = []
        for table in tables[:100]:
            try:
                detail_df = self.spark.sql(f"DESCRIBE DETAIL {table}")
                row = detail_df.collect()[0]
                
                clustering = None
                partitions = None
                size_bytes = None
                num_files = None
                
                if hasattr(row, 'clusteringColumns') and row.clusteringColumns:
                    clustering = list(row.clusteringColumns)
                if hasattr(row, 'partitionColumns') and row.partitionColumns:
                    partitions = list(row.partitionColumns)
                if hasattr(row, 'sizeInBytes'):
                    size_bytes = row.sizeInBytes
                if hasattr(row, 'numFiles'):
                    num_files = row.numFiles
                
                results.append((table, clustering, partitions, size_bytes, num_files))
            except Exception as e:
                logger.debug(f"Could not describe table {table}: {e}")
                results.append((table, None, None, None, None))
        
        return self.spark.createDataFrame(
            results, 
            "table_name STRING, clustering_columns ARRAY<STRING>, partition_columns ARRAY<STRING>, table_size_bytes BIGINT, num_files INT"
        )
    
    def build_staged_updates(self) -> DataFrame:
        """Build staged updates by combining all metadata sources."""
        tables = self.get_tables_to_process()
        logger.info(f"Processing extended metadata for {len(tables)} tables")
        
        if not tables:
            return self.spark.createDataFrame([], self._get_full_schema())
        
        # Extract from various sources
        basic_info = self.extract_basic_table_info(tables)
        column_counts = self.extract_column_counts(tables)
        lineage = self.extract_lineage(tables)
        constraints = self.extract_constraints(tables)
        properties = self.extract_table_properties(tables)
        
        # Resolve catalog connections (one DESCRIBE per catalog)
        catalogs = list(set(t.split(".")[0] for t in tables if "." in t))
        catalog_conn_map = self._resolve_catalog_connections(catalogs)

        conn_rows = []
        for t in tables:
            cat = t.split(".")[0] if "." in t else ""
            info = catalog_conn_map.get(cat, {})
            conn_rows.append((t, info.get("connection_name"), info.get("connection_type"), info.get("connection_url")))
        conn_df = self.spark.createDataFrame(
            conn_rows, "table_name STRING, connection_name STRING, connection_type STRING, connection_url STRING"
        )

        # Start with all tables
        tables_df = self.spark.createDataFrame([(t,) for t in tables], ["table_name"])
        
        # Join all data
        result = (
            tables_df
            .join(basic_info, "table_name", "left")
            .join(column_counts, "table_name", "left")
            .join(lineage, "table_name", "left")
            .join(constraints, "table_name", "left")
            .join(properties, "table_name", "left")
            .join(conn_df, "table_name", "left")
        )
        
        result = (
            result
            .withColumn("row_filter_policy", F.lit(None).cast("string"))
            .withColumn("column_mask_policies", F.lit(None).cast("map<string, string>"))
            .withColumn("updated_at", F.current_timestamp())
        )
        
        return result
    
    def _get_full_schema(self) -> str:
        return """
            table_name STRING, table_type STRING, table_owner STRING, created TIMESTAMP,
            last_altered TIMESTAMP, column_count INT, upstream_tables ARRAY<STRING>,
            downstream_tables ARRAY<STRING>, primary_key_columns ARRAY<STRING>,
            foreign_keys MAP<STRING, STRING>, clustering_columns ARRAY<STRING>,
            partition_columns ARRAY<STRING>, row_filter_policy STRING,
            column_mask_policies MAP<STRING, STRING>, table_size_bytes BIGINT,
            num_files INT, data_source_format STRING, connection_name STRING,
            connection_type STRING, connection_url STRING, updated_at TIMESTAMP
        """
    
    def merge_to_target(self, staged_df: DataFrame) -> Dict[str, int]:
        """Merge staged updates into target table."""
        staged_df.createOrReplaceTempView("staged_extended_metadata")
        
        # MERGE: Upserts extended metadata on `table_name` from `staged_extended_metadata`; MATCH applies COALESCE per field then forces `updated_at` from source; NOT MATCHED `INSERT *` for new tables.
        # WHY: Stores operational enrichments—lineage, keys, clustering/partition layout, size/file counts, policy placeholders—alongside semantic KB for dashboards and agents without repeated system/DESCRIBE queries.
        # TRADEOFFS: No steward review gate (unlike semantic KB)—bad source snapshots need rebuild or manual fix; COALESCE avoids null wipes on partial runs but can retain stale complex fields if new run omits them; extraction limits (e.g. DESCRIBE batching) can leave gaps in huge scopes.
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_target} AS target
        USING staged_extended_metadata AS source
        ON target.table_name = source.table_name

        WHEN MATCHED THEN UPDATE SET
            target.table_type = COALESCE(source.table_type, target.table_type),
            target.table_owner = COALESCE(source.table_owner, target.table_owner),
            target.created = COALESCE(source.created, target.created),
            target.last_altered = COALESCE(source.last_altered, target.last_altered),
            target.column_count = COALESCE(source.column_count, target.column_count),
            target.upstream_tables = COALESCE(source.upstream_tables, target.upstream_tables),
            target.downstream_tables = COALESCE(source.downstream_tables, target.downstream_tables),
            target.primary_key_columns = COALESCE(source.primary_key_columns, target.primary_key_columns),
            target.foreign_keys = COALESCE(source.foreign_keys, target.foreign_keys),
            target.clustering_columns = COALESCE(source.clustering_columns, target.clustering_columns),
            target.partition_columns = COALESCE(source.partition_columns, target.partition_columns),
            target.row_filter_policy = COALESCE(source.row_filter_policy, target.row_filter_policy),
            target.column_mask_policies = COALESCE(source.column_mask_policies, target.column_mask_policies),
            target.table_size_bytes = COALESCE(source.table_size_bytes, target.table_size_bytes),
            target.num_files = COALESCE(source.num_files, target.num_files),
            target.data_source_format = COALESCE(source.data_source_format, target.data_source_format),
            target.connection_name = COALESCE(source.connection_name, target.connection_name),
            target.connection_type = COALESCE(source.connection_type, target.connection_type),
            target.connection_url = COALESCE(source.connection_url, target.connection_url),
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_target}"
        ).collect()[0]["cnt"]
        
        return {"total_records": count}
    
    def run(self) -> Dict[str, Any]:
        """Execute the full ETL pipeline."""
        logger.info(f"Starting extended metadata extraction: {self.config.fully_qualified_target}")
        
        self.create_target_table()
        staged_df = self.build_staged_updates()
        staged_count = staged_df.count()
        logger.info(f"Staged {staged_count} table records for merge")
        
        merge_stats = self.merge_to_target(staged_df)
        logger.info(f"Extended metadata extraction complete. Total records: {merge_stats['total_records']}")
        
        return {
            "staged_count": staged_count,
            "total_records": merge_stats["total_records"]
        }


def extract_extended_metadata(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    incremental: bool = True,
    table_names: list[str] | None = None,
    federation_mode: bool = False,
    metadata_companion_path: str = "",
) -> Dict[str, Any]:
    """Convenience function to extract extended metadata."""
    from dbxmetagen.processing import _check_federation_guard
    _check_federation_guard(spark, catalog_name, schema_name, table_names, federation_mode)

    config = ExtendedMetadataConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        incremental=incremental,
        table_names=table_names,
        federation_mode=federation_mode,
        metadata_companion_path=metadata_companion_path,
    )
    builder = ExtendedMetadataBuilder(spark, config)
    return builder.run()


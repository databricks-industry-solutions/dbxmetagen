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

logger = logging.getLogger(__name__)


@dataclass
class ExtendedMetadataConfig:
    """Configuration for extended metadata extraction."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    target_table: str = "extended_table_metadata"
    
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
            updated_at TIMESTAMP
        )
        COMMENT 'Extended table metadata from system tables'
        """
        self.spark.sql(ddl)
        logger.info(f"Target table {self.config.fully_qualified_target} ready")
    
    def get_tables_to_process(self) -> List[str]:
        """Get list of tables from knowledge base to extract metadata for."""
        df = self.spark.sql(f"""
            SELECT DISTINCT table_name 
            FROM {self.config.fully_qualified_source}
            WHERE table_name IS NOT NULL
        """)
        return [row.table_name for row in df.collect()]
    
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
                        last_altered
                    FROM {catalog}.information_schema.tables
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Could not query information_schema for catalog {catalog}: {e}")
        
        if all_dfs:
            from functools import reduce
            combined = reduce(DataFrame.union, all_dfs)
            # Filter to only tables in our list
            tables_df = self.spark.createDataFrame([(t,) for t in tables], ["filter_table"])
            return combined.join(tables_df, combined.table_name == tables_df.filter_table, "inner").drop("filter_table")
        
        return self.spark.createDataFrame([], self._get_basic_schema())
    
    def _get_basic_schema(self) -> str:
        return "table_name STRING, table_type STRING, table_owner STRING, created TIMESTAMP, last_altered TIMESTAMP"
    
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
                    FROM {catalog}.information_schema.columns
                    GROUP BY table_catalog, table_schema, table_name
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Could not query columns for catalog {catalog}: {e}")
        
        if all_dfs:
            from functools import reduce
            combined = reduce(DataFrame.union, all_dfs)
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
            logger.warning(f"Could not query lineage: {e}")
            return self.spark.createDataFrame([], "table_name STRING, upstream_tables ARRAY<STRING>, downstream_tables ARRAY<STRING>")
    
    def extract_constraints(self, tables: List[str]) -> DataFrame:
        """Extract primary key and foreign key constraints."""
        if not tables:
            return self.spark.createDataFrame([], "table_name STRING, primary_key_columns ARRAY<STRING>, foreign_keys MAP<STRING, STRING>")
        
        all_dfs = []
        catalogs = set(t.split(".")[0] for t in tables if "." in t)
        
        for catalog in catalogs:
            try:
                # Get constraints with their columns
                df = self.spark.sql(f"""
                    SELECT 
                        CONCAT(t.constraint_catalog, '.', t.constraint_schema, '.', t.table_name) as table_name,
                        t.constraint_type,
                        c.column_name
                    FROM {catalog}.information_schema.table_constraints t
                    LEFT JOIN {catalog}.information_schema.constraint_column_usage c
                        ON t.constraint_catalog = c.constraint_catalog 
                        AND t.constraint_schema = c.constraint_schema 
                        AND t.constraint_name = c.constraint_name
                    WHERE t.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Could not query constraints for catalog {catalog}: {e}")
        
        if all_dfs:
            from functools import reduce
            combined = reduce(DataFrame.union, all_dfs)
            
            # Aggregate by table
            pk_df = (
                combined
                .filter(F.col("constraint_type") == "PRIMARY KEY")
                .groupBy("table_name")
                .agg(F.collect_set("column_name").alias("primary_key_columns"))
            )
            
            # For foreign keys, we'd need additional join info - simplified for now
            fk_df = (
                combined
                .filter(F.col("constraint_type") == "FOREIGN KEY")
                .groupBy("table_name")
                .agg(F.map_from_arrays(
                    F.collect_list("column_name"),
                    F.collect_list(F.lit(""))  # Would need ref table info
                ).alias("foreign_keys"))
            )
            
            result = pk_df.join(fk_df, "table_name", "full_outer")
            tables_df = self.spark.createDataFrame([(t,) for t in tables], ["filter_table"])
            return result.join(tables_df, result.table_name == tables_df.filter_table, "inner").drop("filter_table")
        
        return self.spark.createDataFrame([], "table_name STRING, primary_key_columns ARRAY<STRING>, foreign_keys MAP<STRING, STRING>")
    
    def extract_table_properties(self, tables: List[str]) -> DataFrame:
        """Extract clustering and partition info using DESCRIBE EXTENDED."""
        if not tables:
            return self.spark.createDataFrame([], 
                "table_name STRING, clustering_columns ARRAY<STRING>, partition_columns ARRAY<STRING>, table_size_bytes BIGINT, num_files INT")
        
        results = []
        for table in tables[:100]:  # Limit to avoid too many queries
            try:
                # Try DESCRIBE DETAIL for Delta tables
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
        )
        
        # Add placeholders for governance columns (would need additional system table access)
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
            num_files INT, updated_at TIMESTAMP
        """
    
    def merge_to_target(self, staged_df: DataFrame) -> Dict[str, int]:
        """Merge staged updates into target table."""
        staged_df.createOrReplaceTempView("staged_extended_metadata")
        
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
    schema_name: str
) -> Dict[str, Any]:
    """
    Convenience function to extract extended metadata.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for source and target tables
        schema_name: Schema name for source and target tables
        
    Returns:
        Dict with execution statistics
    """
    config = ExtendedMetadataConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = ExtendedMetadataBuilder(spark, config)
    return builder.run()


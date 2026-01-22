"""
Column Knowledge Base ETL module.

Transforms row-based metadata_generation_log into column-centric column_knowledge_base
with aggregated column metadata from comment and PI classification runs.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


@dataclass
class ColumnKnowledgeBaseConfig:
    """Configuration for column knowledge base ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "metadata_generation_log"
    target_table: str = "column_knowledge_base"
    
    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_target(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.target_table}"


class ColumnKnowledgeBaseBuilder:
    """
    Builder class for transforming metadata_generation_log into column_knowledge_base.
    
    Extracts column-level metadata including comments, PI classifications, and data types.
    """
    
    def __init__(self, spark: SparkSession, config: ColumnKnowledgeBaseConfig):
        self.spark = spark
        self.config = config
    
    def create_target_table(self) -> None:
        """Create the target table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_target} (
            column_id STRING NOT NULL,
            table_name STRING,
            catalog STRING,
            `schema` STRING,
            table_short_name STRING,
            column_name STRING,
            comment STRING,
            data_type STRING,
            classification STRING,
            classification_type STRING,
            confidence FLOAT,
            nullable BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        CLUSTER BY (catalog, `schema`, table_short_name)
        COMMENT 'Aggregated column-level metadata from dbxmetagen runs'
        """
        self.spark.sql(ddl)
        logger.info(f"Target table {self.config.fully_qualified_target} ready")
    
    def read_source_data(self) -> DataFrame:
        """Read and filter source data for column-level records."""
        df = self.spark.sql(f"""
            SELECT 
                `table` as table_name,
                metadata_type,
                ddl_type,
                column_name,
                column_content,
                classification,
                type,
                confidence,
                _created_at
            FROM {self.config.fully_qualified_source}
            WHERE `table` IS NOT NULL
              AND column_name IS NOT NULL
              AND ddl_type = 'column'
        """)
        return df
    
    def extract_column_comments(self, source_df: DataFrame) -> DataFrame:
        """Extract column comments, keeping most recent per column."""
        window = Window.partitionBy("table_name", "column_name").orderBy(F.desc("_created_at"))
        
        return (
            source_df
            .filter(F.col("metadata_type") == "comment")
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select(
                F.col("table_name"),
                F.col("column_name"),
                F.col("column_content").alias("comment")
            )
        )
    
    def extract_pi_data(self, source_df: DataFrame) -> DataFrame:
        """Extract PI classifications, keeping most recent per column."""
        window = Window.partitionBy("table_name", "column_name").orderBy(F.desc("_created_at"))
        
        return (
            source_df
            .filter(F.col("metadata_type") == "pi")
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select(
                F.col("table_name"),
                F.col("column_name"),
                F.col("classification"),
                F.col("type").alias("classification_type"),
                F.col("confidence")
            )
        )
    
    def get_all_columns_with_timestamps(self, source_df: DataFrame) -> DataFrame:
        """Get all distinct columns with their first and last seen timestamps."""
        return (
            source_df
            .groupBy("table_name", "column_name")
            .agg(
                F.min("_created_at").alias("first_seen"),
                F.max("_created_at").alias("last_updated")
            )
        )
    
    def get_column_metadata_from_information_schema(self) -> DataFrame:
        """Fetch column data types and nullable info from information_schema."""
        try:
            # Get list of catalogs from our source data
            catalogs_df = self.spark.sql(f"""
                SELECT DISTINCT SPLIT(`table`, '\\\\.')[0] as catalog_name
                FROM {self.config.fully_qualified_source}
                WHERE `table` IS NOT NULL
            """)
            catalogs = [row.catalog_name for row in catalogs_df.collect()]
            
            if not catalogs:
                return self.spark.createDataFrame([], "table_name STRING, column_name STRING, data_type STRING, nullable BOOLEAN")
            
            # Query information_schema for each catalog
            dfs = []
            for catalog in catalogs:
                try:
                    df = self.spark.sql(f"""
                        SELECT 
                            CONCAT(table_catalog, '.', table_schema, '.', table_name) as table_name,
                            column_name,
                            data_type,
                            CASE WHEN is_nullable = 'YES' THEN true ELSE false END as nullable
                        FROM {catalog}.information_schema.columns
                    """)
                    dfs.append(df)
                except Exception as e:
                    logger.warning(f"Could not query information_schema for catalog {catalog}: {e}")
            
            if dfs:
                from functools import reduce
                return reduce(DataFrame.union, dfs)
            else:
                return self.spark.createDataFrame([], "table_name STRING, column_name STRING, data_type STRING, nullable BOOLEAN")
        except Exception as e:
            logger.warning(f"Could not fetch column metadata from information_schema: {e}")
            return self.spark.createDataFrame([], "table_name STRING, column_name STRING, data_type STRING, nullable BOOLEAN")
    
    def build_staged_updates(self) -> DataFrame:
        """Build the staged updates DataFrame by joining all metadata types."""
        source_df = self.read_source_data()
        source_df.cache()
        
        all_columns = self.get_all_columns_with_timestamps(source_df)
        column_comments = self.extract_column_comments(source_df)
        pi_data = self.extract_pi_data(source_df)
        column_metadata = self.get_column_metadata_from_information_schema()
        
        # Join all data together
        result = (
            all_columns
            .join(column_comments, ["table_name", "column_name"], "left")
            .join(pi_data, ["table_name", "column_name"], "left")
            .join(column_metadata, ["table_name", "column_name"], "left")
        )
        
        # Build column_id and parse table name parts
        result = (
            result
            .withColumn("column_id", F.concat_ws(".", F.col("table_name"), F.col("column_name")))
            .withColumn("catalog", F.split(F.col("table_name"), "\\.").getItem(0))
            .withColumn(
                "schema",
                F.when(
                    F.size(F.split(F.col("table_name"), "\\.")) >= 2,
                    F.split(F.col("table_name"), "\\.").getItem(1)
                ).otherwise(F.lit(None))
            )
            .withColumn(
                "table_short_name",
                F.when(
                    F.size(F.split(F.col("table_name"), "\\.")) >= 3,
                    F.split(F.col("table_name"), "\\.").getItem(2)
                ).otherwise(F.col("table_name"))
            )
            .withColumnRenamed("first_seen", "created_at")
            .withColumnRenamed("last_updated", "updated_at")
        )
        
        source_df.unpersist()
        
        return result.select(
            "column_id", "table_name", "catalog", 
            F.col("schema").alias("schema"),
            "table_short_name", "column_name", "comment",
            "data_type", "classification", "classification_type",
            "confidence", "nullable", "created_at", "updated_at"
        )
    
    def merge_to_target(self, staged_df: DataFrame) -> Dict[str, int]:
        """Merge staged updates into the target table."""
        staged_df.createOrReplaceTempView("staged_column_updates")
        
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_target} AS target
        USING staged_column_updates AS source
        ON target.column_id = source.column_id

        WHEN MATCHED THEN UPDATE SET
            target.table_name = COALESCE(source.table_name, target.table_name),
            target.catalog = COALESCE(source.catalog, target.catalog),
            target.`schema` = COALESCE(source.`schema`, target.`schema`),
            target.table_short_name = COALESCE(source.table_short_name, target.table_short_name),
            target.column_name = COALESCE(source.column_name, target.column_name),
            target.comment = COALESCE(source.comment, target.comment),
            target.data_type = COALESCE(source.data_type, target.data_type),
            target.classification = COALESCE(source.classification, target.classification),
            target.classification_type = COALESCE(source.classification_type, target.classification_type),
            target.confidence = COALESCE(source.confidence, target.confidence),
            target.nullable = COALESCE(source.nullable, target.nullable),
            target.updated_at = GREATEST(source.updated_at, target.updated_at)

        WHEN NOT MATCHED THEN INSERT (
            column_id, table_name, catalog, `schema`, table_short_name,
            column_name, comment, data_type, classification, classification_type,
            confidence, nullable, created_at, updated_at
        ) VALUES (
            source.column_id, source.table_name, source.catalog, source.`schema`,
            source.table_short_name, source.column_name, source.comment, source.data_type,
            source.classification, source.classification_type, source.confidence,
            source.nullable, source.created_at, source.updated_at
        )
        """
        
        self.spark.sql(merge_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_target}"
        ).collect()[0]["cnt"]
        
        return {"total_records": count}
    
    def run(self) -> Dict[str, Any]:
        """Execute the full ETL pipeline."""
        logger.info(f"Starting column knowledge base build: {self.config.fully_qualified_target}")
        
        self.create_target_table()
        staged_df = self.build_staged_updates()
        staged_count = staged_df.count()
        logger.info(f"Staged {staged_count} column records for merge")
        
        merge_stats = self.merge_to_target(staged_df)
        logger.info(f"Column knowledge base build complete. Total records: {merge_stats['total_records']}")
        
        return {
            "staged_count": staged_count,
            "total_records": merge_stats["total_records"]
        }


def build_column_knowledge_base(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str
) -> Dict[str, Any]:
    """
    Convenience function to build the column knowledge base.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for source and target tables
        schema_name: Schema name for source and target tables
        
    Returns:
        Dict with execution statistics
    """
    config = ColumnKnowledgeBaseConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = ColumnKnowledgeBaseBuilder(spark, config)
    return builder.run()


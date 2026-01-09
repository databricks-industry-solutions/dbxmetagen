"""
Knowledge Base ETL module.

Transforms row-based metadata_generation_log into table-centric table_knowledge_base
with aggregated metadata from comment, domain, and PI classification runs.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


@dataclass
class KnowledgeBaseConfig:
    """Configuration for knowledge base ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "metadata_generation_log"
    target_table: str = "table_knowledge_base"
    
    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_target(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.target_table}"


def parse_table_name_parts(table_name: str) -> Dict[str, Optional[str]]:
    """
    Parse a fully qualified table name into catalog, schema, and table parts.
    
    Args:
        table_name: Fully qualified table name (e.g., "catalog.schema.table")
        
    Returns:
        Dict with keys: catalog, schema, table_short_name
    """
    if not table_name:
        return {"catalog": None, "schema": None, "table_short_name": None}
    
    parts = table_name.split(".")
    
    return {
        "catalog": parts[0] if len(parts) >= 1 else None,
        "schema": parts[1] if len(parts) >= 2 else None,
        "table_short_name": parts[2] if len(parts) >= 3 else table_name
    }


def classify_has_pii(classification: Optional[str]) -> bool:
    """
    Determine if a classification indicates PII presence.
    
    Args:
        classification: The PI classification value (e.g., 'pii', 'phi', 'pci')
        
    Returns:
        True if classification indicates PII/PHI/PCI, False otherwise
    """
    if not classification:
        return False
    return classification.lower() in ('pii', 'phi', 'pci')


def classify_has_phi(classification: Optional[str]) -> bool:
    """
    Determine if a classification indicates PHI presence.
    
    Args:
        classification: The PI classification value
        
    Returns:
        True if classification indicates PHI specifically, False otherwise
    """
    if not classification:
        return False
    return classification.lower() == 'phi'


class KnowledgeBaseBuilder:
    """
    Builder class for transforming metadata_generation_log into table_knowledge_base.
    
    This class encapsulates the ETL logic and can be used from notebooks or jobs.
    """
    
    def __init__(self, spark: SparkSession, config: KnowledgeBaseConfig):
        """
        Initialize the knowledge base builder.
        
        Args:
            spark: SparkSession instance
            config: KnowledgeBaseConfig with source/target settings
        """
        self.spark = spark
        self.config = config
    
    def create_target_table(self) -> None:
        """Create the target table if it doesn't exist."""
        # Note: `schema` is a reserved word in SQL, must be escaped with backticks
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_target} (
            table_name STRING NOT NULL,
            catalog STRING,
            `schema` STRING,
            table_short_name STRING,
            comment STRING,
            domain STRING,
            subdomain STRING,
            has_pii BOOLEAN,
            has_phi BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        CLUSTER BY (catalog, `schema`)
        COMMENT 'Aggregated table-level metadata from dbxmetagen runs'
        """
        self.spark.sql(ddl)
        logger.info(f"Target table {self.config.fully_qualified_target} ready")
    
    def read_source_data(self) -> DataFrame:
        """
        Read and filter source data from metadata_generation_log.
        
        Returns:
            DataFrame with source data filtered to non-null table names
        """
        df = self.spark.sql(f"""
            SELECT 
                `table` as table_name,
                metadata_type,
                ddl_type,
                column_name,
                column_content,
                domain,
                subdomain,
                classification,
                _created_at
            FROM {self.config.fully_qualified_source}
            WHERE `table` IS NOT NULL
        """)
        return df
    
    def extract_table_comments(self, source_df: DataFrame) -> DataFrame:
        """
        Extract table-level comments, keeping most recent per table.
        
        Args:
            source_df: Source DataFrame from metadata_generation_log
            
        Returns:
            DataFrame with table_name and comment columns
        """
        window = Window.partitionBy("table_name").orderBy(F.desc("_created_at"))
        
        return (
            source_df
            .filter(
                (F.col("metadata_type") == "comment") & 
                (F.col("ddl_type") == "table")
            )
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select(
                F.col("table_name"),
                F.col("column_content").alias("comment")
            )
        )
    
    def extract_domain_data(self, source_df: DataFrame) -> DataFrame:
        """
        Extract domain classification, keeping most recent per table.
        
        Args:
            source_df: Source DataFrame from metadata_generation_log
            
        Returns:
            DataFrame with table_name, domain, subdomain columns
        """
        window = Window.partitionBy("table_name").orderBy(F.desc("_created_at"))
        
        return (
            source_df
            .filter(
                (F.col("metadata_type") == "domain") & 
                (F.col("ddl_type") == "table")
            )
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select(
                F.col("table_name"),
                F.col("domain"),
                F.col("subdomain")
            )
        )
    
    def extract_pi_data(self, source_df: DataFrame) -> DataFrame:
        """
        Aggregate PI classifications at table level.
        
        A table has_pii if ANY column has PII/PHI/PCI classification.
        A table has_phi if ANY column has PHI classification.
        
        Args:
            source_df: Source DataFrame from metadata_generation_log
            
        Returns:
            DataFrame with table_name, has_pii, has_phi columns
        """
        return (
            source_df
            .filter(F.col("metadata_type") == "pi")
            .groupBy("table_name")
            .agg(
                F.max(
                    F.when(
                        F.lower(F.col("classification")).isin("pii", "phi", "pci"),
                        F.lit(True)
                    ).otherwise(F.lit(False))
                ).alias("has_pii"),
                F.max(
                    F.when(
                        F.lower(F.col("classification")) == "phi",
                        F.lit(True)
                    ).otherwise(F.lit(False))
                ).alias("has_phi")
            )
        )
    
    def get_all_tables_with_timestamps(self, source_df: DataFrame) -> DataFrame:
        """
        Get all distinct tables with their first and last seen timestamps.
        
        Args:
            source_df: Source DataFrame from metadata_generation_log
            
        Returns:
            DataFrame with table_name, first_seen, last_updated columns
        """
        return (
            source_df
            .groupBy("table_name")
            .agg(
                F.min("_created_at").alias("first_seen"),
                F.max("_created_at").alias("last_updated")
            )
        )
    
    def build_staged_updates(self) -> DataFrame:
        """
        Build the staged updates DataFrame by joining all metadata types.
        
        Returns:
            DataFrame ready to be merged into the target table
        """
        source_df = self.read_source_data()
        
        # Cache the source for multiple passes
        source_df.cache()
        
        all_tables = self.get_all_tables_with_timestamps(source_df)
        table_comments = self.extract_table_comments(source_df)
        domain_data = self.extract_domain_data(source_df)
        pi_data = self.extract_pi_data(source_df)
        
        # Join all data together
        result = (
            all_tables
            .join(table_comments, "table_name", "left")
            .join(domain_data, "table_name", "left")
            .join(pi_data, "table_name", "left")
        )
        
        # Parse table name parts and add defaults for PI columns
        result = (
            result
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
            .withColumn("has_pii", F.coalesce(F.col("has_pii"), F.lit(False)))
            .withColumn("has_phi", F.coalesce(F.col("has_phi"), F.lit(False)))
            .withColumnRenamed("first_seen", "created_at")
            .withColumnRenamed("last_updated", "updated_at")
        )
        
        # Unpersist the cached source
        source_df.unpersist()
        
        # Rename 'schema' to escaped version for SQL compatibility
        return result.select(
            "table_name", "catalog", 
            F.col("schema").alias("schema"),  # PySpark handles escaping internally
            "table_short_name",
            "comment", "domain", "subdomain", "has_pii", "has_phi",
            "created_at", "updated_at"
        )
    
    def merge_to_target(self, staged_df: DataFrame) -> Dict[str, int]:
        """
        Merge staged updates into the target table.
        
        Uses COALESCE to preserve existing values when source has NULL,
        and OR logic for boolean flags to ensure once-true-always-true.
        
        Args:
            staged_df: DataFrame with staged updates
            
        Returns:
            Dict with merge statistics (rows_affected)
        """
        # Create temp view for the merge
        staged_df.createOrReplaceTempView("staged_updates")
        
        # Note: `schema` is a reserved word in SQL, must be escaped with backticks
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_target} AS target
        USING staged_updates AS source
        ON target.table_name = source.table_name

        WHEN MATCHED THEN UPDATE SET
            target.catalog = COALESCE(source.catalog, target.catalog),
            target.`schema` = COALESCE(source.`schema`, target.`schema`),
            target.table_short_name = COALESCE(source.table_short_name, target.table_short_name),
            target.comment = COALESCE(source.comment, target.comment),
            target.domain = COALESCE(source.domain, target.domain),
            target.subdomain = COALESCE(source.subdomain, target.subdomain),
            target.has_pii = source.has_pii OR target.has_pii,
            target.has_phi = source.has_phi OR target.has_phi,
            target.updated_at = GREATEST(source.updated_at, target.updated_at)

        WHEN NOT MATCHED THEN INSERT (
            table_name, catalog, `schema`, table_short_name,
            comment, domain, subdomain, has_pii, has_phi,
            created_at, updated_at
        ) VALUES (
            source.table_name, source.catalog, source.`schema`, source.table_short_name,
            source.comment, source.domain, source.subdomain, source.has_pii, source.has_phi,
            source.created_at, source.updated_at
        )
        """
        
        self.spark.sql(merge_sql)
        
        # Get count of records in target
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_target}"
        ).collect()[0]["cnt"]
        
        return {"total_records": count}
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the full ETL pipeline.
        
        Returns:
            Dict with execution statistics
        """
        logger.info(f"Starting knowledge base build: {self.config.fully_qualified_target}")
        
        # Create target table
        self.create_target_table()
        
        # Build staged updates
        staged_df = self.build_staged_updates()
        staged_count = staged_df.count()
        logger.info(f"Staged {staged_count} table records for merge")
        
        # Merge to target
        merge_stats = self.merge_to_target(staged_df)
        
        logger.info(f"Knowledge base build complete. Total records: {merge_stats['total_records']}")
        
        return {
            "staged_count": staged_count,
            "total_records": merge_stats["total_records"]
        }


def build_knowledge_base(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str
) -> Dict[str, Any]:
    """
    Convenience function to build the knowledge base.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for source and target tables
        schema_name: Schema name for source and target tables
        
    Returns:
        Dict with execution statistics
    """
    config = KnowledgeBaseConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = KnowledgeBaseBuilder(spark, config)
    return builder.run()


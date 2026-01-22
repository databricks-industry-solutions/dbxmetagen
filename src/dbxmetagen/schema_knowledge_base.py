"""
Schema Knowledge Base ETL module.

Aggregates table-level metadata into schema-level summaries with AI-generated
comments, domain classification, and PII/PHI aggregation.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class SchemaKnowledgeBaseConfig:
    """Configuration for schema knowledge base ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    target_table: str = "schema_knowledge_base"
    model: str = "databricks-meta-llama-3-3-70b-instruct"
    
    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_target(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.target_table}"


class SchemaSummarizer:
    """Generates AI summaries for schemas based on their table comments."""
    
    SYSTEM_PROMPT = """You are an expert at summarizing database schema contents.
Given a list of table names and their descriptions from a schema, generate a concise
summary that describes the overall purpose and contents of the schema.

Instructions:
1. Focus on the common themes and relationships between tables
2. Identify the primary domain or business function
3. Be concise - aim for 100-200 words
4. Use clear, professional language
5. Do not add markdown headers or formatting
6. Focus on facts from the provided descriptions, avoid speculation
"""

    def __init__(self, spark: SparkSession, model: str):
        self.spark = spark
        self.model = model
    
    def summarize_schema(self, schema_id: str, tables_df: DataFrame) -> str:
        """Generate AI summary for a schema based on its tables."""
        try:
            # Collect table info
            tables = tables_df.select("table_short_name", "comment", "domain").collect()
            
            if not tables:
                return None
            
            # Build prompt content
            table_descriptions = []
            for t in tables:
                desc = f"- {t.table_short_name}"
                if t.comment:
                    # Truncate long comments
                    comment = t.comment[:500] + "..." if len(t.comment or "") > 500 else t.comment
                    desc += f": {comment}"
                if t.domain:
                    desc += f" [Domain: {t.domain}]"
                table_descriptions.append(desc)
            
            # Limit number of tables to avoid token limits
            if len(table_descriptions) > 50:
                table_descriptions = table_descriptions[:50]
                table_descriptions.append(f"... and {len(tables) - 50} more tables")
            
            user_content = f"Schema: {schema_id}\n\nTables:\n" + "\n".join(table_descriptions)
            
            # Use AI_QUERY for summarization
            escaped_system = self.SYSTEM_PROMPT.replace("'", "''")
            escaped_user = user_content.replace("'", "''")
            
            result = self.spark.sql(f"""
                SELECT AI_QUERY(
                    '{self.model}',
                    CONCAT(
                        'System: {escaped_system}',
                        '\\n\\nUser: {escaped_user}'
                    )
                ) as summary
            """).collect()[0]['summary']
            
            return result
        except Exception as e:
            logger.warning(f"Could not generate summary for schema {schema_id}: {e}")
            return None


class SchemaKnowledgeBaseBuilder:
    """
    Builder class for transforming table_knowledge_base into schema_knowledge_base.
    
    Aggregates table-level metadata into schema-level summaries.
    """
    
    def __init__(self, spark: SparkSession, config: SchemaKnowledgeBaseConfig):
        self.spark = spark
        self.config = config
        self.summarizer = SchemaSummarizer(spark, config.model)
    
    def create_target_table(self) -> None:
        """Create the target table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_target} (
            schema_id STRING NOT NULL,
            catalog STRING,
            schema_name STRING,
            comment STRING,
            domain STRING,
            has_pii BOOLEAN,
            has_phi BOOLEAN,
            table_count INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Aggregated schema-level metadata from table_knowledge_base'
        """
        self.spark.sql(ddl)
        logger.info(f"Target table {self.config.fully_qualified_target} ready")
    
    def read_source_data(self) -> DataFrame:
        """Read table knowledge base data."""
        return self.spark.sql(f"""
            SELECT 
                catalog,
                `schema`,
                table_name,
                table_short_name,
                comment,
                domain,
                subdomain,
                has_pii,
                has_phi,
                created_at,
                updated_at
            FROM {self.config.fully_qualified_source}
            WHERE catalog IS NOT NULL AND `schema` IS NOT NULL
        """)
    
    def aggregate_schema_metadata(self, source_df: DataFrame) -> DataFrame:
        """Aggregate table metadata at the schema level."""
        return (
            source_df
            .groupBy("catalog", "schema")
            .agg(
                # Count tables
                F.count("table_name").alias("table_count"),
                # Any table has PII/PHI
                F.max(F.col("has_pii").cast("int")).cast("boolean").alias("has_pii"),
                F.max(F.col("has_phi").cast("int")).cast("boolean").alias("has_phi"),
                # Most common domain (mode)
                F.first("domain").alias("domain"),
                # Timestamps
                F.min("created_at").alias("created_at"),
                F.max("updated_at").alias("updated_at")
            )
            .withColumn("schema_id", F.concat_ws(".", F.col("catalog"), F.col("schema")))
            .withColumnRenamed("schema", "schema_name")
        )
    
    def generate_schema_comments(self, aggregated_df: DataFrame, source_df: DataFrame) -> DataFrame:
        """Generate AI comments for each schema."""
        # Collect schemas to process
        schemas = aggregated_df.select("schema_id", "catalog", "schema_name").collect()
        
        comments_data = []
        for schema in schemas:
            schema_id = schema.schema_id
            catalog = schema.catalog
            schema_name = schema.schema_name
            
            # Get tables for this schema
            tables_df = source_df.filter(
                (F.col("catalog") == catalog) & 
                (F.col("schema") == schema_name)
            )
            
            # Generate summary
            comment = self.summarizer.summarize_schema(schema_id, tables_df)
            comments_data.append((schema_id, comment))
        
        # Create DataFrame with comments
        comments_df = self.spark.createDataFrame(comments_data, ["schema_id", "comment"])
        
        # Join back to aggregated data
        return aggregated_df.join(comments_df, "schema_id", "left")
    
    def build_staged_updates(self, generate_comments: bool = True) -> DataFrame:
        """Build staged updates for merge."""
        source_df = self.read_source_data()
        source_df.cache()
        
        aggregated_df = self.aggregate_schema_metadata(source_df)
        
        if generate_comments:
            result = self.generate_schema_comments(aggregated_df, source_df)
        else:
            result = aggregated_df.withColumn("comment", F.lit(None).cast("string"))
        
        source_df.unpersist()
        
        return result.select(
            "schema_id", "catalog", "schema_name", "comment",
            "domain", "has_pii", "has_phi", "table_count",
            "created_at", "updated_at"
        )
    
    def merge_to_target(self, staged_df: DataFrame) -> Dict[str, int]:
        """Merge staged updates into target table."""
        staged_df.createOrReplaceTempView("staged_schema_updates")
        
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_target} AS target
        USING staged_schema_updates AS source
        ON target.schema_id = source.schema_id

        WHEN MATCHED THEN UPDATE SET
            target.catalog = COALESCE(source.catalog, target.catalog),
            target.schema_name = COALESCE(source.schema_name, target.schema_name),
            target.comment = COALESCE(source.comment, target.comment),
            target.domain = COALESCE(source.domain, target.domain),
            target.has_pii = source.has_pii OR target.has_pii,
            target.has_phi = source.has_phi OR target.has_phi,
            target.table_count = source.table_count,
            target.updated_at = GREATEST(source.updated_at, target.updated_at)

        WHEN NOT MATCHED THEN INSERT (
            schema_id, catalog, schema_name, comment, domain,
            has_pii, has_phi, table_count, created_at, updated_at
        ) VALUES (
            source.schema_id, source.catalog, source.schema_name, source.comment,
            source.domain, source.has_pii, source.has_phi, source.table_count,
            source.created_at, source.updated_at
        )
        """
        
        self.spark.sql(merge_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_target}"
        ).collect()[0]["cnt"]
        
        return {"total_records": count}
    
    def run(self, generate_comments: bool = True) -> Dict[str, Any]:
        """Execute the full ETL pipeline."""
        logger.info(f"Starting schema knowledge base build: {self.config.fully_qualified_target}")
        
        self.create_target_table()
        staged_df = self.build_staged_updates(generate_comments)
        staged_count = staged_df.count()
        logger.info(f"Staged {staged_count} schema records for merge")
        
        merge_stats = self.merge_to_target(staged_df)
        logger.info(f"Schema knowledge base build complete. Total records: {merge_stats['total_records']}")
        
        return {
            "staged_count": staged_count,
            "total_records": merge_stats["total_records"]
        }


def build_schema_knowledge_base(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    generate_comments: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to build the schema knowledge base.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for source and target tables
        schema_name: Schema name for source and target tables
        generate_comments: Whether to generate AI comments for schemas
        
    Returns:
        Dict with execution statistics
    """
    config = SchemaKnowledgeBaseConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = SchemaKnowledgeBaseBuilder(spark, config)
    return builder.run(generate_comments)


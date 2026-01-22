"""
Knowledge Graph module for building GraphFrames-compatible node and edge tables.

Creates relationship edges between tables based on:
- Same domain
- Same subdomain  
- Same catalog
- Same schema
- Same security level (PII/PHI status)

Extended to support:
- Column nodes (from column_knowledge_base)
- Schema nodes (from schema_knowledge_base)
- Hierarchical relationships (contains, references, derives_from)
- Embedding-based similarity edges

Requires ML cluster (serverless doesn't support GraphFrames JVM dependencies).
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType

logger = logging.getLogger(__name__)


@dataclass
class KnowledgeGraphConfig:
    """Configuration for knowledge graph ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    nodes_table: str = "graph_nodes"
    edges_table: str = "graph_edges"
    
    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"
    
    @property
    def fully_qualified_edges(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.edges_table}"


def compute_security_level(has_pii: bool, has_phi: bool) -> str:
    """
    Compute security level from PII/PHI flags.
    
    Hierarchy: PHI > PII > PUBLIC
    
    Args:
        has_pii: Whether table has PII
        has_phi: Whether table has PHI
        
    Returns:
        Security level string
    """
    if has_phi:
        return "PHI"
    elif has_pii:
        return "PII"
    else:
        return "PUBLIC"


class KnowledgeGraphBuilder:
    """
    Builder for creating GraphFrames-compatible node and edge tables.
    
    Node table: One row per table with properties
    Edge table: Relationships between tables (same domain, schema, etc.)
    """
    
    # Relationship types we create edges for
    RELATIONSHIP_TYPES = [
        "same_domain",
        "same_subdomain", 
        "same_catalog",
        "same_schema",
        "same_security_level"
    ]
    
    def __init__(self, spark: SparkSession, config: KnowledgeGraphConfig):
        self.spark = spark
        self.config = config
    
    def create_nodes_table(self) -> None:
        """Create the nodes table if it doesn't exist."""
        # Note: `schema` is a reserved word in SQL, must be escaped with backticks
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_nodes} (
            id STRING NOT NULL,
            table_name STRING,
            catalog STRING,
            `schema` STRING,
            table_short_name STRING,
            domain STRING,
            subdomain STRING,
            has_pii BOOLEAN,
            has_phi BOOLEAN,
            security_level STRING,
            comment STRING,
            node_type STRING DEFAULT 'table',
            parent_id STRING,
            data_type STRING,
            quality_score DOUBLE,
            embedding ARRAY<FLOAT>,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'GraphFrames nodes - tables, columns, and schemas from knowledge base'
        """
        self.spark.sql(ddl)
        logger.info(f"Nodes table {self.config.fully_qualified_nodes} ready")
    
    def create_edges_table(self) -> None:
        """Create the edges table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_edges} (
            src STRING NOT NULL,
            dst STRING NOT NULL,
            relationship STRING NOT NULL,
            weight DOUBLE,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'GraphFrames edges - relationships between tables'
        """
        self.spark.sql(ddl)
        logger.info(f"Edges table {self.config.fully_qualified_edges} ready")
    
    def build_nodes_df(self) -> DataFrame:
        """
        Build nodes DataFrame from knowledge base.
        
        Each table becomes a node with id = table_name.
        """
        # Note: `schema` is a reserved word, escaped with backticks
        df = self.spark.sql(f"""
            SELECT 
                table_name,
                catalog,
                `schema`,
                table_short_name,
                domain,
                subdomain,
                has_pii,
                has_phi,
                comment,
                created_at,
                updated_at
            FROM {self.config.fully_qualified_source}
        """)
        
        # Add id column (required for GraphFrames) and security_level
        df = (
            df
            .withColumn("id", F.col("table_name"))
            .withColumn(
                "security_level",
                F.when(F.col("has_phi"), F.lit("PHI"))
                .when(F.col("has_pii"), F.lit("PII"))
                .otherwise(F.lit("PUBLIC"))
            )
            .withColumn("node_type", F.lit("table"))
            .withColumn("parent_id", F.concat_ws(".", F.col("catalog"), F.col("schema")))
            .withColumn("data_type", F.lit(None).cast("string"))
            .withColumn("quality_score", F.lit(None).cast("double"))
            .withColumn("embedding", F.lit(None).cast("array<float>"))
        )
        
        return df.select(
            "id", "table_name", "catalog", "schema", "table_short_name",
            "domain", "subdomain", "has_pii", "has_phi", "security_level",
            "comment", "node_type", "parent_id", "data_type", "quality_score",
            "embedding", "created_at", "updated_at"
        )
    
    def build_edges_for_attribute(
        self, 
        source_df: DataFrame, 
        attribute: str,
        relationship: str
    ) -> DataFrame:
        """
        Build edges between tables that share the same attribute value.
        
        Uses self-join to find pairs of tables with matching attribute.
        Only creates edges where src < dst to avoid duplicates.
        
        Args:
            source_df: DataFrame with id and attribute columns
            attribute: Column name to match on
            relationship: Name for this relationship type
            
        Returns:
            DataFrame with src, dst, relationship columns
        """
        # Filter to tables that have a value for this attribute
        df_with_attr = source_df.filter(F.col(attribute).isNotNull())
        
        # Self-join to find pairs with same attribute value
        # Use src < dst to avoid duplicate pairs and self-loops
        df_a = df_with_attr.select(
            F.col("id").alias("src"),
            F.col(attribute).alias("attr_a")
        )
        df_b = df_with_attr.select(
            F.col("id").alias("dst"),
            F.col(attribute).alias("attr_b")
        )
        
        edges = (
            df_a
            .join(df_b, df_a.attr_a == df_b.attr_b)
            .filter(F.col("src") < F.col("dst"))  # Avoid duplicates and self-loops
            .select("src", "dst")
            .withColumn("relationship", F.lit(relationship))
            .withColumn("weight", F.lit(1.0))
        )
        
        return edges
    
    def build_all_edges_df(self, nodes_df: DataFrame) -> DataFrame:
        """
        Build all edge types from nodes DataFrame.
        
        Creates edges for:
        - same_domain: tables in the same domain
        - same_subdomain: tables in the same subdomain
        - same_catalog: tables in the same catalog
        - same_schema: tables in the same schema
        - same_security_level: tables with same PII/PHI status
        """
        all_edges = []
        
        # Same domain edges
        domain_edges = self.build_edges_for_attribute(
            nodes_df, "domain", "same_domain"
        )
        all_edges.append(domain_edges)
        
        # Same subdomain edges
        subdomain_edges = self.build_edges_for_attribute(
            nodes_df, "subdomain", "same_subdomain"
        )
        all_edges.append(subdomain_edges)
        
        # Same catalog edges
        catalog_edges = self.build_edges_for_attribute(
            nodes_df, "catalog", "same_catalog"
        )
        all_edges.append(catalog_edges)
        
        # Same schema edges
        schema_edges = self.build_edges_for_attribute(
            nodes_df, "schema", "same_schema"
        )
        all_edges.append(schema_edges)
        
        # Same security level edges
        security_edges = self.build_edges_for_attribute(
            nodes_df, "security_level", "same_security_level"
        )
        all_edges.append(security_edges)
        
        # Union all edges
        from functools import reduce
        combined = reduce(DataFrame.union, all_edges)
        
        # Add timestamps
        combined = (
            combined
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )
        
        return combined
    
    def merge_nodes(self, nodes_df: DataFrame) -> Dict[str, int]:
        """
        Incrementally merge nodes into the nodes table.
        """
        nodes_df.createOrReplaceTempView("staged_nodes")
        
        # Note: `schema` is a reserved word in SQL, must be escaped with backticks
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_nodes} AS target
        USING staged_nodes AS source
        ON target.id = source.id

        WHEN MATCHED THEN UPDATE SET
            target.table_name = source.table_name,
            target.catalog = source.catalog,
            target.`schema` = source.`schema`,
            target.table_short_name = source.table_short_name,
            target.domain = COALESCE(source.domain, target.domain),
            target.subdomain = COALESCE(source.subdomain, target.subdomain),
            target.has_pii = source.has_pii OR target.has_pii,
            target.has_phi = source.has_phi OR target.has_phi,
            target.security_level = source.security_level,
            target.comment = COALESCE(source.comment, target.comment),
            target.node_type = COALESCE(source.node_type, target.node_type),
            target.parent_id = COALESCE(source.parent_id, target.parent_id),
            target.data_type = COALESCE(source.data_type, target.data_type),
            target.quality_score = COALESCE(source.quality_score, target.quality_score),
            target.embedding = COALESCE(source.embedding, target.embedding),
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            id, table_name, catalog, `schema`, table_short_name,
            domain, subdomain, has_pii, has_phi, security_level,
            comment, node_type, parent_id, data_type, quality_score,
            embedding, created_at, updated_at
        ) VALUES (
            source.id, source.table_name, source.catalog, source.`schema`, 
            source.table_short_name, source.domain, source.subdomain,
            source.has_pii, source.has_phi, source.security_level,
            source.comment, source.node_type, source.parent_id, source.data_type,
            source.quality_score, source.embedding, source.created_at, source.updated_at
        )
        """
        
        self.spark.sql(merge_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_nodes}"
        ).collect()[0]["cnt"]
        
        return {"total_nodes": count}
    
    def refresh_edges(self, edges_df: DataFrame) -> Dict[str, int]:
        """
        Refresh edges by deleting stale edges and inserting current ones.
        
        Unlike MERGE, this properly handles the case where relationships change
        (e.g., a table moves to a different domain - old domain edges should be removed).
        
        Strategy:
        1. Delete all edges involving nodes that are being updated
        2. Insert all current edges
        
        This ensures the graph always reflects the current state of relationships.
        """
        edges_df.createOrReplaceTempView("staged_edges")
        
        # Get all node IDs being updated (union of src and dst)
        affected_nodes = edges_df.select("src").union(edges_df.select(F.col("dst").alias("src"))).distinct()
        affected_nodes.createOrReplaceTempView("affected_nodes")
        
        # Delete existing edges involving affected nodes
        delete_sql = f"""
        DELETE FROM {self.config.fully_qualified_edges}
        WHERE src IN (SELECT src FROM affected_nodes)
           OR dst IN (SELECT src FROM affected_nodes)
        """
        self.spark.sql(delete_sql)
        
        # Insert all new edges
        insert_sql = f"""
        INSERT INTO {self.config.fully_qualified_edges}
        SELECT src, dst, relationship, weight, created_at, updated_at
        FROM staged_edges
        """
        self.spark.sql(insert_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_edges}"
        ).collect()[0]["cnt"]
        
        return {"total_edges": count}
    
    def merge_edges(self, edges_df: DataFrame) -> Dict[str, int]:
        """
        Incrementally merge edges into the edges table.
        
        Uses composite key (src, dst, relationship) for matching.
        
        Note: For most use cases, prefer refresh_edges() which properly handles
        relationship changes (e.g., when a table's domain changes).
        """
        edges_df.createOrReplaceTempView("staged_edges")
        
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_edges} AS target
        USING staged_edges AS source
        ON target.src = source.src 
           AND target.dst = source.dst 
           AND target.relationship = source.relationship

        WHEN MATCHED THEN UPDATE SET
            target.weight = source.weight,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            src, dst, relationship, weight, created_at, updated_at
        ) VALUES (
            source.src, source.dst, source.relationship, 
            source.weight, source.created_at, source.updated_at
        )
        """
        
        self.spark.sql(merge_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_edges}"
        ).collect()[0]["cnt"]
        
        return {"total_edges": count}
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the full graph building pipeline.
        
        Uses refresh strategy for edges to properly handle relationship changes.
        
        Returns:
            Dict with execution statistics
        """
        logger.info("Starting knowledge graph build")
        
        # Create tables
        self.create_nodes_table()
        self.create_edges_table()
        
        # Build nodes
        nodes_df = self.build_nodes_df()
        node_count = nodes_df.count()
        logger.info(f"Built {node_count} nodes")
        
        # Merge nodes (incremental)
        node_stats = self.merge_nodes(nodes_df)
        
        # Build and refresh edges (delete stale + insert current)
        edges_df = self.build_all_edges_df(nodes_df)
        edge_count = edges_df.count()
        logger.info(f"Built {edge_count} edges")
        
        # Use refresh_edges to handle relationship changes properly
        edge_stats = self.refresh_edges(edges_df)
        
        logger.info(f"Knowledge graph build complete")
        
        return {
            "staged_nodes": node_count,
            "staged_edges": edge_count,
            "total_nodes": node_stats["total_nodes"],
            "total_edges": edge_stats["total_edges"]
        }


def build_knowledge_graph(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str
) -> Dict[str, Any]:
    """
    Convenience function to build the knowledge graph.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        
    Returns:
        Dict with execution statistics
    """
    config = KnowledgeGraphConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = KnowledgeGraphBuilder(spark, config)
    return builder.run()


@dataclass
class ExtendedKnowledgeGraphConfig(KnowledgeGraphConfig):
    """Extended configuration including column and schema knowledge bases."""
    column_kb_table: str = "column_knowledge_base"
    schema_kb_table: str = "schema_knowledge_base"
    extended_metadata_table: str = "extended_table_metadata"
    
    @property
    def fully_qualified_column_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_kb_table}"
    
    @property
    def fully_qualified_schema_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.schema_kb_table}"
    
    @property
    def fully_qualified_extended_metadata(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.extended_metadata_table}"


class ExtendedKnowledgeGraphBuilder(KnowledgeGraphBuilder):
    """
    Extended builder that includes column and schema nodes plus additional edge types.
    
    Node types:
    - table: from table_knowledge_base (existing)
    - column: from column_knowledge_base (new)
    - schema: from schema_knowledge_base (new)
    
    Additional edge types:
    - contains: schema contains table, table contains column
    - references: foreign key relationships
    - derives_from: lineage relationships
    - similar_embedding: embedding similarity (added separately)
    """
    
    EXTENDED_RELATIONSHIP_TYPES = [
        "contains",
        "references",
        "derives_from"
    ]
    
    def __init__(self, spark: SparkSession, config: ExtendedKnowledgeGraphConfig):
        super().__init__(spark, config)
        self.ext_config = config
    
    def build_column_nodes_df(self) -> DataFrame:
        """Build nodes DataFrame from column knowledge base."""
        try:
            df = self.spark.sql(f"""
                SELECT 
                    column_id,
                    table_name,
                    catalog,
                    `schema`,
                    table_short_name,
                    column_name,
                    comment,
                    data_type,
                    classification,
                    confidence,
                    created_at,
                    updated_at
                FROM {self.ext_config.fully_qualified_column_kb}
            """)
            
            return (
                df
                .withColumn("id", F.col("column_id"))
                .withColumn("domain", F.lit(None).cast("string"))
                .withColumn("subdomain", F.lit(None).cast("string"))
                .withColumn("has_pii", F.lower(F.col("classification")).isin("pii", "phi", "pci"))
                .withColumn("has_phi", F.lower(F.col("classification")) == "phi")
                .withColumn(
                    "security_level",
                    F.when(F.lower(F.col("classification")) == "phi", F.lit("PHI"))
                    .when(F.lower(F.col("classification")).isin("pii", "pci"), F.lit("PII"))
                    .otherwise(F.lit("PUBLIC"))
                )
                .withColumn("node_type", F.lit("column"))
                .withColumn("parent_id", F.col("table_name"))
                .withColumn("quality_score", F.lit(None).cast("double"))
                .withColumn("embedding", F.lit(None).cast("array<float>"))
                .select(
                    "id", "table_name", "catalog", "schema", "table_short_name",
                    "domain", "subdomain", "has_pii", "has_phi", "security_level",
                    "comment", "node_type", "parent_id", "data_type", "quality_score",
                    "embedding", "created_at", "updated_at"
                )
            )
        except Exception as e:
            logger.warning(f"Could not build column nodes: {e}")
            return None
    
    def build_schema_nodes_df(self) -> DataFrame:
        """Build nodes DataFrame from schema knowledge base."""
        try:
            df = self.spark.sql(f"""
                SELECT 
                    schema_id,
                    catalog,
                    schema_name,
                    comment,
                    domain,
                    has_pii,
                    has_phi,
                    table_count,
                    created_at,
                    updated_at
                FROM {self.ext_config.fully_qualified_schema_kb}
            """)
            
            return (
                df
                .withColumn("id", F.col("schema_id"))
                .withColumn("table_name", F.lit(None).cast("string"))
                .withColumn("schema", F.col("schema_name"))
                .withColumn("table_short_name", F.lit(None).cast("string"))
                .withColumn("subdomain", F.lit(None).cast("string"))
                .withColumn(
                    "security_level",
                    F.when(F.col("has_phi"), F.lit("PHI"))
                    .when(F.col("has_pii"), F.lit("PII"))
                    .otherwise(F.lit("PUBLIC"))
                )
                .withColumn("node_type", F.lit("schema"))
                .withColumn("parent_id", F.col("catalog"))
                .withColumn("data_type", F.lit(None).cast("string"))
                .withColumn("quality_score", F.lit(None).cast("double"))
                .withColumn("embedding", F.lit(None).cast("array<float>"))
                .select(
                    "id", "table_name", "catalog", "schema", "table_short_name",
                    "domain", "subdomain", "has_pii", "has_phi", "security_level",
                    "comment", "node_type", "parent_id", "data_type", "quality_score",
                    "embedding", "created_at", "updated_at"
                )
            )
        except Exception as e:
            logger.warning(f"Could not build schema nodes: {e}")
            return None
    
    def build_containment_edges(self, nodes_df: DataFrame) -> DataFrame:
        """Build 'contains' edges for hierarchical relationships."""
        edges = []
        
        # Schema contains table
        schema_table_edges = (
            nodes_df
            .filter(F.col("node_type") == "table")
            .select(
                F.col("parent_id").alias("src"),
                F.col("id").alias("dst")
            )
            .filter(F.col("src").isNotNull())
            .withColumn("relationship", F.lit("contains"))
            .withColumn("weight", F.lit(1.0))
        )
        edges.append(schema_table_edges)
        
        # Table contains column
        table_column_edges = (
            nodes_df
            .filter(F.col("node_type") == "column")
            .select(
                F.col("parent_id").alias("src"),
                F.col("id").alias("dst")
            )
            .filter(F.col("src").isNotNull())
            .withColumn("relationship", F.lit("contains"))
            .withColumn("weight", F.lit(1.0))
        )
        edges.append(table_column_edges)
        
        if edges:
            from functools import reduce
            combined = reduce(DataFrame.union, edges)
            return (
                combined
                .withColumn("created_at", F.current_timestamp())
                .withColumn("updated_at", F.current_timestamp())
            )
        
        return self.spark.createDataFrame([], 
            "src STRING, dst STRING, relationship STRING, weight DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP")
    
    def build_lineage_edges(self) -> DataFrame:
        """Build 'derives_from' edges from lineage data."""
        try:
            df = self.spark.sql(f"""
                SELECT 
                    table_name as dst,
                    EXPLODE(upstream_tables) as src
                FROM {self.ext_config.fully_qualified_extended_metadata}
                WHERE upstream_tables IS NOT NULL
            """)
            
            return (
                df
                .withColumn("relationship", F.lit("derives_from"))
                .withColumn("weight", F.lit(1.0))
                .withColumn("created_at", F.current_timestamp())
                .withColumn("updated_at", F.current_timestamp())
            )
        except Exception as e:
            logger.warning(f"Could not build lineage edges: {e}")
            return self.spark.createDataFrame([], 
                "src STRING, dst STRING, relationship STRING, weight DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP")
    
    def build_reference_edges(self) -> DataFrame:
        """Build 'references' edges from foreign key data."""
        try:
            # This would require parsing the foreign_keys map from extended_metadata
            # For now, return empty - can be enhanced when FK data is available
            return self.spark.createDataFrame([], 
                "src STRING, dst STRING, relationship STRING, weight DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP")
        except Exception as e:
            logger.warning(f"Could not build reference edges: {e}")
            return self.spark.createDataFrame([], 
                "src STRING, dst STRING, relationship STRING, weight DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP")
    
    def build_all_extended_edges(self, all_nodes_df: DataFrame) -> DataFrame:
        """Build all edges including extended relationship types."""
        all_edges = []
        
        # Original edges (only for table nodes)
        table_nodes = all_nodes_df.filter(F.col("node_type") == "table")
        original_edges = super().build_all_edges_df(table_nodes)
        all_edges.append(original_edges)
        
        # Containment edges
        containment_edges = self.build_containment_edges(all_nodes_df)
        all_edges.append(containment_edges)
        
        # Lineage edges
        lineage_edges = self.build_lineage_edges()
        if lineage_edges.count() > 0:
            all_edges.append(lineage_edges)
        
        # Reference edges
        reference_edges = self.build_reference_edges()
        if reference_edges.count() > 0:
            all_edges.append(reference_edges)
        
        from functools import reduce
        return reduce(DataFrame.union, all_edges)
    
    def run(self, include_columns: bool = True, include_schemas: bool = True) -> Dict[str, Any]:
        """
        Execute the extended graph building pipeline.
        
        Args:
            include_columns: Whether to include column nodes
            include_schemas: Whether to include schema nodes
        """
        logger.info("Starting extended knowledge graph build")
        
        self.create_nodes_table()
        self.create_edges_table()
        
        # Build all node types
        all_nodes = []
        
        # Table nodes (always included)
        table_nodes = self.build_nodes_df()
        all_nodes.append(table_nodes)
        
        # Column nodes
        if include_columns:
            column_nodes = self.build_column_nodes_df()
            if column_nodes is not None:
                all_nodes.append(column_nodes)
                logger.info(f"Built {column_nodes.count()} column nodes")
        
        # Schema nodes
        if include_schemas:
            schema_nodes = self.build_schema_nodes_df()
            if schema_nodes is not None:
                all_nodes.append(schema_nodes)
                logger.info(f"Built {schema_nodes.count()} schema nodes")
        
        # Union all nodes
        from functools import reduce
        all_nodes_df = reduce(DataFrame.union, all_nodes)
        total_nodes = all_nodes_df.count()
        logger.info(f"Built {total_nodes} total nodes")
        
        # Merge nodes
        node_stats = self.merge_nodes(all_nodes_df)
        
        # Build and refresh edges
        edges_df = self.build_all_extended_edges(all_nodes_df)
        edge_count = edges_df.count()
        logger.info(f"Built {edge_count} edges")
        
        edge_stats = self.refresh_edges(edges_df)
        
        logger.info("Extended knowledge graph build complete")
        
        return {
            "staged_nodes": total_nodes,
            "staged_edges": edge_count,
            "total_nodes": node_stats["total_nodes"],
            "total_edges": edge_stats["total_edges"]
        }


def build_extended_knowledge_graph(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    include_columns: bool = True,
    include_schemas: bool = True
) -> Dict[str, Any]:
    """
    Build extended knowledge graph with column and schema nodes.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        include_columns: Include column-level nodes
        include_schemas: Include schema-level nodes
        
    Returns:
        Dict with execution statistics
    """
    config = ExtendedKnowledgeGraphConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    builder = ExtendedKnowledgeGraphBuilder(spark, config)
    return builder.run(include_columns, include_schemas)


# =============================================================================
# Example GraphFrames Queries
# =============================================================================

GRAPHFRAMES_EXAMPLES = """
# =============================================================================
# GraphFrames Query Examples for Knowledge Graph
# =============================================================================

# First, create the GraphFrame from node and edge tables:
from graphframes import GraphFrame

nodes = spark.table("catalog.schema.graph_nodes")
edges = spark.table("catalog.schema.graph_edges")

g = GraphFrame(nodes, edges)

# -----------------------------------------------------------------------------
# BASIC QUERIES
# -----------------------------------------------------------------------------

# 1. Find all tables in the same domain as a specific table
target_table = "catalog.schema.my_table"
same_domain_tables = g.edges.filter(
    (F.col("relationship") == "same_domain") & 
    ((F.col("src") == target_table) | (F.col("dst") == target_table))
).select(
    F.when(F.col("src") == target_table, F.col("dst"))
     .otherwise(F.col("src")).alias("related_table")
)

# 2. Find tables that share both domain AND schema (intersection)
domain_edges = g.edges.filter(F.col("relationship") == "same_domain")
schema_edges = g.edges.filter(F.col("relationship") == "same_schema")

closely_related = domain_edges.join(
    schema_edges,
    (domain_edges.src == schema_edges.src) & (domain_edges.dst == schema_edges.dst),
    "inner"
).select(domain_edges.src, domain_edges.dst)

# 3. Find all PHI tables and their connections
phi_tables = g.vertices.filter(F.col("security_level") == "PHI")
phi_edges = g.edges.join(phi_tables, g.edges.src == phi_tables.id, "inner")

# 4. Count relationships by type
relationship_counts = g.edges.groupBy("relationship").count().orderBy("count", ascending=False)

# -----------------------------------------------------------------------------
# MOTIF FINDING - Pattern Matching in Graphs
# -----------------------------------------------------------------------------

# Motifs use a special syntax: (a)-[e]->(b) where a,b are nodes and e is edge

# 5. Find triangles: three tables all connected to each other
triangles = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(c)")

# 6. Find tables that bridge two domains (connected to tables in different domains)
# Tables A and B in same schema, B and C in same domain (but A and C different domains)
bridging_tables = g.find("(a)-[e1]->(b); (b)-[e2]->(c)").filter(
    (F.col("e1.relationship") == "same_schema") & 
    (F.col("e2.relationship") == "same_domain") &
    (F.col("a.domain") != F.col("c.domain"))
)

# 7. Find paths between two specific tables
start_table = "catalog.schema.table_a"
end_table = "catalog.schema.table_b"

# Direct connection
direct = g.find("(a)-[e]->(b)").filter(
    ((F.col("a.id") == start_table) & (F.col("b.id") == end_table)) |
    ((F.col("a.id") == end_table) & (F.col("b.id") == start_table))
)

# Two-hop connection
two_hop = g.find("(a)-[e1]->(intermediate); (intermediate)-[e2]->(b)").filter(
    (F.col("a.id") == start_table) & (F.col("b.id") == end_table)
)

# 8. Find PHI tables connected to PII tables (security boundary analysis)
phi_pii_connections = g.find("(phi_table)-[e]->(pii_table)").filter(
    (F.col("phi_table.security_level") == "PHI") & 
    (F.col("pii_table.security_level") == "PII")
)

# -----------------------------------------------------------------------------
# GRAPH ALGORITHMS
# -----------------------------------------------------------------------------

# 9. PageRank - find most "important" tables (highly connected)
pagerank_results = g.pageRank(resetProbability=0.15, maxIter=10)
top_tables = pagerank_results.vertices.orderBy("pagerank", ascending=False).limit(20)

# 10. Connected Components - find clusters of related tables
components = g.connectedComponents()
cluster_sizes = components.groupBy("component").count().orderBy("count", ascending=False)

# 11. Label Propagation - community detection
communities = g.labelPropagation(maxIter=5)
community_sizes = communities.groupBy("label").count().orderBy("count", ascending=False)

# 12. Shortest paths to specific tables
landmark_tables = ["catalog.schema.core_customer", "catalog.schema.core_product"]
shortest_paths = g.shortestPaths(landmarks=landmark_tables)

# -----------------------------------------------------------------------------
# FILTERING AND SUBGRAPH ANALYSIS
# -----------------------------------------------------------------------------

# 13. Create subgraph of only PHI-related tables
phi_subgraph = GraphFrame(
    g.vertices.filter(F.col("security_level") == "PHI"),
    g.edges.filter(F.col("relationship") == "same_security_level")
)

# 14. Create subgraph of a specific domain
domain_filter = "Healthcare"
domain_vertices = g.vertices.filter(F.col("domain") == domain_filter)
domain_vertex_ids = domain_vertices.select("id").rdd.flatMap(lambda x: x).collect()

domain_edges = g.edges.filter(
    F.col("src").isin(domain_vertex_ids) & 
    F.col("dst").isin(domain_vertex_ids)
)
domain_subgraph = GraphFrame(domain_vertices, domain_edges)

# 15. Find isolated tables (no connections)
connected_nodes = g.edges.select("src").union(g.edges.select("dst")).distinct()
isolated = g.vertices.join(connected_nodes, g.vertices.id == connected_nodes.src, "left_anti")
"""


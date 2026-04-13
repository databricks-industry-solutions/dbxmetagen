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
from pyspark.sql.window import Window

from dbxmetagen.table_filter import table_filter_sql, infrastructure_exclude_sql

logger = logging.getLogger(__name__)


@dataclass
class KnowledgeGraphConfig:
    """Configuration for knowledge graph ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "table_knowledge_base"
    nodes_table: str = "graph_nodes"
    edges_table: str = "graph_edges"
    max_edges_group_size: int = 500
    table_names: list[str] | None = None
    exclude_infrastructure: bool = True

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

    # Columns to add via ALTER TABLE for forward-compatible migration.
    # Each tuple is (column_name, column_type).
    _NODE_MIGRATION_COLUMNS = [
        ("node_type", "STRING"),
        ("parent_id", "STRING"),
        ("data_type", "STRING"),
        ("quality_score", "DOUBLE"),
        ("embedding", "ARRAY<FLOAT>"),
        # Phase 1a: graph backbone enrichment
        ("ontology_id", "STRING"),
        ("ontology_type", "STRING"),
        ("display_name", "STRING"),
        ("short_description", "STRING"),
        ("sensitivity", "STRING"),
        ("status", "STRING"),
        ("source_system", "STRING"),
        ("keywords", "ARRAY<STRING>"),
    ]

    # Canonical column order + types for graph_edges (must match CREATE TABLE DDL).
    _EDGE_SCHEMA = [
        ("src", "STRING"), ("dst", "STRING"), ("relationship", "STRING"),
        ("weight", "DOUBLE"), ("edge_id", "STRING"), ("edge_type", "STRING"),
        ("direction", "STRING"), ("join_expression", "STRING"),
        ("join_confidence", "DOUBLE"), ("ontology_rel", "STRING"),
        ("source_system", "STRING"), ("status", "STRING"),
        ("edge_label", "STRING"), ("edge_facet", "STRING"),
        ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ]

    # Canonical column order + types for graph_nodes (must match CREATE TABLE DDL).
    _NODE_SCHEMA = [
        ("id", "STRING"), ("table_name", "STRING"), ("catalog", "STRING"),
        ("`schema`", "STRING"), ("table_short_name", "STRING"),
        ("domain", "STRING"), ("subdomain", "STRING"),
        ("has_pii", "BOOLEAN"), ("has_phi", "BOOLEAN"),
        ("security_level", "STRING"), ("comment", "STRING"),
        ("node_type", "STRING"), ("parent_id", "STRING"),
        ("data_type", "STRING"), ("quality_score", "DOUBLE"),
        ("embedding", "ARRAY<FLOAT>"), ("ontology_id", "STRING"),
        ("ontology_type", "STRING"), ("display_name", "STRING"),
        ("short_description", "STRING"), ("sensitivity", "STRING"),
        ("status", "STRING"), ("source_system", "STRING"),
        ("keywords", "ARRAY<STRING>"),
        ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ]

    def _align_edge_schema(self, df: DataFrame) -> DataFrame:
        """Select and cast columns to match the canonical edge table schema."""
        cols = []
        for name, dtype in self._EDGE_SCHEMA:
            col_ref = name.strip("`")
            if col_ref in df.columns:
                cols.append(F.col(f"`{col_ref}`").cast(dtype).alias(col_ref))
            else:
                cols.append(F.lit(None).cast(dtype).alias(col_ref))
        return df.select(*cols)

    def _align_node_schema(self, df: DataFrame) -> DataFrame:
        """Select and cast columns to match the canonical node table schema."""
        cols = []
        for name, dtype in self._NODE_SCHEMA:
            col_ref = name.strip("`")
            if col_ref in df.columns:
                cols.append(F.col(f"`{col_ref}`").cast(dtype).alias(col_ref))
            else:
                cols.append(F.lit(None).cast(dtype).alias(col_ref))
        return df.select(*cols)

    def _insert_edges(self, df: DataFrame, table: str = None, view_name: str = "_staged_edges"):
        """Schema-safe INSERT into graph_edges: aligns columns and uses named SQL INSERT."""
        target = table or self.config.fully_qualified_edges
        aligned = self._align_edge_schema(df)
        row_count = aligned.count()
        logger.info("Inserting %d edges into %s (columns: %s)",
                    row_count, target, [f.name for f in aligned.schema.fields])
        aligned.createOrReplaceTempView(view_name)
        col_list = ", ".join(c.strip("`") for c, _ in self._EDGE_SCHEMA)
        self.spark.sql(f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM {view_name}")

    def create_nodes_table(self) -> None:
        """Create the nodes table if it doesn't exist, and add new columns if missing."""
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
            node_type STRING,
            parent_id STRING,
            data_type STRING,
            quality_score DOUBLE,
            embedding ARRAY<FLOAT>,
            ontology_id STRING,
            ontology_type STRING,
            display_name STRING,
            short_description STRING,
            sensitivity STRING,
            status STRING,
            source_system STRING,
            keywords ARRAY<STRING>,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Graph nodes - unified backbone for tables, columns, schemas, and ontology entities'
        """
        self.spark.sql(ddl)

        for col_name, col_type in self._NODE_MIGRATION_COLUMNS:
            try:
                self.spark.sql(f"ALTER TABLE {self.config.fully_qualified_nodes} ADD COLUMN {col_name} {col_type}")
                logger.info("Added column %s to %s", col_name, self.config.fully_qualified_nodes)
            except Exception as e:
                if "already exists" in str(e).lower() or "FIELDS_ALREADY_EXISTS" in str(e):
                    pass
                else:
                    logger.debug("Could not add column %s: %s", col_name, e)

        logger.info("Nodes table %s ready", self.config.fully_qualified_nodes)
    
    _EDGE_MIGRATION_COLUMNS = [
        ("edge_id", "STRING"),
        ("edge_type", "STRING"),
        ("direction", "STRING"),
        ("join_expression", "STRING"),
        ("join_confidence", "DOUBLE"),
        ("ontology_rel", "STRING"),
        ("source_system", "STRING"),
        ("status", "STRING"),
        ("edge_label", "STRING"),
        ("edge_facet", "STRING"),
    ]

    def create_edges_table(self) -> None:
        """Create the edges table if it doesn't exist, and add new columns if missing."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_edges} (
            src STRING NOT NULL,
            dst STRING NOT NULL,
            relationship STRING NOT NULL,
            weight DOUBLE,
            edge_id STRING,
            edge_type STRING,
            direction STRING,
            join_expression STRING,
            join_confidence DOUBLE,
            ontology_rel STRING,
            source_system STRING,
            status STRING,
            edge_label STRING,
            edge_facet STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Graph edges - typed relationships between nodes'
        """
        self.spark.sql(ddl)

        for col_name, col_type in self._EDGE_MIGRATION_COLUMNS:
            try:
                self.spark.sql(f"ALTER TABLE {self.config.fully_qualified_edges} ADD COLUMN {col_name} {col_type}")
                logger.info("Added column %s to %s", col_name, self.config.fully_qualified_edges)
            except Exception as e:
                if "already exists" in str(e).lower() or "FIELDS_ALREADY_EXISTS" in str(e):
                    pass
                else:
                    logger.debug("Could not add column %s: %s", col_name, e)

        logger.info("Edges table %s ready", self.config.fully_qualified_edges)
    
    def build_nodes_df(self) -> DataFrame:
        """
        Build nodes DataFrame from knowledge base.
        
        Each table becomes a node with id = table_name.
        """
        tf = table_filter_sql(self.config.table_names or [], column="table_name")
        infra_filter = infrastructure_exclude_sql(column="table_name") if self.config.exclude_infrastructure else ""
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
            WHERE 1=1 {tf} {infra_filter}
        """)
        
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
            .withColumn("display_name", F.col("table_short_name"))
            .withColumn("short_description", F.col("comment"))
            .withColumn("sensitivity",
                F.when(F.col("has_phi"), F.lit("PHI"))
                .when(F.col("has_pii"), F.lit("PII"))
                .otherwise(F.lit("public"))
            )
            .withColumn("status", F.lit("discovered"))
            .withColumn("source_system", F.lit("knowledge_graph"))
            .withColumn("ontology_id", F.lit(None).cast("string"))
            .withColumn("ontology_type", F.lit(None).cast("string"))
            .withColumn("keywords", F.lit(None).cast("array<string>"))
        )

        return df.select(
            "id", "table_name", "catalog", "schema", "table_short_name",
            "domain", "subdomain", "has_pii", "has_phi", "security_level",
            "comment", "node_type", "parent_id", "data_type", "quality_score",
            "embedding", "ontology_id", "ontology_type", "display_name",
            "short_description", "sensitivity", "status", "source_system",
            "keywords", "created_at", "updated_at"
        )
    
    def build_edges_for_attribute(
        self, 
        source_df: DataFrame, 
        attribute: str,
        relationship: str,
        max_group_size: int,
    ) -> DataFrame:
        """
        Build edges between tables that share the same attribute value.
        
        Uses self-join to find pairs of tables with matching attribute.
        Only creates edges where src < dst to avoid duplicates.
        Groups larger than max_group_size are randomly sampled down to
        prevent O(n^2) edge explosion at scale.
        
        Args:
            source_df: DataFrame with id and attribute columns
            attribute: Column name to match on
            relationship: Name for this relationship type
            max_group_size: Cap per attribute value to bound self-join cost
            
        Returns:
            DataFrame with src, dst, relationship columns
        """
        df_with_attr = source_df.filter(F.col(attribute).isNotNull())
        
        # Cap large groups to prevent quadratic edge explosion
        w = Window.partitionBy(attribute).orderBy(F.rand(seed=42))
        df_with_attr = (
            df_with_attr
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") <= max_group_size)
            .drop("_rn")
        )
        
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
            .filter(F.col("src") < F.col("dst"))
            .select("src", "dst")
            .withColumn("relationship", F.lit(relationship))
            .withColumn("weight", F.lit(1.0))
            .withColumn("edge_type", F.lit(relationship))
            .withColumn("direction", F.lit("undirected"))
            .withColumn("source_system", F.lit("knowledge_graph"))
            .withColumn("status", F.lit("candidate"))
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
        cap = self.config.max_edges_group_size
        all_edges = []
        
        # Same domain edges
        domain_edges = self.build_edges_for_attribute(
            nodes_df, "domain", "same_domain", max_group_size=cap
        )
        all_edges.append(domain_edges)
        
        # Same subdomain edges
        subdomain_edges = self.build_edges_for_attribute(
            nodes_df, "subdomain", "same_subdomain", max_group_size=cap
        )
        all_edges.append(subdomain_edges)
        
        # Same catalog edges (skip if mono-catalog -- every pair is vacuous)
        distinct_catalogs = nodes_df.select("catalog").distinct().count()
        if distinct_catalogs > 1:
            catalog_edges = self.build_edges_for_attribute(
                nodes_df, "catalog", "same_catalog", max_group_size=cap
            )
            all_edges.append(catalog_edges)
        else:
            logger.info("Single catalog detected -- skipping same_catalog edges")
        
        # Same schema edges (skip if mono-schema -- every pair is vacuous)
        distinct_schemas = nodes_df.select("schema").distinct().count()
        if distinct_schemas > 1:
            schema_edges = self.build_edges_for_attribute(
                nodes_df, "schema", "same_schema", max_group_size=cap
            )
            all_edges.append(schema_edges)
        else:
            logger.info("Single schema detected -- skipping same_schema edges")
        
        # Same security level edges (exclude PUBLIC -- O(N^2) noise)
        sensitive_nodes = nodes_df.filter(F.col("security_level").isin("PII", "PHI"))
        security_edges = self.build_edges_for_attribute(
            sensitive_nodes, "security_level", "same_security_level", max_group_size=cap
        )
        all_edges.append(security_edges)
        
        from functools import reduce
        combined = reduce(lambda a, b: a.unionByName(b), all_edges)

        combined = (
            combined
            .withColumn("edge_id", F.concat_ws("::", F.col("src"), F.col("dst"), F.col("relationship")))
            .withColumn("join_expression", F.lit(None).cast("string"))
            .withColumn("join_confidence", F.lit(None).cast("double"))
            .withColumn("ontology_rel", F.lit(None).cast("string"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        return self._align_edge_schema(combined)
    
    def merge_nodes(self, nodes_df: DataFrame) -> Dict[str, int]:
        """
        Incrementally merge nodes into the nodes table.
        """
        self._align_node_schema(nodes_df).createOrReplaceTempView("staged_nodes")
        
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
            target.ontology_id = COALESCE(source.ontology_id, target.ontology_id),
            target.ontology_type = COALESCE(source.ontology_type, target.ontology_type),
            target.display_name = COALESCE(source.display_name, target.display_name),
            target.short_description = COALESCE(source.short_description, target.short_description),
            target.sensitivity = COALESCE(source.sensitivity, target.sensitivity),
            target.status = COALESCE(source.status, target.status),
            target.source_system = COALESCE(source.source_system, target.source_system),
            target.keywords = COALESCE(source.keywords, target.keywords),
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            id, table_name, catalog, `schema`, table_short_name,
            domain, subdomain, has_pii, has_phi, security_level, comment,
            node_type, parent_id, data_type, quality_score, embedding,
            ontology_id, ontology_type, display_name, short_description,
            sensitivity, status, source_system, keywords,
            created_at, updated_at
        ) VALUES (
            source.id, source.table_name, source.catalog, source.`schema`,
            source.table_short_name, source.domain, source.subdomain,
            source.has_pii, source.has_phi, source.security_level, source.comment,
            source.node_type, source.parent_id, source.data_type,
            source.quality_score, source.embedding,
            source.ontology_id, source.ontology_type, source.display_name,
            source.short_description, source.sensitivity, source.status,
            source.source_system, source.keywords,
            source.created_at, source.updated_at
        )
        """
        
        self.spark.sql(merge_sql)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_nodes}"
        ).collect()[0]["cnt"]
        
        return {"total_nodes": count}
    
    def refresh_edges(self, edges_df: DataFrame, source_system: str = "knowledge_graph") -> Dict[str, int]:
        """
        Refresh edges by deleting stale edges and inserting current ones.
        
        Only deletes edges matching the given source_system to avoid destroying
        edges from other producers (ontology, similarity, fk_predictions).
        
        Strategy:
        1. Delete edges involving affected nodes for this source_system only
        2. Insert all current edges
        """
        affected_nodes = edges_df.select("src").union(edges_df.select(F.col("dst").alias("src"))).distinct()
        affected_ids = [row.src for row in affected_nodes.collect()]

        if affected_ids:
            id_list = ", ".join(f"'{n}'" for n in affected_ids)
            delete_sql = f"""
            DELETE FROM {self.config.fully_qualified_edges}
            WHERE (src IN ({id_list}) OR dst IN ({id_list}))
              AND (source_system = '{source_system}' OR source_system IS NULL)
            """
            self.spark.sql(delete_sql)
        
        self._insert_edges(edges_df)
        
        count = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_edges}"
        ).collect()[0]["cnt"]
        
        return {"total_edges": count}
    
    def add_inverse_edges(self, edges_df: DataFrame, edge_catalog: Optional[Dict] = None) -> DataFrame:
        """Generate inverse edges for all directed edges using the edge catalog.

        For each edge A->B with relationship R, if the edge catalog has an
        inverse defined for R, produce B->A with the inverse relationship name.
        Symmetric edges and edges without inverses are skipped.
        """
        if not edge_catalog:
            return edges_df

        from pyspark.sql import Row
        inverse_map: Dict[str, str] = {}
        symmetric: set = set()
        for name, entry in edge_catalog.items():
            if hasattr(entry, "symmetric") and entry.symmetric:
                symmetric.add(name)
            elif hasattr(entry, "inverse") and entry.inverse:
                inverse_map[name] = entry.inverse

        if not inverse_map:
            return edges_df

        directed_edges = edges_df.filter(
            F.col("relationship").isin(list(inverse_map.keys()))
        )
        if directed_edges.rdd.isEmpty():
            return edges_df

        inv_edges = (
            directed_edges
            .withColumn("_orig_src", F.col("src"))
            .withColumn("src", F.col("dst"))
            .withColumn("dst", F.col("_orig_src"))
            .drop("_orig_src")
        )

        mapping_expr = F.create_map(
            *[item for k, v in inverse_map.items() for item in (F.lit(k), F.lit(v))]
        )
        inv_edges = (
            inv_edges
            .withColumn("relationship", mapping_expr[F.col("relationship")])
            .withColumn("edge_id", F.concat_ws("::", F.col("src"), F.col("dst"), F.col("relationship")))
            .withColumn("direction", F.lit("inverse"))
        )

        combined = edges_df.unionByName(inv_edges, allowMissingColumns=True)
        logger.info("add_inverse_edges: added %d inverse edges", inv_edges.count())
        return combined

    def merge_edges(self, edges_df: DataFrame) -> Dict[str, int]:
        """
        Incrementally merge edges into the edges table.
        
        Uses composite key (src, dst, relationship) for matching.
        
        Note: For most use cases, prefer refresh_edges() which properly handles
        relationship changes (e.g., when a table's domain changes).
        """
        self._align_edge_schema(edges_df).createOrReplaceTempView("staged_edges")
        
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_edges} AS target
        USING staged_edges AS source
        ON target.src = source.src 
           AND target.dst = source.dst 
           AND target.relationship = source.relationship

        WHEN MATCHED THEN UPDATE SET
            target.weight = source.weight,
            target.edge_type = COALESCE(source.edge_type, target.edge_type),
            target.direction = COALESCE(source.direction, target.direction),
            target.join_expression = COALESCE(source.join_expression, target.join_expression),
            target.join_confidence = COALESCE(source.join_confidence, target.join_confidence),
            target.ontology_rel = COALESCE(source.ontology_rel, target.ontology_rel),
            target.source_system = COALESCE(source.source_system, target.source_system),
            target.status = COALESCE(source.status, target.status),
            target.edge_label = COALESCE(source.edge_label, target.edge_label),
            target.edge_facet = COALESCE(source.edge_facet, target.edge_facet),
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            src, dst, relationship, weight, edge_id, edge_type,
            direction, join_expression, join_confidence, ontology_rel,
            source_system, status, edge_label, edge_facet, created_at, updated_at
        ) VALUES (
            source.src, source.dst, source.relationship, source.weight,
            source.edge_id, source.edge_type, source.direction,
            source.join_expression, source.join_confidence, source.ontology_rel,
            source.source_system, source.status, source.edge_label, source.edge_facet,
            source.created_at, source.updated_at
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
    schema_name: str,
    table_names: list[str] | None = None,
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
        schema_name=schema_name,
        table_names=table_names,
    )
    builder = KnowledgeGraphBuilder(spark, config)
    return builder.run()


@dataclass
class ExtendedKnowledgeGraphConfig(KnowledgeGraphConfig):
    """Extended configuration including column and schema knowledge bases."""
    column_kb_table: str = "column_knowledge_base"
    schema_kb_table: str = "schema_knowledge_base"
    extended_metadata_table: str = "extended_table_metadata"
    fk_confidence_threshold: float = 0.7
    
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
            tf = table_filter_sql(self.ext_config.table_names or [], column="table_name")
            infra_filter = infrastructure_exclude_sql(column="table_name") if self.ext_config.exclude_infrastructure else ""
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
                WHERE 1=1 {tf} {infra_filter}
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
                .withColumn("display_name", F.col("column_name"))
                .withColumn("short_description", F.col("comment"))
                .withColumn("sensitivity",
                    F.when(F.lower(F.col("classification")) == "phi", F.lit("PHI"))
                    .when(F.lower(F.col("classification")).isin("pii", "pci"), F.lit("PII"))
                    .otherwise(F.lit("public"))
                )
                .withColumn("status", F.lit("discovered"))
                .withColumn("source_system", F.lit("knowledge_graph"))
                .withColumn("ontology_id", F.lit(None).cast("string"))
                .withColumn("ontology_type", F.lit(None).cast("string"))
                .withColumn("keywords", F.lit(None).cast("array<string>"))
                .select(
                    "id", "table_name", "catalog", "schema", "table_short_name",
                    "domain", "subdomain", "has_pii", "has_phi", "security_level",
                    "comment", "node_type", "parent_id", "data_type", "quality_score",
                    "embedding", "ontology_id", "ontology_type", "display_name",
                    "short_description", "sensitivity", "status", "source_system",
                    "keywords", "created_at", "updated_at"
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
                .withColumn("display_name", F.col("schema_name"))
                .withColumn("short_description", F.col("comment"))
                .withColumn("sensitivity",
                    F.when(F.col("has_phi"), F.lit("PHI"))
                    .when(F.col("has_pii"), F.lit("PII"))
                    .otherwise(F.lit("public"))
                )
                .withColumn("status", F.lit("discovered"))
                .withColumn("source_system", F.lit("knowledge_graph"))
                .withColumn("ontology_id", F.lit(None).cast("string"))
                .withColumn("ontology_type", F.lit(None).cast("string"))
                .withColumn("keywords", F.lit(None).cast("array<string>"))
                .select(
                    "id", "table_name", "catalog", "schema", "table_short_name",
                    "domain", "subdomain", "has_pii", "has_phi", "security_level",
                    "comment", "node_type", "parent_id", "data_type", "quality_score",
                    "embedding", "ontology_id", "ontology_type", "display_name",
                    "short_description", "sensitivity", "status", "source_system",
                    "keywords", "created_at", "updated_at"
                )
            )
        except Exception as e:
            logger.warning(f"Could not build schema nodes: {e}")
            return None
    
    _EMPTY_EDGE_SCHEMA = (
        "src STRING, dst STRING, relationship STRING, weight DOUBLE, "
        "edge_id STRING, edge_type STRING, direction STRING, "
        "join_expression STRING, join_confidence DOUBLE, ontology_rel STRING, "
        "source_system STRING, status STRING, edge_label STRING, edge_facet STRING, "
        "created_at TIMESTAMP, updated_at TIMESTAMP"
    )

    def _enrich_edges(self, df: DataFrame, edge_type: str, source_sys: str = "knowledge_graph") -> DataFrame:
        """Add standard new-schema columns to an edge DataFrame."""
        enriched = (
            df
            .withColumn("edge_id", F.concat_ws("::", F.col("src"), F.col("dst"), F.col("relationship")))
            .withColumn("edge_type", F.lit(edge_type))
            .withColumn("direction", F.lit("out"))
            .withColumn("join_expression", F.lit(None).cast("string"))
            .withColumn("join_confidence", F.lit(None).cast("double"))
            .withColumn("ontology_rel", F.lit(None).cast("string"))
            .withColumn("source_system", F.lit(source_sys))
            .withColumn("status", F.lit("candidate"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )
        return self._align_edge_schema(enriched)

    def build_containment_edges(self, nodes_df: DataFrame) -> DataFrame:
        """Build 'contains' edges for hierarchical relationships."""
        edges = []

        schema_table_edges = (
            nodes_df
            .filter(F.col("node_type") == "table")
            .select(F.col("parent_id").alias("src"), F.col("id").alias("dst"))
            .filter(F.col("src").isNotNull())
            .withColumn("relationship", F.lit("contains"))
            .withColumn("weight", F.lit(1.0))
        )
        edges.append(schema_table_edges)

        table_column_edges = (
            nodes_df
            .filter(F.col("node_type") == "column")
            .select(F.col("parent_id").alias("src"), F.col("id").alias("dst"))
            .filter(F.col("src").isNotNull())
            .withColumn("relationship", F.lit("contains"))
            .withColumn("weight", F.lit(1.0))
        )
        edges.append(table_column_edges)

        if edges:
            from functools import reduce
            combined = reduce(DataFrame.union, edges)
            return self._enrich_edges(combined, "contains")

        return self.spark.createDataFrame([], self._EMPTY_EDGE_SCHEMA)
    
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
            
            df = df.withColumn("relationship", F.lit("derives_from")).withColumn("weight", F.lit(1.0))
            return self._enrich_edges(df, "derives_from")
        except Exception as e:
            logger.warning("Could not build lineage edges: %s", e)
            return self.spark.createDataFrame([], self._EMPTY_EDGE_SCHEMA)

    def build_reference_edges(self) -> DataFrame:
        """Build 'references' edges from fk_predictions at both table and column level."""
        fk_table = f"{self.ext_config.catalog_name}.{self.ext_config.schema_name}.fk_predictions"
        threshold = self.ext_config.fk_confidence_threshold
        try:
            raw = self.spark.sql(f"""
                SELECT
                    src_table, dst_table, src_column, dst_column,
                    CONCAT(src_table, '.', src_column, ' = ', dst_table, '.', dst_column) AS join_expr,
                    final_confidence AS conf
                FROM {fk_table}
                WHERE final_confidence >= {threshold}
            """)
            if raw.count() == 0:
                return self.spark.createDataFrame([], self._EMPTY_EDGE_SCHEMA)

            def _fk_edges(df, rel_suffix=""):
                rel = "references" + rel_suffix
                return (
                    df
                    .withColumn("relationship", F.lit(rel))
                    .withColumn("weight", F.col("conf"))
                    .withColumn("edge_id", F.concat_ws("::", F.col("src"), F.col("dst"), F.lit(rel)))
                    .withColumn("edge_type", F.lit("references"))
                    .withColumn("direction", F.lit("out"))
                    .withColumn("join_expression", F.col("join_expr"))
                    .withColumn("join_confidence", F.col("conf"))
                    .withColumn("ontology_rel", F.lit(None).cast("string"))
                    .withColumn("source_system", F.lit("fk_predictions"))
                    .withColumn("status", F.lit("candidate"))
                    .withColumn("edge_label", F.lit(None).cast("string"))
                    .withColumn("edge_facet", F.lit(None).cast("string"))
                    .withColumn("created_at", F.current_timestamp())
                    .withColumn("updated_at", F.current_timestamp())
                    .drop("join_expr", "conf", "src_table", "dst_table", "src_column", "dst_column")
                )

            table_df = raw.select(
                F.col("src_table").alias("src"), F.col("dst_table").alias("dst"),
                "join_expr", "conf",
            )
            table_edges = _fk_edges(table_df)

            col_df = raw.select(
                F.concat_ws(".", F.col("src_table"), F.col("src_column")).alias("src"),
                F.concat_ws(".", F.col("dst_table"), F.col("dst_column")).alias("dst"),
                "join_expr", "conf",
            )
            col_edges = _fk_edges(col_df, "_column")

            return table_edges.unionByName(col_edges)
        except Exception as e:
            logger.warning("Could not build reference edges: %s", e)
            return self.spark.createDataFrame([], self._EMPTY_EDGE_SCHEMA)

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
        return reduce(lambda a, b: a.unionByName(b), all_edges)
    
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
        all_nodes_df = reduce(lambda a, b: a.unionByName(b), all_nodes)
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
    include_schemas: bool = True,
    table_names: list[str] | None = None,
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
        schema_name=schema_name,
        table_names=table_names,
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


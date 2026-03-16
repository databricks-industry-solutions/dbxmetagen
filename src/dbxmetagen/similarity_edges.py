"""
Similarity Edges module for adding embedding-based edges to the knowledge graph.

Creates 'similar_embedding' edges between nodes with high cosine similarity,
enabling discovery of semantically related tables, columns, and schemas.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class SimilarityEdgesConfig:
    """Configuration for similarity edge generation."""
    catalog_name: str
    schema_name: str
    nodes_table: str = "graph_nodes"
    edges_table: str = "graph_edges"
    similarity_threshold: float = 0.7
    max_edges_per_node: int = 15
    blocking_node_threshold: int = 5000
    
    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"
    
    @property
    def fully_qualified_edges(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.edges_table}"


_EDGE_SCHEMA = (
    "src STRING, dst STRING, relationship STRING, weight DOUBLE, "
    "edge_id STRING, edge_type STRING, direction STRING, "
    "join_expression STRING, join_confidence DOUBLE, ontology_rel STRING, "
    "source_system STRING, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP"
)


class SimilarityEdgeBuilder:
    """
    Builder for creating similarity-based edges in the knowledge graph.
    
    Uses pre-computed embeddings to find and create edges between
    semantically similar nodes.
    """
    
    RELATIONSHIP_TYPE = "similar_embedding"
    
    def __init__(self, spark: SparkSession, config: SimilarityEdgesConfig):
        self.spark = spark
        self.config = config
        self._cosine_impl = self._detect_cosine_impl()
    
    def _detect_cosine_impl(self) -> str:
        """Detect best cosine similarity implementation. Called once at init."""
        try:
            self.spark.sql("SELECT vector_cosine_similarity(array(1.0), array(1.0))").collect()
            logger.info("Using built-in vector_cosine_similarity (DBR 18.1+)")
            return "builtin"
        except Exception:
            logger.info("Using Pandas UDF cosine similarity (DBR < 18.1)")
            return "pandas_udf"

    def _register_cosine_udf(self):
        """Register a Pandas UDF for cosine similarity as a SQL function."""
        import numpy as np
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        @pandas_udf("double")
        def _cosine_sim_udf(emb_a: pd.Series, emb_b: pd.Series) -> pd.Series:
            a = np.stack(emb_a.values)
            b = np.stack(emb_b.values)
            dot = np.sum(a * b, axis=1)
            norm = np.linalg.norm(a, axis=1) * np.linalg.norm(b, axis=1)
            return pd.Series(np.where(norm > 0, dot / norm, 0.0))

        self.spark.udf.register("_cosine_sim", _cosine_sim_udf)

    def _cosine_sql_expr(self) -> str:
        """Return the SQL expression for cosine similarity based on detected impl."""
        if self._cosine_impl == "builtin":
            return "vector_cosine_similarity(emb_a, emb_b)"
        return "_cosine_sim(emb_a, emb_b)"
    
    def get_nodes_with_embeddings(self) -> DataFrame:
        """Get all nodes that have embeddings."""
        return self.spark.sql(f"""
            SELECT id, node_type, domain, embedding
            FROM {self.config.fully_qualified_nodes}
            WHERE embedding IS NOT NULL
        """)

    def _build_pairs_sql(self, node_count: int) -> str:
        """Build SQL for node pair generation, using blocking for large node sets."""
        if node_count < self.config.blocking_node_threshold:
            return """
            SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
                   b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
                   a.embedding as emb_a, b.embedding as emb_b
            FROM nodes_emb a CROSS JOIN nodes_emb b
            WHERE a.id < b.id
              AND NOT (a.node_type = 'entity' AND b.node_type = 'entity')
            """

        logger.info(
            f"Node count {node_count} >= threshold {self.config.blocking_node_threshold}, "
            "using iterative blocking (intra-domain + cross-domain bridge)"
        )
        return """
        SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
               b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
               a.embedding as emb_a, b.embedding as emb_b
        FROM nodes_emb a CROSS JOIN nodes_emb b
        WHERE a.id < b.id
          AND a.domain = b.domain
          AND NOT (a.node_type = 'entity' AND b.node_type = 'entity')

        UNION ALL

        SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
               b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
               a.embedding as emb_a, b.embedding as emb_b
        FROM nodes_emb a CROSS JOIN nodes_emb b
        WHERE a.id < b.id
          AND a.domain != b.domain
          AND a.node_type = 'column' AND b.node_type = 'column'
          AND (LOWER(ELEMENT_AT(SPLIT(a.id, '\\\\.'), -1)) RLIKE '(_id|_key|_code)$'
               OR LOWER(ELEMENT_AT(SPLIT(b.id, '\\\\.'), -1)) RLIKE '(_id|_key|_code)$')
        """
    
    def compute_similarity_edges(self) -> DataFrame:
        """Compute similarity edges between nodes with embeddings.
        
        Uses a 3-tier cosine similarity approach:
        - Tier 1: vector_cosine_similarity (DBR 18.1+, auto-detected)
        - Tier 2: Pandas UDF with numpy (DBR 15.4+, default)
        - Tier 3: Python fallback (last resort)
        
        Uses iterative blocking when node count exceeds blocking_node_threshold.
        """
        nodes_df = self.get_nodes_with_embeddings()
        node_count = nodes_df.count()
        
        if node_count < 2:
            logger.info("Not enough nodes with embeddings for similarity comparison")
            return self.spark.createDataFrame([], _EDGE_SCHEMA)
        
        logger.info(f"Computing similarity between {node_count} nodes")
        
        nodes_df.createOrReplaceTempView("nodes_emb")

        if self._cosine_impl == "pandas_udf":
            self._register_cosine_udf()

        pairs_sql = self._build_pairs_sql(node_count)
        cosine_expr = self._cosine_sql_expr()

        similarity_sql = f"""
        WITH node_pairs AS (
            {pairs_sql}
        ),
        similarities AS (
            SELECT src, src_type, src_domain, dst, dst_type, dst_domain,
                   {cosine_expr} as similarity
            FROM node_pairs
        ),
        ranked AS (
            SELECT src, dst, src_type, dst_type, src_domain, dst_domain, similarity,
                ROW_NUMBER() OVER (PARTITION BY src ORDER BY similarity DESC) as rn_src,
                ROW_NUMBER() OVER (PARTITION BY dst ORDER BY similarity DESC) as rn_dst
            FROM similarities
            WHERE similarity >= {self.config.similarity_threshold}
        )
        SELECT 
            src, dst,
            '{self.RELATIONSHIP_TYPE}' as relationship,
            similarity as weight,
            CONCAT(src, '::', dst, '::{self.RELATIONSHIP_TYPE}') as edge_id,
            '{self.RELATIONSHIP_TYPE}' as edge_type,
            'undirected' as direction,
            CAST(NULL AS STRING) as join_expression,
            CAST(NULL AS DOUBLE) as join_confidence,
            CAST(NULL AS STRING) as ontology_rel,
            'embedding_similarity' as source_system,
            'candidate' as status,
            current_timestamp() as created_at,
            current_timestamp() as updated_at
        FROM ranked
        WHERE rn_src <= {self.config.max_edges_per_node} 
           OR rn_dst <= {self.config.max_edges_per_node}
        """
        
        try:
            return self.spark.sql(similarity_sql)
        except Exception as e:
            logger.warning(f"SQL-based similarity failed: {e}")
            return self._compute_similarity_fallback(nodes_df)
    
    def _compute_similarity_fallback(self, nodes_df: DataFrame) -> DataFrame:
        """Fallback Python-based similarity computation."""
        nodes = nodes_df.collect()
        edges = []
        
        def cosine_sim(v1, v2):
            if not v1 or not v2:
                return 0.0
            dot = sum(a * b for a, b in zip(v1, v2))
            norm1 = sum(a * a for a in v1) ** 0.5
            norm2 = sum(b * b for b in v2) ** 0.5
            return dot / (norm1 * norm2) if norm1 and norm2 else 0.0
        
        for i, node_a in enumerate(nodes):
            similarities = []
            for j, node_b in enumerate(nodes):
                if i < j and node_a.embedding and node_b.embedding:
                    sim = cosine_sim(list(node_a.embedding), list(node_b.embedding))
                    if sim >= self.config.similarity_threshold:
                        similarities.append((node_b.id, sim))
            
            similarities.sort(key=lambda x: x[1], reverse=True)
            for dst, sim in similarities[:self.config.max_edges_per_node]:
                edges.append({
                    "src": node_a.id,
                    "dst": dst,
                    "relationship": self.RELATIONSHIP_TYPE,
                    "weight": sim
                })
        
        if edges:
            df = self.spark.createDataFrame(edges)
            return (
                df
                .withColumn("edge_id", F.concat_ws("::", F.col("src"), F.col("dst"), F.col("relationship")))
                .withColumn("edge_type", F.lit(self.RELATIONSHIP_TYPE))
                .withColumn("direction", F.lit("undirected"))
                .withColumn("join_expression", F.lit(None).cast("string"))
                .withColumn("join_confidence", F.lit(None).cast("double"))
                .withColumn("ontology_rel", F.lit(None).cast("string"))
                .withColumn("source_system", F.lit("embedding_similarity"))
                .withColumn("status", F.lit("candidate"))
                .withColumn("created_at", F.current_timestamp())
                .withColumn("updated_at", F.current_timestamp())
            )

        return self.spark.createDataFrame([], _EDGE_SCHEMA)
    
    def remove_existing_similarity_edges(self) -> int:
        """Remove existing similarity edges before inserting new ones."""
        delete_sql = f"""
        DELETE FROM {self.config.fully_qualified_edges}
        WHERE relationship = '{self.RELATIONSHIP_TYPE}'
           OR source_system = 'embedding_similarity'
        """
        self.spark.sql(delete_sql)
        return 0
    
    def insert_similarity_edges(self, edges_df: DataFrame) -> int:
        """Insert new similarity edges using schema-aligned SQL INSERT."""
        if edges_df.count() == 0:
            return 0
        from dbxmetagen.knowledge_graph import KnowledgeGraphBuilder
        cols = []
        for name, dtype in KnowledgeGraphBuilder._EDGE_SCHEMA:
            if name in edges_df.columns:
                cols.append(F.col(name).cast(dtype).alias(name))
            else:
                cols.append(F.lit(None).cast(dtype).alias(name))
        aligned = edges_df.select(*cols)
        aligned.createOrReplaceTempView("_staged_sim_edges")
        col_list = ", ".join(c for c, _ in KnowledgeGraphBuilder._EDGE_SCHEMA)
        self.spark.sql(
            f"INSERT INTO {self.config.fully_qualified_edges} ({col_list}) "
            f"SELECT {col_list} FROM _staged_sim_edges"
        )
        return aligned.count()
    
    def run(self) -> Dict[str, Any]:
        """Execute the similarity edge generation pipeline."""
        logger.info("Starting similarity edge generation")
        
        self.remove_existing_similarity_edges()
        
        edges_df = self.compute_similarity_edges()
        edge_count = edges_df.count()
        logger.info(f"Computed {edge_count} similarity edges")
        
        inserted = self.insert_similarity_edges(edges_df)
        
        logger.info(f"Similarity edge generation complete. Edges: {inserted}")
        
        return {
            "edges_created": inserted,
            "threshold": self.config.similarity_threshold
        }


def build_similarity_edges(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    similarity_threshold: float = 0.7,
    max_edges_per_node: int = 15
) -> Dict[str, Any]:
    """
    Convenience function to build similarity edges.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        similarity_threshold: Minimum similarity for edge creation
        max_edges_per_node: Maximum edges per node
        
    Returns:
        Dict with execution statistics
    """
    config = SimilarityEdgesConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        similarity_threshold=similarity_threshold,
        max_edges_per_node=max_edges_per_node
    )
    builder = SimilarityEdgeBuilder(spark, config)
    return builder.run()

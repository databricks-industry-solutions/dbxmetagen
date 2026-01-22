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
    similarity_threshold: float = 0.8
    max_edges_per_node: int = 10
    
    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"
    
    @property
    def fully_qualified_edges(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.edges_table}"


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
    
    def get_nodes_with_embeddings(self) -> DataFrame:
        """Get all nodes that have embeddings."""
        return self.spark.sql(f"""
            SELECT id, node_type, domain, embedding
            FROM {self.config.fully_qualified_nodes}
            WHERE embedding IS NOT NULL
        """)
    
    def compute_similarity_edges(self) -> DataFrame:
        """
        Compute similarity edges between nodes with embeddings.
        
        Uses SQL-based cosine similarity calculation.
        """
        nodes_df = self.get_nodes_with_embeddings()
        node_count = nodes_df.count()
        
        if node_count < 2:
            logger.info("Not enough nodes with embeddings for similarity comparison")
            return self.spark.createDataFrame([], 
                "src STRING, dst STRING, relationship STRING, weight DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP")
        
        logger.info(f"Computing similarity between {node_count} nodes")
        
        nodes_df.createOrReplaceTempView("nodes_emb")
        
        # Compute cosine similarity using SQL
        # For large datasets, consider sampling or approximate methods
        similarity_sql = f"""
        WITH node_pairs AS (
            SELECT 
                a.id as src,
                a.node_type as src_type,
                a.domain as src_domain,
                b.id as dst,
                b.node_type as dst_type,
                b.domain as dst_domain,
                a.embedding as emb_a,
                b.embedding as emb_b
            FROM nodes_emb a
            CROSS JOIN nodes_emb b
            WHERE a.id < b.id
        ),
        similarities AS (
            SELECT 
                src,
                src_type,
                src_domain,
                dst,
                dst_type,
                dst_domain,
                -- Cosine similarity calculation
                AGGREGATE(
                    TRANSFORM(
                        SEQUENCE(0, SIZE(emb_a) - 1),
                        i -> emb_a[i] * emb_b[i]
                    ),
                    CAST(0 AS DOUBLE),
                    (acc, x) -> acc + x
                ) / NULLIF(
                    SQRT(AGGREGATE(TRANSFORM(emb_a, x -> x * x), CAST(0 AS DOUBLE), (acc, x) -> acc + x)) *
                    SQRT(AGGREGATE(TRANSFORM(emb_b, x -> x * x), CAST(0 AS DOUBLE), (acc, x) -> acc + x)),
                    0
                ) as similarity
            FROM node_pairs
        ),
        ranked AS (
            SELECT 
                src,
                dst,
                src_type,
                dst_type,
                src_domain,
                dst_domain,
                similarity,
                ROW_NUMBER() OVER (PARTITION BY src ORDER BY similarity DESC) as rn_src,
                ROW_NUMBER() OVER (PARTITION BY dst ORDER BY similarity DESC) as rn_dst
            FROM similarities
            WHERE similarity >= {self.config.similarity_threshold}
        )
        SELECT 
            src,
            dst,
            '{self.RELATIONSHIP_TYPE}' as relationship,
            similarity as weight,
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
            
            # Keep top N per node
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
                .withColumn("created_at", F.current_timestamp())
                .withColumn("updated_at", F.current_timestamp())
            )
        
        return self.spark.createDataFrame([], 
            "src STRING, dst STRING, relationship STRING, weight DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP")
    
    def remove_existing_similarity_edges(self) -> int:
        """Remove existing similarity edges before inserting new ones."""
        delete_sql = f"""
        DELETE FROM {self.config.fully_qualified_edges}
        WHERE relationship = '{self.RELATIONSHIP_TYPE}'
        """
        self.spark.sql(delete_sql)
        return 0  # Delta doesn't return affected rows directly
    
    def insert_similarity_edges(self, edges_df: DataFrame) -> int:
        """Insert new similarity edges."""
        if edges_df.count() == 0:
            return 0
        
        edges_df.write.mode("append").saveAsTable(self.config.fully_qualified_edges)
        return edges_df.count()
    
    def run(self) -> Dict[str, Any]:
        """Execute the similarity edge generation pipeline."""
        logger.info("Starting similarity edge generation")
        
        # Remove old similarity edges
        self.remove_existing_similarity_edges()
        
        # Compute new edges
        edges_df = self.compute_similarity_edges()
        edge_count = edges_df.count()
        logger.info(f"Computed {edge_count} similarity edges")
        
        # Insert new edges
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
    similarity_threshold: float = 0.8,
    max_edges_per_node: int = 10
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


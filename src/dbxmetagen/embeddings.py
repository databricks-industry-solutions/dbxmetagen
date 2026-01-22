"""
Embeddings module for generating vector embeddings via AI_QUERY.

Uses Databricks Foundation Model (BGE) to generate embeddings for
graph nodes based on their comments/descriptions. Embeddings enable
similarity search and clustering.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingConfig:
    """Configuration for embedding generation."""
    catalog_name: str
    schema_name: str
    nodes_table: str = "graph_nodes"
    model: str = "databricks-bge-large-en"
    batch_size: int = 50
    similarity_threshold: float = 0.8
    
    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"


class EmbeddingGenerator:
    """
    Generator for creating embeddings using Databricks AI_QUERY.
    
    Generates embeddings from node comments and stores them in the graph_nodes table.
    Can also compute similarity edges between nodes.
    """
    
    def __init__(self, spark: SparkSession, config: EmbeddingConfig):
        self.spark = spark
        self.config = config
    
    def get_nodes_needing_embeddings(self) -> DataFrame:
        """Get nodes that have comments but no embeddings."""
        return self.spark.sql(f"""
            SELECT id, comment, node_type
            FROM {self.config.fully_qualified_nodes}
            WHERE comment IS NOT NULL 
              AND LENGTH(comment) > 10
              AND embedding IS NULL
        """)
    
    def generate_embedding_for_text(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding for a single text using AI_QUERY.
        
        Args:
            text: Text to embed
            
        Returns:
            List of floats representing the embedding vector
        """
        try:
            # Escape single quotes in text
            escaped_text = text.replace("'", "''")[:2000]  # Limit length
            
            result = self.spark.sql(f"""
                SELECT AI_QUERY(
                    '{self.config.model}',
                    '{escaped_text}'
                ) as embedding
            """).collect()[0]['embedding']
            
            # AI_QUERY returns different formats - handle accordingly
            if isinstance(result, list):
                return result
            elif isinstance(result, str):
                import json
                try:
                    return json.loads(result)
                except:
                    return None
            return None
        except Exception as e:
            logger.warning(f"Could not generate embedding: {e}")
            return None
    
    def generate_embeddings_batch(self, nodes_df: DataFrame) -> DataFrame:
        """
        Generate embeddings for a batch of nodes using SQL.
        
        Uses a more efficient approach by processing in SQL where possible.
        """
        # Register UDF for embedding generation (fallback approach)
        # For production, consider using batch AI_QUERY
        
        nodes_df.createOrReplaceTempView("nodes_to_embed")
        
        try:
            # Try using AI_QUERY with TRANSFORM (more efficient)
            result = self.spark.sql(f"""
                SELECT 
                    id,
                    AI_QUERY(
                        '{self.config.model}',
                        SUBSTRING(comment, 1, 2000)
                    ) as embedding
                FROM nodes_to_embed
            """)
            return result
        except Exception as e:
            logger.warning(f"Batch embedding failed, falling back to row-by-row: {e}")
            return self._generate_embeddings_row_by_row(nodes_df)
    
    def _generate_embeddings_row_by_row(self, nodes_df: DataFrame) -> DataFrame:
        """Fallback: Generate embeddings one row at a time."""
        results = []
        
        for row in nodes_df.collect():
            embedding = self.generate_embedding_for_text(row.comment)
            if embedding:
                results.append((row.id, embedding))
        
        if results:
            return self.spark.createDataFrame(results, ["id", "embedding"])
        else:
            return self.spark.createDataFrame([], "id STRING, embedding ARRAY<FLOAT>")
    
    def update_node_embeddings(self, embeddings_df: DataFrame) -> int:
        """Update graph_nodes with generated embeddings."""
        embeddings_df.createOrReplaceTempView("new_embeddings")
        
        update_sql = f"""
        MERGE INTO {self.config.fully_qualified_nodes} AS target
        USING new_embeddings AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.embedding = source.embedding,
            target.updated_at = current_timestamp()
        """
        
        self.spark.sql(update_sql)
        
        # Return count of updated nodes
        return embeddings_df.count()
    
    def compute_cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if not vec1 or not vec2 or len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = sum(a * a for a in vec1) ** 0.5
        norm2 = sum(b * b for b in vec2) ** 0.5
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def find_similar_nodes(self) -> DataFrame:
        """
        Find pairs of nodes with similar embeddings.
        
        Returns DataFrame with src, dst, similarity for pairs above threshold.
        """
        # Get nodes with embeddings
        nodes_with_embeddings = self.spark.sql(f"""
            SELECT id, node_type, embedding
            FROM {self.config.fully_qualified_nodes}
            WHERE embedding IS NOT NULL
        """)
        
        if nodes_with_embeddings.count() < 2:
            return self.spark.createDataFrame([], 
                "src STRING, dst STRING, similarity DOUBLE")
        
        # Self-join to compare all pairs (expensive for large datasets)
        # For production, consider using approximate nearest neighbor
        nodes_with_embeddings.createOrReplaceTempView("nodes_emb")
        
        try:
            # Use built-in vector functions if available
            similar_pairs = self.spark.sql(f"""
                WITH node_pairs AS (
                    SELECT 
                        a.id as src,
                        b.id as dst,
                        a.embedding as emb_a,
                        b.embedding as emb_b
                    FROM nodes_emb a
                    CROSS JOIN nodes_emb b
                    WHERE a.id < b.id
                )
                SELECT 
                    src, 
                    dst,
                    AGGREGATE(
                        TRANSFORM(
                            SEQUENCE(0, SIZE(emb_a) - 1),
                            i -> emb_a[i] * emb_b[i]
                        ),
                        CAST(0 AS DOUBLE),
                        (acc, x) -> acc + x
                    ) / (
                        SQRT(AGGREGATE(TRANSFORM(emb_a, x -> x * x), CAST(0 AS DOUBLE), (acc, x) -> acc + x)) *
                        SQRT(AGGREGATE(TRANSFORM(emb_b, x -> x * x), CAST(0 AS DOUBLE), (acc, x) -> acc + x))
                    ) as similarity
                FROM node_pairs
                HAVING similarity >= {self.config.similarity_threshold}
            """)
            return similar_pairs
        except Exception as e:
            logger.warning(f"SQL-based similarity failed: {e}")
            return self._compute_similarity_python(nodes_with_embeddings)
    
    def _compute_similarity_python(self, nodes_df: DataFrame) -> DataFrame:
        """Fallback: Compute similarities using Python."""
        nodes = nodes_df.collect()
        pairs = []
        
        for i, node_a in enumerate(nodes):
            for node_b in nodes[i+1:]:
                if node_a.embedding and node_b.embedding:
                    sim = self.compute_cosine_similarity(
                        list(node_a.embedding), 
                        list(node_b.embedding)
                    )
                    if sim >= self.config.similarity_threshold:
                        pairs.append((node_a.id, node_b.id, sim))
        
        if pairs:
            return self.spark.createDataFrame(pairs, ["src", "dst", "similarity"])
        return self.spark.createDataFrame([], "src STRING, dst STRING, similarity DOUBLE")
    
    def run(self, max_nodes: int = None) -> Dict[str, Any]:
        """
        Execute the embedding generation pipeline.
        
        Args:
            max_nodes: Maximum nodes to process (None = all)
        """
        logger.info("Starting embedding generation")
        
        # Get nodes needing embeddings
        nodes_to_embed = self.get_nodes_needing_embeddings()
        
        if max_nodes:
            nodes_to_embed = nodes_to_embed.limit(max_nodes)
        
        total_to_embed = nodes_to_embed.count()
        logger.info(f"Generating embeddings for {total_to_embed} nodes")
        
        if total_to_embed == 0:
            return {"nodes_embedded": 0, "similar_pairs": 0}
        
        # Generate embeddings in batches
        embeddings_df = self.generate_embeddings_batch(nodes_to_embed)
        
        # Update nodes
        updated = self.update_node_embeddings(embeddings_df)
        logger.info(f"Updated {updated} nodes with embeddings")
        
        return {
            "nodes_embedded": updated,
            "total_candidates": total_to_embed
        }


def generate_embeddings(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    max_nodes: int = None
) -> Dict[str, Any]:
    """
    Convenience function to generate embeddings.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        max_nodes: Maximum nodes to process
        
    Returns:
        Dict with execution statistics
    """
    config = EmbeddingConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    generator = EmbeddingGenerator(spark, config)
    return generator.run(max_nodes)


def find_similar_nodes(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    similarity_threshold: float = 0.8
) -> DataFrame:
    """
    Find similar nodes based on embeddings.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        similarity_threshold: Minimum similarity score
        
    Returns:
        DataFrame with src, dst, similarity columns
    """
    config = EmbeddingConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        similarity_threshold=similarity_threshold
    )
    generator = EmbeddingGenerator(spark, config)
    return generator.find_similar_nodes()


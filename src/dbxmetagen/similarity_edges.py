"""
Similarity Edges module for adding embedding-based edges to the knowledge graph.

Creates 'similar_embedding' edges between nodes with high cosine similarity,
enabling discovery of semantically related tables, columns, and schemas.

Supports two modes:
- Cross-join (default): Exhaustive pairwise cosine with 3-tier blocking.
- ANN (opt-in via use_ann=True): Vector Search index + parallel per-node queries.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple

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
    max_domain_nodes: int = 10000
    # ANN mode (default True -- cross-join falls back for small node counts)
    use_ann: bool = True
    ann_k_multiplier: int = 2
    embedding_dimension: int = 1024
    ann_batch_size: int = 100
    ann_max_workers: int = 10
    ann_max_nodes: int = 100_000
    endpoint_name: str = "dbxmetagen-vs"
    vs_index_suffix: str = "graph_nodes_vs_index"
    # Index readiness
    index_ready_timeout_s: int = 900
    index_ready_initial_delay_s: int = 30

    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"

    @property
    def fully_qualified_edges(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.edges_table}"

    @property
    def fq_vs_index(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.vs_index_suffix}"


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
        self._cosine_impl: str | None = None
        if not config.use_ann:
            self._cosine_impl = self._detect_cosine_impl()

    # ------------------------------------------------------------------
    # Cross-join path helpers
    # ------------------------------------------------------------------

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
            SELECT id, node_type, domain, parent_id, embedding
            FROM {self.config.fully_qualified_nodes}
            WHERE embedding IS NOT NULL
        """)

    def _has_large_domains(self) -> bool:
        """Check if any domain exceeds max_domain_nodes."""
        try:
            row = self.spark.sql(f"""
                SELECT MAX(cnt) as max_cnt FROM (
                    SELECT domain, COUNT(*) as cnt FROM nodes_emb GROUP BY domain
                )
            """).first()
            return row and row.max_cnt and row.max_cnt > self.config.max_domain_nodes
        except Exception:
            return False

    def _build_pairs_sql(self, node_count: int) -> str:
        """Build SQL for node pair generation, using blocking for large node sets.

        Three-tier strategy:
        1. Small catalog (< blocking_node_threshold): full cross-join
        2. Medium catalog: intra-domain cross-join + cross-domain key-column bridge
        3. Large domain (any domain > max_domain_nodes): intra-table cross-join +
           intra-domain key-column bridge + cross-domain key-column bridge
        """
        _KEY_COL_FILTER = (
            "(LOWER(ELEMENT_AT(SPLIT(a.id, '\\\\.'), -1)) RLIKE '(_id|_key|_code)$'"
            " OR LOWER(ELEMENT_AT(SPLIT(b.id, '\\\\.'), -1)) RLIKE '(_id|_key|_code)$')"
        )
        _NO_ENTITY_PAIR = "NOT (a.node_type = 'entity' AND b.node_type = 'entity')"

        if node_count < self.config.blocking_node_threshold:
            return f"""
            SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
                   b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
                   a.embedding as emb_a, b.embedding as emb_b
            FROM nodes_emb a CROSS JOIN nodes_emb b
            WHERE a.id < b.id AND {_NO_ENTITY_PAIR}
            """

        if not self._has_large_domains():
            logger.info(
                "Node count %d >= threshold %d, using domain blocking",
                node_count, self.config.blocking_node_threshold,
            )
            return f"""
            SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
                   b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
                   a.embedding as emb_a, b.embedding as emb_b
            FROM nodes_emb a CROSS JOIN nodes_emb b
            WHERE a.id < b.id AND a.domain = b.domain AND {_NO_ENTITY_PAIR}

            UNION ALL

            SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
                   b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
                   a.embedding as emb_a, b.embedding as emb_b
            FROM nodes_emb a CROSS JOIN nodes_emb b
            WHERE a.id < b.id AND a.domain != b.domain
              AND a.node_type = 'column' AND b.node_type = 'column'
              AND {_KEY_COL_FILTER}
            """

        logger.info(
            "Large domain detected (> %d nodes), using table-level sub-blocking",
            self.config.max_domain_nodes,
        )
        return f"""
        -- Tier 3a: intra-table full cross-join (same parent_id)
        SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
               b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
               a.embedding as emb_a, b.embedding as emb_b
        FROM nodes_emb a CROSS JOIN nodes_emb b
        WHERE a.id < b.id
          AND a.parent_id IS NOT NULL AND a.parent_id = b.parent_id
          AND {_NO_ENTITY_PAIR}

        UNION ALL

        -- Tier 3b: intra-domain key-column bridge (different tables, same domain)
        SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
               b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
               a.embedding as emb_a, b.embedding as emb_b
        FROM nodes_emb a CROSS JOIN nodes_emb b
        WHERE a.id < b.id
          AND a.domain = b.domain
          AND COALESCE(a.parent_id, '') != COALESCE(b.parent_id, '')
          AND a.node_type = 'column' AND b.node_type = 'column'
          AND {_KEY_COL_FILTER}

        UNION ALL

        -- Tier 3c: cross-domain key-column bridge
        SELECT a.id as src, a.node_type as src_type, a.domain as src_domain,
               b.id as dst, b.node_type as dst_type, b.domain as dst_domain,
               a.embedding as emb_a, b.embedding as emb_b
        FROM nodes_emb a CROSS JOIN nodes_emb b
        WHERE a.id < b.id AND a.domain != b.domain
          AND a.node_type = 'column' AND b.node_type = 'column'
          AND {_KEY_COL_FILTER}
        """

    # ------------------------------------------------------------------
    # Cross-join compute (original path, renamed)
    # ------------------------------------------------------------------

    def _compute_similarity_crossjoin(self) -> DataFrame:
        """Compute similarity edges via exhaustive cross-join with blocking."""
        nodes_df = self.get_nodes_with_embeddings()
        node_count = nodes_df.count()

        if node_count < 2:
            logger.info("Not enough nodes with embeddings for similarity comparison")
            return self.spark.createDataFrame([], _EDGE_SCHEMA)

        logger.info(f"Computing similarity between {node_count} nodes (cross-join)")

        nodes_df.createOrReplaceTempView("nodes_emb")

        if self._cosine_impl is None:
            self._cosine_impl = self._detect_cosine_impl()
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

    # ------------------------------------------------------------------
    # ANN path: Vector Search index + parallel per-node queries
    # ------------------------------------------------------------------

    def ensure_endpoint(self) -> str:
        """Ensure the VS endpoint exists and is ONLINE."""
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.errors import NotFound, ResourceDoesNotExist
        w = WorkspaceClient()
        name = self.config.endpoint_name
        try:
            ep = w.vector_search_endpoints.get_endpoint(name)
            logger.info("VS endpoint '%s' exists (state=%s)", name, ep.endpoint_status.state)
        except (NotFound, ResourceDoesNotExist):
            logger.info("Creating VS endpoint '%s'", name)
            w.vector_search_endpoints.create_endpoint(name=name, endpoint_type="STANDARD")
        ep = w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(name)
        logger.info("VS endpoint '%s' confirmed ONLINE", name)
        return name

    def _wait_until_index_ready(self, idx_name: str) -> None:
        """Poll until VS index is ready, with exponential backoff."""
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        deadline = time.monotonic() + self.config.index_ready_timeout_s
        delay = float(self.config.index_ready_initial_delay_s)
        while True:
            idx = w.vector_search_indexes.get_index(idx_name)
            st = idx.status
            if st and st.ready:
                logger.info("VS index '%s' is ready", idx_name)
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            logger.info(
                "VS index '%s' not ready yet (message=%s); sleeping %.0fs",
                idx_name, getattr(st, "message", None) if st else None,
                min(delay, remaining),
            )
            time.sleep(min(delay, remaining))
            delay = min(delay * 2.0, 120.0)
        raise TimeoutError(
            f"VS index '{idx_name}' did not become ready within {self.config.index_ready_timeout_s}s"
        )

    def ensure_graph_nodes_index(self) -> str:
        """Create or sync the Delta Sync VS index on graph_nodes.embedding."""
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.errors import NotFound, ResourceDoesNotExist
        from databricks.sdk.service.vectorsearch import (
            DeltaSyncVectorIndexSpecRequest,
            EmbeddingVectorColumn,
            PipelineType,
            VectorIndexType,
        )

        self.ensure_endpoint()

        w = WorkspaceClient()
        idx_name = self.config.fq_vs_index
        try:
            idx = w.vector_search_indexes.get_index(idx_name)
            logger.info(
                "VS index '%s' already exists (ready=%s)",
                idx_name, idx.status.ready if idx.status else None,
            )
        except (NotFound, ResourceDoesNotExist):
            logger.info("Creating Delta Sync index '%s' (self-managed embeddings)", idx_name)
            w.vector_search_indexes.create_index(
                name=idx_name,
                endpoint_name=self.config.endpoint_name,
                primary_key="id",
                index_type=VectorIndexType.DELTA_SYNC,
                delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                    source_table=self.config.fully_qualified_nodes,
                    embedding_vector_columns=[
                        EmbeddingVectorColumn(
                            name="embedding",
                            embedding_dimension=self.config.embedding_dimension,
                        )
                    ],
                    pipeline_type=PipelineType.TRIGGERED,
                    columns_to_sync=["id", "node_type", "domain", "parent_id"],
                ),
            )

        # Trigger sync and wait for readiness
        try:
            w.vector_search_indexes.sync_index(index_name=idx_name)
        except Exception as e:
            logger.warning("sync_index call returned: %s (index may still sync via schedule)", e)
        self._wait_until_index_ready(idx_name)
        return idx_name

    def compute_similarity_edges_ann(self) -> DataFrame:
        """Compute similarity edges via Vector Search ANN queries."""
        nodes_df = self.get_nodes_with_embeddings()
        node_count = nodes_df.count()

        if node_count < 2:
            logger.info("Not enough nodes with embeddings for ANN similarity")
            return self.spark.createDataFrame([], _EDGE_SCHEMA)

        if node_count > self.config.ann_max_nodes:
            raise ValueError(
                f"Node count {node_count} exceeds ann_max_nodes "
                f"({self.config.ann_max_nodes}). Set use_ann=false or "
                f"increase ann_max_nodes if driver memory allows."
            )

        logger.info("Collecting %d nodes for ANN queries", node_count)
        nodes = nodes_df.collect()
        K = self.config.max_edges_per_node * self.config.ann_k_multiplier

        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient()
        index = vsc.get_index(
            endpoint_name=self.config.endpoint_name,
            index_name=self.config.fq_vs_index,
        )

        def _query_batch(batch):
            batch_edges: List[Tuple] = []
            for node in batch:
                results = index.similarity_search(
                    query_vector=list(node.embedding),
                    columns=["id", "node_type", "domain"],
                    num_results=K,
                    score_threshold=self.config.similarity_threshold,
                )
                for row in results.get("result", {}).get("data_array", []):
                    if row[0] != node.id:
                        batch_edges.append((
                            node.id, node.node_type, node.domain,
                            row[0], row[1], row[2], float(row[-1]),
                        ))
            return batch_edges

        batches = [
            nodes[i:i + self.config.ann_batch_size]
            for i in range(0, len(nodes), self.config.ann_batch_size)
        ]
        logger.info(
            "Querying VS index: %d nodes in %d batches (%d workers)",
            node_count, len(batches), self.config.ann_max_workers,
        )

        all_edges: List[Tuple] = []
        with ThreadPoolExecutor(max_workers=self.config.ann_max_workers) as pool:
            futures = {pool.submit(_query_batch, b): i for i, b in enumerate(batches)}
            for f in as_completed(futures):
                batch_idx = futures[f]
                try:
                    all_edges.extend(f.result())
                except Exception as e:
                    logger.error(
                        "Batch %d/%d failed: %s", batch_idx, len(batches), e,
                    )
                    raise

        logger.info("ANN returned %d raw edge pairs", len(all_edges))
        return self._postprocess_ann_edges(all_edges)

    def _postprocess_ann_edges(self, raw_edges: List[Tuple]) -> DataFrame:
        """Deduplicate and dress ANN results into the standard edge schema."""
        if not raw_edges:
            return self.spark.createDataFrame([], _EDGE_SCHEMA)

        schema = (
            "src STRING, src_type STRING, src_domain STRING, "
            "dst STRING, dst_type STRING, dst_domain STRING, similarity DOUBLE"
        )
        df = self.spark.createDataFrame(raw_edges, schema)

        # Entity-entity exclusion (matches _NO_ENTITY_PAIR in cross-join path)
        df = df.filter(
            ~((F.col("src_type") == "entity") & (F.col("dst_type") == "entity"))
        )

        # Symmetric dedup: keep one direction per pair, max similarity
        df = (
            df
            .withColumn("lo", F.least("src", "dst"))
            .withColumn("hi", F.greatest("src", "dst"))
            .groupBy("lo", "hi")
            .agg(F.max("similarity").alias("weight"))
            .withColumnRenamed("lo", "src")
            .withColumnRenamed("hi", "dst")
        )

        # Per-node cap (matches cross-join ROW_NUMBER behavior)
        from pyspark.sql.window import Window
        w_src = Window.partitionBy("src").orderBy(F.col("weight").desc())
        w_dst = Window.partitionBy("dst").orderBy(F.col("weight").desc())
        max_e = self.config.max_edges_per_node
        df = (
            df
            .withColumn("rn_src", F.row_number().over(w_src))
            .withColumn("rn_dst", F.row_number().over(w_dst))
            .filter((F.col("rn_src") <= max_e) | (F.col("rn_dst") <= max_e))
            .drop("rn_src", "rn_dst")
        )

        return df.select(
            "src", "dst",
            F.lit(self.RELATIONSHIP_TYPE).alias("relationship"),
            F.col("weight"),
            F.concat_ws("::", "src", "dst", F.lit(self.RELATIONSHIP_TYPE)).alias("edge_id"),
            F.lit(self.RELATIONSHIP_TYPE).alias("edge_type"),
            F.lit("undirected").alias("direction"),
            F.lit(None).cast("string").alias("join_expression"),
            F.lit(None).cast("double").alias("join_confidence"),
            F.lit(None).cast("string").alias("ontology_rel"),
            F.lit("embedding_similarity").alias("source_system"),
            F.lit("candidate").alias("status"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )

    # ------------------------------------------------------------------
    # Mode switch (ANN with infra-failure fallback to cross-join)
    # ------------------------------------------------------------------

    def compute_similarity_edges(self) -> DataFrame:
        """Dispatch to ANN or cross-join based on config and node count.

        When use_ann=True but the node count is below blocking_node_threshold,
        cross-join is used instead (cheaper than standing up a VS index for
        a handful of nodes).  ANN failures also fall back to cross-join with
        a warning so the pipeline does not hard-fail on VS issues.
        """
        node_count = self.get_nodes_with_embeddings().count()

        if not self.config.use_ann:
            logger.info("Using cross-join path (use_ann=False)")
            return self._compute_similarity_crossjoin()

        if node_count < self.config.blocking_node_threshold:
            logger.info(
                "Node count %d < blocking_node_threshold %d — using cross-join (cheaper for small graphs)",
                node_count, self.config.blocking_node_threshold,
            )
            return self._compute_similarity_crossjoin()

        try:
            logger.info("Using ANN path (Vector Search) for %d nodes", node_count)
            self.ensure_graph_nodes_index()
            return self.compute_similarity_edges_ann()
        except ValueError:
            raise
        except Exception as e:
            logger.warning(
                "ANN path failed (%s), falling back to cross-join. "
                "This may be slow for %d nodes.", e, node_count,
            )
            return self._compute_similarity_crossjoin()

    # ------------------------------------------------------------------
    # Edge I/O
    # ------------------------------------------------------------------

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
        method = "ann" if self.config.use_ann else "crossjoin"
        logger.info("Starting similarity edge generation (method=%s)", method)

        self.remove_existing_similarity_edges()

        edges_df = self.compute_similarity_edges()
        edge_count = edges_df.count()
        logger.info(f"Computed {edge_count} similarity edges")

        inserted = self.insert_similarity_edges(edges_df)

        logger.info(f"Similarity edge generation complete. Edges: {inserted}")

        return {
            "method": method,
            "edges_created": inserted,
            "threshold": self.config.similarity_threshold,
        }


def build_similarity_edges(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    similarity_threshold: float = 0.7,
    max_edges_per_node: int = 15,
    use_ann: bool = True,
    **kwargs,
) -> Dict[str, Any]:
    """
    Convenience function to build similarity edges.

    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        similarity_threshold: Minimum similarity for edge creation
        max_edges_per_node: Maximum edges per node
        use_ann: If True, use Vector Search ANN instead of cross-join

    Returns:
        Dict with execution statistics including 'method' key
    """
    config = SimilarityEdgesConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        similarity_threshold=similarity_threshold,
        max_edges_per_node=max_edges_per_node,
        use_ann=use_ann,
        **{k: v for k, v in kwargs.items() if hasattr(SimilarityEdgesConfig, k)},
    )
    builder = SimilarityEdgeBuilder(spark, config)
    return builder.run()

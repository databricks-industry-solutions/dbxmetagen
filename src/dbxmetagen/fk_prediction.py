"""
Foreign Key Prediction module.

Join-key discovery uses: (1) blocking by catalog.schema, (2) column embedding
similarity with same-block duplicate-table suppression and stricter cross-block
floors, (3) declared FKs, query-history joins, naming and ontology heuristics,
(4) rule-based scoring, (5) budgeted AI_QUERY as a tie-breaker, with optional
skip-AI for high-trust declared/query pairs.
"""

import logging
import warnings
from dataclasses import dataclass
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

_FK_MODEL = "databricks-gpt-oss-120b"

SR_DECLARED = 0
SR_QUERY = 1
SR_NAME = 2
SR_ONTOLOGY = 3
SR_EMBEDDING = 4


@dataclass
class FKPredictionConfig:
    catalog_name: str
    schema_name: str
    nodes_table: str = "graph_nodes"
    edges_table: str = "graph_edges"
    column_kb_table: str = "column_knowledge_base"
    ontology_entities_table: str = "ontology_entities"
    ontology_relationships_table: str = "ontology_relationships"
    predictions_table: str = "fk_predictions"
    column_similarity_threshold: float = 0.85
    table_similarity_threshold: float = 0.9
    duplicate_table_similarity_threshold: float = 0.97
    cross_block_column_similarity_min: float = 0.92
    cross_block_strict: bool = True
    ontology_cross_block: bool = False
    skip_ai_for_declared_fk: bool = False
    skip_ai_query_min_observations: int = 0
    sample_size: int = 50
    confidence_threshold: float = 0.7
    apply_ddl: bool = False
    dry_run: bool = False
    ontology_match_bonus_weight: float = 0.15
    rule_score_min_for_ai: float = 0.50
    incremental: bool = True
    max_candidates_per_table_pair: int = 5
    cardinality_sample_rows: int = 100000
    max_ai_candidates: int = 200

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"


class FKPredictor:
    RELATIONSHIP_TYPE = "predicted_fk"

    def __init__(self, spark: SparkSession, config: FKPredictionConfig):
        self.spark = spark
        self.config = config
        self._table_samples: dict = {}
        if self.config.table_similarity_threshold != 0.9:
            warnings.warn(
                "FKPredictionConfig.table_similarity_threshold is deprecated and ignored; "
                "use duplicate_table_similarity_threshold and cross_block_column_similarity_min.",
                DeprecationWarning,
                stacklevel=2,
            )

    def _cap_candidates(self, df: DataFrame) -> DataFrame:
        """Limit candidates to top-K per table pair by name match quality."""
        k = self.config.max_candidates_per_table_pair
        w = Window.partitionBy("table_a", "table_b").orderBy(
            F.col("col_similarity").desc()
        )
        return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") <= k).drop("_rn")

    def _apply_tiered_ai_cap(self, candidates: DataFrame) -> DataFrame:
        """Cap only embedding-sourced rows; deterministic sources are never dropped."""
        max_ai = self.config.max_ai_candidates
        min_rule = self.config.rule_score_min_for_ai
        tier_a = candidates.filter(F.col("source_rank") != F.lit(SR_EMBEDDING))
        tier_b = candidates.filter(F.col("source_rank") == F.lit(SR_EMBEDDING))
        tier_b_hi = tier_b.filter(F.col("rule_score") >= min_rule)
        tier_b_lo = tier_b.filter(F.col("rule_score") < min_rule)
        if tier_b_hi.count() <= max_ai:
            return tier_a.unionByName(tier_b)
        w = Window.orderBy(F.col("rule_score").desc())
        tier_b_hi = (
            tier_b_hi.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") <= max_ai)
            .drop("_rn")
        )
        return tier_a.unionByName(tier_b_hi).unionByName(tier_b_lo)

    def _with_skip_ai_flags(self, df: DataFrame) -> DataFrame:
        qmin = self.config.skip_ai_query_min_observations
        return df.withColumn(
            "skip_ai",
            (
                (F.col("source_rank") == F.lit(SR_DECLARED))
                & F.lit(self.config.skip_ai_for_declared_fk)
            )
            | (
                (F.col("source_rank") == F.lit(SR_QUERY))
                & (F.col("query_hit_count") >= F.lit(qmin))
                & (F.col("query_hit_count") > F.lit(0))
            ),
        )

    def _heuristic_ai_fill(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ai_is_fk", F.lit(True))
            .withColumn(
                "ai_confidence",
                F.when(F.col("source_rank") == F.lit(SR_DECLARED), F.lit(0.95)).otherwise(F.lit(0.88)),
            )
            .withColumn(
                "ai_reasoning",
                F.when(F.col("source_rank") == F.lit(SR_DECLARED), F.lit("declared_foreign_key"))
                .when(F.col("source_rank") == F.lit(SR_QUERY), F.lit("query_history_skip_ai"))
                .otherwise(F.lit("heuristic_skip_ai")),
            )
            .withColumn(
                "ai_fk_side",
                F.when(F.col("source_rank") == F.lit(SR_DECLARED), F.lit("a")).otherwise(
                    F.lit(None).cast("string")
                ),
            )
        )

    # ------------------------------------------------------------------
    # Step 1: Candidate selection
    # ------------------------------------------------------------------
    def get_candidates(self) -> DataFrame:
        """Embedding column pairs with block-aware duplicate suppression and cross-block floors.

        Incremental mode applies only here; declared/query/name/ontology paths run fully each job.
        """
        nodes = self.config.fq(self.config.nodes_table)
        edges = self.config.fq(self.config.edges_table)
        preds = self.config.fq(self.config.predictions_table)
        col_thresh = self.config.column_similarity_threshold
        dup_t = self.config.duplicate_table_similarity_threshold
        xb_min = self.config.cross_block_column_similarity_min

        cross_block_sql = ""
        if self.config.cross_block_strict:
            cross_block_sql = f"""
          AND (j.block_a = j.block_b OR j.col_similarity >= {xb_min})"""

        changed_tables_cte = ""
        changed_tables_filter = ""
        if self.config.incremental:
            try:
                last_run = self.spark.sql(
                    f"SELECT COALESCE(MAX(created_at), TIMESTAMP '1970-01-01') AS lr FROM {preds}"
                ).collect()[0].lr
                changed_tables_cte = f""",
        changed_tables AS (
            SELECT DISTINCT parent_id
            FROM {nodes}
            WHERE updated_at > TIMESTAMP '{last_run}'
        )"""
                changed_tables_filter = """
          AND (j.table_a IN (SELECT parent_id FROM changed_tables)
               OR j.table_b IN (SELECT parent_id FROM changed_tables))"""
                logger.info(
                    "Incremental mode: embedding candidates limited to tables changed since %s",
                    last_run,
                )
            except Exception as e:
                logger.warning("Incremental filtering failed (%s), using full candidate set", e)

        sql = f"""
        WITH col_sim_raw AS (
            SELECT LEAST(e.src, e.dst) AS col_a, GREATEST(e.src, e.dst) AS col_b,
                   MAX(e.weight) AS col_similarity
            FROM {edges} e
            JOIN {nodes} n1 ON e.src = n1.id AND n1.node_type = 'column'
            JOIN {nodes} n2 ON e.dst = n2.id AND n2.node_type = 'column'
            WHERE e.relationship = 'similar_embedding'
              AND e.weight >= {col_thresh}
              AND n1.parent_id != n2.parent_id
            GROUP BY LEAST(e.src, e.dst), GREATEST(e.src, e.dst)
        ),
        with_parents AS (
            SELECT cs.col_a, cs.col_b, cs.col_similarity,
                   n1.parent_id AS table_a, n2.parent_id AS table_b,
                   n1.data_type AS dtype_a, n2.data_type AS dtype_b
            FROM col_sim_raw cs
            JOIN {nodes} n1 ON cs.col_a = n1.id
            JOIN {nodes} n2 ON cs.col_b = n2.id
        ),
        with_blocks AS (
            SELECT wp.*,
              CONCAT_WS('.', ELEMENT_AT(SPLIT(wp.table_a, '\\\\.'), 1),
                ELEMENT_AT(SPLIT(wp.table_a, '\\\\.'), 2)) AS block_a,
              CONCAT_WS('.', ELEMENT_AT(SPLIT(wp.table_b, '\\\\.'), 1),
                ELEMENT_AT(SPLIT(wp.table_b, '\\\\.'), 2)) AS block_b
            FROM with_parents wp
        ),
        tbl_sim AS (
            SELECT src, dst, weight AS table_similarity
            FROM {edges}
            WHERE relationship = 'similar_embedding'
        ),
        joined AS (
            SELECT wb.*,
              COALESCE(ts.table_similarity, 0.0) AS table_similarity
            FROM with_blocks wb
            LEFT JOIN tbl_sim ts
              ON (wb.table_a = ts.src AND wb.table_b = ts.dst)
              OR (wb.table_a = ts.dst AND wb.table_b = ts.src)
        ){changed_tables_cte}
        SELECT j.col_a, j.col_b, j.col_similarity,
               j.table_a, j.table_b, j.dtype_a, j.dtype_b, j.table_similarity
        FROM joined j
        WHERE NOT (
            j.block_a = j.block_b
            AND j.table_similarity >= {dup_t}
        ){cross_block_sql}{changed_tables_filter}
        """
        df = self.spark.sql(sql)
        df = (
            df.withColumn("source_rank", F.lit(SR_EMBEDDING))
            .withColumn("query_hit_count", F.lit(0))
        )
        n = df.count()
        logger.info("FK embedding candidates found: %d", n)
        return df

    # ------------------------------------------------------------------
    # Step 1a: Name-based FK candidates (no embedding required)
    # ------------------------------------------------------------------
    def get_name_based_candidates(self) -> DataFrame:
        """Generate FK candidates from column naming conventions like <table>_id -> id."""
        nodes = self.config.fq(self.config.nodes_table)
        sql = f"""
        WITH cols AS (
            SELECT id, parent_id, data_type,
                   LOWER(ELEMENT_AT(SPLIT(id, '\\\\.'), -1)) AS col_short,
                   LOWER(ELEMENT_AT(SPLIT(parent_id, '\\\\.'), -1)) AS tbl_short
            FROM {nodes}
            WHERE node_type = 'column'
        ),
        pk_cols AS (
            SELECT * FROM cols WHERE col_short IN ('id', 'pk')
        ),
        fk_cols AS (
            SELECT * FROM cols
            WHERE col_short RLIKE '(_id|_key)$' AND col_short NOT IN ('id', 'pk')
        ),
        matches AS (
            SELECT
                fk.id AS col_a, pk.id AS col_b,
                fk.parent_id AS table_a, pk.parent_id AS table_b,
                fk.data_type AS dtype_a, pk.data_type AS dtype_b,
                0.0 AS col_similarity, 0.0 AS table_similarity
            FROM fk_cols fk
            JOIN pk_cols pk
              ON fk.parent_id != pk.parent_id
              AND (
                  REPLACE(REPLACE(fk.col_short, '_id', ''), '_key', '')
                  = REGEXP_REPLACE(pk.tbl_short, 's$', '')
                  OR REPLACE(REPLACE(fk.col_short, '_id', ''), '_key', '')
                  = pk.tbl_short
              )
        )
        SELECT LEAST(col_a, col_b) AS col_a, GREATEST(col_a, col_b) AS col_b,
               col_similarity, table_a, table_b, dtype_a, dtype_b, table_similarity
        FROM matches
        """
        try:
            df = self.spark.sql(sql)
            df = self._cap_candidates(df)
            df = df.withColumn("source_rank", F.lit(SR_NAME)).withColumn(
                "query_hit_count", F.lit(0)
            )
            logger.info("Name-based FK candidates: %d", df.count())
            return df
        except Exception as e:
            logger.warning("Name-based candidate generation failed: %s", e)
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, col_similarity DOUBLE, "
                "table_a STRING, table_b STRING, dtype_a STRING, dtype_b STRING, "
                "table_similarity DOUBLE, source_rank INT, query_hit_count INT"
            )

    # ------------------------------------------------------------------
    # Step 1b: Ontology-driven FK candidates
    # ------------------------------------------------------------------
    def get_ontology_relationship_candidates(self) -> DataFrame:
        """Generate FK candidates from ontology_relationships (same-block by default)."""
        ont_rels = self.config.fq(self.config.ontology_relationships_table)
        ont_ent = self.config.fq(self.config.ontology_entities_table)
        nodes = self.config.fq(self.config.nodes_table)
        k = self.config.max_candidates_per_table_pair
        try:
            self.spark.sql(f"SELECT 1 FROM {ont_rels} LIMIT 1").collect()
        except Exception:
            logger.info("ontology_relationships table not available, skipping")
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, col_similarity DOUBLE, "
                "table_a STRING, table_b STRING, dtype_a STRING, dtype_b STRING, "
                "table_similarity DOUBLE, source_rank INT, query_hit_count INT"
            )

        block_filter = ""
        if not self.config.ontology_cross_block:
            block_filter = """
              AND CONCAT_WS('.', ELEMENT_AT(SPLIT(st.table_name, '\\.'), 1),
                    ELEMENT_AT(SPLIT(st.table_name, '\\.'), 2))
                  = CONCAT_WS('.', ELEMENT_AT(SPLIT(dt.table_name, '\\.'), 1),
                    ELEMENT_AT(SPLIT(dt.table_name, '\\.'), 2))"""

        sql = f"""
        WITH rels AS (
            SELECT DISTINCT src_entity_type, dst_entity_type
            FROM {ont_rels}
            WHERE confidence >= 0.4
        ),
        src_tables AS (
            SELECT entity_type, EXPLODE(source_tables) AS table_name FROM {ont_ent}
        ),
        table_pairs AS (
            SELECT st.table_name AS table_a, dt.table_name AS table_b
            FROM rels r
            JOIN src_tables st ON r.src_entity_type = st.entity_type
            JOIN src_tables dt ON r.dst_entity_type = dt.entity_type
            WHERE st.table_name != dt.table_name
            {block_filter}
        ),
        cols AS (
            SELECT id, parent_id, data_type,
                   LOWER(ELEMENT_AT(SPLIT(id, '\\.'), -1)) AS col_short
            FROM {nodes} WHERE node_type = 'column'
        ),
        id_like_cols AS (
            SELECT * FROM cols WHERE col_short RLIKE '(_id|_key|_code)$' OR col_short = 'id'
        ),
        candidate_pairs AS (
            SELECT ca.id AS col_a, cb.id AS col_b,
                   tp.table_a, tp.table_b,
                   ca.data_type AS dtype_a, cb.data_type AS dtype_b,
                   0.0 AS col_similarity, 0.0 AS table_similarity,
                   CASE
                     WHEN ca.col_short = cb.col_short THEN 3.0
                     WHEN REPLACE(REPLACE(ca.col_short, '_id', ''), '_key', '')
                       = REGEXP_REPLACE(LOWER(ELEMENT_AT(SPLIT(tp.table_b, '\\.'), -1)), 's$', '') THEN 2.0
                     WHEN REPLACE(REPLACE(cb.col_short, '_id', ''), '_key', '')
                       = REGEXP_REPLACE(LOWER(ELEMENT_AT(SPLIT(tp.table_a, '\\.'), -1)), 's$', '') THEN 2.0
                     ELSE 1.0
                   END AS name_match_score
            FROM table_pairs tp
            JOIN id_like_cols ca ON ca.parent_id = tp.table_a
            JOIN id_like_cols cb ON cb.parent_id = tp.table_b
            WHERE ca.id != cb.id
              AND (
                  ca.col_short = cb.col_short
                  OR REPLACE(REPLACE(ca.col_short, '_id', ''), '_key', '')
                     = REGEXP_REPLACE(LOWER(ELEMENT_AT(SPLIT(tp.table_b, '\\.'), -1)), 's$', '')
                  OR REPLACE(REPLACE(cb.col_short, '_id', ''), '_key', '')
                     = REGEXP_REPLACE(LOWER(ELEMENT_AT(SPLIT(tp.table_a, '\\.'), -1)), 's$', '')
              )
        ),
        capped AS (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY table_a, table_b ORDER BY name_match_score DESC
                ) AS _tp_rn
                FROM candidate_pairs
            ) z WHERE z._tp_rn <= {k}
        )
        SELECT LEAST(col_a, col_b) AS col_a, GREATEST(col_a, col_b) AS col_b,
               col_similarity, table_a, table_b, dtype_a, dtype_b, table_similarity
        FROM capped
        """
        try:
            df = self.spark.sql(sql)
            df = df.withColumn("source_rank", F.lit(SR_ONTOLOGY)).withColumn(
                "query_hit_count", F.lit(0)
            )
            logger.info("Ontology-driven FK candidates: %d", df.count())
            return df
        except Exception as e:
            logger.warning("Ontology-driven candidate generation failed: %s", e)
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, col_similarity DOUBLE, "
                "table_a STRING, table_b STRING, dtype_a STRING, dtype_b STRING, "
                "table_similarity DOUBLE, source_rank INT, query_hit_count INT"
            )


    # ------------------------------------------------------------------
    # Step 1c-pre: Declared FK candidates from extended_table_metadata
    # ------------------------------------------------------------------
    def get_declared_fk_candidates(self) -> DataFrame:
        """Emit candidates from declared FKs in extended_table_metadata.foreign_keys."""
        ext = self.config.fq("extended_table_metadata")
        nodes = self.config.fq(self.config.nodes_table)
        try:
            rows = self.spark.sql(
                f"SELECT table_name, foreign_keys FROM {ext} "
                f"WHERE foreign_keys IS NOT NULL AND SIZE(foreign_keys) > 0"
            ).collect()
        except Exception:
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, table_a STRING, table_b STRING, "
                "dtype_a STRING, dtype_b STRING, col_similarity DOUBLE, table_similarity DOUBLE, "
                "source_rank INT, query_hit_count INT"
            )
        pairs = []
        for r in rows:
            src_table = r.table_name
            for fk_col, ref_target in (r.foreign_keys or {}).items():
                if not ref_target:
                    continue
                parts = ref_target.split(".")
                if len(parts) < 4:
                    continue
                ref_table = ".".join(parts[:3])
                ref_col = parts[3]
                if src_table == ref_table:
                    continue
                col_a = f"{src_table}.{fk_col}"
                col_b = f"{ref_table}.{ref_col}"
                pairs.append((col_a, col_b, src_table, ref_table, "", "", 1.0, 0.0))
        if not pairs:
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, table_a STRING, table_b STRING, "
                "dtype_a STRING, dtype_b STRING, col_similarity DOUBLE, table_similarity DOUBLE, "
                "source_rank INT, query_hit_count INT"
            )
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("col_a", StringType()), StructField("col_b", StringType()),
            StructField("table_a", StringType()), StructField("table_b", StringType()),
            StructField("dtype_a", StringType()), StructField("dtype_b", StringType()),
            StructField("col_similarity", DoubleType()), StructField("table_similarity", DoubleType()),
        ])
        df = self.spark.createDataFrame(pairs, schema)
        return df.withColumn("source_rank", F.lit(SR_DECLARED)).withColumn(
            "query_hit_count", F.lit(0)
        )

    # ------------------------------------------------------------------
    # Step 1c-post: Query history join candidates
    # ------------------------------------------------------------------
    def get_query_join_candidates(self) -> DataFrame:
        """Mine system.query.history for commonly-used JOIN patterns."""
        import re as _re
        cat, sch = self.config.catalog_name, self.config.schema_name
        empty_schema = (
            "col_a STRING, col_b STRING, table_a STRING, table_b STRING, "
            "dtype_a STRING, dtype_b STRING, col_similarity DOUBLE, table_similarity DOUBLE, "
            "query_hit_count LONG, source_rank INT"
        )
        try:
            rows = self.spark.sql(f"""
                SELECT statement FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
                  AND statement LIKE '%JOIN%'
                  AND statement LIKE '%{cat}.{sch}%'
                LIMIT 2000
            """).collect()
        except Exception as e:
            logger.debug("Query history not available: %s", e)
            return self.spark.createDataFrame([], empty_schema)

        join_re = _re.compile(
            r"(\w+(?:\.\w+){0,3})\.(\w+)\s*=\s*(\w+(?:\.\w+){0,3})\.(\w+)",
            _re.IGNORECASE,
        )
        prefix = f"{cat}.{sch}."
        pair_counts: dict = {}
        for r in rows:
            for m in join_re.finditer(r.statement or ""):
                tbl_a_raw, col_a_raw = m.group(1), m.group(2)
                tbl_b_raw, col_b_raw = m.group(3), m.group(4)
                # Resolve to FQN if alias-like (short name) -- skip if can't resolve
                tbl_a = tbl_a_raw if prefix.lower() in tbl_a_raw.lower() else None
                tbl_b = tbl_b_raw if prefix.lower() in tbl_b_raw.lower() else None
                if not tbl_a or not tbl_b:
                    continue
                if tbl_a.lower() == tbl_b.lower():
                    continue
                col_a_fq = f"{tbl_a}.{col_a_raw}"
                col_b_fq = f"{tbl_b}.{col_b_raw}"
                key = (min(col_a_fq, col_b_fq), max(col_a_fq, col_b_fq))
                pair_counts[key] = pair_counts.get(key, 0) + 1

        if not pair_counts:
            return self.spark.createDataFrame([], empty_schema)

        # Keep pairs seen >= 2 times
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        pairs = []
        for (ca, cb), cnt in pair_counts.items():
            if cnt < 2:
                continue
            tbl_a = ".".join(ca.split(".")[:-1])
            tbl_b = ".".join(cb.split(".")[:-1])
            pairs.append((ca, cb, tbl_a, tbl_b, "", "", min(cnt / 10.0, 1.0), 0.0, int(cnt)))
        if not pairs:
            return self.spark.createDataFrame([], empty_schema)
        schema = StructType([
            StructField("col_a", StringType()), StructField("col_b", StringType()),
            StructField("table_a", StringType()), StructField("table_b", StringType()),
            StructField("dtype_a", StringType()), StructField("dtype_b", StringType()),
            StructField("col_similarity", DoubleType()), StructField("table_similarity", DoubleType()),
            StructField("query_hit_count", LongType()),
        ])
        df = self.spark.createDataFrame(pairs, schema)
        df = df.withColumn("source_rank", F.lit(SR_QUERY))
        logger.info("Query-history FK candidates: %d", df.count())
        return df

    # ------------------------------------------------------------------
    # Step 1d: Ontology entity match (optional)
    # ------------------------------------------------------------------
    def add_entity_match(self, candidates: DataFrame) -> DataFrame:
        """Join ontology_entities + ontology_relationships to score entity alignment.
        0.25 for inter-entity relationship, 0.1 for same-entity (self-referential FK support)."""
        ont = self.config.fq(self.config.ontology_entities_table)
        ont_rels = self.config.fq(self.config.ontology_relationships_table)
        try:
            self.spark.sql(f"SELECT 1 FROM {ont} LIMIT 1").collect()
        except Exception:
            return candidates.withColumn("entity_match", F.lit(0.0)) \
                .withColumn("entity_type_a", F.lit(None).cast("string")) \
                .withColumn("entity_type_b", F.lit(None).cast("string"))
        candidates.createOrReplaceTempView("fk_cand")

        has_rels = False
        try:
            self.spark.sql(f"SELECT 1 FROM {ont_rels} LIMIT 1").collect()
            has_rels = True
        except Exception:
            pass

        rels_cte = ""
        rel_join = ""
        rel_case = "0.0"
        if has_rels:
            rels_cte = f""",
            ent_rels AS (
                SELECT DISTINCT src_entity_type, dst_entity_type
                FROM {ont_rels} WHERE confidence >= 0.4
            )"""
            rel_join = """
            LEFT JOIN ent_rels r1
                ON ta.entity_type = r1.src_entity_type AND tb.entity_type = r1.dst_entity_type
            LEFT JOIN ent_rels r2
                ON ta.entity_type = r2.dst_entity_type AND tb.entity_type = r2.src_entity_type"""
            rel_case = "CASE WHEN r1.src_entity_type IS NOT NULL OR r2.src_entity_type IS NOT NULL THEN 1.0 ELSE 0.0 END"

        df = self.spark.sql(f"""
            WITH table_entity AS (
                SELECT EXPLODE(source_tables) AS table_name, entity_type
                FROM {ont} WHERE confidence >= 0.4
            ){rels_cte}
            SELECT c.*,
                ta.entity_type AS entity_type_a,
                tb.entity_type AS entity_type_b,
                CASE
                    WHEN ta.entity_type IS NOT NULL AND tb.entity_type IS NOT NULL THEN
                        GREATEST(
                            {rel_case},
                            CASE WHEN ta.entity_type = tb.entity_type THEN 0.4 ELSE 0.0 END
                        )
                    ELSE 0.0
                END AS entity_match
            FROM fk_cand c
            LEFT JOIN table_entity ta ON c.table_a = ta.table_name
            LEFT JOIN table_entity tb ON c.table_b = tb.table_name
            {rel_join}
        """)
        return df

    # ------------------------------------------------------------------
    # Step 1d: Lineage-based candidate boosting
    # ------------------------------------------------------------------
    def add_lineage_signal(self, candidates: DataFrame) -> DataFrame:
        """Score candidates based on table lineage overlap from extended_table_metadata."""
        ext = self.config.fq("extended_table_metadata")
        try:
            lineage_rows = self.spark.sql(
                f"SELECT table_name, upstream_tables, downstream_tables FROM {ext}"
            ).collect()
        except Exception:
            return candidates.withColumn("lineage_score", F.lit(0.0))

        upstream_map: dict = {}
        downstream_map: dict = {}
        for r in lineage_rows:
            upstream_map[r.table_name] = set(r.upstream_tables or [])
            downstream_map[r.table_name] = set(r.downstream_tables or [])

        def _score(tbl_a: str, tbl_b: str) -> float:
            up_a, up_b = upstream_map.get(tbl_a, set()), upstream_map.get(tbl_b, set())
            dn_a, dn_b = downstream_map.get(tbl_a, set()), downstream_map.get(tbl_b, set())
            if tbl_b in up_a or tbl_b in dn_a or tbl_a in up_b or tbl_a in dn_b:
                return 1.0
            if up_a & up_b:
                return 0.6
            return 0.0

        pairs = candidates.select("table_a", "table_b").distinct().collect()
        scores = [(r.table_a, r.table_b, _score(r.table_a, r.table_b)) for r in pairs]
        if not scores:
            return candidates.withColumn("lineage_score", F.lit(0.0))

        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("_la", StringType()), StructField("_lb", StringType()),
            StructField("lineage_score", DoubleType()),
        ])
        lin_df = self.spark.createDataFrame(scores, schema)
        result = candidates.join(
            lin_df,
            (candidates.table_a == lin_df._la) & (candidates.table_b == lin_df._lb),
            "left",
        ).drop("_la", "_lb")
        return result.withColumn("lineage_score", F.coalesce(F.col("lineage_score"), F.lit(0.0)))

    # ------------------------------------------------------------------
    # Step 2: Sample values
    # ------------------------------------------------------------------
    def sample_values(self, candidates: DataFrame) -> DataFrame:
        """Fallback: carry forward dtype and rely on AI + rules (no source sampling)."""
        return candidates.withColumn(
            "samples_a", F.lit(None).cast("array<string>")
        ).withColumn("samples_b", F.lit(None).cast("array<string>"))

    def _sample_from_source(self, candidates: DataFrame) -> DataFrame:
        """Sample values from source tables, batched per-table to minimize SQL calls."""
        rows = (
            candidates.select("col_a", "col_b", "table_a", "table_b")
            .distinct()
            .collect()
        )
        n = self.config.sample_size

        # Group columns by table so we issue one query per table
        table_cols: dict = {}  # table -> set of (fq_col_id, short_name)
        for row in rows:
            for col_id, tbl_id in [(row.col_a, row.table_a), (row.col_b, row.table_b)]:
                table_cols.setdefault(tbl_id, set()).add((col_id, col_id.split(".")[-1]))

        from concurrent.futures import ThreadPoolExecutor, as_completed

        sample_map: dict = {}

        def _fetch_table_samples(tbl_id, col_list):
            selects = ", ".join(
                f"CAST(`{short}` AS STRING) AS `{short}`" for _, short in col_list
            )
            result = {}
            try:
                tbl_rows = self.spark.sql(
                    f"SELECT {selects} FROM {tbl_id} LIMIT {n * 3}"
                ).collect()
                for fq_id, short in col_list:
                    vals = list({getattr(r, short) for r in tbl_rows if getattr(r, short, None) is not None})[:n]
                    result[fq_id] = vals
            except Exception:
                for fq_id, _ in col_list:
                    result[fq_id] = []
            return result

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = {
                pool.submit(_fetch_table_samples, tbl_id, list(col_set)): tbl_id
                for tbl_id, col_set in table_cols.items()
            }
            for f in as_completed(futures):
                sample_map.update(f.result())

        bc = self.spark.sparkContext.broadcast(sample_map)
        get_samples = F.udf(lambda cid: bc.value.get(cid, []), "array<string>")
        return candidates.withColumn(
            "samples_a", get_samples(F.col("col_a"))
        ).withColumn("samples_b", get_samples(F.col("col_b")))

    # ------------------------------------------------------------------
    # Step 2b: Cardinality analysis
    # ------------------------------------------------------------------
    def _load_profiling_stats(self) -> dict:
        """Try to load pre-computed column stats keyed by FQN col_id (latest snapshot only)."""
        stats: dict = {}
        try:
            rows = self.spark.sql(
                f"SELECT cs.table_name, cs.column_name, cs.distinct_count "
                f"FROM {self.config.fq('column_profiling_stats')} cs "
                f"INNER JOIN ("
                f"  SELECT snapshot_id, table_name FROM ("
                f"    SELECT snapshot_id, table_name, ROW_NUMBER() OVER "
                f"      (PARTITION BY table_name ORDER BY snapshot_time DESC) rn "
                f"    FROM {self.config.fq('profiling_snapshots')}"
                f"  ) WHERE rn = 1"
                f") latest ON cs.snapshot_id = latest.snapshot_id "
                f"  AND cs.table_name = latest.table_name"
            ).collect()
            for r in rows:
                stats[f"{r.table_name}.{r.column_name}"] = r.distinct_count
        except Exception:
            pass
        return stats

    def _load_profiling_row_counts(self) -> dict:
        """Try to load per-table row counts from profiling_snapshots."""
        counts: dict = {}
        try:
            rows = self.spark.sql(
                f"SELECT table_name, row_count FROM ("
                f"  SELECT table_name, row_count, ROW_NUMBER() OVER "
                f"    (PARTITION BY table_name ORDER BY snapshot_time DESC) rn "
                f"  FROM {self.config.fq('profiling_snapshots')}"
                f") WHERE rn = 1"
            ).collect()
            for r in rows:
                counts[r.table_name] = r.row_count
        except Exception:
            pass
        return counts

    def cardinality_analysis(self, candidates: DataFrame) -> DataFrame:
        """Batch cardinality analysis using profiling stats when available,
        falling back to TABLESAMPLE for unprofiled columns."""
        col_a_rows = candidates.select(
            F.col("col_a").alias("col_id"), F.col("table_a").alias("tbl_id")
        ).distinct()
        col_b_rows = candidates.select(
            F.col("col_b").alias("col_id"), F.col("table_b").alias("tbl_id")
        ).distinct()
        all_cols = col_a_rows.unionByName(col_b_rows).distinct().collect()

        # Try pre-computed profiling stats first
        profiled_distinct = self._load_profiling_stats()
        profiled_row_counts = self._load_profiling_row_counts()

        col_stats: dict = {}
        unprofiled_by_table: dict = {}
        for r in all_cols:
            col_short = r.col_id.split(".")[-1]
            dc = profiled_distinct.get(r.col_id)
            rc = profiled_row_counts.get(r.tbl_id)
            if dc is not None and rc and rc > 0:
                col_stats[r.col_id] = dc / rc
            else:
                unprofiled_by_table.setdefault(r.tbl_id, []).append((r.col_id, col_short))

        # Fallback: TABLESAMPLE for unprofiled columns
        sample_n = self.config.cardinality_sample_rows
        for tbl, cols_list in unprofiled_by_table.items():
            try:
                count_exprs = ", ".join(
                    f"COUNT(DISTINCT `{cs}`) AS `d_{ci.replace('.', '_')}`"
                    for ci, cs in cols_list
                )
                row = self.spark.sql(
                    f"SELECT COUNT(*) AS total, {count_exprs} FROM {tbl} TABLESAMPLE ({sample_n} ROWS)"
                ).collect()[0]
                total = max(row.total, 1)
                for ci, cs in cols_list:
                    col_stats[ci] = row[f"d_{ci.replace('.', '_')}"] / total
            except Exception as e:
                logger.debug("Cardinality query failed for table %s: %s", tbl, e)
                for ci, _ in cols_list:
                    col_stats[ci] = 0.5

        stats = []
        for r in candidates.select("col_a", "col_b").distinct().collect():
            a_ratio = col_stats.get(r.col_a, 0.5)
            b_ratio = col_stats.get(r.col_b, 0.5)
            stats.append((r.col_a, r.col_b, max(a_ratio, b_ratio), a_ratio, b_ratio))

        if not stats:
            return candidates.withColumn("pk_uniqueness", F.lit(0.5)) \
                .withColumn("_card_ratio_a", F.lit(0.5)) \
                .withColumn("_card_ratio_b", F.lit(0.5))

        card_df = self.spark.createDataFrame(
            stats, ["_ca", "_cb", "pk_uniqueness", "_card_ratio_a", "_card_ratio_b"]
        )
        result = candidates.join(
            card_df,
            (candidates.col_a == card_df._ca) & (candidates.col_b == card_df._cb),
            "left",
        ).drop("_ca", "_cb")
        return (
            result
            .withColumn("pk_uniqueness", F.coalesce(F.col("pk_uniqueness"), F.lit(0.5)))
            .withColumn("_card_ratio_a", F.coalesce(F.col("_card_ratio_a"), F.lit(0.5)))
            .withColumn("_card_ratio_b", F.coalesce(F.col("_card_ratio_b"), F.lit(0.5)))
        )

    # ------------------------------------------------------------------
    # Step 2c: Referential integrity check
    # ------------------------------------------------------------------
    def _ensure_table_sample(self, table: str, col_short: str) -> str:
        """Sample a table once and register as temp view; return the view name."""
        view_key = (table, col_short)
        if view_key not in self._table_samples:
            n = self.config.cardinality_sample_rows
            view_name = f"_sample_{table.replace('.', '_')}_{col_short}"
            try:
                self.spark.sql(
                    f"SELECT CAST(`{col_short}` AS STRING) AS val "
                    f"FROM {table} TABLESAMPLE ({n} ROWS) "
                    f"WHERE `{col_short}` IS NOT NULL"
                ).createOrReplaceTempView(view_name)
            except Exception:
                self.spark.createDataFrame([], "val STRING").createOrReplaceTempView(view_name)
            self._table_samples[view_key] = view_name
        return self._table_samples[view_key]

    def referential_integrity(self, candidates: DataFrame) -> DataFrame:
        """Batch RI check using UNION ALL to minimize round-trips."""
        ri_filter = F.col("rule_score") >= self.config.rule_score_min_for_ai
        if "skip_ai" in candidates.columns:
            ri_filter = ri_filter | F.col("skip_ai")
        rows = (
            candidates.filter(ri_filter)
            .select("col_a", "col_b", "table_a", "table_b")
            .distinct()
            .collect()
        )

        # Pre-create all needed table sample views (cached)
        for row in rows:
            self._ensure_table_sample(row.table_a, row.col_a.split(".")[-1])
            self._ensure_table_sample(row.table_b, row.col_b.split(".")[-1])

        # Build a single UNION ALL query for all RI checks
        fragments = []
        for row in rows:
            va = self._table_samples.get((row.table_a, row.col_a.split(".")[-1]))
            vb = self._table_samples.get((row.table_b, row.col_b.split(".")[-1]))
            if not va or not vb:
                continue
            ca, cb = row.col_a.replace("'", "''"), row.col_b.replace("'", "''")
            fragments.append(
                f"SELECT '{ca}' AS ca, '{cb}' AS cb, "
                f"COUNT(DISTINCT a.val) AS total_a, "
                f"SUM(CASE WHEN b.val IS NULL THEN 1 ELSE 0 END) AS orphan_ab "
                f"FROM (SELECT DISTINCT val FROM {va}) a "
                f"LEFT JOIN (SELECT DISTINCT val FROM {vb}) b ON a.val = b.val"
            )
            fragments.append(
                f"SELECT '{cb}' AS ca, '{ca}' AS cb, "
                f"COUNT(DISTINCT a.val) AS total_a, "
                f"SUM(CASE WHEN b.val IS NULL THEN 1 ELSE 0 END) AS orphan_ab "
                f"FROM (SELECT DISTINCT val FROM {vb}) a "
                f"LEFT JOIN (SELECT DISTINCT val FROM {va}) b ON a.val = b.val"
            )

        if not fragments:
            return candidates.withColumn("ri_score", F.lit(0.5))

        from concurrent.futures import ThreadPoolExecutor, as_completed

        BATCH = 200
        all_ri_rows = []
        batches = [
            " UNION ALL ".join(fragments[i : i + BATCH])
            for i in range(0, len(fragments), BATCH)
        ]

        def _exec_ri_batch(sql):
            return self.spark.sql(sql).collect()

        with ThreadPoolExecutor(max_workers=min(8, len(batches))) as pool:
            futures = [pool.submit(_exec_ri_batch, b) for b in batches]
            for f in as_completed(futures):
                all_ri_rows.extend(f.result())

        # Compute RI per pair: max(ri_ab, ri_ba)
        pair_ri: dict = {}
        for r in all_ri_rows:
            total = max(r.total_a, 1)
            ri = max(0.0, 1.0 - r.orphan_ab / total)
            key = tuple(sorted([r.ca, r.cb]))
            pair_ri[key] = max(pair_ri.get(key, 0.0), ri)

        stats = []
        for row in rows:
            key = tuple(sorted([row.col_a, row.col_b]))
            stats.append((row.col_a, row.col_b, pair_ri.get(key, 0.5)))

        ri_df = self.spark.createDataFrame(stats, ["_ra", "_rb", "ri_score"])
        result = candidates.join(
            ri_df,
            (candidates.col_a == ri_df._ra) & (candidates.col_b == ri_df._rb),
            "left",
        ).drop("_ra", "_rb")
        return result.withColumn("ri_score", F.coalesce(F.col("ri_score"), F.lit(0.5)))

    # ------------------------------------------------------------------
    # Step 3: Rule-based scoring
    # ------------------------------------------------------------------
    def rule_score(self, candidates: DataFrame) -> DataFrame:
        """Compute a heuristic joinability score with improved name matching."""
        # dtype compatibility
        dtype_score = (
            F.when(F.col("dtype_a") == F.col("dtype_b"), 1.0)
            .when(
                (F.col("dtype_a").isin("string", "varchar", "char"))
                & (F.col("dtype_b").isin("int", "bigint", "long", "integer")),
                0.7,
            )
            .when(
                (F.col("dtype_b").isin("string", "varchar", "char"))
                & (F.col("dtype_a").isin("int", "bigint", "long", "integer")),
                0.7,
            )
            .otherwise(0.3)
        )

        # Column name suffix heuristic (id, key, code patterns)
        id_pattern = F.when(
            (F.lower(F.col("col_a")).rlike("(_id|_key|_code)$"))
            | (F.lower(F.col("col_b")).rlike("(_id|_key|_code)$")),
            1.0,
        ).otherwise(0.0)

        # table_name + _id pattern: e.g. encounters.patient_id -> patients.id
        col_a_short = F.lower(F.element_at(F.split(F.col("col_a"), "\\."), -1))
        col_b_short = F.lower(F.element_at(F.split(F.col("col_b"), "\\."), -1))
        tbl_a_short = F.lower(F.element_at(F.split(F.col("table_a"), "\\."), -1))
        tbl_b_short = F.lower(F.element_at(F.split(F.col("table_b"), "\\."), -1))
        # Check if col_a contains table_b name (e.g. patient_id contains 'patient' from 'patients')
        table_name_match = F.when(
            col_a_short.contains(F.regexp_replace(tbl_b_short, "s$", ""))
            | col_b_short.contains(F.regexp_replace(tbl_a_short, "s$", "")),
            1.0,
        ).otherwise(0.0)

        # fk_ or ref_ prefix stripping
        fk_prefix = F.when(
            col_a_short.rlike("^(fk_|ref_)") | col_b_short.rlike("^(fk_|ref_)"),
            1.0,
        ).otherwise(0.0)

        # Value overlap (when samples available)
        overlap = F.when(
            F.col("samples_a").isNotNull() & F.col("samples_b").isNotNull(),
            F.size(F.array_intersect("samples_a", "samples_b"))
            / F.greatest(F.size("samples_a"), F.lit(1)).cast("double"),
        ).otherwise(F.lit(0.0))

        entity_match = F.col("entity_match") if "entity_match" in candidates.columns else F.lit(0.0)
        lineage_sig = F.col("lineage_score") if "lineage_score" in candidates.columns else F.lit(0.0)
        bonus = self.config.ontology_match_bonus_weight

        return candidates.withColumn(
            "rule_score",
            F.round(
                F.col("col_similarity") * 0.25
                + dtype_score * 0.15
                + id_pattern * 0.1
                + table_name_match * 0.15
                + fk_prefix * 0.05
                + overlap * 0.1
                + entity_match * bonus
                + lineage_sig * 0.1,
                4,
            ),
        )

    # ------------------------------------------------------------------
    # Step 4: AI judgment
    # ------------------------------------------------------------------
    def ai_judge(self, candidates: DataFrame) -> DataFrame:
        """Use AI_QUERY to judge FK likelihood, parsing JSON string response."""
        from pyspark.sql.types import (
            StructType,
            StructField,
            BooleanType,
            DoubleType,
            StringType,
        )

        candidates.createOrReplaceTempView("fk_scored")
        col_kb = self.config.fq(self.config.column_kb_table)
        tbl_kb = self.config.fq("table_knowledge_base")
        model = _FK_MODEL

        sql = f"""
        WITH kb_dedup AS (
            SELECT column_id, comment,
                   ROW_NUMBER() OVER (PARTITION BY column_id ORDER BY column_id) AS _kb_rn
            FROM {col_kb}
        ),
        kb AS (
            SELECT column_id, comment FROM kb_dedup WHERE _kb_rn = 1
        ),
        tkb_dedup AS (
            SELECT table_name, comment,
                   ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY table_name) AS _tkb_rn
            FROM {tbl_kb}
        ),
        tkb AS (
            SELECT table_name, comment FROM tkb_dedup WHERE _tkb_rn = 1
        )
        SELECT s.*,
            kb_a.comment AS comment_a, kb_b.comment AS comment_b,
            AI_QUERY(
                '{model}',
                CONCAT(
                    'Assess whether these two columns likely form a foreign key relationship. ',
                    'Also determine which column is the foreign key (child) that references the other (parent/primary key). ',
                    'Table A: ', s.table_a,
                    CASE WHEN tkb_a.comment IS NOT NULL THEN CONCAT(' — ', tkb_a.comment) ELSE '' END, '. ',
                    'Table B: ', s.table_b,
                    CASE WHEN tkb_b.comment IS NOT NULL THEN CONCAT(' — ', tkb_b.comment) ELSE '' END, '. ',
                    'Column A: ', s.col_a, ' (type: ', COALESCE(s.dtype_a, 'unknown'), ')',
                    CASE WHEN kb_a.comment IS NOT NULL THEN CONCAT(' — description: ', kb_a.comment) ELSE '' END, '. ',
                    'Column B: ', s.col_b, ' (type: ', COALESCE(s.dtype_b, 'unknown'), ')',
                    CASE WHEN kb_b.comment IS NOT NULL THEN CONCAT(' — description: ', kb_b.comment) ELSE '' END, '. ',
                    'Embedding similarity: ', CAST(s.col_similarity AS STRING), '. ',
                    'Table similarity: ', CAST(s.table_similarity AS STRING), '. ',
                    CASE WHEN s.entity_type_a IS NOT NULL THEN CONCAT('Table A entity type: ', s.entity_type_a, '. ') ELSE '' END,
                    CASE WHEN s.entity_type_b IS NOT NULL THEN CONCAT('Table B entity type: ', s.entity_type_b, '. ') ELSE '' END,
                    CASE WHEN s._card_ratio_a IS NOT NULL THEN CONCAT('Column A uniqueness ratio: ', CAST(s._card_ratio_a AS STRING), '. ') ELSE '' END,
                    CASE WHEN s._card_ratio_b IS NOT NULL THEN CONCAT('Column B uniqueness ratio: ', CAST(s._card_ratio_b AS STRING), '. ') ELSE '' END,
                    'Rule-based score: ', CAST(s.rule_score AS STRING), '. ',
                    'Respond ONLY with a JSON object: ',
                    '{{"is_fk": true/false, "confidence": 0.0-1.0, "fk_column": "a" or "b", "reasoning": "..."}}. ',
                    'confidence = probability this IS a foreign key (0.0 = definitely not, 1.0 = definitely yes). ',
                    'fk_column should be "a" if Column A is the foreign key referencing Column B, or "b" if Column B is the foreign key referencing Column A.'
                )
            ) AS ai_raw
        FROM fk_scored s
        LEFT JOIN kb kb_a ON s.col_a = kb_a.column_id
        LEFT JOIN kb kb_b ON s.col_b = kb_b.column_id
        LEFT JOIN tkb tkb_a ON s.table_a = tkb_a.table_name
        LEFT JOIN tkb tkb_b ON s.table_b = tkb_b.table_name
        WHERE s.rule_score >= {self.config.rule_score_min_for_ai}
        """
        schema = StructType(
            [
                StructField("is_fk", BooleanType()),
                StructField("confidence", DoubleType()),
                StructField("fk_column", StringType()),
                StructField("reasoning", StringType()),
            ]
        )
        df = self.spark.sql(sql)
        # Strip markdown code fences that cheaper models may wrap JSON in
        cleaned = df.withColumn(
            "ai_raw",
            F.regexp_replace(F.col("ai_raw"), r"^```(?:json)?\s*|\s*```$", ""),
        )
        parsed = cleaned.withColumn("ai_parsed", F.from_json(F.col("ai_raw"), schema))
        is_fk = F.coalesce(F.col("ai_parsed.is_fk"), F.lit(False))
        raw_conf = F.greatest(F.lit(0.0), F.least(F.lit(1.0),
            F.coalesce(F.col("ai_parsed.confidence"), F.lit(0.0))))
        return (
            parsed
            .withColumn("ai_is_fk", is_fk)
            .withColumn(
                "ai_confidence",
                F.when(F.col("ai_is_fk"), raw_conf).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "ai_reasoning",
                F.coalesce(F.col("ai_parsed.reasoning"), F.col("ai_raw")),
            )
            .withColumn("ai_fk_side", F.lower(F.col("ai_parsed.fk_column")))
            .drop("ai_raw", "ai_parsed", "comment_a", "comment_b")
        )

    # ------------------------------------------------------------------
    # Step 4b: Join validation -- sample rows and test actual joinability
    # ------------------------------------------------------------------
    def join_validate(self, judged: DataFrame) -> DataFrame:
        """Batch join validation using UNION ALL to minimize round-trips."""
        rows = (
            judged.filter(F.col("ai_confidence") > 0)
            .select("col_a", "col_b", "table_a", "table_b", "ai_confidence")
            .distinct()
            .collect()
        )

        # Pre-create all needed table sample views
        for row in rows:
            self._ensure_table_sample(row.table_a, row.col_a.split(".")[-1])
            self._ensure_table_sample(row.table_b, row.col_b.split(".")[-1])

        fragments = []
        for row in rows:
            va = self._table_samples.get((row.table_a, row.col_a.split(".")[-1]))
            vb = self._table_samples.get((row.table_b, row.col_b.split(".")[-1]))
            if not va or not vb:
                continue
            ca, cb = row.col_a.replace("'", "''"), row.col_b.replace("'", "''")
            fragments.append(
                f"SELECT '{ca}' AS ca, '{cb}' AS cb, "
                f"(SELECT COUNT(*) FROM {va}) AS a_count, "
                f"(SELECT COUNT(*) FROM {vb}) AS b_count, "
                f"(SELECT COUNT(*) FROM {va} a INNER JOIN {vb} b ON a.val = b.val) AS joined"
            )

        if not fragments:
            return judged.withColumn("join_rate", F.lit(0.0)).withColumn(
                "join_matched", F.lit(0)
            )

        BATCH = 200
        all_join_rows = []
        for i in range(0, len(fragments), BATCH):
            sql = " UNION ALL ".join(fragments[i : i + BATCH])
            all_join_rows.extend(self.spark.sql(sql).collect())

        join_stats = []
        for r in all_join_rows:
            min_count = min(r.a_count, r.b_count) or 1
            join_stats.append(
                (r.ca, r.cb, r.joined / min_count, r.a_count, r.b_count, r.joined)
            )

        stats_df = self.spark.createDataFrame(
            join_stats,
            ["_col_a", "_col_b", "join_rate", "sample_a_count", "sample_b_count", "join_matched"],
        )

        result = judged.join(
            stats_df,
            (judged.col_a == stats_df._col_a) & (judged.col_b == stats_df._col_b),
            "left",
        ).drop("_col_a", "_col_b")

        result = (
            result
            .withColumn("join_rate",
                F.least(F.lit(1.0), F.greatest(F.lit(0.0),
                    F.coalesce(F.col("join_rate"), F.lit(0.0)))))
            .withColumn("join_matched", F.coalesce(F.col("join_matched"), F.lit(0)))
            .withColumn(
                "ai_confidence",
                F.round(F.col("ai_confidence") * (0.6 + 0.4 * F.col("join_rate")), 4),
            )
        )
        return result

    # ------------------------------------------------------------------
    # Step 4c: Enforce FK/PK direction (src=FK child, dst=PK parent)
    # ------------------------------------------------------------------
    @staticmethod
    def _enforce_direction(df: DataFrame) -> DataFrame:
        """Swap col_a/col_b so col_a is always the FK (child) side.

        Uses AI judgment (ai_fk_side) as primary signal. Falls back to
        cardinality ratios: the column with lower uniqueness (more duplicates)
        is more likely the FK side.
        """
        has_ai = "ai_fk_side" in df.columns
        has_card = "_card_ratio_a" in df.columns and "_card_ratio_b" in df.columns

        if not has_ai and not has_card:
            return df

        # should_swap = True means col_b is the FK, so we need to swap a<->b
        if has_ai and has_card:
            should_swap = F.when(
                F.col("ai_fk_side") == F.lit("b"), F.lit(True)
            ).when(
                F.col("ai_fk_side") == F.lit("a"), F.lit(False)
            ).otherwise(
                # Fallback: lower uniqueness = more duplicates = FK side
                F.col("_card_ratio_b") < F.col("_card_ratio_a")
            )
        elif has_ai:
            should_swap = F.col("ai_fk_side") == F.lit("b")
        else:
            should_swap = F.col("_card_ratio_b") < F.col("_card_ratio_a")

        swap_pairs = [
            ("col_a", "col_b"), ("table_a", "table_b"),
            ("dtype_a", "dtype_b"),
        ]
        optional_pairs = [
            ("entity_type_a", "entity_type_b"),
            ("_card_ratio_a", "_card_ratio_b"),
        ]
        # Write to temp columns first, then rename. F.col() is a lazy
        # reference by name, so chaining .withColumn("col_a", ...).withColumn(
        # "col_b", F.col("col_a")) would read the ALREADY-MODIFIED col_a,
        # corrupting both sides to the same value.
        for ca, cb in swap_pairs + [p for p in optional_pairs if p[0] in df.columns]:
            orig_a, orig_b = F.col(ca), F.col(cb)
            ta, tb = f"_swap_{ca}", f"_swap_{cb}"
            df = (
                df
                .withColumn(ta, F.when(should_swap, orig_b).otherwise(orig_a))
                .withColumn(tb, F.when(should_swap, orig_a).otherwise(orig_b))
                .drop(ca, cb)
                .withColumnRenamed(ta, ca)
                .withColumnRenamed(tb, cb)
            )

        return df

    # ------------------------------------------------------------------
    # Step 5: Write predictions
    # ------------------------------------------------------------------
    def write_predictions(self, df: DataFrame) -> int:
        """Write FK predictions to output table via MERGE (preserves existing predictions)."""
        target = self.config.fq(self.config.predictions_table)

        try:
            self.spark.sql(f"""
                INSERT OVERWRITE {target}
                SELECT
                    src_column, dst_column, src_table, dst_table,
                    col_similarity, table_similarity, rule_score,
                    LEAST(1.0, GREATEST(0.0, ai_confidence)) as ai_confidence,
                    ai_reasoning, join_rate, join_matched, pk_uniqueness, ri_score,
                    LEAST(1.0, GREATEST(0.0, final_confidence)) as final_confidence,
                    created_at, updated_at, is_fk
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY src_column, dst_column
                        ORDER BY updated_at DESC, final_confidence DESC
                    ) as _rn
                    FROM {target}
                ) WHERE _rn = 1
                  AND src_table != dst_table
                  AND src_column != dst_column
            """)
        except AnalysisException:
            pass

        pk_uniq = F.coalesce(F.col("pk_uniqueness"), F.lit(0.5))
        ri = F.coalesce(F.col("ri_score"), F.lit(0.5))
        capped_join = F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("join_rate")))

        out = df.select(
            F.col("col_a").alias("src_column"),
            F.col("col_b").alias("dst_column"),
            F.col("table_a").alias("src_table"),
            F.col("table_b").alias("dst_table"),
            "col_similarity",
            "table_similarity",
            "rule_score",
            F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.col("ai_confidence"))).alias("ai_confidence"),
            "ai_reasoning",
            F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("join_rate"))).alias("join_rate"),
            F.col("join_matched").cast("int").alias("join_matched"),
            pk_uniq.alias("pk_uniqueness"),
            ri.alias("ri_score"),
            F.greatest(F.lit(0.0), F.least(F.lit(1.0),
                F.col("col_similarity") * 0.15
                + F.col("rule_score") * 0.15
                + F.col("ai_confidence") * 0.25
                + capped_join * 0.15
                + pk_uniq * 0.15
                + ri * 0.15
            )).alias("final_confidence"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            F.col("ai_is_fk").alias("is_fk"),
        ).filter(F.col("ai_confidence") >= self.config.confidence_threshold)

        # Safety net: never write same-table or same-column predictions
        out = out.filter(
            (F.col("src_table") != F.col("dst_table"))
            & (F.col("src_column") != F.col("dst_column"))
        )

        w = Window.partitionBy("src_column", "dst_column").orderBy(F.col("final_confidence").desc())
        out = out.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        count = out.count()
        if count == 0:
            logger.info("No predictions to write")
            return 0

        logger.info("Writing %d FK predictions to %s", count, target)
        staging_view = "_fk_predictions_staging"
        out.createOrReplaceTempView(staging_view)
        # Remove old rows whose direction was reversed (from pre-direction-enforcement runs)
        try:
            self.spark.sql(f"""
                DELETE FROM {target} WHERE EXISTS (
                    SELECT 1 FROM {staging_view} s
                    WHERE s.src_column = {target}.dst_column
                      AND s.dst_column = {target}.src_column
                )
            """)
        except Exception:
            pass
        self.spark.sql(f"""
            MERGE INTO {target} AS t
            USING {staging_view} AS s
            ON t.src_column = s.src_column AND t.dst_column = s.dst_column
            WHEN MATCHED THEN UPDATE SET
                src_table = s.src_table, dst_table = s.dst_table,
                col_similarity = s.col_similarity, table_similarity = s.table_similarity,
                rule_score = s.rule_score, ai_confidence = s.ai_confidence,
                ai_reasoning = s.ai_reasoning, join_rate = s.join_rate,
                join_matched = s.join_matched, pk_uniqueness = s.pk_uniqueness,
                ri_score = s.ri_score, final_confidence = s.final_confidence,
                updated_at = s.updated_at, is_fk = s.is_fk
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info("Merged %d FK predictions", count)
        return count

    # ------------------------------------------------------------------
    # Step 6: Graph edges
    # ------------------------------------------------------------------
    def write_graph_edges(self, df: DataFrame) -> int:
        """Insert predicted_fk edges into graph_edges."""
        edges_table = self.config.fq(self.config.edges_table)
        # Remove old predicted_fk edges
        self.spark.sql(
            f"DELETE FROM {edges_table} WHERE relationship = '{self.RELATIONSHIP_TYPE}'"
        )

        high_conf = df.filter(
            F.col("ai_confidence") >= self.config.confidence_threshold
        )
        # Dedup by (col_a, col_b) keeping highest confidence
        w = Window.partitionBy("col_a", "col_b").orderBy(F.col("ai_confidence").desc())
        high_conf = high_conf.withColumn("_rn", F.row_number().over(w)) \
            .filter(F.col("_rn") == 1).drop("_rn")
        edges = high_conf.select(
            F.col("col_a").alias("src"),
            F.col("col_b").alias("dst"),
            F.lit(self.RELATIONSHIP_TYPE).alias("relationship"),
            F.col("ai_confidence").alias("weight"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        count = edges.count()
        if count > 0:
            edges.write.mode("append").saveAsTable(edges_table)
        logger.info("Inserted %d predicted_fk edges", count)
        return count

    # ------------------------------------------------------------------
    # Step 7: DDL generation
    # ------------------------------------------------------------------
    def generate_ddl(self, df: DataFrame) -> DataFrame:
        """Generate ALTER TABLE ADD CONSTRAINT FK statements."""
        high_conf = df.filter(
            F.col("ai_confidence") >= self.config.confidence_threshold
        )
        ddl = high_conf.withColumn(
            "ddl_statement",
            F.concat(
                F.lit("ALTER TABLE "),
                F.col("table_a"),
                F.lit(" ADD CONSTRAINT fk_"),
                F.regexp_replace(F.col("col_a"), "[^a-zA-Z0-9]", "_"),
                F.lit("_"),
                F.regexp_replace(F.col("col_b"), "[^a-zA-Z0-9]", "_"),
                F.lit(" FOREIGN KEY (`"),
                F.element_at(F.split(F.col("col_a"), "\\."), -1),
                F.lit("`) REFERENCES "),
                F.col("table_b"),
                F.lit(" (`"),
                F.element_at(F.split(F.col("col_b"), "\\."), -1),
                F.lit("`);"),
            ),
        )
        target = self.config.fq("fk_ddl_statements")
        ddl_out = ddl.select(
            F.col("col_a").alias("src_column"),
            F.col("col_b").alias("dst_column"),
            "ddl_statement",
            F.col("ai_confidence").alias("confidence"),
            F.current_timestamp().alias("created_at"),
        )
        ddl_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            target
        )
        return ddl_out

    def apply_ddl(self, ddl_df: DataFrame) -> int:
        """Execute the generated DDL statements."""
        stmts = ddl_df.select("ddl_statement").collect()
        applied = 0
        for row in stmts:
            try:
                self.spark.sql(row.ddl_statement)
                applied += 1
            except Exception as e:
                logger.warning("Failed to apply DDL: %s -- %s", row.ddl_statement, e)
        logger.info("Applied %d/%d FK constraints", applied, len(stmts))
        return applied

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def _ensure_output_tables(self) -> None:
        """Create output tables if they don't exist yet."""
        preds = self.config.fq(self.config.predictions_table)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {preds} (
                src_column STRING, dst_column STRING,
                src_table STRING, dst_table STRING,
                col_similarity DOUBLE, table_similarity DOUBLE,
                rule_score DOUBLE, ai_confidence DOUBLE, ai_reasoning STRING,
                join_rate DOUBLE, join_matched INT,
                pk_uniqueness DOUBLE, ri_score DOUBLE,
                final_confidence DOUBLE, created_at TIMESTAMP,
                updated_at TIMESTAMP, is_fk BOOLEAN
            ) COMMENT 'Predicted foreign key relationships'
        """)
        for col_def in ["updated_at TIMESTAMP", "is_fk BOOLEAN"]:
            try:
                self.spark.sql(f"ALTER TABLE {preds} ADD COLUMNS ({col_def})")
                logger.info("Added column %s to %s", col_def, preds)
            except Exception as e:
                if "FIELDS_ALREADY_EXISTS" not in str(e) and "already exists" not in str(e).lower():
                    raise

        # Idempotent cleanup: remove bad rows every run.
        cleanup_rules = [
            ("self-referential", "src_table = dst_table OR src_column = dst_column"),
            ("stale legacy (is_fk NULL)", "is_fk IS NULL AND ai_confidence >= 1.0"),
        ]
        try:
            for label, where in cleanup_rules:
                n = self.spark.sql(f"SELECT COUNT(*) AS n FROM {preds} WHERE {where}").collect()[0].n
                if n > 0:
                    self.spark.sql(f"DELETE FROM {preds} WHERE {where}")
                    logger.warning("FK cleanup: removed %d %s rows from %s", n, label, preds)
        except AnalysisException as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "DELTA_TABLE_NOT_FOUND" in str(e):
                logger.debug("FK cleanup skipped (table empty or new): %s", e)
            else:
                logger.error("FK cleanup failed: %s", e)
                raise

        ddl = self.config.fq("fk_ddl_statements")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ddl} (
                src_column STRING, dst_column STRING,
                ddl_statement STRING, confidence DOUBLE,
                created_at TIMESTAMP
            ) COMMENT 'Generated FK DDL statements'
        """)

    def run(self) -> Dict[str, Any]:
        """Execute the full FK prediction pipeline."""
        logger.info("Starting FK prediction pipeline")
        self._ensure_output_tables()

        embedding_cands = self.get_candidates()
        name_cands = self.get_name_based_candidates()
        ontology_cands = self.get_ontology_relationship_candidates()
        declared_cands = self.get_declared_fk_candidates()
        query_cands = self.get_query_join_candidates()

        candidates = (
            embedding_cands.unionByName(name_cands, allowMissingColumns=True)
            .unionByName(ontology_cands, allowMissingColumns=True)
            .unionByName(declared_cands, allowMissingColumns=True)
            .unionByName(query_cands, allowMissingColumns=True)
        )

        candidates = candidates.filter(F.col("table_a") != F.col("table_b"))

        dedup_w = Window.partitionBy("col_a", "col_b").orderBy(
            F.col("source_rank").asc(),
            F.col("col_similarity").desc(),
            F.col("query_hit_count").desc(),
        )
        candidates = (
            candidates.withColumn("_rn", F.row_number().over(dedup_w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        candidates = candidates.fillna(
            0.0, subset=["col_similarity", "table_similarity", "query_hit_count"]
        )

        total_cand = candidates.count()
        if total_cand == 0:
            logger.info("No FK candidates found")
            return {"candidates": 0, "ai_query_rows": 0, "predictions": 0, "edges": 0, "ddl_applied": 0}

        try:
            candidates = self._sample_from_source(candidates)
        except Exception as e:
            logger.warning("Value sampling failed, using null samples: %s", e)
            candidates = self.sample_values(candidates)

        candidates = self.add_entity_match(candidates)
        candidates = self.add_lineage_signal(candidates)
        candidates = self.rule_score(candidates)

        try:
            candidates = self.cardinality_analysis(candidates)
        except Exception as e:
            logger.warning("Cardinality analysis failed, continuing: %s", e)
            candidates = (
                candidates.withColumn("pk_uniqueness", F.lit(0.5))
                .withColumn("_card_ratio_a", F.lit(0.5))
                .withColumn("_card_ratio_b", F.lit(0.5))
            )

        candidates = self._with_skip_ai_flags(candidates)
        min_rule = self.config.rule_score_min_for_ai
        candidates = self._apply_tiered_ai_cap(candidates)

        need_llm = candidates.filter((~F.col("skip_ai")) & (F.col("rule_score") >= min_rule))
        skip_llm = candidates.filter(F.col("skip_ai"))

        ai_eligible = need_llm.count()
        if self.config.dry_run:
            logger.info(
                "DRY RUN: %d candidates, %d would be sent to AI_QUERY, %d heuristic",
                total_cand,
                ai_eligible,
                skip_llm.count(),
            )
            return {
                "dry_run": True,
                "candidates": total_cand,
                "ai_query_rows": ai_eligible,
                "predictions": 0,
                "edges": 0,
                "ddl_applied": 0,
            }

        judged_ai = self.ai_judge(need_llm)
        judged_skip = self._heuristic_ai_fill(skip_llm)
        judged = judged_ai.unionByName(judged_skip, allowMissingColumns=True)

        ai_positive = judged.filter(F.col("ai_is_fk") == True).count()
        ai_negative = judged.filter(F.col("ai_is_fk") == False).count()
        logger.info("AI judgment: %d positive, %d negative", ai_positive, ai_negative)

        judged = self.join_validate(judged)

        try:
            judged = self.referential_integrity(judged)
        except Exception as e:
            logger.warning("RI check failed, continuing: %s", e)
            judged = judged.withColumn("ri_score", F.lit(0.5))

        judged = self._enforce_direction(judged)

        n_preds = self.write_predictions(judged)
        n_edges = self.write_graph_edges(judged)
        ddl_df = self.generate_ddl(judged)
        n_ddl = 0
        if self.config.apply_ddl:
            n_ddl = self.apply_ddl(ddl_df)

        return {
            "dry_run": False,
            "candidates": total_cand,
            "ai_query_rows": ai_eligible,
            "predictions": n_preds,
            "edges": n_edges,
            "ddl_applied": n_ddl,
        }



def predict_foreign_keys(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    column_similarity_threshold: float = 0.85,
    table_similarity_threshold: float = 0.9,
    duplicate_table_similarity_threshold: float = 0.97,
    cross_block_column_similarity_min: float = 0.92,
    cross_block_strict: bool = True,
    ontology_cross_block: bool = False,
    skip_ai_for_declared_fk: bool = False,
    skip_ai_query_min_observations: int = 0,
    confidence_threshold: float = 0.7,
    sample_size: int = 50,
    apply_ddl: bool = False,
    dry_run: bool = False,
    incremental: bool = True,
    max_ai_candidates: int = 200,
    rule_score_min_for_ai: float = 0.50,
    max_candidates_per_table_pair: int = 5,
) -> Dict[str, Any]:
    """Convenience function to run FK prediction."""
    config = FKPredictionConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        column_similarity_threshold=column_similarity_threshold,
        table_similarity_threshold=table_similarity_threshold,
        duplicate_table_similarity_threshold=duplicate_table_similarity_threshold,
        cross_block_column_similarity_min=cross_block_column_similarity_min,
        cross_block_strict=cross_block_strict,
        ontology_cross_block=ontology_cross_block,
        skip_ai_for_declared_fk=skip_ai_for_declared_fk,
        skip_ai_query_min_observations=skip_ai_query_min_observations,
        confidence_threshold=confidence_threshold,
        sample_size=sample_size,
        apply_ddl=apply_ddl,
        dry_run=dry_run,
        incremental=incremental,
        max_ai_candidates=max_ai_candidates,
        rule_score_min_for_ai=rule_score_min_for_ai,
        max_candidates_per_table_pair=max_candidates_per_table_pair,
    )
    predictor = FKPredictor(spark, config)
    return predictor.run()


"""
Foreign Key Prediction module.

Uses column embedding similarity, table similarity filtering, sample value
comparison, rule-based scoring, and AI judgment to predict FK relationships
between columns across tables.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


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
    table_similarity_threshold: float = 0.9  # max table similarity (exclude near-duplicate tables)
    sample_size: int = 5
    confidence_threshold: float = 0.7
    model_endpoint: str = "databricks-gpt-oss-120b"
    apply_ddl: bool = False
    dry_run: bool = False
    ontology_match_bonus_weight: float = 0.15
    rule_score_min_for_ai: float = 0.65
    incremental: bool = True
    max_candidates_per_table_pair: int = 10
    cardinality_sample_rows: int = 100000
    max_ai_candidates: int = 500

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"


class FKPredictor:
    RELATIONSHIP_TYPE = "predicted_fk"

    def __init__(self, spark: SparkSession, config: FKPredictionConfig):
        self.spark = spark
        self.config = config
        self._table_samples: dict = {}

    def _cap_candidates(self, df: DataFrame) -> DataFrame:
        """Limit candidates to top-K per table pair by name match quality."""
        k = self.config.max_candidates_per_table_pair
        w = Window.partitionBy("table_a", "table_b").orderBy(
            F.col("col_similarity").desc()
        )
        return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") <= k).drop("_rn")

    # ------------------------------------------------------------------
    # Step 1: Candidate selection
    # ------------------------------------------------------------------
    def get_candidates(self) -> DataFrame:
        """Column pairs with high column similarity but low table similarity.
        When incremental, only considers pairs where at least one table changed."""
        nodes = self.config.fq(self.config.nodes_table)
        edges = self.config.fq(self.config.edges_table)
        preds = self.config.fq(self.config.predictions_table)
        col_thresh = self.config.column_similarity_threshold
        tbl_thresh = self.config.table_similarity_threshold

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
          AND (wp.table_a IN (SELECT parent_id FROM changed_tables)
               OR wp.table_b IN (SELECT parent_id FROM changed_tables))"""
                logger.info(f"Incremental mode: filtering candidates to tables changed since {last_run}")
            except Exception as e:
                logger.warning(f"Incremental filtering failed ({e}), using full candidate set")

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
        col_sim AS (
            SELECT col_a, col_b, col_similarity FROM col_sim_raw
        ),
        with_parents AS (
            SELECT cs.*,
                   n1.parent_id AS table_a, n2.parent_id AS table_b,
                   n1.data_type AS dtype_a, n2.data_type AS dtype_b
            FROM col_sim cs
            JOIN {nodes} n1 ON cs.col_a = n1.id
            JOIN {nodes} n2 ON cs.col_b = n2.id
        ),
        tbl_sim AS (
            SELECT src, dst, weight AS table_similarity
            FROM {edges}
            WHERE relationship = 'similar_embedding'
        ){changed_tables_cte}
        SELECT wp.*,
               COALESCE(ts.table_similarity, 0) AS table_similarity
        FROM with_parents wp
        LEFT JOIN tbl_sim ts
            ON (wp.table_a = ts.src AND wp.table_b = ts.dst)
            OR (wp.table_a = ts.dst AND wp.table_b = ts.src)
        WHERE COALESCE(ts.table_similarity, 0) < {tbl_thresh}{changed_tables_filter}
        """
        df = self.spark.sql(sql)
        logger.info("FK candidates found: %d", df.count())
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
            logger.info("Name-based FK candidates: %d", df.count())
            return df
        except Exception as e:
            logger.warning("Name-based candidate generation failed: %s", e)
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, col_similarity DOUBLE, "
                "table_a STRING, table_b STRING, dtype_a STRING, dtype_b STRING, "
                "table_similarity DOUBLE"
            )

    # ------------------------------------------------------------------
    # Step 1b: Ontology-driven FK candidates
    # ------------------------------------------------------------------
    def get_ontology_relationship_candidates(self) -> DataFrame:
        """Generate FK candidates from ontology_relationships entity pairs."""
        ont_rels = self.config.fq(self.config.ontology_relationships_table)
        ont_ent = self.config.fq(self.config.ontology_entities_table)
        nodes = self.config.fq(self.config.nodes_table)
        try:
            self.spark.sql(f"SELECT 1 FROM {ont_rels} LIMIT 1").collect()
        except Exception:
            logger.info("ontology_relationships table not available, skipping")
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, col_similarity DOUBLE, "
                "table_a STRING, table_b STRING, dtype_a STRING, dtype_b STRING, "
                "table_similarity DOUBLE"
            )

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
        ),
        cols AS (
            SELECT id, parent_id, data_type,
                   LOWER(ELEMENT_AT(SPLIT(id, '\\\\.'), -1)) AS col_short
            FROM {nodes} WHERE node_type = 'column'
        ),
        id_like_cols AS (
            SELECT * FROM cols WHERE col_short RLIKE '(_id|_key|_code)$' OR col_short = 'id'
        ),
        candidate_pairs AS (
            SELECT ca.id AS col_a, cb.id AS col_b,
                   tp.table_a, tp.table_b,
                   ca.data_type AS dtype_a, cb.data_type AS dtype_b,
                   0.0 AS col_similarity, 0.0 AS table_similarity
            FROM table_pairs tp
            JOIN id_like_cols ca ON ca.parent_id = tp.table_a
            JOIN id_like_cols cb ON cb.parent_id = tp.table_b
            WHERE ca.id != cb.id
              AND (
                  ca.col_short = cb.col_short
                  OR REPLACE(REPLACE(ca.col_short, '_id', ''), '_key', '')
                     = REGEXP_REPLACE(LOWER(ELEMENT_AT(SPLIT(tp.table_b, '\\\\.'), -1)), 's$', '')
                  OR REPLACE(REPLACE(cb.col_short, '_id', ''), '_key', '')
                     = REGEXP_REPLACE(LOWER(ELEMENT_AT(SPLIT(tp.table_a, '\\\\.'), -1)), 's$', '')
              )
        )
        SELECT LEAST(col_a, col_b) AS col_a, GREATEST(col_a, col_b) AS col_b,
               col_similarity, table_a, table_b, dtype_a, dtype_b, table_similarity
        FROM candidate_pairs
        """
        try:
            df = self.spark.sql(sql)
            df = self._cap_candidates(df)
            logger.info("Ontology-driven FK candidates: %d", df.count())
            return df
        except Exception as e:
            logger.warning("Ontology-driven candidate generation failed: %s", e)
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, col_similarity DOUBLE, "
                "table_a STRING, table_b STRING, dtype_a STRING, dtype_b STRING, "
                "table_similarity DOUBLE"
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
                "dtype_a STRING, dtype_b STRING, col_similarity DOUBLE, table_similarity DOUBLE"
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
                col_a = f"{src_table}.{fk_col}"
                col_b = f"{ref_table}.{ref_col}"
                pairs.append((col_a, col_b, src_table, ref_table, "", "", 1.0, 0.0))
        if not pairs:
            return self.spark.createDataFrame(
                [], "col_a STRING, col_b STRING, table_a STRING, table_b STRING, "
                "dtype_a STRING, dtype_b STRING, col_similarity DOUBLE, table_similarity DOUBLE"
            )
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("col_a", StringType()), StructField("col_b", StringType()),
            StructField("table_a", StringType()), StructField("table_b", StringType()),
            StructField("dtype_a", StringType()), StructField("dtype_b", StringType()),
            StructField("col_similarity", DoubleType()), StructField("table_similarity", DoubleType()),
        ])
        return self.spark.createDataFrame(pairs, schema)

    # ------------------------------------------------------------------
    # Step 1c: Ontology entity match (optional)
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
            rel_case = "CASE WHEN r1.src_entity_type IS NOT NULL OR r2.src_entity_type IS NOT NULL THEN 0.25 ELSE 0.0 END"

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
                            CASE WHEN ta.entity_type = tb.entity_type THEN 0.1 ELSE 0.0 END
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
                return 0.2
            if up_a & up_b:
                return 0.15
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
        """Try to sample actual values from source tables for each column pair."""
        rows = (
            candidates.select("col_a", "col_b", "table_a", "table_b")
            .distinct()
            .collect()
        )
        sampled = []
        n = self.config.sample_size

        for row in rows:
            for col_id, tbl_id in [(row.col_a, row.table_a), (row.col_b, row.table_b)]:
                col_short = col_id.split(".")[-1]
                try:
                    vals = self.spark.sql(
                        f"SELECT DISTINCT CAST(`{col_short}` AS STRING) AS v "
                        f"FROM {tbl_id} WHERE `{col_short}` IS NOT NULL LIMIT {n}"
                    ).collect()
                    sampled.append((col_id, [r.v for r in vals]))
                except Exception:
                    sampled.append((col_id, []))

        sample_map = dict(sampled)
        # Broadcast as UDF
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
            stats.append((r.col_a, r.col_b, max(a_ratio, b_ratio)))

        if not stats:
            return candidates.withColumn("pk_uniqueness", F.lit(0.5))

        card_df = self.spark.createDataFrame(stats, ["_ca", "_cb", "pk_uniqueness"])
        result = candidates.join(
            card_df,
            (candidates.col_a == card_df._ca) & (candidates.col_b == card_df._cb),
            "left",
        ).drop("_ca", "_cb")
        return result.withColumn("pk_uniqueness", F.coalesce(F.col("pk_uniqueness"), F.lit(0.5)))

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
        """Batch RI check using pre-sampled temp views for each (table, column)."""
        rows = (
            candidates.filter(F.col("rule_score") >= self.config.rule_score_min_for_ai)
            .select("col_a", "col_b", "table_a", "table_b")
            .distinct()
            .collect()
        )
        stats = []
        for row in rows:
            col_a_short = row.col_a.split(".")[-1]
            col_b_short = row.col_b.split(".")[-1]
            try:
                va = self._ensure_table_sample(row.table_a, col_a_short)
                vb = self._ensure_table_sample(row.table_b, col_b_short)
                orphan_ab = self.spark.sql(
                    f"SELECT COUNT(DISTINCT a.val) AS orphans "
                    f"FROM {va} a LEFT ANTI JOIN {vb} b ON a.val = b.val"
                ).collect()[0].orphans
                total_a = self.spark.sql(f"SELECT COUNT(DISTINCT val) AS cnt FROM {va}").collect()[0].cnt
                ri_ab = max(0.0, 1.0 - orphan_ab / max(total_a, 1))
                orphan_ba = self.spark.sql(
                    f"SELECT COUNT(DISTINCT b.val) AS orphans "
                    f"FROM {vb} b LEFT ANTI JOIN {va} a ON b.val = a.val"
                ).collect()[0].orphans
                total_b = self.spark.sql(f"SELECT COUNT(DISTINCT val) AS cnt FROM {vb}").collect()[0].cnt
                ri_ba = max(0.0, 1.0 - orphan_ba / max(total_b, 1))
                stats.append((row.col_a, row.col_b, max(ri_ab, ri_ba)))
            except Exception as e:
                logger.debug("RI check failed for %s<->%s: %s", row.col_a, row.col_b, e)
                stats.append((row.col_a, row.col_b, 0.5))

        if not stats:
            return candidates.withColumn("ri_score", F.lit(0.5))

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
            0.2,
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
            0.25,
        ).otherwise(0.0)

        # fk_ or ref_ prefix stripping
        fk_prefix = F.when(
            col_a_short.rlike("^(fk_|ref_)") | col_b_short.rlike("^(fk_|ref_)"),
            0.1,
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
        model = self.config.model_endpoint

        sql = f"""
        WITH kb_dedup AS (
            SELECT column_id, comment,
                   ROW_NUMBER() OVER (PARTITION BY column_id ORDER BY column_id) AS _kb_rn
            FROM {col_kb}
        ),
        kb AS (
            SELECT column_id, comment FROM kb_dedup WHERE _kb_rn = 1
        )
        SELECT s.*,
            kb_a.comment AS comment_a, kb_b.comment AS comment_b,
            AI_QUERY(
                '{model}',
                CONCAT(
                    'Assess whether these two columns likely form a foreign key relationship. ',
                    'Column A: ', s.col_a, ' (type: ', COALESCE(s.dtype_a, 'unknown'), ')',
                    CASE WHEN kb_a.comment IS NOT NULL THEN CONCAT(' — description: ', kb_a.comment) ELSE '' END, '. ',
                    'Column B: ', s.col_b, ' (type: ', COALESCE(s.dtype_b, 'unknown'), ')',
                    CASE WHEN kb_b.comment IS NOT NULL THEN CONCAT(' — description: ', kb_b.comment) ELSE '' END, '. ',
                    'Embedding similarity: ', CAST(s.col_similarity AS STRING), '. ',
                    'Tables: ', s.table_a, ' and ', s.table_b, '. ',
                    'Table similarity: ', CAST(s.table_similarity AS STRING), '. ',
                    CASE WHEN s.entity_type_a IS NOT NULL THEN CONCAT('Table A entity type: ', s.entity_type_a, '. ') ELSE '' END,
                    CASE WHEN s.entity_type_b IS NOT NULL THEN CONCAT('Table B entity type: ', s.entity_type_b, '. ') ELSE '' END,
                    'Rule-based score: ', CAST(s.rule_score AS STRING), '. ',
                    'Respond ONLY with a JSON object: {{"is_fk": true/false, "confidence": 0.0-1.0, "reasoning": "..."}}'
                )
            ) AS ai_raw
        FROM fk_scored s
        LEFT JOIN kb kb_a ON s.col_a = kb_a.column_id
        LEFT JOIN kb kb_b ON s.col_b = kb_b.column_id
        WHERE s.rule_score >= {self.config.rule_score_min_for_ai}
        """
        schema = StructType(
            [
                StructField("is_fk", BooleanType()),
                StructField("confidence", DoubleType()),
                StructField("reasoning", StringType()),
            ]
        )
        df = self.spark.sql(sql)
        parsed = df.withColumn("ai_parsed", F.from_json(F.col("ai_raw"), schema))
        return (
            parsed.withColumn(
                "ai_confidence",
                F.greatest(F.lit(0.0), F.least(F.lit(1.0),
                    F.coalesce(F.col("ai_parsed.confidence"), F.lit(0.0)))),
            )
            .withColumn(
                "ai_reasoning",
                F.coalesce(F.col("ai_parsed.reasoning"), F.col("ai_raw")),
            )
            .withColumn("ai_is_fk", F.coalesce(F.col("ai_parsed.is_fk"), F.lit(False)))
            .drop("ai_raw", "ai_parsed", "comment_a", "comment_b")
        )

    # ------------------------------------------------------------------
    # Step 4b: Join validation -- sample rows and test actual joinability
    # ------------------------------------------------------------------
    def join_validate(self, judged: DataFrame) -> DataFrame:
        """Batch join validation using cached table samples."""
        rows = (
            judged.filter(F.col("ai_confidence") > 0)
            .select("col_a", "col_b", "table_a", "table_b", "ai_confidence")
            .distinct()
            .collect()
        )

        join_stats = []
        for row in rows:
            col_a_short = row.col_a.split(".")[-1]
            col_b_short = row.col_b.split(".")[-1]
            try:
                va = self._ensure_table_sample(row.table_a, col_a_short)
                vb = self._ensure_table_sample(row.table_b, col_b_short)
                a_count = self.spark.sql(f"SELECT COUNT(*) AS c FROM {va}").collect()[0].c
                b_count = self.spark.sql(f"SELECT COUNT(*) AS c FROM {vb}").collect()[0].c
                joined = self.spark.sql(
                    f"SELECT COUNT(*) AS c FROM {va} a INNER JOIN {vb} b ON a.val = b.val"
                ).collect()[0].c
                min_count = min(a_count, b_count) or 1
                join_stats.append(
                    (row.col_a, row.col_b, joined / min_count, a_count, b_count, joined)
                )
            except Exception as e:
                logger.warning("Join validation failed for %s <-> %s: %s", row.col_a, row.col_b, e)
                join_stats.append((row.col_a, row.col_b, 0.0, 0, 0, 0))

        if not join_stats:
            return judged.withColumn("join_rate", F.lit(0.0)).withColumn(
                "join_matched", F.lit(0)
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
            result.withColumn("join_rate", F.coalesce(F.col("join_rate"), F.lit(0.0)))
            .withColumn("join_matched", F.coalesce(F.col("join_matched"), F.lit(0)))
            .withColumn(
                "ai_confidence",
                F.round(F.col("ai_confidence") * (0.4 + 0.6 * F.col("join_rate")), 4),
            )
        )
        return result

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
                    created_at, updated_at
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY src_column, dst_column
                        ORDER BY updated_at DESC, final_confidence DESC
                    ) as _rn
                    FROM {target}
                ) WHERE _rn = 1
            """)
        except AnalysisException:
            pass

        pk_uniq = F.coalesce(F.col("pk_uniqueness"), F.lit(0.5))
        ri = F.coalesce(F.col("ri_score"), F.lit(0.5))

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
            "join_rate",
            F.col("join_matched").cast("int").alias("join_matched"),
            pk_uniq.alias("pk_uniqueness"),
            ri.alias("ri_score"),
            F.greatest(F.lit(0.0), F.least(F.lit(1.0),
                F.col("col_similarity") * 0.15
                + F.col("rule_score") * 0.15
                + F.col("ai_confidence") * 0.25
                + F.col("join_rate") * 0.15
                + pk_uniq * 0.15
                + ri * 0.15
            )).alias("final_confidence"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ).filter(F.col("ai_confidence") >= self.config.confidence_threshold)

        w = Window.partitionBy("src_column", "dst_column").orderBy(F.col("final_confidence").desc())
        out = out.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        count = out.count()
        if count == 0:
            logger.info("No predictions to write")
            return 0

        staging_view = "_fk_predictions_staging"
        out.createOrReplaceTempView(staging_view)
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
                updated_at = s.updated_at
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
                F.lit(" FOREIGN KEY ("),
                F.element_at(F.split(F.col("col_a"), "\\."), -1),
                F.lit(") REFERENCES "),
                F.col("table_b"),
                F.lit(" ("),
                F.element_at(F.split(F.col("col_b"), "\\."), -1),
                F.lit(");"),
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
                updated_at TIMESTAMP
            ) COMMENT 'Predicted foreign key relationships'
        """)
        try:
            self.spark.sql(f"ALTER TABLE {preds} ADD COLUMNS (updated_at TIMESTAMP)")
        except Exception as e:
            if "FIELDS_ALREADY_EXISTS" not in str(e) and "already exists" not in str(e).lower():
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

        # Gather candidates from all sources and deduplicate
        embedding_cands = self.get_candidates()
        name_cands = self.get_name_based_candidates()
        ontology_cands = self.get_ontology_relationship_candidates()
        declared_cands = self.get_declared_fk_candidates()

        candidates = embedding_cands \
            .unionByName(name_cands, allowMissingColumns=True) \
            .unionByName(ontology_cands, allowMissingColumns=True) \
            .unionByName(declared_cands, allowMissingColumns=True)

        # Dedup: keep highest col_similarity per (col_a, col_b) pair
        dedup_w = Window.partitionBy("col_a", "col_b").orderBy(F.col("col_similarity").desc())
        candidates = candidates.withColumn("_rn", F.row_number().over(dedup_w)) \
            .filter(F.col("_rn") == 1).drop("_rn")
        # Fill nulls from non-embedding sources
        candidates = candidates.fillna(0.0, subset=["col_similarity", "table_similarity"])

        if candidates.count() == 0:
            logger.info("No FK candidates found")
            return {"candidates": 0, "ai_query_rows": 0, "predictions": 0, "edges": 0, "ddl_applied": 0}

        # Try to sample actual values; fall back to null samples
        try:
            candidates = self._sample_from_source(candidates)
        except Exception as e:
            logger.warning("Value sampling failed, using null samples: %s", e)
            candidates = self.sample_values(candidates)

        candidates = self.add_entity_match(candidates)
        candidates = self.add_lineage_signal(candidates)
        candidates = self.rule_score(candidates)

        # Cardinality analysis
        try:
            candidates = self.cardinality_analysis(candidates)
        except Exception as e:
            logger.warning("Cardinality analysis failed, continuing: %s", e)
            candidates = candidates.withColumn("pk_uniqueness", F.lit(0.5))

        min_rule = self.config.rule_score_min_for_ai
        ai_eligible = candidates.filter(F.col("rule_score") >= min_rule).count()

        # Cap AI candidates to control cost
        max_ai = self.config.max_ai_candidates
        if ai_eligible > max_ai:
            logger.info("Capping AI candidates from %d to %d (top by rule_score)", ai_eligible, max_ai)
            w = Window.orderBy(F.col("rule_score").desc())
            candidates = candidates.withColumn("_ai_rn", F.row_number().over(w))
            below_gate = candidates.filter(F.col("rule_score") < min_rule)
            above_gate = candidates.filter(
                (F.col("rule_score") >= min_rule) & (F.col("_ai_rn") <= max_ai)
            )
            candidates = above_gate.unionByName(below_gate).drop("_ai_rn")
            ai_eligible = max_ai

        if self.config.dry_run:
            total = candidates.count()
            logger.info(
                "DRY RUN: %d candidates, %d would be sent to AI_QUERY",
                total,
                ai_eligible,
            )
            return {
                "dry_run": True,
                "candidates": total,
                "ai_query_rows": ai_eligible,
                "predictions": 0,
                "edges": 0,
                "ddl_applied": 0,
            }

        # AI judgment
        judged = self.ai_judge(candidates)

        # Join validation -- test actual joinability
        judged = self.join_validate(judged)

        # Referential integrity check on top candidates
        try:
            judged = self.referential_integrity(judged)
        except Exception as e:
            logger.warning("RI check failed, continuing: %s", e)
            judged = judged.withColumn("ri_score", F.lit(0.5))

        # Write outputs
        n_preds = self.write_predictions(judged)
        n_edges = self.write_graph_edges(judged)
        ddl_df = self.generate_ddl(judged)
        n_ddl = 0
        if self.config.apply_ddl:
            n_ddl = self.apply_ddl(ddl_df)

        return {
            "dry_run": False,
            "candidates": candidates.count(),
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
    confidence_threshold: float = 0.7,
    sample_size: int = 5,
    model_endpoint: str = "databricks-gpt-oss-120b",
    apply_ddl: bool = False,
    dry_run: bool = False,
    incremental: bool = True,
) -> Dict[str, Any]:
    """Convenience function to run FK prediction."""
    config = FKPredictionConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        column_similarity_threshold=column_similarity_threshold,
        table_similarity_threshold=table_similarity_threshold,
        confidence_threshold=confidence_threshold,
        sample_size=sample_size,
        model_endpoint=model_endpoint,
        apply_ddl=apply_ddl,
        dry_run=dry_run,
        incremental=incremental,
    )
    predictor = FKPredictor(spark, config)
    return predictor.run()

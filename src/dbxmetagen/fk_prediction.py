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

logger = logging.getLogger(__name__)


@dataclass
class FKPredictionConfig:
    catalog_name: str
    schema_name: str
    nodes_table: str = "graph_nodes"
    edges_table: str = "graph_edges"
    column_kb_table: str = "column_knowledge_base"
    predictions_table: str = "fk_predictions"
    column_similarity_threshold: float = 0.75
    table_similarity_threshold: float = 0.75
    sample_size: int = 5
    confidence_threshold: float = 0.7
    model_endpoint: str = "databricks-gpt-oss-120b"
    apply_ddl: bool = False
    dry_run: bool = True

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"


class FKPredictor:
    RELATIONSHIP_TYPE = "predicted_fk"

    def __init__(self, spark: SparkSession, config: FKPredictionConfig):
        self.spark = spark
        self.config = config

    # ------------------------------------------------------------------
    # Step 1: Candidate selection
    # ------------------------------------------------------------------
    def get_candidates(self) -> DataFrame:
        """Column pairs with high column similarity but low table similarity."""
        nodes = self.config.fq(self.config.nodes_table)
        edges = self.config.fq(self.config.edges_table)
        col_thresh = self.config.column_similarity_threshold
        tbl_thresh = self.config.table_similarity_threshold

        sql = f"""
        WITH col_sim AS (
            SELECT e.src AS col_a, e.dst AS col_b, e.weight AS col_similarity
            FROM {edges} e
            JOIN {nodes} n1 ON e.src = n1.id AND n1.node_type = 'column'
            JOIN {nodes} n2 ON e.dst = n2.id AND n2.node_type = 'column'
            WHERE e.relationship = 'similar_embedding'
              AND e.weight >= {col_thresh}
              AND n1.parent_id != n2.parent_id
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
        )
        SELECT wp.*,
               COALESCE(ts.table_similarity, 0) AS table_similarity
        FROM with_parents wp
        LEFT JOIN tbl_sim ts
            ON (wp.table_a = ts.src AND wp.table_b = ts.dst)
            OR (wp.table_a = ts.dst AND wp.table_b = ts.src)
        WHERE COALESCE(ts.table_similarity, 0) < {tbl_thresh}
        """
        df = self.spark.sql(sql)
        logger.info("FK candidates found: %d", df.count())
        return df

    # ------------------------------------------------------------------
    # Step 2: Sample values
    # ------------------------------------------------------------------
    def sample_values(self, candidates: DataFrame) -> DataFrame:
        """For each candidate pair, sample distinct non-null values from each column."""
        candidates.createOrReplaceTempView("fk_candidates")
        n = self.config.sample_size

        # Extract column short names from the column id (format: catalog.schema.table.column)
        # Fallback: carry forward dtype and rely on AI + rules
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
    # Step 3: Rule-based scoring
    # ------------------------------------------------------------------
    def rule_score(self, candidates: DataFrame) -> DataFrame:
        """Compute a heuristic joinability score."""
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

        # Value overlap (when samples available)
        overlap = F.when(
            F.col("samples_a").isNotNull() & F.col("samples_b").isNotNull(),
            F.size(F.array_intersect("samples_a", "samples_b"))
            / F.greatest(F.size("samples_a"), F.lit(1)).cast("double"),
        ).otherwise(F.lit(0.0))

        return candidates.withColumn(
            "rule_score",
            F.round(
                F.col("col_similarity") * 0.4
                + dtype_score * 0.25
                + id_pattern * 0.15
                + overlap * 0.2,
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
        model = self.config.model_endpoint

        sql = f"""
        SELECT *,
            AI_QUERY(
                '{model}',
                CONCAT(
                    'Assess whether these two columns likely form a foreign key relationship. ',
                    'Column A: ', col_a, ' (type: ', COALESCE(dtype_a, 'unknown'), '). ',
                    'Column B: ', col_b, ' (type: ', COALESCE(dtype_b, 'unknown'), '). ',
                    'Embedding similarity: ', CAST(col_similarity AS STRING), '. ',
                    'Tables: ', table_a, ' and ', table_b, '. ',
                    'Table similarity: ', CAST(table_similarity AS STRING), '. ',
                    'Rule-based score: ', CAST(rule_score AS STRING), '. ',
                    'Respond ONLY with a JSON object: {{"is_fk": true/false, "confidence": 0.0-1.0, "reasoning": "..."}}'
                )
            ) AS ai_raw
        FROM fk_scored
        WHERE rule_score >= 0.3
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
                "ai_confidence", F.coalesce(F.col("ai_parsed.confidence"), F.lit(0.0))
            )
            .withColumn(
                "ai_reasoning",
                F.coalesce(F.col("ai_parsed.reasoning"), F.col("ai_raw")),
            )
            .withColumn("ai_is_fk", F.coalesce(F.col("ai_parsed.is_fk"), F.lit(False)))
            .drop("ai_raw", "ai_parsed")
        )

    # ------------------------------------------------------------------
    # Step 4b: Join validation -- sample rows and test actual joinability
    # ------------------------------------------------------------------
    JOIN_SAMPLE_SIZE = 10000

    def join_validate(self, judged: DataFrame) -> DataFrame:
        """For each predicted FK pair, sample rows from both tables and test
        the actual join rate. Adjusts confidence based on statistical fit."""
        rows = (
            judged.filter(F.col("ai_confidence") > 0)
            .select("col_a", "col_b", "table_a", "table_b", "ai_confidence")
            .distinct()
            .collect()
        )

        join_stats = []
        n = self.JOIN_SAMPLE_SIZE
        for row in rows:
            col_a_short = row.col_a.split(".")[-1]
            col_b_short = row.col_b.split(".")[-1]
            try:
                sample_a = self.spark.sql(
                    f"SELECT CAST(`{col_a_short}` AS STRING) AS val "
                    f"FROM {row.table_a} TABLESAMPLE ({n} ROWS)"
                )
                sample_b = self.spark.sql(
                    f"SELECT CAST(`{col_b_short}` AS STRING) AS val "
                    f"FROM {row.table_b} TABLESAMPLE ({n} ROWS)"
                )
                a_count = sample_a.count()
                b_count = sample_b.count()
                joined = sample_a.join(sample_b, "val", "inner").count()
                # Join rate relative to the smaller sample
                min_count = min(a_count, b_count) or 1
                join_rate = joined / min_count
                join_stats.append(
                    (row.col_a, row.col_b, join_rate, a_count, b_count, joined)
                )
            except Exception as e:
                logger.warning(
                    "Join validation failed for %s <-> %s: %s", row.col_a, row.col_b, e
                )
                join_stats.append((row.col_a, row.col_b, 0.0, 0, 0, 0))

        if not join_stats:
            return judged.withColumn("join_rate", F.lit(0.0)).withColumn(
                "join_matched", F.lit(0)
            )

        stats_df = self.spark.createDataFrame(
            join_stats,
            [
                "_col_a",
                "_col_b",
                "join_rate",
                "sample_a_count",
                "sample_b_count",
                "join_matched",
            ],
        )

        result = judged.join(
            stats_df,
            (judged.col_a == stats_df._col_a) & (judged.col_b == stats_df._col_b),
            "left",
        ).drop("_col_a", "_col_b")

        # Adjust confidence: blend AI confidence with join rate evidence
        # If join_rate is near 0, it's strong evidence against FK
        # If join_rate is high (>0.5), it confirms the FK
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
        """Write FK predictions to output table."""
        target = self.config.fq(self.config.predictions_table)
        self.spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS {target} (
            src_column STRING, dst_column STRING,
            src_table STRING, dst_table STRING,
            col_similarity DOUBLE, table_similarity DOUBLE,
            rule_score DOUBLE, ai_confidence DOUBLE, ai_reasoning STRING,
            join_rate DOUBLE, join_matched INT,
            final_confidence DOUBLE, created_at TIMESTAMP
        ) COMMENT 'Predicted foreign key relationships'
        """
        )

        out = df.select(
            F.col("col_a").alias("src_column"),
            F.col("col_b").alias("dst_column"),
            F.col("table_a").alias("src_table"),
            F.col("table_b").alias("dst_table"),
            "col_similarity",
            "table_similarity",
            "rule_score",
            "ai_confidence",
            "ai_reasoning",
            "join_rate",
            F.col("join_matched").cast("int").alias("join_matched"),
            (
                F.col("rule_score") * 0.3
                + F.col("ai_confidence") * 0.4
                + F.col("join_rate") * 0.3
            ).alias("final_confidence"),
            F.current_timestamp().alias("created_at"),
        ).filter(F.col("ai_confidence") >= self.config.confidence_threshold)

        count = out.count()
        out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            target
        )
        logger.info("Wrote %d FK predictions", count)
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
    def run(self) -> Dict[str, Any]:
        """Execute the full FK prediction pipeline."""
        logger.info("Starting FK prediction pipeline")

        candidates = self.get_candidates()
        if candidates.count() == 0:
            logger.info("No FK candidates found")
            return {"candidates": 0, "predictions": 0, "edges": 0, "ddl_applied": 0}

        # Try to sample actual values; fall back to null samples
        try:
            candidates = self._sample_from_source(candidates)
        except Exception as e:
            logger.warning("Value sampling failed, using null samples: %s", e)
            candidates = self.sample_values(candidates)

        candidates = self.rule_score(candidates)

        # Count rows that would be sent to AI_QUERY (rule_score >= 0.3)
        ai_eligible = candidates.filter(F.col("rule_score") >= 0.3).count()

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
    column_similarity_threshold: float = 0.75,
    table_similarity_threshold: float = 0.75,
    confidence_threshold: float = 0.7,
    sample_size: int = 5,
    model_endpoint: str = "databricks-gpt-oss-120b",
    apply_ddl: bool = False,
    dry_run: bool = True,
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
    )
    predictor = FKPredictor(spark, config)
    return predictor.run()

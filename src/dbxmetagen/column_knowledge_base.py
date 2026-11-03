"""
Column Knowledge Base ETL module.

Transforms row-based metadata_generation_log into column-centric column_knowledge_base
with aggregated column metadata from comment and PI classification runs.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, Any, List
from pyspark.sql import SparkSession

from dbxmetagen.table_filter import table_filter_sql

_MERGE_MAX_RETRIES = 3
_MERGE_BACKOFF_SECONDS = [5, 15, 45]

logger = logging.getLogger(__name__)


@dataclass
class ColumnKnowledgeBaseConfig:
    """Configuration for column knowledge base ETL."""
    catalog_name: str
    schema_name: str
    source_table: str = "metadata_generation_log"
    target_table: str = "column_knowledge_base"
    table_names: list[str] | None = None

    @property
    def fully_qualified_source(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.source_table}"
    
    @property
    def fully_qualified_target(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.target_table}"


class ColumnKnowledgeBaseBuilder:
    """
    Builder class for transforming metadata_generation_log into column_knowledge_base.
    
    Extracts column-level metadata including comments, PI classifications, and data types.
    """
    
    def __init__(self, spark: SparkSession, config: ColumnKnowledgeBaseConfig):
        self.spark = spark
        self.config = config
    
    def create_target_table(self) -> None:
        """Create the target table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_target} (
            column_id STRING NOT NULL,
            table_name STRING,
            catalog STRING,
            `schema` STRING,
            table_short_name STRING,
            column_name STRING,
            comment STRING,
            data_type STRING,
            classification STRING,
            classification_type STRING,
            confidence FLOAT,
            nullable BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            review_updated_at TIMESTAMP
        )
        CLUSTER BY (catalog, `schema`, table_short_name)
        COMMENT 'Aggregated column-level metadata from dbxmetagen runs'
        """
        self.spark.sql(ddl)
        logger.info(f"Target table {self.config.fully_qualified_target} ready")
    
    def _source_filter(self) -> str:
        """Corpus filter clause shared by the pairs query and the MERGE source."""
        return table_filter_sql(self.config.table_names or [], column="`table`")

    def _corpus_catalog_schema_pairs(self) -> List[tuple]:
        """Distinct lowercased ``(catalog, schema)`` pairs present in the corpus.

        Collected to Python for ONE reason: so the information_schema predicate
        can inline them as LITERALS. A join/subquery against
        ``system.information_schema.columns`` does not reliably push down, and
        that system view has a large fixed per-query cost -- inlined literal
        predicates are what let the metastore prune without a scan. This is a
        single cheap query over the (already corpus-filtered) log; it does NOT
        touch information_schema.
        """
        rows = self.spark.sql(f"""
            SELECT DISTINCT
                SPLIT(LOWER(`table`), '[.]')[0] AS cat,
                SPLIT(LOWER(`table`), '[.]')[1] AS sch
            FROM {self.config.fully_qualified_source}
            WHERE `table` IS NOT NULL
              AND column_name IS NOT NULL
              {self._source_filter()}
              AND ddl_type = 'column'
        """).collect()
        return [(r["cat"], r["sch"]) for r in rows if r["cat"] and r["sch"]]

    def _info_schema_predicate(self, pairs: List[tuple]) -> str:
        r"""Pushdown-friendly WHERE predicate selecting only the corpus schemas.

        One ``table_catalog = '..' AND table_schema IN (..)`` group per catalog,
        OR-ed together. Predicate columns stay BARE against lowercase literals:
        UC normalizes identifiers to lowercase on storage and the pairs are
        derived from ``LOWER(\`table\`)``, so a bare predicate both matches and
        pushes down to the metastore. Wrapping a predicate column in ``LOWER()``
        would defeat pushdown (measured 10x slower). Empty corpus -> ``1=0`` so
        the types CTE is empty rather than an unbounded scan.
        """
        if not pairs:
            return "1=0"
        by_catalog: Dict[str, List[str]] = {}
        for cat, sch in pairs:
            by_catalog.setdefault(cat, []).append(sch)
        groups = []
        for cat, schemas in by_catalog.items():
            cat_esc = cat.replace("'", "''")
            in_list = ", ".join(
                "'" + s.replace("'", "''") + "'" for s in sorted(set(schemas))
            )
            groups.append(f"(table_catalog = '{cat_esc}' AND table_schema IN ({in_list}))")
        return " OR ".join(groups)

    def build_merge_sql(self, pairs: List[tuple]) -> str:
        r"""Build the ENTIRE column-KB build as a single declarative MERGE.

        The operation is a pivot of ``metadata_generation_log`` into one row per
        column -- latest ``comment``, latest ``pi`` classification, first/last
        seen timestamps -- left-joined to ``data_type``/``nullable`` from
        information_schema, upserted on ``column_id``. Expressed as one
        ``MERGE INTO ... USING (SELECT ...)`` the optimizer plans and runs it
        exactly once: no temp table, no ``count()``-triggered recompute, no lazy
        DataFrame DAG re-executed by both the count and the merge. The only
        irreducible cost is a single read of ``system.information_schema.columns``
        (a slow metastore-wide view); everything else is kilobytes of log data.

        Steward-win semantics are preserved: when ``target.review_updated_at``
        is newer than the source, the reviewed ``comment``/``classification``/
        ``classification_type``/``confidence`` are frozen.
        """
        info_pred = self._info_schema_predicate(pairs)
        return f"""
        MERGE INTO {self.config.fully_qualified_target} AS target
        USING (
            WITH src AS (
                SELECT
                    LOWER(`table`) AS table_name,
                    column_name,
                    metadata_type,
                    column_content,
                    classification,
                    `type` AS classification_type,
                    confidence,
                    _created_at
                FROM {self.config.fully_qualified_source}
                WHERE `table` IS NOT NULL
                  AND column_name IS NOT NULL
                  {self._source_filter()}
                  AND ddl_type = 'column'
            ),
            cols AS (
                SELECT table_name, column_name,
                       MIN(_created_at) AS created_at,
                       MAX(_created_at) AS updated_at
                FROM src
                GROUP BY table_name, column_name
            ),
            comments AS (
                SELECT table_name, column_name, comment FROM (
                    SELECT table_name, column_name,
                           column_content AS comment,
                           ROW_NUMBER() OVER (
                               PARTITION BY table_name, column_name
                               ORDER BY _created_at DESC
                           ) AS rn
                    FROM src
                    WHERE metadata_type = 'comment'
                ) WHERE rn = 1
            ),
            pi AS (
                SELECT table_name, column_name, classification, classification_type, confidence FROM (
                    SELECT table_name, column_name,
                           classification, classification_type, confidence,
                           ROW_NUMBER() OVER (
                               PARTITION BY table_name, column_name
                               ORDER BY _created_at DESC
                           ) AS rn
                    FROM src
                    WHERE metadata_type = 'pi'
                ) WHERE rn = 1
            ),
            types AS (
                SELECT
                    LOWER(CONCAT(table_catalog, '.', table_schema, '.', table_name)) AS table_name,
                    column_name,
                    data_type,
                    CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable
                FROM system.information_schema.columns
                WHERE {info_pred}
            )
            SELECT
                CONCAT_WS('.', cols.table_name, cols.column_name) AS column_id,
                cols.table_name,
                SPLIT(cols.table_name, '[.]')[0] AS catalog,
                CASE WHEN SIZE(SPLIT(cols.table_name, '[.]')) >= 2
                     THEN SPLIT(cols.table_name, '[.]')[1] END AS `schema`,
                CASE WHEN SIZE(SPLIT(cols.table_name, '[.]')) >= 3
                     THEN SPLIT(cols.table_name, '[.]')[2]
                     ELSE cols.table_name END AS table_short_name,
                cols.column_name,
                comments.comment,
                types.data_type,
                pi.classification,
                pi.classification_type,
                pi.confidence,
                types.nullable,
                cols.created_at,
                cols.updated_at
            FROM cols
            LEFT JOIN comments USING (table_name, column_name)
            LEFT JOIN pi USING (table_name, column_name)
            LEFT JOIN types USING (table_name, column_name)
        ) AS source
        ON target.column_id = source.column_id

        WHEN MATCHED THEN UPDATE SET
            target.table_name = COALESCE(source.table_name, target.table_name),
            target.catalog = COALESCE(source.catalog, target.catalog),
            target.`schema` = COALESCE(source.`schema`, target.`schema`),
            target.table_short_name = COALESCE(source.table_short_name, target.table_short_name),
            target.column_name = COALESCE(source.column_name, target.column_name),
            target.comment = CASE
                WHEN target.review_updated_at IS NOT NULL AND target.review_updated_at > source.updated_at
                THEN target.comment ELSE COALESCE(source.comment, target.comment) END,
            target.data_type = COALESCE(source.data_type, target.data_type),
            target.classification = CASE
                WHEN target.review_updated_at IS NOT NULL AND target.review_updated_at > source.updated_at
                THEN target.classification ELSE COALESCE(source.classification, target.classification) END,
            target.classification_type = CASE
                WHEN target.review_updated_at IS NOT NULL AND target.review_updated_at > source.updated_at
                THEN target.classification_type ELSE COALESCE(source.classification_type, target.classification_type) END,
            target.confidence = CASE
                WHEN target.review_updated_at IS NOT NULL AND target.review_updated_at > source.updated_at
                THEN target.confidence ELSE COALESCE(source.confidence, target.confidence) END,
            target.nullable = COALESCE(source.nullable, target.nullable),
            target.updated_at = GREATEST(source.updated_at, target.updated_at)

        WHEN NOT MATCHED THEN INSERT (
            column_id, table_name, catalog, `schema`, table_short_name,
            column_name, comment, data_type, classification, classification_type,
            confidence, nullable, created_at, updated_at
        ) VALUES (
            source.column_id, source.table_name, source.catalog, source.`schema`,
            source.table_short_name, source.column_name, source.comment, source.data_type,
            source.classification, source.classification_type, source.confidence,
            source.nullable, source.created_at, source.updated_at
        )
        """

    def run(self) -> Dict[str, Any]:
        """Execute the column KB build as ONE declarative MERGE.

        First principles: this is a pivot of a tiny log (kilobytes) into one row
        per column, enriched by a single information_schema lookup, upserted on
        column_id. So it is a single ``MERGE INTO target USING (SELECT ...)`` --
        no temp table, no ``count()`` that forces a recompute, no lazy DAG run
        twice. The earlier temp-table/materialize machinery existed only to work
        around re-executing an imperative multi-step DataFrame pipeline; a single
        statement makes "run exactly once" a property of the query plan instead
        of something we engineer.
        """
        logger.info(f"Starting column knowledge base build: {self.config.fully_qualified_target}")

        self.create_target_table()

        pairs = self._corpus_catalog_schema_pairs()
        merge_sql = self.build_merge_sql(pairs)

        for attempt in range(_MERGE_MAX_RETRIES + 1):
            try:
                self.spark.sql(merge_sql)
                break
            except Exception as e:
                err = str(e)
                if attempt < _MERGE_MAX_RETRIES and ("Concurrent" in err or "DELTA_CONCURRENT" in err):
                    wait = _MERGE_BACKOFF_SECONDS[attempt]
                    logger.warning("MERGE conflict (attempt %d), retrying in %ds: %s", attempt + 1, wait, err[:200])
                    time.sleep(wait)
                else:
                    raise

        # staged_count = distinct corpus columns this run produced. Cheap log-only
        # query (no information_schema); kept for the run-summary metric.
        staged_count = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT DISTINCT LOWER(`table`) AS t, column_name
                FROM {self.config.fully_qualified_source}
                WHERE `table` IS NOT NULL
                  AND column_name IS NOT NULL
                  {self._source_filter()}
                  AND ddl_type = 'column'
            )
        """).collect()[0]["cnt"]
        logger.info(f"Staged {staged_count} column records for merge")

        total_records = self.spark.sql(
            f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_target}"
        ).collect()[0]["cnt"]

        logger.info(f"Column knowledge base build complete. Total records: {total_records}")

        return {"staged_count": staged_count, "total_records": total_records}

    def bootstrap(self, table_names: List[str]) -> int:
        """Populate column KB from information_schema with zero LLM calls.

        Inserts only rows that don't already exist (MERGE WHEN NOT MATCHED).
        Includes data_type, nullable, and existing UC column comments.
        """
        self.create_target_table()
        by_catalog_schema: Dict[str, List[str]] = {}
        for fqtn in table_names:
            parts = fqtn.split(".")
            if len(parts) != 3:
                continue
            key = f"{parts[0]}.{parts[1]}"
            by_catalog_schema.setdefault(key, []).append(parts[2])

        all_dfs = []
        for cs_key, short_names in by_catalog_schema.items():
            cat, sch = cs_key.split(".", 1)
            in_clause = ", ".join(f"'{t}'" for t in short_names)
            try:
                df = self.spark.sql(f"""
                    SELECT
                        CONCAT(table_catalog, '.', table_schema, '.', table_name, '.', column_name) AS column_id,
                        CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_name,
                        table_catalog AS catalog,
                        table_schema AS `schema`,
                        table_name AS table_short_name,
                        column_name,
                        comment,
                        data_type,
                        CAST(NULL AS STRING) AS classification,
                        CAST(NULL AS STRING) AS classification_type,
                        CAST(NULL AS FLOAT) AS confidence,
                        CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable,
                        current_timestamp() AS created_at,
                        current_timestamp() AS updated_at,
                        CAST(NULL AS TIMESTAMP) AS review_updated_at
                    FROM system.information_schema.columns
                    WHERE table_catalog = '{cat}' AND table_schema = '{sch}'
                      AND table_name IN ({in_clause})
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning("Bootstrap: could not query %s.information_schema.columns: %s", cat, e)

        if not all_dfs:
            return 0

        from functools import reduce
        combined = reduce(lambda a, b: a.union(b), all_dfs)
        combined.createOrReplaceTempView("_ckb_bootstrap_src")

        # MERGE: `WHEN NOT MATCHED THEN INSERT *` into column KB from `_ckb_bootstrap_src` on `column_id`, filling from `information_schema.columns` plus UC comments, types, nullable, null AI fields.
        # WHY: Fast column skeleton for KB consumers before LLM passes; complements `merge_to_target` without resetting enriched rows for keys that already exist.
        # TRADEOFFS: No upsert on bootstrap (insert-only); `INSERT *` needs matching schemas; orphaned column_ids remain until separately cleaned.
        self.spark.sql(f"""
            MERGE INTO {self.config.fully_qualified_target} AS target
            USING _ckb_bootstrap_src AS source
            ON target.column_id = source.column_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        count = combined.count()
        logger.info("Bootstrap: merged %d column rows into %s", count, self.config.fully_qualified_target)
        return count

    def log_bootstrap_to_generation_log(self, table_names: List[str]) -> int:
        """Write existing UC column comments to metadata_generation_log.

        Creates rows matching the schema that ``mode=comment`` would produce,
        with ``metadata_type='comment'`` and ``ddl_type='column'``.
        One row per column per table.
        """
        from dbxmetagen.processing import ensure_log_table

        log_table = f"{self.config.catalog_name}.{self.config.schema_name}.metadata_generation_log"
        ensure_log_table(self.spark, self.config.catalog_name, self.config.schema_name)

        by_catalog_schema: Dict[str, List[str]] = {}
        for fqtn in table_names:
            parts = fqtn.split(".")
            if len(parts) != 3:
                continue
            key = f"{parts[0]}.{parts[1]}"
            by_catalog_schema.setdefault(key, []).append(parts[2])

        all_dfs = []
        for cs_key, short_names in by_catalog_schema.items():
            cat, sch = cs_key.split(".", 1)
            in_clause = ", ".join(f"'{t}'" for t in short_names)
            try:
                df = self.spark.sql(f"""
                    SELECT
                        'comment' AS metadata_type,
                        CONCAT(table_catalog, '.', table_schema, '.', table_name) AS `table`,
                        CAST(NULL AS STRING) AS tokenized_table,
                        'column' AS ddl_type,
                        column_name,
                        current_timestamp() AS _created_at,
                        COALESCE(comment, '') AS column_content,
                        CAST(NULL AS STRING) AS classification,
                        CAST(NULL AS STRING) AS type,
                        CAST(NULL AS DOUBLE) AS confidence,
                        CAST(NULL AS STRING) AS presidio_results,
                        CAST(NULL AS STRING) AS domain,
                        CAST(NULL AS STRING) AS subdomain,
                        CAST(NULL AS STRING) AS recommended_domain,
                        CAST(NULL AS STRING) AS recommended_subdomain,
                        CAST(NULL AS STRING) AS reasoning,
                        CAST(NULL AS STRING) AS metadata_summary,
                        table_catalog AS catalog,
                        table_schema AS `schema`,
                        table_name,
                        CAST(NULL AS STRING) AS ddl,
                        CAST(NULL AS STRING) AS current_user,
                        'import' AS model,
                        CAST(NULL AS INT) AS sample_size,
                        CAST(NULL AS INT) AS max_tokens,
                        CAST(NULL AS DOUBLE) AS temperature,
                        CAST(NULL AS INT) AS columns_per_call,
                        'bootstrap_import' AS status
                    FROM system.information_schema.columns
                    WHERE table_catalog = '{cat}' AND table_schema = '{sch}'
                      AND table_name IN ({in_clause})
                """)
                all_dfs.append(df)
            except Exception as e:
                logger.warning("log_bootstrap: could not query %s.information_schema.columns: %s", cat, e)

        if not all_dfs:
            return 0

        from functools import reduce
        combined = reduce(lambda a, b: a.union(b), all_dfs)
        combined.write.mode("append").option("mergeSchema", "true").saveAsTable(log_table)
        count = combined.count()
        logger.info("log_bootstrap: wrote %d column-level rows to %s", count, log_table)
        return count


def build_column_knowledge_base(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_names: list[str] | None = None,
) -> Dict[str, Any]:
    """
    Convenience function to build the column knowledge base.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for source and target tables
        schema_name: Schema name for source and target tables
        
    Returns:
        Dict with execution statistics
    """
    config = ColumnKnowledgeBaseConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_names=table_names,
    )
    builder = ColumnKnowledgeBaseBuilder(spark, config)
    return builder.run()


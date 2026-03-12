"""Build a Databricks Vector Search index over enriched metadata documents.

Creates a `metadata_documents` Delta table by joining table, column, entity,
metric-view, and FK-relationship metadata, then provisions a VS endpoint and
Delta Sync index with managed embeddings for similarity / hybrid retrieval.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass
class VectorIndexConfig:
    catalog_name: str
    schema_name: str
    endpoint_name: str = "dbxmetagen-vs"
    index_suffix: str = "metadata_vs_index"
    documents_table: str = "metadata_documents"
    embedding_model: str = "databricks-gte-large-en"

    @property
    def fq_documents(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.documents_table}"

    @property
    def fq_index(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.index_suffix}"

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"


_COLUMNS_TO_SYNC = [
    "doc_id", "doc_type", "content", "catalog_name", "schema_name",
    "table_name", "domain", "subdomain", "entity_type",
    "has_pii", "has_phi", "security_level", "data_type",
    "confidence_score",
]


class VectorIndexBuilder:
    def __init__(self, spark: SparkSession, config: VectorIndexConfig):
        self.spark = spark
        self.config = config

    def build_documents_table(self) -> int:
        cfg = self.config
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg.fq_documents} (
                doc_id STRING NOT NULL,
                doc_type STRING,
                content STRING,
                catalog_name STRING,
                schema_name STRING,
                table_name STRING,
                domain STRING,
                subdomain STRING,
                entity_type STRING,
                has_pii BOOLEAN,
                has_phi BOOLEAN,
                security_level STRING,
                data_type STRING,
                confidence_score FLOAT,
                updated_at TIMESTAMP
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

        table_docs = f"""
            SELECT
                CONCAT('table::', t.table_name) AS doc_id,
                'table' AS doc_type,
                CONCAT(
                    COALESCE(t.comment, t.table_short_name), '\\n',
                    'Domain: ', COALESCE(t.domain, 'unknown'),
                    CASE WHEN t.subdomain IS NOT NULL THEN CONCAT(' / ', t.subdomain) ELSE '' END, '\\n',
                    COALESCE(
                        CONCAT('Primary entity: ',
                            (SELECT o.entity_type
                             FROM {cfg.fq('ontology_entities')} o
                             WHERE ARRAY_CONTAINS(o.source_tables, t.table_name)
                               AND COALESCE(o.entity_role, 'primary') = 'primary'
                             LIMIT 1)),
                        ''
                    ), '\\n',
                    COALESCE(
                        CONCAT('Relationships: ',
                            (SELECT CONCAT_WS('; ', COLLECT_SET(
                                CONCAT(r.src_entity_type, ' -[', r.relationship_name, ']-> ', r.dst_entity_type)
                            ))
                             FROM {cfg.fq('ontology_relationships')} r
                             JOIN {cfg.fq('ontology_entities')} oe ON oe.entity_type = r.src_entity_type
                             WHERE ARRAY_CONTAINS(oe.source_tables, t.table_name))),
                        ''
                    ), '\\n',
                    COALESCE(
                        CONCAT('Column properties: ',
                            (SELECT CONCAT_WS(', ', COLLECT_LIST(
                                CONCAT(cp.column_name, '=', cp.property_role,
                                       CASE WHEN cp.linked_entity_type IS NOT NULL THEN CONCAT('->', cp.linked_entity_type) ELSE '' END)
                            ))
                             FROM {cfg.fq('ontology_column_properties')} cp
                             WHERE cp.table_name = t.table_name)),
                        ''
                    ), '\\n',
                    'Columns (',
                    CAST((SELECT COUNT(*) FROM {cfg.fq('column_knowledge_base')} c2 WHERE c2.table_name = t.table_name) AS STRING),
                    '): ',
                    COALESCE(
                        (SELECT CONCAT_WS(', ', COLLECT_LIST(
                            CONCAT(c.column_name, ' (', COALESCE(c.data_type, ''), ')')
                         ))
                         FROM {cfg.fq('column_knowledge_base')} c
                         WHERE c.table_name = t.table_name),
                        ''
                    ), '\\n',
                    COALESCE(
                        CONCAT('FK joins: ',
                            (SELECT CONCAT_WS('; ', COLLECT_SET(
                                CONCAT(f.src_table, '.', f.src_column, ' -> ', f.dst_table, '.', f.dst_column,
                                       ' [conf=', CAST(ROUND(f.final_confidence, 2) AS STRING), ']')
                            ))
                             FROM {cfg.fq('fk_predictions')} f
                             WHERE (f.src_table = t.table_name OR f.dst_table = t.table_name)
                               AND f.final_confidence >= 0.5)),
                        ''
                    ), '\\n',
                    'Row count: ', COALESCE(CAST(
                        (SELECT p.row_count FROM {cfg.fq('profiling_snapshots')} p
                         WHERE p.table_name = t.table_name
                         ORDER BY p.snapshot_time DESC LIMIT 1) AS STRING), 'unknown')
                ) AS content,
                t.catalog AS catalog_name,
                t.schema AS schema_name,
                t.table_name,
                t.domain,
                t.subdomain,
                CAST(NULL AS STRING) AS entity_type,
                t.has_pii,
                t.has_phi,
                CAST(NULL AS STRING) AS security_level,
                CAST(NULL AS STRING) AS data_type,
                CAST(NULL AS FLOAT) AS confidence_score,
                current_timestamp() AS updated_at
            FROM {cfg.fq('table_knowledge_base')} t
        """

        column_docs = f"""
            SELECT
                CONCAT('column::', c.table_name, '.', c.column_name) AS doc_id,
                'column' AS doc_type,
                CONCAT(
                    c.column_name, ' (', COALESCE(c.data_type, 'unknown'), ')\\n',
                    COALESCE(c.comment, ''), '\\n',
                    'Table: ', COALESCE(c.table_name, ''),
                    COALESCE(
                        CONCAT(' [domain: ',
                            (SELECT t2.domain FROM {cfg.fq('table_knowledge_base')} t2 WHERE t2.table_name = c.table_name LIMIT 1),
                        ']'), ''
                    ), '\\n',
                    CASE WHEN c.classification IS NOT NULL
                         THEN CONCAT('Classification: ', c.classification, ' (', COALESCE(c.classification_type, ''), ')\\n')
                         ELSE '' END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM {cfg.fq('fk_predictions')} fk
                        WHERE (fk.src_table = c.table_name AND fk.src_column = c.column_name)
                           OR (fk.dst_table = c.table_name AND fk.dst_column = c.column_name)
                    ) THEN 'Role: FK column' ELSE '' END
                ) AS content,
                c.catalog AS catalog_name,
                c.schema AS schema_name,
                c.table_name,
                CAST(NULL AS STRING) AS domain,
                CAST(NULL AS STRING) AS subdomain,
                CAST(NULL AS STRING) AS entity_type,
                CAST(NULL AS BOOLEAN) AS has_pii,
                CAST(NULL AS BOOLEAN) AS has_phi,
                CAST(NULL AS STRING) AS security_level,
                c.data_type,
                CAST(NULL AS FLOAT) AS confidence_score,
                current_timestamp() AS updated_at
            FROM {cfg.fq('column_knowledge_base')} c
            WHERE c.comment IS NOT NULL AND LENGTH(c.comment) > 5
        """

        entity_docs = f"""
            SELECT
                CONCAT('entity::', o.entity_id) AS doc_id,
                'entity' AS doc_type,
                CONCAT(
                    o.entity_name, ' (', o.entity_type, ') [', COALESCE(o.entity_role, 'primary'), ']\\n',
                    COALESCE(o.description, ''), '\\n',
                    'Source tables: ', COALESCE(CONCAT_WS(', ', o.source_tables), ''), '\\n',
                    COALESCE(
                        CONCAT('Named relationships: ',
                            (SELECT CONCAT_WS('; ', COLLECT_SET(
                                CONCAT(r.src_entity_type, ' -[', r.relationship_name, ']-> ', r.dst_entity_type)
                            ))
                             FROM {cfg.fq('ontology_relationships')} r
                             WHERE r.src_entity_type = o.entity_type OR r.dst_entity_type = o.entity_type)),
                        ''
                    ), '\\n',
                    COALESCE(
                        CONCAT('Column properties: ',
                            (SELECT CONCAT_WS(', ', COLLECT_LIST(
                                CONCAT(cp.column_name, '=', cp.property_role,
                                       CASE WHEN cp.linked_entity_type IS NOT NULL THEN CONCAT('->', cp.linked_entity_type) ELSE '' END)
                            ))
                             FROM {cfg.fq('ontology_column_properties')} cp
                             WHERE cp.owning_entity_id = o.entity_id)),
                        ''
                    ), '\\n',
                    'Confidence: ', CAST(o.confidence AS STRING)
                ) AS content,
                CAST(NULL AS STRING) AS catalog_name,
                CAST(NULL AS STRING) AS schema_name,
                CAST(NULL AS STRING) AS table_name,
                CAST(NULL AS STRING) AS domain,
                CAST(NULL AS STRING) AS subdomain,
                o.entity_type,
                CAST(NULL AS BOOLEAN) AS has_pii,
                CAST(NULL AS BOOLEAN) AS has_phi,
                CAST(NULL AS STRING) AS security_level,
                CAST(NULL AS STRING) AS data_type,
                CAST(o.confidence AS FLOAT) AS confidence_score,
                current_timestamp() AS updated_at
            FROM {cfg.fq('ontology_entities')} o
            WHERE o.confidence >= 0.4
        """

        metric_docs = f"""
            SELECT
                CONCAT('metric_view::', m.definition_id) AS doc_id,
                'metric_view' AS doc_type,
                CONCAT(
                    COALESCE(m.metric_view_name, ''), '\\n',
                    'Source: ', COALESCE(m.source_table, ''), '\\n',
                    'Questions: ', COALESCE(m.source_questions, ''), '\\n',
                    'Measures/Dimensions: ', SUBSTRING(COALESCE(m.json_definition, ''), 1, 2000)
                ) AS content,
                CAST(NULL AS STRING) AS catalog_name,
                CAST(NULL AS STRING) AS schema_name,
                m.source_table AS table_name,
                CAST(NULL AS STRING) AS domain,
                CAST(NULL AS STRING) AS subdomain,
                CAST(NULL AS STRING) AS entity_type,
                CAST(NULL AS BOOLEAN) AS has_pii,
                CAST(NULL AS BOOLEAN) AS has_phi,
                CAST(NULL AS STRING) AS security_level,
                CAST(NULL AS STRING) AS data_type,
                CAST(NULL AS FLOAT) AS confidence_score,
                current_timestamp() AS updated_at
            FROM {cfg.fq('metric_view_definitions')} m
            WHERE m.status IN ('validated', 'applied')
        """

        fk_docs = f"""
            SELECT
                CONCAT('fk::', f.src_table, '.', f.src_column, '->', f.dst_table, '.', f.dst_column) AS doc_id,
                'fk_relationship' AS doc_type,
                CONCAT(
                    'Foreign key: ', f.src_table, '.', f.src_column,
                    ' references ', f.dst_table, '.', f.dst_column, '\\n',
                    'Join: SELECT * FROM ', f.src_table, ' JOIN ', f.dst_table,
                    ' ON ', f.src_table, '.', f.src_column, ' = ', f.dst_table, '.', f.dst_column, '\\n',
                    'Confidence: ', CAST(ROUND(f.final_confidence, 3) AS STRING), '\\n',
                    COALESCE(CONCAT('PK Uniqueness: ', CAST(ROUND(f.pk_uniqueness, 3) AS STRING)), ''), '\\n',
                    COALESCE(CONCAT('Reasoning: ', f.ai_reasoning), '')
                ) AS content,
                CAST(NULL AS STRING) AS catalog_name,
                CAST(NULL AS STRING) AS schema_name,
                f.src_table AS table_name,
                CAST(NULL AS STRING) AS domain,
                CAST(NULL AS STRING) AS subdomain,
                CAST(NULL AS STRING) AS entity_type,
                CAST(NULL AS BOOLEAN) AS has_pii,
                CAST(NULL AS BOOLEAN) AS has_phi,
                CAST(NULL AS STRING) AS security_level,
                CAST(NULL AS STRING) AS data_type,
                CAST(f.final_confidence AS FLOAT) AS confidence_score,
                current_timestamp() AS updated_at
            FROM {cfg.fq('fk_predictions')} f
            WHERE f.final_confidence >= 0.5
        """

        union_sql = f"""
            MERGE INTO {cfg.fq_documents} AS tgt
            USING (
                {table_docs}
                UNION ALL
                {column_docs}
                UNION ALL
                {entity_docs}
                UNION ALL
                {metric_docs}
                UNION ALL
                {fk_docs}
            ) AS src
            ON tgt.doc_id = src.doc_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(union_sql)
        count = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {cfg.fq_documents}").collect()[0]["cnt"]
        logger.info("metadata_documents now has %d rows", count)
        return count

    def ensure_endpoint(self) -> str:
        w = WorkspaceClient()
        name = self.config.endpoint_name
        try:
            ep = w.vector_search_endpoints.get_endpoint(name)
            logger.info(
                "VS endpoint '%s' exists (state=%s, message=%s, num_indexes=%s)",
                name, ep.endpoint_status.state, ep.endpoint_status.message, ep.num_indexes,
            )
        except Exception:
            logger.info("Creating VS endpoint '%s'", name)
            w.vector_search_endpoints.create_endpoint(name=name, endpoint_type="STANDARD")

        ep = w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(name)
        logger.info("VS endpoint '%s' confirmed ONLINE (num_indexes=%s)", name, ep.num_indexes)
        return name

    def ensure_index(self) -> str:
        w = WorkspaceClient()
        idx_name = self.config.fq_index
        try:
            idx = w.vector_search_indexes.get_index(idx_name)
            logger.info(
                "VS index '%s' already exists (ready=%s, message=%s, indexed_rows=%s)",
                idx_name, idx.status.ready, idx.status.message, idx.status.indexed_row_count,
            )
            return idx_name
        except Exception:
            pass

        logger.info("Creating Delta Sync index '%s' with managed embeddings", idx_name)
        max_attempts, delay = 5, 30
        for attempt in range(1, max_attempts + 1):
            try:
                w.vector_search_indexes.create_index(
                    name=idx_name,
                    endpoint_name=self.config.endpoint_name,
                    primary_key="doc_id",
                    index_type=VectorIndexType.DELTA_SYNC,
                    delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                        source_table=self.config.fq_documents,
                        embedding_source_columns=[
                            EmbeddingSourceColumn(
                                name="content",
                                embedding_model_endpoint_name=self.config.embedding_model,
                            )
                        ],
                        pipeline_type=PipelineType.TRIGGERED,
                        columns_to_sync=_COLUMNS_TO_SYNC,
                    ),
                )
                logger.info("create_index succeeded on attempt %d", attempt)
                return idx_name
            except Exception as e:
                if "not ready" in str(e).lower() and attempt < max_attempts:
                    logger.warning(
                        "create_index attempt %d/%d failed: %s -- retrying in %ds",
                        attempt, max_attempts, e, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 120)
                else:
                    raise

    def sync(self):
        w = WorkspaceClient()
        logger.info("Triggering sync for '%s'", self.config.fq_index)
        w.vector_search_indexes.sync_index(index_name=self.config.fq_index)

    def run(self) -> Dict[str, Any]:
        doc_count = self.build_documents_table()
        endpoint = self.ensure_endpoint()
        index = self.ensure_index()
        self.sync()
        return {
            "documents": doc_count,
            "endpoint": endpoint,
            "index": index,
        }


def build_vector_index(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    endpoint_name: str = "dbxmetagen-vs",
) -> Dict[str, Any]:
    """Convenience entry point for the notebook."""
    config = VectorIndexConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        endpoint_name=endpoint_name,
    )
    builder = VectorIndexBuilder(spark, config)
    return builder.run()

"""Read curated example SQL + benchmarks from existing Genie spaces.

Backlog items 14/15. Runs INSIDE the Databricks App (Spark-free): the REST reads
here + the app's Statement Execution API writes to the CDF-enabled
``genie_sql_examples`` table + a Vector Search index (built via
``build_genie_examples_index``) let the user pull a space's curated SQL at
metric-view build time, on a button click -- no job, no cluster.

The typed SDK ``GenieSpace`` exposes only id/title/description/warehouse_id --
the curated SQL lives on the LEGACY data-rooms REST endpoints (confirmed live
against DMVM 2026-07):

  * GET /api/2.0/genie/spaces                       -> list (paginated)
  * GET /api/2.0/data-rooms/{id}                    -> + table_identifiers
  * GET /api/2.0/data-rooms/{id}/curated-questions  -> curated_questions[], where
      question_type in {SAMPLE_QUESTION, BENCHMARK, BENCHMARK_SUGGESTION};
      BENCHMARK* rows carry answer_text = the curated example SQL.

We keep rows with non-empty ``answer_text`` (the SQL exemplars) plus their NL
``question_text`` and the space's ``table_identifiers``, write them to
``genie_sql_examples`` (CDF on, 30d retention -- same pattern as
``metadata_documents`` / ``ontology_chunks``), and build a
``genie_examples_vs_index`` DELTA_SYNC index on the shared ``dbxmetagen-vs``
endpoint. Item 15 retrieves these exemplars to seed metric-view generation.

Ingestion is scoped to an EXPLICIT list of space ids/name-substrings so trusted
spaces are ingested and test/junk spaces are skipped.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# curated-question types whose answer_text holds curated example SQL.
_SQL_QUESTION_TYPES = {"BENCHMARK", "BENCHMARK_SUGGESTION"}

_COLUMNS_TO_SYNC = [
    "example_id", "space_id", "space_title", "question_text", "sql",
    "content", "question_type", "table_identifiers", "updated_at",
]


@dataclass
class GenieSQLPullerConfig:
    catalog_name: str
    schema_name: str
    endpoint_name: str = "dbxmetagen-vs"
    documents_table: str = "genie_sql_examples"
    index_suffix: str = "genie_examples_vs_index"
    embedding_model: str = "databricks-gte-large-en"
    # Explicit scope: space ids (exact) and/or case-insensitive title substrings.
    space_ids: List[str] = field(default_factory=list)
    title_contains: List[str] = field(default_factory=list)
    # If True, keep SAMPLE_QUESTION rows too (they have no SQL) as NL-only context.
    include_sample_questions: bool = False

    @property
    def fq_documents(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.documents_table}"

    @property
    def fq_index(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.index_suffix}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _example_id(space_id: str, question_text: str, sql: str) -> str:
    """Deterministic id so re-pulls MERGE (not duplicate) the same exemplar."""
    h = hashlib.sha1(f"{space_id}||{question_text}||{sql}".encode("utf-8")).hexdigest()[:16]
    return f"{space_id}::{h}"


class GenieSQLPuller:
    """Read curated example SQL + benchmarks from existing Genie spaces via REST.

    Spark-free: all reads go through the Databricks SDK REST client, so this runs
    inside the Databricks App (no cluster). Persistence to the CDF-enabled
    ``genie_sql_examples`` table and the VS index are done by the caller (the app)
    via the Statement Execution API + Vector Search SDK -- see the
    ``/api/semantic-layer/pull-genie-sql`` endpoint in api_server.py.
    """

    def __init__(self, config: GenieSQLPullerConfig, ws: Optional[WorkspaceClient] = None):
        self.config = config
        self.ws = ws or WorkspaceClient()

    # -- Genie REST reads -----------------------------------------------------

    def list_spaces(self) -> List[Dict[str, Any]]:
        """List all Genie spaces, following pagination."""
        out: List[Dict[str, Any]] = []
        token: Optional[str] = None
        while True:
            path = "/api/2.0/genie/spaces"
            if token:
                path += f"?page_token={token}"
            resp = self.ws.api_client.do("GET", path)
            out.extend(resp.get("spaces", []) or [])
            token = resp.get("next_page_token")
            if not token:
                break
        return out

    def _select_spaces(self) -> List[Dict[str, Any]]:
        """Filter listed spaces to the explicit ingestion scope."""
        cfg = self.config
        if not cfg.space_ids and not cfg.title_contains:
            raise ValueError(
                "GenieSQLPuller requires an explicit scope: set space_ids and/or "
                "title_contains. Refusing to pull from all spaces by default."
            )
        wanted_ids = set(cfg.space_ids)
        needles = [t.lower() for t in cfg.title_contains]
        selected = []
        for s in self.list_spaces():
            sid = s.get("space_id", "")
            title = (s.get("title") or "").lower()
            if sid in wanted_ids or any(n in title for n in needles):
                selected.append(s)
        return selected

    def _table_identifiers(self, space_id: str) -> List[str]:
        try:
            dr = self.ws.api_client.do("GET", f"/api/2.0/data-rooms/{space_id}")
            return list(dr.get("table_identifiers", []) or [])
        except Exception as e:
            logger.warning("data-rooms/%s failed: %s", space_id, e)
            return []

    def _curated_questions(self, space_id: str) -> List[Dict[str, Any]]:
        try:
            resp = self.ws.api_client.do(
                "GET", f"/api/2.0/data-rooms/{space_id}/curated-questions"
            )
            return list(resp.get("curated_questions", []) or [])
        except Exception as e:
            logger.warning("curated-questions/%s failed: %s", space_id, e)
            return []

    # -- Extraction -----------------------------------------------------------

    def extract_examples(self) -> List[Dict[str, Any]]:
        """Pull curated SQL exemplars from the in-scope spaces."""
        cfg = self.config
        rows: List[Dict[str, Any]] = []
        spaces = self._select_spaces()
        logger.info("Genie SQL pull: %d space(s) in scope", len(spaces))
        for s in spaces:
            sid = s.get("space_id", "")
            title = s.get("title", "")
            tables = self._table_identifiers(sid)
            tables_str = ", ".join(tables)
            for q in self._curated_questions(sid):
                qtype = q.get("question_type", "")
                sql = (q.get("answer_text") or "").strip()
                question = (q.get("question_text") or "").strip()
                keep_sql = qtype in _SQL_QUESTION_TYPES and sql
                keep_sample = cfg.include_sample_questions and qtype == "SAMPLE_QUESTION"
                if not (keep_sql or keep_sample):
                    continue
                # content = what the VS index embeds: NL question + SQL + tables so
                # a mart-description query retrieves semantically-matching exemplars.
                content = "\n".join(filter(None, [
                    f"Question: {question}" if question else "",
                    f"Tables: {tables_str}" if tables_str else "",
                    f"SQL:\n{sql}" if sql else "",
                ]))
                rows.append({
                    "example_id": _example_id(sid, question, sql),
                    "space_id": sid,
                    "space_title": title,
                    "question_text": question,
                    "sql": sql,
                    "content": content,
                    "question_type": qtype,
                    "table_identifiers": tables_str,
                    "updated_at": _now(),
                })
        logger.info("Genie SQL pull: extracted %d exemplar(s)", len(rows))
        return rows

    # Persistence (genie_sql_examples table) and index build are done by the app
    # via execute_sql (Statement Execution API) + build_genie_examples_index()
    # below -- NOT here, so this module stays Spark-free and app-runnable.


# --- table DDL / row SQL helpers (used by the app's execute_sql writes) -------

GENIE_SQL_EXAMPLES_COLUMNS = [
    "example_id", "space_id", "space_title", "question_text",
    "sql", "content", "question_type", "table_identifiers", "updated_at",
]


def create_table_sql(fq_documents: str) -> str:
    """DDL for the CDF-enabled genie_sql_examples table (same pattern as
    metadata_documents / ontology_chunks). Run via the app's execute_sql."""
    return (
        f"CREATE TABLE IF NOT EXISTS {fq_documents} (\n"
        "    example_id STRING NOT NULL,\n"
        "    space_id STRING,\n"
        "    space_title STRING,\n"
        "    question_text STRING,\n"
        "    sql STRING,\n"
        "    content STRING,\n"
        "    question_type STRING,\n"
        "    table_identifiers STRING,\n"
        "    updated_at TIMESTAMP\n"
        ") USING DELTA\n"
        "TBLPROPERTIES (\n"
        "    'delta.enableChangeDataFeed' = 'true',\n"
        "    'delta.deletedFileRetentionDuration' = 'interval 30 days'\n"
        ")"
    )


def build_genie_examples_index(config: GenieSQLPullerConfig) -> Dict[str, str]:
    """Provision + sync the genie_examples DELTA_SYNC index (own primary key /
    columns; mirrors OntologyVectorIndexBuilder but for genie_sql_examples)."""
    import time

    from databricks.sdk.service.vectorsearch import (
        DeltaSyncVectorIndexSpecRequest,
        EmbeddingSourceColumn,
        EndpointType,
        PipelineType,
        VectorIndexType,
    )

    w = WorkspaceClient()
    ep = config.endpoint_name
    try:
        w.vector_search_endpoints.get_endpoint(ep)
    except Exception:
        logger.info("Creating VS endpoint '%s'", ep)
        w.vector_search_endpoints.create_endpoint(name=ep, endpoint_type=EndpointType.STANDARD)
    w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(ep)

    idx_name = config.fq_index
    try:
        w.vector_search_indexes.get_index(idx_name)
        logger.info("VS index '%s' already exists", idx_name)
    except Exception:
        logger.info("Creating VS index '%s'", idx_name)
        w.vector_search_indexes.create_index(
            name=idx_name,
            endpoint_name=ep,
            primary_key="example_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=config.fq_documents,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name="content",
                        embedding_model_endpoint_name=config.embedding_model,
                    )
                ],
                pipeline_type=PipelineType.TRIGGERED,
                columns_to_sync=_COLUMNS_TO_SYNC,
            ),
        )
    # Trigger a sync (best-effort; index may still be provisioning).
    try:
        w.vector_search_indexes.sync_index(index_name=idx_name)
    except Exception as e:
        logger.info("sync_index('%s') deferred: %s", idx_name, e)
    return {"endpoint": ep, "index": idx_name}


# ---------------------------------------------------------------------------
# Retrieval (item 15 seed): find curated SQL exemplars for a target table/mart
# ---------------------------------------------------------------------------

def query_examples(
    fq_index: str,
    query_text: str,
    num_results: int = 5,
    endpoint_name: str = "dbxmetagen-vs",
) -> List[Dict[str, Any]]:
    """Retrieve top-K curated SQL exemplars matching a mart/table description.

    Returns dicts with keys: question_text, sql, space_title, table_identifiers, score.
    """
    from databricks.vector_search.client import VectorSearchClient

    vsc = VectorSearchClient()
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=fq_index)
    results = index.similarity_search(
        query_text=query_text,
        query_type="HYBRID",
        columns=["example_id", "question_text", "sql", "space_title", "table_identifiers"],
        num_results=num_results,
    )
    rows = results.get("result", {}).get("data_array", [])
    cols = [c["name"] for c in results.get("manifest", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in rows]

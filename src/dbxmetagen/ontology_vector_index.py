"""Vector Search index and query helpers for ontology-backed entity/edge retrieval.

Manages a ``ontology_vs_index`` Delta Sync index on the ``ontology_chunks`` table,
with managed embeddings and HYBRID retrieval filtered by ``ontology_bundle``.
Reuses the existing ``dbxmetagen-vs`` endpoint created by :mod:`vector_index`.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)

logger = logging.getLogger(__name__)

_COLUMNS_TO_SYNC = [
    "chunk_id", "chunk_type", "ontology_bundle", "source_ontology",
    "name", "content", "uri", "domain", "range_entity",
    "parent_entities", "keywords", "tier",
]


@dataclass
class OntologyVectorIndexConfig:
    catalog_name: str
    schema_name: str
    endpoint_name: str = "dbxmetagen-vs"
    index_suffix: str = "ontology_vs_index"
    documents_table: str = "ontology_chunks"
    embedding_model: str = "databricks-gte-large-en"
    index_ready_timeout_s: int = 900
    index_ready_initial_delay_s: int = 30
    sync_max_attempts: int = 5
    sync_initial_delay_s: int = 30

    @property
    def fq_documents(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.documents_table}"

    @property
    def fq_index(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.index_suffix}"


def _transient_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "not ready" in msg


class OntologyVectorIndexBuilder:
    """Provision and sync the ontology VS index (reuses existing VS endpoint)."""

    def __init__(self, config: OntologyVectorIndexConfig):
        self.config = config

    def ensure_endpoint(self) -> str:
        w = WorkspaceClient()
        name = self.config.endpoint_name
        try:
            w.vector_search_endpoints.get_endpoint(name)
            logger.info("VS endpoint '%s' already exists", name)
        except Exception:
            logger.info("Creating VS endpoint '%s'", name)
            w.vector_search_endpoints.create_endpoint(name=name, endpoint_type="STANDARD")
        w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(name)
        return name

    def _wait_until_ready(self, idx_name: str) -> None:
        w = WorkspaceClient()
        deadline = time.monotonic() + self.config.index_ready_timeout_s
        delay = float(self.config.index_ready_initial_delay_s)
        while True:
            idx = w.vector_search_indexes.get_index(idx_name)
            st = idx.status
            if st and st.ready:
                logger.info("VS index '%s' ready", idx_name)
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            logger.info("VS index '%s' not ready, sleeping %.0fs", idx_name, min(delay, remaining))
            time.sleep(min(delay, remaining))
            delay = min(delay * 2.0, 120.0)
        raise TimeoutError(f"VS index '{idx_name}' not ready within {self.config.index_ready_timeout_s}s")

    def ensure_index(self) -> str:
        w = WorkspaceClient()
        idx_name = self.config.fq_index
        try:
            w.vector_search_indexes.get_index(idx_name)
            logger.info("VS index '%s' already exists", idx_name)
        except Exception:
            logger.info("Creating Delta Sync index '%s'", idx_name)
            max_attempts, delay = 5, 30
            for attempt in range(1, max_attempts + 1):
                try:
                    w.vector_search_indexes.create_index(
                        name=idx_name,
                        endpoint_name=self.config.endpoint_name,
                        primary_key="chunk_id",
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
                    break
                except Exception as e:
                    if _transient_error(e) and attempt < max_attempts:
                        logger.warning("create_index attempt %d/%d: %s", attempt, max_attempts, e)
                        time.sleep(delay)
                        delay = min(delay * 2, 120)
                    else:
                        raise
        self._wait_until_ready(idx_name)
        return idx_name

    def sync(self) -> None:
        w = WorkspaceClient()
        idx_name = self.config.fq_index
        delay = float(self.config.sync_initial_delay_s)
        for attempt in range(1, self.config.sync_max_attempts + 1):
            try:
                w.vector_search_indexes.sync_index(index_name=idx_name)
                return
            except Exception as e:
                if _transient_error(e) and attempt < self.config.sync_max_attempts:
                    logger.warning("sync attempt %d: %s", attempt, e)
                    time.sleep(delay)
                    delay = min(delay * 2.0, 120.0)
                else:
                    raise

    def run(self) -> Dict[str, str]:
        ep = self.ensure_endpoint()
        idx = self.ensure_index()
        self.sync()
        return {"endpoint": ep, "index": idx}


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def query_entities(
    fq_index: str,
    table_blob: str,
    bundle: str,
    num_results: int = 8,
    endpoint_name: str = "dbxmetagen-vs",
) -> List[Dict[str, Any]]:
    """Retrieve top-K ontology entity chunks matching a table description.

    Uses HYBRID search (keyword + semantic via RRF) with bundle and type filters.
    Returns dicts with keys: name, content, uri, parent_entities, keywords, score.
    """
    from databricks.vector_search.client import VectorSearchClient

    vsc = VectorSearchClient()
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=fq_index)
    results = index.similarity_search(
        query_text=table_blob,
        query_type="HYBRID",
        columns=["chunk_id", "name", "content", "uri", "parent_entities", "keywords"],
        num_results=num_results,
        filters={
            "ontology_bundle": bundle,
            "chunk_type": "entity",
        },
    )
    rows = results.get("result", {}).get("data_array", [])
    cols = [c["name"] for c in results.get("manifest", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in rows]


def query_edges(
    fq_index: str,
    fk_blob: str,
    bundle: str,
    src_entity: Optional[str] = None,
    dst_entity: Optional[str] = None,
    num_results: int = 8,
    endpoint_name: str = "dbxmetagen-vs",
) -> List[Dict[str, Any]]:
    """Retrieve top-K ontology edge chunks matching an FK relationship description.

    Uses HYBRID search with bundle and type filters.
    """
    from databricks.vector_search.client import VectorSearchClient

    filters: Dict[str, Any] = {
        "ontology_bundle": bundle,
        "chunk_type": "edge",
    }

    vsc = VectorSearchClient()
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=fq_index)
    results = index.similarity_search(
        query_text=fk_blob,
        query_type="HYBRID",
        columns=["chunk_id", "name", "content", "uri", "domain", "range_entity"],
        num_results=num_results,
        filters=filters,
    )
    rows = results.get("result", {}).get("data_array", [])
    cols = [c["name"] for c in results.get("manifest", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in rows]

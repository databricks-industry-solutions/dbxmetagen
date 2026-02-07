"""FastAPI backend for dbxmetagen dashboard app."""

import os
import json
import logging
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Databricks client
# ---------------------------------------------------------------------------

_ws: Optional[WorkspaceClient] = None


def get_workspace_client() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


def execute_sql(query: str, warehouse_id: Optional[str] = None):
    """Execute SQL via Statement Execution API and return rows as list[dict]."""
    ws = get_workspace_client()
    wh = warehouse_id or os.environ.get("WAREHOUSE_ID", "")
    resp = ws.statement_execution.execute_statement(
        statement=query, warehouse_id=wh, wait_timeout="30s"
    )
    if resp.status and resp.status.state and resp.status.state.value == "FAILED":
        raise HTTPException(500, detail=resp.status.error.message if resp.status.error else "SQL failed")
    cols = [c.name for c in resp.manifest.schema.columns] if resp.manifest else []
    rows = []
    if resp.result and resp.result.data_array:
        for row in resp.result.data_array:
            rows.append(dict(zip(cols, row)))
    return rows


# ---------------------------------------------------------------------------
# App config
# ---------------------------------------------------------------------------

CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "metadata_results")


def fq(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("dbxmetagen API starting – catalog=%s schema=%s", CATALOG, SCHEMA)
    yield

app = FastAPI(title="dbxmetagen API", version="0.6.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class JobRunRequest(BaseModel):
    job_name: str
    table_names: str
    mode: str = "comment"
    apply_ddl: bool = False
    extra_params: dict = {}


class GraphQueryRequest(BaseModel):
    question: str
    max_hops: int = 3


# ---------------------------------------------------------------------------
# Jobs endpoints
# ---------------------------------------------------------------------------

@app.get("/api/jobs")
def list_jobs():
    """List dbxmetagen jobs in the workspace."""
    ws = get_workspace_client()
    jobs = ws.jobs.list(name="dbxmetagen")
    return [{"job_id": j.job_id, "name": j.settings.name if j.settings else ""} for j in jobs]


@app.post("/api/jobs/run")
def run_job(req: JobRunRequest):
    """Trigger a dbxmetagen job."""
    ws = get_workspace_client()
    # Find matching job
    matching = [j for j in ws.jobs.list(name=req.job_name)]
    if not matching:
        raise HTTPException(404, detail=f"Job '{req.job_name}' not found")
    job = matching[0]
    params = {
        "table_names": req.table_names,
        "mode": req.mode,
        "apply_ddl": str(req.apply_ddl).lower(),
        **req.extra_params,
    }
    run = ws.jobs.run_now(job_id=job.job_id, job_parameters=params)
    return {"run_id": run.run_id}


@app.get("/api/jobs/{run_id}/status")
def get_run_status(run_id: int):
    """Get status of a job run."""
    ws = get_workspace_client()
    run = ws.jobs.get_run(run_id=run_id)
    return {
        "run_id": run.run_id,
        "state": run.state.life_cycle_state.value if run.state else "UNKNOWN",
        "result": run.state.result_state.value if run.state and run.state.result_state else None,
    }


# ---------------------------------------------------------------------------
# Metadata endpoints
# ---------------------------------------------------------------------------

@app.get("/api/metadata/log")
def get_metadata_log(limit: int = 100, table_name: Optional[str] = None):
    """Query metadata_generation_log."""
    where = f"WHERE table_name LIKE '%{table_name}%'" if table_name else ""
    q = f"SELECT * FROM {fq('metadata_generation_log')} {where} ORDER BY _timestamp DESC LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/metadata/knowledge-base")
def get_knowledge_base(limit: int = 100):
    q = f"SELECT * FROM {fq('table_knowledge_base')} ORDER BY table_name LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/metadata/column-kb")
def get_column_kb(table_name: Optional[str] = None, limit: int = 200):
    where = f"WHERE table_name LIKE '%{table_name}%'" if table_name else ""
    q = f"SELECT * FROM {fq('column_knowledge_base')} {where} ORDER BY table_name, column_name LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/metadata/schema-kb")
def get_schema_kb():
    q = f"SELECT * FROM {fq('schema_knowledge_base')} ORDER BY schema_name"
    return execute_sql(q)


# ---------------------------------------------------------------------------
# Profiling endpoints
# ---------------------------------------------------------------------------

@app.get("/api/profiling/snapshots")
def get_profiling_snapshots(limit: int = 100):
    q = f"SELECT * FROM {fq('profiling_snapshots')} ORDER BY profiled_at DESC LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/profiling/column-stats")
def get_column_stats(table_name: Optional[str] = None, limit: int = 200):
    where = f"WHERE table_name LIKE '%{table_name}%'" if table_name else ""
    q = f"SELECT * FROM {fq('column_profiling_stats')} {where} ORDER BY table_name LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/profiling/quality-scores")
def get_quality_scores(limit: int = 100):
    q = f"SELECT * FROM {fq('data_quality_scores')} ORDER BY scored_at DESC LIMIT {limit}"
    return execute_sql(q)


# ---------------------------------------------------------------------------
# Ontology endpoints
# ---------------------------------------------------------------------------

@app.get("/api/ontology/entities")
def get_ontology_entities(limit: int = 200):
    q = f"SELECT * FROM {fq('ontology_entities')} ORDER BY confidence DESC LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/ontology/summary")
def get_ontology_summary():
    q = f"""
        SELECT entity_type, COUNT(*) as count,
               ROUND(AVG(confidence), 2) as avg_confidence,
               SUM(CASE WHEN validated THEN 1 ELSE 0 END) as validated
        FROM {fq('ontology_entities')}
        GROUP BY entity_type ORDER BY count DESC
    """
    return execute_sql(q)


# ---------------------------------------------------------------------------
# Analytics endpoints
# ---------------------------------------------------------------------------

@app.get("/api/analytics/clusters")
def get_cluster_assignments(limit: int = 500):
    q = f"SELECT * FROM {fq('node_cluster_assignments')} ORDER BY cluster, id LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/analytics/clustering-metrics")
def get_clustering_metrics():
    q = f"SELECT * FROM {fq('clustering_metrics')} ORDER BY run_timestamp DESC, phase, k"
    return execute_sql(q)


@app.get("/api/analytics/similarity-edges")
def get_similarity_edges(min_weight: float = 0.8, limit: int = 200):
    q = f"""
        SELECT * FROM {fq('graph_edges')}
        WHERE relationship = 'similar_embedding' AND weight >= {min_weight}
        ORDER BY weight DESC LIMIT {limit}
    """
    return execute_sql(q)


# ---------------------------------------------------------------------------
# Graph endpoints (used by GraphRAG and direct exploration)
# ---------------------------------------------------------------------------

@app.get("/api/graph/nodes")
def get_graph_nodes(
    node_type: Optional[str] = None,
    domain: Optional[str] = None,
    limit: int = 200,
):
    conditions = []
    if node_type:
        conditions.append(f"node_type = '{node_type}'")
    if domain:
        conditions.append(f"domain = '{domain}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    q = f"SELECT id, node_type, domain, security_level, comment FROM {fq('graph_nodes')} {where} ORDER BY id LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/graph/edges")
def get_graph_edges(src: Optional[str] = None, dst: Optional[str] = None, limit: int = 200):
    conditions = []
    if src:
        conditions.append(f"src = '{src}'")
    if dst:
        conditions.append(f"dst = '{dst}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    q = f"SELECT * FROM {fq('graph_edges')} {where} ORDER BY weight DESC LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/graph/neighbors/{node_id}")
def get_node_neighbors(node_id: str, hops: int = 1):
    """Get neighbors of a node up to N hops."""
    q = f"""
        SELECT e.dst as neighbor, e.relationship, e.weight,
               n.node_type, n.domain, n.comment
        FROM {fq('graph_edges')} e
        JOIN {fq('graph_nodes')} n ON e.dst = n.id
        WHERE e.src = '{node_id}'
        ORDER BY e.weight DESC
    """
    return execute_sql(q)


# ---------------------------------------------------------------------------
# GraphRAG endpoint (delegates to agent)
# ---------------------------------------------------------------------------

@app.post("/api/graph/query")
async def graph_rag_query(req: GraphQueryRequest):
    """Answer a natural-language question by traversing the knowledge graph."""
    from agent.graph import run_graph_agent
    result = await run_graph_agent(req.question, max_hops=req.max_hops)
    return result


# ---------------------------------------------------------------------------
# Serve React static files (production build)
# ---------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "src", "dist")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

"""FastAPI backend for dbxmetagen dashboard app."""

import os
import re
import logging
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from db import pg_execute, get_engine, pg_configured

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


_NOT_FOUND_RE = re.compile(
    r"TABLE_OR_VIEW_NOT_FOUND|SCHEMA_NOT_FOUND|CATALOG_NOT_FOUND"
    r"|does not exist|INVALID_SCHEMA_OR_RELATION_NAME"
    r"|relation .+ does not exist",
    re.IGNORECASE,
)


def execute_sql(query: str, warehouse_id: Optional[str] = None):
    """Execute SQL via Statement Execution API and return rows as list[dict].

    Returns [] for missing-table/schema/catalog errors (expected before
    pipelines have run).  Raises HTTPException for other failures.
    """
    wh = warehouse_id or os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        raise HTTPException(500, detail="WAREHOUSE_ID not configured")
    try:
        ws = get_workspace_client()
        resp = ws.statement_execution.execute_statement(
            statement=query, warehouse_id=wh, wait_timeout="30s"
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("SDK error executing SQL: %s", exc)
        raise HTTPException(500, detail=str(exc))
    if resp.status and resp.status.state and resp.status.state.value == "FAILED":
        msg = resp.status.error.message if resp.status.error else "SQL failed"
        if _NOT_FOUND_RE.search(msg):
            logger.warning("Table/schema not found: %s", msg)
            raise HTTPException(404, detail=msg)
        raise HTTPException(500, detail=msg)
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
    if pg_configured():
        logger.info("Lakebase PG connection configured -> %s:%s/%s",
                     os.environ.get("PGHOST"), os.environ.get("PGPORT", "5432"),
                     os.environ.get("PGDATABASE"))
        get_engine()  # eagerly create engine to fail fast on bad config
    else:
        logger.warning("PGHOST not set – add Lakebase database resource in Apps UI")
    yield

app = FastAPI(title="dbxmetagen API", version="0.6.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/config")
def get_config():
    """Return current catalog/schema defaults for frontend forms."""
    return {"catalog_name": CATALOG, "schema_name": SCHEMA}


@app.get("/api/debug/config")
def debug_config():
    """Return runtime env values for diagnostics."""
    return {
        "CATALOG_NAME": CATALOG,
        "SCHEMA_NAME": SCHEMA,
        "WAREHOUSE_ID": os.environ.get("WAREHOUSE_ID", ""),
        "PGHOST": os.environ.get("PGHOST", ""),
        "fq_example": fq("data_quality_scores"),
    }


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class JobRunRequest(BaseModel):
    job_id: Optional[int] = None
    job_name: Optional[str] = None
    # Metadata job params
    table_names: Optional[str] = None
    mode: Optional[str] = None
    apply_ddl: bool = False
    # Analytics pipeline params
    catalog_name: Optional[str] = None
    schema_name: Optional[str] = None
    extra_params: dict = {}


class GraphQueryRequest(BaseModel):
    question: str
    max_hops: int = 3


class GraphTraverseRequest(BaseModel):
    start_node: str
    max_hops: int = 3
    relationship: Optional[str] = None
    direction: str = "outgoing"  # outgoing | incoming | both


# ---------------------------------------------------------------------------
# Jobs endpoints
# ---------------------------------------------------------------------------

@app.get("/api/jobs")
def list_jobs():
    """List dbxmetagen jobs in the workspace."""
    ws = get_workspace_client()
    results = [{"job_id": j.job_id, "name": j.settings.name if j.settings else ""}
               for j in ws.jobs.list(name="dbxmetagen")]
    if not results:
        # Fallback: list all jobs and filter client-side
        results = [{"job_id": j.job_id, "name": j.settings.name if j.settings else ""}
                   for j in ws.jobs.list()
                   if j.settings and j.settings.name and "dbxmetagen" in j.settings.name.lower()]
    return results


@app.post("/api/jobs/run")
def run_job(req: JobRunRequest):
    """Trigger a dbxmetagen job by job_id (preferred) or job_name suffix match."""
    ws = get_workspace_client()
    if req.job_id:
        target_job_id = req.job_id
    elif req.job_name:
        all_jobs = list(ws.jobs.list(name="dbxmetagen"))
        if not all_jobs:
            all_jobs = list(ws.jobs.list())
        matching = [j for j in all_jobs if j.settings and j.settings.name and j.settings.name.endswith(req.job_name)]
        if not matching:
            matching = [j for j in all_jobs if j.settings and req.job_name in (j.settings.name or "")]
        if not matching:
            available = [j.settings.name for j in all_jobs if j.settings and j.settings.name]
            raise HTTPException(404, detail=f"Job '{req.job_name}' not found. Available: {available}")
        target_job_id = matching[0].job_id
    else:
        raise HTTPException(400, detail="Provide job_id or job_name")
    params = {}
    if req.table_names is not None:
        params["table_names"] = req.table_names
    if req.mode is not None:
        params["mode"] = req.mode
        params["apply_ddl"] = str(req.apply_ddl).lower()
    if req.catalog_name is not None:
        params["catalog_name"] = req.catalog_name
    if req.schema_name is not None:
        params["schema_name"] = req.schema_name
    params.update(req.extra_params)
    run = ws.jobs.run_now(job_id=target_job_id, job_parameters=params)
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
    q = f"SELECT * FROM {fq('metadata_generation_log')} {where} ORDER BY _created_at DESC LIMIT {limit}"
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
    q = f"SELECT * FROM {fq('profiling_snapshots')} ORDER BY created_at DESC LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/profiling/column-stats")
def get_column_stats(table_name: Optional[str] = None, limit: int = 200):
    where = f"WHERE table_name LIKE '%{table_name}%'" if table_name else ""
    q = f"SELECT * FROM {fq('column_profiling_stats')} {where} ORDER BY table_name LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/profiling/quality-scores")
def get_quality_scores(limit: int = 100):
    q = f"SELECT * FROM {fq('data_quality_scores')} ORDER BY created_at DESC LIMIT {limit}"
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
        SELECT * FROM public.graph_edges
        WHERE relationship = 'similar_embedding' AND weight >= {min_weight}
        ORDER BY weight DESC LIMIT {limit}
    """
    try:
        return pg_execute(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


# ---------------------------------------------------------------------------
# FK Predictions
# ---------------------------------------------------------------------------

@app.get("/api/analytics/fk-predictions")
def get_fk_predictions(limit: int = 200):
    """Return predicted foreign key relationships."""
    q = f"SELECT * FROM {fq('fk_predictions')} ORDER BY final_confidence DESC LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/analytics/fk-ddl")
def get_fk_ddl():
    """Return generated FK DDL statements."""
    q = f"SELECT * FROM {fq('fk_ddl_statements')} ORDER BY confidence DESC"
    return execute_sql(q)


# ---------------------------------------------------------------------------
# Unprofiled tables (information_schema coverage)
# ---------------------------------------------------------------------------

@app.get("/api/coverage/summary")
def get_coverage_summary():
    """Count tables per catalog/schema that have NOT been profiled yet.

    Uses information_schema.tables as the source of truth, joined against
    table_knowledge_base to determine which tables have been processed.
    """
    q = f"""
        SELECT t.table_catalog, t.table_schema,
               COUNT(*) as total_tables,
               COUNT(kb.table_name) as profiled_tables,
               COUNT(*) - COUNT(kb.table_name) as unprofiled_tables
        FROM system.information_schema.tables t
        LEFT JOIN {fq('table_knowledge_base')} kb
          ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = kb.table_name
        WHERE t.table_catalog = '{CATALOG}'
          AND t.table_schema NOT IN ('information_schema', '__internal')
          AND t.table_type = 'MANAGED'
        GROUP BY t.table_catalog, t.table_schema
        ORDER BY unprofiled_tables DESC
    """
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code != 404:
            raise
        # Table not found -- fall back to information_schema only
        q_simple = f"""
            SELECT table_catalog, table_schema, COUNT(*) as total_tables,
                   0 as profiled_tables, COUNT(*) as unprofiled_tables
            FROM system.information_schema.tables
            WHERE table_catalog = '{CATALOG}'
              AND table_schema NOT IN ('information_schema', '__internal')
              AND table_type = 'MANAGED'
            GROUP BY table_catalog, table_schema
            ORDER BY total_tables DESC
        """
        return execute_sql(q_simple)


@app.get("/api/coverage/tables")
def get_coverage_tables(catalog: Optional[str] = None, schema: Optional[str] = None):
    """List individual tables and whether they've been profiled."""
    cat = catalog or CATALOG
    conditions = [f"t.table_catalog = '{cat}'"]
    if schema:
        conditions.append(f"t.table_schema = '{schema}'")
    else:
        conditions.append("t.table_schema NOT IN ('information_schema', '__internal')")
    where = " AND ".join(conditions)
    q = f"""
        SELECT t.table_catalog, t.table_schema, t.table_name, t.table_type,
               CASE WHEN kb.table_name IS NOT NULL THEN true ELSE false END as is_profiled
        FROM system.information_schema.tables t
        LEFT JOIN {fq('table_knowledge_base')} kb
          ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = kb.table_name
        WHERE {where} AND t.table_type = 'MANAGED'
        ORDER BY t.table_schema, t.table_name
    """
    try:
        return execute_sql(q)
    except HTTPException:
        # Fallback without KB join
        q_simple = f"""
            SELECT table_catalog, table_schema, table_name, table_type,
                   false as is_profiled
            FROM system.information_schema.tables
            WHERE {where.replace('t.', '')} AND table_type = 'MANAGED'
            ORDER BY table_schema, table_name
        """
        return execute_sql(q_simple)


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
    q = f"SELECT id, node_type, domain, security_level, comment FROM public.graph_nodes {where} ORDER BY id LIMIT {limit}"
    return pg_execute(q)


@app.get("/api/graph/edges")
def get_graph_edges(src: Optional[str] = None, dst: Optional[str] = None, limit: int = 200):
    conditions = []
    if src:
        conditions.append(f"src = '{src}'")
    if dst:
        conditions.append(f"dst = '{dst}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    q = f"SELECT * FROM public.graph_edges {where} ORDER BY weight DESC LIMIT {limit}"
    return pg_execute(q)


@app.get("/api/graph/neighbors/{node_id}")
def get_node_neighbors(node_id: str, relationship: Optional[str] = None):
    """Get neighbors of a node, optionally filtered by edge relationship."""
    rel_filter = f" AND e.relationship = '{relationship}'" if relationship else ""
    q = f"""
        SELECT e.dst as neighbor, e.relationship, e.weight,
               n.node_type, n.domain, n.comment
        FROM public.graph_edges e
        JOIN public.graph_nodes n ON e.dst = n.id
        WHERE e.src = '{node_id}'{rel_filter}
        ORDER BY e.weight DESC
    """
    return pg_execute(q)


# ---------------------------------------------------------------------------
# Graph traversal (multi-hop against Lakebase catalog)
# ---------------------------------------------------------------------------

def multi_hop_traverse(
    start_node: str,
    max_hops: int = 3,
    relationship: Optional[str] = None,
    direction: str = "outgoing",
) -> dict:
    """Iterative multi-hop graph traversal against Lakebase tables via direct PG.

    Returns {"nodes": [...], "edges": [...], "paths": [[node_ids...], ...]}.
    """
    visited_nodes: dict[str, dict] = {}
    collected_edges: list[dict] = []
    frontier = {start_node}
    paths: list[list[str]] = [[start_node]]

    for hop in range(max_hops):
        if not frontier:
            break
        id_list = ", ".join(f"'{n}'" for n in frontier)
        if direction == "outgoing":
            edge_filter = f"src IN ({id_list})"
        elif direction == "incoming":
            edge_filter = f"dst IN ({id_list})"
        else:
            edge_filter = f"(src IN ({id_list}) OR dst IN ({id_list}))"
        rel_filter = f" AND relationship = '{relationship}'" if relationship else ""
        edge_q = f"SELECT src, dst, relationship, weight FROM public.graph_edges WHERE {edge_filter}{rel_filter}"
        edges = pg_execute(edge_q)
        if not edges:
            break
        collected_edges.extend(edges)
        next_frontier: set[str] = set()
        for e in edges:
            for nid in (e.get("src"), e.get("dst")):
                if nid and nid not in visited_nodes:
                    next_frontier.add(nid)
        frontier = next_frontier
        new_paths = []
        for e in edges:
            for p in paths:
                if p[-1] == e.get("src") and e.get("dst") not in set(p):
                    new_paths.append(p + [e["dst"]])
                elif direction != "outgoing" and p[-1] == e.get("dst") and e.get("src") not in set(p):
                    new_paths.append(p + [e["src"]])
        if new_paths:
            paths = new_paths

    # Fetch details for all discovered nodes
    all_ids = {start_node}
    for e in collected_edges:
        all_ids.add(e.get("src", ""))
        all_ids.add(e.get("dst", ""))
    all_ids.discard("")
    if all_ids:
        ids_sql = ", ".join(f"'{n}'" for n in all_ids)
        node_q = f"SELECT id, node_type, domain, subdomain, security_level, comment, table_name FROM public.graph_nodes WHERE id IN ({ids_sql})"
        node_rows = pg_execute(node_q)
        for n in node_rows:
            visited_nodes[n["id"]] = n

    return {
        "nodes": list(visited_nodes.values()),
        "edges": collected_edges,
        "paths": paths,
        "hops_completed": min(max_hops, len(paths[0]) - 1 if paths and paths[0] else 0),
    }


@app.post("/api/graph/traverse")
def graph_traverse(req: GraphTraverseRequest):
    """Multi-hop graph traversal starting from a node, querying Lakebase catalog."""
    return multi_hop_traverse(
        start_node=req.start_node,
        max_hops=req.max_hops,
        relationship=req.relationship,
        direction=req.direction,
    )


# ---------------------------------------------------------------------------
# GraphRAG endpoint (delegates to agent)
# ---------------------------------------------------------------------------

@app.post("/api/graph/query")
async def graph_rag_query(req: GraphQueryRequest):
    """Answer a natural-language question by traversing the knowledge graph."""
    try:
        from agent.graph import run_graph_agent
    except ImportError as e:
        raise HTTPException(503, detail=f"Agent not available: {e}")
    try:
        result = await run_graph_agent(req.question, max_hops=req.max_hops)
        return result
    except Exception as exc:
        msg = str(exc)
        if "REQUEST_LIMIT_EXCEEDED" in msg or "429" in msg or "RateLimitError" in msg:
            raise HTTPException(429, detail="Model rate limit exceeded. Try again in a minute or switch to a different model.") from exc
        logger.error("GraphRAG agent error: %s", exc)
        raise HTTPException(500, detail=f"Agent error: {msg}") from exc


# ---------------------------------------------------------------------------
# Serve React static files (production build)
# ---------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "src", "dist")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

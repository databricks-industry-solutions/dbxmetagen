"""FastAPI backend for dbxmetagen dashboard app."""

import os
import re
import json
import time
import uuid as _uuid
import queue
import logging
import threading

import yaml
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from db import pg_execute, get_engine, pg_configured

logger = logging.getLogger(__name__)

# Background Genie builder tasks: task_id -> {status, stage, result, error, created}
_genie_tasks: dict[str, dict] = {}

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


def execute_sql(query: str, warehouse_id: Optional[str] = None, timeout: int = 30):
    """Execute SQL via Statement Execution API and return rows as list[dict].

    Returns [] for missing-table/schema/catalog errors (expected before
    pipelines have run).  Raises HTTPException for other failures.
    Polls for completion when the initial wait_timeout is exceeded.
    """
    wh = warehouse_id or os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        raise HTTPException(500, detail="WAREHOUSE_ID not configured")
    try:
        ws = get_workspace_client()
        wait_s = min(timeout, 50)
        resp = ws.statement_execution.execute_statement(
            statement=query, warehouse_id=wh, wait_timeout=f"{wait_s}s"
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("SDK error executing SQL: %s", exc)
        raise HTTPException(500, detail=str(exc))

    # Poll if still running (AI_QUERY can exceed the initial wait_timeout)
    deadline = time.time() + timeout
    while (
        resp.status
        and resp.status.state
        and resp.status.state.value in ("PENDING", "RUNNING")
    ):
        if time.time() > deadline:
            raise HTTPException(504, detail=f"Query timed out after {timeout}s")
        time.sleep(3)
        try:
            resp = ws.statement_execution.get_statement(resp.statement_id)
        except Exception as exc:
            logger.error("Error polling statement %s: %s", resp.statement_id, exc)
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


def graph_query(sql: str) -> list[dict]:
    """Query graph tables: try Lakebase PG first, fall back to UC Delta tables."""
    if pg_configured():
        try:
            return pg_execute(sql)
        except HTTPException:
            logger.warning("PG graph query failed, falling back to UC")
        except Exception as e:
            logger.warning("PG graph query unexpected error: %s, falling back to UC", e)
    uc_sql = sql.replace("public.graph_nodes", fq("graph_nodes")).replace(
        "public.graph_edges", fq("graph_edges")
    )
    return execute_sql(uc_sql)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("dbxmetagen API starting – catalog=%s schema=%s", CATALOG, SCHEMA)
    if pg_configured():
        logger.info(
            "Lakebase PG connection configured -> %s:%s/%s",
            os.environ.get("PGHOST"),
            os.environ.get("PGPORT", "5432"),
            os.environ.get("PGDATABASE"),
        )
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


class SemanticLayerQuestionsRequest(BaseModel):
    questions: list[str]


class SemanticProfileRequest(BaseModel):
    profile_name: str
    questions: list[str]
    table_patterns: list[str] = []


class SemanticGenerateRequest(BaseModel):
    tables: list[str]
    questions: list[str]
    catalog_name: Optional[str] = None
    schema_name: Optional[str] = None
    model_endpoint: str = "databricks-claude-sonnet-4-6"
    project_id: Optional[str] = None
    mode: str = (
        "replace"  # "replace" (supersede matching), "additive" (skip supersede), "replace_all" (supersede ALL in project)
    )


class SemanticProjectRequest(BaseModel):
    project_name: str
    description: str = ""


class MetricViewCreateRequest(BaseModel):
    target_catalog: str
    target_schema: str


class GenieGenerateRequest(BaseModel):
    table_identifiers: list[str]
    questions: list[str] = []
    model_endpoint: str = "databricks-claude-3-7-sonnet"


class GenieCreateRequest(BaseModel):
    title: str
    serialized_space: dict
    warehouse_id: Optional[str] = None
    space_id: Optional[str] = None  # if provided, update instead of create


# ---------------------------------------------------------------------------
# Jobs endpoints
# ---------------------------------------------------------------------------


def _list_dbxmetagen_jobs(ws):
    """List jobs containing 'dbxmetagen' in the name (client-side filter)."""
    try:
        all_jobs = list(ws.jobs.list())
    except Exception as e:
        logger.error("ws.jobs.list() failed: %s", e)
        raise HTTPException(
            503,
            detail=f"Failed to list jobs from Databricks API: {e}. "
            "Check app SPN permissions and workspace connectivity.",
        )
    matched = [
        j
        for j in all_jobs
        if j.settings and j.settings.name and "dbxmetagen" in j.settings.name.lower()
    ]
    logger.info(
        "Job discovery: %d total jobs visible to SPN, %d matched 'dbxmetagen' filter",
        len(all_jobs),
        len(matched),
    )
    if all_jobs and not matched:
        names_sample = [
            j.settings.name for j in all_jobs[:10] if j.settings and j.settings.name
        ]
        logger.warning(
            "SPN sees %d jobs but none contain 'dbxmetagen'. Sample names: %s",
            len(all_jobs),
            names_sample,
        )
    return matched


# ---------------------------------------------------------------------------
# Auto-create jobs when bundle-deployed jobs aren't visible
# ---------------------------------------------------------------------------

_BUNDLE_ROOT = os.environ.get("BUNDLE_ROOT_PATH", "").rstrip("/")
if _BUNDLE_ROOT:
    logger.info("BUNDLE_ROOT_PATH=%s", _BUNDLE_ROOT)
else:
    logger.warning("BUNDLE_ROOT_PATH not set; job auto-creation disabled")
_NODE_TYPE = os.environ.get("NODE_TYPE", "i3.xlarge")
_SPARK_VER = "17.3.x-cpu-ml-scala2.13"
_cached_whl: Optional[str] = None

_ALL_JOB_SUFFIXES = [
    "metadata_job",
    "parallel_modes_job",
    "full_analytics_pipeline",
    "fk_prediction",
    "ontology_prediction",
    "semantic_layer",
    "sync_graph_lakebase",
]


def _nb_path(name: str) -> str:
    return f"{_BUNDLE_ROOT}/files/notebooks/{name}"


def _find_whl(ws) -> Optional[str]:
    global _cached_whl
    if _cached_whl is not None:
        return _cached_whl or None
    if not _BUNDLE_ROOT:
        _cached_whl = ""
        return None
    try:
        for obj in ws.workspace.list(f"{_BUNDLE_ROOT}/artifacts", recursive=True):
            p = getattr(obj, "path", None)
            if p and "dbxmetagen" in p and p.endswith(".whl"):
                _cached_whl = p
                logger.info("Discovered wheel: %s", p)
                return p
    except Exception as e:
        logger.warning("Wheel discovery at %s/artifacts failed: %s", _BUNDLE_ROOT, e)
    _cached_whl = ""
    return None


def _cluster_spec(key="cluster", workers=1, autoscale=None):
    spec = {"spark_version": _SPARK_VER, "node_type_id": _NODE_TYPE}
    if autoscale:
        spec["autoscale"] = {"min_workers": autoscale[0], "max_workers": autoscale[1]}
    else:
        spec["num_workers"] = workers
    return {"job_cluster_key": key, "new_cluster": spec}


def _nb_task(
    key,
    notebook,
    param_names,
    *,
    deps=None,
    fixed=None,
    cluster_key="cluster",
    whl=None,
    extra_libs=None,
):
    bp = {p: f"{{{{job.parameters.{p}}}}}" for p in param_names}
    if fixed:
        bp.update(fixed)
    t = {
        "task_key": key,
        "notebook_task": {"notebook_path": _nb_path(notebook), "base_parameters": bp},
        "job_cluster_key": cluster_key,
    }
    libs = []
    if whl:
        libs.append({"whl": whl})
    if extra_libs:
        libs.extend(extra_libs)
    if libs:
        t["libraries"] = libs
    if deps:
        t["depends_on"] = [{"task_key": d} for d in deps]
    return t


def _build_job_spec(suffix: str, whl: Optional[str]) -> Optional[dict]:
    """Return ws.jobs.create() kwargs for the given job suffix, or None."""
    cat = os.environ.get("CATALOG_NAME", "")
    sch = os.environ.get("SCHEMA_NAME", "")
    wh = os.environ.get("WAREHOUSE_ID", "")

    def task(key, nb, params, **kw):
        return _nb_task(key, nb, params, whl=whl, **kw)

    def param(n, d=""):
        return {"name": n, "default": d}

    cp = [param("catalog_name", cat), param("schema_name", sch)]
    kp = ["catalog_name", "schema_name"]

    if suffix == "metadata_job":
        return {
            "tasks": [
                task(
                    "generate_metadata",
                    "generate_metadata.py",
                    ["mode", "table_names", "catalog_name", "schema_name", "apply_ddl"],
                )
            ],
            "parameters": [
                param("table_names"),
                param("mode", "comment"),
                param("apply_ddl", "false"),
            ]
            + cp,
            "job_clusters": [_cluster_spec(autoscale=(1, 4))],
        }

    if suffix == "parallel_modes_job":
        bp = ["table_names", "catalog_name", "schema_name", "apply_ddl"]
        return {
            "tasks": [
                task(
                    "generate_comments",
                    "generate_metadata.py",
                    bp,
                    fixed={"mode": "comment"},
                ),
                task("generate_pi", "generate_metadata.py", bp, fixed={"mode": "pi"}),
                task(
                    "generate_domain",
                    "generate_metadata.py",
                    bp,
                    fixed={"mode": "domain"},
                ),
            ],
            "parameters": [param("table_names"), param("apply_ddl", "false")] + cp,
            "job_clusters": [_cluster_spec(autoscale=(1, 4))],
        }

    okp = kp + ["ontology_bundle"]

    if suffix == "full_analytics_pipeline":
        return {
            "tasks": [
                task("build_knowledge_base", "build_knowledge_base.py", kp),
                task(
                    "build_column_kb",
                    "build_column_kb.py",
                    kp,
                    deps=["build_knowledge_base"],
                ),
                task(
                    "build_schema_kb",
                    "build_schema_kb.py",
                    kp,
                    deps=["build_knowledge_base"],
                ),
                task(
                    "extract_extended_metadata",
                    "extract_extended_metadata.py",
                    kp,
                    deps=["build_knowledge_base"],
                ),
                task(
                    "build_knowledge_graph",
                    "build_knowledge_graph.py",
                    kp,
                    deps=[
                        "build_column_kb",
                        "build_schema_kb",
                        "extract_extended_metadata",
                    ],
                    extra_libs=[{"pypi": {"package": "graphframes"}}],
                ),
                task(
                    "generate_embeddings",
                    "generate_embeddings.py",
                    kp,
                    deps=["build_knowledge_graph"],
                ),
                task(
                    "run_profiling",
                    "run_profiling.py",
                    kp,
                    deps=["build_knowledge_graph"],
                ),
                task(
                    "build_ontology",
                    "build_ontology.py",
                    okp,
                    deps=["build_knowledge_base", "build_column_kb"],
                ),
                task(
                    "build_similarity_edges",
                    "build_similarity_edges.py",
                    kp,
                    deps=["generate_embeddings"],
                ),
                task(
                    "cluster_analysis",
                    "cluster_analysis.py",
                    kp,
                    deps=["generate_embeddings"],
                ),
                task(
                    "compute_data_quality",
                    "compute_data_quality.py",
                    kp,
                    deps=["run_profiling"],
                ),
                task(
                    "validate_ontology",
                    "validate_ontology.py",
                    kp,
                    deps=["build_ontology"],
                ),
                task(
                    "predict_foreign_keys",
                    "predict_foreign_keys.py",
                    kp,
                    deps=["build_similarity_edges", "compute_data_quality"],
                ),
                task(
                    "final_analysis",
                    "final_analysis.py",
                    kp,
                    deps=[
                        "predict_foreign_keys",
                        "cluster_analysis",
                        "compute_data_quality",
                        "validate_ontology",
                    ],
                ),
            ],
            "parameters": cp + [param("ontology_bundle", "healthcare")],
            "job_clusters": [_cluster_spec(autoscale=(1, 4))],
        }

    if suffix == "fk_prediction":
        return {
            "tasks": [
                task(
                    "predict_foreign_keys",
                    "predict_foreign_keys.py",
                    ["catalog_name", "schema_name", "apply_ddl"],
                )
            ],
            "parameters": [param("apply_ddl", "false")] + cp,
            "job_clusters": [_cluster_spec(autoscale=(1, 4))],
        }

    if suffix == "ontology_prediction":
        return {
            "tasks": [
                task("build_knowledge_base", "build_knowledge_base.py", kp),
                task("build_column_kb", "build_column_kb.py", kp),
                task(
                    "build_ontology",
                    "build_ontology.py",
                    okp,
                    deps=["build_knowledge_base", "build_column_kb"],
                ),
                task(
                    "validate_ontology",
                    "validate_ontology.py",
                    kp,
                    deps=["build_ontology"],
                ),
            ],
            "parameters": cp + [param("ontology_bundle", "healthcare")],
            "job_clusters": [_cluster_spec()],
        }

    if suffix == "semantic_layer":
        return {
            "tasks": [
                task("generate_semantic_layer", "generate_semantic_layer.py", kp),
                task(
                    "apply_metric_views",
                    "apply_metric_views.py",
                    ["catalog_name", "schema_name", "warehouse_id"],
                    deps=["generate_semantic_layer"],
                ),
            ],
            "parameters": cp + [param("warehouse_id", wh)],
            "job_clusters": [_cluster_spec()],
        }

    if suffix == "sync_graph_lakebase":
        return {
            "tasks": [
                task(
                    "sync_to_lakebase",
                    "sync_graph_to_lakebase.py",
                    ["source_catalog", "source_schema", "lakebase_catalog"],
                )
            ],
            "parameters": [
                param("source_catalog", cat),
                param("source_schema", sch),
                param(
                    "lakebase_catalog",
                    os.environ.get("LAKEBASE_CATALOG", "dbxmetagen_graphrag"),
                ),
            ],
            "job_clusters": [_cluster_spec()],
        }

    return None


def _auto_create_job(ws, suffix: str) -> Optional[int]:
    """Auto-create a job for the given suffix. Returns job_id or None."""
    if not _BUNDLE_ROOT:
        logger.warning("BUNDLE_ROOT_PATH not set; cannot auto-create job '%s'", suffix)
        return None

    clean = suffix.lstrip("_")
    name = f"dbxmetagen_{clean}"

    try:
        for j in ws.jobs.list(name=name):
            if j.settings and j.settings.name == name:
                logger.info("Job '%s' already exists (id=%s)", name, j.job_id)
                return j.job_id
    except Exception:
        pass

    whl = _find_whl(ws)
    spec = _build_job_spec(clean, whl)
    if not spec:
        logger.warning("No template for job suffix '%s'", clean)
        return None

    spec["name"] = name
    try:
        resp = ws.jobs.create(**spec)
        logger.info("Auto-created job '%s' -> id=%s", name, resp.job_id)
        return resp.job_id
    except Exception as e:
        logger.error("Failed to auto-create job '%s': %s", name, e)
        return None


@app.get("/api/jobs")
def list_jobs():
    """List dbxmetagen jobs in the workspace. Auto-creates from templates if none found."""
    ws = get_workspace_client()
    jobs = _list_dbxmetagen_jobs(ws)
    if not jobs and _BUNDLE_ROOT:
        logger.info("No dbxmetagen jobs found; auto-creating from templates")
        for suffix in _ALL_JOB_SUFFIXES:
            _auto_create_job(ws, suffix)
        jobs = _list_dbxmetagen_jobs(ws)
    if not jobs:
        logger.warning(
            "No dbxmetagen jobs found.%s",
            (
                " Set BUNDLE_ROOT_PATH env var to enable auto-creation."
                if not _BUNDLE_ROOT
                else ""
            ),
        )
    return [{"job_id": j.job_id, "name": j.settings.name} for j in jobs]


@app.post("/api/jobs/run")
def run_job(req: JobRunRequest):
    """Trigger a dbxmetagen job by job_id (preferred) or job_name suffix match."""
    logger.info("run_job request: job_id=%s, job_name=%s", req.job_id, req.job_name)
    ws = get_workspace_client()
    if req.job_id:
        target_job_id = req.job_id
    elif req.job_name:
        all_jobs = _list_dbxmetagen_jobs(ws)
        matching = [j for j in all_jobs if j.settings.name.endswith(req.job_name)]
        if not matching:
            matching = [j for j in all_jobs if req.job_name in j.settings.name]
        if matching:
            target_job_id = matching[0].job_id
        else:
            created_id = _auto_create_job(ws, req.job_name)
            if created_id:
                target_job_id = created_id
            else:
                available = [j.settings.name for j in all_jobs]
                raise HTTPException(
                    404,
                    detail=f"Job '{req.job_name}' not found. "
                    f"Run 'databricks bundle deploy' to create and grant jobs to the app. "
                    f"Available jobs ({len(available)}): {available}",
                )
    else:
        raise HTTPException(400, detail="Provide job_id or job_name")
    params = {}
    if req.table_names:
        params["table_names"] = req.table_names
    if req.mode:
        params["mode"] = req.mode
    if req.apply_ddl:
        params["apply_ddl"] = "true"
    if req.catalog_name:
        params["catalog_name"] = req.catalog_name
    if req.schema_name:
        params["schema_name"] = req.schema_name
    params.update(req.extra_params)
    try:
        run = ws.jobs.run_now(job_id=target_job_id, job_parameters=params)
    except Exception as e:
        logger.error("jobs.run_now(job_id=%s) failed: %s", target_job_id, e)
        raise HTTPException(
            500,
            detail=f"Failed to trigger job {target_job_id}: {e}. "
            "The SPN may lack CAN_MANAGE_RUN permission on this job.",
        )
    return {"run_id": run.run_id}


@app.get("/api/jobs/{run_id}/status")
def get_run_status(run_id: int):
    """Get status of a job run."""
    ws = get_workspace_client()
    run = ws.jobs.get_run(run_id=run_id)
    return {
        "run_id": run.run_id,
        "state": run.state.life_cycle_state.value if run.state else "UNKNOWN",
        "result": (
            run.state.result_state.value
            if run.state and run.state.result_state
            else None
        ),
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


@app.get("/api/ontology/bundles")
def get_ontology_bundles():
    """List available ontology bundles with metadata."""
    from dbxmetagen.ontology import list_available_bundles
    return list_available_bundles()


@app.get("/api/ontology/compare")
def compare_ontology_bundles(bundle_a: str = "", bundle_b: str = ""):
    """Compare entity discoveries across two ontology bundles."""
    if not bundle_a or not bundle_b:
        return {"error": "Provide both bundle_a and bundle_b query params"}
    q = f"""
        SELECT
            COALESCE(a.entity_type, b.entity_type) AS entity_type,
            COALESCE(a.source_table, b.source_table) AS source_table,
            a.confidence AS {bundle_a}_confidence,
            b.confidence AS {bundle_b}_confidence,
            CASE
                WHEN a.entity_type IS NOT NULL AND b.entity_type IS NOT NULL THEN 'both'
                WHEN a.entity_type IS NOT NULL THEN '{bundle_a}_only'
                ELSE '{bundle_b}_only'
            END AS presence
        FROM (
            SELECT entity_type, explode(source_tables) AS source_table, MAX(confidence) AS confidence
            FROM {fq('ontology_entities')}
            WHERE ontology_bundle = '{bundle_a}'
            GROUP BY entity_type, source_table
        ) a
        FULL OUTER JOIN (
            SELECT entity_type, explode(source_tables) AS source_table, MAX(confidence) AS confidence
            FROM {fq('ontology_entities')}
            WHERE ontology_bundle = '{bundle_b}'
            GROUP BY entity_type, source_table
        ) b ON a.entity_type = b.entity_type AND a.source_table = b.source_table
        ORDER BY source_table, entity_type
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
    return graph_query(q)


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
# Visualization composite endpoints
# ---------------------------------------------------------------------------


@app.get("/api/viz/fk-map")
def viz_fk_map():
    """Composite data for FK Map visualization: table nodes, FK edges, clusters."""
    tables = graph_query(
        "SELECT id, node_type, domain, security_level, comment "
        "FROM public.graph_nodes WHERE node_type='table' ORDER BY id"
    )
    fk_edges = execute_sql(
        f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
        f"FROM {fq('fk_predictions')} ORDER BY final_confidence DESC LIMIT 500"
    )
    clusters = execute_sql(
        f"SELECT id, cluster FROM {fq('node_cluster_assignments')} "
        f"WHERE node_type='table' ORDER BY cluster, id"
    )
    return {"tables": tables, "fk_edges": fk_edges, "clusters": clusters}


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
        WHERE {where}
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
            WHERE {where.replace('t.', '')}
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
    return graph_query(q)


@app.get("/api/graph/edges")
def get_graph_edges(
    src: Optional[str] = None, dst: Optional[str] = None, limit: int = 200
):
    conditions = []
    if src:
        conditions.append(f"src = '{src}'")
    if dst:
        conditions.append(f"dst = '{dst}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    q = f"SELECT * FROM public.graph_edges {where} ORDER BY weight DESC LIMIT {limit}"
    return graph_query(q)


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
    return graph_query(q)


# ---------------------------------------------------------------------------
# Graph traversal (multi-hop against Lakebase catalog)
# ---------------------------------------------------------------------------


def multi_hop_traverse(
    start_node: str,
    max_hops: int = 3,
    relationship: Optional[str] = None,
    direction: str = "outgoing",
) -> dict:
    """Iterative multi-hop graph traversal (Lakebase PG with UC fallback).

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
        edges = graph_query(edge_q)
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
                elif (
                    direction != "outgoing"
                    and p[-1] == e.get("dst")
                    and e.get("src") not in set(p)
                ):
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
        node_rows = graph_query(node_q)
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
            raise HTTPException(
                429,
                detail="Model rate limit exceeded. Try again in a minute or switch to a different model.",
            ) from exc
        logger.error("GraphRAG agent error: %s", exc)
        raise HTTPException(500, detail=f"Agent error: {msg}") from exc


# ---------------------------------------------------------------------------
# Catalog / Schema / Table discovery (cascading selectors)
# ---------------------------------------------------------------------------


@app.get("/api/catalogs")
def list_catalogs():
    q = (
        "SELECT catalog_name FROM system.information_schema.catalogs "
        "WHERE catalog_name NOT IN ('system', '__databricks_internal') "
        "ORDER BY catalog_name"
    )
    rows = execute_sql(q)
    return [r["catalog_name"] for r in rows]


@app.get("/api/schemas")
def list_schemas(catalog: str):
    q = (
        f"SELECT schema_name FROM `{catalog}`.information_schema.schemata "
        f"WHERE schema_name NOT IN ('information_schema', '__internal') "
        f"ORDER BY schema_name"
    )
    rows = execute_sql(q)
    return [r["schema_name"] for r in rows]


@app.get("/api/tables")
def list_tables(catalog: str, schema: str):
    q = (
        f"SELECT table_name FROM `{catalog}`.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_type = 'MANAGED' "
        f"ORDER BY table_name"
    )
    rows = execute_sql(q)
    return [r["table_name"] for r in rows]


# ---------------------------------------------------------------------------
# Semantic Layer endpoints
# ---------------------------------------------------------------------------

_sl_tables_ensured = False


def _ensure_semantic_layer_tables():
    global _sl_tables_ensured
    if _sl_tables_ensured:
        return
    try:
        execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq('semantic_layer_questions')} (
                question_id STRING NOT NULL,
                question_text STRING,
                status STRING,
                created_at TIMESTAMP,
                processed_at TIMESTAMP
            ) COMMENT 'Business questions for semantic layer generation'
        """
        )
        execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq('metric_view_definitions')} (
                definition_id STRING NOT NULL,
                metric_view_name STRING,
                source_table STRING,
                json_definition STRING,
                source_questions STRING,
                status STRING,
                validation_errors STRING,
                genie_space_id STRING,
                created_at TIMESTAMP,
                applied_at TIMESTAMP,
                version INT DEFAULT 1,
                parent_definition_id STRING,
                project_id STRING
            ) COMMENT 'Generated metric view definitions with version history'
        """
        )
        # Idempotent: add columns if table pre-dates those changes
        for col_ddl in [
            "version INT, parent_definition_id STRING",
            "project_id STRING",
            "complexity_score INT, complexity_level STRING",
        ]:
            try:
                execute_sql(
                    f"ALTER TABLE {fq('metric_view_definitions')} ADD COLUMNS ({col_ddl})"
                )
            except Exception:
                pass
        execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq('semantic_layer_profiles')} (
                profile_id STRING NOT NULL,
                profile_name STRING,
                questions STRING,
                table_patterns STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) COMMENT 'Named question profiles for semantic layer'
        """
        )
        execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq('semantic_layer_projects')} (
                project_id STRING NOT NULL,
                project_name STRING,
                description STRING,
                created_at TIMESTAMP,
                selected_tables STRING
            ) COMMENT 'Named projects for grouping metric view definitions'
        """
        )
        try:
            execute_sql(
                f"ALTER TABLE {fq('semantic_layer_projects')} ADD COLUMNS (selected_tables STRING)"
            )
        except Exception:
            pass
        _sl_tables_ensured = True
        logger.info("Semantic layer tables ensured")
    except Exception as e:
        logger.warning("Could not ensure semantic layer tables (will retry): %s", e)


@app.get("/api/semantic-layer/questions")
def list_semantic_questions():
    _ensure_semantic_layer_tables()
    q = f"SELECT question_id, question_text, status, created_at, processed_at FROM {fq('semantic_layer_questions')} ORDER BY created_at DESC"
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


@app.post("/api/semantic-layer/questions")
def add_semantic_questions(req: SemanticLayerQuestionsRequest):
    _ensure_semantic_layer_tables()
    from datetime import datetime as _dt

    now = _dt.utcnow().isoformat()
    rows = []
    for q_text in req.questions:
        q_text = q_text.strip()
        if not q_text:
            continue
        qid = str(_uuid.uuid4())
        escaped = q_text.replace("'", "''")
        rows.append(f"('{qid}', '{escaped}', 'pending', '{now}', NULL)")
    if not rows:
        raise HTTPException(400, detail="No valid questions provided")
    values = ", ".join(rows)
    try:
        execute_sql(f"INSERT INTO {fq('semantic_layer_questions')} VALUES {values}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to insert semantic layer questions: %s", e)
        raise HTTPException(500, detail=f"Failed to save questions: {e}")
    return {"added": len(rows)}


@app.get("/api/semantic-layer/definitions")
def list_semantic_definitions(project_id: Optional[str] = None):
    _ensure_semantic_layer_tables()
    where = "WHERE status != 'superseded'"
    if project_id:
        where += f" AND project_id = '{project_id}'"
    q = (
        f"SELECT definition_id, metric_view_name, source_table, status, "
        f"validation_errors, genie_space_id, created_at, applied_at, "
        f"COALESCE(version, 1) as version, parent_definition_id, project_id "
        f"FROM {fq('metric_view_definitions')} "
        f"{where} "
        f"ORDER BY created_at DESC"
    )
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


@app.get("/api/semantic-layer/definitions/{definition_id}/json")
def get_semantic_definition_json(definition_id: str):
    q = (
        f"SELECT json_definition FROM {fq('metric_view_definitions')} "
        f"WHERE definition_id = '{definition_id}'"
    )
    rows = execute_sql(q)
    if not rows:
        raise HTTPException(404, detail="Definition not found")
    return {"json_definition": rows[0].get("json_definition", "")}


@app.delete("/api/semantic-layer/definitions/{definition_id}")
def delete_semantic_definition(definition_id: str):
    _ensure_semantic_layer_tables()
    execute_sql(
        f"DELETE FROM {fq('metric_view_definitions')} WHERE definition_id = '{definition_id}'"
    )
    return {"deleted": True, "definition_id": definition_id}


# --- Profiles ---


@app.get("/api/semantic-layer/profiles")
def list_profiles():
    _ensure_semantic_layer_tables()
    q = f"SELECT profile_id, profile_name, questions, table_patterns, created_at, updated_at FROM {fq('semantic_layer_profiles')} ORDER BY updated_at DESC"
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


@app.post("/api/semantic-layer/profiles")
def save_profile(req: SemanticProfileRequest):
    _ensure_semantic_layer_tables()
    from datetime import datetime as _dt

    now = _dt.utcnow().isoformat()
    qs_json = json.dumps(req.questions).replace("'", "''")
    tp_json = json.dumps(req.table_patterns).replace("'", "''")
    name_esc = req.profile_name.replace("'", "''")

    existing = execute_sql(
        f"SELECT profile_id FROM {fq('semantic_layer_profiles')} WHERE profile_name = '{name_esc}'"
    )
    if existing:
        pid = existing[0]["profile_id"]
        execute_sql(
            f"UPDATE {fq('semantic_layer_profiles')} "
            f"SET questions = '{qs_json}', table_patterns = '{tp_json}', updated_at = '{now}' "
            f"WHERE profile_id = '{pid}'"
        )
        return {"profile_id": pid, "updated": True}
    pid = str(_uuid.uuid4())
    execute_sql(
        f"INSERT INTO {fq('semantic_layer_profiles')} VALUES "
        f"('{pid}', '{name_esc}', '{qs_json}', '{tp_json}', '{now}', '{now}')"
    )
    return {"profile_id": pid, "updated": False}


@app.delete("/api/semantic-layer/profiles/{profile_id}")
def delete_profile(profile_id: str):
    _ensure_semantic_layer_tables()
    execute_sql(
        f"DELETE FROM {fq('semantic_layer_profiles')} WHERE profile_id = '{profile_id}'"
    )
    return {"deleted": True}


# --- Projects ---


@app.get("/api/semantic-layer/projects")
def list_projects():
    _ensure_semantic_layer_tables()
    try:
        return execute_sql(
            f"SELECT project_id, project_name, description, created_at, selected_tables "
            f"FROM {fq('semantic_layer_projects')} ORDER BY created_at DESC"
        )
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


@app.post("/api/semantic-layer/projects")
def create_project(req: SemanticProjectRequest):
    _ensure_semantic_layer_tables()
    from datetime import datetime as _dt

    pid = str(_uuid.uuid4())
    now = _dt.utcnow().isoformat()
    name_esc = req.project_name.replace("'", "''")
    desc_esc = req.description.replace("'", "''")
    execute_sql(
        f"INSERT INTO {fq('semantic_layer_projects')} VALUES "
        f"('{pid}', '{name_esc}', '{desc_esc}', '{now}', NULL)"
    )
    return {"project_id": pid, "project_name": req.project_name}


@app.delete("/api/semantic-layer/projects/{project_id}")
def delete_project(project_id: str):
    _ensure_semantic_layer_tables()
    execute_sql(
        f"DELETE FROM {fq('semantic_layer_projects')} WHERE project_id = '{project_id}'"
    )
    execute_sql(
        f"UPDATE {fq('metric_view_definitions')} SET project_id = NULL "
        f"WHERE project_id = '{project_id}'"
    )
    return {"deleted": True}


# --- In-app metric view generation ---

_sl_tasks: dict[str, dict] = {}


def _build_sl_context(tables: list[str], cat: str, sch: str) -> str:
    """Build context string from knowledge base tables, scoped to selected tables."""
    fq_tables = []
    for t in tables:
        if "." in t:
            fq_tables.append(t)
        else:
            fq_tables.append(f"{cat}.{sch}.{t}")
    in_clause = ", ".join(f"'{t}'" for t in fq_tables)

    parts: list[str] = []
    table_rows = execute_sql(
        f"SELECT table_name, comment, domain, subdomain FROM {fq('table_knowledge_base')} "
        f"WHERE table_name IN ({in_clause})"
    )
    col_rows = execute_sql(
        f"SELECT table_name, column_name, data_type, comment, classification "
        f"FROM {fq('column_knowledge_base')} WHERE table_name IN ({in_clause})"
    )
    col_by_table: dict[str, list] = {}
    for c in col_rows:
        col_by_table.setdefault(c["table_name"], []).append(c)

    fk_rows = []
    try:
        fk_rows = execute_sql(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} WHERE is_fk = 'true' AND final_confidence >= 0.7 "
            f"AND (src_table IN ({in_clause}) OR dst_table IN ({in_clause}))"
        )
    except HTTPException:
        pass

    ont_rows = []
    try:
        ont_rows = execute_sql(
            f"SELECT entity_type, source_tables FROM {fq('ontology_entities')} WHERE confidence >= 0.4"
        )
    except HTTPException:
        pass
    entity_map: dict[str, str] = {}
    for o in ont_rows:
        src_tables = o.get("source_tables") or ""
        if isinstance(src_tables, str):
            try:
                src_tables = json.loads(src_tables)
            except (json.JSONDecodeError, TypeError):
                src_tables = [src_tables] if src_tables else []
        for t in src_tables:
            entity_map[t] = o["entity_type"]

    for t in table_rows:
        tname = t["table_name"]
        ent = entity_map.get(tname, "")
        ent_str = f" Entity: {ent}" if ent else ""
        line = f"Table: {tname} (Comment: \"{t.get('comment', '')}\" Domain: {t.get('domain', '')} / {t.get('subdomain', '')}){ent_str}"
        cols = col_by_table.get(tname, [])
        col_strs = [
            f"  - {c['column_name']} {c.get('data_type', '')} : {c.get('comment', '')}"
            for c in cols
        ]
        parts.append(
            line + "\n  Columns:\n" + "\n".join(col_strs) if col_strs else line
        )

    if fk_rows:
        parts.append("\nFOREIGN KEY RELATIONSHIPS:")
        for fk in fk_rows:
            parts.append(
                f"  {fk['src_table']}.{fk['src_column']} -> {fk['dst_table']}.{fk['dst_column']} (confidence {fk['final_confidence']})"
            )

    # Include ontology metric suggestions if available
    metric_rows = []
    try:
        metric_rows = execute_sql(
            f"SELECT metric_name, description, entity_id, aggregation_type, source_field, filter_condition "
            f"FROM {fq('ontology_metrics')}"
        )
    except (HTTPException, Exception):
        pass
    if metric_rows:
        parts.append(
            "\nONTOLOGY METRIC SUGGESTIONS (use as hints for measures/dimensions):"
        )
        for m in metric_rows:
            line = f"  - {m.get('metric_name', '')}: {m.get('description', '')}"
            if m.get("aggregation_type") and m.get("source_field"):
                line += f"  -> {m['aggregation_type']}({m['source_field']})"
            if m.get("filter_condition"):
                line += f" WHERE {m['filter_condition']}"
            parts.append(line)

    if not table_rows:
        short_names = [t.split(".")[-1] for t in fq_tables]
        short_clause = ", ".join(f"'{t}'" for t in short_names)
        info_cols = execute_sql(
            f"SELECT table_name, column_name, data_type "
            f"FROM `{cat}`.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name IN ({short_clause})"
        )
        col_by_tbl: dict[str, list] = {}
        for c in info_cols:
            col_by_tbl.setdefault(c["table_name"], []).append(c)
        for tname, cols in col_by_tbl.items():
            col_strs = [
                f"  - {c['column_name']} {c.get('data_type', '')}" for c in cols
            ]
            parts.append(
                f"Table: {cat}.{sch}.{tname}\n  Columns:\n" + "\n".join(col_strs)
            )

    return "\n".join(parts)


_FEW_SHOT = """\
INPUT tables:
  sales.orders columns: [order_id BIGINT, customer_id BIGINT, order_date DATE, total_amount DECIMAL(10,2), region STRING, status STRING, is_returned BOOLEAN]
  sales.customers columns: [id BIGINT, name STRING, segment STRING, signup_date DATE]
  FK: orders.customer_id -> customers.id (confidence 0.95)

INPUT questions:
  1. What is total revenue by region?
  2. How many orders per month?
  3. What is the fulfillment rate by segment?

OUTPUT:
[
  {"name": "order_performance_metrics", "source": "sales.orders",
   "comment": "Order performance including revenue, fulfillment rates, and return analysis",
   "filter": "status IS NOT NULL",
   "dimensions": [
     {"name": "Order Month", "expr": "DATE_TRUNC('MONTH', order_date)", "comment": "Month of order placement"},
     {"name": "Order Quarter", "expr": "DATE_TRUNC('QUARTER', order_date)", "comment": "Quarter of order placement"},
     {"name": "Region", "expr": "region", "comment": "Sales region"},
     {"name": "Status", "expr": "status", "comment": "Order fulfillment status"},
     {"name": "Customer Segment", "expr": "segment", "comment": "Customer segment from joined customers table"},
     {"name": "Customer Tier", "expr": "CASE WHEN segment IN ('Enterprise', 'Strategic') THEN 'Top Tier' WHEN segment = 'Mid-Market' THEN 'Growth' ELSE 'Standard' END", "comment": "Customer tier grouping"}],
   "measures": [
     {"name": "Total Revenue", "expr": "SUM(total_amount)", "comment": "Sum of all order values"},
     {"name": "Order Count", "expr": "COUNT(*)", "comment": "Number of orders"},
     {"name": "Avg Order Value", "expr": "AVG(total_amount)", "comment": "Average order amount"},
     {"name": "Unique Customers", "expr": "COUNT(DISTINCT customer_id)", "comment": "Distinct customer count"},
     {"name": "Revenue per Customer", "expr": "SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_id), 0)", "comment": "Average revenue per unique customer"},
     {"name": "Fulfillment Rate", "expr": "SUM(CASE WHEN status = 'fulfilled' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of orders fulfilled (0 to 1)"},
     {"name": "Return Rate", "expr": "SUM(CASE WHEN is_returned = TRUE THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of orders returned (0 to 1)"},
     {"name": "Fulfilled Revenue", "expr": "SUM(total_amount) FILTER (WHERE status = 'fulfilled')", "comment": "Revenue from fulfilled orders only"},
     {"name": "High Value Order Count", "expr": "COUNT(*) FILTER (WHERE total_amount > 1000)", "comment": "Orders above 1000 threshold"}],
   "joins": [{"name": "customers", "source": "sales.customers", "on": "customers.id = orders.customer_id"}]}
]"""


def _build_prompt(questions: list[str], context: str) -> str:
    q_block = "\n".join(f"  {i+1}. {q}" for i, q in enumerate(questions))
    return f"""You are a data modeler building a semantic layer for Databricks Unity Catalog.

TASK: Generate metric view definitions (as a JSON array) that enable answering the business questions below.

RULES:
1. Create metric views organized around analytical themes from the questions, not just one-to-one with tables. Multiple metric views from the same table are fine if they address different analytical angles
2. Only reference columns that exist in the metadata below
3. Use standard SQL aggregate functions: SUM, COUNT, AVG, MIN, MAX, COUNT(DISTINCT ...)
4. Use DATE_TRUNC for date dimensions with SINGLE-QUOTED string intervals: DATE_TRUNC('MONTH', col), DATE_TRUNC('WEEK', col). NEVER use bare keywords like DATE_TRUNC(MONTH, col) or MONTH(col) -- always quote the interval
5. ALWAYS single-quote ALL string literal values everywhere in expressions:
   - Comparisons: status = 'fulfilled', NOT status = fulfilled
   - THEN/ELSE results: CASE WHEN x = 'A' THEN 'Category A' ELSE 'Other' END, NOT THEN Category A
   - IN lists: department IN ('Surgery', 'Pediatrics'), NOT IN (Surgery, Pediatrics)
   - CONCAT separators: CONCAT(hospital, ' - ', department), NOT CONCAT(hospital, - , department)
   The ONLY unquoted tokens should be column names, SQL keywords, and numbers
6. Include joins ONLY when FK relationships exist in the metadata
7. Every metric view MUST have at least one measure and one dimension
8. Add a top-level "comment" describing the metric view's purpose
9. Add a "comment" to each dimension and measure explaining what it represents
10. Use "filter" (optional) for persistent WHERE clauses (e.g. excluding null/test rows)
11. Use measure-level FILTER for conditional aggregation: SUM(col) FILTER (WHERE condition)
12. If some questions are not answerable with metrics (e.g. document search, free-text lookups, SOP retrieval), generate metric views for the ones that ARE quantitative/analytical and silently ignore the rest
13. Each metric view "name" must be unique and descriptive (e.g. staffing_efficiency_metrics, ed_throughput_analysis). Vary names based on the analytical theme, not just the table name
14. Output ONLY a valid JSON array, no explanation
15. When ONTOLOGY METRIC SUGGESTIONS are provided, use them as the primary source for measure definitions. Translate each suggestion's aggregation_type and source_field into a measure expr. If a suggestion has a filter_condition, use it as a FILTER clause
16. When Entity types are annotated on tables, generate entity-specific analytical metrics:
    - People/patients/users: include counts, return/readmission rates, segmentation dimensions, and per-entity averages
    - Transactions/events/encounters: include volume counts, value sums, time-based rates, and categorical breakdowns
    - Resources/staff/inventory: include utilization rates (active/total), efficiency ratios, and capacity measures
    Always include at least one RATIO measure (x / NULLIF(y, 0)) and one RATE measure (conditional_count * 1.0 / NULLIF(total, 0)) per metric view
17. When FOREIGN KEY RELATIONSHIPS exist between selected tables, generate cross-table metrics that join fact tables to dimension tables. Use dimension table columns as grouping dimensions and fact table columns as measures

EXAMPLE:
{_FEW_SHOT}

CATALOG METADATA:
{context}

BUSINESS QUESTIONS:
{q_block}

OUTPUT (JSON array only):"""


def _parse_ai_json(response: str) -> list[dict]:
    text = response.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        return []
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        results = []
        depth, obj_start = 0, None
        for i, ch in enumerate(text[start : end + 1]):
            if ch == "{":
                if depth == 0:
                    obj_start = i + start
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and obj_start is not None:
                    try:
                        results.append(json.loads(text[obj_start : i + start + 1]))
                    except json.JSONDecodeError:
                        pass
                    obj_start = None
        return results


_SQL_KEYWORDS = {
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "DATE_TRUNC",
    "DISTINCT",
    "MONTH",
    "QUARTER",
    "YEAR",
    "WEEK",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "CAST",
    "AS",
    "STRING",
    "INT",
    "BIGINT",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "DATE",
    "TIMESTAMP",
    "BOOLEAN",
    "COALESCE",
    "IF",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "CONCAT",
    "UPPER",
    "LOWER",
    "TRIM",
    "FILTER",
    "WHERE",
    "BETWEEN",
    "IN",
    "LIKE",
    "IS",
    "FROM",
    "TO",
    "DATEDIFF",
    "TIMESTAMPDIFF",
    "DATE_ADD",
    "DATE_SUB",
    "ADD_MONTHS",
    "ROUND",
    "ABS",
    "CEIL",
    "CEILING",
    "FLOOR",
    "POWER",
    "SQRT",
    "MOD",
    "LENGTH",
    "SUBSTRING",
    "REPLACE",
    "REGEXP_REPLACE",
    "REGEXP_EXTRACT",
    "SPLIT",
    "ARRAY",
    "MAP",
    "STRUCT",
    "NAMED_STRUCT",
    "EXPLODE",
    "COLLECT_LIST",
    "COLLECT_SET",
    "APPROX_COUNT_DISTINCT",
    "PERCENTILE",
    "PERCENTILE_APPROX",
    "STDDEV",
    "VARIANCE",
    "FIRST",
    "LAST",
    "NVL",
    "IFNULL",
    "NULLIF",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "MONTHS_BETWEEN",
    "TO_DATE",
    "TO_TIMESTAMP",
    "DATE_FORMAT",
    "UNIX_TIMESTAMP",
}


def _extract_column_refs(expr: str) -> list[str]:
    """Extract likely column references from a SQL expression."""
    cleaned = re.sub(r"'[^']*'", "", expr)
    cleaned = re.sub(r'"[^"]*"', "", cleaned)
    func_tokens = {
        m.group(1).upper() for m in re.finditer(r"\b([a-zA-Z_]\w*)\s*\(", cleaned)
    }
    tokens = re.findall(r"\b([a-zA-Z_]\w*)\b", cleaned)
    return [
        t
        for t in tokens
        if t.upper() not in _SQL_KEYWORDS and t.upper() not in func_tokens
    ]


def _validate_definition_structure(defn: dict) -> list[str]:
    """Structural validation: check source table exists and columns are valid."""
    errors = []
    source = defn.get("source", "")
    if not source:
        errors.append("Missing source table")
        return errors
    if not defn.get("dimensions") and not defn.get("measures"):
        errors.append("Definition must have at least one dimension or measure")

    parts = source.split(".")
    if len(parts) != 3:
        return errors  # can't verify without fully qualified name

    cat, sch, tbl = parts
    existing_cols: set[str] = set()
    try:
        rows = execute_sql(
            f"SELECT column_name FROM `{cat}`.information_schema.columns "
            f"WHERE table_catalog = '{cat}' AND table_schema = '{sch}' AND table_name = '{tbl}'"
        )
        existing_cols = {r["column_name"].lower() for r in rows}
    except Exception:
        errors.append(f"Could not verify table {source}")
        return errors

    if not existing_cols:
        errors.append(f"Table {source} not found in information_schema")
        return errors

    for j in defn.get("joins", []):
        j_source = j.get("source", "")
        j_parts = j_source.split(".")
        if len(j_parts) == 3:
            try:
                j_rows = execute_sql(
                    f"SELECT column_name FROM `{j_parts[0]}`.information_schema.columns "
                    f"WHERE table_catalog = '{j_parts[0]}' AND table_schema = '{j_parts[1]}' AND table_name = '{j_parts[2]}'"
                )
                existing_cols.update(r["column_name"].lower() for r in j_rows)
            except Exception:
                pass

    for item_type in ("dimensions", "measures"):
        for item in defn.get(item_type, []):
            col_refs = _extract_column_refs(item.get("expr", ""))
            for col in col_refs:
                if col.lower() not in existing_cols:
                    errors.append(
                        f"{item_type} '{item.get('name', '')}': column '{col}' not found in {source}"
                    )

    return errors


_DATE_TRUNC_INTERVALS = {
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
}
_SQL_RESERVED = {
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "CASE",
    "WHEN",
    "IN",
    "IS",
    "LIKE",
    "BETWEEN",
    "SELECT",
    "FROM",
    "WHERE",
    "FILTER",
    "DISTINCT",
    "SUM",
    "AVG",
    "COUNT",
    "MIN",
    "MAX",
    "DATE_TRUNC",
    "IF",
    "COALESCE",
    "NULLIF",
    "OVER",
    "PARTITION",
    "BY",
    "ORDER",
    "ASC",
    "DESC",
}


def _fix_unquoted_literals(expr: str) -> str:
    """Quote bare words used as string literals in comparisons."""

    def _replacer(m):
        op, word, trail = m.group(1), m.group(2), m.group(3)
        if word.upper() in _SQL_RESERVED or word.upper() in _DATE_TRUNC_INTERVALS:
            return m.group(0)
        return f"{op}'{word}'{trail}"

    return re.sub(
        r"([=!<>]+\s*)([A-Za-z_]\w*)(\s*(?:THEN|ELSE|END|AND|OR|WHEN|,|\))|$)",
        _replacer,
        expr,
        flags=re.IGNORECASE,
    )


def _fix_then_else_literals(expr: str) -> str:
    """Quote multi-word bare text after THEN/ELSE keywords."""
    _BOUNDARY = {"WHEN", "THEN", "ELSE", "END", "AND", "OR", "CASE"}

    def _replacer(m):
        kw = m.group(1)
        body = m.group(2).strip()
        words = body.split()
        if len(words) < 2:
            if words and words[0].upper() not in _SQL_RESERVED and words[0].upper() not in _BOUNDARY:
                return f"{kw} '{body}'"
            return m.group(0)
        return f"{kw} '{body}'"

    return re.sub(
        r"\b(THEN|ELSE)\s+((?:[A-Za-z_]\w*\s+)*[A-Za-z_]\w*)(?=\s+(?:WHEN|ELSE|END|AND|OR)\b|\s*$)",
        _replacer,
        expr,
        flags=re.IGNORECASE,
    )


def _fix_in_clause_literals(expr: str) -> str:
    """Quote bare words inside IN (...) clauses."""

    def _fix_in_body(m):
        prefix = m.group(1)
        body = m.group(2)
        tokens = [t.strip() for t in body.split(",")]
        fixed = []
        for tok in tokens:
            if not tok:
                fixed.append(tok)
            elif tok.startswith("'") or tok.startswith('"'):
                fixed.append(tok)
            elif re.match(r"^-?\d+(\.\d+)?$", tok):
                fixed.append(tok)
            elif tok.upper() in _SQL_RESERVED:
                fixed.append(tok)
            else:
                fixed.append(f"'{tok}'")
        return f"{prefix}{', '.join(fixed)})"

    return re.sub(
        r"(\bIN\s*\()([^)]+)\)",
        _fix_in_body,
        expr,
        flags=re.IGNORECASE,
    )


def _fix_concat_separators(expr: str) -> str:
    """Quote bare non-alphanumeric tokens between commas in function calls."""
    return re.sub(
        r",\s*([^\w\s'\"`(][^\w'\"`(]*?)\s*,",
        lambda m: f", '{m.group(1).strip()}',",
        expr,
    )


def _autofix_expr(expr: str) -> str:
    """Fix common AI expression mistakes before validation."""

    # Fix unquoted DATE_TRUNC intervals: DATE_TRUNC(WEEK, col) -> DATE_TRUNC('WEEK', col)
    def _fix_date_trunc(m):
        interval = m.group(1)
        rest = m.group(2)
        if interval.upper() in _DATE_TRUNC_INTERVALS:
            return f"DATE_TRUNC('{interval}'{rest}"
        return m.group(0)

    expr = re.sub(
        r"DATE_TRUNC\(\s*([A-Za-z]+)(,)", _fix_date_trunc, expr, flags=re.IGNORECASE
    )

    # Fix bare interval function calls: WEEK(col) -> DATE_TRUNC('WEEK', col)
    for iv in _DATE_TRUNC_INTERVALS:
        pat = re.compile(rf"\b{iv}\s*\(([^)]+)\)", re.IGNORECASE)
        match = pat.search(expr)
        if match and expr.strip().upper().startswith(iv.upper()):
            expr = pat.sub(rf"DATE_TRUNC('{iv}', \1)", expr)

    # Fix unquoted string literals in comparisons (= value, != value)
    expr = _fix_unquoted_literals(expr)
    # Fix unquoted THEN/ELSE result values
    expr = _fix_then_else_literals(expr)
    # Fix unquoted values in IN (...) clauses
    expr = _fix_in_clause_literals(expr)
    # Fix bare separators in CONCAT-style calls
    expr = _fix_concat_separators(expr)
    return expr


def _validate_expr(expr: str, source_table: str) -> tuple:
    """Test a SQL expression. Returns (error_or_None, possibly_fixed_expr)."""
    try:
        execute_sql(f"SELECT {expr} FROM {source_table} LIMIT 0")
        return None, expr
    except Exception as e:
        err_str = str(e)
        m = re.search(r"UNRESOLVED_COLUMN.*?name `(\w+)`", err_str)
        if m:
            bare = m.group(1)
            fixed = re.sub(
                rf"([=!<>]\s{{0,4}}){re.escape(bare)}(?=[\s),$]|$)",
                rf"\1'{bare}'",
                expr,
            )
            if fixed != expr:
                try:
                    execute_sql(f"SELECT {fixed} FROM {source_table} LIMIT 0")
                    return None, fixed
                except Exception:
                    pass
        return err_str, expr


def _score_definition_complexity(defn: dict) -> dict:
    """Score a metric view definition's analytical richness.

    Returns {"complexity_score": int, "complexity_level": str}.
    """
    score = 0
    _COMPUTED_DIM_PATTERNS = re.compile(r"CASE\b|DATE_TRUNC\b|CONCAT\b", re.IGNORECASE)
    for dim in defn.get("dimensions", []):
        if _COMPUTED_DIM_PATTERNS.search(dim.get("expr", "")):
            score += 1
    for meas in defn.get("measures", []):
        expr = meas.get("expr", "")
        if re.search(r"/\s*NULLIF\b", expr, re.IGNORECASE):
            score += 2
        elif re.search(r"FILTER\b|DISTINCT\b|CASE\b", expr, re.IGNORECASE):
            score += 1
    if defn.get("joins"):
        score += 2
    if defn.get("filter"):
        score += 1
    if score < 3:
        level = "trivial"
    elif score <= 5:
        level = "moderate"
    else:
        level = "rich"
    return {"complexity_score": score, "complexity_level": level}


def _run_sl_generation(
    task_id: str,
    tables: list[str],
    questions: list[str],
    cat: str,
    sch: str,
    model: str,
    project_id: str = None,
    mode: str = "replace",
):
    """Background thread for in-app metric view generation."""
    from datetime import datetime as _dt

    task = _sl_tasks[task_id]
    try:
        _ensure_semantic_layer_tables()

        # Save selected tables to project
        if project_id and tables:
            tables_json = json.dumps(tables).replace("'", "''")
            try:
                execute_sql(
                    f"UPDATE {fq('semantic_layer_projects')} SET selected_tables = '{tables_json}' "
                    f"WHERE project_id = '{project_id}'"
                )
            except Exception:
                pass

        # replace_all: supersede ALL non-superseded definitions in this project first
        if mode == "replace_all" and project_id:
            try:
                execute_sql(
                    f"UPDATE {fq('metric_view_definitions')} SET status = 'superseded' "
                    f"WHERE project_id = '{project_id}' AND status != 'superseded'"
                )
            except Exception:
                pass

        task["stage"] = "building_context"
        context = _build_sl_context(tables, cat, sch)
        if not context.strip():
            task.update(
                {
                    "status": "error",
                    "error": "No metadata found for selected tables. Run metadata generation first.",
                }
            )
            return

        task["stage"] = "calling_ai"
        prompt = _build_prompt(questions, context)
        escaped = prompt.replace("'", "''")
        rows = execute_sql(
            f"SELECT AI_QUERY('{model}', '{escaped}') as response", timeout=180
        )
        response = rows[0]["response"] if rows else ""
        logger.info(
            "AI response (first 500 chars): %s",
            response[:500] if response else "<empty>",
        )
        definitions = _parse_ai_json(response)
        if not definitions:
            snippet = (response[:200] + "...") if len(response) > 200 else response
            task.update(
                {
                    "status": "error",
                    "error": f"AI returned no valid metric view definitions. Response preview: {snippet}",
                }
            )
            return

        task.update({"stage": "validating", "generated": len(definitions)})
        now = _dt.utcnow().isoformat()
        stats = {"generated": 0, "validated": 0, "failed": 0}

        for defn in definitions:
            defn_id = str(_uuid.uuid4())
            mv_name = defn.get("name", f"metric_view_{defn_id[:8]}")
            source = defn.get("source", "")

            # Auto-fix common AI expression mistakes before validation
            for item_type in ("dimensions", "measures"):
                for item in defn.get(item_type, []):
                    if item.get("expr"):
                        item["expr"] = _autofix_expr(item["expr"])
            if defn.get("filter"):
                defn["filter"] = _autofix_expr(defn["filter"])

            json_str = json.dumps(defn).replace("'", "''")

            # In "replace" mode, supersede existing definitions with the same name
            if mode == "replace":
                mv_esc = mv_name.replace("'", "''")
                proj_clause = f" AND project_id = '{project_id}'" if project_id else ""
                try:
                    execute_sql(
                        f"UPDATE {fq('metric_view_definitions')} SET status = 'superseded' "
                        f"WHERE metric_view_name = '{mv_esc}' "
                        f"AND status != 'superseded'{proj_clause}"
                    )
                except Exception:
                    pass

            errors = _validate_definition_structure(defn)
            if not errors:
                for item_type in ("dimensions", "measures"):
                    for item in defn.get(item_type, []):
                        expr = item.get("expr", "")
                        if source and expr:
                            err, fixed = _validate_expr(expr, source)
                            if err:
                                errors.append(
                                    f"{item_type} '{item.get('name', '')}': {err}"
                                )
                            elif fixed != expr:
                                item["expr"] = fixed

            # Re-serialize after possible expression fixes
            json_str = json.dumps(defn).replace("'", "''")
            status = "validated" if not errors else "failed"
            error_str = "; ".join(errors).replace("'", "''") if errors else ""
            proj_val = f"'{project_id}'" if project_id else "NULL"
            cx = _score_definition_complexity(defn)
            execute_sql(
                f"INSERT INTO {fq('metric_view_definitions')} VALUES "
                f"('{defn_id}', '{mv_name}', '{source}', '{json_str}', '', "
                f"'{status}', '{error_str}', NULL, '{now}', NULL, 1, NULL, {proj_val}, "
                f"{cx['complexity_score']}, '{cx['complexity_level']}')"
            )
            stats[status] += 1
            stats["generated"] += 1

        task.update({"status": "done", "stage": "done", "result": stats})

    except Exception as e:
        logger.error("Semantic layer generation error: %s", e, exc_info=True)
        task.update({"status": "error", "error": str(e)})


@app.post("/api/semantic-layer/generate")
def start_sl_generation(req: SemanticGenerateRequest):
    """Start in-app metric view generation as a background task."""
    wh = os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        raise HTTPException(500, detail="WAREHOUSE_ID not configured")
    if not req.tables:
        raise HTTPException(400, detail="No tables selected")
    if not req.questions:
        raise HTTPException(400, detail="No questions provided")

    cat = req.catalog_name or CATALOG
    sch = req.schema_name or SCHEMA
    task_id = str(_uuid.uuid4())[:12]
    _sl_tasks[task_id] = {
        "status": "running",
        "stage": "starting",
        "created": time.time(),
    }

    threading.Thread(
        target=_run_sl_generation,
        args=(
            task_id,
            req.tables,
            req.questions,
            cat,
            sch,
            req.model_endpoint,
            req.project_id,
            req.mode,
        ),
        daemon=True,
    ).start()

    cutoff = time.time() - 1800
    for tid in list(_sl_tasks):
        if _sl_tasks.get(tid, {}).get("created", 0) < cutoff:
            _sl_tasks.pop(tid, None)

    return {"task_id": task_id}


@app.get("/api/semantic-layer/generate/{task_id}")
def poll_sl_generation(task_id: str):
    task = _sl_tasks.get(task_id)
    if not task:
        raise HTTPException(404, detail="Task not found")
    return task


# ---------------------------------------------------------------------------
# Metric-view per-definition actions (retry / improve / create)
# ---------------------------------------------------------------------------

_DEFAULT_MODEL = "databricks-claude-sonnet-4-6"


def _fetch_definition(definition_id: str) -> dict:
    """Load a single metric_view_definitions row by ID."""
    rows = execute_sql(
        f"SELECT * FROM {fq('metric_view_definitions')} "
        f"WHERE definition_id = '{definition_id}'"
    )
    if not rows:
        raise HTTPException(404, detail="Definition not found")
    return rows[0]


def _parse_single_json(text: str) -> dict:
    """Extract a single JSON object from an AI response."""
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("No JSON object found in AI response")
    return json.loads(text[start : end + 1])


def _validate_definition(defn: dict, source: str) -> tuple[str, str]:
    """Two-tier validation: structural then expression. Returns (status, errors_str)."""
    errors = _validate_definition_structure(defn)
    if not errors:
        for item_type in ("dimensions", "measures"):
            for item in defn.get(item_type, []):
                expr = item.get("expr", "")
                if source and expr:
                    err, fixed = _validate_expr(expr, source)
                    if err:
                        errors.append(f"{item_type} {item.get('name', '')}: {err}")
                    elif fixed != expr:
                        item["expr"] = fixed
    status = "validated" if not errors else "failed"
    return status, "; ".join(errors)


def _update_definition_row(definition_id: str, defn: dict, status: str, errors: str):
    """Create a new version row and mark the old one as superseded."""
    from datetime import datetime as _dt

    json_str = json.dumps(defn).replace("'", "''")
    error_esc = errors.replace("'", "''")

    rows = execute_sql(
        f"SELECT version, metric_view_name, source_table, source_questions, project_id "
        f"FROM {fq('metric_view_definitions')} WHERE definition_id = '{definition_id}'"
    )
    old_version = int(rows[0].get("version") or 1) if rows else 1
    mv_name = rows[0].get("metric_view_name", "") if rows else ""
    source_table = rows[0].get("source_table", defn.get("source", "")) if rows else ""
    source_qs = rows[0].get("source_questions", "") if rows else ""
    proj_id = rows[0].get("project_id") if rows else None

    execute_sql(
        f"UPDATE {fq('metric_view_definitions')} SET status = 'superseded' "
        f"WHERE definition_id = '{definition_id}'"
    )

    new_id = str(_uuid.uuid4())
    new_version = old_version + 1
    now = _dt.utcnow().isoformat()
    proj_val = f"'{proj_id}'" if proj_id else "NULL"
    execute_sql(
        f"INSERT INTO {fq('metric_view_definitions')} VALUES ("
        f"'{new_id}', '{mv_name}', '{source_table}', '{json_str}', '{source_qs}', "
        f"'{status}', '{error_esc}', NULL, '{now}', NULL, {new_version}, '{definition_id}', {proj_val})"
    )
    return new_id


def _yaml_esc(s: str) -> str:
    """Escape a string for embedding inside YAML double-quotes."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _definition_to_yaml(defn: dict) -> str:
    """Convert a JSON definition to the YAML body for CREATE VIEW WITH METRICS.

    Uses manual construction instead of yaml.dump to guarantee SQL single
    quotes inside CASE/FILTER expressions survive the round-trip.
    """
    lines = ['version: "1.1"', f'source: {defn["source"]}']
    if defn.get("comment"):
        lines.append(f'comment: "{_yaml_esc(defn["comment"])}"')
    if defn.get("filter"):
        lines.append(f'filter: "{_yaml_esc(defn["filter"])}"')
    lines.append("dimensions:")
    for d in defn.get("dimensions", []):
        lines.append(f'  - name: "{_yaml_esc(d["name"])}"')
        lines.append(f'    expr: "{_yaml_esc(d["expr"])}"')
        if d.get("comment"):
            lines.append(f'    comment: "{_yaml_esc(d["comment"])}"')
    lines.append("measures:")
    for m in defn.get("measures", []):
        lines.append(f'  - name: "{_yaml_esc(m["name"])}"')
        lines.append(f'    expr: "{_yaml_esc(m["expr"])}"')
        if m.get("comment"):
            lines.append(f'    comment: "{_yaml_esc(m["comment"])}"')
    if defn.get("joins"):
        lines.append("joins:")
        for j in defn["joins"]:
            lines.append(f'  - name: "{_yaml_esc(j.get("name", ""))}"')
            lines.append(f'    source: {j.get("source", "")}')
            if j.get("on"):
                lines.append(f'    on: "{_yaml_esc(j["on"])}"')
    return "\n".join(lines) + "\n"


@app.post("/api/semantic-layer/definitions/{definition_id}/retry")
def retry_definition(definition_id: str):
    """Re-attempt a failed definition by asking AI to fix validation errors."""
    row = _fetch_definition(definition_id)
    version = int(row.get("version") or 1)
    if version >= 3:
        raise HTTPException(
            409,
            detail=f"Max retries reached (v{version}). Consider re-generating with different questions or tables.",
        )

    defn = (
        json.loads(row["json_definition"])
        if isinstance(row["json_definition"], str)
        else row["json_definition"]
    )
    source = defn.get("source", row.get("source_table", ""))
    validation_errors = row.get("validation_errors", "")

    # Fetch actual available columns so the AI knows what exists
    available_cols = ""
    parts = source.split(".")
    if len(parts) == 3:
        try:
            col_rows = execute_sql(
                f"SELECT column_name, data_type FROM `{parts[0]}`.information_schema.columns "
                f"WHERE table_catalog = '{parts[0]}' AND table_schema = '{parts[1]}' AND table_name = '{parts[2]}'"
            )
            available_cols = ", ".join(
                f"{r['column_name']} ({r.get('data_type', '')})" for r in col_rows
            )
        except Exception:
            pass

    context = _build_sl_context([source], CATALOG, SCHEMA)
    prompt = f"""You are fixing a metric view definition that has SQL errors.

ORIGINAL DEFINITION:
{json.dumps(defn, indent=2)}

ERRORS:
{validation_errors}

AVAILABLE COLUMNS in {source}:
{available_cols}

TABLE METADATA:
{context}

Fix the SQL expressions. Rules:
- Only reference columns listed in AVAILABLE COLUMNS above
- Use standard Spark SQL functions (DATEDIFF, ROUND, ABS, etc.) -- these are fine as functions but are NOT columns
- DATE_TRUNC requires a quoted interval: DATE_TRUNC('MONTH', col) not DATE_TRUNC(MONTH, col)
- If a needed column doesn't exist, remove that measure/dimension rather than inventing columns
- Every metric view must keep at least one measure and one dimension

OUTPUT: Return ONLY the corrected JSON definition (single object, not array)."""

    escaped = prompt.replace("'", "''")
    rows = execute_sql(
        f"SELECT AI_QUERY('{_DEFAULT_MODEL}', '{escaped}') as response", timeout=180
    )
    response = rows[0]["response"] if rows else ""
    logger.info(
        "Retry AI response (first 500 chars): %s",
        response[:500] if response else "<empty>",
    )

    new_defn = _parse_single_json(response)
    new_defn.setdefault("source", source)
    new_defn.setdefault("name", defn.get("name", ""))
    status, errs = _validate_definition(new_defn, new_defn.get("source", source))
    new_id = _update_definition_row(definition_id, new_defn, status, errs)

    return {
        "definition_id": new_id,
        "parent_id": definition_id,
        "status": status,
        "validation_errors": errs,
    }


@app.post("/api/semantic-layer/definitions/{definition_id}/improve")
def improve_definition(definition_id: str):
    """Enhance a validated definition to be more sophisticated."""
    row = _fetch_definition(definition_id)
    defn = (
        json.loads(row["json_definition"])
        if isinstance(row["json_definition"], str)
        else row["json_definition"]
    )
    source = defn.get("source", row.get("source_table", ""))

    context = _build_sl_context([source], CATALOG, SCHEMA)
    cx = _score_definition_complexity(defn)
    trivial_nudge = ""
    if cx["complexity_level"] == "trivial":
        trivial_nudge = """
CRITICAL: This definition is TRIVIAL (score {score}). You MUST significantly increase analytical depth:
- Add at least one RATIO measure: e.g. SUM(x) / NULLIF(COUNT(DISTINCT y), 0)
- Add at least one RATE measure: e.g. SUM(CASE WHEN cond THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)
- Add a FILTER measure: e.g. COUNT(*) FILTER (WHERE threshold_col > value)
- Add a COUNT DISTINCT measure
- Add at least one multi-tier CASE WHEN dimension
""".format(score=cx["complexity_score"])
    prompt = f"""You are improving a validated metric view definition to make it more sophisticated.

CURRENT DEFINITION:
{json.dumps(defn, indent=2)}

TABLE METADATA:
{context}
{trivial_nudge}
Improve this definition:
- Add more useful dimensions (date truncations at multiple granularities, categorizations)
- Add derived measures (ratios, running totals, averages alongside sums)
- Add meaningful comments to all dimensions and measures
- Use "filter" for persistent WHERE clauses and FILTER (WHERE ...) for conditional measures
- Add joins if FK relationships exist in the metadata
- Keep all existing valid dimensions and measures
- ALL string literals in SQL expressions MUST be wrapped in single quotes

OUTPUT: Return ONLY the improved JSON definition (single object, not array)."""

    escaped = prompt.replace("'", "''")
    rows = execute_sql(
        f"SELECT AI_QUERY('{_DEFAULT_MODEL}', '{escaped}') as response", timeout=180
    )
    response = rows[0]["response"] if rows else ""
    logger.info(
        "Improve AI response (first 500 chars): %s",
        response[:500] if response else "<empty>",
    )

    new_defn = _parse_single_json(response)
    new_defn.setdefault("source", source)
    new_defn.setdefault("name", defn.get("name", ""))
    status, errs = _validate_definition(new_defn, new_defn.get("source", source))
    new_id = _update_definition_row(definition_id, new_defn, status, errs)

    return {
        "definition_id": new_id,
        "parent_id": definition_id,
        "status": status,
        "validation_errors": errs,
    }


@app.post("/api/semantic-layer/definitions/{definition_id}/create")
def create_metric_view(definition_id: str, req: MetricViewCreateRequest):
    """Materialize a metric view definition as a UC METRIC VIEW via DDL."""
    row = _fetch_definition(definition_id)
    defn = (
        json.loads(row["json_definition"])
        if isinstance(row["json_definition"], str)
        else row["json_definition"]
    )

    # Auto-fix expressions before building YAML
    for item_type in ("dimensions", "measures"):
        for item in defn.get(item_type, []):
            if item.get("expr"):
                item["expr"] = _autofix_expr(item["expr"])
    if defn.get("filter"):
        defn["filter"] = _autofix_expr(defn["filter"])

    # Pre-validate each expression against the source table (auto-fix on retry)
    source = defn.get("source", "")
    pre_errors = []
    if source:
        for item_type in ("dimensions", "measures"):
            for item in defn.get(item_type, []):
                expr = item.get("expr", "")
                if expr:
                    err, fixed = _validate_expr(expr, source)
                    if err:
                        pre_errors.append(f"{item_type} {item.get('name', '')}: {err}")
                    elif fixed != expr:
                        item["expr"] = fixed
    if pre_errors:
        raise HTTPException(400, detail="; ".join(pre_errors))

    mv_name = defn.get("name", row.get("metric_view_name", f"mv_{definition_id[:8]}"))
    yaml_body = _definition_to_yaml(defn)
    fq_name = f"`{req.target_catalog}`.`{req.target_schema}`.`{mv_name}`"
    ddl = f"CREATE OR REPLACE VIEW {fq_name} WITH METRICS LANGUAGE YAML AS $$\n{yaml_body}$$"

    try:
        execute_sql(ddl)
    except Exception as e:
        raise HTTPException(400, detail=f"DDL execution failed: {e}")

    from datetime import datetime as _dt

    now = _dt.utcnow().isoformat()
    execute_sql(
        f"UPDATE {fq('metric_view_definitions')} SET "
        f"status = 'applied', applied_at = '{now}' "
        f"WHERE definition_id = '{definition_id}'"
    )
    return {"definition_id": definition_id, "status": "applied", "metric_view": fq_name}


# ---------------------------------------------------------------------------
# Genie Builder endpoints
# ---------------------------------------------------------------------------


@app.post("/api/genie/generate")
def genie_generate(req: GenieGenerateRequest):
    """Start the Genie builder agent as a background task."""
    wh = os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        raise HTTPException(500, detail="WAREHOUSE_ID not configured")

    task_id = str(_uuid.uuid4())[:12]
    _genie_tasks[task_id] = {
        "status": "running",
        "stage": "starting",
        "created": time.time(),
    }

    def _run():
        try:
            from agent.genie_builder import GenieContextAssembler
            from agent.genie_agent import run_genie_agent

            ws = get_workspace_client()
            progress_q: queue.Queue = queue.Queue()

            # Phase 1: deterministic context assembly
            _genie_tasks[task_id]["stage"] = "gathering_context"
            assembler = GenieContextAssembler(ws, wh, CATALOG, SCHEMA)
            ctx = assembler.assemble(req.table_identifiers, req.questions or None)

            # Phase 2: agent generation
            _genie_tasks[task_id]["stage"] = "agent_running"

            def _monitor_progress():
                while True:
                    try:
                        event = progress_q.get(timeout=600)
                    except queue.Empty:
                        _genie_tasks[task_id].update(
                            {
                                "status": "error",
                                "error": "Agent timed out after 10 minutes",
                            }
                        )
                        return
                    if event.get("stage") == "done":
                        _genie_tasks[task_id].update(
                            {
                                "status": "done",
                                "stage": "done",
                                "result": event.get("result"),
                            }
                        )
                        return
                    if event.get("stage") == "error":
                        _genie_tasks[task_id].update(
                            {
                                "status": "error",
                                "error": event.get("message", "Unknown error"),
                            }
                        )
                        return
                    _genie_tasks[task_id]["stage"] = event.get("stage", "running")

            monitor = threading.Thread(target=_monitor_progress, daemon=True)
            monitor.start()

            run_genie_agent(ws, wh, ctx, progress_q, model_endpoint=req.model_endpoint)
        except Exception as e:
            logger.error("Genie builder error: %s", e, exc_info=True)
            _genie_tasks[task_id].update({"status": "error", "error": str(e)})

    threading.Thread(target=_run, daemon=True).start()

    # Clean up old tasks (> 30 min)
    cutoff = time.time() - 1800
    for tid in list(_genie_tasks):
        if _genie_tasks.get(tid, {}).get("created", 0) < cutoff:
            _genie_tasks.pop(tid, None)

    return {"task_id": task_id}


@app.get("/api/genie/tasks/{task_id}")
def genie_task_status(task_id: str):
    """Poll status of a Genie builder task."""
    task = _genie_tasks.get(task_id)
    if not task:
        raise HTTPException(404, detail="Task not found")
    return task


from genie_schema import build_serialized_space


def _transform_to_genie_schema(raw: dict) -> dict:
    """Convert agent output into the Databricks Genie API format via Pydantic whitelist."""
    return build_serialized_space(raw)


def _strip_field(obj, field_name):
    """Recursively remove a field from all dicts in the structure."""
    if isinstance(obj, dict):
        obj.pop(field_name, None)
        for v in obj.values():
            _strip_field(v, field_name)
    elif isinstance(obj, list):
        for item in obj:
            _strip_field(item, field_name)


def _validate_serialized_space(ss: dict) -> list[str]:
    """Validate the transformed serialized_space before sending to Genie API."""
    errors = []
    if "data_sources" not in ss:
        errors.append("Missing required 'data_sources' section")
    else:
        ds = ss["data_sources"]
        if not isinstance(ds, dict):
            errors.append("'data_sources' must be a dict")
        elif not ds.get("tables") and not ds.get("metric_views"):
            errors.append("'data_sources' must have at least one table or metric_view")
        for i, tbl in enumerate(ds.get("tables", [])):
            if not tbl.get("identifier"):
                errors.append(f"data_sources.tables[{i}] missing 'identifier'")
    inst = ss.get("instructions", {})
    if not isinstance(inst, dict):
        errors.append("'instructions' must be a dict")
    return errors


@app.post("/api/genie/create")
def genie_create(req: GenieCreateRequest):
    """Create or update a Genie space via the Databricks REST API."""
    transformed = _transform_to_genie_schema(req.serialized_space)
    validation_errors = _validate_serialized_space(transformed)
    if validation_errors:
        raise HTTPException(
            400, detail=f"Invalid serialized_space: {'; '.join(validation_errors)}"
        )

    ws = get_workspace_client()
    wh = req.warehouse_id or os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        raise HTTPException(500, detail="WAREHOUSE_ID not configured")

    def _do_genie_request(space_json):
        body = {
            "title": req.title,
            "warehouse_id": wh,
            "serialized_space": json.dumps(space_json),
        }
        if req.space_id:
            ws.api_client.do(
                "PATCH", f"/api/2.0/genie/spaces/{req.space_id}", body=body
            )
            return {"space_id": req.space_id, "title": req.title, "updated": True}
        else:
            resp = ws.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
            return {
                "space_id": resp.get("space_id", resp.get("id")),
                "title": req.title,
                "updated": False,
            }

    _MAX_GENIE_RETRIES = 5
    last_err: Exception | None = None
    for attempt in range(_MAX_GENIE_RETRIES + 1):
        try:
            return _do_genie_request(transformed)
        except Exception as e:
            last_err = e
            m = re.search(r"Cannot find field: (\w+)", str(e))
            if m:
                bad_field = m.group(1)
                logger.warning(
                    "Attempt %d: stripping unknown field '%s'", attempt + 1, bad_field
                )
                _strip_field(transformed, bad_field)
                continue
            break
    logger.error("Genie create/update failed: %s", last_err)
    raise HTTPException(500, detail=f"Failed to create/update Genie space: {last_err}")


# ---------------------------------------------------------------------------
# Serve React static files (production build)
# ---------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "src", "dist")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

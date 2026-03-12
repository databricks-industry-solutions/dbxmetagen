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
from typing import Optional, Union
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


# Safe for use in LIKE/WHERE: alphanumeric, underscore, dot, hyphen, space, %
_SAFE_IDENT_RE = re.compile(r"^[a-zA-Z0-9_.\- %]*$")


def _safe_sql_str(s: Optional[str]) -> str:
    """Escape single quotes for SQL string literal."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


_labeled_table_ensured = False
_review_column_ensured: set[str] = set()


def _ensure_labeled_updates_table():
    global _labeled_table_ensured
    if _labeled_table_ensured:
        return
    execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fq('metadata_labeled_updates')} (
            update_id STRING NOT NULL,
            source_kb STRING NOT NULL,
            entity_identifier STRING NOT NULL,
            field_name STRING NOT NULL,
            old_value STRING,
            new_value STRING,
            updated_at TIMESTAMP,
            updated_by STRING
        ) COMMENT 'History of human corrections from metadata review app'
        """
    )
    _labeled_table_ensured = True


def _ensure_review_updated_at(table_key: str):
    global _review_column_ensured
    if table_key in _review_column_ensured:
        return
    try:
        execute_sql(f"ALTER TABLE {fq(table_key)} ADD COLUMN review_updated_at TIMESTAMP")
    except Exception:
        pass
    _review_column_ensured.add(table_key)


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
    ontology_bundle: Optional[str] = None
    domain_config: Optional[str] = None
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
    metric_view_names: list[str] = []
    model_endpoint: str = "databricks-claude-3-7-sonnet"


class SuggestQuestionsRequest(BaseModel):
    table_identifiers: list[str]
    metric_view_names: list[str] = []
    model_endpoint: str = "databricks-claude-3-7-sonnet"
    count: int = 8
    purpose: str = "genie"  # "genie" or "metric_views"


class GenieCreateRequest(BaseModel):
    title: str
    description: Optional[str] = None
    serialized_space: dict
    warehouse_id: Optional[str] = None
    space_id: Optional[str] = None  # if provided, update instead of create


# ---------------------------------------------------------------------------
# Jobs endpoints
# ---------------------------------------------------------------------------


def _list_dbxmetagen_jobs(ws):
    """Return project jobs using known IDs (env var), falling back to list()."""
    if _KNOWN_JOB_IDS:
        jobs = []
        for name, job_id in _KNOWN_JOB_IDS.items():
            try:
                j = ws.jobs.get(job_id)
                jobs.append(j)
            except Exception as e:
                logger.warning("ws.jobs.get(%s=%d) failed: %s", name, job_id, e)
        logger.info(
            "Job discovery via valueFrom: %d/%d reachable",
            len(jobs),
            len(_KNOWN_JOB_IDS),
        )
        return jobs

    logger.info("No job IDs via valueFrom; falling back to ws.jobs.list()")
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
        if j.settings
        and j.settings.name
        and any(kw in j.settings.name.lower() for kw in _JOB_NAME_KEYWORDS)
    ]
    logger.info(
        "Job discovery via list(): %d total, %d matched", len(all_jobs), len(matched)
    )
    return matched


# ---------------------------------------------------------------------------
# Job configuration -- IDs injected via app.yaml valueFrom references
# ---------------------------------------------------------------------------

_JOB_ENV_MAP = {
    "metadata_generator": "METADATA_GENERATOR_JOB_ID",
    "metadata_parallel_modes": "METADATA_PARALLEL_MODES_JOB_ID",
    "sync_ddl": "SYNC_DDL_JOB_ID",
    "full_analytics_pipeline": "FULL_ANALYTICS_PIPELINE_JOB_ID",
    "fk_prediction": "FK_PREDICTION_JOB_ID",
    "sync_graph_lakebase": "SYNC_GRAPH_LAKEBASE_JOB_ID",
    "ontology_prediction": "ONTOLOGY_PREDICTION_JOB_ID",
    "knowledge_base_builder": "KNOWLEDGE_BASE_BUILDER_JOB_ID",
    "profiling": "PROFILING_JOB_ID",
    "metagen_with_kb": "METAGEN_WITH_KB_JOB_ID",
    "semantic_layer": "SEMANTIC_LAYER_JOB_ID",
}

_KNOWN_JOB_IDS: dict[str, int] = {}
for _name, _env_var in _JOB_ENV_MAP.items():
    _val = os.environ.get(_env_var)
    if _val:
        try:
            _KNOWN_JOB_IDS[_name] = int(_val)
        except ValueError:
            logger.warning("Invalid %s value: %s", _env_var, _val)

if _KNOWN_JOB_IDS:
    logger.info(
        "Loaded %d job IDs via valueFrom: %s",
        len(_KNOWN_JOB_IDS),
        list(_KNOWN_JOB_IDS.keys()),
    )
else:
    logger.warning("No job IDs found in env vars; will fall back to ws.jobs.list()")


@app.get("/api/jobs")
def list_jobs():
    """List dbxmetagen jobs visible to the app."""
    ws = get_workspace_client()
    jobs = _list_dbxmetagen_jobs(ws)
    return [{"job_id": j.job_id, "name": j.settings.name} for j in jobs]


@app.post("/api/jobs/run")
def run_job(req: JobRunRequest):
    """Trigger a dbxmetagen job by job_id (preferred) or job_name suffix match."""
    logger.info("run_job request: job_id=%s, job_name=%s", req.job_id, req.job_name)
    ws = get_workspace_client()
    if req.job_id:
        target_job_id = req.job_id
    elif req.job_name:
        # Prefer direct lookup in known job IDs (exact or substring match)
        target_job_id = None
        if _KNOWN_JOB_IDS:
            if req.job_name in _KNOWN_JOB_IDS:
                target_job_id = _KNOWN_JOB_IDS[req.job_name]
            else:
                for name, jid in _KNOWN_JOB_IDS.items():
                    if req.job_name in name or name in req.job_name:
                        target_job_id = jid
                        break
            if target_job_id:
                logger.info(
                    "Resolved job_name '%s' via known IDs -> %d",
                    req.job_name,
                    target_job_id,
                )

        if not target_job_id:
            all_jobs = _list_dbxmetagen_jobs(ws)
            matching = [
                j
                for j in all_jobs
                if j.settings
                and j.settings.name
                and j.settings.name.endswith(req.job_name)
            ]
            if not matching:
                matching = [
                    j
                    for j in all_jobs
                    if j.settings
                    and j.settings.name
                    and req.job_name in j.settings.name
                ]
            if matching:
                target_job_id = matching[0].job_id
            else:
                available = [j.settings.name for j in all_jobs if j.settings]
                raise HTTPException(
                    404,
                    detail=f"Job '{req.job_name}' not found. "
                    f"Run 'databricks bundle deploy' to create jobs, then restart the app. "
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
    if req.ontology_bundle:
        params["ontology_bundle"] = req.ontology_bundle
    if req.domain_config:
        params["domain_config_path"] = _resolve_domain_config_path(req.domain_config)
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
    """Get status of a job run with task-level detail."""
    ws = get_workspace_client()
    try:
        run = ws.jobs.get_run(run_id=run_id)
    except Exception as e:
        logger.error("get_run(run_id=%s) failed: %s", run_id, e)
        raise HTTPException(502, detail=f"Failed to fetch run {run_id}: {e}")

    tasks = []
    for t in run.tasks or []:
        ts = t.state if t else None
        tasks.append(
            {
                "task_key": t.task_key,
                "state": (
                    ts.life_cycle_state.value
                    if ts and ts.life_cycle_state
                    else "UNKNOWN"
                ),
                "result": ts.result_state.value if ts and ts.result_state else None,
            }
        )

    return {
        "run_id": run.run_id,
        "state": run.state.life_cycle_state.value if run.state else "UNKNOWN",
        "result": (
            run.state.result_state.value
            if run.state and run.state.result_state
            else None
        ),
        "state_message": (
            getattr(run.state, "state_message", None) if run.state else None
        ),
        "run_page_url": getattr(run, "run_page_url", None),
        "start_time": getattr(run, "start_time", None),
        "end_time": getattr(run, "end_time", None),
        "tasks": tasks,
    }


@app.get("/api/jobs/runs")
def list_recent_runs(limit: int = 50):
    """Return recent runs across all dbxmetagen jobs."""
    ws = get_workspace_client()
    try:
        dbx_jobs = _list_dbxmetagen_jobs(ws)
    except HTTPException:
        return []
    job_name_map = {j.job_id: j.settings.name for j in dbx_jobs if j.settings}
    runs = []
    for j in dbx_jobs:
        try:
            for r in ws.jobs.list_runs(job_id=j.job_id, limit=10):
                st = r.state if r else None
                runs.append(
                    {
                        "run_id": r.run_id,
                        "job_id": j.job_id,
                        "job_name": job_name_map.get(j.job_id, ""),
                        "state": (
                            st.life_cycle_state.value
                            if st and st.life_cycle_state
                            else "UNKNOWN"
                        ),
                        "result": (
                            st.result_state.value if st and st.result_state else None
                        ),
                        "state_message": (
                            getattr(st, "state_message", None) if st else None
                        ),
                        "start_time": getattr(r, "start_time", None),
                        "run_page_url": getattr(r, "run_page_url", None),
                    }
                )
        except Exception as e:
            logger.warning("list_runs(job_id=%s) failed: %s", j.job_id, e)
    runs.sort(key=lambda r: r.get("start_time") or 0, reverse=True)
    return runs[:limit]


_JOB_NAME_KEYWORDS = {
    "metadata",
    "metagen",
    "dbxmetagen",
    "profiling",
    "ontology",
    "semantic",
    "fk_prediction",
    "sync",
}


@app.get("/api/jobs/health")
def jobs_health_check():
    """Diagnostic preflight: check SPN connectivity and job visibility."""
    report = {
        "known_job_ids_configured": len(_KNOWN_JOB_IDS),
        "jobs_reachable": {},
        "project_jobs_found": 0,
        "project_job_names": [],
        "errors": [],
    }
    ws = get_workspace_client()

    if _KNOWN_JOB_IDS:
        reachable = {}
        for name, job_id in _KNOWN_JOB_IDS.items():
            try:
                j = ws.jobs.get(job_id)
                reachable[name] = {
                    "id": job_id,
                    "status": "ok",
                    "job_name": j.settings.name if j.settings else None,
                }
                report["project_job_names"].append(
                    j.settings.name if j.settings else f"id:{job_id}"
                )
            except Exception as e:
                reachable[name] = {"id": job_id, "status": "error", "error": str(e)}
                report["errors"].append(f"Job '{name}' (id={job_id}) unreachable: {e}")
        report["jobs_reachable"] = reachable
        report["project_jobs_found"] = sum(
            1 for v in reachable.values() if v["status"] == "ok"
        )
    else:
        report["errors"].append(
            "No job IDs found. Ensure app.yaml has valueFrom entries for each job resource "
            "and dbxmetagen_app.yml declares matching resources, "
            "then run 'databricks bundle deploy' and restart the app."
        )

    return report


# ---------------------------------------------------------------------------
# Metadata endpoints
# ---------------------------------------------------------------------------


@app.get("/api/metadata/log")
def get_metadata_log(limit: int = 100, table_name: Optional[str] = None):
    """Query metadata_generation_log."""
    where = f"WHERE table_name LIKE '%{table_name}%'" if table_name else ""
    q = f"SELECT * FROM {fq('metadata_generation_log')} {where} ORDER BY _created_at DESC LIMIT {limit}"
    return execute_sql(q)


def _validate_filter(val: Optional[str], param: str) -> None:
    if val is None or val == "":
        return
    if not _SAFE_IDENT_RE.match(val):
        raise HTTPException(400, f"Invalid {param}: only alphanumeric, underscore, dot, hyphen, space allowed")


@app.get("/api/metadata/knowledge-base")
def get_knowledge_base(
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    limit: int = 100,
):
    _validate_filter(table_name, "table_name")
    _validate_filter(schema_name, "schema_name")
    clauses = []
    if table_name:
        clauses.append(f"table_name LIKE '%{table_name}%'")
    if schema_name:
        clauses.append(f"`schema` = {_safe_sql_str(schema_name)}")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    q = f"SELECT * FROM {fq('table_knowledge_base')} {where} ORDER BY table_name LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/metadata/column-kb")
def get_column_kb(
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
    limit: int = 200,
):
    _validate_filter(table_name, "table_name")
    _validate_filter(column_name, "column_name")
    clauses = []
    if table_name:
        clauses.append(f"table_name LIKE '%{table_name}%'")
    if column_name:
        clauses.append(f"column_name LIKE '%{column_name}%'")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    q = f"SELECT * FROM {fq('column_knowledge_base')} {where} ORDER BY table_name, column_name LIMIT {limit}"
    return execute_sql(q)


@app.get("/api/metadata/schema-kb")
def get_schema_kb(schema_name: Optional[str] = None):
    _validate_filter(schema_name, "schema_name")
    where = f"WHERE schema_name = {_safe_sql_str(schema_name)}" if schema_name else ""
    q = f"SELECT * FROM {fq('schema_knowledge_base')} {where} ORDER BY schema_name"
    return execute_sql(q)


@app.get("/api/metadata/geo-classifications")
def get_geo_classifications(
    table_name: Optional[str] = None,
    classification: Optional[str] = None,
    limit: int = 500,
):
    """Query geo_classifications table."""
    clauses = []
    if table_name:
        _validate_filter(table_name, "table_name")
        clauses.append(f"table_name = {_safe_sql_str(table_name)}")
    if classification:
        _validate_filter(classification, "classification")
        clauses.append(f"classification = {_safe_sql_str(classification)}")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    q = f"SELECT * FROM {fq('geo_classifications')} {where} ORDER BY table_name, column_name LIMIT {limit}"
    return execute_sql(q)


# --- PATCH KB: request bodies ---
class TableKBRow(BaseModel):
    table_name: str
    comment: Optional[str] = None
    domain: Optional[str] = None
    subdomain: Optional[str] = None


class ColumnKBRow(BaseModel):
    column_id: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    comment: Optional[str] = None
    classification: Optional[str] = None


class SchemaKBRow(BaseModel):
    schema_id: str
    comment: Optional[str] = None
    domain: Optional[str] = None


def _record_labeled_update(
    source_kb: str,
    entity_id: str,
    field_name: str,
    old_value: Optional[str],
    new_value: Optional[str],
    updated_by: Optional[str] = None,
):
    _ensure_labeled_updates_table()
    uid = str(_uuid.uuid4())
    execute_sql(
        f"""
        INSERT INTO {fq('metadata_labeled_updates')}
        (update_id, source_kb, entity_identifier, field_name, old_value, new_value, updated_at, updated_by)
        VALUES ({_safe_sql_str(uid)}, {_safe_sql_str(source_kb)}, {_safe_sql_str(entity_id)}, {_safe_sql_str(field_name)},
                {_safe_sql_str(old_value)}, {_safe_sql_str(new_value)}, current_timestamp(), {_safe_sql_str(updated_by)})
        """
    )


@app.patch("/api/metadata/knowledge-base")
def patch_knowledge_base(body: list[TableKBRow]):
    _ensure_labeled_updates_table()
    _ensure_review_updated_at("table_knowledge_base")
    tbl = fq("table_knowledge_base")
    for row in body:
        if not row.table_name or not _SAFE_IDENT_RE.match(row.table_name):
            raise HTTPException(400, "Invalid table_name")
        current = execute_sql(
            f"SELECT table_name, comment, domain, subdomain FROM {tbl} WHERE table_name = {_safe_sql_str(row.table_name)} LIMIT 1"
        )
        old = current[0] if current else {}
        updates = []
        if row.comment is not None:
            updates.append(f"comment = {_safe_sql_str(row.comment)}")
            if old.get("comment") != row.comment:
                _record_labeled_update("table_kb", row.table_name, "comment", old.get("comment"), row.comment)
        if row.domain is not None:
            updates.append(f"domain = {_safe_sql_str(row.domain)}")
            if old.get("domain") != row.domain:
                _record_labeled_update("table_kb", row.table_name, "domain", old.get("domain"), row.domain)
        if row.subdomain is not None:
            updates.append(f"subdomain = {_safe_sql_str(row.subdomain)}")
            if old.get("subdomain") != row.subdomain:
                _record_labeled_update("table_kb", row.table_name, "subdomain", old.get("subdomain"), row.subdomain)
        if not updates:
            continue
        updates.append("updated_at = current_timestamp()")
        updates.append("review_updated_at = current_timestamp()")
        execute_sql(
            f"UPDATE {tbl} SET {', '.join(updates)} WHERE table_name = {_safe_sql_str(row.table_name)}"
        )
    return {"updated": len(body)}


@app.patch("/api/metadata/column-kb")
def patch_column_kb(body: list[ColumnKBRow]):
    _ensure_labeled_updates_table()
    _ensure_review_updated_at("column_knowledge_base")
    tbl = fq("column_knowledge_base")
    for row in body:
        ident = row.column_id or (f"{row.table_name}.{row.column_name}" if row.table_name and row.column_name else None)
        if not ident or not _SAFE_IDENT_RE.match(ident.replace(".", "x")):
            raise HTTPException(400, "Provide column_id or (table_name, column_name)")
        where = f"column_id = {_safe_sql_str(row.column_id)}" if row.column_id else f"table_name = {_safe_sql_str(row.table_name)} AND column_name = {_safe_sql_str(row.column_name)}"
        current = execute_sql(f"SELECT column_id, comment, classification FROM {tbl} WHERE {where} LIMIT 1")
        old = current[0] if current else {}
        entity_id = old.get("column_id") or ident
        updates = []
        if row.comment is not None:
            updates.append(f"comment = {_safe_sql_str(row.comment)}")
            if old.get("comment") != row.comment:
                _record_labeled_update("column_kb", entity_id, "comment", old.get("comment"), row.comment)
        if row.classification is not None:
            updates.append(f"classification = {_safe_sql_str(row.classification)}")
            if old.get("classification") != row.classification:
                _record_labeled_update("column_kb", entity_id, "classification", old.get("classification"), row.classification)
        if not updates:
            continue
        updates.append("updated_at = current_timestamp()")
        updates.append("review_updated_at = current_timestamp()")
        execute_sql(f"UPDATE {tbl} SET {', '.join(updates)} WHERE {where}")
    return {"updated": len(body)}


@app.patch("/api/metadata/schema-kb")
def patch_schema_kb(body: list[SchemaKBRow]):
    _ensure_labeled_updates_table()
    _ensure_review_updated_at("schema_knowledge_base")
    tbl = fq("schema_knowledge_base")
    for row in body:
        if not row.schema_id or not _SAFE_IDENT_RE.match(row.schema_id):
            raise HTTPException(400, "Invalid schema_id")
        current = execute_sql(
            f"SELECT schema_id, comment, domain FROM {tbl} WHERE schema_id = {_safe_sql_str(row.schema_id)} LIMIT 1"
        )
        old = current[0] if current else {}
        updates = []
        if row.comment is not None:
            updates.append(f"comment = {_safe_sql_str(row.comment)}")
            if old.get("comment") != row.comment:
                _record_labeled_update("schema_kb", row.schema_id, "comment", old.get("comment"), row.comment)
        if row.domain is not None:
            updates.append(f"domain = {_safe_sql_str(row.domain)}")
            if old.get("domain") != row.domain:
                _record_labeled_update("schema_kb", row.schema_id, "domain", old.get("domain"), row.domain)
        if not updates:
            continue
        updates.append("updated_at = current_timestamp()")
        updates.append("review_updated_at = current_timestamp()")
        execute_sql(
            f"UPDATE {tbl} SET {', '.join(updates)} WHERE schema_id = {_safe_sql_str(row.schema_id)}"
        )
    return {"updated": len(body)}


# --- Generate / Apply DDL from KB ---
_DOMAIN_TAG = "domain"
_SUBDOMAIN_TAG = "subdomain"
_PI_CLASS_TAG = "data_classification"
_PI_SUBCLASS_TAG = "data_subclassification"


def _full_table_name(row: dict) -> str:
    t = (row.get("table_name") or "").strip()
    if not t:
        return ""
    parts = t.split(".")
    if len(parts) == 3:
        return ".".join(f"`{p}`" for p in parts)
    c = (row.get("catalog") or "").strip()
    s = (row.get("schema") or "").strip()
    return f"`{c}`.`{s}`.`{t}`" if c and s else t


def _escape_comment(t: Optional[str]) -> str:
    if t is None or t == "":
        return ""
    return str(t).replace('"', "'").replace("\\", "\\\\")


def _generate_table_ddl_rows(
    rows: list[dict], ddl_type: str = "all",
    domain_tag: str = _DOMAIN_TAG, subdomain_tag: str = _SUBDOMAIN_TAG,
) -> list[str]:
    stmts = []
    for r in rows:
        full = _full_table_name(r)
        if not full:
            continue
        if ddl_type in ("all", "comments"):
            comment = _escape_comment(r.get("comment"))
            if comment:
                stmts.append(f'COMMENT ON TABLE {full} IS "{comment}";')
        if ddl_type in ("all", "domain"):
            domain = (r.get("domain") or "").strip()
            subdomain = (r.get("subdomain") or "").strip()
            if domain:
                if subdomain:
                    stmts.append(f"ALTER TABLE {full} SET TAGS ('{domain_tag}' = '{domain}', '{subdomain_tag}' = '{subdomain}');")
                else:
                    stmts.append(f"ALTER TABLE {full} SET TAGS ('{domain_tag}' = '{domain}');")
    return stmts


def _generate_column_ddl_rows(
    rows: list[dict], ddl_type: str = "all",
    pi_class_tag: str = _PI_CLASS_TAG, pi_subclass_tag: str = _PI_SUBCLASS_TAG,
) -> list[str]:
    stmts = []
    for r in rows:
        full = _full_table_name(r)
        col = (r.get("column_name") or "").strip()
        if not full or not col:
            continue
        if ddl_type in ("all", "comments"):
            comment = _escape_comment(r.get("comment"))
            if comment:
                stmts.append(f'COMMENT ON COLUMN {full}.`{col}` IS "{comment}";')
        if ddl_type in ("all", "sensitivity"):
            classification = (r.get("classification") or "").strip()
            if classification and classification.lower() != "none":
                stmts.append(
                    f"ALTER TABLE {full} ALTER COLUMN `{col}` SET TAGS "
                    f"('{pi_class_tag}' = '{classification}', '{pi_subclass_tag}' = '{classification}');"
                )
    return stmts


class GenerateDDLBody(BaseModel):
    scope: str  # "table" | "schema" | "column" | "geo"
    identifiers: Optional[list[str]] = None
    tag_key: Optional[str] = None  # legacy sensitivity tag override
    ddl_type: Optional[str] = None  # "comments" | "domain" | "sensitivity" | None (= all)
    domain_tag_key: Optional[str] = None
    subdomain_tag_key: Optional[str] = None
    sensitivity_tag_key: Optional[str] = None
    sensitivity_type_tag_key: Optional[str] = None


def _table_where(identifiers: list[str]) -> str:
    safe = [_safe_sql_str(x) for x in identifiers if _SAFE_IDENT_RE.match(x)]
    if not safe:
        raise HTTPException(400, "No valid table identifiers")
    return " OR ".join([f"table_name = {s}" for s in safe])


@app.post("/api/metadata/generate-ddl")
def generate_ddl(body: GenerateDDLBody):
    scope = (body.scope or "table").lower()
    identifiers = body.identifiers or []
    ddl_type = (body.ddl_type or "all").lower()
    domain_tag = body.domain_tag_key or _DOMAIN_TAG
    subdomain_tag = body.subdomain_tag_key or _SUBDOMAIN_TAG
    pi_class = body.sensitivity_tag_key or body.tag_key or _PI_CLASS_TAG
    pi_subclass = body.sensitivity_type_tag_key or (_PI_SUBCLASS_TAG if pi_class == _PI_CLASS_TAG else pi_class)
    tbl_kb = fq("table_knowledge_base")
    col_kb = fq("column_knowledge_base")
    stmts: list[str] = []

    if scope in ("table", "schema"):
        if scope == "schema" and identifiers:
            safe = [_safe_sql_str(x) for x in identifiers if _SAFE_IDENT_RE.match(x)]
            where = " OR ".join([f"`schema` = {s}" for s in safe]) if safe else "1=0"
        elif identifiers:
            where = _table_where(identifiers)
        else:
            where = "1=1"
        if ddl_type in ("all", "comments", "domain"):
            tbl_rows = execute_sql(
                f"SELECT catalog, `schema`, table_name, comment, domain, subdomain FROM {tbl_kb} WHERE {where} LIMIT 500"
            )
            stmts += _generate_table_ddl_rows(tbl_rows, ddl_type, domain_tag, subdomain_tag)
        if ddl_type in ("all", "comments", "sensitivity"):
            col_where = where if scope == "schema" else (
                " OR ".join([f"table_name = {_safe_sql_str(x)}" for x in identifiers if _SAFE_IDENT_RE.match(x)])
                if identifiers else "1=1"
            )
            col_rows = execute_sql(
                f"SELECT catalog, `schema`, table_name, column_name, comment, classification FROM {col_kb} WHERE {col_where} LIMIT 2000"
            )
            stmts += _generate_column_ddl_rows(col_rows, ddl_type, pi_class, pi_subclass)

    elif scope == "column":
        if identifiers:
            safe = [_safe_sql_str(x) for x in identifiers if _SAFE_IDENT_RE.match(x)]
            if not safe:
                raise HTTPException(400, "No valid identifiers")
            where = " OR ".join([f"table_name = {s} OR column_id = {s}" for s in safe])
        else:
            where = "1=1"
        col_rows = execute_sql(
            f"SELECT catalog, `schema`, table_name, column_name, comment, classification FROM {col_kb} WHERE {where} LIMIT 500"
        )
        stmts = _generate_column_ddl_rows(col_rows, ddl_type, pi_class, pi_subclass)

    elif scope == "geo":
        geo_tbl = fq("geo_classifications")
        tk = body.tag_key or "geo_classification"
        _validate_filter(tk, "tag_key")
        if identifiers:
            safe = [_safe_sql_str(x) for x in identifiers if _SAFE_IDENT_RE.match(x)]
            where = " OR ".join([f"table_name = {s}" for s in safe]) if safe else "1=0"
        else:
            where = "1=1"
        rows = execute_sql(
            f"SELECT table_name, column_name, classification FROM {geo_tbl} WHERE ({where}) AND confidence >= 0.5 LIMIT 2000"
        )
        for r in rows:
            tn = (r.get("table_name") or "").strip()
            cn = (r.get("column_name") or "").strip()
            cls = (r.get("classification") or "").strip()
            if tn and cn and cls:
                stmts.append(f"ALTER TABLE {tn} ALTER COLUMN `{cn}` SET TAGS ('{tk}' = '{cls}');")
    else:
        raise HTTPException(400, "scope must be table, schema, column, or geo")

    sql = "\n".join(stmts) if stmts else "-- No DDL generated"
    return {"sql": sql, "statements": stmts}


_GOVERNED_TAG_HINT = (
    "This may be a governed tag requiring policy updates. "
    "You can use a custom tag key (e.g. 'discovered_classification') to write a discovery tag instead, "
    "or update the governed tag policy in Unity Catalog > Tags to allow this value."
)


@app.post("/api/metadata/apply-ddl")
def apply_ddl(body: GenerateDDLBody):
    out = generate_ddl(body)
    stmts = out.get("statements") or []
    errors = []
    applied = 0
    for s in stmts:
        try:
            execute_sql(s, timeout=60)
            applied += 1
        except Exception as e:
            err_str = str(e)
            detail: dict = {"statement": s[:200], "error": err_str}
            if "PERMISSION_DENIED" in err_str and "tag" in err_str.lower():
                detail["governed_tag"] = True
                detail["hint"] = _GOVERNED_TAG_HINT
            errors.append(detail)
    if errors:
        return {
            "message": "Some DDL statements failed",
            "applied": applied,
            "errors": errors,
        }
    return {"applied": applied}


# ---------------------------------------------------------------------------
# Review Editor combined endpoint
# ---------------------------------------------------------------------------

class ReviewCombinedRequest(BaseModel):
    tables: Optional[list[str]] = None
    schemas: Optional[list[str]] = None


@app.post("/api/metadata/review-combined")
def review_combined(body: ReviewCombinedRequest):
    """Fetch combined table + column KB data, with ontology and FK info per table."""
    tbl_kb = fq("table_knowledge_base")
    col_kb = fq("column_knowledge_base")
    ent_tbl = fq("ontology_entities")
    fk_tbl = fq("fk_predictions")

    where_parts = []
    if body.tables:
        safe = [_safe_sql_str(t) for t in body.tables if _SAFE_IDENT_RE.match(t)]
        if safe:
            where_parts.append("(" + " OR ".join(f"table_name = {s}" for s in safe) + ")")
    if body.schemas:
        for s in body.schemas:
            parts = s.split(".")
            if len(parts) == 2 and all(_SAFE_IDENT_RE.match(p) for p in parts):
                where_parts.append(f"(catalog = '{parts[0]}' AND `schema` = '{parts[1]}')")
    if not where_parts:
        raise HTTPException(400, "Provide at least one table or schema")
    where = " OR ".join(where_parts)

    tbl_rows = execute_sql(f"""
        SELECT table_name, catalog, `schema`, table_short_name, comment,
               domain, subdomain, has_pii, has_phi
        FROM {tbl_kb} WHERE {where} LIMIT 200
    """)
    if not tbl_rows:
        return {"tables": []}

    tbl_names = [r["table_name"] for r in tbl_rows]
    safe_names = [_safe_sql_str(n) for n in tbl_names]
    in_clause = ", ".join(safe_names)

    col_rows = execute_sql(f"""
        SELECT column_id, table_name, column_name, data_type, comment,
               classification, classification_type, confidence
        FROM {col_kb} WHERE table_name IN ({in_clause})
    """)

    onto_rows, fk_rows, col_prop_rows = [], [], []
    _onto_where = f"SIZE(source_tables) > 0 AND EXISTS(source_tables, t -> t IN ({in_clause}))"
    try:
        onto_rows = execute_sql(f"""
            SELECT entity_id, entity_type, entity_name, confidence,
                   source_columns, validation_notes, validated,
                   COALESCE(entity_role, 'primary') AS entity_role,
                   discovery_confidence,
                   EXPLODE(source_tables) as table_name
            FROM {ent_tbl}
            WHERE {_onto_where}
        """)
    except Exception as e:
        logger.warning("Enriched ontology query failed (%s), falling back to simple query", e)
        try:
            onto_rows = execute_sql(f"""
                SELECT entity_type, entity_name, confidence, EXPLODE(source_tables) as table_name
                FROM {ent_tbl}
                WHERE {_onto_where}
            """)
        except Exception:
            pass

    # Fetch column properties
    cp_tbl = fq("ontology_column_properties")
    try:
        col_prop_rows = execute_sql(f"""
            SELECT property_id, table_name, column_name, property_name,
                   property_role, owning_entity_id, owning_entity_type,
                   linked_entity_type, confidence
            FROM {cp_tbl}
            WHERE table_name IN ({in_clause})
        """)
    except Exception:
        pass
    try:
        fk_rows = execute_sql(f"""
            SELECT src_column, src_table, dst_column, dst_table, final_confidence,
                   ai_reasoning, ai_confidence, col_similarity, rule_score
            FROM {fk_tbl}
            WHERE src_table IN ({in_clause}) OR dst_table IN ({in_clause})
        """)
    except Exception as e:
        logger.warning("Enriched FK query failed (%s), falling back to simple query", e)
        try:
            fk_rows = execute_sql(f"""
                SELECT src_column, src_table, dst_column, dst_table, final_confidence
                FROM {fk_tbl}
                WHERE src_table IN ({in_clause}) OR dst_table IN ({in_clause})
            """)
        except Exception:
            pass

    cols_by_table = {}
    for c in col_rows:
        cols_by_table.setdefault(c["table_name"], []).append(c)
    onto_by_table = {}
    for o in onto_rows:
        raw_cols = o.get("source_columns")
        if isinstance(raw_cols, str):
            try:
                raw_cols = json.loads(raw_cols)
            except Exception:
                raw_cols = None
        onto_by_table.setdefault(o["table_name"], []).append({
            "entity_id": o.get("entity_id"),
            "entity_type": o["entity_type"],
            "entity_name": o["entity_name"],
            "confidence": o["confidence"],
            "entity_role": o.get("entity_role", "primary"),
            "discovery_confidence": o.get("discovery_confidence"),
            "source_columns": raw_cols if isinstance(raw_cols, list) else None,
            "validation_notes": o.get("validation_notes"),
            "validated": o.get("validated"),
        })
    for tbl_name, ents in onto_by_table.items():
        seen = {}
        for e in ents:
            key = (e["entity_type"], tuple(e["source_columns"] or []))
            if key not in seen or float(e["confidence"] or 0) > float(seen[key]["confidence"] or 0):
                seen[key] = e
        onto_by_table[tbl_name] = list(seen.values())

    # Build column properties lookup
    col_props_by_table: dict = {}
    for cp in col_prop_rows:
        col_props_by_table.setdefault(cp["table_name"], []).append(cp)

    fk_by_table = {}
    for f in fk_rows:
        for tn in [f.get("src_table"), f.get("dst_table")]:
            if tn in tbl_names:
                fk_by_table.setdefault(tn, []).append(f)

    result = []
    for t in tbl_rows:
        tn = t["table_name"]
        ents = onto_by_table.get(tn, [])
        primary_ents = [e for e in ents if e.get("entity_role") == "primary"]
        primary_entity = primary_ents[0] if primary_ents else None
        result.append({
            **t,
            "columns": cols_by_table.get(tn, []),
            "primary_entity": primary_entity,
            "ontology_entities": ents,
            "column_properties": col_props_by_table.get(tn, []),
            "fk_predictions": fk_by_table.get(tn, []),
        })
    return {"tables": result}


class ExportVolumeRequest(BaseModel):
    tables: list[str]
    format: str = "tsv"
    include_columns: bool = True
    metadata_type: Optional[str] = None


@app.post("/api/metadata/export-volume")
def export_to_volume(body: ExportVolumeRequest):
    """Export metadata for selected tables to a volume as TSV or Excel."""
    import io, csv
    from datetime import datetime

    combined = review_combined(ReviewCombinedRequest(tables=body.tables))
    rows = []
    for t in combined.get("tables", []):
        rows.append({
            "level": "table", "table_name": t["table_name"], "column_name": "",
            "data_type": "", "comment": t.get("comment", ""),
            "domain": t.get("domain", ""), "subdomain": t.get("subdomain", ""),
            "has_pii": str(t.get("has_pii", "")), "has_phi": str(t.get("has_phi", "")),
            "classification": "", "classification_type": "",
        })
        if body.include_columns:
            for c in t.get("columns", []):
                rows.append({
                    "level": "column", "table_name": t["table_name"],
                    "column_name": c.get("column_name", ""), "data_type": c.get("data_type", ""),
                    "comment": c.get("comment", ""), "domain": "", "subdomain": "",
                    "has_pii": "", "has_phi": "",
                    "classification": c.get("classification", ""),
                    "classification_type": c.get("classification_type", ""),
                })
    if not rows:
        raise HTTPException(400, "No data to export")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_date = datetime.now().strftime("%Y%m%d")
    volume_name = os.environ.get("VOLUME_NAME", "generated_metadata")
    ws = get_workspace_client()
    current_user = "app"
    try:
        current_user = ws.current_user.me().user_name.split("@")[0]
    except Exception:
        pass

    if body.format == "excel":
        import openpyxl
        wb = openpyxl.Workbook()
        ws_sheet = wb.active
        ws_sheet.title = "Metadata"
        headers = list(rows[0].keys())
        ws_sheet.append(headers)
        for r in rows:
            ws_sheet.append([r.get(h, "") for h in headers])
        buf = io.BytesIO()
        wb.save(buf)
        content = buf.getvalue()
        ext = "xlsx"
    else:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
        content = buf.getvalue().encode("utf-8")
        ext = "tsv"

    type_suffix = f"_{body.metadata_type}" if body.metadata_type else ""
    vol_path = f"/Volumes/{CATALOG}/{SCHEMA}/{volume_name}/{current_user}/{current_date}/review_export{type_suffix}_{ts}.{ext}"
    try:
        ws.files.upload(vol_path, io.BytesIO(content) if isinstance(content, bytes) else io.BytesIO(content), overwrite=True)
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to write to volume: {e}")
    return {"path": vol_path, "rows": len(rows), "format": ext}


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


@app.get("/api/ontology/column-properties/{table_name:path}")
def get_ontology_column_properties(table_name: str):
    safe_tbl = _safe_sql_str(table_name)
    q = f"""
        SELECT property_id, table_name, column_name, property_name,
               property_role, owning_entity_id, owning_entity_type,
               linked_entity_type, confidence, discovery_method
        FROM {fq('ontology_column_properties')}
        WHERE table_name = {safe_tbl}
        ORDER BY column_name
    """
    try:
        return execute_sql(q)
    except Exception:
        return []


@app.get("/api/ontology/relationships")
def get_ontology_relationships():
    q = f"""
        SELECT relationship_id, src_entity_type, relationship_name,
               dst_entity_type, cardinality, evidence_column,
               evidence_table, source, confidence
        FROM {fq('ontology_relationships')}
        ORDER BY confidence DESC
    """
    try:
        return execute_sql(q)
    except Exception:
        return []


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


_DOMAIN_CONFIG_DIR = "configurations"
_BUNDLE_SUBDIR = os.path.join(_DOMAIN_CONFIG_DIR, "ontology_bundles")
_CONFIG_DIR_CANDIDATES = [
    _DOMAIN_CONFIG_DIR,
    os.path.join("..", _DOMAIN_CONFIG_DIR),
    os.path.join(os.path.dirname(__file__), _DOMAIN_CONFIG_DIR),
    os.path.join(os.path.dirname(__file__), "..", "..", "..", _DOMAIN_CONFIG_DIR),
]


def _find_domain_config_dir() -> Optional[str]:
    for d in _CONFIG_DIR_CANDIDATES:
        resolved = os.path.abspath(d)
        exists = os.path.isdir(resolved)
        logger.debug("config-dir candidate: %s (resolved=%s, exists=%s)", d, resolved, exists)
        if exists:
            return d
    logger.warning("No config dir found. cwd=%s __file__=%s", os.getcwd(), __file__)
    return None


def _find_bundle_dir() -> Optional[str]:
    """Locate the ontology_bundles directory."""
    for base in _CONFIG_DIR_CANDIDATES:
        bd = os.path.join(base, "ontology_bundles")
        resolved = os.path.abspath(bd)
        exists = os.path.isdir(resolved)
        logger.debug("bundle-dir candidate: %s (resolved=%s, exists=%s)", bd, resolved, exists)
        if exists:
            return bd
    logger.warning("No bundle dir found. cwd=%s __file__=%s", os.getcwd(), __file__)
    return None


def _list_bundles_local() -> list[dict]:
    """Read ontology bundle YAMLs directly (no dbxmetagen import needed)."""
    bd = _find_bundle_dir()
    if not bd:
        logger.warning("_list_bundles_local: no bundle dir found, returning empty list")
        return []
    bundles = []
    for fname in sorted(os.listdir(bd)):
        if not fname.endswith(".yaml"):
            continue
        try:
            with open(os.path.join(bd, fname), "r") as f:
                raw = yaml.safe_load(f)
            meta = raw.get("metadata", {})
            bundles.append({
                "key": fname.replace(".yaml", ""),
                "name": meta.get("name", fname.replace(".yaml", "")),
                "industry": meta.get("industry", "general"),
                "description": meta.get("description", ""),
                "standards_alignment": meta.get("standards_alignment", ""),
                "entity_count": len(raw.get("ontology", {}).get("entities", {}).get("definitions", {})),
                "domain_count": len(raw.get("domains", {})),
                "bundle_type": meta.get("bundle_type", "ontology"),
                "tag_key": meta.get("tag_key", ""),
            })
        except Exception as e:
            logger.debug("Could not read bundle %s: %s", fname, e)
    return bundles


def _resolve_bundle_path_local(bundle_name: str) -> str:
    """Resolve a bundle key to its YAML path."""
    filename = f"{bundle_name}.yaml" if not bundle_name.endswith(".yaml") else bundle_name
    bd = _find_bundle_dir()
    if bd:
        path = os.path.join(bd, filename)
        if os.path.exists(path):
            return path
    return os.path.join(_BUNDLE_SUBDIR, filename)


@app.get("/api/ontology/bundles")
def get_ontology_bundles():
    """List available ontology bundles with metadata."""
    return _list_bundles_local()


@app.get("/api/debug/config-paths")
def debug_config_paths():
    """Diagnostic endpoint: show which config paths exist and what was found."""
    result = {"cwd": os.getcwd(), "__file__": __file__, "candidates": []}
    for base in _CONFIG_DIR_CANDIDATES:
        bd = os.path.join(base, "ontology_bundles")
        base_abs = os.path.abspath(base)
        bd_abs = os.path.abspath(bd)
        entry = {
            "config_dir": base, "config_dir_abs": base_abs, "config_exists": os.path.isdir(base_abs),
            "bundle_dir": bd, "bundle_dir_abs": bd_abs, "bundle_exists": os.path.isdir(bd_abs),
        }
        if os.path.isdir(bd_abs):
            entry["bundle_files"] = sorted(os.listdir(bd_abs))
        result["candidates"].append(entry)
    result["resolved_bundle_dir"] = _find_bundle_dir()
    result["resolved_config_dir"] = _find_domain_config_dir()
    result["bundle_count"] = len(_list_bundles_local())
    return result


def _resolve_domain_config_path(key: str) -> str:
    """Resolve a domain config key to a file path for the job parameter."""
    bundles = {b["key"]: b for b in _list_bundles_local()}
    if key in bundles:
        return _resolve_bundle_path_local(key)
    cfg_dir = _find_domain_config_dir()
    if cfg_dir:
        path = os.path.join(cfg_dir, f"{key}.yaml")
        if os.path.exists(path):
            return path
    return key


@app.get("/api/domain-configs")
def list_domain_configs():
    """List domain config sources: ontology bundles + standalone YAML files."""
    items = []
    for b in _list_bundles_local():
        if b.get("domain_count", 0) > 0:
            items.append({
                "key": b["key"],
                "name": b.get("name", b["key"]),
                "source": "bundle",
                "domain_count": b["domain_count"],
            })

    cfg_dir = _find_domain_config_dir()
    if cfg_dir:
        for fname in sorted(os.listdir(cfg_dir)):
            if not fname.startswith("domain_config") or not fname.endswith(".yaml"):
                continue
            file_key = fname.replace(".yaml", "")
            try:
                with open(os.path.join(cfg_dir, fname), "r") as f:
                    raw = yaml.safe_load(f)
                domain_count = len(raw.get("domains", {})) if raw else 0
            except Exception:
                domain_count = 0
            items.append({
                "key": file_key,
                "name": file_key.replace("_", " ").replace("domain config ", "").title() + " (standalone)",
                "source": "file",
                "domain_count": domain_count,
            })
    return items


class OntologyApplyItem(BaseModel):
    entity_type: str
    source_tables: Union[list[str], str]
    source_columns: Optional[list[str]] = None
    entity_role: Optional[str] = None


class OntologyApplyBody(BaseModel):
    selections: list[OntologyApplyItem]


@app.post("/api/ontology/apply-tags")
def ontology_apply_tags(body: OntologyApplyBody):
    """Apply entity_type tags at table level (primary only) and column level, then verify."""
    table_entities: dict[str, set[str]] = {}
    col_entities: dict[tuple[str, str], set[str]] = {}
    for item in body.selections:
        tables = item.source_tables if isinstance(item.source_tables, list) else [item.source_tables]
        et = (item.entity_type or "").strip()
        cols = item.source_columns or []
        role = (item.entity_role or "primary").strip()
        for tbl in tables:
            if not (tbl and isinstance(tbl, str) and et):
                continue
            tbl = tbl.strip()
            # Only primary entities get table-level tags
            if role == "primary":
                table_entities.setdefault(tbl, set()).add(et)
            for col in cols:
                if col and isinstance(col, str):
                    col_entities.setdefault((tbl, col.strip()), set()).add(et)

    table_results = []
    col_results = []

    # --- Table-level tags ---
    for tbl_clean, ets in table_entities.items():
        if not _SAFE_IDENT_RE.match(tbl_clean.replace(".", "x")):
            table_results.append({"table": tbl_clean, "ok": False, "error": "Invalid table name"})
            continue
        tag_val = ",".join(sorted(ets)).replace("'", "''")
        sql = f"ALTER TABLE {tbl_clean} SET TAGS ('entity_type' = '{tag_val}')"
        try:
            execute_sql(sql, timeout=30)
        except Exception as e:
            table_results.append({"table": tbl_clean, "ok": False, "sql": sql, "error": str(e)})
            continue
        parts = tbl_clean.split(".")
        verified = None
        if len(parts) == 3:
            cat, sch, tname = [p.strip("`") for p in parts]
            try:
                rows = execute_sql(
                    f"SELECT tag_value FROM system.information_schema.table_tags "
                    f"WHERE catalog_name = '{cat}' AND schema_name = '{sch}' "
                    f"AND table_name = '{tname}' AND tag_name = 'entity_type'",
                    timeout=15,
                )
                verified = rows[0]["tag_value"] if rows else None
            except Exception as ve:
                logger.warning("Table tag verification failed for %s: %s", tbl_clean, ve)
        if verified is not None:
            table_results.append({"table": tbl_clean, "ok": True, "sql": sql, "verified": verified})
        else:
            table_results.append({"table": tbl_clean, "ok": False, "sql": sql,
                "error": f"SQL succeeded but tag not found -- check APPLY TAG permissions"})

    # --- Column-level tags ---
    for (tbl_clean, col_name), ets in col_entities.items():
        if not _SAFE_IDENT_RE.match(tbl_clean.replace(".", "x")):
            continue
        tag_val = ",".join(sorted(ets)).replace("'", "''")
        col_safe = col_name.replace("`", "")
        sql = f"ALTER TABLE {tbl_clean} ALTER COLUMN `{col_safe}` SET TAGS ('entity_type' = '{tag_val}')"
        try:
            execute_sql(sql, timeout=30)
        except Exception as e:
            col_results.append({"table": tbl_clean, "column": col_name, "ok": False, "sql": sql, "error": str(e)})
            continue
        parts = tbl_clean.split(".")
        verified = None
        if len(parts) == 3:
            cat, sch, tname = [p.strip("`") for p in parts]
            try:
                rows = execute_sql(
                    f"SELECT tag_value FROM system.information_schema.column_tags "
                    f"WHERE catalog_name = '{cat}' AND schema_name = '{sch}' "
                    f"AND table_name = '{tname}' AND column_name = '{col_safe}' "
                    f"AND tag_name = 'entity_type'",
                    timeout=15,
                )
                verified = rows[0]["tag_value"] if rows else None
            except Exception as ve:
                logger.warning("Column tag verification failed for %s.%s: %s", tbl_clean, col_name, ve)
        if verified is not None:
            col_results.append({"table": tbl_clean, "column": col_name, "ok": True, "sql": sql, "verified": verified})
        else:
            col_results.append({"table": tbl_clean, "column": col_name, "ok": False, "sql": sql,
                "error": f"SQL succeeded but column tag not found -- check permissions"})

    return {"results": table_results, "column_results": col_results}


@app.get("/api/ontology/entity-type-options")
def get_entity_type_options():
    """Return deduplicated entity type names from all ontology bundle YAMLs."""
    bd = _find_bundle_dir()
    if not bd:
        return []
    types = set()
    for fname in os.listdir(bd):
        if not fname.endswith(".yaml"):
            continue
        try:
            with open(os.path.join(bd, fname), "r") as f:
                raw = yaml.safe_load(f)
            defs = raw.get("ontology", {}).get("entities", {}).get("definitions", {})
            types.update(defs.keys())
        except Exception:
            pass
    return sorted(types)


class UpdateEntityTypeBody(BaseModel):
    entity_id: str
    new_entity_type: str


@app.post("/api/ontology/update-entity-type")
def update_entity_type(body: UpdateEntityTypeBody):
    """Update an entity's entity_type in the ontology_entities table."""
    eid = body.entity_id.replace("'", "''")
    et = body.new_entity_type.replace("'", "''")
    try:
        execute_sql(f"""
            UPDATE {fq('ontology_entities')}
            SET entity_type = '{et}', entity_name = '{et}', updated_at = current_timestamp()
            WHERE entity_id = '{eid}'
        """, timeout=30)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))


class FKApplyPredictionItem(BaseModel):
    src_table: str
    src_column: str
    dst_table: str
    dst_column: str


class FKApplyPredictionsBody(BaseModel):
    predictions: list[FKApplyPredictionItem]


@app.post("/api/analytics/fk-apply-from-predictions")
def fk_apply_from_predictions(body: FKApplyPredictionsBody):
    """Generate and execute FK constraint DDL from prediction data."""
    results = []
    for p in body.predictions:
        src_short = p.src_column.split(".")[-1] if "." in p.src_column else p.src_column
        dst_short = p.dst_column.split(".")[-1] if "." in p.dst_column else p.dst_column
        constraint = f"fk_{src_short}_{dst_short}"
        ddl = f"ALTER TABLE {p.src_table} ADD CONSTRAINT {constraint} FOREIGN KEY ({src_short}) REFERENCES {p.dst_table}({dst_short})"
        try:
            execute_sql(ddl, timeout=60)
            results.append({"ddl": ddl, "ok": True})
        except Exception as e:
            results.append({"ddl": ddl, "ok": False, "error": str(e)})
    return {"results": results}


@app.get("/api/ontology/relationships")
def get_ontology_relationships(limit: int = 500):
    """Return entity-level relationship edges for the ontology graph visualization."""
    q = f"""
        SELECT src, dst, relationship, weight
        FROM {fq('graph_edges')}
        WHERE relationship NOT IN ('similar_embedding', 'shares_column_name')
        ORDER BY weight DESC LIMIT {limit}
    """
    return execute_sql(q)


@app.get("/api/ontology/coverage")
def get_ontology_coverage():
    """Return entity_type x source_table matrix with confidence for heatmap."""
    q = f"""
        SELECT entity_type, source_tables, confidence
        FROM {fq('ontology_entities')}
        WHERE entity_type IS NOT NULL
    """
    return execute_sql(q)


@app.get("/api/ontology/metrics")
def get_ontology_metrics():
    """Return computed ontology health metrics from ontology_metrics table."""
    q = f"""
        SELECT metric_name, description, sql_definition as value, updated_at
        FROM {fq('ontology_metrics')}
        WHERE aggregation_type = 'COMPUTED'
        ORDER BY metric_name
    """
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


@app.get("/api/ontology/edge-summary")
def get_ontology_edge_summary():
    """Return edge counts by relationship type for ontology-relevant edges."""
    q = f"""
        SELECT relationship, COUNT(*) as count
        FROM {fq('graph_edges')}
        WHERE relationship IN ('instance_of', 'has_attribute', 'is_a', 'references')
        GROUP BY relationship
        ORDER BY count DESC
    """
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


@app.get("/api/ontology/confidence-distribution")
def get_ontology_confidence_distribution():
    """Return confidence band distribution for entities."""
    q = f"""
        SELECT
            CASE
                WHEN confidence < 0.4 THEN '0-0.4'
                WHEN confidence < 0.6 THEN '0.4-0.6'
                WHEN confidence < 0.8 THEN '0.6-0.8'
                ELSE '0.8-1.0'
            END as band,
            COUNT(*) as count
        FROM {fq('ontology_entities')}
        WHERE entity_type IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    try:
        return execute_sql(q)
    except HTTPException as e:
        if e.status_code == 404:
            return []
        raise


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
    try:
        q = f"SELECT * FROM {fq('fk_ddl_statements')} ORDER BY confidence DESC"
        return execute_sql(q)
    except HTTPException:
        return []


class FKApplyBody(BaseModel):
    statements: list[str]


@app.post("/api/analytics/fk-apply")
def fk_apply(body: FKApplyBody):
    """Execute selected FK DDL statements. Run FK prediction job first to populate fk_ddl_statements."""
    results = []
    for stmt in (body.statements or []):
        s = (stmt or "").strip()
        if not s or not s.upper().startswith("ALTER TABLE"):
            results.append({"ok": False, "error": "Not an ALTER TABLE statement"})
            continue
        try:
            execute_sql(s, timeout=60)
            results.append({"ok": True})
        except Exception as e:
            results.append({"ok": False, "error": str(e)})
    return {"results": results}


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


@app.get("/api/coverage/type-breakdown")
def get_coverage_type_breakdown():
    """Count tables per table_type across the configured catalog."""
    q = f"""
        SELECT table_type, COUNT(*) as count
        FROM system.information_schema.tables
        WHERE table_catalog = '{CATALOG}'
          AND table_schema NOT IN ('information_schema', '__internal')
        GROUP BY table_type
        ORDER BY count DESC
    """
    return execute_sql(q)


@app.get("/api/coverage/metadata-summary")
def get_coverage_metadata_summary(catalog: Optional[str] = None, schema: Optional[str] = None):
    """Return metadata completeness rates, optionally filtered by catalog.schema."""
    schema_filter = ""
    if catalog and schema:
        schema_filter = f" WHERE table_name LIKE '{catalog}.{schema}.%'"
    result = {}
    try:
        rows = execute_sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END) as with_comments,
                SUM(CASE WHEN has_pii = true OR has_phi = true THEN 1 ELSE 0 END) as with_pii,
                SUM(CASE WHEN domain IS NOT NULL AND domain != '' THEN 1 ELSE 0 END) as with_domain
            FROM {fq('table_knowledge_base')}{schema_filter}
        """)
        result = rows[0] if rows else {}
    except Exception:
        result = {"total": 0, "with_comments": 0, "with_pii": 0, "with_domain": 0}
    onto_filter = f" WHERE t.table_name LIKE '{catalog}.{schema}.%'" if catalog and schema else ""
    try:
        onto = execute_sql(f"SELECT COUNT(DISTINCT t.table_name) as with_ontology FROM (SELECT EXPLODE(source_tables) as table_name FROM {fq('ontology_entities')}) t{onto_filter}")
        result["with_ontology"] = onto[0]["with_ontology"] if onto else 0
    except Exception:
        result["with_ontology"] = 0
    fk_filter = f" WHERE src_table LIKE '{catalog}.{schema}.%' OR dst_table LIKE '{catalog}.{schema}.%'" if catalog and schema else ""
    try:
        fks = execute_sql(f"SELECT COUNT(DISTINCT src_table) + COUNT(DISTINCT dst_table) as with_fk FROM {fq('fk_predictions')}{fk_filter}")
        result["with_fk"] = fks[0]["with_fk"] if fks else 0
    except Exception:
        result["with_fk"] = 0
    return result


@app.get("/api/coverage/tables")
def get_coverage_tables(catalog: Optional[str] = None, schema: Optional[str] = None):
    """List individual tables and whether they've been profiled."""
    cat = catalog or CATALOG
    conditions = [f"t.table_catalog = '{cat}'"]
    if schema:
        conditions.append(f"t.table_schema = '{schema}'")
    else:
        conditions.append("t.table_schema NOT IN ('information_schema', '__internal')")
    conditions.append("t.table_type IN ('MANAGED', 'EXTERNAL', 'VIEW', 'STREAMING_TABLE', 'MATERIALIZED_VIEW', 'FOREIGN')")
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
    q = f"SELECT id, table_short_name, node_type, domain, security_level, comment FROM public.graph_nodes {where} ORDER BY id LIMIT {limit}"
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
               n.table_short_name, n.node_type, n.domain, n.comment
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
        f"FROM ("
        f"  SELECT *, ROW_NUMBER() OVER ("
        f"    PARTITION BY metric_view_name, source_table "
        f"    ORDER BY CASE status "
        f"      WHEN 'applied' THEN 0 WHEN 'validated' THEN 1 "
        f"      WHEN 'created' THEN 2 ELSE 3 END, "
        f"    created_at DESC"
        f"  ) AS _rn "
        f"  FROM {fq('metric_view_definitions')} "
        f"  {where}"
        f") WHERE _rn = 1 "
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
def delete_semantic_definition(
    definition_id: str,
    drop_view: bool = False,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
):
    _ensure_semantic_layer_tables()
    if drop_view and catalog and schema:
        rows = execute_sql(
            f"SELECT metric_view_name, json_definition, status FROM {fq('metric_view_definitions')} "
            f"WHERE definition_id = '{definition_id}'"
        )
        if rows and rows[0].get("status") == "applied":
            defn = rows[0].get("json_definition", "{}")
            if isinstance(defn, str):
                defn = json.loads(defn) if defn.strip() else {}
            mv_name = defn.get("name") or rows[0].get("metric_view_name", "")
            if mv_name:
                try:
                    execute_sql(f"DROP VIEW IF EXISTS `{catalog}`.`{schema}`.`{mv_name}`")
                except Exception:
                    pass
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


@app.get("/api/semantic/metric-views")
def list_metric_views(status: Optional[str] = None):
    """List metric views from the definitions table.

    Defaults to status='created'. Pass status='all' to return every non-superseded
    metric view (latest version only), or a comma-separated list like
    'applied,validated,created'.
    """
    _ensure_semantic_layer_tables()
    if status and status.lower() == "all":
        status_filter = "status != 'superseded'"
    elif status:
        vals = ", ".join(f"'{s.strip()}'" for s in status.split(",") if s.strip())
        status_filter = f"status IN ({vals})" if vals else "status = 'created'"
    else:
        status_filter = "status = 'created'"
    q = (
        f"SELECT definition_id, metric_view_name, source_table, status, "
        f"genie_space_id, created_at "
        f"FROM ("
        f"  SELECT *, ROW_NUMBER() OVER ("
        f"    PARTITION BY metric_view_name, source_table "
        f"    ORDER BY CASE status "
        f"      WHEN 'applied' THEN 0 WHEN 'validated' THEN 1 "
        f"      WHEN 'created' THEN 2 ELSE 3 END, "
        f"    created_at DESC"
        f"  ) AS _rn "
        f"  FROM {fq('metric_view_definitions')} "
        f"  WHERE {status_filter}"
        f") WHERE _rn = 1 "
        f"ORDER BY source_table, metric_view_name"
    )
    try:
        return execute_sql(q)
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
15. When Entity types are annotated on tables, generate entity-specific analytical metrics:
    - People/patients/users: include counts, return/readmission rates, segmentation dimensions, and per-entity averages
    - Transactions/events/encounters: include volume counts, value sums, time-based rates, and categorical breakdowns
    - Resources/staff/inventory: include utilization rates (active/total), efficiency ratios, and capacity measures
    Always include at least one RATIO measure (x / NULLIF(y, 0)) and one RATE measure (conditional_count * 1.0 / NULLIF(total, 0)) per metric view
16. When FOREIGN KEY RELATIONSHIPS exist between selected tables, generate cross-table metrics that join fact tables to dimension tables. Use dimension table columns as grouping dimensions and fact table columns as measures

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
    """Quote bare text after THEN/ELSE that isn't already quoted or a number/column/keyword."""
    def _replacer(m):
        kw = m.group(1)
        body = m.group(2).strip()
        if not body:
            return m.group(0)
        if body.startswith("'") or body.startswith('"'):
            return m.group(0)
        if re.match(r"^-?\d+(\.\d+)?$", body):
            return m.group(0)
        tokens = body.split()
        if len(tokens) == 1 and tokens[0].upper() in _SQL_RESERVED:
            return m.group(0)
        if len(tokens) == 1 and re.match(r"^[A-Za-z_]\w*$", tokens[0]):
            if tokens[0].upper() not in _SQL_RESERVED:
                return f"{kw} '{body}'"
            return m.group(0)
        return f"{kw} '{body}'"

    return re.sub(
        r"\b(THEN|ELSE)\s+(.*?)(?=\s+(?:WHEN|ELSE|END)\b)",
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
    cx = _score_definition_complexity(defn)
    execute_sql(
        f"INSERT INTO {fq('metric_view_definitions')} VALUES ("
        f"'{new_id}', '{mv_name}', '{source_table}', '{json_str}', '{source_qs}', "
        f"'{status}', '{error_esc}', NULL, '{now}', NULL, {new_version}, '{definition_id}', {proj_val}, "
        f"{cx['complexity_score']}, '{cx['complexity_level']}')"
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
    source = defn.get("source", "")
    if not source:
        raise ValueError("Metric view definition missing 'source' table")
    lines = ['version: "1.1"', f'source: {source}']
    if defn.get("comment"):
        lines.append(f'comment: "{_yaml_esc(defn["comment"])}"')
    if defn.get("filter"):
        lines.append(f'filter: "{_yaml_esc(defn["filter"])}"')
    lines.append("dimensions:")
    for i, d in enumerate(defn.get("dimensions", [])):
        name = d.get("name", f"dim_{i}")
        expr = d.get("expr", name)
        lines.append(f'  - name: "{_yaml_esc(name)}"')
        lines.append(f'    expr: "{_yaml_esc(expr)}"')
        if d.get("comment"):
            lines.append(f'    comment: "{_yaml_esc(d["comment"])}"')
    lines.append("measures:")
    for i, m in enumerate(defn.get("measures", [])):
        name = m.get("name", f"measure_{i}")
        expr = m.get("expr", "COUNT(*)")
        lines.append(f'  - name: "{_yaml_esc(name)}"')
        lines.append(f'    expr: "{_yaml_esc(expr)}"')
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


_agent_ref_cache: dict[str, dict] = {}

def _load_agent_reference(name: str, sections: list[str] | None = None) -> str:
    """Load a JSON agent reference file and return selected sections as prompt text."""
    if name not in _agent_ref_cache:
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "configurations", "agent_references", name),
            os.path.join(os.path.dirname(__file__), "configurations", "agent_references", name),
            os.path.join("configurations", "agent_references", name),
        ]
        for p in candidates:
            p = os.path.normpath(p)
            if os.path.isfile(p):
                with open(p) as f:
                    _agent_ref_cache[name] = json.load(f)
                break
        else:
            _agent_ref_cache[name] = {}

    ref = _agent_ref_cache[name]
    if not ref:
        return ""
    keys = sections or list(ref.keys())
    parts = []
    for k in keys:
        val = ref.get(k)
        if val is None:
            continue
        if isinstance(val, list):
            parts.append(f"\n### {k}")
            for item in val:
                parts.append(f"- {item}" if isinstance(item, str) else f"- {json.dumps(item)}")
        elif isinstance(val, str):
            parts.append(f"\n### {k}\n{val}")
        else:
            parts.append(f"\n### {k}\n{json.dumps(val, indent=2)}")
    return "\n".join(parts)


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
    ref_rules = _load_agent_reference("metric_view_reference.json", ["yaml_syntax_rules", "anti_patterns"])
    prompt = f"""You are fixing a metric view definition that has SQL errors.

ORIGINAL DEFINITION:
{json.dumps(defn, indent=2)}

ERRORS:
{validation_errors}

AVAILABLE COLUMNS in {source}:
{available_cols}

TABLE METADATA:
{context}

REFERENCE: SYNTAX RULES & ANTI-PATTERNS (follow these strictly)
{ref_rules}

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
""".format(
            score=cx["complexity_score"]
        )
    ref_improve = _load_agent_reference("metric_view_reference.json", ["measure_patterns", "validation_checklist", "join_templates"])
    prompt = f"""You are improving a validated metric view definition to make it more sophisticated.

CURRENT DEFINITION:
{json.dumps(defn, indent=2)}

TABLE METADATA:
{context}
{trivial_nudge}
REFERENCE: MEASURE PATTERNS & VALIDATION CHECKLIST (use these to guide improvements)
{ref_improve}

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


class SuggestFixRequest(BaseModel):
    error_message: str


@app.post("/api/semantic-layer/definitions/{definition_id}/suggest-fix")
def suggest_metric_view_fix(definition_id: str, req: SuggestFixRequest):
    """Ask AI for a minimal fix to the definition given a create/DDL error. Returns suggested JSON only; does not save."""
    row = _fetch_definition(definition_id)
    defn_str = row.get("json_definition") or "{}"
    if isinstance(defn_str, dict):
        defn_str = json.dumps(defn_str)
    err = (req.error_message or "").strip() or "Unknown error"
    prompt = f"""This metric view creation failed with error: {err}

Current definition (JSON):
{defn_str}

Suggest a minimal fix. Return ONLY the corrected JSON object (no markdown, no explanation). Keep source, name, and structure; fix only what the error indicates."""

    escaped = prompt.replace("'", "''")
    rows = execute_sql(
        f"SELECT AI_QUERY('{_DEFAULT_MODEL}', '{escaped}') as response", timeout=180
    )
    response = rows[0]["response"] if rows else ""
    try:
        suggested = _parse_single_json(response)
        return {"suggested_json": json.dumps(suggested, indent=2)}
    except Exception as e:
        return {"suggested_json": defn_str, "parse_error": str(e)}


class PutDefinitionRequest(BaseModel):
    json_definition: str


@app.put("/api/semantic-layer/definitions/{definition_id}")
def put_semantic_definition(definition_id: str, req: PutDefinitionRequest):
    """Update a definition's JSON. Optionally bump version."""
    _ensure_semantic_layer_tables()
    raw = (req.json_definition or "").strip()
    try:
        json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(400, detail=f"Invalid JSON: {e}")
    status_clause = ""
    rows = execute_sql(
        f"SELECT status FROM {fq('metric_view_definitions')} WHERE definition_id = {_safe_sql_str(definition_id)}"
    )
    if rows and rows[0].get("status") == "applied":
        status_clause = ", status = 'validated'"
    execute_sql(
        f"UPDATE {fq('metric_view_definitions')} SET json_definition = {_safe_sql_str(raw)}{status_clause} "
        f"WHERE definition_id = {_safe_sql_str(definition_id)}"
    )
    new_status = "validated" if status_clause else (rows[0]["status"] if rows else "unknown")
    return {"definition_id": definition_id, "updated": True, "status": new_status}


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
    try:
        yaml_body = _definition_to_yaml(defn)
    except (KeyError, ValueError, TypeError) as e:
        raise HTTPException(400, detail=f"Malformed metric view definition: {e}")
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


@app.post("/api/semantic-layer/definitions/{definition_id}/drop")
def drop_metric_view(definition_id: str, req: MetricViewCreateRequest):
    """Drop the applied UC metric view and reset status to validated."""
    row = _fetch_definition(definition_id)
    defn = (
        json.loads(row["json_definition"])
        if isinstance(row["json_definition"], str)
        else row["json_definition"]
    )
    mv_name = defn.get("name", row.get("metric_view_name", ""))
    if not mv_name:
        raise HTTPException(400, detail="Cannot determine metric view name")
    fq_name = f"`{req.target_catalog}`.`{req.target_schema}`.`{mv_name}`"
    try:
        execute_sql(f"DROP VIEW IF EXISTS {fq_name}")
    except Exception as e:
        raise HTTPException(400, detail=f"DROP failed: {e}")
    execute_sql(
        f"UPDATE {fq('metric_view_definitions')} SET status = 'validated', applied_at = NULL "
        f"WHERE definition_id = '{definition_id}'"
    )
    return {"dropped": True, "definition_id": definition_id, "metric_view": fq_name}


# ---------------------------------------------------------------------------
# Genie Builder endpoints
# ---------------------------------------------------------------------------


@app.post("/api/genie/generate-questions")
def genie_generate_questions(req: SuggestQuestionsRequest):
    """Generate business-user-friendly questions for the selected tables using an LLM."""
    wh = os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        raise HTTPException(500, detail="WAREHOUSE_ID not configured")
    from agent.genie_builder import GenieContextAssembler
    from langchain_community.chat_models import ChatDatabricks

    ws = get_workspace_client()
    assembler = GenieContextAssembler(ws, wh, CATALOG, SCHEMA)
    ctx = assembler.assemble(
        req.table_identifiers,
        questions=None,
        metric_view_names=req.metric_view_names or None,
    )

    if req.purpose == "metric_views":
        prompt = f"""You are a business intelligence strategist. Your task is to generate questions that would drive the creation of reusable KPI metric views.

Below is metadata about available tables and their business context.

{ctx.get('context_text', '')}

Generate exactly {req.count} questions that a BUSINESS LEADER would ask to track organizational performance. Rules:
- Think about what a CEO, CFO, VP, or department head would ask in a weekly review meeting
- Focus on measurable outcomes: revenue growth, cost efficiency, customer satisfaction, operational throughput, quality metrics
- Frame questions around time-based trends ("How has X changed over the past quarter?"), comparisons ("Which segment leads in Y?"), and thresholds ("Are we meeting our Z target?")
- Prefer questions that naturally decompose into a measure (SUM, AVG, COUNT) and dimensions (time, category, region)
- Do NOT mention column names, table names, or SQL concepts
- Use the language of the business domain, not the data model

Return ONLY a JSON array of strings, no other text."""
    else:
        prompt = f"""You are a data analyst helping business users explore their data through a natural language SQL interface (Databricks Genie).

Below is metadata about the available tables, columns, relationships, and metric views.

{ctx.get('context_text', '')}

Generate exactly {req.count} questions that a BUSINESS USER would naturally ask. Rules:
- Questions should be outcome-oriented and insight-driven (e.g. "What are the top performing regions by revenue this quarter?")
- Do NOT reference column names, table names, or technical schema details
- Focus on trends, comparisons, rankings, anomalies, and KPIs
- Vary the question types: aggregations, time-series trends, top-N, filters, comparisons
- Use natural business language a non-technical executive would use

Return ONLY a JSON array of strings, no other text."""

    llm = ChatDatabricks(endpoint=req.model_endpoint, temperature=0.7, max_tokens=2048)
    response = llm.invoke(prompt)
    content = response.content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        content = content.rsplit("```", 1)[0]
    questions = json.loads(content)
    return {"questions": questions[:req.count]}


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
            ctx = assembler.assemble(
                req.table_identifiers,
                req.questions or None,
                metric_view_names=req.metric_view_names or None,
            )

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
        valid_ids = set()
        for i, tbl in enumerate(ds.get("tables", [])):
            if not tbl.get("identifier"):
                errors.append(f"data_sources.tables[{i}] missing 'identifier'")
            else:
                valid_ids.add(tbl["identifier"])
        for i, mv in enumerate(ds.get("metric_views", [])):
            if not mv.get("identifier"):
                errors.append(f"data_sources.metric_views[{i}] missing 'identifier'")
            else:
                valid_ids.add(mv["identifier"])
        # Join spec references must be in data_sources
        for i, j in enumerate(ss.get("instructions", {}).get("join_specs", [])):
            left_id = j.get("left", {}).get("identifier") if isinstance(j.get("left"), dict) else None
            right_id = j.get("right", {}).get("identifier") if isinstance(j.get("right"), dict) else None
            if left_id and left_id not in valid_ids:
                errors.append(f"join_specs[{i}].left.identifier '{left_id}' not in data_sources")
            if right_id and right_id not in valid_ids:
                errors.append(f"join_specs[{i}].right.identifier '{right_id}' not in data_sources")
    inst = ss.get("instructions", {})
    if not isinstance(inst, dict):
        errors.append("'instructions' must be a dict")
    return errors


def _validate_data_sources_exist(ss: dict, warehouse_id: str) -> list[str]:
    """Run SELECT 1 FROM identifier LIMIT 1 for each data source; return list of errors."""
    errors = []
    ds = ss.get("data_sources", {})
    identifiers = [t.get("identifier") for t in ds.get("tables", []) if t.get("identifier")]
    identifiers += [m.get("identifier") for m in ds.get("metric_views", []) if m.get("identifier")]
    for ident in identifiers:
        quoted = ".".join(f"`{p}`" for p in ident.split("."))
        try:
            execute_sql(f"SELECT 1 FROM {quoted} LIMIT 1", warehouse_id=warehouse_id, timeout=15)
        except HTTPException as e:
            errors.append(f"data_sources identifier '{ident}': {e.detail}")
        except Exception as e:
            errors.append(f"data_sources identifier '{ident}': {e}")
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

    exist_errors = _validate_data_sources_exist(transformed, wh)
    if exist_errors:
        raise HTTPException(400, detail="Data source validation failed: " + "; ".join(exist_errors))

    def _do_genie_request(space_json):
        body = {
            "title": req.title,
            "warehouse_id": wh,
            "serialized_space": json.dumps(space_json),
        }
        if req.description:
            body["description"] = req.description
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
            err_str = str(e)
            m = re.search(r"Cannot find field: (\w+)", err_str)
            if m:
                bad_field = m.group(1)
                logger.warning(
                    "Attempt %d: stripping unknown field '%s'", attempt + 1, bad_field
                )
                _strip_field(transformed, bad_field)
                continue
            if "parse export proto" in err_str.lower() or "failed to parse" in err_str.lower():
                inst = transformed.get("instructions", {})
                if inst.get("join_specs"):
                    logger.warning("Attempt %d: stripping join_specs due to proto parse error", attempt + 1)
                    inst["join_specs"] = []
                    continue
            break
    logger.error("Genie create/update failed: %s", last_err)
    raise HTTPException(500, detail=f"Failed to create/update Genie space: {last_err}")


# ---------------------------------------------------------------------------
# Genie Space tracking endpoints
# ---------------------------------------------------------------------------

def _ensure_genie_tracking_table():
    try:
        execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {fq('genie_spaces')} (
                space_id STRING, title STRING, tables ARRAY<STRING>,
                config_json STRING, version INT,
                created_at TIMESTAMP, updated_at TIMESTAMP, deleted_at TIMESTAMP
            )
        """, timeout=30)
    except Exception as e:
        logger.warning("Could not create genie_spaces tracking table: %s", e)


@app.get("/api/genie/spaces")
def list_genie_spaces():
    _ensure_genie_tracking_table()
    return execute_sql(f"""
        SELECT space_id, title, tables, version, created_at, updated_at
        FROM {fq('genie_spaces')}
        WHERE deleted_at IS NULL
        ORDER BY updated_at DESC
    """)


@app.post("/api/genie/spaces/track")
def track_genie_space(space_id: str, title: str, tables: list[str], config_json: str = ""):
    _ensure_genie_tracking_table()
    execute_sql(f"""
        INSERT INTO {fq('genie_spaces')}
        VALUES ('{space_id}', '{title}', ARRAY({','.join(f"'{t}'" for t in tables)}),
                '{config_json.replace("'", "''")}', 1, current_timestamp(), current_timestamp(), NULL)
    """, timeout=30)
    return {"ok": True}


@app.delete("/api/genie/spaces/{space_id}")
def delete_genie_space(space_id: str):
    _ensure_genie_tracking_table()
    try:
        ws = get_workspace_client()
        ws.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
    except Exception as e:
        logger.warning("Could not delete Genie space %s from Databricks: %s", space_id, e)
    execute_sql(f"UPDATE {fq('genie_spaces')} SET deleted_at = current_timestamp() WHERE space_id = '{space_id}'", timeout=30)
    return {"ok": True, "space_id": space_id}


# ---------------------------------------------------------------------------
# Metadata Intelligence Agent endpoints
# ---------------------------------------------------------------------------

VS_ENDPOINT = os.environ.get("VECTOR_SEARCH_ENDPOINT", "dbxmetagen-vs")
VS_INDEX_SUFFIX = os.environ.get("VECTOR_SEARCH_INDEX", "metadata_vs_index")


class AgentChatRequest(BaseModel):
    message: str
    history: list = []
    mode: str = "quick"


@app.post("/api/agent/chat")
async def agent_chat(req: AgentChatRequest):
    try:
        from agent.metadata_agent import run_metadata_agent
    except ImportError as e:
        raise HTTPException(503, detail=f"Agent not available: {e}")
    mode = req.mode if req.mode in ("quick", "deep") else "quick"
    try:
        result = await run_metadata_agent(req.message, history=req.history, mode=mode)
        return result
    except Exception as exc:
        msg = str(exc)
        if "REQUEST_LIMIT_EXCEEDED" in msg or "429" in msg or "RateLimitError" in msg:
            raise HTTPException(429, detail="Model rate limit exceeded. Try again shortly.") from exc
        logger.error("Metadata agent error: %s", exc)
        raise HTTPException(500, detail=f"Agent error: {msg}") from exc


@app.get("/api/agent/stats")
def agent_stats():
    """Summary statistics for the agent landing page."""
    stats = {}
    try:
        rows = execute_sql(f"SELECT COUNT(*) AS cnt FROM {fq('table_knowledge_base')}")
        stats["tables_profiled"] = rows[0]["cnt"] if rows else 0
    except Exception:
        stats["tables_profiled"] = 0
    try:
        rows = execute_sql(f"SELECT COUNT(DISTINCT entity_type) AS cnt FROM {fq('ontology_entities')}")
        stats["entity_types"] = rows[0]["cnt"] if rows else 0
    except Exception:
        stats["entity_types"] = 0
    try:
        rows = execute_sql(f"SELECT COUNT(*) AS cnt FROM {fq('fk_predictions')} WHERE final_confidence >= 0.5")
        stats["fk_predictions"] = rows[0]["cnt"] if rows else 0
    except Exception:
        stats["fk_predictions"] = 0
    try:
        rows = execute_sql(f"SELECT COUNT(*) AS cnt FROM {fq('metric_view_definitions')} WHERE status IN ('validated','applied')")
        stats["metric_views"] = rows[0]["cnt"] if rows else 0
    except Exception:
        stats["metric_views"] = 0
    try:
        rows = execute_sql(f"SELECT doc_type, COUNT(*) AS cnt FROM {fq('metadata_documents')} GROUP BY doc_type")
        stats["vs_documents"] = sum(r["cnt"] for r in rows) if rows else 0
        stats["vs_by_type"] = {r["doc_type"]: r["cnt"] for r in rows} if rows else {}
    except Exception:
        stats["vs_documents"] = 0
        stats["vs_by_type"] = {}
    return stats


@app.get("/api/agent/suggestions")
def agent_suggestions():
    """Context-aware suggestion chips."""
    suggestions = [
        {"label": "What tables exist in my catalog?", "query": "What tables exist in my catalog?"},
        {"label": "Show me the data quality summary", "query": "Show me the data quality summary for all tables"},
    ]
    try:
        ents = execute_sql(f"SELECT COUNT(*) AS cnt FROM {fq('ontology_entities')}")
        if ents and ents[0]["cnt"] and int(ents[0]["cnt"]) > 0:
            suggestions.append({"label": "What entity types were discovered?", "query": "What entity types were discovered and which tables do they map to?"})
    except Exception:
        pass
    try:
        fks = execute_sql(f"SELECT COUNT(*) AS cnt FROM {fq('fk_predictions')} WHERE final_confidence >= 0.5")
        if fks and fks[0]["cnt"] and int(fks[0]["cnt"]) > 0:
            suggestions.append({"label": "How are my tables related?", "query": "Show me the foreign key relationships between tables"})
    except Exception:
        pass
    try:
        mvs = execute_sql(f"SELECT COUNT(*) AS cnt FROM {fq('metric_view_definitions')} WHERE status IN ('validated','applied')")
        if mvs and mvs[0]["cnt"] and int(mvs[0]["cnt"]) > 0:
            suggestions.append({"label": "What metric views are available?", "query": "List all available metric views with their source tables and measures"})
    except Exception:
        pass
    suggestions.append({"label": "Which columns contain PII or PHI?", "query": "Which columns contain PII or PHI data?"})
    return suggestions


@app.get("/api/agent/domain-stats")
def agent_domain_stats():
    """Domain-level breakdowns for the agent stats panel."""
    result = {}
    try:
        rows = execute_sql(f"SELECT domain, COUNT(*) AS cnt FROM {fq('table_knowledge_base')} GROUP BY domain ORDER BY cnt DESC LIMIT 20")
        result["tables_by_domain"] = [{"domain": r.get("domain", "unknown"), "count": int(r["cnt"])} for r in rows] if rows else []
    except Exception:
        result["tables_by_domain"] = []
    try:
        rows = execute_sql(f"SELECT entity_type, COUNT(*) AS cnt FROM {fq('ontology_entities')} GROUP BY entity_type ORDER BY cnt DESC LIMIT 20")
        result["entities_by_type"] = [{"type": r.get("entity_type", "unknown"), "count": int(r["cnt"])} for r in rows] if rows else []
    except Exception:
        result["entities_by_type"] = []
    try:
        rows = execute_sql(f"""
            SELECT t.domain, COUNT(*) AS cnt
            FROM {fq('fk_predictions')} f
            JOIN {fq('table_knowledge_base')} t ON f.src_table = t.table_name
            WHERE f.final_confidence >= 0.5
            GROUP BY t.domain ORDER BY cnt DESC LIMIT 15
        """)
        result["fk_by_domain"] = [{"domain": r.get("domain", "unknown"), "count": int(r["cnt"])} for r in rows] if rows else []
    except Exception:
        result["fk_by_domain"] = []
    return result


# ---------------------------------------------------------------------------
# Vector Search endpoints
# ---------------------------------------------------------------------------


class VectorSearchRequest(BaseModel):
    query: str
    doc_type: Optional[str] = None
    num_results: int = 5
    query_type: str = "ANN"


@app.get("/api/vector/status")
def vector_status():
    """Get VS endpoint and index status + document counts."""
    vs_index_name = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
    result: dict = {"endpoint_name": VS_ENDPOINT, "index_name": vs_index_name}
    try:
        ws = get_workspace_client()
        ep = ws.vector_search_endpoints.get_endpoint(VS_ENDPOINT)
        result["endpoint_state"] = ep.endpoint_status.state.value if ep.endpoint_status else "UNKNOWN"
    except Exception as e:
        err = str(e)
        logger.warning("VS endpoint check failed for '%s': %s", VS_ENDPOINT, err)
        if "RESOURCE_DOES_NOT_EXIST" in err or "does not exist" in err.lower() or "not found" in err.lower():
            result["endpoint_state"] = "NOT_FOUND"
        else:
            result["endpoint_state"] = "ERROR"
        result["endpoint_error"] = err
    try:
        ws = get_workspace_client()
        idx = ws.vector_search_indexes.get_index(vs_index_name)
        result["index_status"] = str(idx.status) if idx.status else "UNKNOWN"
    except Exception as e:
        err = str(e)
        if "RESOURCE_DOES_NOT_EXIST" in err or "does not exist" in err.lower() or "not found" in err.lower():
            result["index_status"] = "NOT_FOUND"
        else:
            result["index_status"] = "ERROR"
        result["index_error"] = err
    try:
        rows = execute_sql(f"SELECT doc_type, COUNT(*) AS cnt FROM {fq('metadata_documents')} GROUP BY doc_type ORDER BY cnt DESC")
        result["doc_counts"] = {r["doc_type"]: int(r["cnt"]) for r in rows} if rows else {}
        result["total_documents"] = sum(result["doc_counts"].values())
    except Exception:
        result["doc_counts"] = {}
        result["total_documents"] = 0
    return result


@app.post("/api/vector/search")
def vector_search(req: VectorSearchRequest):
    """Execute a similarity search against the metadata VS index."""
    vs_index_name = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
    try:
        from databricks.vector_search.client import VectorSearchClient
        ws = get_workspace_client()
        try:
            _token = ws.config.token
        except Exception:
            _token = None
        vsc = VectorSearchClient(workspace_url=ws.config.host, personal_access_token=_token) if _token else VectorSearchClient(workspace_url=ws.config.host)
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=vs_index_name)
        kwargs = dict(
            query_text=req.query,
            columns=["doc_id", "doc_type", "content", "table_name", "domain", "entity_type", "confidence_score"],
            num_results=min(max(req.num_results, 1), 20),
        )
        if req.doc_type:
            kwargs["filters"] = {"doc_type": req.doc_type}
        if req.query_type == "HYBRID":
            kwargs["query_type"] = "HYBRID"
        results = index.similarity_search(**kwargs)
        matches = []
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)] if cols else []
        for row in results.get("result", {}).get("data_array", []):
            if col_names:
                matches.append(dict(zip(col_names, row)))
            else:
                matches.append({"data": row})
        return {"matches": matches, "count": len(matches), "query_type": req.query_type}
    except Exception as e:
        raise HTTPException(500, detail=f"Vector search failed: {e}")


@app.post("/api/vector/sync")
def vector_sync():
    """Trigger a sync of the metadata VS index."""
    vs_index_name = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
    try:
        ws = get_workspace_client()
        ws.vector_search_indexes.sync_index(index_name=vs_index_name)
        return {"status": "sync_triggered", "index": vs_index_name}
    except Exception as e:
        raise HTTPException(500, detail=f"Sync failed: {e}")


@app.post("/api/vector/rebuild")
def vector_rebuild():
    """Rebuild requires Spark -- run the Build Vector Index job instead."""
    raise HTTPException(
        501,
        detail="Full rebuild is not available from the app. "
        "Run the Build Vector Index job from the Batch Jobs tab.",
    )


# ---------------------------------------------------------------------------
# Serve React static files (production build)
# ---------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "src", "dist")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

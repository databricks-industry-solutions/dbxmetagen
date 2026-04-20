# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Jobs & Infrastructure
# MAGIC
# MAGIC Creates the 5 essential dbxmetagen jobs, grants UC permissions, and
# MAGIC provisions a Vector Search endpoint. Supports `setup` and `teardown`.
# MAGIC
# MAGIC **Requirements**: Run **after** Notebook 01 (app must exist).

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.68.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "metadata_results", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("repo_path", "", "Repo root (e.g. /Workspace/Repos/<user>/dbxmetagen)")
dbutils.widgets.text("volume_path", "", "Volume Path (wheel built by NB01)")
dbutils.widgets.text("app_name", "dbxmetagen-app", "App Name")
dbutils.widgets.text("node_type", "i3.2xlarge", "Node Type")
dbutils.widgets.text("spark_version", "17.3.x-cpu-ml-scala2.13", "Spark Version")
dbutils.widgets.text("vs_endpoint_name", "dbxmetagen-vs", "Vector Search Endpoint")
dbutils.widgets.dropdown("mode", "setup", ["setup", "teardown"], "Mode")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
repo_path = dbutils.widgets.get("repo_path").rstrip("/")
volume_path = dbutils.widgets.get("volume_path").rstrip("/")
app_name = dbutils.widgets.get("app_name")
node_type = dbutils.widgets.get("node_type")
spark_version = dbutils.widgets.get("spark_version")
vs_endpoint_name = dbutils.widgets.get("vs_endpoint_name")
mode = dbutils.widgets.get("mode")

notebooks_path = f"{repo_path}/notebooks"

assert catalog_name, "catalog_name is required"
assert warehouse_id, "warehouse_id is required"
assert repo_path, "repo_path is required -- set to the Git folder root"
assert volume_path, "volume_path is required (wheel built by NB01)"

# COMMAND ----------

import os, glob

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute
from databricks.sdk.service.apps import App, AppResource, AppResourceJob, AppResourceJobJobPermission
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
current_user = w.current_user.me().user_name

JOB_ENV_MAP = {
    "METADATA_GENERATOR_JOB_ID": "metadata_generator_job",
    "METADATA_PARALLEL_MODES_JOB_ID": "metadata_parallel_modes_job",
    "SYNC_DDL_JOB_ID": "sync_ddl_job",
    "FULL_ANALYTICS_PIPELINE_JOB_ID": "full_analytics_pipeline_job",
    "FK_PREDICTION_JOB_ID": "fk_prediction_job",
    "SYNC_GRAPH_LAKEBASE_JOB_ID": "sync_graph_lakebase_job",
    "ONTOLOGY_PREDICTION_JOB_ID": "ontology_prediction_job",
    "KNOWLEDGE_BASE_BUILDER_JOB_ID": "knowledge_base_builder_job",
    "PROFILING_JOB_ID": "profiling_job",
    "METAGEN_WITH_KB_JOB_ID": "metagen_with_kb_job",
    "SEMANTIC_LAYER_JOB_ID": "semantic_layer_job",
    "METADATA_KB_BUILD_JOB_ID": "metadata_kb_build_job",
    "METADATA_PARALLEL_KB_BUILD_JOB_ID": "parallel_kb_build_job",
    "METADATA_SERVERLESS_JOB_ID": "metadata_serverless_job",
    "METADATA_PARALLEL_SERVERLESS_JOB_ID": "parallel_serverless_job",
    "KB_ENRICHED_MODES_JOB_ID": "kb_enriched_modes_job",
}

def generate_app_yaml(catalog, schema, bound_resource_names):
    """Generate app.yaml, using valueFrom only for resources that are bound."""
    lines = [
        "command:",
        "  - uvicorn",
        "  - api_server:app",
        "  - --host",
        "  - 0.0.0.0",
        "  - --port",
        '  - "8000"',
        "",
        "env:",
        f'  - name: CATALOG_NAME\n    value: "{catalog}"',
        f'  - name: SCHEMA_NAME\n    value: "{schema}"',
        "  - name: WAREHOUSE_ID\n    valueFrom: sql_warehouse",
        '  - name: GRAPHRAG_MODEL\n    value: "databricks-claude-sonnet-4-6"',
        '  - name: NODE_TYPE\n    value: "i3.2xlarge"',
    ]
    for env_name, res_name in JOB_ENV_MAP.items():
        if res_name in bound_resource_names:
            lines.append(f"  - name: {env_name}\n    valueFrom: {res_name}")
        else:
            lines.append(f'  - name: {env_name}\n    value: ""')
    lines += [
        '  - name: VECTOR_SEARCH_ENDPOINT\n    value: "dbxmetagen-vs"',
        '  - name: VECTOR_SEARCH_INDEX\n    value: "metadata_vs_index"',
        '  - name: LLM_MODEL\n    value: "databricks-claude-sonnet-4-6"',
        '  - name: MLFLOW_TRACE_TIMEOUT_SECONDS\n    value: "120"',
    ]
    return "\n".join(lines) + "\n"

# Resolve wheel path
whls = sorted(glob.glob(f"{volume_path}/dbxmetagen-*.whl"))
assert whls, f"No wheel found in {volume_path}"
whl_volume_path = whls[-1]
print(f"Wheel: {whl_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve App Service Principal

# COMMAND ----------

try:
    app_info = w.apps.get(app_name)
except NotFound:
    if mode == "teardown":
        print(f"App '{app_name}' not found -- will skip SPN-dependent teardown steps")
        app_info = None
    else:
        raise AssertionError(
            f"App '{app_name}' not found. Run Notebook 01 first to create the app."
        )

app_spn_id = getattr(app_info, "service_principal_id", None) if app_info else None
app_spn_uuid = getattr(app_info, "service_principal_client_id", None) if app_info else None

if not app_spn_uuid and app_spn_id:
    sp = w.service_principals.get(app_spn_id)
    app_spn_uuid = sp.application_id

if mode != "teardown":
    assert app_spn_uuid, (
        "Could not resolve app SPN UUID. Run Notebook 01 first to create the app."
    )

print(f"App SPN UUID: {app_spn_uuid}")
print(f"App SPN numeric ID: {app_spn_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Definition Helpers

# COMMAND ----------

whl_lib = compute.Library(whl=whl_volume_path)
graphframes_lib = compute.Library(pypi=compute.PythonPyPiLibrary(package="graphframes"))

def make_cluster(key, min_w=2, max_w=4, num_w=None):
    spec = compute.ClusterSpec(
        spark_version=spark_version,
        node_type_id=node_type,
        data_security_mode=compute.DataSecurityMode.SINGLE_USER,
    )
    if num_w is not None:
        spec.num_workers = num_w
    else:
        spec.autoscale = compute.AutoScale(min_workers=min_w, max_workers=max_w)
    return jobs.JobCluster(job_cluster_key=key, new_cluster=spec)

def nb_task(key, notebook, params, deps=None, extra_libs=None):
    libs = [whl_lib] + (extra_libs or [])
    return jobs.Task(
        task_key=key,
        max_retries=1,
        job_cluster_key="cluster",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{notebooks_path}/{notebook}",
            base_parameters=params),
        libraries=libs,
        depends_on=[jobs.TaskDependency(task_key=d) for d in (deps or [])])

def make_acl():
    return [jobs.JobAccessControlRequest(
        service_principal_name=app_spn_uuid,
        permission_level=jobs.JobPermissionLevel.CAN_MANAGE_RUN)]

def jp(name, default):
    return jobs.JobParameterDefinition(name=name, default=str(default))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 1: Metadata Generator (single mode)

# COMMAND ----------

def build_metadata_generator_job():
    params = [
        jp("table_names", "none"), jp("mode", "comment"),
        jp("catalog_name", catalog_name), jp("schema_name", schema_name),
        jp("current_user", current_user), jp("permission_groups", "none"),
        jp("permission_users", "none"), jp("run_id", "{{job.run_id}}"),
        jp("include_previously_failed_tables", "false"),
        jp("apply_ddl", "false"), jp("use_kb_comments", "false"),
        jp("ontology_bundle", ""), jp("domain_config_path", ""),
        jp("sample_size", "5"), jp("include_lineage", "false"),
        jp("model", "databricks-claude-sonnet-4-6"),
    ]
    base = {p.name: f"{{{{job.parameters.{p.name}}}}}" for p in params}
    tasks = [nb_task("generate_metadata", "generate_metadata.py", base)]
    return (f"{app_name}_metadata_job", tasks, params,
            [make_cluster("cluster")], "metadata_generator_job", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 2: Parallel Modes (comment -> PI + domain)

# COMMAND ----------

def build_parallel_modes_job():
    params = [
        jp("table_names", "none"), jp("catalog_name", catalog_name),
        jp("schema_name", schema_name), jp("apply_ddl", "false"),
        jp("current_user", current_user), jp("permission_groups", "none"),
        jp("permission_users", "none"), jp("run_id", "{{job.run_id}}"),
        jp("include_previously_failed_tables", "false"),
        jp("use_kb_comments", "false"), jp("ontology_bundle", ""),
        jp("domain_config_path", ""), jp("sample_size", "5"),
        jp("include_lineage", "false"),
        jp("model", "databricks-claude-sonnet-4-6"),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    common = {
        "table_names": ref("table_names"),
        "catalog_name": ref("catalog_name"),
        "schema_name": ref("schema_name"),
        "apply_ddl": ref("apply_ddl"),
        "current_user": ref("current_user"),
        "permission_groups": ref("permission_groups"),
        "permission_users": ref("permission_users"),
        "run_id": ref("run_id"),
        "cleanup_control_table": "false",
        "include_previously_failed_tables": ref("include_previously_failed_tables"),
        "ontology_bundle": ref("ontology_bundle"),
        "domain_config_path": ref("domain_config_path"),
        "sample_size": ref("sample_size"),
        "include_lineage": ref("include_lineage"),
        "model": ref("model"),
    }
    tasks = [
        nb_task("generate_comments", "generate_metadata.py",
                {**common, "mode": "comment"}),
        nb_task("generate_pi", "generate_metadata.py",
                {**common, "mode": "pi", "include_existing_table_comment": "true",
                 "use_kb_comments": ref("use_kb_comments")},
                deps=["generate_comments"]),
        nb_task("generate_domain", "generate_metadata.py",
                {**common, "mode": "domain", "include_existing_table_comment": "true",
                 "use_kb_comments": ref("use_kb_comments")},
                deps=["generate_comments"]),
    ]
    return (f"{app_name}_parallel_modes_job", tasks, params,
            [make_cluster("cluster")], "metadata_parallel_modes_job", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 3: Full Analytics Pipeline (16 tasks, 7 stages)

# COMMAND ----------

def build_analytics_pipeline_job():
    params = [
        jp("catalog_name", catalog_name), jp("schema_name", schema_name),
        jp("similarity_threshold", "0.8"), jp("max_edges_per_node", "10"),
        jp("generate_comments", "true"), jp("cluster_min_k", "2"),
        jp("cluster_max_k", "15"), jp("cluster_node_types", "table"),
        jp("ontology_bundle", "healthcare"), jp("domain_config_path", ""),
        jp("incremental", "true"),
        jp("model", "databricks-claude-sonnet-4-6"),
        jp("table_names", ""), jp("max_ai_candidates", "200"),
        jp("rule_score_min_for_ai", "0.50"),
        jp("max_candidates_per_table_pair", "5"),
        jp("apply_ddl", "false"),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    cat_sch = {"catalog_name": ref("catalog_name"), "schema_name": ref("schema_name")}
    cat_sch_tbl = {**cat_sch, "table_names": ref("table_names")}

    tasks = [
        # Stage 1: Knowledge bases
        nb_task("build_knowledge_base", "build_knowledge_base.py", cat_sch_tbl),
        nb_task("build_column_kb", "build_column_kb.py", cat_sch_tbl),
        nb_task("build_schema_kb", "build_schema_kb.py",
                {**cat_sch_tbl, "generate_comments": ref("generate_comments")},
                deps=["build_knowledge_base"]),
        nb_task("extract_extended_metadata", "extract_extended_metadata.py",
                {**cat_sch_tbl, "incremental": ref("incremental")},
                deps=["build_knowledge_base"]),
        # Stage 2: Knowledge graph
        nb_task("build_knowledge_graph", "build_knowledge_graph.py", cat_sch_tbl,
                deps=["build_column_kb", "build_schema_kb", "extract_extended_metadata"],
                extra_libs=[graphframes_lib]),
        # Stage 3: Parallel branches
        nb_task("run_profiling", "run_profiling.py",
                {**cat_sch_tbl, "incremental": ref("incremental")},
                deps=["build_knowledge_base"]),
        nb_task("build_ontology", "build_ontology.py",
                {**cat_sch_tbl, "ontology_bundle": ref("ontology_bundle"),
                 "domain_config_path": ref("domain_config_path"),
                 "incremental": ref("incremental"), "model": ref("model"),
                 "apply_ddl": ref("apply_ddl")},
                deps=["build_knowledge_base", "build_column_kb"]),
        nb_task("generate_embeddings", "generate_embeddings.py", cat_sch,
                deps=["build_knowledge_graph", "build_ontology"]),
        # Stage 4: Secondary
        nb_task("build_similarity_edges", "build_similarity_edges.py",
                {**cat_sch, "similarity_threshold": ref("similarity_threshold"),
                 "max_edges_per_node": ref("max_edges_per_node")},
                deps=["generate_embeddings"]),
        nb_task("cluster_analysis", "cluster_analysis.py",
                {**cat_sch, "min_k": ref("cluster_min_k"),
                 "max_k": ref("cluster_max_k"),
                 "node_types": ref("cluster_node_types")},
                deps=["generate_embeddings"]),
        nb_task("compute_data_quality", "compute_data_quality.py", cat_sch,
                deps=["run_profiling"]),
        nb_task("validate_ontology", "validate_ontology.py", cat_sch,
                deps=["build_ontology"]),
        # Stage 5: FK prediction + ontology refresh
        nb_task("predict_foreign_keys", "predict_foreign_keys.py",
                {**cat_sch, "column_similarity_threshold": "0.75",
                 "table_similarity_threshold": "0.7", "confidence_threshold": "0.7",
                 "apply_ddl": ref("apply_ddl"), "dry_run": "false",
                 "incremental": ref("incremental"),
                 "max_ai_candidates": ref("max_ai_candidates"),
                 "rule_score_min_for_ai": ref("rule_score_min_for_ai"),
                 "max_candidates_per_table_pair": ref("max_candidates_per_table_pair")},
                deps=["build_similarity_edges"]),
        nb_task("refresh_ontology_edges", "refresh_ontology_edges.py",
                {**cat_sch_tbl, "ontology_bundle": ref("ontology_bundle"),
                 "model": ref("model")},
                deps=["predict_foreign_keys", "build_ontology"]),
        # Stage 6: Final analysis
        nb_task("final_analysis", "final_analysis.py", cat_sch,
                deps=["refresh_ontology_edges", "cluster_analysis",
                      "compute_data_quality", "validate_ontology"]),
        # Stage 7: Vector index
        nb_task("build_vector_index", "build_vector_index.py",
                {**cat_sch, "endpoint_name": "dbxmetagen-vs"},
                deps=["final_analysis"]),
    ]
    return (f"{app_name}_full_analytics_pipeline", tasks, params,
            [make_cluster("cluster")], "full_analytics_pipeline_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 4: Knowledge Base Builder

# COMMAND ----------

def build_kb_job():
    params = [jp("catalog_name", catalog_name), jp("schema_name", schema_name)]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    cat_sch = {"catalog_name": ref("catalog_name"), "schema_name": ref("schema_name")}
    tasks = [
        nb_task("build_knowledge_base", "build_knowledge_base.py", cat_sch),
        nb_task("build_knowledge_graph", "build_knowledge_graph.py", cat_sch,
                deps=["build_knowledge_base"], extra_libs=[graphframes_lib]),
    ]
    return (f"{app_name}_knowledge_base_job", tasks, params,
            [make_cluster("cluster", num_w=1)], "knowledge_base_builder_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 5: Sync Reviewed DDL

# COMMAND ----------

def build_sync_ddl_job():
    params = [
        jp("reviewed_file_name", ""),
        jp("mode", "comment"),
        jp("current_user_override", current_user),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    tasks = [
        nb_task("sync_reviewed_ddl", "sync_reviewed_ddl.py", {
            "reviewed_file_name": ref("reviewed_file_name"),
            "mode": ref("mode"),
            "current_user_override": ref("current_user_override"),
        }),
    ]
    return (f"{app_name}_sync_ddl_job", tasks, params,
            [make_cluster("cluster", min_w=1, max_w=2)], "sync_ddl_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Jobs

# COMMAND ----------

ESSENTIAL_JOBS = [
    build_metadata_generator_job,
    build_parallel_modes_job,
    build_analytics_pipeline_job,
    build_kb_job,
    build_sync_ddl_job,
]

if mode == "teardown":
    print("=== Teardown mode -- skipping job creation ===")
else:
    existing_jobs = {j.settings.name: j for j in w.jobs.list()
                     if j.settings and j.settings.name}
    created_jobs = {}

    for builder in ESSENTIAL_JOBS:
        name, tasks, params, clusters, res_name, max_concurrent = builder()
        acl = make_acl()
        if name in existing_jobs:
            jid = existing_jobs[name].job_id
            w.jobs.reset(
                job_id=jid,
                new_settings=jobs.JobSettings(
                    name=name, tasks=tasks, parameters=params,
                    job_clusters=clusters,
                    max_concurrent_runs=max_concurrent))
            w.jobs.set_permissions(
                job_id=str(jid),
                access_control_list=acl)
            created_jobs[res_name] = jid
            print(f"  Updated: {name} (id={jid})")
        else:
            result = w.jobs.create(
                name=name, tasks=tasks, parameters=params,
                job_clusters=clusters, access_control_list=acl,
                max_concurrent_runs=max_concurrent)
            created_jobs[res_name] = result.job_id
            print(f"  Created: {name} (id={result.job_id})")

    print(f"\n{len(created_jobs)} essential jobs ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update App Resources with New Job IDs

# COMMAND ----------

if mode == "setup" and created_jobs:
    from databricks.sdk.service.apps import (
        AppResourceSqlWarehouse, AppResourceSqlWarehouseSqlWarehousePermission,
        AppDeployment, AppDeploymentMode,
    )
    wh = AppResource(name="sql_warehouse", sql_warehouse=AppResourceSqlWarehouse(
        id=warehouse_id,
        permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE))
    job_res = [AppResource(name=rn, job=AppResourceJob(
        id=str(jid), permission=AppResourceJobJobPermission.CAN_MANAGE_RUN))
        for rn, jid in created_jobs.items()]

    all_res = [wh] + job_res
    app_obj = App(name=app_name, resources=all_res)
    w.apps.update(name=app_name, app=app_obj)
    print(f"Updated app resources with {len(job_res)} job bindings")

    deploy_dir = f"/Workspace/Users/{current_user}/.dbxmetagen_deploy/{app_name}"
    bound_names = {r.name for r in all_res}
    app_yaml = generate_app_yaml(catalog_name, schema_name, bound_names)
    with open(f"{deploy_dir}/app.yaml", "w") as f:
        f.write(app_yaml)
    print(f"Regenerated app.yaml ({len(bound_names)} bound resources)")

    print("Redeploying app to pick up new resource bindings...")
    w.apps.deploy_and_wait(
        app_name=app_name,
        app_deployment=AppDeployment(
            source_code_path=deploy_dir,
            mode=AppDeploymentMode.SNAPSHOT))
    print("Redeploy complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UC Grants

# COMMAND ----------

def run_sql(statement):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="30s")
    state = resp.status.state.value
    print(f"  [{state}] {statement[:90]}")
    return state

if mode == "setup":
    print("=== Granting UC permissions ===")
    run_sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
    for grant in [
        f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{app_spn_uuid}`",
        f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_spn_uuid}`",
        f"GRANT CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_spn_uuid}`",
        f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_spn_uuid}`",
        f"GRANT MODIFY ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_spn_uuid}`",
    ]:
        run_sql(grant)
elif mode == "teardown":
    if app_spn_uuid:
        print("=== Revoking UC permissions ===")
        for revoke in [
            f"REVOKE USE CATALOG ON CATALOG `{catalog_name}` FROM `{app_spn_uuid}`",
            f"REVOKE USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `{app_spn_uuid}`",
            f"REVOKE CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `{app_spn_uuid}`",
            f"REVOKE SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `{app_spn_uuid}`",
            f"REVOKE MODIFY ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `{app_spn_uuid}`",
        ]:
            run_sql(revoke)
    else:
        print("=== Skipping UC revoke (app SPN not available) ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search Endpoint

# COMMAND ----------

if mode == "setup":
    try:
        ep = w.vector_search_endpoints.get_endpoint(vs_endpoint_name)
        print(f"VS endpoint '{vs_endpoint_name}' exists (state={ep.endpoint_status.state})")
    except NotFound:
        print(f"Creating VS endpoint '{vs_endpoint_name}'...")
        w.vector_search_endpoints.create_endpoint(
            name=vs_endpoint_name, endpoint_type="STANDARD")
        print("Creation requested -- may take a few minutes to come online")
elif mode == "teardown":
    try:
        w.vector_search_endpoints.delete_endpoint(vs_endpoint_name)
        print(f"Deleted VS endpoint '{vs_endpoint_name}'")
    except NotFound:
        print(f"VS endpoint '{vs_endpoint_name}' not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown: Delete Jobs

# COMMAND ----------

if mode == "teardown":
    print("=== Deleting essential jobs ===")
    existing_jobs = {j.settings.name: j for j in w.jobs.list()
                     if j.settings and j.settings.name}
    for builder in ESSENTIAL_JOBS:
        jname = builder()[0]
        if jname in existing_jobs:
            w.jobs.delete(job_id=existing_jobs[jname].job_id)
            print(f"  Deleted: {jname}")
        else:
            print(f"  Not found: {jname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown: Drop Schema (optional)

# COMMAND ----------

if mode == "teardown":
    dbutils.widgets.dropdown("confirm_drop_schema", "no", ["no", "yes"],
                             "DROP SCHEMA CASCADE? (destroys all tables)")
    if dbutils.widgets.get("confirm_drop_schema") == "yes":
        run_sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema_name}` CASCADE")
        print("Schema dropped")
    else:
        print("Schema preserved (set confirm_drop_schema=yes to drop)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

if mode == "setup":
    print("=" * 60)
    print("VALIDATION CHECKLIST")
    print("=" * 60)

    # App
    try:
        a = w.apps.get(app_name)
        status = getattr(a, "status", None) or getattr(a, "compute_status", "unknown")
        print(f"  [OK] App '{app_name}' exists (status={status})")
    except NotFound:
        print(f"  [FAIL] App '{app_name}' not found")

    # Jobs
    all_names = {j.settings.name for j in w.jobs.list() if j.settings and j.settings.name}
    for builder in ESSENTIAL_JOBS:
        jname = builder()[0]
        status = "OK" if jname in all_names else "MISSING"
        print(f"  [{status}] Job '{jname}'")

    # VS endpoint
    try:
        ep = w.vector_search_endpoints.get_endpoint(vs_endpoint_name)
        state = ep.endpoint_status.state if ep.endpoint_status else "UNKNOWN"
        print(f"  [OK] VS endpoint '{vs_endpoint_name}' (state={state})")
    except NotFound:
        print(f"  [FAIL] VS endpoint '{vs_endpoint_name}' not found")

    print("=" * 60)
    print("Done. If any jobs were missing from the app, re-run Notebook 01")
    print("with mode=deploy to wire them in.")

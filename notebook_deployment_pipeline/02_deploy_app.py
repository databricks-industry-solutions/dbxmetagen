# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy dbxmetagen App & Grant Permissions
# MAGIC
# MAGIC Deploys or destroys the dbxmetagen Databricks App using the SDK, then
# MAGIC grants the app's service principal permissions on jobs and Unity Catalog.
# MAGIC
# MAGIC Run this **after** Notebook 01 (jobs must exist so the app can bind to them).
# MAGIC
# MAGIC **Requirements**: the dbxmetagen repo cloned as a Git folder.

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.68.0 hatchling tomli -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, glob, shutil, subprocess, sys, time
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "metadata_results", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("repo_path", "", "Repo root (e.g. /Workspace/Repos/<user>/dbxmetagen)")
dbutils.widgets.text("volume_path", "", "Volume for wheel (e.g. /Volumes/cat/sch/vol)")
dbutils.widgets.text("app_name", "dbxmetagen-app", "App Name")
dbutils.widgets.dropdown("enable_obo", "false", ["false", "true"], "Enable OBO")
dbutils.widgets.dropdown("mode", "deploy", ["deploy", "destroy"], "Mode")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
repo_path = dbutils.widgets.get("repo_path").rstrip("/")
volume_path = dbutils.widgets.get("volume_path").rstrip("/")
app_name = dbutils.widgets.get("app_name")
enable_obo = dbutils.widgets.get("enable_obo")
mode = dbutils.widgets.get("mode")

assert catalog_name, "catalog_name is required"
assert warehouse_id, "warehouse_id is required"
assert repo_path, "repo_path is required -- set to the Git folder root"
assert volume_path, "volume_path is required -- wheel is copied here for job clusters"
assert os.path.exists(f"{repo_path}/pyproject.toml"), (
    f"pyproject.toml not found at {repo_path}. Is this the dbxmetagen repo root?"
)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as jobs_svc
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppDeploymentMode,
    AppResource,
    AppResourceJob,
    AppResourceJobJobPermission,
    AppResourceSqlWarehouse,
    AppResourceSqlWarehouseSqlWarehousePermission,
)
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
_current_user = w.current_user.me().user_name
deploy_dir = f"/Workspace/Users/{_current_user}/.dbxmetagen_deploy/{app_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedded Templates

# COMMAND ----------

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
    "SETUP_MCP_SERVERS_JOB_ID": "setup_mcp_servers_job",
}

def generate_app_yaml(catalog, schema, bound_resource_names, obo="false"):
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
        f'  - name: ENABLE_OBO\n    value: "{obo}"',
        '  - name: MLFLOW_TRACE_TIMEOUT_SECONDS\n    value: "120"',
    ]
    return "\n".join(lines) + "\n"

_req_template_path = f"{repo_path}/apps/dbxmetagen-app/app/requirements.txt.template"
if os.path.exists(_req_template_path):
    with open(_req_template_path) as _f:
        REQUIREMENTS_TEMPLATE = _f.read()
    print(f"Loaded requirements template from repo: {_req_template_path}")
else:
    REQUIREMENTS_TEMPLATE = """\
fastapi>=0.115.0
uvicorn>=0.32.0
databricks-sdk==0.68.0
databricks-langchain>=0.2.0
langchain-core>=0.3.0
langgraph>=0.2.0
requests>=2.25.0
pydantic>=2.9.0
pyyaml>=6.0
cachetools>=5.3.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
openpyxl>=3.1.0
databricks-vectorsearch>=0.40
sqlparse>=0.5.0
./__WHL_NAME__
"""
    print("WARNING: requirements.txt.template not found in repo, using embedded fallback")

USER_API_SCOPES = [
    "files.files",
    "serving.serving-endpoints",
    "sql.statement-execution",
]

RESOURCE_TO_JOB_SUFFIX = {
    "metadata_generator_job": "metadata_job",
    "metadata_parallel_modes_job": "parallel_modes_job",
    "sync_ddl_job": "sync_ddl_job",
    "full_analytics_pipeline_job": "full_analytics_pipeline",
    "fk_prediction_job": "fk_prediction",
    "sync_graph_lakebase_job": "sync_graph_lakebase",
    "ontology_prediction_job": "ontology_prediction",
    "knowledge_base_builder_job": "knowledge_base_job",
    "profiling_job": "profiling_job",
    "metagen_with_kb_job": "metadata_with_kb_job",
    "semantic_layer_job": "semantic_layer",
    "metadata_kb_build_job": "metadata_kb_build_job",
    "parallel_kb_build_job": "parallel_kb_build_job",
    "metadata_serverless_job": "metadata_serverless_job",
    "parallel_serverless_job": "parallel_serverless_job",
    "kb_enriched_modes_job": "kb_enriched_modes_job",
    "setup_mcp_servers_job": "setup_mcp_servers",
}

# COMMAND ----------

if mode == "destroy":
    print(f"=== Destroying app: {app_name} ===")
    try:
        w.apps.stop_and_wait(name=app_name)
        print("  App stopped")
    except Exception as e:
        print(f"  Stop skipped: {e}")
    try:
        w.apps.delete(name=app_name)
        print("  App deleted")
    except NotFound:
        print("  App not found (already deleted)")
    if os.path.exists(deploy_dir):
        shutil.rmtree(deploy_dir)
        print(f"  Cleaned up {deploy_dir}")
    print("Done. Run Notebook 01 with mode=teardown to clean up jobs and infra.")
    dbutils.notebook.exit("destroyed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Wheel
# MAGIC
# MAGIC Builds from the Git folder with a deploy timestamp so the app platform
# MAGIC always reinstalls. The build runs in `/tmp` to avoid modifying the repo.

# COMMAND ----------

with open(f"{repo_path}/pyproject.toml", "rb") as f:
    base_version = tomllib.load(f)["project"]["version"]
deploy_version = f"{base_version}+{int(time.time())}"

build_dir = "/tmp/dbxmetagen_build"
dist_dir = "/tmp/dbxmetagen_dist"
for d in (build_dir, dist_dir):
    if os.path.exists(d):
        shutil.rmtree(d)

shutil.copytree(f"{repo_path}/src", f"{build_dir}/src")
shutil.copy(f"{repo_path}/pyproject.toml", f"{build_dir}/pyproject.toml")
shutil.copy(f"{repo_path}/README.md", f"{build_dir}/README.md")
if os.path.exists(f"{repo_path}/LICENSE.md"):
    shutil.copy(f"{repo_path}/LICENSE.md", f"{build_dir}/LICENSE.md")

for cfg in ("variables.yml", "variables.advanced.yml"):
    repo_cfg = f"{repo_path}/{cfg}"
    pkg_cfg = f"{build_dir}/src/dbxmetagen/{cfg}"
    if os.path.exists(repo_cfg):
        shutil.copy(repo_cfg, pkg_cfg)

pyproject = f"{build_dir}/pyproject.toml"
with open(pyproject) as f:
    content = f.read()
content = content.replace(f'version = "{base_version}"', f'version = "{deploy_version}"')
with open(pyproject, "w") as f:
    f.write(content)

subprocess.check_call(
    [sys.executable, "-m", "pip", "wheel", "--no-deps",
     "--no-build-isolation", "--wheel-dir", dist_dir, build_dir],
    stdout=subprocess.DEVNULL,
)

whl_path = glob.glob(f"{dist_dir}/dbxmetagen-*.whl")[0]
whl_name = os.path.basename(whl_path)
print(f"Built wheel: {whl_name} (version {deploy_version})")

os.makedirs(volume_path, exist_ok=True)
shutil.copy(whl_path, f"{volume_path}/{whl_name}")
print(f"Copied wheel to {volume_path}/{whl_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage App Source

# COMMAND ----------

app_source_dir = f"{repo_path}/apps/dbxmetagen-app/app"
assert os.path.exists(app_source_dir), (
    f"App source not found at {app_source_dir}. Check repo_path."
)

if os.path.exists(deploy_dir):
    shutil.rmtree(deploy_dir)
shutil.copytree(app_source_dir, deploy_dir)
print(f"Copied app source to {deploy_dir}")

shutil.copy(whl_path, f"{deploy_dir}/{whl_name}")
print(f"Staged wheel: {whl_name}")

configs_src = f"{repo_path}/configurations"
if os.path.exists(configs_src):
    shutil.copytree(configs_src, f"{deploy_dir}/configurations", dirs_exist_ok=True)
    print("Copied configurations/ from repo")
else:
    print("WARNING: configurations/ not found in repo root.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate requirements.txt

# COMMAND ----------

reqs = REQUIREMENTS_TEMPLATE.replace("__WHL_NAME__", whl_name)
with open(f"{deploy_dir}/requirements.txt", "w") as f:
    f.write(reqs)
print(f"Generated requirements.txt (wheel: {whl_name})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look Up Existing Jobs
# MAGIC
# MAGIC Finds jobs created by Notebook 01 (or a prior bundle deploy) so the app
# MAGIC can bind to them on its first deploy -- no redeploy needed.

# COMMAND ----------

all_jobs = {j.settings.name: j.job_id
            for j in w.jobs.list() if j.settings and j.settings.name}

job_resources = []
bound_job_ids = {}
missing_jobs = []
for res_name, suffix in RESOURCE_TO_JOB_SUFFIX.items():
    expected_name = f"{app_name}_{suffix}"
    if expected_name in all_jobs:
        jid = all_jobs[expected_name]
        job_resources.append(AppResource(
            name=res_name,
            job=AppResourceJob(
                id=str(jid),
                permission=AppResourceJobJobPermission.CAN_MANAGE_RUN)))
        bound_job_ids[res_name] = jid
    else:
        missing_jobs.append((res_name, expected_name))

print(f"Found {len(job_resources)} jobs, {len(missing_jobs)} not yet created")
if missing_jobs:
    for res_name, expected in missing_jobs:
        print(f"  missing: {res_name} -> {expected}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate app.yaml

# COMMAND ----------

wh_resource = AppResource(
    name="sql_warehouse",
    sql_warehouse=AppResourceSqlWarehouse(
        id=warehouse_id,
        permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE))

all_resources = [wh_resource] + job_resources
bound_names = {r.name for r in all_resources}

app_yaml = generate_app_yaml(catalog_name, schema_name, bound_names, obo=enable_obo)
with open(f"{deploy_dir}/app.yaml", "w") as f:
    f.write(app_yaml)
print(f"Generated app.yaml ({len(bound_names)} bound resources)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update App

# COMMAND ----------

app_obj = App(
    name=app_name,
    description="dbxmetagen dashboard: metadata review, profiling, ontology, and graph analytics",
    resources=all_resources,
    user_api_scopes=USER_API_SCOPES,
)

try:
    existing = w.apps.get(app_name)
    print(f"App '{app_name}' exists -- updating resources")
    w.apps.update(name=app_name, app=app_obj)
except NotFound:
    print(f"Creating app '{app_name}'")
    w.apps.create_and_wait(app=app_obj)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy and Start

# COMMAND ----------

print(f"Deploying from {deploy_dir} ...")
deployment = w.apps.deploy_and_wait(
    app_name=app_name,
    app_deployment=AppDeployment(
        source_code_path=deploy_dir,
        mode=AppDeploymentMode.SNAPSHOT))
print(f"Deployment complete: {deployment.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve App SPN and Grant Permissions
# MAGIC
# MAGIC Now that the app exists, grant its service principal `CAN_MANAGE_RUN` on
# MAGIC all jobs and the necessary UC catalog/schema permissions.

# COMMAND ----------

app_info = w.apps.get(app_name)
app_spn_id = getattr(app_info, "service_principal_id", None)
app_spn_uuid = getattr(app_info, "service_principal_client_id", None)

if not app_spn_uuid and app_spn_id:
    sp = w.service_principals.get(app_spn_id)
    app_spn_uuid = sp.application_id

print(f"App SPN UUID:       {app_spn_uuid}")
print(f"App SPN numeric ID: {app_spn_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant CAN_MANAGE_RUN on jobs

# COMMAND ----------

if app_spn_uuid and bound_job_ids:
    acl = [jobs_svc.JobAccessControlRequest(
        service_principal_name=app_spn_uuid,
        permission_level=jobs_svc.JobPermissionLevel.CAN_MANAGE_RUN)]
    for res_name, jid in bound_job_ids.items():
        w.jobs.update_permissions(job_id=str(jid), access_control_list=acl)
        print(f"  Granted CAN_MANAGE_RUN on {res_name} (id={jid})")
elif not app_spn_uuid:
    print("WARNING: Could not resolve app SPN -- skipping job ACLs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant UC permissions

# COMMAND ----------

def run_sql(statement):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="30s")
    state = resp.status.state.value
    print(f"  [{state}] {statement[:90]}")
    return state

if app_spn_uuid:
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
else:
    print("WARNING: Could not resolve app SPN -- skipping UC grants")
    print("  Grant manually after the app SPN is available.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant CAN_USE on Vector Search endpoint

# COMMAND ----------

if app_spn_uuid:
    vs_endpoint_name = "dbxmetagen-vs"
    try:
        from databricks.sdk.service.iam import AccessControlRequest, Permission
        vs_ep = w.vector_search_endpoints.get_endpoint(vs_endpoint_name)
        ep_id = vs_ep.id
        if ep_id:
            w.permissions.update(
                request_object_type="vector-search-endpoints",
                request_object_id=ep_id,
                access_control_list=[AccessControlRequest(
                    service_principal_name=app_spn_uuid,
                    permission_level=Permission.CAN_USE)])
            print(f"  Granted CAN_USE on VS endpoint '{vs_endpoint_name}' to app SPN")
        else:
            print(f"  WARNING: VS endpoint '{vs_endpoint_name}' has no numeric ID -- grant CAN_USE manually")
    except NotFound:
        print(f"  VS endpoint '{vs_endpoint_name}' not found -- skipping grant")
    except Exception as e:
        print(f"  WARNING: Could not grant VS endpoint permissions: {e}")
else:
    print("Skipping VS endpoint grant (no app SPN resolved)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print(f"App Name:               {app_name}")
print(f"App URL:                Open in Workspace > Apps > {app_name}")
print(f"Service Principal ID:   {app_spn_id}")
print(f"SPN Client ID (UUID):   {app_spn_uuid}")
print(f"Source staged at:       {deploy_dir}")
print(f"Wheel version:          {deploy_version}")
print(f"Wheel in Volume:        {volume_path}/{whl_name}")
print(f"Jobs bound:             {len(bound_job_ids)}")
print(f"Jobs missing:           {len(missing_jobs)}")
print("=" * 60)
if not app_spn_uuid:
    print("\nWARNING: SPN not resolved -- grant job ACLs and UC perms manually.")

# Notebook Deployment Pipeline

Deploy dbxmetagen entirely from Databricks notebooks -- no CLI, no CI/CD,
no local tooling required.

## When to Use

Use these notebooks when the target environment **cannot run the
Databricks CLI** (no local machine access, no CI runner, no web terminal).
The notebooks use the Databricks Python SDK to create the app, jobs, and
infrastructure directly.

If you **can** run the CLI, use `deploy.sh` (recommended) or the manual
instructions in `docs/MANUAL_DEPLOYMENT.md` instead.

## Prerequisites

- **PyPI access** from the cluster (the first cell installs
  `databricks-sdk==0.68.0` and `hatchling` via `%pip install`;
  if your cluster is air-gapped, pre-install these on the cluster
  image or via an init script)
- **The dbxmetagen repo cloned as a Git folder** in the workspace
  (e.g. `/Workspace/Repos/<user>/dbxmetagen`)
- A UC Volume to store the built wheel (job clusters reference it)
- A catalog with `CREATE SCHEMA` permission
- A SQL warehouse ID

## How It Works

1. **NB01 builds the wheel** from the Git folder source using
   `pip wheel` -- no pre-built artifacts or GitHub Release downloads needed.
   The version is stamped with a deploy timestamp (same as `deploy.sh`)
   so the app platform always reinstalls on each deploy. It then creates
   the 8 essential jobs and provisions a Vector Search endpoint.

2. **NB01 loads `variables.yml`** from the repo and uses it as the source
   of truth for job parameter defaults (model, sample_size, ontology_bundle,
   etc.). This keeps notebook-deployed jobs in sync with DAB deployments.

3. **NB02 stages the app** by copying the app source from
   `{repo_path}/apps/dbxmetagen-app/app/`, `configurations/` from the
   repo root, and a freshly-built wheel into
   `/Workspace/Users/<you>/.dbxmetagen_deploy/<app_name>/`. It also
   builds its own wheel copy for the app `requirements.txt`.

4. **NB02 creates the app** and binds it to the jobs created by NB01.
   Since the jobs already exist, all resource bindings are wired on the
   first deploy -- **no redeploy is needed**. After deploying, NB02
   grants the app's service principal `CAN_MANAGE_RUN` on all jobs and
   UC catalog/schema permissions.

## Execution Order

### Initial Deploy

1. **Run `01_deploy_jobs_and_infra.py`** with `mode=setup`
   - Builds the wheel from source with deploy timestamp
   - Copies wheel to UC Volume for job cluster libraries
   - Creates 8 essential jobs via SDK (without SPN ACLs)
   - Provisions a Vector Search endpoint

2. **Run `02_deploy_app.py`** with `mode=deploy`
   - Builds the wheel (app-specific copy with deploy timestamp)
   - Stages app source + wheel + configurations to workspace
   - Discovers existing jobs and binds them as app resources
   - Creates the app and deploys -- single deploy, all bindings wired
   - Resolves the app SPN and grants `CAN_MANAGE_RUN` on all jobs
   - Grants UC permissions and VS endpoint `CAN_USE` to the app SPN

### Update

Re-run both notebooks in the same order. Each run builds a fresh wheel
with a new timestamp, so the app platform always picks up the latest code.

### Teardown

1. **Run `02_deploy_app.py`** with `mode=destroy`
   - Stops and deletes the app
   - Cleans up staged source files

2. **Run `01_deploy_jobs_and_infra.py`** with `mode=teardown`
   - Deletes jobs, removes VS endpoint
   - Optionally drops the metadata schema (set `confirm_drop_schema=yes`)

## Differences from `deploy.sh` / DABs

This pipeline is a simplified alternative. The following features from the
full DABs deployment are **not replicated**:

### Only 8 of 16+ jobs created

The notebooks create the 8 most essential jobs (6 classic + 2 serverless).
The remaining jobs are not created:

| Missing Job | Dashboard Feature Affected |
|---|---|
| `profiling_job` | Standalone profiling runs from the dashboard |
| `ontology_prediction_job` | Standalone ontology prediction |
| `fk_prediction_job` | Standalone FK prediction |
| `semantic_layer_job` | Semantic layer / Genie builder |
| `sync_graph_lakebase_job` | Graph-to-Lakebase sync |
| `metagen_with_kb_job` | KB-enriched single-mode generation |
| `metadata_kb_build_job` | Metadata + KB build combo |
| `parallel_kb_build_job` | Parallel modes + KB build combo |
| `kb_enriched_modes_job` | KB-enriched parallel modes |

The app will start and work for core features (metadata generation,
review, analytics pipeline, MCP servers). Dashboard buttons for missing
jobs will show errors or be unavailable.

To add more jobs, deploy once via `deploy.sh` or bundle (which creates
all 16+ jobs), then re-run NB02 to wire them to the app.

### No email notifications

DABs jobs include `email_notifications.on_failure` set to the deploying
user. The notebook-created jobs do not configure email notifications.
Add them manually via the Jobs UI after deployment if needed.

### No app-level permissions

DABs generates an app permissions block from `permission_groups` and
`permission_users` variables, granting `CAN_USE` to specified groups and
users. The notebook pipeline does not set app permissions -- only the
deploying user (and workspace admins) can access the app.

To grant access after deployment, use the Apps UI or SDK:

```python
w.apps.update(name="dbxmetagen-app", app=App(
    name="dbxmetagen-app",
    permissions=[AppPermission(group_name="data-team", permission="CAN_USE")],
))
```

## Widget Reference

### 01_deploy_jobs_and_infra.py

| Widget | Required | Default | Notes |
|--------|----------|---------|-------|
| `catalog_name` | Yes | | UC catalog for metadata tables |
| `schema_name` | Yes | `metadata_results` | Schema for metadata output |
| `warehouse_id` | Yes | | SQL warehouse (used for teardown schema drop) |
| `repo_path` | Yes | | Repo root, e.g. `/Workspace/Repos/<user>/dbxmetagen` |
| `volume_path` | Yes | | UC Volume for the built wheel (job libraries) |
| `app_name` | No | `dbxmetagen-app` | Prefix for job names |
| `node_type` | No | `i3.2xlarge` | Instance type for job clusters |
| `spark_version` | No | `17.3.x-cpu-ml-scala2.13` | DBR version for job clusters |
| `vs_endpoint_name` | No | `dbxmetagen-vs` | Vector Search endpoint name |
| `policy_id` | No | | Cluster policy ID for classic job clusters |
| `budget_policy_id` | No | | Serverless budget policy ID for cost attribution |
| `mode` | No | `setup` | `setup` or `teardown` |

### 02_deploy_app.py

| Widget | Required | Default | Notes |
|--------|----------|---------|-------|
| `catalog_name` | Yes | | Must match NB01 |
| `schema_name` | Yes | `metadata_results` | Must match NB01 |
| `warehouse_id` | Yes | | SQL warehouse for the app and UC grants |
| `repo_path` | Yes | | Must match NB01 |
| `volume_path` | Yes | | Must match NB01 -- wheel location |
| `app_name` | No | `dbxmetagen-app` | Databricks App name |
| `enable_obo` | No | `false` | Enable On-Behalf-Of user authentication |
| `mode` | No | `deploy` | `deploy` or `destroy` |

## Jobs Created

The notebooks create 8 essential jobs (out of 16+ available via bundle):

| Job | Purpose |
|-----|---------|
| `<app>_metadata_job` | Single-mode metadata generation |
| `<app>_parallel_modes_job` | Comment -> PI + Domain pipeline |
| `<app>_full_analytics_pipeline` | 18-task analytics DAG |
| `<app>_knowledge_base_job` | KB + knowledge graph build |
| `<app>_sync_ddl_job` | Apply reviewed DDL changes |
| `<app>_metadata_serverless_job` | Serverless single-mode generation + KB |
| `<app>_parallel_serverless_job` | Serverless parallel modes + KB |
| `<app>_setup_mcp_servers` | MCP server setup (single-node) |

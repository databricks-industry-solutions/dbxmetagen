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

- **DBR 16.2+** cluster (needed for workspace file writes via `shutil`)
- **`databricks-sdk==0.68.0`** (pre-installed on DBR 16.2+, verified at
  notebook startup)
- **PyPI access** from the cluster (the wheel build installs `hatchling`;
  if your cluster is air-gapped, pre-install hatchling on the cluster
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
   so the app platform always reinstalls on each deploy.

2. **NB01 stages the app** by copying the app source from
   `{repo_path}/apps/dbxmetagen-app/app/`, `configurations/` from the
   repo root, and the freshly-built wheel into
   `/Workspace/Users/<you>/.dbxmetagen_deploy/<app_name>/`. It also copies the wheel to the UC
   Volume for job cluster library references.

3. **NB02 creates jobs** that point to notebooks at
   `{repo_path}/notebooks/`. Since the repo is a Git folder, the
   notebooks and `configurations/` are already in the right place.

## Execution Order

### Initial Deploy

1. **Run `01_deploy_app.py`** with `mode=deploy`
   - Builds the wheel from source with deploy timestamp
   - Creates the Databricks App and its service principal
   - Stages app source + wheel + configurations to workspace
   - Copies wheel to UC Volume
   - Wires any existing jobs as app resources

2. **Run `02_deploy_jobs_and_infra.py`** with `mode=setup`
   - Creates 5 essential jobs via SDK (referencing wheel from Volume)
   - Grants UC permissions to the app service principal
   - Provisions a Vector Search endpoint
   - Updates the app with the new job resource bindings and redeploys

3. **(Optional) Re-run `01_deploy_app.py`** with `mode=deploy`
   - Only needed if you want to wire additional jobs created outside
     these notebooks (e.g. from a prior bundle deploy)

### Update

Re-run both notebooks in the same order. Each run builds a fresh wheel
with a new timestamp, so the app platform always picks up the latest code.

### Teardown

1. **Run `02_deploy_jobs_and_infra.py`** with `mode=teardown`
   - Deletes jobs, revokes UC permissions, removes VS endpoint
   - Optionally drops the metadata schema (set `confirm_drop_schema=yes`)

2. **Run `01_deploy_app.py`** with `mode=destroy`
   - Stops and deletes the app
   - Cleans up staged source files

## Differences from `deploy.sh` / DABs

This pipeline is a simplified alternative. The following features from the
full DABs deployment are **not replicated**:

### Only 5 of 16+ jobs created

The notebooks create the 5 most essential jobs. The remaining 11 jobs
are not created:

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
| `metadata_serverless_job` | Serverless single-mode generation |
| `parallel_serverless_job` | Serverless parallel modes |
| `kb_enriched_modes_job` | KB-enriched parallel modes |

The app will start and work for core features (metadata generation,
review, analytics pipeline). Dashboard buttons for missing jobs will show
errors or be unavailable.

To add more jobs, deploy once via `deploy.sh` or bundle (which creates
all 16+ jobs), then re-run NB01 to wire them to the app.

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

### Hardcoded parameter defaults

DABs jobs resolve defaults from `variables.yml` (e.g.
`${var.max_ai_candidates}`, `${var.model}`). The notebook jobs use
hardcoded defaults that match the current `variables.yml` values. If your
deployment uses non-default values, update the `jp()` calls in the
notebook's job builder functions.

## Widget Reference

### 01_deploy_app.py

| Widget | Required | Default | Notes |
|--------|----------|---------|-------|
| `catalog_name` | Yes | | UC catalog for metadata tables |
| `schema_name` | Yes | `metadata_results` | Schema for metadata output |
| `warehouse_id` | Yes | | SQL warehouse for the app |
| `repo_path` | Yes | | Repo root, e.g. `/Workspace/Repos/<user>/dbxmetagen` |
| `volume_path` | Yes | | UC Volume for the built wheel (used by NB02 for job libraries) |
| `app_name` | No | `dbxmetagen-app` | Databricks App name |
| `mode` | No | `deploy` | `deploy` or `destroy` |

### 02_deploy_jobs_and_infra.py

| Widget | Required | Default | Notes |
|--------|----------|---------|-------|
| `catalog_name` | Yes | | Must match NB01 |
| `schema_name` | Yes | `metadata_results` | Must match NB01 |
| `warehouse_id` | Yes | | Must match NB01 |
| `repo_path` | Yes | | Must match NB01 -- notebooks are at `{repo_path}/notebooks/` |
| `volume_path` | Yes | | Must match NB01 -- wheel location |
| `app_name` | No | `dbxmetagen-app` | Must match NB01 |
| `node_type` | No | `i3.2xlarge` | Instance type for job clusters |
| `spark_version` | No | `17.3.x-cpu-ml-scala2.13` | DBR version for job clusters |
| `vs_endpoint_name` | No | `dbxmetagen-vs` | Vector Search endpoint name |
| `mode` | No | `setup` | `setup` or `teardown` |

## Jobs Created

The notebooks create 5 essential jobs (out of 16+ available via bundle):

| Job | Purpose |
|-----|---------|
| `<app>_metadata_job` | Single-mode metadata generation |
| `<app>_parallel_modes_job` | Comment -> PI + Domain pipeline |
| `<app>_full_analytics_pipeline` | 16-task analytics DAG |
| `<app>_knowledge_base_job` | KB + knowledge graph build |
| `<app>_sync_ddl_job` | Apply reviewed DDL changes |

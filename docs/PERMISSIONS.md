# Permissions Reference

dbxmetagen uses two separate identities at runtime: the **app service principal** (SPN) and the **job owner / deployer**. This document explains what each identity needs, how grants are applied by each deployment method, and how On-Behalf-Of (OBO) mode changes the picture.

---

## Two-Identity Model

| Identity | Created by | Used for |
|----------|-----------|----------|
| **App Service Principal** | Auto-created by Databricks Apps on first deploy | Dashboard UI: browsing metadata, triggering jobs, SQL queries, Vector Search, agent chat |
| **Job run identity** | The **deploying identity** (auto-typed), or a different service principal via the advanced path | Job execution: metadata generation, DDL writes, `ALTER TABLE`, `SET TAGS`, `AI_QUERY` calls |

The app SPN is a **read-heavy** identity -- it needs `SELECT` to browse and `MODIFY` / `CREATE TABLE` only because the UI writes to control tables (metric view definitions, agent state, semantic layer questions). The deployer identity is a **write-heavy** identity that creates and modifies tables in the target catalog during job runs.

Jobs **omit** `run_as` entirely -- it is set neither per job (`resources/jobs/*.yml`) nor at the target level (`databricks.yml`). A target-level `run_as` would also apply to the app, and DAB rejects an app whose `run_as` differs from the deploying identity. With `run_as` omitted, DAB types the **deploying identity** automatically -- correctly whether you deploy as a user *or* a service principal. (This is deliberate: a static `user_name` default broke SP deploys with `cannot be set as run_as`, and an empty `{}` fails with `run_as object must have user_name or service_principal_name fields filled` -- only omission auto-types both.)

**To run jobs as a service principal, there are two ways:**

1. **Deploy as that SP (recommended).** The jobs run as it automatically -- no `run_as` config at all. The SP needs the output-schema privileges below; if it is the catalog/schema owner (e.g. it created them) that is automatic, otherwise grant them explicitly (the same grants `grant_app_permissions.sh` applies to the app SP).

2. **Advanced -- deploy as X, run jobs as a different SP Y** (e.g. deploy as a user but run jobs as the app SP). Add `run_as: ${var.run_as}` back to the job resource(s) you want, then set `{"run_as": {"service_principal_name": "<Y-application-id>"}}` in `variable-overrides.json` (CLI) or the UI's **Configure variable overrides** editor (only the jobs you added the line to run as Y; the rest stay as the deployer). X must hold the **Service Principal: User** role on Y **before** the deploy -- workspace admins are not exempt, and without it the Jobs API rejects the job with `PERMISSION_DENIED ... must have 'servicePrincipal.user' role`. `scripts/grant_app_permissions.sh` detects an SP `run_as`, grants X that role (if X is Y's manager or an admin), gives Y the same output-schema and Vector Search permissions as the app SP plus the create privileges only jobs need (`CREATE VOLUME`, `CREATE FUNCTION`, `CREATE MATERIALIZED VIEW`), and grants Y `CAN_READ` on the bundle's workspace folder (`root_path`) -- the job notebooks and wheel live in the deployer's folder, so without it the job fails with `Unable to access the notebook ... lacks the required permissions`. Because the role must exist before the deploy, run the script once **before** the first advanced deploy as well as after (`deploy.sh` does both). Source-table access is not granted by the script. Prefer a dedicated SP over the app's own SP: the app SP is recreated (new id) if the app is deleted and recreated.

---

## App SPN Permissions

### Unity Catalog grants

The app SPN needs these grants on the metadata schema:

```sql
GRANT USE CATALOG ON CATALOG `<catalog>` TO `<SPN_UUID>`;
GRANT USE SCHEMA  ON SCHEMA  `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT CREATE TABLE ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT READ VOLUME ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT WRITE VOLUME ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
```

`CREATE TABLE` and `MODIFY` are needed because the app writes to `metric_view_definitions`, `semantic_layer_questions`, and agent state tables directly from the UI (not via jobs).

### SQL Warehouse

`CAN_USE` on the configured warehouse. This is declared in the DAB app resource (`resources/apps/dbxmetagen_app.yml`) and applied automatically by `bundle deploy`.

### Job control

`CAN_MANAGE_RUN` on all 22 bound jobs. This is declared in the DAB app resource (both as app `resources:` entries and per-job `permissions:` blocks) and applied automatically in a single deploy pass. It allows the UI to trigger and monitor job runs.

### Vector Search endpoint

`CAN_USE` on the `dbxmetagen-vs` endpoint. This is **not** managed by DAB (VS endpoints are not yet a supported DAB resource type). It is granted by `scripts/grant_app_permissions.sh` -- see the "How each deploy method handles it" section below.

**What happens without this grant:** The app does not crash on startup. Failures are mostly silent:

- `/api/vector/status` returns HTTP 200 with `endpoint_error` / `index_error` fields -- the UI shows VS as unavailable.
- The deep analysis agent's `search_metadata` tool catches the error and falls back to SQL keyword discovery. Retrieval quality is degraded (no semantic search) but the agent still responds.
- `/api/vector/search` returns HTTP 500 with `"Vector search failed: ..."` -- this is the first visible hard error a user would see.
- Semantic layer VS enrichment silently returns empty (no error surfaced).

Because most paths swallow VS errors gracefully, a missing `CAN_USE` grant can go unnoticed until a user tries explicit vector search or notices degraded agent answers. Check `/api/vector/status` to verify VS access after deployment.

### How each deploy method handles it

| Grant | `bundle deploy` + `grant_app_permissions.sh` | Notebook deploy (NB02) | Manual deploy |
|-------|----------------------------------------------|----------------------|---------------|
| UC (7 grants above) | Automatic (grants script) | Automatic | You run the SQL (see `MANUAL_DEPLOYMENT.md`) |
| SQL warehouse `CAN_USE` | Via DAB resource | Via SDK `AppResource` | Via DAB resource |
| Job `CAN_MANAGE_RUN` | Via DAB resource, single deploy pass | Via SDK `update_permissions` | Via DAB resource |
| VS endpoint `CAN_USE` | Automatic (grants script, Permissions API) | Automatic (Permissions API) | **You must grant manually** (see below) |

To grant VS endpoint `CAN_USE` manually:

```bash
# Get the numeric endpoint ID
VS_INFO=$(databricks api get /api/2.0/vector-search/endpoints/dbxmetagen-vs --profile MYPROFILE)
VS_EP_ID=$(echo "$VS_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# Grant CAN_USE to the app SPN
databricks api patch "/api/2.0/permissions/vector-search-endpoints/${VS_EP_ID}" \
    --profile MYPROFILE \
    --json "{\"access_control_list\": [{\"service_principal_name\": \"<SPN_UUID>\", \"permission_level\": \"CAN_USE\"}]}"
```

---

## Job Owner Permissions

The user or service principal that **owns** the jobs needs:

| Resource | Permission | Why |
|----------|-----------|-----|
| Target catalog / schema | `CREATE TABLE`, `ALTER TABLE`, `SET TAGS`, `SELECT`, `MODIFY` | Metadata generation creates tables, writes results, applies DDL and UC tags |
| Model serving endpoint | Access to the configured `AI_QUERY` model (e.g. `databricks-claude-sonnet-4-6`) | LLM calls for metadata, domain classification, ontology, FK judgment |
| Source tables (if different catalog) | `SELECT` | Reading sample data from tables being documented |

In most setups the deployer is a catalog admin on the metadata catalog, which covers these automatically. If the deployer is **not** a catalog admin, the grants above must be applied explicitly.

### Identity by target

| Target | Job identity |
|--------|-------------|
| `dev` / `demo` / `prod` | The **deploying identity** -- jobs omit `run_as`, so DAB auto-types the user or SP you deploy as. (Advanced: add `run_as` back to a job + set `var.run_as` to run it as a *different* SP.) |

### Running jobs as a service principal (checklist)

When jobs run as the **deploying identity** (the default -- including when you
deploy *as* a service principal) the deployer already owns the bundle folder, the
notebook MLflow experiments and the tables, so most of this is automatic; the one
thing to check is that a deploying SP can write to the output schema (it is, if the
SP owns/created it -- otherwise grant the output-schema privileges below). The
checklist applies in full to the **advanced path**, where the job identity (Y) is a
service principal **different from the deployer** (X):

| Item | Handled by | Notes |
|------|-----------|-------|
| Deployer holds **Service Principal: User** on the SP | `grant_app_permissions.sh` (run **before** the first deploy; `deploy.sh` does) | Admins are not exempt. Without it every job create fails with `must have 'servicePrincipal.user' role`. |
| Output schema + Vector Search (same as the app SP) | `grant_app_permissions.sh` | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT`, `MODIFY`, `READ/WRITE VOLUME`, VS `CAN_USE`. |
| Job-only create privileges on the output schema | `grant_app_permissions.sh` | `CREATE VOLUME` (generated_metadata volume), `CREATE FUNCTION` (MCP functions), `CREATE MATERIALIZED VIEW` (materialized metric views). The app never creates these, so the app SP doesn't need them. |
| **Can Read** on the bundle folder (`root_path`) | `grant_app_permissions.sh` (after each deploy) | Job notebooks and the wheel live in the deployer's folder. Without it: `Unable to access the notebook ...`. `bundle validate` then warns the folder has a permission not configured in the bundle -- expected. |
| MLflow benchmarking | Automatic | The notebook experiment belongs to the deployer; benchmarking falls back to `/Users/<sp-id>/dbxmetagen_benchmarks` (visible to admins, or share it), or disables itself -- it never fails the job. |
| Genie space created by `semantic_layer_job` | Automatic | Owned by the SP; shared `CAN_MANAGE` with the `genie_space_manager` job parameter (defaults to the deployer). |
| **Source tables** | Manual | `SELECT` on every table you generate metadata for; to apply DDL also `MODIFY` and `APPLY TAG`. |
| **Lakebase** (`sync_graph_lakebase_job`) | Manual | `CREATE CATALOG` on the metastore (for the Lakebase catalog) and access to the database instance. |
| Cluster / budget policies | Manual, only if set | `CAN_USE` on `policy_id` / `budget_policy_id`. |
| Notebook deployment path (`run_jobs_as_app_sp=true`) | Notebook 02 | Grants the app SP `CAN_READ` on `repo_path` and `READ VOLUME` on `volume_path`. |

**Switching an existing deployment to an SP.** Tables, metric views and functions
created earlier are owned by the deployer. The SP can write to them (`MODIFY`),
but operations that need **ownership** -- `DROP TABLE` (cleanup job),
`CREATE OR REPLACE` (similarity / cluster analysis tables, metric views) and
schema-changing overwrites -- fail until ownership moves to the SP or to a group
that contains it, for example:

```sql
-- once per existing object in the output schema
ALTER TABLE `<catalog>`.`<schema>`.`<table>` OWNER TO `<sp-application-id>`;
ALTER VIEW  `<catalog>`.`<schema>`.`<metric_view>` OWNER TO `<sp-application-id>`;
```

New objects are created (and owned) by the SP, so a fresh deployment needs no migration.

---

## On-Behalf-Of (OBO) Mode

When `enable_obo=true`, the app passes the logged-in user's access token to SQL and Files API calls instead of using the SPN. This makes catalog access honor per-user UC permissions.

### What changes with OBO

- **SQL queries** (via `execute_sql`) run under the logged-in user's identity.
- **Files API** calls use the user's token.
- The `/api/auth/check` endpoint reports the resolved user identity.

### What stays the same with OBO

- **Vector Search** always uses SPN credentials (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`), regardless of OBO state.
- **Background threads** (e.g. generation tasks started by the UI) fall back to the SPN because `ContextVar` tokens are not inherited by daemon threads.
- **Job execution** is unaffected -- jobs always run as the job owner.

### Prerequisites

1. The workspace has the **"Databricks Apps - user token passthrough"** feature (now GA; formerly the "On-Behalf-Of User Authorization" preview). Declaring `user_api_scopes` requires it.
2. Set `enable_obo=true` at deploy time to use the user token at runtime (scopes are declared regardless).

If the workspace does NOT have the feature, override `user_api_scopes` to `[]` so no scopes are declared — otherwise the deploy fails with `Databricks Apps - user token passthrough feature is not enabled for organization`.

### User API scopes

The app declares `user_api_scopes` on EVERY deploy (default in `app_variables.yml`), controlling which Databricks APIs may be called under the user's token when OBO is active at runtime. `enable_obo` selects the principal at runtime and does NOT gate this declaration.

| Deploy method | Scopes declared (default) |
|--------------|----------------|
| `bundle deploy` / `deploy.sh` | `files.files`, `serving.serving-endpoints`, `sql.statement-execution`, `dashboards.genie` |
| Notebook deploy (NB02) | `files.files`, `serving.serving-endpoints`, `sql.statement-execution`, `dashboards.genie` |

`files.files` (UC volumes), `sql.statement-execution` (SQL), and `dashboards.genie` (Genie space create/update) are the three the app actively uses under the user token. `serving.serving-endpoints` is declared for parity/future OBO serving calls (not exercised by the app today) — drop it from the `user_api_scopes` override for a minimal consent surface.

---

## End-User Access

### Who can open the app

`permission_groups` and `permission_users` in your `.env` file control **who can open the Databricks App** in the workspace. These generate `CAN_USE` entries in the DAB app resource definition.

These settings do **not** grant Unity Catalog access to the underlying tables. They only control the app-level ACL.

### UC access for end users

For end users to see metadata through the app:

- **OBO off (default):** All SQL runs as the app SPN. Users see whatever the SPN can `SELECT`. No per-user UC enforcement.
- **OBO on:** SQL runs as the logged-in user. Each user needs their own `SELECT` grants on the metadata schema (or be a member of a group with those grants).

To automatically grant groups/users `SELECT` after metadata generation, set `grant_permissions_after_creation=true` along with `permission_groups` / `permission_users` in `variables.yml`. This is a **job-time** setting that runs GRANTs after creating tables -- it is separate from the app-level `CAN_USE`.

---

## Quick Reference

| Resource | App SPN | Job Owner | End User (OBO off) | End User (OBO on) |
|----------|---------|-----------|--------------------|--------------------|
| Metadata schema (`SELECT`) | Required | Required | N/A (SPN proxies) | Required |
| Metadata schema (`MODIFY`, `CREATE TABLE`) | Required | Required | N/A | N/A |
| Metadata schema (`ALTER TABLE`, `SET TAGS`) | Not needed | Required | N/A | N/A |
| SQL warehouse | `CAN_USE` (via DAB) | N/A (jobs use clusters) | N/A | N/A |
| Jobs (15 bound jobs) | `CAN_MANAGE_RUN` (via DAB) | `CAN_MANAGE` (bundle owner) | N/A | N/A |
| VS endpoint | `CAN_USE` (manual or deploy script) | N/A | N/A | N/A |
| Model serving endpoint | Not needed | Access required for `AI_QUERY` | N/A | N/A |
| Databricks App | N/A | N/A | `CAN_USE` (via `permission_groups` / `permission_users`) | `CAN_USE` (via `permission_groups` / `permission_users`) |
| Source tables (to be documented) | Not needed | `SELECT` | N/A | N/A |

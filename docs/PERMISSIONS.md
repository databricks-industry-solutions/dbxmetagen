# Permissions Reference

dbxmetagen uses two separate identities at runtime: the **app service principal** (SPN) and the **job owner / deployer**. This document explains what each identity needs, how grants are applied by each deployment method, and how On-Behalf-Of (OBO) mode changes the picture.

---

## Two-Identity Model

| Identity | Created by | Used for |
|----------|-----------|----------|
| **App Service Principal** | Auto-created by Databricks Apps on first deploy | Dashboard UI: browsing metadata, triggering jobs, SQL queries, Vector Search, agent chat |
| **Job Owner / Deployer** | The user (or `run_as` SP) who ran `databricks bundle deploy` | Job execution: metadata generation, DDL writes, `ALTER TABLE`, `SET TAGS`, `AI_QUERY` calls |

The app SPN is a **read-heavy** identity -- it needs `SELECT` to browse and `MODIFY` / `CREATE TABLE` only because the UI writes to control tables (metric view definitions, agent state, semantic layer questions). The deployer identity is a **write-heavy** identity that creates and modifies tables in the target catalog during job runs.

In `dev` / `demo` targets, jobs run as the **job owner** (whoever created the job via `bundle deploy`). Every target sets `run_as: ${var.run_as}` in `databricks.yml`; the `run_as` variable defaults to the deploying user and can be overridden to a service principal (e.g. in `variable-overrides.json`) to pin job execution.

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
| `dev` | Job owner (the user who ran `bundle deploy`) |
| `demo` | Job owner (the user who ran `bundle deploy`) |
| `prod` | `run_as: ${var.run_as}` in `databricks.yml` (defaults to `${var.current_user}`; override to an SP) |

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

1. A workspace admin enables the **"Databricks Apps - On-Behalf-Of User Authorization"** preview (Admin Console > Previews).
2. Set `enable_obo=true` **and** override the `user_api_scopes` variable (e.g. in `variable-overrides.json`) before running `databricks bundle deploy`.

If the preview is not enabled but scopes are declared, the deploy will fail with: `Databricks Apps - user token passthrough feature is not enabled for organization`.

### User API scopes

When OBO is enabled, the app declares `user_api_scopes` that control which Databricks APIs can be called under the user's token. Scopes are supplied via the `user_api_scopes` bundle variable (default: empty = OBO off):

| Deploy method | Scopes declared |
|--------------|----------------|
| `bundle deploy` (override `user_api_scopes`) | Whatever you set, e.g. `files.files`, `sql.statement-execution`, `dashboards.genie` |
| Notebook deploy (NB02) | `files.files`, `serving.serving-endpoints`, `sql.statement-execution`, `dashboards.genie` |

`dashboards.genie` is needed for Genie Space creation/update via the OBO token. `serving.serving-endpoints` is only needed if OBO-authenticated users call model serving endpoints directly from the app UI (not currently used), so it can be omitted from the bundle variable.

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

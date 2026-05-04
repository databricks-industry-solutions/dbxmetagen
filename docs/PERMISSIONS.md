# Permissions Reference

dbxmetagen uses two separate identities at runtime: the **app service principal** (SPN) and the **job owner / deployer**. This document explains what each identity needs, how grants are applied by each deployment method, and how On-Behalf-Of (OBO) mode changes the picture.

---

## Two-Identity Model

| Identity | Created by | Used for |
|----------|-----------|----------|
| **App Service Principal** | Auto-created by Databricks Apps on first deploy | Dashboard UI: browsing metadata, triggering jobs, SQL queries, Vector Search, agent chat |
| **Job Owner / Deployer** | The user who ran `deploy.sh` or `databricks bundle deploy` | Job execution: metadata generation, DDL writes, `ALTER TABLE`, `SET TAGS`, `AI_QUERY` calls |

The app SPN is a **read-heavy** identity -- it needs `SELECT` to browse and `MODIFY` / `CREATE TABLE` only because the UI writes to control tables (metric view definitions, agent state, semantic layer questions). The deployer identity is a **write-heavy** identity that creates and modifies tables in the target catalog during job runs.

In `dev` / `demo` targets, jobs run as the **job owner** (whoever created the job via `bundle deploy`). The `prod` target adds an explicit `run_as` block in `databricks.yml.template` to pin job execution to a specific user.

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
```

`CREATE TABLE` and `MODIFY` are needed because the app writes to `metric_view_definitions`, `semantic_layer_questions`, and agent state tables directly from the UI (not via jobs).

### SQL Warehouse

`CAN_USE` on the configured warehouse. This is declared in the DAB app resource (`dbxmetagen_app.yml.template`) and applied automatically by `bundle deploy`.

### Job control

`CAN_MANAGE_RUN` on all 17 bound jobs. This is declared in the DAB app resource and applied automatically. It allows the UI to trigger and monitor job runs.

### Vector Search endpoint

`CAN_USE` on the `dbxmetagen-vs` endpoint. This is **not** managed by DAB (VS endpoints are not yet a supported DAB resource type). It must be granted separately -- see the "How each deploy method handles it" section below.

### How each deploy method handles it

| Grant | `deploy.sh` | Notebook deploy (NB02) | Manual deploy |
|-------|-------------|----------------------|---------------|
| UC (5 grants above) | Automatic, every deploy | Automatic | You run the SQL (see `MANUAL_DEPLOYMENT.md` step 7) |
| SQL warehouse `CAN_USE` | Via DAB resource | Via SDK `AppResource` | Via DAB resource |
| Job `CAN_MANAGE_RUN` | Via DAB resource + second deploy pass | Via SDK `update_permissions` | Via DAB resource + second deploy pass |
| VS endpoint `CAN_USE` | Automatic (Permissions API) | Automatic (Permissions API) | **You must grant manually** (see below) |

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
| `prod` | Explicit `run_as: user_name: ${var.current_user}` in `databricks.yml.template` |

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
2. Set `enable_obo=true` in your `<target>.env` file before running `deploy.sh`.

If the preview is not enabled but `enable_obo=true` is set, the deploy will fail with: `Databricks Apps - user token passthrough feature is not enabled for organization`.

### User API scopes by deploy method

When OBO is enabled, the app declares `user_api_scopes` that control which Databricks APIs can be called under the user's token:

| Deploy method | Scopes declared |
|--------------|----------------|
| `deploy.sh` | `files.files`, `sql.statement-execution` |
| Notebook deploy (NB02) | `files.files`, `serving.serving-endpoints`, `sql.statement-execution` |

NB02 includes `serving.serving-endpoints` because it always sets `user_api_scopes` unconditionally. `deploy.sh` omits it. In practice the serving scope is only needed if you want OBO-authenticated users to call model serving endpoints directly from the app UI (not currently used).

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
| Jobs (17 bound jobs) | `CAN_MANAGE_RUN` (via DAB) | `CAN_MANAGE` (bundle owner) | N/A | N/A |
| VS endpoint | `CAN_USE` (manual or deploy script) | N/A | N/A | N/A |
| Model serving endpoint | Not needed | Access required for `AI_QUERY` | N/A | N/A |
| Databricks App | N/A | N/A | `CAN_USE` (via `permission_groups` / `permission_users`) | `CAN_USE` (via `permission_groups` / `permission_users`) |
| Source tables (to be documented) | Not needed | `SELECT` | N/A | N/A |

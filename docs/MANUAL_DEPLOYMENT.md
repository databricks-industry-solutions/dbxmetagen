# Workspace UI Deployment (Databricks Asset Bundles)

Deploy dbxmetagen from within the Databricks workspace using a Git Folder and
the bundle editor's built-in **Deploy** button. This is a **first-class
deployment path**, a peer to the CLI `databricks bundle deploy` -- it runs the
same DAB engine and the same `artifacts.build` hook (so the wheel, app
`requirements.txt`, and `configurations/` are built for you; see Section 4).
Nothing here is a downgraded fallback: no local machine, `uv`, or shell tooling
is required, because the build runs in the workspace.

Use whichever path fits: the **CLI** for scripting/CI and repeatable
multi-workspace deploys, the **Workspace UI** for a self-contained in-workspace
workflow. They target the same bundle and are interchangeable per deploy.

> **Known rough edges (as of this writing).** This path is still being
> hardened alongside the CLI path. Two things to be aware of:
> - **The app is not started by the bundle deploy.** `bundle deploy` (CLI or UI)
>   registers/updates the app but does not deploy its source or start it. After
>   deploying, open **Workspace > Apps > dbxmetagen-app** and click **Deploy**
>   then **Start** (Section 9). Until you do, the running app reflects a previous
>   deploy -- including its old job IDs and OBO consent.
> - **Pick one target per workspace.** The bundle has `dev`, `demo`, and `prod`
>   targets, and there is a single app (`${var.app_name}`) whose `config.env`
>   job IDs are rewritten by whichever target you deploy last. Deploying more
>   than one target to the same workspace makes the app point at one target's
>   jobs while the app service principal may be granted `CAN_MANAGE_RUN` on
>   another's -- surfacing as "job unreachable" errors in the app. Standardize
>   on one target (`dev` is the bundle default) unless you deliberately want
>   parallel deployments.

---

## 1. Prerequisites

- A Databricks workspace with **Unity Catalog** enabled
- A catalog you own or have `CREATE SCHEMA` on
- A SQL warehouse ID (find in SQL Warehouses page)
- Git integration configured (Settings > Developer > Git integration)
- Access to the dbxmetagen GitHub repository

---

## 2. Clone the Repo into a Git Folder

1. In the workspace sidebar, click **Workspace**
2. Navigate to your user folder
3. Click **Create > Git Folder**
4. Paste the repository URL and click **Create Git Folder**

Your repo is now at `/Workspace/Users/<you>/dbxmetagen`.

---

## 3. Set Your Bundle Variables

`databricks.yml`, `apps/dbxmetagen-app/app/app.yaml`, and
`resources/apps/dbxmetagen_app.yml` are **static committed files** -- there is
nothing to generate from a template. You only supply per-workspace values as
**bundle variables**.

The bundle reads per-workspace values from a `variable-overrides.json` at the
path `.databricks/bundle/<target>/variable-overrides.json` (where `<target>` is
`dev`, `demo`, or `prod`). A committed `variable-overrides.example.json` at the
repo root holds the placeholder keys to copy. It contains:

```json
{
  "catalog_name": "my_catalog",
  "schema_name": "metadata_results",
  "warehouse_id": "abc123def456",
  "vs_endpoint_name": "dbxmetagen-vs",
  "enable_obo": false
}
```

> **Why this path (important for the workspace UI):** the bundle engine — whether
> driven by the CLI or the workspace Deploy button — only loads the override file
> from `.databricks/bundle/<target>/`. A plain **repo-root**
> `variable-overrides.json` is silently ignored. The `.databricks/` directory is
> git-ignored, so it is **not** part of your Git Folder clone; you create the file
> in place after cloning (steps below).

**In the workspace UI (Git Folder):** `--var` and `BUNDLE_VAR_*` are CLI-only, so
the override file is your mechanism. After cloning the Git Folder:

1. In the workspace file browser, open your bundle root
   (`/Workspace/Users/<you>/dbxmetagen`).
2. Create the folder path `.databricks/bundle/<target>/` (e.g.
   `.databricks/bundle/dev/`) if it does not exist. Use **Create > File** and
   type the full relative path `.databricks/bundle/dev/variable-overrides.json`
   — the editor creates the intermediate folders.
3. Paste the JSON above (copy it from `variable-overrides.example.json` at the
   repo root) and fill in your real `catalog_name`, `schema_name`, and
   `warehouse_id`. Save.

**From the CLI:** copy the example into place instead:

```bash
mkdir -p .databricks/bundle/dev
cp variable-overrides.example.json .databricks/bundle/dev/variable-overrides.json
# then edit that file with your values
```

Optional keys you may add: `vs_endpoint_name`, `enable_obo`, `node_type`,
`budget_policy_id`. To apply a **cluster policy**, override the whole
`metadata_job_cluster` (and/or `lakebase_job_cluster`) complex variable and add
`policy_id` + `apply_policy_default_values` -- see the example in `databricks.yml`
(a bare `policy_id` string is intentionally not a variable: the v1.8.0+ direct
deploy engine rejects an empty `policy_id`, so the whole cluster block is
substituted instead). For a service-principal run identity or OBO scopes, add the
complex variables (see `example.env` for all keys):

```json
{
  "run_as": { "service_principal_name": "your-spn-uuid" },
  "enable_obo": true,
  "user_api_scopes": ["files.files", "sql.statement-execution", "dashboards.genie"],
  "app_permissions": [{ "group_name": "your_group", "level": "CAN_USE" }]
}
```

The workspace **host** is not a bundle variable -- the Deploy button uses the
workspace you're in.

---

## 4. The App Wheel and Configurations (built automatically)

**No manual staging is required.** The `artifacts.build` hook
(`scripts/build_artifacts.sh`) builds the wheel, copies it into the app source
dir, regenerates `requirements.txt`, and stages `configurations/` -- and it runs
on **both** the CLI (`databricks bundle deploy`) **and** the workspace Deploy
button. (The workspace bundle editor's Deploy runs the same bundle engine and
executes the build hook; a fresh, timestamp-stamped wheel appears under `dist/`
in your Git Folder on each deploy.) So there is nothing to download from GitHub
Releases and no notebook to run first -- just deploy (Section 6).

If you ever need to verify the build produced its outputs, check that these
exist in your Git Folder after a deploy:

- `dist/dbxmetagen-<version>+<timestamp>-py3-none-any.whl`
- `apps/dbxmetagen-app/app/dbxmetagen-<version>+<timestamp>-py3-none-any.whl`
- `apps/dbxmetagen-app/app/requirements.txt` (its last line pins that wheel)
- `apps/dbxmetagen-app/app/configurations/`

---

## 5. Update `variables.yml` (Optional)

Most defaults in `variables.yml` work out of the box. Review and update
if needed:

| Variable | When to change |
|----------|---------------|
| `node_type` | Azure (`Standard_DS4_v2`) or GCP (`n2-highmem-8`) users -- default is AWS `i3.2xlarge` |
| `model_endpoint` | To use a different foundation model |
| `federation_mode` | Set `true` for federated catalogs (Redshift, Snowflake via UC) |
| `ontology_bundle` | Change to `healthcare`, `financial_services`, etc. |

You do **not** need to set `catalog_name`, `schema_name`, or
`warehouse_id` here -- those are already set in `databricks.yml` via the
targets block.

---

## 6. Deploy

1. Open `databricks.yml` in the workspace file editor
2. Click the **Deployments icon** (rocket) in the right sidebar
3. Select a target (e.g. `dev`)
4. Click **Deploy**
5. Review the deployment summary in the confirmation dialog
6. Click **Deploy** again to confirm

The deployment validates the bundle, creates all jobs, and deploys the app in a
**single pass** -- the app service principal receives `CAN_MANAGE_RUN` on its jobs
during the same deploy (no second pass is needed). Status appears in the Project
output window.

> **Note:** The Deploy button does not run the `artifacts.build` hook, so it uses
> whatever wheel you staged in step 4. When you deploy from the CLI
> (`databricks bundle deploy`), the wheel is built and staged automatically.

When deployment completes, deployed resources appear in the
**Bundle resources** pane.

---

## 7. Post-Deploy: Grant UC Permissions to the App

After the first deploy, the app gets a service principal. You need to
grant it access to your catalog/schema.

### Find the app SPN

1. Go to **Workspace > Apps** in the left sidebar
2. Click `dbxmetagen-app`
3. Note the **Service Principal** name (a UUID like `aaaa-bbbb-cccc-dddd`)

### Grant permissions

Run these in the **SQL Editor** or a notebook, replacing `<SPN_UUID>`
with the service principal name from above:

```sql
CREATE SCHEMA IF NOT EXISTS `<catalog>`.`<schema>`;

GRANT USE CATALOG ON CATALOG `<catalog>` TO `<SPN_UUID>`;
GRANT USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT CREATE TABLE ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT READ VOLUME ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT WRITE VOLUME ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
```

---

## 8. Post-Deploy: Vector Search Endpoint

The analytics pipeline needs a Vector Search endpoint for embeddings
and deep analysis.

### Create the endpoint

In the workspace left sidebar, go to **Compute > Vector Search** and
click **Create endpoint**. Name it `dbxmetagen-vs` (or whatever you set
for the `vs_endpoint_name` variable), select **Standard** type, and create.

Wait until the endpoint status shows **Online**.

### Grant the app SPN access

Run in SQL Editor or a notebook:

```sql
-- Find the endpoint's numeric ID from the VS page URL or API
-- Then grant via the Permissions API (use a notebook):
```

> **CLI note:** If you deploy from the CLI, `scripts/grant_app_permissions.sh`
> creates the endpoint and grants `CAN_USE` automatically. For the workspace-UI
> path, do it manually as below.

Open the Vector Search endpoint in the workspace UI, click **Permissions**, and
add the app service principal with **CAN_USE** access.

---

## 9. Start the App

1. Go to **Workspace > Apps** in the left sidebar
2. Click `dbxmetagen-app`
3. Click **Deploy** on the app (the workspace UI's app Deploy button), then
   **Start**. A bundle deploy registers the app but does not deploy its source
   or start it -- from the CLI this is `databricks bundle run dbxmetagen_app`.

The app is now accessible at the URL shown on the Apps page.

---

## 10. Updating / Redeploying

To update an existing deployment:

1. Pull latest changes from Git (right-click Git Folder > **Pull**)
2. If the wheel version changed, re-run the staging notebook (step 4)
3. Open `databricks.yml` > Deployments panel > **Deploy**

Deployment is idempotent -- it updates existing resources in place. There is no
second deploy pass: the app service principal's `CAN_MANAGE_RUN` on jobs resolves
within a single deploy.

---

## 11. Teardown

### Remove bundle resources

Open `databricks.yml` > Deployments panel > right-click the target >
**Destroy** (or redeploy with resources removed).

Alternatively, delete the app and jobs manually from their respective
workspace pages.

### Revoke UC permissions

```sql
REVOKE USE CATALOG ON CATALOG `<catalog>` FROM `<SPN_UUID>`;
REVOKE USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
REVOKE CREATE TABLE ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
REVOKE SELECT ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
REVOKE MODIFY ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
```

### Delete Vector Search endpoint

Delete from **Compute > Vector Search** in the workspace UI.

### Drop schema (optional -- DESTRUCTIVE)

Only if you want to delete **all tables** in the metadata schema:

```sql
DROP SCHEMA IF EXISTS `<catalog>`.`<schema>` CASCADE;
```

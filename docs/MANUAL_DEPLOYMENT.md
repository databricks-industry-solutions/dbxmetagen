# Manual Deployment Guide (Workspace UI)

Deploy dbxmetagen entirely from within the Databricks workspace using
Git Folders and the built-in Deploy button. No local CLI, `uv`, or
shell scripting required.

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

Create a file named `variable-overrides.json` at the repo root:

```json
{
  "catalog_name": "my_catalog",
  "schema_name": "metadata_results",
  "warehouse_id": "abc123def456"
}
```

Optional keys you may add: `vs_endpoint_name`, `app_display_name`, `enable_obo`,
`node_type`, `policy_id`, `budget_policy_id`. For a service-principal run identity
or OBO scopes, add the complex variables (see `example.env` for all keys):

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

## 4. Stage the App Wheel and Configurations

When you deploy with the CLI, the `artifacts.build` hook
(`scripts/build_artifacts.sh`) builds the wheel, copies it into the app source
dir, and regenerates `requirements.txt` automatically. **A pure workspace-UI
deploy does not run that hook**, so for the Deploy-button path you must stage the
wheel yourself. Run this in a **notebook** attached to any cluster (the cells use
`%sh` to operate on workspace files):

**Cell 1 -- Download the wheel from GitHub Releases:**

```
%sh
# Replace X.Y.Z with the release version
cd /Workspace/Users/$USER/dbxmetagen
curl -L -o "apps/dbxmetagen-app/app/dbxmetagen-X.Y.Z-py3-none-any.whl" \
  "https://github.com/<org>/dbxmetagen/releases/download/vX.Y.Z/dbxmetagen-X.Y.Z-py3-none-any.whl"
```

**Cell 2 -- Copy configurations:**

```
%sh
cd /Workspace/Users/$USER/dbxmetagen
cp -r configurations apps/dbxmetagen-app/app/configurations
```

**Cell 3 -- Generate requirements.txt:**

```
%sh
cd /Workspace/Users/$USER/dbxmetagen
WHL_NAME=$(basename apps/dbxmetagen-app/app/dbxmetagen-*.whl)
sed "s|__WHL_NAME__|${WHL_NAME}|" \
    apps/dbxmetagen-app/app/requirements.txt.template \
    > apps/dbxmetagen-app/app/requirements.txt
```

Verify that `apps/dbxmetagen-app/app/requirements.txt` ends with
`./<your-wheel-filename>.whl`.

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
3. Click **Start** (or the app may auto-start after deploy)

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

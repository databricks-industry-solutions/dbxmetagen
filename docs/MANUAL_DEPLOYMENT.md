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

## 3. Create `databricks.yml` from Template

Open `databricks.yml.template` in the workspace file editor. Duplicate
the file (right-click > Clone) or copy its contents into a new file
named `databricks.yml` in the same directory.

Replace these placeholders with your values:

| Placeholder | Replace with | Example |
|-------------|-------------|---------|
| `__DATABRICKS_HOST__` | Your workspace URL | `https://my-workspace.cloud.databricks.com` |
| `__CATALOG_NAME__` | Target catalog | `my_catalog` |
| `__SCHEMA_NAME__` | Target schema | `metadata_results` |
| `__WAREHOUSE_ID__` | SQL warehouse ID | `abc123def456` |
| `__VS_ENDPOINT_NAME__` | Vector Search endpoint name | `dbxmetagen-vs` |

Also handle the `__RUN_AS__` lines:

- **No service principal (typical):** Delete every line containing `__RUN_AS__`
- **With service principal:** Replace each `__RUN_AS__` with:

```yaml
    run_as:
      service_principal_name: "your-spn-uuid"
```

Save the file.

---

## 4. Create `app.yaml` from Template

Open `apps/dbxmetagen-app/app/app.yaml.template`. Create a copy named
`app.yaml` in the same directory (`apps/dbxmetagen-app/app/app.yaml`).

Replace these placeholders:

| Placeholder | Replace with |
|-------------|-------------|
| `__CATALOG_NAME__` | Your catalog name |
| `__SCHEMA_NAME__` | Your schema name |
| `__ENABLE_OBO__` | `false` (or `true` if workspace admin enabled On-Behalf-Of auth) |
| `__VS_ENDPOINT_NAME__` | `dbxmetagen-vs` (or your custom endpoint name) |
| `__APP_DISPLAY_NAME__` | A display name, or leave empty |

Save the file.

---

## 5. Create `dbxmetagen_app.yml` from Template

Open `resources/apps/dbxmetagen_app.yml.template`. Create a copy named
`dbxmetagen_app.yml` in the same directory
(`resources/apps/dbxmetagen_app.yml`).

Replace these placeholders:

- **`__USER_API_SCOPES__`** -- Delete this line entirely (unless OBO is
  enabled, in which case replace with):

```yaml
      user_api_scopes:
        - "files.files"
        - "sql.statement-execution"
        - "dashboards.genie"
```

- **`__APP_PERMISSIONS__`** -- Delete this line entirely (only the
  deploying user gets access). To grant access to others, replace with:

```yaml
      permissions:
        - group_name: "your_group"
          level: CAN_USE
```

Save the file.

---

## 6. Stage the App Wheel and Configurations

The app installs its own copy of the dbxmetagen wheel at runtime. You
need to place the wheel, configurations, and a requirements file into
the app source directory. Run this in a **notebook** attached to any
cluster (the cells use `%sh` to operate on workspace files):

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

## 7. Update `variables.yml` (Optional)

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

## 8. Deploy

1. Open `databricks.yml` in the workspace file editor
2. Click the **Deployments icon** (rocket) in the right sidebar
3. Select a target (e.g. `dev`)
4. Click **Deploy**
5. Review the deployment summary in the confirmation dialog
6. Click **Deploy** again to confirm

The deployment validates the bundle, builds the wheel (via the
`artifacts` block), creates all jobs, and deploys the app. Status
appears in the Project output window.

When deployment completes, deployed resources appear in the
**Bundle resources** pane.

---

## 9. Post-Deploy: Grant UC Permissions to the App

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
```

---

## 10. Post-Deploy: Vector Search Endpoint

The analytics pipeline needs a Vector Search endpoint for embeddings
and deep analysis.

### Create the endpoint

In the workspace left sidebar, go to **Compute > Vector Search** and
click **Create endpoint**. Name it `dbxmetagen-vs` (or whatever you set
in `__VS_ENDPOINT_NAME__`), select **Standard** type, and create.

Wait until the endpoint status shows **Online**.

### Grant the app SPN access

Run in SQL Editor or a notebook:

```sql
-- Find the endpoint's numeric ID from the VS page URL or API
-- Then grant via the Permissions API (use a notebook):
```

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
# The Permissions API grant is done by the deploy script normally.
# For workspace-only deploy, grant via the VS endpoint's Permissions tab in the UI.
```

Alternatively, open the Vector Search endpoint in the workspace UI,
click **Permissions**, and add the app service principal with
**CAN_USE** access.

---

## 11. Start the App

1. Go to **Workspace > Apps** in the left sidebar
2. Click `dbxmetagen-app`
3. Click **Start** (or the app may auto-start after deploy)

The app is now accessible at the URL shown on the Apps page.

---

## 12. Updating / Redeploying

To update an existing deployment:

1. Pull latest changes from Git (right-click Git Folder > **Pull**)
2. If templates changed, re-apply your edits to the generated files
   (steps 3-5)
3. If the wheel version changed, re-run the staging notebook (step 6)
4. Open `databricks.yml` > Deployments panel > **Deploy**

Deployment is idempotent -- it updates existing resources in place.

---

## 13. Second Deploy Pass (Wire SPN Permissions)

Job definitions grant `CAN_MANAGE_RUN` to the app service principal.
Since the SPN only exists after the first deploy, you need to deploy a
second time so those permissions resolve:

1. Open `databricks.yml` > Deployments panel > **Deploy**

This is the same action as step 8. The second deploy picks up the
now-existing SPN and applies job-level permissions.

---

## 14. Teardown

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

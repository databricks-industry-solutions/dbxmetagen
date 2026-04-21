# Manual Deployment Guide

Step-by-step instructions for deploying dbxmetagen when you can only use
`databricks bundle deploy` (e.g. from a CI/CD pipeline or a locked-down
workstation). Every action that `deploy.sh` performs is broken out below
as an explicit command you can run yourself.

> **No CLI at all?** If you cannot run the Databricks CLI or any local
> tooling, see `notebook_deployment_pipeline/README.md` for a fully
> notebook-based deployment using the Python SDK.

---

## 1. Prerequisites

| Tool | Minimum version | Purpose |
|------|----------------|---------|
| Databricks CLI | >= 0.283.0 | Bundle deploy, app management |
| `uv` | any | Build wheel locally (skip if downloading from releases) |
| `sed` | any | Tokenize template files (or edit by hand) |

You also need:

- A Databricks workspace with Unity Catalog enabled
- A catalog you own or have `CREATE SCHEMA` on
- A SQL warehouse ID
- A personal access token (PAT) or OAuth configured for the CLI

---

## 2. Initial Setup

```bash
# Clone the repository
git clone <repo-url> dbxmetagen && cd dbxmetagen

# Create your environment file from the template
cp example.env dev.env
```

Edit `dev.env` and fill in every value:

| Variable | Required | Example |
|----------|----------|---------|
| `DATABRICKS_HOST` | Yes | `https://my-workspace.cloud.databricks.com` |
| `catalog_name` | Yes | `my_catalog` |
| `schema_name` | Yes | `metadata_results` |
| `warehouse_id` | Yes | `abc123def456` |
| `node_type` | No | `Standard_D8s_v3` (Azure), `n2-standard-8` (GCP) |
| `permission_groups` | No | `data_analysts,data_scientists` |
| `permission_users` | No | `user1@company.com,user2@company.com` |

Load the variables into your shell:

```bash
set -a && source dev.env && set +a
```

Authenticate the CLI and confirm it works:

```bash
databricks configure --profile MYPROFILE
databricks current-user me --profile MYPROFILE
```

---

## 3. Generate Configuration Files

Three template files need placeholder replacement. For each one you can
use the `sed` command shown, or open the file and find-and-replace by hand.

### 3a. `databricks.yml`

```bash
sed -e "s|__DATABRICKS_HOST__|${DATABRICKS_HOST}|g" \
    -e "s|__CATALOG_NAME__|${catalog_name}|g" \
    -e "s|__SCHEMA_NAME__|${schema_name}|g" \
    -e "s|__WAREHOUSE_ID__|${warehouse_id}|g" \
    databricks.yml.template > databricks.yml
```

**Manual alternative:** Copy `databricks.yml.template` to `databricks.yml`
and replace the four `__PLACEHOLDER__` tokens with your values.

### 3b. `app.yaml`

```bash
sed -e "s|__CATALOG_NAME__|${catalog_name}|g" \
    -e "s|__SCHEMA_NAME__|${schema_name}|g" \
    apps/dbxmetagen-app/app/app.yaml.template \
    > apps/dbxmetagen-app/app/app.yaml
```

### 3c. `dbxmetagen_app.yml` (app resource definition)

```bash
sed 's|__APP_PERMISSIONS__||' \
    resources/apps/dbxmetagen_app.yml.template \
    > resources/apps/dbxmetagen_app.yml
```

This creates the file with **no extra permissions** (only the deploying
user gets CAN_MANAGE via the top-level bundle permissions).

To add group/user access, replace `__APP_PERMISSIONS__` with a permissions
block instead. Example with a group:

```yaml
      permissions:
        - group_name: "data_analysts"
          level: CAN_USE
```

---

## 4. Stage the App Wheel and Configurations

**How the two wheels work:** `databricks.yml` declares an `artifacts`
block that automatically runs `uv build` during `bundle deploy`. That
wheel is used by **job clusters**. The **app**, however, installs its
own copy from `requirements.txt` (which references `./dbxmetagen-*.whl`
inside the app source directory). You must stage this app copy manually.

Choose **one** option to get the wheel, then stage it:

### Option A: Download from GitHub Releases

```bash
# Replace X.Y.Z with the release version
curl -L -o "apps/dbxmetagen-app/app/dbxmetagen-X.Y.Z-py3-none-any.whl" \
  "https://github.com/<org>/dbxmetagen/releases/download/vX.Y.Z/dbxmetagen-X.Y.Z-py3-none-any.whl"
```

### Option B: Build locally then copy

```bash
uv build
cp dist/dbxmetagen-*.whl apps/dbxmetagen-app/app/
```

> **Tip:** After the first `bundle deploy` (step 6), a freshly-built
> wheel will exist in `dist/`. On subsequent deploys you can simply
> re-copy from there instead of rebuilding manually.

### Stage configurations and requirements.txt

```bash
# Copy ontology/domain configurations
cp -r configurations apps/dbxmetagen-app/app/configurations

# Generate requirements.txt from template
WHL_NAME=$(basename apps/dbxmetagen-app/app/dbxmetagen-*.whl)
sed "s|__WHL_NAME__|${WHL_NAME}|" \
    apps/dbxmetagen-app/app/requirements.txt.template \
    > apps/dbxmetagen-app/app/requirements.txt
```

Verify the generated `requirements.txt` ends with `./<your-wheel-name>.whl`.

---

## 5. First Deploy

This creates all jobs, the app, and its service principal.

```bash
databricks bundle validate -t dev --profile MYPROFILE

databricks bundle deploy -t dev --profile MYPROFILE \
    --var "deploying_user=$(databricks current-user me --profile MYPROFILE --output json | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"userName\"])')"
```

If you know your username you can pass it directly:
`--var "deploying_user=you@company.com"`.

**Azure / GCP users:** The default `node_type` is `i3.2xlarge` (AWS).
Pass your cloud's node type or job clusters will fail to provision:

```bash
--var "node_type=Standard_D8s_v3"   # Azure example
--var "node_type=n2-standard-8"     # GCP example
```

---

## 6. Find the App Service Principal

The first deploy created a service principal for the app. You need its
IDs for the next steps.

```bash
databricks apps get dbxmetagen-app --profile MYPROFILE --output json
```

Look for two fields in the JSON output:

| Field | Format | Used for |
|-------|--------|----------|
| `service_principal_id` | Numeric (e.g. `12345`) | Second deploy pass (step 6b) |
| `service_principal_client_id` | UUID (e.g. `aaaa-bbbb-...`) | UC GRANT statements (step 7) |

If `service_principal_client_id` is missing (older workspaces), resolve
it from the numeric ID:

```bash
databricks service-principals get <numeric_id> --profile MYPROFILE --output json
# Look for "applicationId" in the output
```

If neither CLI command works, find the service principal in the workspace
Admin Console under **Settings > Identity and access > Service principals**.

---

## 6b. Second Deploy (wire SPN permissions onto jobs)

The job definitions grant `CAN_MANAGE_RUN` to the app's service
principal. Now that the SPN exists, redeploy so those permissions resolve:

```bash
databricks bundle deploy -t dev --profile MYPROFILE \
    --var "deploying_user=you@company.com" \
    --var "app_service_principal_application_id=<NUMERIC_SP_ID>"
```

Use the **numeric** `service_principal_id` from step 6 (not the UUID).

Include `--var "node_type=..."` here too if you passed it in step 5.

---

## 7. Grant UC Permissions to the App Service Principal

Run these SQL statements in the Databricks SQL editor, a notebook, or
via the CLI. Replace `<SPN_UUID>` with the `service_principal_client_id`
(UUID) from step 6.

```sql
CREATE SCHEMA IF NOT EXISTS `<catalog>`.`<schema>`;

GRANT USE CATALOG ON CATALOG `<catalog>` TO `<SPN_UUID>`;
GRANT USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT CREATE TABLE ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
GRANT MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `<SPN_UUID>`;
```

Or via CLI:

```bash
for STMT in \
  "CREATE SCHEMA IF NOT EXISTS \`${catalog_name}\`.\`${schema_name}\`" \
  "GRANT USE CATALOG ON CATALOG \`${catalog_name}\` TO \`<SPN_UUID>\`" \
  "GRANT USE SCHEMA ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`<SPN_UUID>\`" \
  "GRANT CREATE TABLE ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`<SPN_UUID>\`" \
  "GRANT SELECT ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`<SPN_UUID>\`" \
  "GRANT MODIFY ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`<SPN_UUID>\`"; do
  databricks api post /api/2.0/sql/statements --profile MYPROFILE \
    --json "{\"warehouse_id\": \"${warehouse_id}\", \"statement\": \"${STMT}\", \"wait_timeout\": \"30s\"}"
done
```

---

## 8. Create Vector Search Endpoint

```bash
databricks api post /api/2.0/vector-search/endpoints --profile MYPROFILE \
    --json '{"name": "dbxmetagen-vs", "endpoint_type": "STANDARD"}'
```

Check status:

```bash
databricks api get /api/2.0/vector-search/endpoints/dbxmetagen-vs --profile MYPROFILE
```

Wait until `endpoint_status.state` is `ONLINE` before using vector search
features in the app.

---

## 9. Start the App

```bash
databricks bundle run -t dev --profile MYPROFILE \
    --var "deploying_user=you@company.com" \
    --var "app_service_principal_application_id=<NUMERIC_SP_ID>" \
    dbxmetagen_app
```

The app is now accessible at **Workspace > Apps > dbxmetagen-app**.

---

## 10. Updating an Existing Deployment

To redeploy with a new wheel or configuration changes:

1. Replace the wheel in `apps/dbxmetagen-app/app/` with the new version
2. Regenerate `requirements.txt` (step 4)
3. If configurations changed, re-copy `configurations/` (step 4)
4. Run `databricks bundle deploy` (step 5 -- skip the `validate` if you want)
5. Restart the app (step 9)

`databricks bundle deploy` is idempotent -- it updates existing resources
in place.

---

## 11. Teardown

### Remove all bundle resources (jobs + app)

```bash
databricks bundle destroy -t dev --profile MYPROFILE
```

This deletes all jobs and the app created by the bundle.

### Revoke UC permissions

```sql
REVOKE USE CATALOG ON CATALOG `<catalog>` FROM `<SPN_UUID>`;
REVOKE USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
REVOKE CREATE TABLE ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
REVOKE SELECT ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
REVOKE MODIFY ON SCHEMA `<catalog>`.`<schema>` FROM `<SPN_UUID>`;
```

### Delete Vector Search endpoint

```bash
databricks api delete /api/2.0/vector-search/endpoints/dbxmetagen-vs \
    --profile MYPROFILE
```

### Drop schema (optional -- DESTRUCTIVE)

Only run this if you want to delete **all tables** in the metadata schema:

```sql
DROP SCHEMA IF EXISTS `<catalog>`.`<schema>` CASCADE;
```

### Clean up local files

```bash
rm -f databricks.yml
rm -f apps/dbxmetagen-app/app/app.yaml
rm -f resources/apps/dbxmetagen_app.yml
rm -f apps/dbxmetagen-app/app/requirements.txt
rm -f apps/dbxmetagen-app/app/dbxmetagen-*.whl
rm -rf apps/dbxmetagen-app/app/configurations
```

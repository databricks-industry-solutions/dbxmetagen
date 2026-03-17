# Environment Variables Configuration

This document describes how to configure environment-specific values that should NOT be committed to git.

## Setup

1. Copy `example.env` to `dev.env`:
   ```bash
   cp example.env dev.env
   ```

2. Edit `dev.env` with your environment-specific values

3. Add `dev.env` to `.gitignore` (already done)

## How It Works

During deployment, `deploy.sh` will:
1. Load variables from `{target}.env` (e.g., `dev.env` for `--target dev`)
2. Generate `databricks.yml` from `databricks.yml.template`, replacing placeholders like `__CATALOG_NAME__`, `__SCHEMA_NAME__`, `__WAREHOUSE_ID__`, and `__DATABRICKS_HOST__`
3. Inject `__CATALOG_NAME__` and `__SCHEMA_NAME__` into `app.yaml` via sed replacement
4. Build the frontend and Python wheel
5. Run `databricks bundle validate` and `databricks bundle deploy`

This ensures sensitive values are never committed to git.

## Supported Environment Variables

### `DATABRICKS_HOST`
- **Used in**: `databricks.yml.template` placeholder `__DATABRICKS_HOST__`
- **Description**: Your Databricks workspace URL
- **Example**: `https://adb-123456789.9.azuredatabricks.net/`

### `TARGET`
- **Description**: Deployment target environment
- **Example**: `dev`, `staging`, `prod`

### `catalog_name`
- **Used in**: `databricks.yml.template` and `app.yaml` placeholder `__CATALOG_NAME__`
- **Description**: Unity Catalog catalog name
- **Example**: `my_catalog`

### `schema_name`
- **Used in**: `databricks.yml.template` and `app.yaml` placeholder `__SCHEMA_NAME__`
- **Description**: Unity Catalog schema for metadata results
- **Example**: `metadata_results`

### `warehouse_id`
- **Used in**: `databricks.yml.template` placeholder `__WAREHOUSE_ID__`
- **Description**: SQL warehouse ID
- **Example**: `3abb59fcfb739e0d`

### `permission_groups`
- **Description**: Comma-separated list of groups to grant read access
- **Example**: `account users, data_engineers, analysts`
- **Default**: None (no group grants)

### `permission_users`
- **Description**: Comma-separated list of users to grant read access
- **Example**: `user1@company.com, user2@company.com`
- **Default**: None (no additional user grants beyond job user)

## Best Practices

1. **Never commit `dev.env`** - It contains environment-specific and potentially sensitive values
2. **Keep `example.env` updated** - When adding new env vars, update the example
3. **Document new variables** - Add them to this file and to `example.env`
4. **Use descriptive values** - Make it clear what each variable does

## Adding New Environment Variables

To add a new environment variable:

1. Add it to `example.env` with a placeholder value
2. Add a corresponding `__PLACEHOLDER__` in `databricks.yml.template` or `app.yaml`
3. Add a `sed` replacement line in `deploy.sh`
4. Update this documentation

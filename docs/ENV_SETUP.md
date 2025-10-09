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
1. Load variables from `dev.env`
2. Merge them into `variables.yml` to create `variables.yml.merged`
3. Use the merged file for deployment
4. Clean up temporary files after deployment

This ensures sensitive values are never committed to git.

## Supported Environment Variables

### `DATABRICKS_HOST`
- **Maps to**: `workspace_host` in variables.yml
- **Description**: Your Databricks workspace URL
- **Example**: `https://adb-123456789.9.azuredatabricks.net/`

### `TARGET`
- **Description**: Deployment target environment
- **Example**: `dev`, `staging`, `prod`

### `permission_groups`
- **Maps to**: `permission_groups` in variables.yml
- **Description**: Comma-separated list of groups to grant read access
- **Example**: `account users, data_engineers, analysts`
- **Default**: None (no group grants)

### `permission_users`
- **Maps to**: `permission_users` in variables.yml
- **Description**: Comma-separated list of users to grant read access
- **Example**: `user1@company.com, user2@company.com`
- **Default**: None (no additional user grants beyond job user)

## Best Practices

1. **Never commit `dev.env`** - It contains environment-specific and potentially sensitive values
2. **Keep `example.env` updated** - When adding new env vars, update the example
3. **Document new variables** - Add them to this file and to `example.env`
4. **Use descriptive values** - Make it clear what each variable does

## Adding New Environment Variables

To add a new environment variable that overrides `variables.yml`:

1. Add it to `example.env` with a placeholder value
2. Add it to the `env_mappings` dictionary in `deploy.sh`'s `merge_env_to_variables()` function:
   ```python
   env_mappings = {
       'workspace_host': 'DATABRICKS_HOST',
       'permission_groups': 'permission_groups',
       'permission_users': 'permission_users',
       'your_new_var': 'YOUR_ENV_VAR',  # Add here
   }
   ```
3. Update this documentation

## Temporary Files

The following files are created during deployment and cleaned up automatically:
- `variables.yml.merged` - Merged configuration file
- `variables.yml.tmp` - Temporary file during merge
- `app/variables.yml` - Copy used by the app during deployment

All of these are in `.gitignore` and will not be committed.


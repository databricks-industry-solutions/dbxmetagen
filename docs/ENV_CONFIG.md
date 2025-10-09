# Environment Configuration

## Overview

Environment-specific values from `dev.env` are used in two ways:
1. **For bundle resources (jobs)**: Passed as `--var` flags to `databricks bundle`
2. **For the app**: Written to `env_overrides.yml` which the app loads at runtime

This keeps sensitive values out of git while ensuring both app and jobs use the correct configuration.

## Setup

1. **Copy the example:**
   ```bash
   cp example.env dev.env
   ```

2. **Edit with your values:**
   ```bash
   DATABRICKS_HOST=https://adb-123456789.9.azuredatabricks.net/
   TARGET=dev
   permission_groups=account users, data_engineers
   permission_users=user1@company.com, user2@company.com
   ```

3. **Deploy:**
   ```bash
   ./deploy.sh
   ```

## How It Works

### Deployment Flow

1. **Load `dev.env`:** Sources environment variables
2. **Create override files:**
   - `variables_override.yml` → Included in bundle, overrides `variables.yml` defaults
   - `env_overrides.yml` → Deployed with app for runtime config
3. **Deploy bundle:** Jobs and app are deployed with correct values
4. **Runtime:**
   - App loads `variables.yml` + `env_overrides.yml`
   - Jobs use variables from `variables.yml` + `variables_override.yml`

### Variable Mapping

| dev.env Variable | Bundle Variable | Used By |
|-----------------|-----------------|---------|
| `DATABRICKS_HOST` | `workspace_host` | App + Jobs |
| `permission_groups` | `permission_groups` | App + Jobs |
| `permission_users` | `permission_users` | App + Jobs |

## Files Created/Modified

- `dev.env` - Your local config (gitignored, never committed)
- `variables_override.yml` - Bundle variable overrides (committed as empty, regenerated on deploy)
- `app/env_overrides.yml` - Runtime overrides for app (temporary, gitignored)
- `app/deploying_user.yml` - Deploying user info (temporary, gitignored)
- `app/app_env.yml` - Target environment (temporary, gitignored)

**Note:** `variables_override.yml` is tracked in git as an empty file. During deployment, it's regenerated with your `dev.env` values. Don't commit the generated version - it will be overwritten on next deployment.

## Best Practices

✅ Keep `dev.env` local  
✅ Update `example.env` when adding new variables  
✅ Document new variables  
✅ Values work for both app UI and notebook jobs

❌ Never commit `dev.env` or override files  
❌ Don't put sensitive values in `variables.yml` defaults


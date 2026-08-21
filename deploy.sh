#!/bin/bash
# LEGACY deploy wrapper -- FULLY SUPPORTED (not going away). New users can call the
# canonical flow directly:
#   databricks bundle deploy -t <target> -p <profile>   # builds wheel via artifacts.build hook
#   databricks bundle run    -t <target> -p <profile> dbxmetagen_app   # deploy app source + start
#   scripts/grant_app_permissions.sh -t <target> -p <profile>          # UC + Vector Search grants
#
# This wrapper chains those three AND preserves the old {target}.env experience: if a
# {target}.env exists it is sourced and its scalar values are translated into bundle
# variable overrides (--var), so existing customers keep deploying exactly as before
# with no migration. It no longer GENERATES YAML (databricks.yml / app.yaml / the app
# resource are static committed files now). New users can instead put values in
# variable-overrides.json (see example.env + variable-overrides.example.json).
#
# Usage: ./deploy.sh [OPTIONS]
#   -t, --target TARGET    Bundle target (default: dev)
#   -p, --profile PROFILE  Databricks CLI profile (default: DEFAULT)
#       --no-app           Skip app source deploy + start (jobs + code still deploy)
#       --yes-frontend     Rebuild the React frontend (npm install && npm run build).
#                          DEFAULT is NO rebuild -- the built dist/ is committed and shipped as-is.
#       --no-frontend      Explicitly skip the frontend build (this is the default; kept for compat)
#       --no-vs            Skip Vector Search endpoint provisioning + grant
#   -h, --help             Show this help
set -e

# Bridge a pip proxy / private index to uv (uv does not read pip config), so existing
# customers behind a corporate proxy keep working as they did on the old flow. An
# explicit UV_INDEX_URL always wins; this only fills it in when unset.
if [ -z "${UV_INDEX_URL:-}" ]; then
    _pip_idx=$(pip3 config get global.index-url 2>/dev/null || true)
    [ -n "$_pip_idx" ] && export UV_INDEX_URL="$_pip_idx" && echo "Using pip index-url for uv build: $UV_INDEX_URL"
fi

TARGET="dev"
PROFILE="DEFAULT"
SKIP_APP=false
SKIP_FRONTEND=true   # default: do NOT rebuild the frontend (dist/ is committed); pass --yes-frontend to force
SKIP_VS=false
DEPLOY_VARS=()   # bundle-variable overrides forwarded from a legacy {target}.env, if present

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)   TARGET="$2"; shift 2 ;;
        -p|--profile)  PROFILE="$2"; shift 2 ;;
        --no-app)      SKIP_APP=true; shift ;;
        --yes-frontend) SKIP_FRONTEND=false; shift ;;
        --no-frontend) SKIP_FRONTEND=true; shift ;;
        --no-vs)       SKIP_VS=true; shift ;;
        --permissions) echo "Note: --permissions is no longer needed; grants run automatically below."; shift ;;
        -h|--help)     sed -n '15,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)             echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

echo "=================================================================="
echo " deploy.sh (legacy wrapper -- fully supported). It chains:"
echo "   databricks bundle deploy -t $TARGET -p $PROFILE"
echo "   databricks bundle run    -t $TARGET -p $PROFILE dbxmetagen_app"
echo "   scripts/grant_app_permissions.sh -t $TARGET -p $PROFILE"
echo " and translates a ${TARGET}.env (if present) into bundle vars."
echo "=================================================================="

# Legacy {target}.env support (fully supported). If present, source it and forward its
# scalar values as bundle-variable overrides so existing customers deploy exactly as
# before -- no migration needed. Sourcing also EXPORTS the values, so
# grant_app_permissions.sh (which honors exported catalog_name/schema_name/warehouse_id/
# app_name/vs_endpoint_name) picks them up too. New users: use variable-overrides.json.
ENV_FILE="${TARGET}.env"
if [ -f "$ENV_FILE" ]; then
    echo ""
    echo "=== Loading legacy ${ENV_FILE} (fully supported) ==="
    set -a; source "$ENV_FILE"; set +a
    # Scalar vars that map 1:1 to a declared bundle variable -> forward as --var.
    for _v in catalog_name schema_name warehouse_id vs_endpoint_name node_type \
              budget_policy_id enable_obo app_name app_name_suffix app_display_name model; do
        if [ -n "${!_v:-}" ]; then
            DEPLOY_VARS+=(--var "${_v}=${!_v}")
        fi
    done
    [ ${#DEPLOY_VARS[@]} -gt 0 ] && echo "  Forwarded ${#DEPLOY_VARS[@]} override(s) from ${ENV_FILE}."
    # Knobs whose SHAPE changed -- can't be a simple --var; must move to
    # variable-overrides.json (see variable-overrides.advanced.example.json).
    for _old in policy_id spn_id permission_groups permission_users; do
        if [ -n "${!_old:-}" ]; then
            echo "  NOTE: '${_old}' changed shape and was NOT forwarded -- migrate it to variable-overrides.json:"
            case "$_old" in
                policy_id)  echo "        override the whole metadata_job_cluster block (+ policy_id, apply_policy_default_values)." ;;
                spn_id)     echo "        run_as: {\"service_principal_name\": \"...\"}." ;;
                permission_groups|permission_users) echo "        app_permissions: [{group_name|user_name, level: CAN_USE}]." ;;
            esac
        fi
    done
    # user_api_scopes is declared by default now (app_variables.yml), so enabling OBO
    # needs no scope handling here -- enable_obo just flips the runtime principal.
    echo ""
fi

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Install: https://docs.databricks.com/dev-tools/cli/install.html" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

# --- Frontend build (opt-in). dist/ is committed and shipped as-is, so a rebuild is only
#     needed when the app source changed and you want it reflected in this deploy. Default
#     skips it; pass --yes-frontend to force. ---
if [ "$SKIP_FRONTEND" = false ] && [ "$SKIP_APP" = false ] && [ -f apps/dbxmetagen-app/app/src/package.json ]; then
    if command -v npm &> /dev/null; then
        echo ""
        echo "=== Rebuilding frontend (--yes-frontend) ==="
        (cd apps/dbxmetagen-app/app/src && npm install && npm run build)
    else
        echo "Note: --yes-frontend given but npm not found -- shipping the committed dist/ as-is."
    fi
elif [ "$SKIP_APP" = false ]; then
    echo "Skipping frontend rebuild (default) -- shipping the committed dist/. Pass --yes-frontend to rebuild."
fi

# --- Pre-flight: warn (do NOT fail) if no catalog_name source is visible, so a
#     customer doesn't end up with a deployed-but-mis-configured app that only
#     shows "CATALOG_NAME not set". Checks every place a value could come from. ---
_have_catalog=false
[ -n "${catalog_name:-}" ] && _have_catalog=true                       # exported (shell or sourced {target}.env)
[ -n "${BUNDLE_VAR_catalog_name:-}" ] && _have_catalog=true            # BUNDLE_VAR_* env
[ -f ".databricks/bundle/${TARGET}/variable-overrides.json" ] && _have_catalog=true
printf '%s\n' "${DEPLOY_VARS[@]}" | grep -q '^catalog_name=' && _have_catalog=true   # forwarded from {target}.env
if [ "$_have_catalog" = false ]; then
    echo ""
    echo "WARNING: no catalog_name found for target '${TARGET}'. Checked: \$catalog_name,"
    echo "         \$BUNDLE_VAR_catalog_name, .databricks/bundle/${TARGET}/variable-overrides.json,"
    echo "         and ${TARGET}.env. The deploy will still succeed, but the app will show"
    echo "         'CATALOG_NAME not set' until you configure it. Set it via any of:"
    echo "           mkdir -p .databricks/bundle/${TARGET} && \\"
    echo "             cp variable-overrides.example.json .databricks/bundle/${TARGET}/variable-overrides.json  # then edit"
    echo "           - or create ${TARGET}.env with 'catalog_name=...' (legacy)"
    echo "           - or export BUNDLE_VAR_catalog_name=..."
    echo ""
fi

# --- Deploy (wheel builds via the artifacts.build hook) ---
# The app_lifecycle variable defaults to {} (no `started` field), which is safe on
# BOTH deploy engines -- so this build never trips the terraform engine's
# "lifecycle.started is only supported in direct deployment mode" error, regardless of
# whether this bundle's state is on terraform (legacy) or direct. The `bundle run`
# step below deploys the app source AND starts it, so the started lifecycle is not
# needed here. (A complex var cannot be set via --var/BUNDLE_VAR_*; the one-step
# started:true is opt-in via variable-overrides.json on the direct engine only.)
echo ""
echo "=== bundle deploy (target=${TARGET}, profile=${PROFILE}) ==="
databricks bundle deploy -t "$TARGET" -p "$PROFILE" "${DEPLOY_VARS[@]}"

# --- Deploy app source + start ---
if [ "$SKIP_APP" = false ]; then
    echo ""
    echo "=== bundle run dbxmetagen_app (deploy app source + start) ==="
    databricks bundle run -t "$TARGET" -p "$PROFILE" "${DEPLOY_VARS[@]}" dbxmetagen_app
fi

# --- Post-deploy UC + Vector Search grants (idempotent) ---
echo ""
echo "=== grant_app_permissions.sh ==="
GRANT_ARGS=(-t "$TARGET" -p "$PROFILE")
[ "$SKIP_VS" = true ] && GRANT_ARGS+=(--no-vs)
scripts/grant_app_permissions.sh "${GRANT_ARGS[@]}"

echo ""
echo "=== Deployment complete ==="

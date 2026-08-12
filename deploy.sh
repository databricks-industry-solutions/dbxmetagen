#!/bin/bash
# DEPRECATED compatibility wrapper. Kept so existing CI/CD that calls ./deploy.sh
# keeps working. It is a thin shim around the current, canonical flow:
#   databricks bundle deploy -t <target> -p <profile>   # builds wheel via artifacts.build hook
#   databricks bundle run    -t <target> -p <profile> dbxmetagen_app   # deploy app source + start
#   scripts/grant_app_permissions.sh -t <target> -p <profile>          # UC + Vector Search grants
#
# This is NOT the old template-generating deploy.sh. It does not generate
# databricks.yml / app.yaml / app resource YAML (those are static committed
# files now), and it does NOT source a {target}.env file. Per-workspace config
# comes from bundle variable overrides -- see example.env and
# variable-overrides.example.json. New users should call the three commands
# directly; this wrapper exists only for backward compatibility.
#
# Usage: ./deploy.sh [OPTIONS]
#   -t, --target TARGET    Bundle target (default: dev)
#   -p, --profile PROFILE  Databricks CLI profile (default: DEFAULT)
#       --no-app           Skip app source deploy + start (jobs + code still deploy)
#       --no-frontend      Skip the npm frontend build (dist/ is committed anyway)
#       --no-vs            Skip Vector Search endpoint provisioning + grant
#   -h, --help             Show this help
set -e

TARGET="dev"
PROFILE="DEFAULT"
SKIP_APP=false
SKIP_FRONTEND=false
SKIP_VS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)   TARGET="$2"; shift 2 ;;
        -p|--profile)  PROFILE="$2"; shift 2 ;;
        --no-app)      SKIP_APP=true; shift ;;
        --no-frontend) SKIP_FRONTEND=true; shift ;;
        --no-vs)       SKIP_VS=true; shift ;;
        --permissions) echo "Note: --permissions is no longer needed; grants run automatically below."; shift ;;
        -h|--help)     sed -n '15,21p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)             echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

echo "=================================================================="
echo " deploy.sh is DEPRECATED -- it now just wraps the canonical flow:"
echo "   databricks bundle deploy -t $TARGET -p $PROFILE"
echo "   databricks bundle run    -t $TARGET -p $PROFILE dbxmetagen_app"
echo "   scripts/grant_app_permissions.sh -t $TARGET -p $PROFILE"
echo " Prefer calling those directly. See README 'Deploy' + example.env."
echo "=================================================================="

# The old deploy.sh sourced {target}.env. That is no longer read -- config now
# comes from bundle variable overrides. Warn loudly if one is present so a CI
# job that relied on it does not silently deploy with default catalog/schema.
ENV_FILE="${TARGET}.env"
if [ -f "$ENV_FILE" ]; then
    echo ""
    echo "WARNING: '${ENV_FILE}' exists but is NO LONGER read by this script."
    echo "  Per-workspace config now comes from bundle variable overrides:"
    echo "    .databricks/bundle/${TARGET}/variable-overrides.json  (DAB auto-loads this path)"
    echo "    or --var / BUNDLE_VAR_* environment variables."
    echo "  Migrate your ${ENV_FILE} values there. See example.env + variable-overrides.example.json."
    echo ""
fi

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Install: https://docs.databricks.com/dev-tools/cli/install.html" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

# --- Optional frontend build (dist/ is committed; only needed if the app changed) ---
if [ "$SKIP_FRONTEND" = false ] && [ "$SKIP_APP" = false ] && [ -f apps/dbxmetagen-app/app/src/package.json ]; then
    if command -v npm &> /dev/null; then
        echo ""
        echo "=== Building frontend ==="
        (cd apps/dbxmetagen-app/app/src && npm install && npm run build)
    else
        echo "Note: npm not found -- using the committed prebuilt frontend (dist/). Use --no-frontend to silence."
    fi
fi

# --- Deploy (wheel builds via the artifacts.build hook) ---
echo ""
echo "=== bundle deploy (target=${TARGET}, profile=${PROFILE}) ==="
databricks bundle deploy -t "$TARGET" -p "$PROFILE"

# --- Deploy app source + start ---
if [ "$SKIP_APP" = false ]; then
    echo ""
    echo "=== bundle run dbxmetagen_app (deploy app source + start) ==="
    databricks bundle run -t "$TARGET" -p "$PROFILE" dbxmetagen_app
fi

# --- Post-deploy UC + Vector Search grants (idempotent) ---
echo ""
echo "=== grant_app_permissions.sh ==="
GRANT_ARGS=(-t "$TARGET" -p "$PROFILE")
[ "$SKIP_VS" = true ] && GRANT_ARGS+=(--no-vs)
scripts/grant_app_permissions.sh "${GRANT_ARGS[@]}"

echo ""
echo "=== Deployment complete ==="

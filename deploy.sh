#!/bin/bash
# Deploy dbxmetagen to Databricks
# Usage: ./deploy.sh [OPTIONS]
#   --profile PROFILE    Databricks CLI profile (default: DEFAULT)
#   --target TARGET      Bundle target: dev or prod (default: dev)
#   --no-app             Skip app build, SP detection, and app start (jobs + bundle still deploy)
#   --no-frontend        Skip frontend npm install/build (use pre-built dist/ from repo)
#   --no-vs              Skip Vector Search endpoint provisioning
#   --help               Show this help

set -e

# Bridge pip proxy config to uv (internal devs have pip configured for the
# Databricks PyPI proxy; uv doesn't read pip config, so we forward it).
if [ -z "$UV_INDEX_URL" ]; then
    _pip_idx=$(pip3 config get global.index-url 2>/dev/null || true)
    [ -n "$_pip_idx" ] && export UV_INDEX_URL="$_pip_idx"
fi

TARGET="dev"
PROFILE="DEFAULT"
SKIP_APP=false
SKIP_FRONTEND=false
SKIP_VS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)     PROFILE="$2"; shift 2 ;;
        --target)      TARGET="$2"; shift 2 ;;
        --no-app)      SKIP_APP=true; shift ;;
        --no-frontend) SKIP_FRONTEND=true; shift ;;
        --no-vs)       SKIP_VS=true; shift ;;
        --permissions) echo "Note: --permissions is no longer needed; UC grants now run automatically."; shift ;;
        --help|-h)
            sed -n '2,9p' "$0"; exit 0 ;;
        *)
            echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

# --- Prerequisites ---
if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Install: https://docs.databricks.com/dev-tools/cli/install.html"
    exit 1
fi

if [ "$SKIP_FRONTEND" = false ] && [ "$SKIP_APP" = false ]; then
    if ! command -v npm &> /dev/null; then
        echo "Error: npm not found. npm is required to build the frontend app."
        echo "  Install Node.js (which includes npm): https://nodejs.org/"
        echo "  Or via brew: brew install node"
        echo "  Or skip the frontend build:  ./deploy.sh --no-frontend"
        exit 1
    fi
fi

if [ ! -f "databricks.yml.template" ]; then
    echo "Error: databricks.yml.template not found. Run from the dbxmetagen directory."
    exit 1
fi

# --- Load environment ---
ENV_FILE="${TARGET}.env"
if [ ! -f "${ENV_FILE}" ]; then
    echo "Error: ${ENV_FILE} not found. Create it from example.env:"
    echo "  cp example.env ${ENV_FILE}"
    exit 1
fi

echo "=== dbxmetagen Deployment ==="
echo "Target: ${TARGET}"
echo "Profile: ${PROFILE}"
echo "Env file: ${ENV_FILE}"

set -a
source "${ENV_FILE}"
set +a

# --- Validate required env vars ---
for VAR in DATABRICKS_HOST catalog_name schema_name warehouse_id; do
    if [ -z "${!VAR}" ]; then
        echo "Error: ${VAR} not set in ${ENV_FILE}"
        exit 1
    fi
done

echo "Host: ${DATABRICKS_HOST}"
echo "Catalog: ${catalog_name}"
echo "Schema: ${schema_name}"
echo "Warehouse: ${warehouse_id}"

# --- Authenticate ---
if ! databricks current-user me --profile "$PROFILE" &> /dev/null; then
    echo "Error: Not authenticated. Run: databricks configure --profile $PROFILE"
    exit 1
fi
CURRENT_USER=$(databricks current-user me --profile "$PROFILE" --output json | jq -r '.userName')
echo "Deploying as: ${CURRENT_USER}"

# --- Generate databricks.yml from template ---
echo ""
echo "=== Generating databricks.yml ==="
sed -e "s|__DATABRICKS_HOST__|${DATABRICKS_HOST}|g" \
    -e "s|__CATALOG_NAME__|${catalog_name}|g" \
    -e "s|__SCHEMA_NAME__|${schema_name}|g" \
    -e "s|__WAREHOUSE_ID__|${warehouse_id}|g" \
    databricks.yml.template > databricks.yml

echo "=== Generating app.yaml from template ==="
sed -e "s|__CATALOG_NAME__|${catalog_name}|g" \
    -e "s|__SCHEMA_NAME__|${schema_name}|g" \
    apps/dbxmetagen-app/app/app.yaml.template > apps/dbxmetagen-app/app/app.yaml

echo "=== Generating app resource YAML with permissions ==="
python3 -c "
import os, sys
groups = os.environ.get('permission_groups', 'none')
users = os.environ.get('permission_users', 'none')
deployer = os.environ.get('CURRENT_USER', '')
lines = []
if groups and groups.strip('\"') not in ('none', ''):
    for g in groups.strip('\"').split(','):
        g = g.strip()
        if g:
            lines.append('        - group_name: \"{}\"'.format(g))
            lines.append('          level: CAN_USE')
if users and users.strip('\"') not in ('none', ''):
    for u in users.strip('\"').split(','):
        u = u.strip()
        if u and u != deployer:
            lines.append('        - user_name: \"{}\"'.format(u))
            lines.append('          level: CAN_USE')
block = ''
if lines:
    block = '      permissions:\n' + '\n'.join(lines)
template = open('resources/apps/dbxmetagen_app.yml.template').read()
result = template.replace('__APP_PERMISSIONS__', block)
open('resources/apps/dbxmetagen_app.yml', 'w').write(result)
n_entries = len(lines) // 2
print('App permissions: {} entries'.format(n_entries))
" || { echo "ERROR: Failed to generate app resource YAML"; exit 1; }

if [ "$SKIP_APP" = false ]; then
    # --- Build frontend ---
    echo ""
    if [ "$SKIP_FRONTEND" = true ]; then
        echo "=== Skipping frontend build (--no-frontend) -- using pre-built dist/ ==="
        if [ ! -f "apps/dbxmetagen-app/app/src/dist/index.html" ]; then
            echo "ERROR: Pre-built frontend not found at apps/dbxmetagen-app/app/src/dist/"
            echo "  Run 'cd apps/dbxmetagen-app/app/src && npm install && npm run build' first, or deploy without --no-frontend."
            exit 1
        fi
    else
        echo "=== Building frontend ==="
        if [ -f "apps/dbxmetagen-app/app/src/package.json" ]; then
            (cd apps/dbxmetagen-app/app/src && npm install --silent && npm run build) || {
                echo "ERROR: Frontend build failed."
                echo "  Try running manually:  cd apps/dbxmetagen-app/app/src && npm install && npm run build"
                echo "  If npm registry is unreachable, check your network/proxy settings."
                echo "  Or skip the frontend build:  ./deploy.sh --no-frontend"
                exit 1
            }
            echo "Frontend built successfully"
        else
            echo "No frontend package.json found, skipping build"
        fi
    fi

    # --- Check for existing app SP ---
    APP_NAME=$(grep -m1 'default:.*dbxmetagen' resources/app_variables.yml | awk '{print $2}' | tr -d '"')
    APP_NAME="${APP_NAME:-dbxmetagen-app}"
    APP_SP_ID=""

    echo ""
    echo "=== Checking for existing app: ${APP_NAME} ==="
    APP_JSON=$(databricks apps get "${APP_NAME}" --profile "$PROFILE" --output json 2>&1) || {
        echo "App does not exist yet -- will be created on first deploy."
        APP_JSON=""
    }

    if [ -n "${APP_JSON}" ]; then
        APP_SP_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_id',''))" 2>/dev/null || echo "")
        if [ -n "${APP_SP_ID}" ] && [ "${APP_SP_ID}" != "None" ]; then
            echo "Found app SP ID: ${APP_SP_ID}"
        else
            APP_SP_ID=""
        fi
    fi
else
    echo ""
    echo "=== Skipping app (--no-app) ==="
    APP_SP_ID=""
fi

# --- Sync requirements.txt from lock file ---
echo ""
echo "=== Syncing requirements.txt ==="
bash scripts/export_requirements.sh

# --- Build Python package ---
echo ""
echo "=== Building Python package ==="
rm -rf dist/
uv build
echo "Python package built: $(ls dist/*.whl)"

# --- Copy configurations and wheel into app source for deployment ---
echo ""
echo "=== Copying configurations and wheel into app source ==="
cp -r configurations apps/dbxmetagen-app/app/configurations
WHL_NAME=$(basename dist/dbxmetagen-*.whl)
cp "dist/${WHL_NAME}" "apps/dbxmetagen-app/app/${WHL_NAME}"
sed "s|__WHL_NAME__|${WHL_NAME}|" \
    apps/dbxmetagen-app/app/requirements.txt.template \
    > apps/dbxmetagen-app/app/requirements.txt
echo "Wheel: ${WHL_NAME}"
cleanup() {
    rm -rf apps/dbxmetagen-app/app/configurations
    rm -f apps/dbxmetagen-app/app/dbxmetagen-*.whl
    rm -f apps/dbxmetagen-app/app/requirements.txt
    rm -f resources/apps/dbxmetagen_app.yml
}
trap cleanup EXIT

# --- Validate and deploy ---
echo ""
echo "=== Validating bundle ==="
DEPLOY_VARS=(--var "deploying_user=${CURRENT_USER}")
if [ -n "${node_type:-}" ]; then
    DEPLOY_VARS+=(--var "node_type=${node_type}")
fi
if [ -n "${APP_SP_ID}" ]; then
    DEPLOY_VARS+=(--var "app_service_principal_application_id=${APP_SP_ID}")
fi

databricks bundle validate -t "$TARGET" --profile "$PROFILE"

echo ""
echo "=== Deploying bundle ==="
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" "${DEPLOY_VARS[@]}"

if [ "$SKIP_APP" = false ]; then
    # --- First deploy: get newly created SP and redeploy with permissions ---
    if [ -z "${APP_SP_ID}" ]; then
        echo ""
        echo "=== Checking for newly created app SP ==="
        APP_JSON=$(databricks apps get "${APP_NAME}" --profile "$PROFILE" --output json 2>&1) || APP_JSON=""
        if [ -n "${APP_JSON}" ]; then
            APP_SP_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_id',''))" 2>/dev/null || echo "")
            if [ -n "${APP_SP_ID}" ] && [ "${APP_SP_ID}" != "None" ]; then
                echo "New SP detected: ${APP_SP_ID}"
                echo "Redeploying with SP permissions..."
                databricks bundle deploy -t "$TARGET" --profile "$PROFILE" \
                    --var "deploying_user=${CURRENT_USER}" \
                    --var "app_service_principal_application_id=${APP_SP_ID}"
            fi
        fi
    fi
fi

# --- Grant UC permissions to app SPN (idempotent, runs every deploy) ---
if [ -n "${APP_SP_ID}" ]; then
    echo ""
    echo "=== Granting UC permissions to app SPN ==="

    SPN_JSON=$(databricks service-principals get "${APP_SP_ID}" --profile "$PROFILE" --output json 2>&1) || {
        echo "ERROR: Failed to get service principal: ${SPN_JSON}"
        exit 1
    }
    SPN_APP_ID=$(echo "${SPN_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('applicationId',''))")

    if [ -z "${SPN_APP_ID}" ]; then
        echo "ERROR: applicationId not found for SPN ${APP_SP_ID}"
        exit 1
    fi

    echo "SPN application_id: ${SPN_APP_ID}"

    # Ensure the output schema exists before granting permissions on it
    echo "  Creating schema if not exists: ${catalog_name}.${schema_name}"
    CREATE_RESULT=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" \
        --json "{\"warehouse_id\": \"${warehouse_id}\", \"statement\": \"CREATE SCHEMA IF NOT EXISTS \`${catalog_name}\`.\`${schema_name}\`\", \"wait_timeout\": \"30s\"}" 2>&1)
    CREATE_STATUS=$(echo "${CREATE_RESULT}" | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('status',{}); print(s.get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR")
    if [ "${CREATE_STATUS}" = "SUCCEEDED" ]; then
        echo "    OK"
    else
        echo "    WARNING: Could not create schema (${CREATE_STATUS}). Grants may fail if schema does not exist."
    fi

    GRANT_STATEMENTS=(
        "GRANT USE CATALOG ON CATALOG \`${catalog_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT USE SCHEMA ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT CREATE TABLE ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT SELECT ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT MODIFY ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
    )

    GRANT_FAIL=0
    for STMT in "${GRANT_STATEMENTS[@]}"; do
        echo "  ${STMT}"
        RESULT=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" \
            --json "{\"warehouse_id\": \"${warehouse_id}\", \"statement\": \"${STMT}\", \"wait_timeout\": \"30s\"}" 2>&1)
        STATUS=$(echo "${RESULT}" | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('status',{}); print(s.get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR")
        if [ "${STATUS}" = "SUCCEEDED" ]; then
            echo "    OK"
        else
            echo "    FAILED (${STATUS})"
            echo "    ${RESULT}"
            GRANT_FAIL=1
        fi
    done
    if [ "${GRANT_FAIL}" -eq 1 ]; then
        echo ""
        echo "WARNING: One or more GRANT statements failed. The app will still start, but some features"
        echo "         may not work until permissions are fixed. Re-run deploy or grant manually via Databricks UI."
    fi
fi

# --- Ensure Vector Search endpoint ---
if [ "$SKIP_VS" = false ]; then
    VS_ENDPOINT="${vs_endpoint_name:-dbxmetagen-vs}"
    echo ""
    echo "=== Ensuring Vector Search endpoint: ${VS_ENDPOINT} ==="
    VS_CHECK=$(databricks api get "/api/2.0/vector-search/endpoints/${VS_ENDPOINT}" --profile "$PROFILE" 2>&1) || VS_CHECK=""
    VS_STATE=$(echo "${VS_CHECK}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('endpoint_status',{}).get('state',''))" 2>/dev/null || echo "")
    if [ -n "${VS_STATE}" ] && [ "${VS_STATE}" != "" ]; then
        echo "VS endpoint exists (state=${VS_STATE})"
    else
        echo "Creating VS endpoint '${VS_ENDPOINT}'..."
        databricks api post /api/2.0/vector-search/endpoints --profile "$PROFILE" \
            --json "{\"name\": \"${VS_ENDPOINT}\", \"endpoint_type\": \"STANDARD\"}" 2>&1 || echo "  (may already exist)"
        echo "VS endpoint creation requested"
    fi
else
    echo ""
    echo "=== Skipping Vector Search endpoint (--no-vs) ==="
fi

if [ "$SKIP_APP" = false ]; then
    # --- Start app ---
    echo ""
    echo "=== Starting app ==="
    databricks bundle run -t "$TARGET" --profile "$PROFILE" \
        --var "deploying_user=${CURRENT_USER}" \
        --var "app_service_principal_application_id=${APP_SP_ID:-None}" \
        dbxmetagen_app

    echo ""
    echo "=== Deployment complete ==="
    echo "Access your app: Databricks workspace > Apps > ${APP_NAME}"
else
    echo ""
    echo "=== Deployment complete (jobs and code only) ==="
fi

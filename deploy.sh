#!/bin/bash
# Deploy dbxmetagen to Databricks
# Usage: ./deploy.sh [OPTIONS]
#   --profile PROFILE    Databricks CLI profile (default: DEFAULT)
#   --target TARGET      Bundle target: dev or prod (default: dev)
#   --no-app             Deploy jobs and code only (skip app build/start)
#   --permissions        Run UC permission grants for app SPN
#   --help               Show this help

set -e

TARGET="dev"
PROFILE="DEFAULT"
RUN_PERMISSIONS=false
SKIP_APP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)     PROFILE="$2"; shift 2 ;;
        --target)      TARGET="$2"; shift 2 ;;
        --no-app)      SKIP_APP=true; shift ;;
        --permissions) RUN_PERMISSIONS=true; shift ;;
        --help|-h)
            sed -n '2,7p' "$0"; exit 0 ;;
        *)
            echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

# --- Prerequisites ---
if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Install: https://docs.databricks.com/dev-tools/cli/install.html"
    exit 1
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

echo "=== Injecting env vars into app.yaml ==="
sed -i.bak \
    -e "s|__CATALOG_NAME__|${catalog_name}|g" \
    -e "s|__SCHEMA_NAME__|${schema_name}|g" \
    apps/dbxmetagen-app/app/app.yaml
rm -f apps/dbxmetagen-app/app/app.yaml.bak
# Restore app.yaml placeholders on exit so the working tree stays clean
APP_YAML_RESTORE=true

if [ "$SKIP_APP" = false ]; then
    # --- Build frontend ---
    echo ""
    echo "=== Building frontend ==="
    if [ -f "apps/dbxmetagen-app/app/src/package.json" ]; then
        (cd apps/dbxmetagen-app/app/src && npm install --silent && npm run build)
        echo "Frontend built successfully"
    else
        echo "No frontend package.json found, skipping build"
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

# --- Build Python package ---
echo ""
echo "=== Building Python package ==="
rm -rf dist/
poetry build
echo "Python package built: $(ls dist/*.whl)"

# --- Copy configurations into app source for deployment ---
echo ""
echo "=== Copying configurations into app source ==="
cp -r configurations apps/dbxmetagen-app/app/configurations
cleanup() {
    rm -rf apps/dbxmetagen-app/app/configurations
    [ "${APP_YAML_RESTORE:-}" = true ] && git checkout -- apps/dbxmetagen-app/app/app.yaml 2>/dev/null || true
}
trap cleanup EXIT

# --- Validate and deploy ---
echo ""
echo "=== Validating bundle ==="
DEPLOY_VARS=(--var "deploying_user=${CURRENT_USER}")
if [ -n "${APP_SP_ID}" ]; then
    DEPLOY_VARS+=(--var "app_service_principal_application_id=${APP_SP_ID}")
fi

databricks bundle validate -t "$TARGET" --profile "$PROFILE"

echo ""
echo "=== Deploying bundle ==="
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" "${DEPLOY_VARS[@]}"

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

# --- Grant UC permissions to app SPN ---
if [ "$RUN_PERMISSIONS" = true ] && [ -n "${APP_SP_ID}" ]; then
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

    GRANT_STATEMENTS=(
        "GRANT USE CATALOG ON CATALOG \`${catalog_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT USE SCHEMA ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT CREATE TABLE ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT SELECT ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
        "GRANT MODIFY ON SCHEMA \`${catalog_name}\`.\`${schema_name}\` TO \`${SPN_APP_ID}\`"
    )

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
        fi
    done
fi

# --- Ensure Vector Search endpoint ---
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

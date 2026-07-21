#!/bin/bash
# Post-deploy UC + Vector Search grants for the dbxmetagen app service principal.
#
# Run AFTER `databricks bundle deploy` (the app + its service principal must exist):
#   databricks bundle deploy -t <target> -p <profile>
#   scripts/grant_app_permissions.sh -t <target> -p <profile>
#
# Why this is a separate script and not part of the bundle:
#   DAB has no native grants: block for an app's OWN service principal, and there
#   is no DAB resource type for a Vector Search endpoint. Both must be granted
#   after the SP exists. This is the same pattern the agent-store repo uses.
#
# The script is idempotent -- safe to re-run. It resolves the app SP, ensures the
# output schema exists, grants the SP the UC privileges the app needs, ensures the
# VS endpoint exists, and grants the SP CAN_USE on it.
#
# Config (catalog/schema/warehouse/app name/vs endpoint) is read from the resolved
# bundle via `databricks bundle summary`, so it uses exactly the same variable
# values as the deploy. You can override any value by exporting it before running
# (catalog_name, schema_name, warehouse_id, app_name, vs_endpoint_name).
set -e

TARGET="dev"
PROFILE="DEFAULT"
SKIP_VS=false

usage() {
    cat <<'EOF'
Usage: scripts/grant_app_permissions.sh [OPTIONS]
  -t, --target TARGET    Bundle target (default: dev)
  -p, --profile PROFILE  Databricks CLI profile (default: DEFAULT)
      --no-vs            Skip Vector Search endpoint provisioning + grant
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)  TARGET="$2"; shift 2 ;;
        -p|--profile) PROFILE="$2"; shift 2 ;;
        --no-vs)      SKIP_VS=true; shift ;;
        -h|--help)    usage; exit 0 ;;
        *) echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found." >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# --- Resolve config from the deployed bundle (honors the deploy's var overrides) ---
echo "=== Resolving bundle config (target=${TARGET}) ==="
SUMMARY=$(databricks bundle summary -t "$TARGET" -p "$PROFILE" --output json 2>/dev/null) || SUMMARY=""

resolve_var() {
    # $1 = variable name, $2 = env override (if already set, wins)
    local name="$1" envval="$2"
    if [ -n "${envval}" ]; then echo "${envval}"; return; fi
    if [ -n "${SUMMARY}" ]; then
        echo "${SUMMARY}" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('variables', {}).get('$name', {})
    print(v.get('value', v.get('default', '')) or '')
except Exception:
    print('')
" 2>/dev/null || echo ""
    fi
}

CATALOG=$(resolve_var catalog_name "${catalog_name:-}")
SCHEMA=$(resolve_var schema_name "${schema_name:-}")
WAREHOUSE_ID=$(resolve_var warehouse_id "${warehouse_id:-}")
APP_NAME=$(resolve_var app_name "${app_name:-}")
VS_ENDPOINT=$(resolve_var vs_endpoint_name "${vs_endpoint_name:-}")
APP_NAME="${APP_NAME:-dbxmetagen-app}"
VS_ENDPOINT="${VS_ENDPOINT:-dbxmetagen-vs}"

for pair in "catalog_name=${CATALOG}" "schema_name=${SCHEMA}" "warehouse_id=${WAREHOUSE_ID}"; do
    if [ -z "${pair#*=}" ]; then
        echo "Error: could not resolve ${pair%%=*}. Set it in your bundle vars or export it before running." >&2
        exit 1
    fi
done

echo "  App:       ${APP_NAME}"
echo "  Catalog:   ${CATALOG}"
echo "  Schema:    ${SCHEMA}"
echo "  Warehouse: ${WAREHOUSE_ID}"

# --- Resolve the app service principal applicationId (UUID) ---
echo ""
echo "=== Resolving app service principal for: ${APP_NAME} ==="
APP_JSON=$(databricks apps get "${APP_NAME}" --profile "$PROFILE" --output json 2>&1) || {
    echo "Error: app '${APP_NAME}' not found. Deploy the bundle first:" >&2
    echo "  databricks bundle deploy -t ${TARGET} -p ${PROFILE}" >&2
    exit 1
}

SPN_APP_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id','') or '')" 2>/dev/null || echo "")
APP_SP_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_id','') or '')" 2>/dev/null || echo "")

# Fall back to SCIM lookup (requires workspace admin) if client_id absent.
if [ -z "${SPN_APP_ID}" ] || [ "${SPN_APP_ID}" = "None" ]; then
    if [ -n "${APP_SP_ID}" ] && [ "${APP_SP_ID}" != "None" ]; then
        echo "  service_principal_client_id absent; trying SCIM lookup on ${APP_SP_ID}..."
        SPN_JSON=$(databricks service-principals get "${APP_SP_ID}" --profile "$PROFILE" --output json 2>&1) && \
            SPN_APP_ID=$(echo "${SPN_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('applicationId','') or '')" 2>/dev/null || echo "")
    fi
fi

if [ -z "${SPN_APP_ID}" ] || [ "${SPN_APP_ID}" = "None" ]; then
    echo "" >&2
    echo "WARNING: Could not resolve the app service principal's applicationId." >&2
    echo "  Grant UC permissions manually as a catalog admin:" >&2
    echo "    GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`<applicationId>\`;" >&2
    echo "    GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY, READ VOLUME, WRITE VOLUME" >&2
    echo "      ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`<applicationId>\`;" >&2
    echo "  Find the applicationId in: Admin Console > Service Principals > ${APP_NAME}" >&2
    exit 1
fi
echo "  App SPN applicationId: ${SPN_APP_ID}"

run_sql() {
    local stmt="$1"
    databricks api post /api/2.0/sql/statements --profile "$PROFILE" \
        --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${stmt}\", \"wait_timeout\": \"30s\"}" 2>&1
}
sql_state() { echo "$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR"; }

# --- Ensure schema exists, then grant UC privileges ---
echo ""
echo "=== Granting UC permissions to app SPN ==="
echo "  Creating schema if not exists: ${CATALOG}.${SCHEMA}"
CREATE_RESULT=$(run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`")
if [ "$(sql_state "${CREATE_RESULT}")" = "SUCCEEDED" ]; then echo "    OK"; else echo "    WARNING: could not create schema. Grants may fail if it does not exist."; fi

# READ/WRITE VOLUME at schema level so the SP can read/write UC Volumes (e.g.
# generated_metadata) for DDL exports and imported ontology bundles. Schema-level
# grants cascade to all volumes and don't require the volume to exist yet.
GRANT_STATEMENTS=(
    "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${SPN_APP_ID}\`"
    "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT CREATE TABLE ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT MODIFY ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT READ VOLUME ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT WRITE VOLUME ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
)
GRANT_FAIL=0
for STMT in "${GRANT_STATEMENTS[@]}"; do
    echo "  ${STMT}"
    RESULT=$(run_sql "${STMT}")
    if [ "$(sql_state "${RESULT}")" = "SUCCEEDED" ]; then echo "    OK"; else echo "    FAILED"; echo "    ${RESULT}"; GRANT_FAIL=1; fi
done
if [ "${GRANT_FAIL}" -eq 1 ]; then
    echo ""
    echo "WARNING: One or more GRANT statements failed. The app will still start, but some"
    echo "         features may not work. Re-run this script or grant manually via the UI."
fi

# --- Ensure Vector Search endpoint + grant CAN_USE ---
if [ "$SKIP_VS" = false ]; then
    echo ""
    echo "=== Ensuring Vector Search endpoint: ${VS_ENDPOINT} ==="
    VS_CHECK=$(databricks api get "/api/2.0/vector-search/endpoints/${VS_ENDPOINT}" --profile "$PROFILE" 2>&1) || VS_CHECK=""
    VS_STATE=$(echo "${VS_CHECK}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('endpoint_status',{}).get('state','') or '')" 2>/dev/null || echo "")
    if [ -n "${VS_STATE}" ]; then
        echo "  VS endpoint exists (state=${VS_STATE})"
    else
        echo "  Creating VS endpoint '${VS_ENDPOINT}'..."
        databricks api post /api/2.0/vector-search/endpoints --profile "$PROFILE" \
            --json "{\"name\": \"${VS_ENDPOINT}\", \"endpoint_type\": \"STANDARD\"}" 2>&1 || echo "  (may already exist)"
    fi

    # Permissions API requires the numeric endpoint_id, not the name.
    VS_EP_ID=$(echo "${VS_CHECK}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id','') or '')" 2>/dev/null || echo "")
    if [ -z "${VS_EP_ID}" ]; then
        VS_FRESH=$(databricks api get "/api/2.0/vector-search/endpoints/${VS_ENDPOINT}" --profile "$PROFILE" 2>&1) || VS_FRESH=""
        VS_EP_ID=$(echo "${VS_FRESH}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id','') or '')" 2>/dev/null || echo "")
    fi
    if [ -n "${VS_EP_ID}" ]; then
        echo "  Granting CAN_USE on VS endpoint '${VS_ENDPOINT}' (id=${VS_EP_ID}) to app SPN..."
        databricks api patch "/api/2.0/permissions/vector-search-endpoints/${VS_EP_ID}" \
            --profile "$PROFILE" \
            --json "{\"access_control_list\": [{\"service_principal_name\": \"${SPN_APP_ID}\", \"permission_level\": \"CAN_USE\"}]}" 2>&1 || \
            echo "  WARNING: Could not grant VS endpoint permissions (may need manual grant)"
    else
        echo "  WARNING: Could not resolve VS endpoint numeric ID -- grant CAN_USE manually"
    fi
else
    echo ""
    echo "=== Skipping Vector Search endpoint (--no-vs) ==="
fi

echo ""
echo "=== Grants complete ==="

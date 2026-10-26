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
# If the bundle's run_as variable is a service principal (jobs run as that SP --
# see variables.yml), the script also:
#   1. ensures the DEPLOYING identity holds the "Service Principal: User" role
#      (roles/servicePrincipal.user) on that SP -- the Jobs API refuses to create
#      a job whose run_as SP the deployer cannot use, and workspace admins are not
#      exempt. This must exist BEFORE `bundle deploy`, so with an SP run_as run the
#      script once before the first deploy as well (deploy.sh does this for you):
#        scripts/grant_app_permissions.sh -t <target> -p <profile>   # before deploy
#        databricks bundle deploy -t <target> -p <profile>
#        scripts/grant_app_permissions.sh -t <target> -p <profile>   # after deploy
#      Before the first deploy the app does not exist yet; the script then grants
#      the jobs SP only and exits 0 (re-run after deploy for the app SP).
#   2. grants the jobs SP the same output-schema + VS privileges as the app SP,
#      plus the create privileges only the jobs need (CREATE VOLUME / FUNCTION /
#      MATERIALIZED VIEW on the output schema).
#   3. grants the jobs SP CAN_READ on the bundle's workspace folder (root_path):
#      the job notebooks + wheel live in the deployer's folder, which only the
#      deployer can read, so without this every task fails with "Unable to access
#      the notebook ... lacks the required permissions". Run after each deploy.
#
# Config (catalog/schema/warehouse/app name/vs endpoint) is read from the resolved
# bundle via `databricks bundle summary`, so it uses exactly the same variable
# values as the deploy. You can override any value by exporting it before running
# (catalog_name, schema_name, warehouse_id, app_name, vs_endpoint_name, and
# account_id -- needed for the role step; defaults to the CLI profile's account_id).
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
# Before the first deploy `bundle summary` may have nothing to report; the resolved
# config (incl. variables) is also available from `bundle validate`.
[ -z "${SUMMARY}" ] && { SUMMARY=$(databricks bundle validate -t "$TARGET" -p "$PROFILE" --output json 2>/dev/null) || SUMMARY=""; }

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
APP_NAME_SUFFIX=$(resolve_var app_name_suffix "${app_name_suffix:-}")
VS_ENDPOINT=$(resolve_var vs_endpoint_name "${vs_endpoint_name:-}")
APP_NAME="${APP_NAME:-dbxmetagen-app}"
# The app resource name is app_name + optional app_name_suffix (see
# resources/apps/dbxmetagen_app.yml). Match it so `apps get` resolves the right
# app when a suffix is used to run multiple instances in one workspace.
APP_NAME="${APP_NAME}${APP_NAME_SUFFIX}"
VS_ENDPOINT="${VS_ENDPOINT:-dbxmetagen-vs}"

for pair in "catalog_name=${CATALOG}" "schema_name=${SCHEMA}" "warehouse_id=${WAREHOUSE_ID}"; do
    if [ -z "${pair#*=}" ]; then
        echo "Error: could not resolve ${pair%%=*}. Set it in your bundle vars or export it before running." >&2
        exit 1
    fi
done

# Jobs identity: the run_as complex variable. Only a service_principal_name needs
# grants here; the default (deploying user) already holds its own privileges.
JOBS_SP_ID=""
if [ -n "${SUMMARY}" ]; then
    JOBS_SP_ID=$(echo "${SUMMARY}" | python3 -c "
import sys, json
try:
    v = json.load(sys.stdin).get('variables', {}).get('run_as', {})
    ra = v.get('value', v.get('default')) or {}
    print(ra.get('service_principal_name', '') if isinstance(ra, dict) else '')
except Exception:
    print('')
" 2>/dev/null || echo "")
fi

echo "  App:       ${APP_NAME}"
echo "  Catalog:   ${CATALOG}"
echo "  Schema:    ${SCHEMA}"
echo "  Warehouse: ${WAREHOUSE_ID}"
[ -n "${JOBS_SP_ID}" ] && echo "  Jobs run_as SP: ${JOBS_SP_ID}"

# --- Jobs run_as SP: deployer needs the Service Principal: User role on it ---
# Granted via the workspace proxy of the account access-control rule-set API.
# Read-modify-write with the etag so existing rules on the SP are preserved.
ROLE_OK=true
if [ -n "${JOBS_SP_ID}" ]; then
    echo ""
    echo "=== Ensuring deployer holds Service Principal: User on jobs SP ==="
    ME_JSON=$(databricks current-user me --profile "$PROFILE" --output json 2>/dev/null) || ME_JSON=""
    DEPLOYER_PRINCIPAL=$(echo "${ME_JSON}" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    # A service principal deployer (CI) is addressed by applicationId.
    print('servicePrincipals/' + d['applicationId'] if d.get('applicationId') else 'users/' + d.get('userName', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")
    ACCOUNT_ID="${account_id:-}"
    if [ -z "${ACCOUNT_ID}" ]; then
        ACCOUNT_ID=$(databricks auth describe --profile "$PROFILE" --output json 2>/dev/null | python3 -c "
import sys, json
try:
    v = json.load(sys.stdin).get('details', {}).get('configuration', {}).get('account_id', {})
    print((v.get('value') if isinstance(v, dict) else v) or '')
except Exception:
    print('')
" 2>/dev/null || echo "")
    fi

    if [ -z "${DEPLOYER_PRINCIPAL}" ] || [ "${DEPLOYER_PRINCIPAL}" = "users/" ] || [ -z "${ACCOUNT_ID}" ]; then
        ROLE_OK=false
        [ -z "${ACCOUNT_ID}" ] && echo "  Could not resolve account_id (export account_id=<id> or add it to the CLI profile)."
    else
        echo "  Deployer: ${DEPLOYER_PRINCIPAL}"
        RULESET_NAME="accounts/${ACCOUNT_ID}/servicePrincipals/${JOBS_SP_ID}/ruleSets/default"
        RULESET=$(databricks api get "/api/2.0/preview/accounts/access-control/rule-sets?name=${RULESET_NAME}&etag=" --profile "$PROFILE" 2>&1) || RULESET=""
        # Prints HAS (already granted), an update payload, or ERR.
        PLAN=$(echo "${RULESET}" | DEPLOYER="${DEPLOYER_PRINCIPAL}" python3 -c "
import sys, json, os
me, role = os.environ['DEPLOYER'], 'roles/servicePrincipal.user'
try:
    rs = json.load(sys.stdin)
    rules = rs.get('grant_rules') or []
    rule = next((r for r in rules if r.get('role') == role), None)
    if rule and me in (rule.get('principals') or []):
        print('HAS')
    else:
        if rule:
            rule['principals'] = (rule.get('principals') or []) + [me]
        else:
            rules.append({'role': role, 'principals': [me]})
        print(json.dumps({'name': rs['name'], 'rule_set': {'name': rs['name'], 'etag': rs['etag'], 'grant_rules': rules}}))
except Exception:
    print('ERR')
" 2>/dev/null || echo "ERR")
        if [ "${PLAN}" = "HAS" ]; then
            echo "  Already granted"
        elif [ "${PLAN}" = "ERR" ]; then
            ROLE_OK=false
            echo "  Could not read the SP's rule set: ${RULESET}"
        elif databricks api put /api/2.0/preview/accounts/access-control/rule-sets --profile "$PROFILE" --json "${PLAN}" > /dev/null 2>&1; then
            echo "  Granted roles/servicePrincipal.user to ${DEPLOYER_PRINCIPAL}"
        else
            ROLE_OK=false
            echo "  Could not grant the role (caller must be a manager of the SP or an admin)."
        fi
    fi
    if [ "${ROLE_OK}" = false ]; then
        echo "  WARNING: grant it manually, or bundle deploy will fail with PERMISSION_DENIED:" >&2
        echo "    Settings > Identity and access > Service principals > ${JOBS_SP_ID} > Permissions" >&2
        echo "    > Grant access > <deploying user> : Service Principal: User" >&2
    fi
fi

# --- Resolve the app service principal applicationId (UUID) ---
# Before the first deploy the app does not exist yet. With an SP run_as that is
# expected (this script runs pre-deploy to set up the jobs SP); otherwise it is
# an error, as before.
echo ""
echo "=== Resolving app service principal for: ${APP_NAME} ==="
SPN_APP_ID=""
if APP_JSON=$(databricks apps get "${APP_NAME}" --profile "$PROFILE" --output json 2>&1); then
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
elif [ -n "${JOBS_SP_ID}" ]; then
    echo "  App '${APP_NAME}' not deployed yet -- granting the jobs SP only."
    echo "  Re-run this script after bundle deploy to grant the app SP."
else
    echo "Error: app '${APP_NAME}' not found. Deploy the bundle first:" >&2
    echo "  databricks bundle deploy -t ${TARGET} -p ${PROFILE}" >&2
    exit 1
fi
# The app's own SP already gets every UC/VS grant below; don't grant it twice.
# RUN_AS_SP keeps the original id for the bundle-folder grant, which the app SP
# does NOT otherwise get.
RUN_AS_SP="${JOBS_SP_ID}"
[ "${JOBS_SP_ID}" = "${SPN_APP_ID}" ] && JOBS_SP_ID=""

run_sql() {
    local stmt="$1"
    databricks api post /api/2.0/sql/statements --profile "$PROFILE" \
        --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${stmt}\", \"wait_timeout\": \"30s\"}" 2>&1
}
sql_state() { echo "$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR"; }

# --- Ensure schema exists, then grant UC privileges ---
echo ""
echo "=== Ensuring output schema exists ==="
echo "  Creating schema if not exists: ${CATALOG}.${SCHEMA}"
CREATE_RESULT=$(run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`")
if [ "$(sql_state "${CREATE_RESULT}")" = "SUCCEEDED" ]; then echo "    OK"; else echo "    WARNING: could not create schema. Grants may fail if it does not exist."; fi

# READ/WRITE VOLUME at schema level so the SP can read/write UC Volumes (e.g.
# generated_metadata) for DDL exports and imported ontology bundles. Schema-level
# grants cascade to all volumes and don't require the volume to exist yet.
GRANT_FAIL=0
grant_schema_privileges() {
    local principal="$1"
    local stmts=(
        "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${principal}\`"
        "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${principal}\`"
        "GRANT CREATE TABLE ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${principal}\`"
        "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${principal}\`"
        "GRANT MODIFY ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${principal}\`"
        "GRANT READ VOLUME ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${principal}\`"
        "GRANT WRITE VOLUME ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${principal}\`"
    )
    local stmt result
    for stmt in "${stmts[@]}"; do
        echo "  ${stmt}"
        result=$(run_sql "${stmt}")
        if [ "$(sql_state "${result}")" = "SUCCEEDED" ]; then echo "    OK"; else echo "    FAILED"; echo "    ${result}"; GRANT_FAIL=1; fi
    done
}
if [ -n "${SPN_APP_ID}" ]; then
    echo ""
    echo "=== Granting UC permissions to app SPN ==="
    grant_schema_privileges "${SPN_APP_ID}"
fi
if [ -n "${JOBS_SP_ID}" ]; then
    echo ""
    echo "=== Granting UC permissions to jobs run_as SP ==="
    grant_schema_privileges "${JOBS_SP_ID}"
fi
# The jobs also CREATE objects the app never does: the output volume
# (generated_metadata), MCP UC functions, and materialized metric views. Applies to
# any run_as SP, including the app's own SP (RUN_AS_SP is kept even then).
if [ -n "${RUN_AS_SP}" ]; then
    echo ""
    echo "=== Granting job-only create privileges to jobs run_as SP ==="
    for stmt in \
        "GRANT CREATE VOLUME ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${RUN_AS_SP}\`" \
        "GRANT CREATE FUNCTION ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${RUN_AS_SP}\`" \
        "GRANT CREATE MATERIALIZED VIEW ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${RUN_AS_SP}\`"; do
        echo "  ${stmt}"
        result=$(run_sql "${stmt}")
        if [ "$(sql_state "${result}")" = "SUCCEEDED" ]; then echo "    OK"; else echo "    FAILED"; echo "    ${result}"; GRANT_FAIL=1; fi
    done
fi
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
        VS_ACL=""
        for sp in ${SPN_APP_ID} ${JOBS_SP_ID}; do
            VS_ACL="${VS_ACL:+${VS_ACL}, }{\"service_principal_name\": \"${sp}\", \"permission_level\": \"CAN_USE\"}"
        done
        echo "  Granting CAN_USE on VS endpoint '${VS_ENDPOINT}' (id=${VS_EP_ID}) to:${SPN_APP_ID:+ app SPN}${JOBS_SP_ID:+ jobs SP}..."
        databricks api patch "/api/2.0/permissions/vector-search-endpoints/${VS_EP_ID}" \
            --profile "$PROFILE" \
            --json "{\"access_control_list\": [${VS_ACL}]}" 2>&1 || \
            echo "  WARNING: Could not grant VS endpoint permissions (may need manual grant)"
    else
        echo "  WARNING: Could not resolve VS endpoint numeric ID -- grant CAN_USE manually"
    fi
else
    echo ""
    echo "=== Skipping Vector Search endpoint (--no-vs) ==="
fi

# --- Jobs run_as SP: read access to the bundle's workspace folder ---
if [ -n "${RUN_AS_SP}" ]; then
    echo ""
    echo "=== Granting jobs run_as SP read access to the bundle folder ==="
    ROOT_PATH=$(echo "${SUMMARY}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('workspace',{}).get('root_path','') or '')" 2>/dev/null || echo "")
    ROOT_ID=""
    if [ -n "${ROOT_PATH}" ]; then
        ROOT_ID=$(databricks workspace get-status "${ROOT_PATH}" --profile "$PROFILE" --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('object_id','') or '')" 2>/dev/null || echo "")
    fi
    if [ -z "${ROOT_ID}" ]; then
        # Expected before the first deploy: the folder is created by bundle deploy.
        echo "  Bundle folder ${ROOT_PATH:-<unknown>} not found -- re-run this script after bundle deploy."
    elif databricks api patch "/api/2.0/permissions/directories/${ROOT_ID}" --profile "$PROFILE" \
            --json "{\"access_control_list\": [{\"service_principal_name\": \"${RUN_AS_SP}\", \"permission_level\": \"CAN_READ\"}]}" > /dev/null 2>&1; then
        echo "  Granted CAN_READ on ${ROOT_PATH} to ${RUN_AS_SP}"
    else
        echo "  WARNING: could not grant CAN_READ on ${ROOT_PATH}; jobs will fail to open their notebooks." >&2
        echo "    Grant it manually: Workspace > ${ROOT_PATH} > Share > ${RUN_AS_SP} : Can Read" >&2
    fi
fi

echo ""
echo "=== Grants complete ==="
if [ -z "${SPN_APP_ID}" ]; then
    echo "  (app not deployed yet: re-run this script after bundle deploy to grant the app SP)"
fi

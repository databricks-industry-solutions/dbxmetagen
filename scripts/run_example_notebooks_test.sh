#!/usr/bin/env bash
# Run the example notebooks end-to-end on a Databricks workspace, simulating
# the customer experience: upload notebooks, create a job, run it.
#
# Usage:
#   ./scripts/run_example_notebooks_test.sh --profile DMVM --catalog eswanson_demo \
#       --warehouse-id 3abb59fcfb739e0d
#
# Optional:
#   --table-names   Tables to process (default: samples.nyctaxi.trips)
#   --schema        Output schema (default: example_test_<timestamp>)
#   --model         Model endpoint (default: databricks-claude-sonnet-4-6)
#   --no-cleanup    Skip dropping the output schema after the run
#   --no-wait       Submit the job and exit without waiting for completion

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXAMPLES_DIR="$REPO_ROOT/examples"

PROFILE=""
CATALOG=""
WAREHOUSE_ID=""
TABLE_NAMES="samples.nyctaxi.trips"
SCHEMA="example_test_$(date +%s)"
MODEL="databricks-claude-sonnet-4-6"
CLEANUP=true
WAIT=true

while [[ $# -gt 0 ]]; do
  case $1 in
    --profile)     PROFILE="$2"; shift 2 ;;
    --catalog)     CATALOG="$2"; shift 2 ;;
    --warehouse-id) WAREHOUSE_ID="$2"; shift 2 ;;
    --table-names) TABLE_NAMES="$2"; shift 2 ;;
    --schema)      SCHEMA="$2"; shift 2 ;;
    --model)       MODEL="$2"; shift 2 ;;
    --no-cleanup)  CLEANUP=false; shift ;;
    --no-wait)     WAIT=false; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

[[ -z "$PROFILE" ]] && { echo "Error: --profile is required"; exit 1; }
[[ -z "$CATALOG" ]] && { echo "Error: --catalog is required"; exit 1; }
[[ -z "$WAREHOUSE_ID" ]] && { echo "Error: --warehouse-id is required"; exit 1; }

CLI="databricks --profile $PROFILE"

# Get current user for workspace path
CURRENT_USER=$($CLI current-user me --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
WORKSPACE_DIR="/Workspace/Users/$CURRENT_USER/dbxmetagen_example_test"

echo "=== dbxmetagen example notebooks test ==="
echo "Profile:     $PROFILE"
echo "Catalog:     $CATALOG"
echo "Schema:      $SCHEMA"
echo "Tables:      $TABLE_NAMES"
echo "Model:       $MODEL"
echo "Warehouse:   $WAREHOUSE_ID"
echo "Upload to:   $WORKSPACE_DIR"
echo ""

# Upload notebooks (import-dir strips .py so %run ./helpers/common resolves)
echo "Uploading example notebooks..."
$CLI workspace import-dir "$EXAMPLES_DIR" "$WORKSPACE_DIR" --overwrite

echo "Upload complete."
echo ""

# Build job JSON
PARAMS="catalog_name=$CATALOG,schema_name=$SCHEMA,table_names=$TABLE_NAMES,model_endpoint=$MODEL,warehouse_id=$WAREHOUSE_ID"

JOB_JSON=$(cat <<ENDJOB
{
  "name": "dbxmetagen_example_test_$SCHEMA",
  "tasks": [
    {
      "task_key": "generate_metadata",
      "notebook_task": {
        "notebook_path": "$WORKSPACE_DIR/01_generate_metadata",
        "base_parameters": {
          "catalog_name": "$CATALOG",
          "schema_name": "$SCHEMA",
          "table_names": "$TABLE_NAMES",
          "model_endpoint": "$MODEL"
        }
      },
      "environment_key": "default",
      "max_retries": 0
    },
    {
      "task_key": "build_knowledge_bases",
      "depends_on": [{"task_key": "generate_metadata"}],
      "notebook_task": {
        "notebook_path": "$WORKSPACE_DIR/02_build_knowledge_bases",
        "base_parameters": {
          "catalog_name": "$CATALOG",
          "schema_name": "$SCHEMA",
          "table_names": "$TABLE_NAMES",
          "model_endpoint": "$MODEL"
        }
      },
      "environment_key": "default",
      "max_retries": 0
    },
    {
      "task_key": "build_analytics",
      "depends_on": [{"task_key": "build_knowledge_bases"}],
      "notebook_task": {
        "notebook_path": "$WORKSPACE_DIR/03_build_analytics",
        "base_parameters": {
          "catalog_name": "$CATALOG",
          "schema_name": "$SCHEMA",
          "table_names": "$TABLE_NAMES",
          "model_endpoint": "$MODEL",
          "ontology_bundle": "general"
        }
      },
      "environment_key": "default",
      "max_retries": 0
    },
    {
      "task_key": "generate_semantic_layer",
      "depends_on": [{"task_key": "build_analytics"}],
      "notebook_task": {
        "notebook_path": "$WORKSPACE_DIR/04_generate_semantic_layer",
        "base_parameters": {
          "catalog_name": "$CATALOG",
          "schema_name": "$SCHEMA",
          "table_names": "$TABLE_NAMES",
          "model_endpoint": "$MODEL"
        }
      },
      "environment_key": "default",
      "max_retries": 0
    },
    {
      "task_key": "create_genie_spaces",
      "depends_on": [{"task_key": "generate_semantic_layer"}],
      "notebook_task": {
        "notebook_path": "$WORKSPACE_DIR/05_create_genie_spaces",
        "base_parameters": {
          "catalog_name": "$CATALOG",
          "schema_name": "$SCHEMA",
          "table_names": "$TABLE_NAMES",
          "model_endpoint": "$MODEL",
          "warehouse_id": "$WAREHOUSE_ID"
        }
      },
      "environment_key": "default",
      "max_retries": 0
    }
  ],
  "environments": [
    {
      "environment_key": "default",
      "spec": {"client": "1"}
    }
  ]
}
ENDJOB
)

# Create and run the job
echo "Creating job..."
JOB_ID=$($CLI jobs create --json "$JOB_JSON" --output json | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
echo "Job ID: $JOB_ID"

echo "Starting run..."
RUN_ID=$($CLI jobs run-now "$JOB_ID" --no-wait --output json | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
echo "Run ID: $RUN_ID"

RUN_URL=$($CLI jobs get-run "$RUN_ID" --output json | python3 -c "import sys,json; print(json.load(sys.stdin)['run_page_url'])")
echo "Run URL: $RUN_URL"
echo ""

if [[ "$WAIT" == "false" ]]; then
  echo "Submitted (--no-wait). Check the run at: $RUN_URL"
  exit 0
fi

# Poll until complete
echo "Waiting for completion..."
while true; do
  STATE=$($CLI jobs get-run "$RUN_ID" --output json | python3 -c "import sys,json; s=json.load(sys.stdin)['state']; print(s.get('result_state') or s['life_cycle_state'])")
  echo "  $(date +%H:%M:%S) $STATE"
  case "$STATE" in
    SUCCESS)
      echo ""
      echo "All example notebooks passed."
      break
      ;;
    FAILED|TIMEDOUT|CANCELED|INTERNAL_ERROR)
      echo ""
      echo "Run failed with state: $STATE"
      echo "See: $RUN_URL"
      # Still try cleanup
      break
      ;;
  esac
  sleep 30
done

# Cleanup
if [[ "$CLEANUP" == "true" ]]; then
  echo ""
  echo "Cleaning up..."
  $CLI jobs delete "$JOB_ID" 2>/dev/null && echo "  Deleted job $JOB_ID"
  $CLI workspace delete "$WORKSPACE_DIR" --recursive 2>/dev/null && echo "  Deleted workspace dir"
  echo "  Output schema $CATALOG.$SCHEMA left in place (drop manually if desired)"
fi

[[ "$STATE" == "SUCCESS" ]] && exit 0 || exit 1

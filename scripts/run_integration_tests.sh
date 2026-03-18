#!/bin/bash
# Integration Test Runner for dbxmetagen
#
# Generates a minimal databricks.yml (tests-only, no app or production jobs)
# then deploys and runs integration tests. The original databricks.yml is
# backed up and restored automatically.

set -e

if [ -x "/opt/homebrew/bin/databricks" ]; then
  DATABRICKS_CLI="/opt/homebrew/bin/databricks"
elif [ -x "/usr/local/bin/databricks" ]; then
  DATABRICKS_CLI="/usr/local/bin/databricks"
elif command -v databricks &> /dev/null; then
  DATABRICKS_CLI="databricks"
else
  echo "Error: Databricks CLI not found. Install with: brew install databricks/tap/databricks"
  exit 1
fi

GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

TARGET="integration_test"
DEPLOY_ONLY=false
RUN_ONLY=false
TEST_CATALOG=""
TEST_SCHEMA="dbxmetagen_tests"
HOST=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --deploy-only) DEPLOY_ONLY=true; shift ;;
    --run-only)    RUN_ONLY=true;    shift ;;
    --target)      TARGET="$2";      shift 2 ;;
    --test-catalog) TEST_CATALOG="$2"; shift 2 ;;
    --test-schema) TEST_SCHEMA="$2"; shift 2 ;;
    --host)        HOST="$2";        shift 2 ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --deploy-only            Deploy but don't run"
      echo "  --run-only               Run without deploying (assumes already deployed)"
      echo "  --target TARGET          Bundle target name (default: integration_test)"
      echo "  --test-catalog CATALOG   Test catalog (default: read from databricks.yml dev target)"
      echo "  --test-schema SCHEMA     Test schema (default: dbxmetagen_tests)"
      echo "  --host HOST              Workspace host (default: read from databricks.yml dev target)"
      echo "  --help                   Show this help"
      exit 0 ;;
    *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
  esac
done

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}dbxmetagen Integration Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Read host and catalog from existing databricks.yml if not specified
if [ -f databricks.yml ]; then
  if [ -z "$HOST" ]; then
    HOST=$(grep -A5 "dev:" databricks.yml | grep "host:" | head -1 | awk '{print $2}' | tr -d '"' || true)
  fi
  if [ -z "$TEST_CATALOG" ]; then
    TEST_CATALOG=$(grep -A10 "dev:" databricks.yml | grep "catalog_name:" | head -1 | awk '{print $2}' | tr -d '"' || true)
  fi
fi

if [ -z "$HOST" ]; then
  echo -e "${RED}Could not determine workspace host. Pass --host or ensure databricks.yml exists.${NC}"
  exit 1
fi
if [ -z "$TEST_CATALOG" ]; then
  echo -e "${RED}Could not determine test catalog. Pass --test-catalog.${NC}"
  exit 1
fi

# Backup and restore helpers
BACKUP_YML=""
swap_in_test_config() {
  if [ -f databricks.yml ]; then
    BACKUP_YML=".databricks.yml.backup.$$"
    cp databricks.yml "$BACKUP_YML"
  fi
  cat > databricks.yml <<ENDOFYML
bundle:
  name: dbxmetagen
  databricks_cli_version: ">=0.283.0"

include:
  - resources/tests/*.yml
  - variables.yml
  - variables.advanced.yml

artifacts:
  default:
    type: whl
    build: poetry build
    path: .

targets:
  ${TARGET}:
    mode: development
    workspace:
      host: ${HOST}
    variables:
      catalog_name: ${TEST_CATALOG}
      schema_name: ${TEST_SCHEMA}
      warehouse_id: ""
    permissions:
      - user_name: \${var.deploying_user}
        level: CAN_MANAGE
ENDOFYML
  echo -e "${GREEN}Generated minimal test bundle config${NC}"
}

restore_config() {
  if [ -n "$BACKUP_YML" ] && [ -f "$BACKUP_YML" ]; then
    mv "$BACKUP_YML" databricks.yml
    echo "Restored original databricks.yml"
  fi
}
trap restore_config EXIT

echo "  Host:    $HOST"
echo "  Catalog: $TEST_CATALOG"
echo "  Schema:  $TEST_SCHEMA"
echo "  Target:  $TARGET"
echo ""

swap_in_test_config

# Step 1: Deploy
if [ "$RUN_ONLY" = false ]; then
  echo -e "${BLUE}Step 1: Deploying integration tests${NC}"
  echo ""

  echo "Building wheel..."
  poetry build -q

  echo "Deploying bundle..."
  "$DATABRICKS_CLI" bundle deploy -t "$TARGET"

  echo -e "${GREEN}Deployment complete${NC}"
  echo ""
fi

# Step 2: Run tests
if [ "$DEPLOY_ONLY" = false ]; then
  echo -e "${BLUE}Step 2: Running integration tests${NC}"
  echo ""

  "$DATABRICKS_CLI" bundle run integration_tests -t "$TARGET" \
    --params test_catalog="$TEST_CATALOG" \
    --params test_schema="$TEST_SCHEMA"

  echo ""
  echo -e "${GREEN}Integration tests completed${NC}"
  echo "Check the job run in your Databricks workspace for results."
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Done!${NC}"
echo -e "${BLUE}========================================${NC}"

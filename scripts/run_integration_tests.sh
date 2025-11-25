#!/bin/bash
# Integration Test Runner for dbxmetagen
# 
# This script deploys the integration test job and runs it.
# Integration tests validate end-to-end functionality in a real Databricks environment.

set -e  # Exit on error

# Use the modern Databricks CLI (not the old Python-based one)
if [ -x "/opt/homebrew/bin/databricks" ]; then
  DATABRICKS_CLI="/opt/homebrew/bin/databricks"
elif [ -x "/usr/local/bin/databricks" ]; then
  DATABRICKS_CLI="/usr/local/bin/databricks"
elif command -v databricks &> /dev/null; then
  DATABRICKS_CLI="databricks"
else
  echo "Error: Databricks CLI not found. Please install it:"
  echo "  brew tap databricks/tap"
  echo "  brew install databricks"
  exit 1
fi

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}dbxmetagen Integration Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if integration tests are enabled in databricks.yml
if ! grep -q "^  - resources/jobs/integration_tests.job.yml" databricks.yml; then
  echo -e "${RED}⚠ Integration tests are not enabled in databricks.yml${NC}"
  echo ""
  echo "To enable integration tests, uncomment this line in databricks.yml:"
  echo "  # - resources/jobs/integration_tests.job.yml"
  echo ""
  echo "After uncommenting, run this script again."
  echo ""
  exit 1
fi

# Parse command line arguments
TARGET="integration_test"
RUN_TESTS=true
DEPLOY_ONLY=false
RUN_ONLY=false
TEST_CATALOG="dev_integration_tests"
TEST_SCHEMA="dbxmetagen_tests"

while [[ $# -gt 0 ]]; do
  case $1 in
    --deploy-only)
      DEPLOY_ONLY=true
      RUN_TESTS=false
      shift
      ;;
    --run-only)
      RUN_ONLY=true
      shift
      ;;
    --target)
      TARGET="$2"
      shift 2
      ;;
    --test-catalog)
      TEST_CATALOG="$2"
      shift 2
      ;;
    --test-schema)
      TEST_SCHEMA="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --deploy-only          Deploy integration tests but don't run them"
      echo "  --run-only             Run tests without deploying (assumes already deployed)"
      echo "  --target TARGET        Databricks bundle target (default: integration_test)"
      echo "  --test-catalog CATALOG Test catalog name (default: dev_integration_tests)"
      echo "  --test-schema SCHEMA   Test schema name (default: dbxmetagen_tests)"
      echo "  --help                 Show this help message"
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Step 1: Deploy (unless --run-only)
if [ "$RUN_ONLY" = false ]; then
  echo -e "${BLUE}Step 1: Deploying integration tests to target '${TARGET}'${NC}"
  echo ""
  
  # Build the wheel first
  echo "Building wheel..."
  poetry build
  
  # Deploy the bundle
  echo "Deploying bundle..."
  "$DATABRICKS_CLI" bundle deploy -t "$TARGET"
  
  echo -e "${GREEN}✓ Deployment complete${NC}"
  echo ""
fi

# Step 2: Run tests (unless --deploy-only)
if [ "$DEPLOY_ONLY" = false ] && [ "$RUN_TESTS" = true ]; then
  echo -e "${BLUE}Step 2: Running integration tests${NC}"
  echo ""
  echo "Test catalog: $TEST_CATALOG"
  echo "Test schema: $TEST_SCHEMA"
  echo ""
  
  # Run the integration tests job
  "$DATABRICKS_CLI" bundle run integration_tests -t "$TARGET" \
    --params test_catalog="$TEST_CATALOG" \
    --params test_schema="$TEST_SCHEMA"
  
  echo ""
  echo -e "${GREEN}✓ Integration tests completed${NC}"
  echo ""
  echo "To view test results, check the job run in your Databricks workspace."
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Done!${NC}"
echo -e "${BLUE}========================================${NC}"


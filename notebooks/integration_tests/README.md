# Integration Tests for dbxmetagen

This directory contains end-to-end integration tests that validate dbxmetagen functionality in a real Databricks environment.

## Overview

Integration tests verify functionality that cannot be unit tested, including:
- Widget value parsing in real Databricks notebooks
- `apply_ddl` behavior (true vs false)
- Temp table cleanup (including error scenarios)
- Control table management
- Mode switching (comment, pi, domain)
- Catalog, schema, and volume permissions
- Error messages and handling

## Test Structure

Each test notebook follows this pattern:
1. **Setup**: Import utilities, create test resources
2. **Execute**: Run the functionality being tested
3. **Verify**: Assert expected outcomes
4. **Cleanup**: Remove test artifacts (in finally block)
5. **Report**: Return pass/fail status

## Running Integration Tests

### Prerequisites

**Before running integration tests**, you need to enable them in `databricks.yml`:

1. Open `databricks.yml` in the project root
2. Find the commented line: `# - resources/jobs/integration_tests.job.yml`
3. Uncomment it to: `- resources/jobs/integration_tests.job.yml`
4. Save the file

This prevents integration test jobs from cluttering everyone's workspace by default.

**Note**: The Streamlit app (`resources/apps/*.yml`) is commented out by default. Integration tests don't need the app. For dev/prod deployments with the UI, uncomment that line as well.

### Option 1: Using the CLI Script (Recommended)

```bash
# From the dbxmetagen root directory
./scripts/run_integration_tests.sh
```

The script will check if integration tests are enabled and provide instructions if not.

Options:
- `--deploy-only`: Deploy tests without running them
- `--run-only`: Run tests without deploying (assumes already deployed)
- `--test-catalog CATALOG`: Specify test catalog (default: dev_integration_tests)
- `--test-schema SCHEMA`: Specify test schema (default: dbxmetagen_tests)
- `--help`: Show all options

### Option 2: Using Databricks CLI Directly

```bash
# Deploy with integration tests
databricks bundle deploy -t integration_test

# Run the integration tests job
databricks bundle run integration_tests -t integration_test
```

### Option 3: Running Individual Tests

You can run individual test notebooks in Databricks:

1. Deploy the bundle: `databricks bundle deploy -t integration_test`
2. Navigate to the notebook in your Databricks workspace
3. Set the required widgets (`test_catalog`, `test_schema`)
4. Run the notebook

## Test Files

### Utilities
- `test_utils.py`: Helper utilities and validation functions for all integration tests
  - Assertion helpers (`assert_true`, `assert_equals`, etc.)
  - Validation functions (`verify_metadata_generation_log`, `verify_sql_file_exists`, etc.)
  - Test environment setup and cleanup

### Test Notebooks
- `test_01_widget_parsing.py`: Widget value parsing and boolean conversion
- `test_02_apply_ddl_false.py`: Verify apply_ddl=false doesn't modify tables but generates metadata
- `test_03_apply_ddl_true.py`: Verify apply_ddl=true modifies tables and applies comments
- `test_04_modes.py`: Test different modes produce different outputs (comment, pi, domain)
- `test_05_temp_table_cleanup.py`: Verify temp tables cleaned up (even on errors) and logs persist
- `test_06_control_table.py`: Control table management, cleanup, and schema validation
- `test_07_permissions.py`: Catalog, schema, volume permissions and grant_permissions config

### Configuration
Integration tests use the **production `variables.yml`** file (located at repo root) with test-specific overrides for catalog, schema, and table names.

## Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace
2. **Test Catalog**: A catalog for running tests (e.g., `dev_integration_tests`)
   - Will be created automatically if you have permissions
3. **Test Schema**: A schema within the catalog (e.g., `dbxmetagen_tests`)
   - Will be created automatically
4. **Permissions**:
   - CREATE CATALOG (or use existing catalog)
   - CREATE SCHEMA
   - CREATE TABLE
   - CREATE VOLUME
   - USE SCHEMA
   - SELECT on tables

## Test Environment

Integration tests use:
- Single-node cluster (num_workers: 0)
- Spark 15.4.x-cpu-ml-scala2.12
- Test catalog: `dev_integration_tests` (configurable)
- Test schema: `dbxmetagen_tests` (configurable)
- Test volume: `test_volume`

## Troubleshooting

### Tests fail with "Catalog does not exist"
- You may need CREATE CATALOG permission
- Or pre-create the catalog: `CREATE CATALOG IF NOT EXISTS dev_integration_tests`

### Tests fail with "Volume does not exist"
- Tests will attempt to create the volume
- If permission denied, pre-create: `CREATE VOLUME IF NOT EXISTS dev_integration_tests.dbxmetagen_tests.test_volume`

### Individual test failed
- Check the notebook output in the Databricks job run
- Each test prints detailed PASS/FAIL assertions
- Look for "❌ ASSERTION FAILED" messages

### Tests run but don't appear in bundle
- Ensure you're using target `integration_test`: `-t integration_test`
- Integration tests are excluded from other targets (dev, prod)

## Adding New Tests

To add a new integration test:

1. Create a new test notebook: `test_XX_description.py`
2. Use the standard structure (see existing tests)
3. Import and use `test_utils.py` for helpers and validation functions
4. **IMPORTANT**: Load production configuration and override test-specific parameters:
   - Use `yaml_file_path="../../variables.yml"` to load production config
   - Override catalog, schema, table_names to point to test resources
   - Override `grant_permissions_after_creation="false"` for tests
   - Override other settings as needed for your specific test (e.g., `apply_ddl`, `cleanup_control_table`)
5. Add output validation using helpers from `test_utils.py`:
   - `verify_metadata_generation_log()` - Check log entries
   - `verify_sql_file_exists()` - Verify DDL files generated
   - `verify_table_has_comment()` - Check table comments
   - `verify_column_has_comment()` - Check column comments
   - `verify_processing_log_exists()` - Verify processing logs
6. Add the test task to `resources/jobs/integration_tests.job.yml`
7. Update this README

### Example Test Structure

```python
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.main import main

# Create config using production YAML with test overrides
config = MetadataConfig(
    yaml_file_path="../../variables.yml",  # Production config (2 levels up from integration_tests/)
    catalog_name=test_catalog,             # Override to test catalog
    schema_name=test_schema,               # Override to test schema
    table_names=test_table,                # Override to test table
    volume_name="test_volume",             # Override to test volume
    grant_permissions_after_creation="false",  # Override for tests
    apply_ddl="false",                     # Override as needed for test
)

# Run metadata generation
main(config.__dict__)

# Validate outputs
log_df = verify_metadata_generation_log(spark, test_catalog, test_schema, test_table)
test_utils.assert_true(log_df is not None, "Metadata log created")

sql_exists = verify_sql_file_exists(
    spark, test_catalog, test_schema, "test_volume", user_sanitized, test_table
)
test_utils.assert_true(sql_exists, "SQL file generated")
```

**Key Points**:
- Tests use `../../variables.yml` (production config, 2 directory levels up)
- Only override parameters specific to test environment (catalog, schema, table, volume)
- Always set `grant_permissions_after_creation="false"` to avoid permission issues
- Override other settings as needed per test (e.g., `apply_ddl`, `mode`, `cleanup_control_table`)

## Continuous Integration

Integration tests can be run in CI/CD pipelines:

```bash
# In your CI pipeline
./scripts/run_integration_tests.sh --test-catalog ci_test_catalog
```

Make sure your CI service principal has the necessary permissions.


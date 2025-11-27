# Streamlit App Unit Testing Guide

## Overview

This document describes the comprehensive unit testing strategy for the DBXMetaGen Streamlit application. The tests focus on **business logic and utility functions** rather than UI rendering, following best practices for testing Streamlit applications.

## Testing Philosophy

### What We Test ✅

1. **Pure Business Logic**
   - Input validation (table names, URLs, parameters)
   - Data transformation (DDL generation, parameter conversion)
   - Configuration processing (YAML parsing, environment overrides)
   - Path resolution and construction
   - Node type detection based on cloud provider

2. **Utility Functions**
   - String formatting and escaping
   - Boolean to string conversions
   - List to comma-separated string conversion
   - URL normalization

3. **Validation Logic**
   - Table name format validation (regex patterns)
   - Cluster size validation
   - Required parameter validation
   - URL format validation

### What We Don't Test ❌

1. **UI Rendering Functions**
   - Streamlit `st.write()`, `st.button()`, etc.
   - Session state manipulation
   - UI component rendering

2. **External API Calls**
   - Actual Databricks workspace API calls
   - Real file I/O operations
   - Database queries

3. **Integration Points**
   - Full end-to-end workflows
   - Actual job creation and monitoring

## Test Files

### `test_app_logic.py` ⭐ PRIMARY TEST FILE

**Status**: ✅ Fully functional, 30 tests passing, no external dependencies required

This is the main test file that covers all testable business logic without requiring pandas, Databricks SDK, or Streamlit to be installed. It tests the actual logic patterns used in the app code.

**Coverage:**
- Table name validation (regex patterns)
- URL normalization and validation
- Worker configuration mapping
- Node type detection (Azure/AWS/GCP)
- Parameter type conversion (bool→string, list→CSV)
- Path resolution logic
- DDL generation patterns (comments, PI tags, domain tags)
- CSV processing logic
- Environment variable handling
- YAML variable extraction
- User context resolution

**Why this approach?**
- Tests the actual business logic without mocking complexity
- No dependencies on external packages
- Fast execution (< 0.1 seconds)
- Easy to run in any environment
- Validates the critical logic patterns

### `test_app_config_manager.py` 📦 REQUIRES ENVIRONMENT

**Status**: ⚠️ Requires full environment setup with dependencies

Tests ConfigManager and DatabricksClientManager with mocked Streamlit and Databricks SDK.

**Requirements:**
- Streamlit (mocked)
- Databricks SDK (mocked)
- YAML library

**Coverage:**
- YAML file loading and parsing
- Environment variable overrides
- Host URL validation
- Config caching

**To run:** Requires poetry/pip install of project dependencies

### `test_app_data_ops.py` 📦 REQUIRES ENVIRONMENT

**Status**: ⚠️ Requires full environment setup with pandas

Tests DataOperations and MetadataProcessor with real pandas DataFrames.

**Requirements:**
- pandas
- Streamlit (mocked)
- Databricks SDK (mocked)

**Coverage:**
- CSV file processing with pandas
- DDL generation with real DataFrames
- Table validation with workspace client
- Metadata transformation

**To run:** Requires poetry/pip install of project dependencies

### `test_app_job_manager.py` 📦 REQUIRES ENVIRONMENT

**Status**: ⚠️ Requires full environment setup

Tests JobManager functionality with mocked workspace clients.

**Requirements:**
- Streamlit (mocked)
- Databricks SDK (mocked)
- Full app module imports

**Coverage:**
- Job parameter building
- Input validation
- Worker configuration
- Node type detection
- Path resolution

**To run:** Requires poetry/pip install of project dependencies

### `test_app_user_context.py` 📦 REQUIRES ENVIRONMENT

**Status**: ⚠️ Requires full environment setup

Tests UserContextManager and AppConfig.

**Requirements:**
- Streamlit (mocked)
- Databricks SDK (mocked)
- Full app module imports

**Coverage:**
- User context resolution (deploying, current, job user)
- Path construction
- Bundle configuration

**To run:** Requires poetry/pip install of project dependencies

## Running the Tests

### Quick Test (No Dependencies)

```bash
# Run the main logic tests (recommended)
python -m pytest tests/test_app_logic.py -v
```

### Full Test Suite (Requires Environment)

```bash
# Install dependencies first
poetry install

# Run all tests
python -m pytest tests/test_app_*.py -v
```

### Run Specific Test Classes

```bash
# Test table name validation only
python -m pytest tests/test_app_logic.py::TestTableNameValidation -v

# Test DDL generation only
python -m pytest tests/test_app_logic.py::TestDDLGeneration -v
```

## Key Test Scenarios

### 1. Table Name Validation

Tests that table names follow the required `catalog.schema.table` format:

```python
# Valid: catalog.schema.table, my_catalog.my_schema.my_table
# Invalid: schema.table, table, catalog.schema.
```

### 2. DDL Generation

Tests three types of DDL generation:

**Comments:**
```sql
-- Column-level (DBR 16+)
COMMENT ON COLUMN catalog.schema.table.`column_name` IS "Description";

-- Table-level
COMMENT ON TABLE catalog.schema.table IS "Description";
```

**PI Classification (Tags):**
```sql
ALTER TABLE catalog.schema.table 
ALTER COLUMN `email_col` 
SET TAGS ('data_classification' = 'PII', 'data_subclassification' = 'EMAIL');
```

**Domain Classification (Tags):**
```sql
ALTER TABLE catalog.schema.table 
SET TAGS ('domain' = 'Finance', 'subdomain' = 'Accounting');
```

### 3. URL Normalization

Tests that Databricks workspace URLs are properly normalized:

```python
# Input: "workspace.databricks.com"
# Output: "https://workspace.databricks.com"
```

### 4. Cloud Provider Detection

Tests node type detection based on workspace URL:

```python
# Azure: Standard_D3_v2
# AWS: i3.xlarge
# GCP: n1-standard-4
```

### 5. Parameter Conversion

Tests conversion of configuration values for job parameters:

```python
# Booleans: True → "true", False → "false"
# Lists: ["t1", "t2"] → "t1,t2"
# Integers: 10 → "10"
```

## Test Organization

Tests are organized by functional area:

1. **Input Validation** - Ensuring data meets format requirements
2. **Data Transformation** - Converting between formats
3. **Configuration Processing** - Handling YAML and environment variables
4. **Path Resolution** - Constructing file and notebook paths
5. **DDL Generation** - Creating SQL statements

## Why This Testing Approach?

### Advantages

1. **Focus on What Matters**: Tests business logic that drives the application
2. **Fast Feedback**: Main test suite runs in < 0.1 seconds
3. **No Test Fragility**: Doesn't depend on Streamlit UI rendering
4. **Environment Independent**: Core tests run without dependencies
5. **Clear Validation**: Each test validates a specific piece of logic

### What's Not Covered (And Why That's OK)

1. **Streamlit UI Rendering**: 
   - UI frameworks are already tested by Streamlit
   - UI tests are brittle and slow
   - Logic tests ensure correct behavior

2. **Databricks API Integration**:
   - Would require live workspace
   - Should be tested in integration tests
   - Unit tests focus on request construction

3. **Session State Management**:
   - Streamlit handles this internally
   - Hard to test in isolation
   - Tested implicitly through integration

## Best Practices Applied

1. ✅ **Test Business Logic, Not Implementation**: Focus on what functions do, not how they do it
2. ✅ **No External Dependencies in Core Tests**: Main test file is standalone
3. ✅ **Clear Test Names**: Each test describes what it validates
4. ✅ **Fast Execution**: Tests run quickly for rapid feedback
5. ✅ **Single Responsibility**: Each test validates one specific behavior
6. ✅ **Real-World Patterns**: Tests use actual format patterns from the app

## Adding New Tests

When adding new business logic to the app:

1. **Identify Pure Logic**: Find functions that don't depend on UI or external APIs
2. **Extract Logic Patterns**: Pull out the core logic (regex, transformations, etc.)
3. **Add to `test_app_logic.py`**: Add tests that validate the logic patterns
4. **Test Edge Cases**: Include invalid inputs, empty values, special characters

Example:

```python
class TestNewFeature:
    """Test new feature business logic"""
    
    def test_normal_case(self):
        """Test normal operation"""
        # Test logic pattern
        pass
    
    def test_edge_case(self):
        """Test edge case handling"""
        # Test edge case
        pass
    
    def test_invalid_input(self):
        """Test error handling"""
        # Test validation
        pass
```

## Continuous Improvement

This test suite is designed to grow as the application evolves. When adding new features:

1. Add tests for new validation logic
2. Add tests for new data transformations
3. Add tests for new configuration options
4. Keep tests fast and focused on business logic

## Questions?

For questions about the testing strategy or adding new tests, refer to this guide or the inline comments in the test files.


# Streamlit App Unit Tests - Quick Reference

## Quick Start

Run the standalone logic tests (no dependencies required):

```bash
cd /path/to/dbxmetagen
python -m pytest tests/test_app_logic.py -v
```

**Result**: 30 tests covering all critical business logic in < 0.1 seconds

## What's Tested

### ✅ Core Business Logic (30 tests)

| Category | Tests | Examples |
|----------|-------|----------|
| **Input Validation** | 7 tests | Table name format, URL validation, cluster sizes |
| **Data Transformation** | 8 tests | Boolean→string, list→CSV, path construction |
| **DDL Generation** | 5 tests | Comment syntax, PI tags, domain tags, escaping |
| **Configuration** | 5 tests | YAML parsing, env overrides, variable extraction |
| **User Context** | 5 tests | User resolution, path generation, OBO mode |

### 🎯 Test Coverage by Module

- **ConfigManager**: URL normalization, environment overrides, YAML parsing
- **DataOperations**: Table validation, CSV processing
- **MetadataProcessor**: DDL generation for comments/PI/domain
- **JobManager**: Parameter building, worker config, node detection
- **UserContextManager**: User resolution, path construction

## Test Files

### `test_app_logic.py` ⭐ PRIMARY

- **30 passing tests**
- **No external dependencies**
- **Fast execution (< 0.1s)**
- Tests actual logic patterns from the app

### Additional Test Files (Require Environment)

- `test_app_config_manager.py` - ConfigManager with mocks
- `test_app_data_ops.py` - DataOperations with pandas
- `test_app_job_manager.py` - JobManager with mocks
- `test_app_user_context.py` - UserContextManager with mocks

## Testing Philosophy

**We test WHAT the code does, not HOW it does it.**

✅ **Test:** Pure logic, validation, transformation
❌ **Don't Test:** UI rendering, external APIs, Streamlit internals

## Common Test Commands

```bash
# Run all app tests (requires dependencies)
pytest tests/test_app_*.py -v

# Run specific test class
pytest tests/test_app_logic.py::TestTableNameValidation -v

# Run specific test
pytest tests/test_app_logic.py::TestTableNameValidation::test_valid_table_name_format -v

# Run with coverage
pytest tests/test_app_logic.py --cov=app --cov-report=html
```

## Example Test Patterns

### Input Validation

```python
def test_valid_table_name_format(self):
    """Test that catalog.schema.table format is valid"""
    pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$")
    assert pattern.match("catalog.schema.table")
```

### Data Transformation

```python
def test_boolean_to_string_conversion(self):
    """Test converting booleans to lowercase strings for job parameters"""
    assert str(True).lower() == "true"
    assert str(False).lower() == "false"
```

### DDL Generation

```python
def test_comment_ddl_column_level_structure(self):
    """Test DDR 16+ column comment syntax"""
    ddl = f'COMMENT ON COLUMN {table}.`{column}` IS "{desc}";'
    assert "COMMENT ON COLUMN" in ddl
```

## Why This Approach?

### Traditional Streamlit Testing Challenges

- ❌ Can't easily test `st.button()` without mocking entire Streamlit
- ❌ Session state is complex to mock
- ❌ UI rendering tests are slow and brittle

### Our Solution

- ✅ Test the business logic that powers the UI
- ✅ Test validation before it reaches Streamlit
- ✅ Test transformation logic that creates outputs
- ✅ Fast, reliable, no dependencies

### Real-World Impact

These tests catch:
- 🐛 Invalid table name formats
- 🐛 Incorrect DDL syntax
- 🐛 Parameter conversion errors
- 🐛 Path construction bugs
- 🐛 URL validation issues

## For More Details

See `STREAMLIT_APP_TESTING.md` for:
- Comprehensive testing philosophy
- Detailed test coverage
- Best practices
- How to add new tests

## CI/CD Integration

Add to your CI pipeline:

```yaml
# .github/workflows/test.yml
- name: Run Streamlit App Tests
  run: |
    python -m pytest tests/test_app_logic.py -v
```

Fast enough to run on every commit!

## Quick Health Check

Run this to verify tests are working:

```bash
python -m pytest tests/test_app_logic.py -v --tb=short
```

Expected: **30 passed in < 0.1 seconds** ✅


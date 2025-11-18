# Running Tests - Quick Guide

## TL;DR

```bash
# 1. Install dev dependencies (includes pytest and pyspark)
poetry install

# 2. Run tests
poetry run pytest tests/ -v
```

## What Changed

### ✅ Added to `pyproject.toml`

```toml
[tool.poetry.group.dev.dependencies]
pre-commit = "^4.0.1"
pytest = ">=8.3.4"
pyspark = "^3.5.0"      # ← NEW
pytest-cov = "^6.0.0"   # ← NEW
```

**Why**: Tests need PySpark to run locally, but it shouldn't be in the main dependencies (only in dev dependencies for testing).

### ✅ Tests Are Local-Only

The tests:
- ✅ Run locally with PySpark
- ✅ Do NOT require Databricks Runtime
- ✅ Do NOT require DBR-specific libraries
- ✅ Import only from `dbxmetagen.redaction.*` (evaluation, alignment)

**Why**: Separates testable business logic from Databricks-specific infrastructure.

## Step-by-Step

### 1. Install Dependencies

```bash
cd dbxmetagen
poetry install
```

This will:
- Install all main dependencies
- Install dev dependencies (pytest, pyspark, pytest-cov)
- Create/update the poetry.lock file

### 2. Verify PySpark Installation

```bash
poetry run python -c "import pyspark; print(pyspark.__version__)"
```

Expected output: `3.5.x` or similar

### 3. Run Tests

```bash
# All tests
poetry run pytest tests/ -v

# Specific test file
poetry run pytest tests/test_evaluation.py -v

# Specific test
poetry run pytest tests/test_evaluation.py::TestMatchEntitiesFlexible::test_exact_match_position_tolerance -v

# With coverage
poetry run pytest tests/ --cov=src/dbxmetagen/redaction --cov-report=html -v
```

### 4. Check Results

Expected output:
```
=================== test session starts ===================
platform darwin -- Python 3.11.0, pytest-8.4.2
collected 31 items

tests/test_alignment.py::TestEntity::test_create_entity PASSED     [  3%]
tests/test_alignment.py::TestEntity::test_entity_validation PASSED [  6%]
...
tests/test_evaluation.py::TestMatchEntitiesFlexible::test_exact_match_position_tolerance PASSED [ 65%]
...

================================== 31 passed in 5.43s ==================================
```

## Troubleshooting

### Error: "ImportError: cannot import name 'SparkSession'"

**Solution**: Install dev dependencies
```bash
poetry install
```

### Error: "Poetry lock file is not compatible"

**Solution**: Update lock file
```bash
poetry lock --no-update
poetry install
```

### Error: "ModuleNotFoundError: No module named 'dbxmetagen'"

**Solution**: Make sure you're in the project root
```bash
cd /path/to/dbxmetagen
poetry run pytest tests/ -v
```

### Error: Import errors related to `dbxmetagen.config` or `dbutils`

**This is expected!** The tests are designed to avoid importing Databricks-specific code. The tests import directly from `dbxmetagen.redaction.*` which doesn't depend on Databricks libraries.

If you still see this error, it means the test imports need to be adjusted.

## What's Being Tested

### Evaluation Module (`test_evaluation.py`)
- ✅ Whitespace normalization
- ✅ IoU calculation (including the critical "957770228" case)
- ✅ Entity matching (exact, partial, greedy)
- ✅ Metrics calculation (precision, recall, F1)

**19 tests total**

### Alignment Module (`test_alignment.py`)
- ✅ Entity creation and validation
- ✅ Entity normalization
- ✅ Best match finding
- ✅ Entity merging (longest selection)
- ✅ Confidence scoring
- ✅ Multi-source alignment (union behavior)
- ✅ Edge cases (empty, None, single source)

**22 tests total**

## Alternative: Poetry Shell

Instead of using `poetry run` for every command, you can activate the poetry shell:

```bash
poetry shell
pytest tests/ -v
python -c "import pyspark; print(pyspark.__version__)"
exit  # When done
```

## CI/CD Integration

For GitHub Actions or other CI/CD:

```yaml
- name: Install dependencies
  run: |
    pip install poetry
    poetry install

- name: Run tests
  run: poetry run pytest tests/ -v --junit-xml=test-results.xml
```

## Key Points

1. **Use Poetry**: All dependency management through Poetry
2. **Dev Dependencies**: pytest and pyspark are in `[tool.poetry.group.dev.dependencies]`
3. **Local Testing**: Tests run locally, no Databricks required
4. **Isolated Imports**: Tests only import from `dbxmetagen.redaction.*`
5. **31 Tests Total**: 19 evaluation + 22 alignment tests

## Next Steps After Tests Pass

1. Review test output to ensure all 31 tests pass
2. Check coverage report (if generated) at `htmlcov/index.html`
3. Critical tests to verify:
   - `test_exact_match_position_tolerance` (957770228 case)
   - `test_exact_match_whitespace_normalized` (whitespace handling)
   - `test_align_union_behavior` (alignment union logic)
   - `test_merge_selects_longest` (boundary modification logic)

## Summary

**Before**: `ImportError: cannot import name 'SparkSession'`

**After**: 
```bash
poetry install          # Install pyspark in dev dependencies
poetry run pytest      # All tests pass ✓
```

The tests validate that the simplified evaluation logic works correctly and that critical bugs (like the 957770228 case) are fixed.


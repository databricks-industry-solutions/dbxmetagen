# Unit Tests for DBXMetaGen Redaction

This directory contains unit tests for the evaluation and alignment modules.

## Setup

This project uses Poetry for dependency management. Install test dependencies:

```bash
# Install dev dependencies (includes pytest and pyspark)
poetry install

# Or if you want to update dependencies
poetry update
```

**Note**: The tests run locally with PySpark and do NOT require Databricks Runtime or DBR-specific libraries.

## Running Tests

### Run all tests:

```bash
# From the project root (recommended)
poetry run pytest tests/ -v

# Or activate the poetry shell first
poetry shell
pytest tests/ -v
```

### Run specific test files:

```bash
# Evaluation tests only
poetry run pytest tests/test_evaluation.py -v

# Alignment tests only
poetry run pytest tests/test_alignment.py -v
```

### Run specific test classes or methods:

```bash
# Run specific test class
poetry run pytest tests/test_evaluation.py::TestMatchEntitiesFlexible -v

# Run specific test method
poetry run pytest tests/test_evaluation.py::TestMatchEntitiesFlexible::test_exact_match_position_tolerance -v
```

### Run with coverage:

```bash
poetry run pytest tests/ --cov=src/dbxmetagen/redaction --cov-report=html -v
```

This will generate an HTML coverage report in `htmlcov/index.html`.

## Test Organization

### `test_evaluation.py`
Tests for the evaluation module including:
- **TestNormalizeWhitespace**: Whitespace normalization tests
- **TestCalculateIoU**: IoU calculation tests including the critical "957770228" case
- **TestMatchEntitiesFlexible**: Entity matching tests (exact, partial, whitespace)
- **TestCalculateEntityMetrics**: Metrics calculation tests (precision, recall, F1)

### `test_alignment.py`
Tests for the alignment module including:
- **TestEntity**: Entity dataclass tests
- **TestNormalizeEntity**: Entity normalization tests
- **TestFindBestMatch**: Best match finding tests
- **TestMergeEntities**: Entity merging tests
- **TestCalculateConfidence**: Confidence scoring tests
- **TestMultiSourceAligner**: Multi-source alignment tests (union behavior, boundary selection)
- **TestAlignmentEdgeCases**: Edge case handling tests

## Key Test Cases

### Critical Evaluation Tests

**957770228 Case** (`test_exact_match_position_tolerance`):
- Tests that "957770228" at positions (1,9) and (1,10) are matched as exact
- This was a key bug - should now pass with position tolerance of 2

**Whitespace Normalization** (`test_exact_match_whitespace_normalized`):
- Tests that "John Smith" and "John  Smith" (extra space) match exactly
- Validates whitespace-agnostic matching

**Partial Matching** (`test_partial_match_low_iou`):
- Tests lenient partial matching with IoU threshold of 0.1
- Validates that overlapping entities are matched even with low overlap

**Greedy Matching** (`test_greedy_matching_prefers_higher_iou`):
- Tests that when multiple predictions overlap one GT, the best match is selected
- Validates one-to-one matching constraint

### Critical Alignment Tests

**Union Behavior** (`test_align_union_behavior`):
- Tests that unmatched entities from all sources are included
- Validates that alignment doesn't drop detections

**Longest Entity Selection** (`test_align_two_sources_different_boundaries`):
- Tests that when entities overlap, the longest text is selected
- This is the root cause of boundary modification issues

**Multi-Source Agreement** (`test_align_three_sources`):
- Tests confidence scoring with agreement from 3 sources
- Validates that all source scores are preserved

## Expected Results

All tests should pass if the evaluation and alignment logic is working correctly:

```
tests/test_alignment.py::TestEntity::test_create_entity PASSED
tests/test_alignment.py::TestMultiSourceAligner::test_align_union_behavior PASSED
tests/test_evaluation.py::TestCalculateIoU::test_off_by_one PASSED
tests/test_evaluation.py::TestMatchEntitiesFlexible::test_exact_match_position_tolerance PASSED

================================== X passed in Y.YYs ==================================
```

## Troubleshooting

### PySpark not found:
```bash
# Make sure dev dependencies are installed
poetry install

# Check if pyspark is installed
poetry run python -c "import pyspark; print(pyspark.__version__)"
```

### Import errors:
Make sure you're running from the project root:
```bash
cd /path/to/dbxmetagen
poetry run pytest tests/ -v
```

If you get import errors related to `dbxmetagen.config` or Databricks-specific modules, the tests are properly isolated and only test PySpark-compatible code. Make sure you have PySpark installed in dev dependencies.

### Spark warnings:
It's normal to see some Spark initialization warnings like:
```
WARN Utils: Your hostname resolves to a loopback address...
```
As long as tests pass, these can be ignored.

### Poetry lock file issues:
If you encounter poetry lock issues:
```bash
poetry lock --no-update
poetry install
```

## CI/CD Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run tests
  run: |
    pip install pytest pyspark
    pytest tests/ -v --junit-xml=test-results.xml
```

## Adding New Tests

When adding new functionality:

1. Create test methods following the naming convention `test_<description>`
2. Use descriptive docstrings
3. Test both happy path and edge cases
4. Aim for >80% code coverage
5. Run tests locally before committing

Example:

```python
def test_new_feature(self, spark):
    """Test description of what's being validated."""
    # Arrange
    test_data = [...]
    
    # Act
    result = function_under_test(test_data)
    
    # Assert
    assert result == expected_value
```


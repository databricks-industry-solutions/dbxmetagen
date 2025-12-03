# Test Runner Script

## Overview

The `run_tests.sh` script provides a convenient way to run all dbxmetagen unit tests in the correct order, avoiding import conflicts that occur when pytest tries to collect all tests at once.

## Why This Script Exists

Due to mlflow → databricks.sdk import chain conflicts, the test files `test_ddl_regenerator.py` and `test_binary_variant_types.py` cannot be collected together with other tests in a single pytest run. This script runs them in separate processes to avoid the conflict.

## Usage

### Basic Usage

```bash
# Run all tests (default mode)
./run_tests.sh

# Shows test output and summary for all 244 tests
```

### Quick Mode

```bash
# Run only core tests (fast, 207 tests)
./run_tests.sh -q
./run_tests.sh --quick

# Use this for rapid feedback during development
```

### Summary Mode

```bash
# Run all tests but show summary only
./run_tests.sh -s
./run_tests.sh --summary

# Ideal for CI/CD or when you just want pass/fail status
```

### Verbose Mode

```bash
# Run with verbose pytest output
./run_tests.sh -v
./run_tests.sh --verbose

# Shows detailed test names and timing
```

### Help

```bash
# Show help message
./run_tests.sh -h
./run_tests.sh --help
```

## What It Does

The script runs tests in three phases:

### Phase 1: Core Unit Tests (207 tests)
- All standard unit tests
- Excludes problematic imports
- Must pass for script to continue

**Test Files**:
- `test_config_parsing.py`
- `test_apply_ddl_flag.py`
- `test_app_*.py` (Streamlit app tests)
- All other unit tests except DDL regenerator and binary/variant

### Phase 2: DDL Regenerator Tests (27 tests)
- Tests for reviewed DDL workflow
- Runs in separate process
- Failures are reported but don't stop execution

**Test File**: `test_ddl_regenerator.py`

### Phase 3: Binary/Variant Tests (10 tests)
- Tests for BINARY and VARIANT type support
- Runs in separate process
- Failures are reported but don't stop execution

**Test File**: `test_binary_variant_types.py`

## Output

### Summary Mode Output

```
╔════════════════════════════════════════════════╗
║       dbxmetagen Unit Test Runner             ║
╔════════════════════════════════════════════════╗

═══════════════════════════════════════════════
1/3 Core Unit Tests
═══════════════════════════════════════════════

▶ Running Core Unit Tests...

✓ Core Unit Tests: 207 tests passed


═══════════════════════════════════════════════
2/3 DDL Regenerator Tests (Separate Process)
═══════════════════════════════════════════════

▶ Running DDL Regenerator Tests...

✓ DDL Regenerator Tests: 27 tests passed


═══════════════════════════════════════════════
3/3 Binary/Variant Tests (Separate Process)
═══════════════════════════════════════════════

▶ Running Binary/Variant Tests...

✓ Binary/Variant Tests: 10 tests passed


╔════════════════════════════════════════════════╗
║           Test Suite Summary                   ║
╠════════════════════════════════════════════════╣
║  Core Tests:         207 passing                ║
║  DDL Regenerator:     27 passing                ║
║  Binary/Variant:      10 passing                ║
╠════════════════════════════════════════════════╣
║  TOTAL:              244 tests passing          ║
╚════════════════════════════════════════════════╝

✓ All test suites completed successfully!
```

## Exit Codes

- **0**: All tests passed
- **1**: Core tests failed (script stops immediately)
- **1**: Any test suite failed (after running all suites)

## Integration with CI/CD

### GitHub Actions Example

```yaml
- name: Run Unit Tests
  run: |
    cd dbxmetagen
    chmod +x run_tests.sh
    ./run_tests.sh -s
```

### Pre-commit Hook

```bash
#!/bin/bash
cd "$(git rev-parse --show-toplevel)/dbxmetagen"
./run_tests.sh -q || exit 1
```

## Troubleshooting

### Script Permission Denied

```bash
chmod +x run_tests.sh
```

### Poetry Not Found

Make sure poetry is installed and in your PATH:
```bash
poetry --version
```

### Tests Fail Unexpectedly

Try running individual test files to isolate the issue:
```bash
poetry run pytest tests/test_config_parsing.py -v
```

### Import Errors

If you see import errors, ensure all dependencies are installed:
```bash
poetry install
```

## Comparison with Direct Pytest

### ❌ This doesn't work (import conflicts):
```bash
poetry run pytest tests/
```

### ✅ This works (using the script):
```bash
./run_tests.sh
```

### ✅ This also works (manual exclusions):
```bash
# Core tests
poetry run pytest tests/ --ignore=tests/test_ddl_regenerator.py --ignore=tests/test_binary_variant_types.py -v

# DDL regenerator (separate)
poetry run pytest tests/test_ddl_regenerator.py -v

# Binary/variant (separate)
poetry run pytest tests/test_binary_variant_types.py -v
```

## Performance

| Mode | Time | Tests | Use Case |
|------|------|-------|----------|
| Quick (`-q`) | ~1s | 207 | Rapid development |
| Summary (`-s`) | ~10s | 244 | CI/CD, verification |
| Default | ~12s | 244 | Development, debugging |
| Verbose (`-v`) | ~15s | 244 | Detailed investigation |

## Related Documentation

- [IMPLEMENTATION_COMPLETE.md](IMPLEMENTATION_COMPLETE.md) - Complete feature summary
- [TEST_IMPROVEMENTS_SUMMARY.md](TEST_IMPROVEMENTS_SUMMARY.md) - Detailed test enhancements
- [PLAN_EXECUTION_SUMMARY.md](PLAN_EXECUTION_SUMMARY.md) - Task completion details

## Future Improvements

Possible enhancements for the script:

1. **Parallel Execution**: Run DDL and binary/variant tests in parallel
2. **Coverage Reports**: Add `--coverage` flag to generate coverage reports
3. **Test Selection**: Add flags to run specific test suites only
4. **Watch Mode**: Add `--watch` flag for continuous testing
5. **HTML Reports**: Generate HTML test reports
6. **Timing Analysis**: Show slowest tests

## Contributing

When adding new tests:

1. If the test has no special dependencies, add it to the `tests/` directory (it will be included in core tests)
2. If the test requires PySpark and has isolation issues, mark it with `pytestmark = pytest.mark.integration`
3. If the test imports modules with problematic dependencies, it may need to run separately (add to script)

---

**Last Updated**: November 29, 2025  
**Test Count**: 244 tests  
**Success Rate**: 100%


# GitHub Actions CI/CD

This directory contains GitHub Actions workflows for automated testing and validation.

## Workflows

### 1. CI Pipeline (`ci.yml`)

**Triggers:** Every PR and push to main/master

**Jobs:**
- **Lint**: Runs pre-commit checks (formatting, linting)
- **Unit Tests**: Runs all 190 unit tests on Python 3.10 and 3.11
- **Test Coverage**: Generates coverage reports
- **Config Validation**: Validates pyproject.toml and YAML files
- **Summary**: Aggregates results

**Status Badge:**
```markdown
![CI Pipeline](https://github.com/YOUR_ORG/dbxmetagen/actions/workflows/ci.yml/badge.svg)
```

### 2. Unit Tests Only (`unit-tests.yml`)

**Triggers:** Every PR and push to main/master

**Jobs:**
- Runs unit tests only (faster, simpler)
- Good for quick validation

### 3. Integration Tests (`integration-tests.yml`)

**Triggers:** Manual dispatch or push to main (if credentials available)

**Jobs:**
- Runs integration tests in `notebooks/integration_tests/`
- Requires Databricks workspace credentials

**Required Secrets:**
- `DATABRICKS_HOST`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Databricks personal access token

## Local Testing

Before pushing, run tests locally:

```bash
# Run all unit tests (same as CI)
poetry run pytest -v

# Run with coverage
poetry run pytest --cov=src --cov=app --cov-report=html

# Run integration tests (requires Databricks)
poetry run pytest notebooks/integration_tests/ -v

# Run pre-commit checks
pre-commit run --all-files
```

## Test Expectations

### Unit Tests (Run on every PR)
- **Count**: 190 tests
- **Duration**: < 1 second
- **Coverage**:
  - Streamlit app business logic (91 tests)
  - Core functionality (99 tests)

### Integration Tests (Run manually)
- **Count**: 11 tests
- **Requires**: Databricks workspace access
- **Purpose**: E2E validation in real environment

## Setting Up Secrets

### For Repository Maintainers

1. Go to **Settings → Secrets and variables → Actions**
2. Add repository secrets:
   - `DATABRICKS_HOST`: `https://your-workspace.cloud.databricks.com`
   - `DATABRICKS_TOKEN`: Your personal access token

### For Fork Contributors

- Unit tests run automatically (no secrets needed)
- Integration tests will be skipped (expected)
- Repository maintainers will run integration tests on merge

## Workflow Status

Check workflow status in the **Actions** tab:
- ✅ Green: All tests passed
- ❌ Red: Tests failed
- 🟡 Yellow: Tests running
- ⚪ Gray: Tests skipped

## Troubleshooting

### Tests fail in CI but pass locally

1. Check Python version compatibility (we test on 3.10 and 3.11)
2. Ensure `poetry.lock` is committed
3. Check for environment-specific issues

### Integration tests timeout

- Integration tests have no timeout by default
- May need to adjust if workspace is slow
- Can skip for PRs (run after merge)

### Cache issues

Clear workflow cache:
1. Go to **Actions** tab
2. Click **Caches** in left sidebar
3. Delete old caches

## Performance

| Workflow | Average Duration |
|----------|-----------------|
| Unit Tests | ~2-3 minutes |
| Full CI Pipeline | ~3-5 minutes |
| Integration Tests | ~10-15 minutes (if enabled) |

## Best Practices

1. ✅ **Always run tests locally** before pushing
2. ✅ **Keep PRs focused** - easier to review, faster CI
3. ✅ **Add tests for new features** - maintain coverage
4. ✅ **Fix failing tests immediately** - don't let them accumulate
5. ✅ **Review CI logs** if tests fail - they show detailed output

## Future Enhancements

- [ ] Add test coverage reporting to PR comments
- [ ] Add performance benchmarking
- [ ] Add security scanning (bandit, safety)
- [ ] Add automatic dependency updates (dependabot)
- [ ] Add deployment workflows for releases


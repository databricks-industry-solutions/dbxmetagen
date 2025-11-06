# Deployment Guide for Redaction Notebooks

## Package Installation Methods

The redaction notebooks require the `dbxmetagen` package to be installed. There are three deployment approaches:

### 1. DABs (Asset Bundles) - Recommended for Production

**Setup:**
1. Update version in `databricks.yml` if needed:
```yaml
variables:
  package_version:
    default: "0.5.1"  # Update this when releasing new version
```

2. Deploy with DABs:
```bash
databricks bundle deploy -t prod
```

3. The wheel is automatically uploaded to:
```
/Workspace/<workspace-root>/.databricks/bundle/<target>/files/dist/dbxmetagen-<version>-py3-none-any.whl
```

**In Notebook:**
Option A - Install via %pip (manual):
```python
# Uncomment and update target name (dev/prod)
WHEEL_PATH = "/Workspace/.../files/dist/dbxmetagen-0.5.1-py3-none-any.whl"
%pip install $WHEEL_PATH
%restart_python
```

Option B - Add as task library (recommended):
In your job YAML, add the wheel as a library:
```yaml
tasks:
  - task_key: detection
    libraries:
      - whl: "../../dist/dbxmetagen-${var.package_version}-py3-none-any.whl"
    notebook_task:
      notebook_path: ./notebooks/redaction/1. Benchmarking Detection
```

### 2. Volume Deployment - Good for Shared Development

**Setup:**
```bash
# Build wheel
poetry build

# Upload to volume
databricks fs cp dist/dbxmetagen-0.5.1-py3-none-any.whl \
  dbfs:/Volumes/catalog/schema/volume/dbxmetagen-0.5.1-py3-none-any.whl
```

**In Notebook:**
```python
WHEEL_PATH = "/Volumes/catalog/schema/volume/dbxmetagen-0.5.1-py3-none-any.whl"
%pip install $WHEEL_PATH
%restart_python
```

### 3. Editable Install - Development Only

For active development in Databricks Repos:

```python
WHEEL_PATH = "/Workspace/Repos/username/dbxmetagen/dbxmetagen"
%pip install -e $WHEEL_PATH
%restart_python
```

## Version Management

### Updating the Version

1. Update in `pyproject.toml`:
```toml
[tool.poetry]
version = "0.5.2"  # New version
```

2. Update in `databricks.yml`:
```yaml
variables:
  package_version:
    default: "0.5.2"  # Match pyproject.toml
```

3. Rebuild and redeploy:
```bash
poetry build
databricks bundle deploy -t prod
```

4. The new wheel is automatically deployed with the updated version

## Best Practices

- **Production**: Use DABs with task libraries (no %pip in notebook)
- **Development**: Use volume deployment or editable install
- **Version sync**: Keep `pyproject.toml` and `databricks.yml` versions in sync
- **Testing**: Test with volume deployment before promoting to DABs production


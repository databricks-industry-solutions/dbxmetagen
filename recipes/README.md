# dbxmetagen Recipe Job YAMLs

Portable Databricks Asset Bundle job definitions for consuming dbxmetagen
from another project.

## Usage

1. Copy the recipe YAML(s) you need into your project's `resources/` folder
2. Download the wheel from [GitHub Releases](../../releases) and upload to a Volume,
   or build locally with `poetry build` and upload `dist/*.whl`
3. Define the required variables in your `databricks.yml`:

```yaml
variables:
  dbxmetagen_wheel:
    default: "/Volumes/my_catalog/my_schema/dbxmetagen/dbxmetagen-<version>-py3-none-any.whl"
  dbxmetagen_notebooks:
    default: "/Workspace/Shared/dbxmetagen"
  catalog_name:
    default: "my_catalog"
  schema_name:
    default: "my_schema"
  node_type:
    default: "i3.xlarge"
```

4. Run `databricks bundle deploy` and `databricks bundle run <job_name>`

## Available Recipes

| Recipe | Description |
|--------|------------|
| `metagen.job.yml` | Generate comments for Delta tables |
| `analytics_pipeline.job.yml` | Full analytics: KB, graph, embeddings, similarity, ontology, FK, vector index |
| `build_vector_index.job.yml` | Build/refresh the vector search index |

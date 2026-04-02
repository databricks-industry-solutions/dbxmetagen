# dbxmetagen Examples

These notebooks demonstrate using **dbxmetagen as a pip-installable library** in standalone Databricks notebooks. They are intended for:

- Embedding dbxmetagen into your own projects or pipelines
- One-off ad-hoc metadata generation without deploying the full application
- Quick evaluation of dbxmetagen capabilities

Each notebook installs dbxmetagen directly from GitHub via `%pip install git+...` and does **not** require cloning the repo or running `deploy.sh`.

## Full Deployment

For the full-featured experience -- including the interactive dashboard, pre-configured jobs with incremental processing, Genie space generation, and the review/apply workflow -- use the main repo's `deploy.sh` workflow as documented in the [top-level README](../README.md#quickstart).

## Notebooks

| Notebook | Description |
|----------|-------------|
| `01_quickstart_metadata.py` | Comment, PI, or domain generation with widgets |
| `02_analytics_pipeline.py` | Full KB, graph, embeddings, ontology, similarity, quality pipeline |
| `03_advanced_analytics.py` | FK prediction and ontology validation |

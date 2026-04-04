# dbxmetagen Examples

These notebooks demonstrate using **dbxmetagen as a pip-installable library** in standalone Databricks notebooks. They are intended for:

- Embedding dbxmetagen into your own projects or pipelines
- One-off ad-hoc metadata generation without deploying the full application
- Quick evaluation of dbxmetagen capabilities

Each notebook installs dbxmetagen directly from GitHub via `%pip install git+...` and does **not** require cloning the repo or running `deploy.sh`.

## Full Deployment

For the full-featured experience -- including the interactive dashboard, pre-configured jobs with incremental processing, Genie space generation, and the review/apply workflow -- use the main repo's `deploy.sh` workflow as documented in the [top-level README](../README.md#quickstart).

## Quickstart Notebooks

Single-notebook examples for ad-hoc use or quick evaluation.

| Notebook | Description |
|----------|-------------|
| `01_quickstart_metadata.py` | Comment, PI, or domain generation with widgets |
| `02_analytics_pipeline.py` | Full KB, graph, embeddings, ontology, similarity, quality pipeline |
| `03_advanced_analytics.py` | FK prediction and ontology validation |

## Integration Example

The [`integration/`](integration/) folder shows how to embed dbxmetagen into an existing project as a multi-step pipeline, suitable for orchestration as a Databricks job. This is the recommended pattern for production use outside the dbxmetagen dashboard.

| Notebook | Description |
|----------|-------------|
| `integration/01_generate_metadata.py` | Metadata generation with overrides YAML |
| `integration/02_build_knowledge_bases.py` | Table, column, and schema knowledge bases |
| `integration/03_build_analytics.py` | Knowledge graph, ontology, embeddings, profiling, FK prediction, quality |
| `integration/04_generate_semantic_layer.py` | Metric view definitions from business questions |
| `integration/05_create_genie_space.py` | Genie space creation and JSON export |

Includes sample configs (`metagen_overrides.yml`, `business_questions.yaml`) and a [README](integration/README.md) with a DAB job definition template.

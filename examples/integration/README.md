# Integrating dbxmetagen into Your Project

This example shows how to embed **dbxmetagen as a pip-installable library** into an existing Databricks project. It demonstrates the full pipeline from metadata generation through Genie space creation, split across independent notebooks that can be orchestrated as a Databricks job.

> For the simpler single-notebook quickstart, see [`../01_quickstart_metadata.py`](../01_quickstart_metadata.py).

## When to Use This Pattern

- You have an existing project (e.g., a multi-agent system, a data platform, an analytics app) and want to enrich it with AI-generated metadata, knowledge bases, ontology, FK predictions, metric views, or Genie spaces.
- You want to run dbxmetagen as part of a larger job DAG alongside your own ETL.
- You don't need (or don't want) to deploy the full dbxmetagen dashboard app.

## Architecture

```
Your Project
  |
  |-- your_etl/
  |     |-- your_notebooks...
  |
  |-- metagen_pipeline/            <-- these example notebooks
  |     |-- 01_generate_metadata.py
  |     |-- 02_build_knowledge_bases.py
  |     |-- 03_build_analytics.py
  |     |-- 04_generate_semantic_layer.py
  |     |-- 05_create_genie_space.py
  |
  |-- configurations/
  |     |-- metagen_overrides.yml   <-- minimal config overrides
  |     |-- business_questions.yaml <-- questions for metric view generation
  |
  |-- databricks.yml               <-- your bundle with metagen job tasks
```

## Pipeline Stages

| Step | Notebook | Depends On | Output Tables |
|------|----------|------------|---------------|
| 1 | `01_generate_metadata` | -- | `metadata_generation_log` |
| 2 | `02_build_knowledge_bases` | Step 1 | `table_knowledge_base`, `column_knowledge_base`, `schema_knowledge_base`, `extended_metadata` |
| 3 | `03_build_analytics` | Step 2 | `knowledge_graph_nodes/edges`, `ontology_entities/relations`, `embeddings`, `similarity_edges`, `fk_predictions`, `profiling_results`, `data_quality_scores` |
| 4 | `04_generate_semantic_layer` | Step 2 | `metric_view_definitions`, `semantic_layer_questions` |
| 5 | `05_create_genie_space` | Steps 3 + 4 | Genie space (REST API) + exported JSON in UC Volume |

Steps 3 and 4 can run in parallel since they both depend on step 2 only.

## Configuration

### `metagen_overrides.yml`

Instead of copying dbxmetagen's full `variables.yml` (200+ lines), ship a **minimal overrides file** with only the parameters that differ from upstream defaults. See [`metagen_overrides.yml`](metagen_overrides.yml) for the template.

Notebook `01_generate_metadata` loads this file via `yaml_file_path`, and MetadataConfig merges it over built-in defaults. Explicit `main()` kwargs override both.

### `business_questions.yaml`

Feed domain-specific questions into metric view generation. If empty, notebook 04 auto-generates questions from the table knowledge base. See [`business_questions.yaml`](business_questions.yaml) for the format.

## Job Definition (DAB)

Wire the notebooks into your `databricks.yml` as a job with task dependencies:

```yaml
resources:
  jobs:
    metadata_pipeline:
      tasks:
        - task_key: generate_metadata
          notebook_task:
            notebook_path: metagen_pipeline/01_generate_metadata.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_name: ${var.schema_name}
              table_names: ${var.metagen_table_names}
              model_endpoint: ${var.metagen_model_endpoint}
              install_source: ${var.metagen_install_source}

        - task_key: build_knowledge_bases
          depends_on: [{task_key: generate_metadata}]
          notebook_task:
            notebook_path: metagen_pipeline/02_build_knowledge_bases.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_name: ${var.schema_name}
              install_source: ${var.metagen_install_source}

        - task_key: build_analytics
          depends_on: [{task_key: build_knowledge_bases}]
          notebook_task:
            notebook_path: metagen_pipeline/03_build_analytics.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_name: ${var.schema_name}
              ontology_bundle: ${var.metagen_ontology_bundle}
              model_endpoint: ${var.metagen_model_endpoint}
              install_source: ${var.metagen_install_source}

        - task_key: generate_semantic_layer
          depends_on: [{task_key: build_knowledge_bases}]
          notebook_task:
            notebook_path: metagen_pipeline/04_generate_semantic_layer.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_name: ${var.schema_name}
              model_endpoint: ${var.metagen_model_endpoint}
              install_source: ${var.metagen_install_source}

        - task_key: create_genie_space
          depends_on:
            - {task_key: build_analytics}
            - {task_key: generate_semantic_layer}
          notebook_task:
            notebook_path: metagen_pipeline/05_create_genie_space.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_name: ${var.schema_name}
              volume_name: ${var.volume_name}
              table_names: ${var.metagen_table_names}
              model_endpoint: ${var.metagen_model_endpoint}
              warehouse_id: ${var.sql_warehouse_id}
              max_tables_per_space: ${var.genie_max_tables}
              install_source: ${var.metagen_install_source}

variables:
  metagen_table_names:
    default: "my_catalog.my_schema.*"
  metagen_model_endpoint:
    default: databricks-claude-sonnet-4-6
  metagen_ontology_bundle:
    default: general
  metagen_install_source:
    default: "git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main"
  genie_max_tables:
    default: "25"
```

## Parameters Not Exposed (Hidden Defaults)

The notebooks expose the most important knobs as widgets. Several optional parameters are left at library defaults for simplicity. Override them in your own copy if needed.

| Function | Hidden Parameter | Default | What It Controls |
|----------|-----------------|---------|------------------|
| `build_similarity_edges` | `similarity_threshold` | 0.7 | Min cosine similarity to create an edge |
| `build_similarity_edges` | `max_edges_per_node` | 15 | Cap on graph density per node |
| `predict_foreign_keys` | `confidence_threshold` | 0.7 | Min confidence to keep a FK prediction |
| `predict_foreign_keys` | `dry_run` | False | Set True to preview without LLM calls |
| `predict_foreign_keys` | `sample_size` | 5 | Rows sampled for join validation |
| `generate_semantic_layer` | `fk_confidence_threshold` | 0.7 | Min FK confidence for join generation |
| `generate_semantic_layer` | `validate_expressions` | True | SQL-validate generated metric expressions |
| `generate_embeddings` | `max_nodes` | None (all) | Cap on nodes to embed |
| `run_profiling` | `max_tables` | None (all) | Cap on tables to profile |
| `run_profiling` | `incremental` | True | Skip already-profiled tables |
| `build_genie_space` | `questions` | None | Sample business questions for the space |
| `build_genie_space` | `metric_view_names` | None | Filter which metric views to include |
| `build_genie_space` | `description` | Auto-generated | Custom space description |
| `build_ontology` | `apply_tags` | False | Write entity tags to UC tables/columns |
| `build_ontology` | `incremental` | True | Skip already-processed tables |

The `embedding_model` used by `generate_embeddings` is hardcoded internally to `databricks-bge-large-en` and cannot be overridden via the convenience function. To change it, use the `EmbeddingConfig` class directly.

## Genie Space Table Limits

Genie spaces degrade in quality above ~25 tables (including metric views). Notebook `05_create_genie_space` handles this automatically:

1. **Under the limit** -- creates a single space (unchanged behavior).
2. **Over the limit** -- groups tables by `entity_type` from the `ontology_entities` table (built in step 3), then creates one Genie space per group.
3. **FK merging** -- groups connected by foreign key predictions are merged together (up to the cap) so joinable tables stay in the same space.
4. **Fallback** -- if `ontology_entities` doesn't exist (step 3 was skipped), tables are split into simple batches of `max_tables_per_space`.

The `max_tables_per_space` widget (default `25`) controls the cap. Override via the `GENIE_MAX_TABLES` env var or DAB variable.

## Multi-Mode Metadata Generation

Step 1 runs in a single `mode` (default `comment`). To get full coverage -- AI-generated comments, PII/PHI detection, and domain classification -- run step 1 three times with different mode values:

```
mode=comment   # AI-generated table/column descriptions
mode=pi        # PII, PHI, PCI, and medical information detection
mode=domain    # Business domain and subdomain classification
```

Each run appends to the same `metadata_generation_log` table. The knowledge base builders (step 2) merge all modes automatically. You can wire this as three parallel tasks in your DAB job definition, or use dbxmetagen's built-in `parallel_mode_generation_job` which handles this.

## Consuming the Outputs

After the pipeline runs, your project can read the metadata tables directly:

```python
# Table-level knowledge base
spark.table(f"{catalog}.{schema}.table_knowledge_base")

# Column-level knowledge base with AI-generated comments
spark.table(f"{catalog}.{schema}.column_knowledge_base")

# FK predictions for relationship discovery
spark.table(f"{catalog}.{schema}.fk_predictions").filter("final_confidence >= 0.7")

# Ontology entities mapped to tables
spark.table(f"{catalog}.{schema}.ontology_entities")

# Metric view definitions for analytics
spark.table(f"{catalog}.{schema}.metric_view_definitions").filter("status = 'validated'")
```

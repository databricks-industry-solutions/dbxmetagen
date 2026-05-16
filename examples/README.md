# dbxmetagen Examples

Standalone notebooks that show how to use dbxmetagen as a **pip-installable library** in your own projects. Each notebook installs dbxmetagen from GitHub -- no repo clone, no `deploy.sh`, no Asset Bundles needed.

> **Looking for the full experience?** The main repo's `deploy.sh` gives you a web dashboard, pre-configured jobs with concurrent processing, review/apply workflow, and more. See the [top-level README](../README.md#quickstart).

## How to use

1. Upload these notebooks (including the `helpers/` folder) to your Databricks workspace
2. Open notebook 01, set the `catalog_name` widget, and run
3. Continue through notebooks 02-05 in order

That's it. Each notebook `%pip install`s dbxmetagen from GitHub `main` and restarts Python, so there's nothing to pre-install.

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Access to a foundation model endpoint (default: `databricks-claude-sonnet-4-6`)
- Tables you want to generate metadata for
- A SQL warehouse ID (for Genie space creation in step 05)

## Pipeline

Run the notebooks in order. Each step depends on the outputs of the previous step.

| Notebook | Description | Outputs |
|----------|-------------|---------|
| `01_generate_metadata` | Runs all three modes (comment + PI + domain) for richest Genie context | `metadata_generation_log` |
| `02_build_knowledge_bases` | Structured KB tables from raw metadata | `table_knowledge_base`, `column_knowledge_base`, `schema_knowledge_base`, `extended_metadata` |
| `03_build_analytics` | Graph, ontology, embeddings, profiling, FK prediction, quality | `knowledge_graph_*`, `ontology_*`, `embeddings`, `fk_predictions`, `data_quality_scores` |
| `04_generate_semantic_layer` | Metric view definitions from business questions | `metric_view_definitions`, `semantic_layer_questions` |
| `05_create_genie_spaces` | Genie spaces with auto-splitting for large schemas | Genie space(s) + JSON exports in UC Volume |

## Configuration

All notebooks share configuration via `helpers/common.py` (loaded with `%run`). Set the widgets once and they propagate to every step:

| Widget | Required | Default | Description |
|--------|----------|---------|-------------|
| `catalog_name` | Yes | -- | Target UC catalog |
| `table_names` | No | `catalog.schema.*` | Comma-separated FQNs or wildcard |
| `schema_name` | No | `metadata_results` | Output schema for metadata tables |
| `model_endpoint` | No | `databricks-claude-sonnet-4-6` | Foundation model endpoint |
| `ontology_bundle` | No | `general` | `general`, `healthcare`, `financial_services`, `retail_cpg` |
| `warehouse_id` | For step 05 | -- | SQL warehouse for Genie spaces |
| `volume_name` | No | `generated_metadata` | UC Volume for exports |

## Compute Requirements

These notebooks run on **serverless compute** or classic clusters with DBR 14+.

## Customizing Business Questions

Step 04 auto-generates business questions from your table descriptions. For better metric views, create a `business_questions.yaml` next to the notebooks:

```yaml
questions:
  - "What were total sales last quarter by region?"
  - "Which customers have the highest lifetime value?"
  - "What is the month-over-month revenue trend?"
```

## Running as a Job

You can chain these notebooks into a Databricks workflow. Upload them to your workspace, then create a job with task dependencies -- no DAB required:

```yaml
tasks:
  - task_key: generate_metadata
    notebook_task:
      notebook_path: /Workspace/Users/you@company.com/dbxmetagen_examples/01_generate_metadata
      base_parameters:
        catalog_name: my_catalog
        table_names: my_catalog.my_schema.*
        model_endpoint: databricks-claude-sonnet-4-6
    environment_key: default

  - task_key: build_knowledge_bases
    depends_on: [{task_key: generate_metadata}]
    notebook_task:
      notebook_path: /Workspace/Users/you@company.com/dbxmetagen_examples/02_build_knowledge_bases
      base_parameters:
        catalog_name: my_catalog
    environment_key: default

  - task_key: build_analytics
    depends_on: [{task_key: build_knowledge_bases}]
    notebook_task:
      notebook_path: /Workspace/Users/you@company.com/dbxmetagen_examples/03_build_analytics
      base_parameters:
        catalog_name: my_catalog
        ontology_bundle: general
        model_endpoint: databricks-claude-sonnet-4-6
    environment_key: default

  - task_key: generate_semantic_layer
    depends_on: [{task_key: build_analytics}]
    notebook_task:
      notebook_path: /Workspace/Users/you@company.com/dbxmetagen_examples/04_generate_semantic_layer
      base_parameters:
        catalog_name: my_catalog
        model_endpoint: databricks-claude-sonnet-4-6
    environment_key: default

  - task_key: create_genie_spaces
    depends_on: [{task_key: generate_semantic_layer}]
    notebook_task:
      notebook_path: /Workspace/Users/you@company.com/dbxmetagen_examples/05_create_genie_spaces
      base_parameters:
        catalog_name: my_catalog
        warehouse_id: your_warehouse_id
        model_endpoint: databricks-claude-sonnet-4-6
    environment_key: default

environments:
  - environment_key: default
    spec:
      client: "1"
```

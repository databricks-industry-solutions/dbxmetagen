# dbxmetagen Examples

End-to-end notebooks for running the full dbxmetagen pipeline via `pip install`. Upload these notebooks to a Databricks workspace and run them in order to go from raw tables to production-ready Genie spaces.

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

Step 01 runs all three metadata modes (comment, PI, domain) by default. This produces the richest context for Genie -- descriptions power the instructions, PII flags inform guardrails, and domain classification drives ontology grouping. Edit the `MODES` list in step 01 if you only need a subset.

## Compute Requirements

These notebooks run on **serverless compute** or classic clusters with DBR 14+. Step 04 (`generate_semantic_layer`) uses `AI_QUERY` internally, which requires a SQL warehouse context -- on serverless this is automatic; on classic clusters, configure the warehouse at the cluster level.

## Customizing Business Questions

Step 04 auto-generates business questions from your table descriptions. For better metric views, create a `business_questions.yaml` next to the notebooks:

```yaml
questions:
  - "What were total sales last quarter by region?"
  - "Which customers have the highest lifetime value?"
  - "What is the month-over-month revenue trend?"
```

## Running as a Job

These notebooks work as a Databricks job with task dependencies. Example DAB job definition:

```yaml
resources:
  jobs:
    dbxmetagen_pipeline:
      name: "dbxmetagen-pipeline"
      tasks:
        - task_key: generate_metadata
          notebook_task:
            notebook_path: examples/01_generate_metadata
            base_parameters:
              catalog_name: ${var.catalog}
              schema_name: ${var.schema}
              table_names: ${var.catalog}.${var.schema}.*
              model_endpoint: databricks-claude-sonnet-4-6
          environment_key: default

        - task_key: build_knowledge_bases
          depends_on:
            - task_key: generate_metadata
          notebook_task:
            notebook_path: examples/02_build_knowledge_bases
            base_parameters:
              catalog_name: ${var.catalog}
              schema_name: ${var.schema}
          environment_key: default

        - task_key: build_analytics
          depends_on:
            - task_key: build_knowledge_bases
          notebook_task:
            notebook_path: examples/03_build_analytics
            base_parameters:
              catalog_name: ${var.catalog}
              schema_name: ${var.schema}
              ontology_bundle: general
              model_endpoint: databricks-claude-sonnet-4-6
          environment_key: default

        - task_key: generate_semantic_layer
          depends_on:
            - task_key: build_analytics
          notebook_task:
            notebook_path: examples/04_generate_semantic_layer
            base_parameters:
              catalog_name: ${var.catalog}
              schema_name: ${var.schema}
              model_endpoint: databricks-claude-sonnet-4-6
          environment_key: default

        - task_key: create_genie_spaces
          depends_on:
            - task_key: generate_semantic_layer
          notebook_task:
            notebook_path: examples/05_create_genie_spaces
            base_parameters:
              catalog_name: ${var.catalog}
              schema_name: ${var.schema}
              warehouse_id: ${var.warehouse_id}
              model_endpoint: databricks-claude-sonnet-4-6
          environment_key: default

      environments:
        - environment_key: default
          spec:
            client: "1"
```

## Full Deployment

For the full experience -- interactive dashboard, pre-configured jobs with concurrent processing, review/apply workflow -- use the main repo's `deploy.sh` as documented in the [top-level README](../README.md#quickstart).

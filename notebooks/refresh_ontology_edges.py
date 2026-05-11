# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Ontology Edges
# MAGIC
# MAGIC Lightweight post-FK-prediction task that re-runs only the ontology edge-building
# MAGIC steps so that FK-evidence-backed relationships land in the ontology within a
# MAGIC single pipeline run.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("ontology_bundle", "", "Ontology Bundle")
dbutils.widgets.text("model", "", "Model Endpoint")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty=all)")
dbutils.widgets.text("sweep_stale_edges", "false", "Sweep stale edges")
dbutils.widgets.text("incremental", "true", "Incremental")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
_raw_bundle = dbutils.widgets.get("ontology_bundle")
ontology_bundle = _raw_bundle.strip()
model_endpoint = dbutils.widgets.get("model").strip() or None
table_names_raw = dbutils.widgets.get("table_names")
sweep_stale = dbutils.widgets.get("sweep_stale_edges").strip().lower() in ("true", "1", "yes")
incremental = dbutils.widgets.get("incremental").strip().lower() in ("true", "1", "yes")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Ontology bundle (raw widget): '{_raw_bundle}'")
print(f"Ontology bundle (resolved): {ontology_bundle or '(none)'}")

# COMMAND ----------

import sys

sys.path.append("../src")

from dbxmetagen.ontology import refresh_ontology_relationships, resolve_bundle_path
from dbxmetagen.table_filter import parse_table_names

table_names = parse_table_names(table_names_raw) or None

effective_config = "configurations/ontology_config.yaml"
if ontology_bundle:
    effective_config = resolve_bundle_path(ontology_bundle)
    print(f"Resolved bundle '{ontology_bundle}' -> {effective_config}")

result = refresh_ontology_relationships(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    config_path=effective_config,
    ontology_bundle=ontology_bundle,
    model_endpoint=model_endpoint,
    table_names=table_names,
    sweep_stale=sweep_stale,
    incremental=incremental,
)

print(f"Ontology edge refresh complete:")
print(f"  Named relationships: {result['named_relationships']}")
print(f"  Bundle edges: {result['bundle_edges']}")
print(f"  Graph edges added: {result['graph_edges']}")

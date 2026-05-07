# Databricks notebook source
# MAGIC %md
# MAGIC # Build Ontology
# MAGIC
# MAGIC This notebook discovers and builds the ontology layer from the knowledge base.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text(
    "config_path", "configurations/ontology_config.yaml", "Config Path"
)
dbutils.widgets.text("apply_ddl", "false", "Apply DDL (tags)")
dbutils.widgets.text(
    "ontology_bundle", "", "Ontology Bundle (e.g. healthcare, general)"
)
dbutils.widgets.text("incremental", "true", "Incremental (true/false)")
dbutils.widgets.text("entity_tag_key", "entity_type", "UC Tag Key for entities")
dbutils.widgets.text("model", "", "Model Endpoint")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty=all)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
config_path = dbutils.widgets.get("config_path")
apply_tags = dbutils.widgets.get("apply_ddl").lower() in ("true", "1", "yes")
_raw_bundle = dbutils.widgets.get("ontology_bundle")
ontology_bundle = _raw_bundle.strip()
incremental = dbutils.widgets.get("incremental").lower() == "true"
entity_tag_key = dbutils.widgets.get("entity_tag_key").strip() or "entity_type"
model_endpoint = dbutils.widgets.get("model").strip() or None
table_names_raw = dbutils.widgets.get("table_names")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Config: {config_path}")
print(f"Ontology bundle (raw widget): '{_raw_bundle}'")
print(f"Ontology bundle (resolved): {ontology_bundle or '(none -- using config_path)'}")
print(f"Apply tags: {apply_tags}")
print(f"Entity tag key: {entity_tag_key}")
print(f"Incremental: {incremental}")
print(f"Model endpoint: {model_endpoint or '(default)'}")
if table_names_raw:
    print(f"Table filter: {table_names_raw}")

# COMMAND ----------

import sys

sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.ontology import build_ontology, resolve_bundle_path
from dbxmetagen.table_filter import parse_table_names

table_names = parse_table_names(table_names_raw) or None

effective_config = config_path
if ontology_bundle:
    effective_config = resolve_bundle_path(ontology_bundle)
    print(f"Resolved bundle '{ontology_bundle}' -> {effective_config}")

# Auto-detect ontology VS index if it was built by a prior pipeline stage
ontology_vs_index = ""
expected_index = f"{catalog_name}.{schema_name}.ontology_vs_index"
try:
    from databricks.vector_search.client import VectorSearchClient
    vsc = VectorSearchClient(disable_notice=True)
    idx = vsc.get_index(endpoint_name="dbxmetagen-vs", index_name=expected_index)
    if idx:
        ontology_vs_index = expected_index
        print(f"Ontology VS index detected: {ontology_vs_index}")
except Exception:
    print(f"Ontology VS index not available ({expected_index}) -- using keyword/tier-1 path")

result = build_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    config_path=effective_config,
    apply_tags=apply_tags,
    ontology_bundle=ontology_bundle,
    incremental=incremental,
    entity_tag_key=entity_tag_key,
    model_endpoint=model_endpoint,
    table_names=table_names,
    ontology_vs_index=ontology_vs_index,
)

print(f"Ontology build complete:")
print(f"  Table entities discovered: {result['entities_discovered']}")
print(f"  Column entities discovered: {result.get('column_entities_discovered', 0)}")
print(f"  Entity types: {result['entity_types']}")
print(f"  Graph edges added: {result['edges_added']}")
print(f"  UC tags applied: {result.get('tags_applied', 0)}")


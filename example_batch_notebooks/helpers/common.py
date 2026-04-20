# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Configuration
# MAGIC
# MAGIC Common widgets, constants, and helpers used by all pipeline notebooks.
# MAGIC Import via `%run ./helpers/common` **after** the `%pip install` cell in each notebook.

# COMMAND ----------

import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC
# MAGIC These are the shared parameters across the pipeline.
# MAGIC Fill in **catalog_name** and **table_names** at minimum; everything else has sensible defaults.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "metadata_results", "Output Schema")
dbutils.widgets.text("volume_name", "generated_metadata", "Volume Name")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated or catalog.schema.*)")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4-6", "Model Endpoint")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (for Genie)")
dbutils.widgets.dropdown("ontology_bundle", "general", ["general", "healthcare", "financial_services", "retail_cpg"], "Ontology Bundle")

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
VOLUME = dbutils.widgets.get("volume_name")
TABLE_NAMES = dbutils.widgets.get("table_names")
MODEL = dbutils.widgets.get("model_endpoint")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
ONTOLOGY_BUNDLE = dbutils.widgets.get("ontology_bundle")

assert CATALOG, "catalog_name is required -- fill in the widget above"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

METAGEN_OUTPUT_TABLES = {
    "metadata_generation_log", "table_knowledge_base", "column_knowledge_base",
    "schema_knowledge_base", "extended_metadata", "knowledge_graph_nodes",
    "knowledge_graph_edges", "embeddings", "data_quality_scores",
    "similarity_edges", "ontology_entities", "ontology_relations",
    "fk_predictions", "metric_view_definitions", "semantic_layer_questions",
    "profiling_results", "cluster_assignments", "ontology_validation_results",
}


def resolve_tables(table_names_str: str, catalog: str, schema: str) -> list[str]:
    """Resolve table name input to a list of fully-qualified table identifiers.

    Handles wildcards (catalog.schema.*), comma-separated lists, and filters out
    dbxmetagen output tables to avoid processing our own metadata.

    For wildcards like ``prodcat.sales.*``, the catalog and schema are parsed from
    the pattern rather than using the output schema defaults.
    """
    raw = (table_names_str or "").strip()
    if not raw:
        rows = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        tables = [f"{catalog}.{schema}.{r.tableName}" for r in rows]
        return [t for t in tables if t.split(".")[-1] not in METAGEN_OUTPUT_TABLES]
    if "*" in raw:
        parts = raw.replace("*", "").strip(".").split(".")
        wc_catalog = parts[0] if len(parts) >= 1 and parts[0] else catalog
        wc_schema = parts[1] if len(parts) >= 2 and parts[1] else schema
        rows = spark.sql(f"SHOW TABLES IN `{wc_catalog}`.`{wc_schema}`").collect()
        tables = [f"{wc_catalog}.{wc_schema}.{r.tableName}" for r in rows]
        return [t for t in tables if t.split(".")[-1] not in METAGEN_OUTPUT_TABLES]
    return [t.strip() for t in raw.split(",") if t.strip()]


def resolve_source_tables_from_kb(catalog: str, schema: str) -> list[str]:
    """Read distinct source table names from table_knowledge_base.

    Useful when TABLE_NAMES is empty and you need the original source tables
    (e.g. for Genie space creation) rather than listing the output schema.
    """
    kb = spark.table(f"`{catalog}`.`{schema}`.table_knowledge_base")
    rows = kb.select("table_name").distinct().collect()
    return [r.table_name for r in rows if r.table_name]


def fqn(table: str) -> str:
    """Return backtick-quoted fully-qualified table name in the output schema."""
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def show_table(name: str, limit: int = 5):
    """Display a preview of a dbxmetagen output table."""
    table = fqn(name)
    count = spark.table(table).count()
    print(f"{name}: {count} rows")
    display(spark.table(table).limit(limit))

# COMMAND ----------

print(f"Catalog:  {CATALOG}")
print(f"Schema:   {SCHEMA}")
print(f"Volume:   {VOLUME}")
print(f"Model:    {MODEL}")
print(f"Tables:   {TABLE_NAMES or '(all in schema)'}")
print(f"Ontology: {ONTOLOGY_BUNDLE}")
print(f"Warehouse: {WAREHOUSE_ID or '(not set -- needed for step 05)'}")

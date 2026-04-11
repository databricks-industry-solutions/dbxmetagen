# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Create Genie Spaces from dbxmetagen Metadata
# MAGIC
# MAGIC Uses the `dbxmetagen.genie` module to:
# MAGIC 1. Assemble context from knowledge bases, FK predictions, ontology, and metric views
# MAGIC 2. Run a 3-phase LLM agent to generate instructions, SQL snippets, and synonyms
# MAGIC 3. Create the Genie space(s) via REST API
# MAGIC 4. Export the space JSON to the UC Volume
# MAGIC
# MAGIC **Auto-splitting:** Genie spaces degrade above ~25 tables. When the resolved table
# MAGIC count exceeds `max_tables_per_space`, this notebook groups tables by ontology
# MAGIC entity type (from step 3) and creates one space per group. FK-connected groups
# MAGIC are merged to keep joinable tables together. If ontology data is unavailable,
# MAGIC tables are split into simple batches.
# MAGIC
# MAGIC **Prerequisites:** Run the metadata pipeline (steps 1-4) first.
# MAGIC A SQL warehouse ID is required for the Genie space.

# COMMAND ----------

import os
import sys

_mp = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
if _mp not in sys.path:
    sys.path.insert(0, _mp)
try:
    from install_dbxmetagen import install_dbxmetagen
except ImportError:
    sys.path.insert(0, os.path.join(os.getcwd(), "metagen_pipeline"))
    from install_dbxmetagen import install_dbxmetagen

dbutils.widgets.text("install_source", os.getenv("METAGEN_INSTALL_SOURCE", "auto"))
src = dbutils.widgets.get("install_source")
install_dbxmetagen(src)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, json, pathlib, re, math
from collections import defaultdict
from pyspark.sql import SparkSession

dbutils.widgets.text("catalog_name", os.getenv("CATALOG_NAME", ""), "Catalog Name (required)")
dbutils.widgets.text("schema_name", os.getenv("SCHEMA_NAME", "default"), "Output Schema")
dbutils.widgets.text("volume_name", os.getenv("VOLUME_NAME", "generated_metadata"), "Volume Name")
dbutils.widgets.text("table_names", os.getenv("METAGEN_TABLE_NAMES", ""), "Table Names")
dbutils.widgets.text("model_endpoint", os.getenv("METAGEN_MODEL_ENDPOINT", "databricks-claude-sonnet-4-6"), "Model Endpoint")
dbutils.widgets.text("warehouse_id", os.getenv("SQL_WAREHOUSE_ID", ""), "SQL Warehouse ID (required)")
dbutils.widgets.text("max_tables_per_space", os.getenv("GENIE_MAX_TABLES", "25"), "Max Tables per Space")
dbutils.widgets.text("max_total_tables", os.getenv("METAGEN_MAX_TABLES", "100"), "Max Total Tables")
# Re-declared so DAB base_parameters can pass install_source without "widget not found" errors.
dbutils.widgets.text("install_source", os.getenv("METAGEN_INSTALL_SOURCE", "auto"))

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
table_names_raw = dbutils.widgets.get("table_names")
model_endpoint = dbutils.widgets.get("model_endpoint")
warehouse_id = dbutils.widgets.get("warehouse_id")
max_tables = int(dbutils.widgets.get("max_tables_per_space"))
max_total_tables = int(dbutils.widgets.get("max_total_tables"))

spark = SparkSession.builder.getOrCreate()

if not warehouse_id:
    raise ValueError("warehouse_id is required (set SQL_WAREHOUSE_ID env var or widget)")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Warehouse: {warehouse_id}")
print(f"Model: {model_endpoint}")
print(f"Max tables/space: {max_tables}")
print(f"Max total tables: {max_total_tables}")
print(f"Table names: {table_names_raw}")

# COMMAND ----------

# DBTITLE 1,Resolve Table Identifiers
METAGEN_OUTPUT_TABLES = {
    "metadata_generation_log", "table_knowledge_base", "column_knowledge_base",
    "schema_knowledge_base", "extended_metadata", "knowledge_graph_nodes",
    "knowledge_graph_edges", "embeddings", "data_quality_scores",
    "similarity_edges", "ontology_entities", "ontology_relations",
    "fk_predictions", "metric_view_definitions", "semantic_layer_questions",
    "profiling_results", "cluster_assignments", "ontology_validation_results",
}

if table_names_raw and "*" not in table_names_raw:
    tables = [t.strip() for t in table_names_raw.split(",") if t.strip()]
else:
    rows = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`").collect()
    tables = [f"{catalog_name}.{schema_name}.{r.tableName}" for r in rows]
    tables = [t for t in tables if t.split(".")[-1] not in METAGEN_OUTPUT_TABLES]

print(f"Tables to include: {len(tables)}")
for t in tables:
    print(f"  {t}")

if len(tables) > max_total_tables:
    n_spaces = math.ceil(len(tables) / max_tables)
    raise ValueError(
        f"Resolved {len(tables)} tables, which exceeds the safety limit of {max_total_tables}.\n\n"
        f"At {max_tables} tables per space, this would create ~{n_spaces} Genie spaces. "
        f"Each space runs a 3-phase LLM agent, so this would be very expensive and slow.\n\n"
        f"To proceed intentionally, raise max_total_tables (widget or METAGEN_MAX_TABLES env var).\n"
        f"To narrow the scope, specify explicit table names instead of a wildcard."
    )

# COMMAND ----------

# DBTITLE 1,Group Tables for Multiple Spaces
def _group_by_ontology(spark, catalog, schema, all_tables, max_per_space):
    """Group tables using ontology entity_type, merge FK-connected groups, enforce cap."""
    table_set = set(all_tables)
    groups = defaultdict(set)

    try:
        ent_rows = spark.sql(
            f"SELECT entity_type, source_tables FROM `{catalog}`.`{schema}`.ontology_entities"
        ).collect()
    except Exception:
        return None

    for row in ent_rows:
        etype = row.entity_type or "uncategorized"
        for t in (row.source_tables or []):
            if t in table_set:
                groups[etype].add(t)

    assigned = set().union(*groups.values()) if groups else set()
    unassigned = table_set - assigned
    if unassigned:
        groups["uncategorized"].update(unassigned)

    try:
        fk_rows = spark.sql(
            f"SELECT source_table, target_table FROM `{catalog}`.`{schema}`.fk_predictions"
        ).collect()
        fk_pairs = [(r.source_table, r.target_table) for r in fk_rows]
    except Exception:
        fk_pairs = []

    if fk_pairs:
        table_to_group = {}
        for g, tbls in groups.items():
            for t in tbls:
                table_to_group[t] = g

        for src, tgt in fk_pairs:
            g1 = table_to_group.get(src)
            g2 = table_to_group.get(tgt)
            if g1 and g2 and g1 != g2:
                merged = groups[g1] | groups[g2]
                if len(merged) <= max_per_space:
                    groups[g1] = merged
                    for t in groups[g2]:
                        table_to_group[t] = g1
                    del groups[g2]

    final = {}
    for name, tbls in groups.items():
        tbl_list = sorted(tbls)
        if len(tbl_list) <= max_per_space:
            final[name] = tbl_list
        else:
            n_chunks = math.ceil(len(tbl_list) / max_per_space)
            chunk_size = math.ceil(len(tbl_list) / n_chunks)
            for i in range(n_chunks):
                chunk = tbl_list[i * chunk_size : (i + 1) * chunk_size]
                final[f"{name} ({i + 1})"] = chunk

    return final


def _chunk_tables(all_tables, max_per_space):
    """Simple fallback: split into even batches."""
    sorted_tables = sorted(all_tables)
    n_chunks = math.ceil(len(sorted_tables) / max_per_space)
    chunk_size = math.ceil(len(sorted_tables) / n_chunks)
    return {
        f"batch {i + 1}": sorted_tables[i * chunk_size : (i + 1) * chunk_size]
        for i in range(n_chunks)
    }


if len(tables) <= max_tables:
    space_groups = {"all": tables}
    print(f"Table count ({len(tables)}) within limit -- creating single space")
else:
    print(f"Table count ({len(tables)}) exceeds limit ({max_tables}) -- grouping for multiple spaces")
    space_groups = _group_by_ontology(spark, catalog_name, schema_name, tables, max_tables)
    if space_groups is None:
        print("  ontology_entities not found -- falling back to simple batching")
        space_groups = _chunk_tables(tables, max_tables)
    else:
        print(f"  Grouped into {len(space_groups)} spaces using ontology entity types")

print(f"\nSpace plan ({len(space_groups)} spaces):")
for name, tbls in space_groups.items():
    print(f"  [{name}] {len(tbls)} tables")

# COMMAND ----------

# DBTITLE 1,Build and Create Genie Spaces
from dbxmetagen.genie import build_genie_space

results = []

for group_name, group_tables in space_groups.items():
    if len(space_groups) == 1 and group_name == "all":
        title = f"{catalog_name}.{schema_name} Analytics"
    else:
        title = f"{catalog_name}.{schema_name} - {group_name} Analytics"

    print(f"\nCreating space: {title} ({len(group_tables)} tables)...")
    result = build_genie_space(
        warehouse_id=warehouse_id,
        catalog=catalog_name,
        schema=schema_name,
        table_identifiers=group_tables,
        title=title,
        model_endpoint=model_endpoint,
        create=True,
    )
    space_id = result.get("space_id", "unknown")
    print(f"  Created: {space_id}")
    results.append({"group": group_name, "result": result, "tables": group_tables})

# COMMAND ----------

# DBTITLE 1,Export Space JSONs to Volume
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()

outdir = pathlib.Path(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/genie_exports")
outdir.mkdir(exist_ok=True, parents=True)

for entry in results:
    r = entry["result"]
    sid = r.get("space_id", "unknown")
    safe_title = re.sub(r"[^a-zA-Z0-9_]", "_", r["title"])[:50]
    base = f"{sid}__{safe_title}"

    full = ws.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}", query={"include_serialized_space": "true"})
    (outdir / f"{base}.space.json").write_text(json.dumps(full, indent=2), encoding="utf-8")
    (outdir / f"{base}.serialized.json").write_text(json.dumps(r["serialized_space"], indent=2), encoding="utf-8")
    print(f"Exported: {base}")

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\nGenie spaces created: {len(results)}")
for entry in results:
    r = entry["result"]
    print(f"  [{entry['group']}] {r.get('space_id','?')} -- {r['title']} ({len(entry['tables'])} tables)")
print(f"Export dir: {outdir}")

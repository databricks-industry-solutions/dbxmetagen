# Databricks notebook source
# MAGIC %md
# MAGIC # Build Ontology Vector Search Index
# MAGIC
# MAGIC Generates text chunks from the active ontology bundle and writes them to
# MAGIC `ontology_chunks`, then provisions / syncs the `ontology_vs_index` on the
# MAGIC shared `dbxmetagen-vs` endpoint. The index enables HYBRID semantic retrieval
# MAGIC for entity and edge classification, replacing the token-heavy tier-1 prompt dump.

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("ontology_bundle", "general", "Ontology Bundle")
dbutils.widgets.text("endpoint_name", "dbxmetagen-vs", "VS Endpoint Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
ontology_bundle = dbutils.widgets.get("ontology_bundle")
endpoint_name = dbutils.widgets.get("endpoint_name")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building ontology VS index for {catalog_name}.{schema_name}")
print(f"Bundle: {ontology_bundle}, Endpoint: {endpoint_name}")

# COMMAND ----------

import sys
sys.path.append("../src")

from pathlib import Path

# Resolve bundle YAML path -- check repo-local configurations/ first, then wheel-bundled
bundle_path = None
for base in [
    Path("../configurations/ontology_bundles"),
    Path("/Workspace") / dbutils.widgets.get("catalog_name"),
]:
    candidate = base / f"{ontology_bundle}.yaml"
    if candidate.exists():
        bundle_path = str(candidate)
        break

if not bundle_path:
    from dbxmetagen.ontology import resolve_bundle_path
    bundle_path = resolve_bundle_path(ontology_bundle)

print(f"Bundle source: {bundle_path}")

# COMMAND ----------

from dbxmetagen.ontology_chunker import build_ontology_chunks_table

chunk_count = build_ontology_chunks_table(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    bundle_name=ontology_bundle,
    source=bundle_path,
)
print(f"Ontology chunks written: {chunk_count}")

# COMMAND ----------

from dbxmetagen.ontology_vector_index import OntologyVectorIndexConfig, OntologyVectorIndexBuilder

vs_config = OntologyVectorIndexConfig(
    catalog_name=catalog_name,
    schema_name=schema_name,
    endpoint_name=endpoint_name,
)
builder = OntologyVectorIndexBuilder(vs_config)
result = builder.run()

print(f"Ontology VS index ready")
print(f"  Endpoint: {result['endpoint']}")
print(f"  Index:    {result['index']}")

# COMMAND ----------

df = spark.sql(f"""
    SELECT chunk_type, ontology_bundle, COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.ontology_chunks
    GROUP BY chunk_type, ontology_bundle
    ORDER BY ontology_bundle, chunk_type
""")
display(df)

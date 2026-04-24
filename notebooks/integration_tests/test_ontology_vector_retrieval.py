# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Ontology Vector Retrieval
# MAGIC
# MAGIC End-to-end test of the vector-backed ontology classification pipeline:
# MAGIC 1. Generate chunks from the active bundle
# MAGIC 2. Write to `ontology_chunks` Delta table
# MAGIC 3. Build and sync the `ontology_vs_index`
# MAGIC 4. Query entities and edges via HYBRID search
# MAGIC 5. Run `predict_entity` with `pass0_mode="vector"`

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("ontology_bundle", "healthcare", "Ontology Bundle")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
ontology_bundle = dbutils.widgets.get("ontology_bundle")

assert catalog_name and schema_name, "catalog_name and schema_name are required"
print(f"Testing vector retrieval for {catalog_name}.{schema_name}, bundle={ontology_bundle}")

# COMMAND ----------

import sys
sys.path.append("../../src")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build ontology chunks

# COMMAND ----------

from dbxmetagen.ontology import resolve_bundle_path
from dbxmetagen.ontology_chunker import build_ontology_chunks_table

bundle_path = resolve_bundle_path(ontology_bundle)
chunk_count = build_ontology_chunks_table(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    bundle_name=ontology_bundle,
    source=bundle_path,
)
print(f"Chunks written: {chunk_count}")
assert chunk_count > 0, "Expected at least 1 chunk"

# COMMAND ----------

df = spark.sql(f"""
    SELECT chunk_type, COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.ontology_chunks
    WHERE ontology_bundle = '{ontology_bundle}'
    GROUP BY chunk_type
""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build / sync vector index

# COMMAND ----------

from dbxmetagen.ontology_vector_index import OntologyVectorIndexConfig, OntologyVectorIndexBuilder

vs_config = OntologyVectorIndexConfig(
    catalog_name=catalog_name,
    schema_name=schema_name,
)
builder = OntologyVectorIndexBuilder(vs_config)
result = builder.run()
print(f"Index ready: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query entities

# COMMAND ----------

import time
time.sleep(10)  # brief settle time after sync

from dbxmetagen.ontology_vector_index import query_entities

fq_index = vs_config.fq_index
entity_results = query_entities(
    fq_index=fq_index,
    table_blob="patients table with id, name, date_of_birth, gender columns",
    bundle=ontology_bundle,
    num_results=5,
)
print(f"Entity results ({len(entity_results)}):")
for r in entity_results:
    print(f"  {r.get('name', '?')}: score={r.get('score', '?')}")

assert len(entity_results) > 0, "Expected at least 1 entity result"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Query edges

# COMMAND ----------

from dbxmetagen.ontology_vector_index import query_edges

edge_results = query_edges(
    fq_index=fq_index,
    fk_blob="patients.id references encounters.patient_id. Source entity: Patient. Target entity: Encounter.",
    bundle=ontology_bundle,
    num_results=5,
)
print(f"Edge results ({len(edge_results)}):")
for r in edge_results:
    print(f"  {r.get('name', '?')}: score={r.get('score', '?')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: predict_entity with vector mode

# COMMAND ----------

from dbxmetagen.ontology_index import OntologyIndexLoader
from dbxmetagen.ontology_predictor import predict_entity
from pathlib import Path

bundle_yaml = Path(resolve_bundle_path(ontology_bundle))
tier_dir = bundle_yaml.parent / bundle_yaml.stem
loader = OntologyIndexLoader(base_dir=str(tier_dir))

from databricks_langchain import ChatDatabricks

def llm_fn(system_prompt, user_prompt):
    llm = ChatDatabricks(
        endpoint="databricks-claude-sonnet-4-6",
        temperature=0.0, max_tokens=2048, max_retries=2,
    )
    response = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    return response.content if hasattr(response, "content") else str(response)

result = predict_entity(
    table_name="patients",
    columns="id, name, date_of_birth, gender, address",
    sample="Description: Main patient demographics table",
    loader=loader,
    llm_fn=llm_fn,
    pass0_mode="vector",
    vs_index=fq_index,
    vs_bundle=ontology_bundle,
)

print(f"Predicted entity: {result.predicted_entity}")
print(f"Confidence: {result.confidence_score}")
print(f"URI: {result.equivalent_class_uri}")
print(f"Passes run: {result.passes_run}")
print(f"Rationale: {result.rationale}")

assert result.predicted_entity != "Unknown", f"Expected a real entity, got Unknown"
assert result.confidence_score > 0.0, "Expected non-zero confidence"

# COMMAND ----------

print("All integration tests passed!")

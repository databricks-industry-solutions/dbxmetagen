# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Generate Semantic Layer (Metric Views)
# MAGIC
# MAGIC Uses dbxmetagen to generate metric view definitions from business questions.
# MAGIC Definitions are stored in `metric_view_definitions` and `semantic_layer_questions`.
# MAGIC
# MAGIC If no business questions are configured, auto-generates from the table knowledge base.
# MAGIC
# MAGIC **Prerequisites:** Run `02_build_knowledge_bases` first (and optionally `03_build_analytics`
# MAGIC for FK-aware join generation).
# MAGIC
# MAGIC **Note:** `generate_semantic_layer` uses Spark SQL `AI_QUERY` internally, which requires
# MAGIC a SQL warehouse context. On serverless compute this is provided automatically; on classic
# MAGIC clusters the warehouse must be configured at the cluster level.

# COMMAND ----------

# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main pyyaml

# COMMAND ----------

import os
import yaml
from pyspark.sql import SparkSession

dbutils.widgets.text("catalog_name", os.getenv("CATALOG_NAME", ""), "Catalog Name (required)")
dbutils.widgets.text("schema_name", os.getenv("SCHEMA_NAME", "default"), "Output Schema")
dbutils.widgets.text("model_endpoint", os.getenv("METAGEN_MODEL_ENDPOINT", "databricks-claude-sonnet-4-6"), "Model Endpoint")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_endpoint = dbutils.widgets.get("model_endpoint")

spark = SparkSession.builder.getOrCreate()

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Model: {model_endpoint}")

# COMMAND ----------

# DBTITLE 1,Load Business Questions from Config
# Search for business_questions.yaml relative to the notebook and in common locations.
bq_candidates = [
    "../../configurations/business_questions.yaml",
    "configurations/business_questions.yaml",
]
try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_root = "/Workspace" + str(nb_path).rsplit("/", 3)[0]
    bq_candidates.append(f"{bundle_root}/configurations/business_questions.yaml")
except Exception:
    pass

questions = []
for path in bq_candidates:
    if os.path.exists(path):
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
        questions = cfg.get("questions", [])
        print(f"Loaded {len(questions)} questions from {path}")
        break

if not questions:
    print("No questions in config -- auto-generating from table_knowledge_base")
    try:
        kb_df = spark.table(f"{catalog_name}.{schema_name}.table_knowledge_base")
        rows = kb_df.select("table_name", "comment").collect()
        for r in rows:
            short = r.table_name.split(".")[-1].replace("_", " ") if r.table_name else "data"
            questions.append(f"What are the key trends and totals in {short}?")
            questions.append(f"Summarize the {short} data by its main dimensions")
        print(f"  Generated {len(questions)} questions from {len(rows)} tables")
    except Exception as e:
        print(f"  Could not read table_knowledge_base: {e}")

    if not questions:
        print("No tables found -- nothing to generate.")
        dbutils.notebook.exit("No questions and no KB tables")

# COMMAND ----------

# DBTITLE 1,Generate Metric View Definitions
from dbxmetagen import generate_semantic_layer

result = generate_semantic_layer(
    spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    questions=questions,
    model_endpoint=model_endpoint,
)
print(f"Result: {result}")

# COMMAND ----------

# DBTITLE 1,Show Generated Definitions
defs_table = f"{catalog_name}.{schema_name}.metric_view_definitions"
count = spark.table(defs_table).count()
print(f"metric_view_definitions: {count} rows")
display(spark.table(defs_table).limit(10))

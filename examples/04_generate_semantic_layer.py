# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Generate Semantic Layer (Metric Views)
# MAGIC
# MAGIC Uses AI to generate metric view definitions from business questions about your data.
# MAGIC Metric views provide pre-built SQL expressions that Genie can use to answer
# MAGIC analytical questions accurately -- they're the "cheat sheet" that makes Genie smart.
# MAGIC
# MAGIC If no business questions are configured, auto-generates questions from the
# MAGIC table knowledge base.
# MAGIC
# MAGIC **Outputs:** `metric_view_definitions`, `semantic_layer_questions`
# MAGIC
# MAGIC **Prerequisites:** Run `02_build_knowledge_bases` first (and `03_build_analytics`
# MAGIC for FK-aware join generation).
# MAGIC
# MAGIC **Compute note:** `generate_semantic_layer` uses `AI_QUERY` internally, which requires
# MAGIC a SQL warehouse context. On serverless compute this is automatic. On classic clusters,
# MAGIC configure the warehouse at the cluster level or use the `warehouse_id` widget.

# COMMAND ----------

# MAGIC %pip install -qqq https://github.com/databricks-industry-solutions/dbxmetagen/archive/refs/heads/main.zip pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./helpers/common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Business Questions
# MAGIC
# MAGIC You can provide your own questions that a business analyst would ask about your data.
# MAGIC Place a `business_questions.yaml` file next to the notebooks (see example in this folder).
# MAGIC If none are provided, we auto-generate questions from the table knowledge base.

# COMMAND ----------

import yaml

questions = []

for path in ["./business_questions.yaml", "../business_questions.yaml"]:
    if os.path.exists(path):
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
        questions = cfg.get("questions", [])
        if questions:
            print(f"Loaded {len(questions)} questions from {path}")
        break

if not questions:
    print("No questions file found -- auto-generating from table_knowledge_base")
    try:
        kb_df = spark.table(fqn("table_knowledge_base"))
        rows = kb_df.select("table_name", "comment").collect()
        for r in rows:
            short = r.table_name.split(".")[-1].replace("_", " ") if r.table_name else "data"
            questions.append(f"What are the key trends and totals in {short}?")
            questions.append(f"Summarize the {short} data by its main dimensions")
        print(f"  Generated {len(questions)} questions from {len(rows)} tables")
    except Exception as e:
        print(f"  Could not read table_knowledge_base: {e}")

if not questions:
    dbutils.notebook.exit("No questions and no KB tables -- run steps 01-02 first")

print(f"\nQuestions ({len(questions)}):")
for q in questions[:10]:
    print(f"  - {q}")
if len(questions) > 10:
    print(f"  ... and {len(questions) - 10} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Metric View Definitions
# MAGIC
# MAGIC Each question becomes one or more metric view definitions with SQL expressions.
# MAGIC These are stored in `metric_view_definitions` and used by Genie for accurate answers.

# COMMAND ----------

from dbxmetagen import generate_semantic_layer

result = generate_semantic_layer(
    spark,
    catalog_name=CATALOG,
    schema_name=SCHEMA,
    questions=questions,
    model_endpoint=MODEL,
)
print(f"Result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Definitions

# COMMAND ----------

show_table("metric_view_definitions", limit=10)

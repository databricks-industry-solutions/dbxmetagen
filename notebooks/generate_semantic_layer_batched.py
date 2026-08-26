# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Semantic Layer (Batched, per-table)
# MAGIC
# MAGIC The default `generate_semantic_layer` runs ONE two-phase plan over all pending
# MAGIC questions, and the planner consolidates them into a handful of cross-theme metric
# MAGIC views (≈5 regardless of how many questions/tables you feed it). That is great for a
# MAGIC curated semantic layer but does NOT give per-table coverage at scale.
# MAGIC
# MAGIC This driver loops over a target table list and, for each table, generates questions
# MAGIC (reusing `generate_questions`'s AI path), ingests them tagged with `source_table`,
# MAGIC then calls `generate_metric_views(table_filter=<table>)` so each table yields its own
# MAGIC view(s). Context is auto-scoped to the table + its FK neighbors, keeping each prompt
# MAGIC cheap and on-topic.
# MAGIC
# MAGIC Cost: one questions AI_QUERY + one plan + N generate AI_QUERY per table. Use
# MAGIC `max_tables` to cap a smoke run, and `databricks-claude-sonnet-4-6` for quality.

# COMMAND ----------
# MAGIC %pip install -qqqq -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("table_names", "", "Comma-separated FQNs or wildcards (e.g. cat.schema.*)")
dbutils.widgets.text("catalog_name", "", "Catalog holding the KB / semantic-layer tables")
dbutils.widgets.text("schema_name", "", "Schema holding the KB / semantic-layer tables")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4-6", "Model Endpoint")
dbutils.widgets.text("questions_per_table", "4", "Questions to generate per table (1-6)")
dbutils.widgets.text("max_tables", "", "Optional cap on number of tables (smoke runs)")
dbutils.widgets.dropdown("use_ai_questions", "true", ["false", "true"], "AI-refine questions")

table_names_raw = dbutils.widgets.get("table_names")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_endpoint = dbutils.widgets.get("model_endpoint")
questions_per_table = max(1, min(6, int(dbutils.widgets.get("questions_per_table") or "4")))
max_tables_raw = dbutils.widgets.get("max_tables").strip()
max_tables = int(max_tables_raw) if max_tables_raw else None
use_ai_questions = dbutils.widgets.get("use_ai_questions") == "true"

if not table_names_raw or not catalog_name or not schema_name:
    raise ValueError("table_names, catalog_name, and schema_name are all required")

print(f"KB schema: {catalog_name}.{schema_name}")
print(f"Model: {model_endpoint}   q/table: {questions_per_table}   use_ai: {use_ai_questions}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resolve target tables

# COMMAND ----------

import sys
sys.path.append("../src")

from dbxmetagen.processing import expand_schema_wildcards, ensure_fully_scoped_table_names

raw_list = [t.strip() for t in table_names_raw.split(",") if t.strip()]
expanded = expand_schema_wildcards(raw_list)
resolved = ensure_fully_scoped_table_names(expanded, catalog_name)
target_fqns = sorted(set(resolved))
if max_tables:
    target_fqns = target_fqns[:max_tables]
print(f"Resolved {len(target_fqns)} target tables")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read knowledge bases (scoped to targets)

# COMMAND ----------

def _sql_in_list(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

_in = _sql_in_list(target_fqns)

tbl_rows = [r.asDict() for r in spark.sql(
    f"SELECT table_name, comment, domain, subdomain "
    f"FROM {catalog_name}.{schema_name}.table_knowledge_base WHERE table_name IN ({_in})"
).collect()]
col_rows = [r.asDict() for r in spark.sql(
    f"SELECT table_name, column_name, data_type, comment, classification "
    f"FROM {catalog_name}.{schema_name}.column_knowledge_base WHERE table_name IN ({_in})"
).collect()]

cols_by_table = {}
for c in col_rows:
    cols_by_table.setdefault(c["table_name"], []).append(c)
tbl_by_name = {t["table_name"]: t for t in tbl_rows}
print(f"KB: {len(tbl_rows)} tables, {len(col_rows)} columns")

missing = [t for t in target_fqns if t not in tbl_by_name]
if missing:
    print(f"WARNING: {len(missing)} target(s) absent from table_knowledge_base "
          f"(run bootstrap_knowledge_bases.py first): {missing[:5]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Per-table question generation (AI)
# MAGIC
# MAGIC Mirrors `generate_questions.py`'s AI path but scoped to a single table so the
# MAGIC questions stay table-specific (the batched generator wants per-table questions).

# COMMAND ----------

import json

def ai_questions_for_table(tname):
    t = tbl_by_name.get(tname, {"table_name": tname})
    cols = cols_by_table.get(tname, [])
    col_lines = "; ".join(
        f"{c['column_name']} ({c['data_type']}): {c.get('comment') or ''}".strip()
        for c in cols[:60]
    )
    prompt = (
        f"You are a healthcare analytics expert. Given this table, propose "
        f"{questions_per_table} concise, table-specific business questions a metric view "
        f"should answer. Favor measurable aggregations and breakdowns over free-text lookups. "
        f"Return ONLY a JSON array of strings.\n\n"
        f"Table: {tname}\nComment: {t.get('comment') or ''}\n"
        f"Domain: {t.get('domain') or ''} / {t.get('subdomain') or ''}\nColumns: {col_lines}"
    )
    prompt_sql = prompt.replace("'", "''")
    resp = spark.sql(f"SELECT ai_query('{model_endpoint}', '{prompt_sql}') AS r").collect()[0]["r"]
    arr = json.loads(resp[resp.index("["): resp.rindex("]") + 1])
    return [str(q).strip() for q in arr if str(q).strip()][:questions_per_table]


def templated_questions_for_table(tname):
    # Minimal fallback if AI is off or fails: one generic question keeps the table covered.
    short = tname.split(".")[-1].replace("_", " ")
    return [f"What are the key metrics and breakdowns for {short}?"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Batched generation loop

# COMMAND ----------

from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig

config = SemanticLayerConfig(
    catalog_name=catalog_name,
    schema_name=schema_name,
    model_endpoint=model_endpoint,
    validate_expressions=True,   # run dbxmetagen's own autofix before storing
)
gen = SemanticLayerGenerator(spark, config)
gen.create_tables()

totals = {"generated": 0, "validated": 0, "failed": 0}
per_table = []

for i, tname in enumerate(target_fqns, 1):
    try:
        if use_ai_questions:
            try:
                qs = ai_questions_for_table(tname)
            except Exception as e:  # noqa: BLE001
                print(f"[{i}/{len(target_fqns)}] {tname}: AI questions failed ({e}); using fallback")
                qs = templated_questions_for_table(tname)
        else:
            qs = templated_questions_for_table(tname)
        if not qs:
            qs = templated_questions_for_table(tname)

        gen.ingest_questions(qs, source_table=tname)
        res = gen.generate_metric_views(table_filter=tname)
        for k in totals:
            totals[k] += res.get(k, 0)
        per_table.append((tname, len(qs), res.get("generated", 0),
                          res.get("validated", 0), res.get("failed", 0)))
        print(f"[{i}/{len(target_fqns)}] {tname}: {len(qs)} q -> "
              f"gen={res.get('generated',0)} val={res.get('validated',0)} fail={res.get('failed',0)}")
    except Exception as e:  # noqa: BLE001 -- one bad table must not abort the batch
        print(f"[{i}/{len(target_fqns)}] {tname}: ERROR {e}")
        per_table.append((tname, 0, 0, 0, 0))

print(f"\nBATCH COMPLETE: {totals}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(spark.createDataFrame(
    per_table, ["source_table", "questions", "generated", "validated", "failed"]
))

display(spark.sql(f"""
    SELECT status, COUNT(*) AS n
    FROM {catalog_name}.{schema_name}.metric_view_definitions
    GROUP BY status ORDER BY status
"""))

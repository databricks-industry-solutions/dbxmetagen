# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Semantic Layer Questions
# MAGIC
# MAGIC Auto-generates business questions for the semantic layer and ingests them into
# MAGIC `semantic_layer_questions`. The semantic layer generator
# MAGIC (`generate_semantic_layer.py`) produces one metric-view definition per coherent
# MAGIC question cluster, so this notebook is the entry point that decides *which* tables
# MAGIC get metric views and *what* they measure.
# MAGIC
# MAGIC Reads `table_knowledge_base` + `column_knowledge_base` (populated cheaply by
# MAGIC `bootstrap_knowledge_bases.py` from live UC comments) and classifies each target
# MAGIC table's columns into measures / time dimensions / categorical dimensions, then
# MAGIC emits templated questions. Default path makes **zero LLM calls**; set `use_ai=true`
# MAGIC to have one AI_QUERY per table refine the questions instead.

# COMMAND ----------
# MAGIC %pip install -qqqq -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("table_names", "", "Comma-separated FQNs or wildcards (e.g. cat.schema.*)")
dbutils.widgets.text("catalog_name", "", "Catalog holding the KB tables (semantic-layer output)")
dbutils.widgets.text("schema_name", "", "Schema holding the KB tables (semantic-layer output)")
dbutils.widgets.text("model_endpoint", "databricks-gpt-oss-120b", "Model Endpoint (only used when use_ai=true)")
dbutils.widgets.text("questions_per_table", "3", "Questions to emit per table (1-5)")
dbutils.widgets.dropdown("use_ai", "false", ["false", "true"], "Use AI_QUERY to refine questions")

table_names_raw = dbutils.widgets.get("table_names")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_endpoint = dbutils.widgets.get("model_endpoint")
questions_per_table = max(1, min(5, int(dbutils.widgets.get("questions_per_table") or "3")))
use_ai = dbutils.widgets.get("use_ai") == "true"

if not table_names_raw or not catalog_name or not schema_name:
    raise ValueError("table_names, catalog_name, and schema_name are all required")

print(f"KB schema: {catalog_name}.{schema_name}")
print(f"Targets:   {table_names_raw}")
print(f"Per table: {questions_per_table}   use_ai: {use_ai}")

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
print(f"Resolved {len(target_fqns)} target tables")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read knowledge bases (scoped to targets)
# MAGIC
# MAGIC Mirrors the column set that `SemanticLayerGenerator.build_context()` reads, so the
# MAGIC questions reference the same metadata the generator will see. `table_name` in both
# MAGIC KB tables is the fully-qualified `catalog.schema.table`.

# COMMAND ----------

def _sql_in_list(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

if not target_fqns:
    raise ValueError("No target tables resolved -- nothing to generate questions for.")

_in = _sql_in_list(target_fqns)

tbl_rows = spark.sql(
    f"SELECT table_name, comment, domain, subdomain "
    f"FROM {catalog_name}.{schema_name}.table_knowledge_base "
    f"WHERE table_name IN ({_in})"
).collect()

col_rows = spark.sql(
    f"SELECT table_name, column_name, data_type, comment, classification "
    f"FROM {catalog_name}.{schema_name}.column_knowledge_base "
    f"WHERE table_name IN ({_in})"
).collect()

# Convert Spark Rows to plain dicts so downstream code can use dict access AND .get()
# uniformly (a Row has no .get(), which breaks the AI-prompt path otherwise).
tbl_rows = [r.asDict() for r in tbl_rows]
col_rows = [r.asDict() for r in col_rows]

cols_by_table = {}
for c in col_rows:
    cols_by_table.setdefault(c["table_name"], []).append(c)

print(f"table_knowledge_base rows: {len(tbl_rows)}   column rows: {len(col_rows)}")
missing = [t for t in target_fqns if t not in {r['table_name'] for r in tbl_rows}]
if missing:
    print(f"WARNING: {len(missing)} target(s) absent from table_knowledge_base "
          f"(run bootstrap_knowledge_bases.py first): {missing[:5]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Classify columns
# MAGIC
# MAGIC Heuristics align with `build_context()`'s temporal / categorical / numeric hints.

# COMMAND ----------

_NUMERIC_TYPES = ("int", "bigint", "smallint", "tinyint", "double", "float", "decimal", "numeric")
_MEASURE_KW = ("amount", "amt", "cost", "price", "revenue", "premium", "paid", "allowed",
               "charge", "total", "count", "cnt", "qty", "quantity", "units", "balance",
               "spend", "savings", "score")
_TIME_KW = ("date", "month", "year", "period", "_dt", "report")
_CATEG_KW = ("type", "status", "state", "category", "code", "flag", "indicator", "group",
             "line_of_business", "lob", "segment", "class", "level", "region", "name")
# ETL/audit columns are timestamps but NOT business time dimensions -- exclude them so
# questions trend on service_date/report_month rather than ingest bookkeeping.
_AUDIT_KW = ("load", "ingest", "etl", "_loaded", "inserted", "updated_at", "created_at",
             "_ts", "batch", "source_key", "surrogate", "_scd", "effective_from",
             "effective_to", "rundate", "run_date", "extract")


def _is_numeric(dt):
    dt = (dt or "").lower()
    return any(dt.startswith(t) for t in _NUMERIC_TYPES)


def _is_audit(name):
    return any(k in (name or "").lower() for k in _AUDIT_KW)


def _is_temporal(dt, name):
    dt = (dt or "").lower()
    lname = (name or "").lower()
    if _is_audit(name):
        return False
    return dt.startswith("date") or dt.startswith("timestamp") or any(k in lname for k in _TIME_KW)


def classify(columns):
    measures, time_dims, categ_dims = [], [], []
    for c in columns:
        name = (c["column_name"] or "")
        lname = name.lower()
        dt = c["data_type"] or ""
        if _is_audit(name):
            continue  # ETL/audit columns are not business measures or dimensions
        if _is_temporal(dt, name):
            time_dims.append(name)
        elif _is_numeric(dt) and any(k in lname for k in _MEASURE_KW):
            measures.append(name)
        elif any(k in lname for k in _CATEG_KW) and "key" not in lname and "_id" not in lname:
            categ_dims.append(name)
    # Fallback: any non-audit numeric column can be a measure if none matched by keyword.
    if not measures:
        measures = [c["column_name"] for c in columns
                    if _is_numeric(c["data_type"]) and not _is_audit(c["column_name"])][:3]
    return measures, time_dims, categ_dims

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build templated questions

# COMMAND ----------

import re


def _humanize(token):
    return re.sub(r"[_\.]+", " ", token or "").strip()


def questions_for_table(tbl_row, columns, limit):
    short = tbl_row["table_name"].split(".")[-1]
    subject = _humanize(tbl_row["domain"] or "") or _humanize(short)
    measures, time_dims, categ_dims = classify(columns)
    out = []
    m0 = _humanize(measures[0]) if measures else None
    t0 = _humanize(time_dims[0]) if time_dims else None
    c0 = _humanize(categ_dims[0]) if categ_dims else None
    c1 = _humanize(categ_dims[1]) if len(categ_dims) > 1 else None

    def _total(measure):
        # Avoid "total total ..." when the measure name already implies an aggregate.
        if any(w in measure.lower() for w in ("total", "count", "sum", "amount", "number")):
            return measure
        return f"total {measure}"

    if m0 and t0:
        out.append(f"What is the {_total(m0)} by {t0} for {subject}?")
    if m0 and c0:
        out.append(f"How does {m0} break down by {c0} for {subject}?")
    if m0 and t0 and c0:
        out.append(f"What is the trend of {m0} over {t0} segmented by {c0} for {subject}?")
    if m0 and c1:
        out.append(f"What is {m0} by {c1} for {subject}?")
    if len(measures) > 1 and t0:
        out.append(f"What is the {_total(_humanize(measures[1]))} by {t0} for {subject}?")
    # Last-resort generic question so every table yields at least one.
    if not out:
        out.append(f"What are the key metrics for {subject}?")

    # De-dup while preserving order, then cap.
    seen, deduped = set(), []
    for q in out:
        if q not in seen:
            seen.add(q)
            deduped.append(q)
    return deduped[:limit]


templated = {}
for t in tbl_rows:
    templated[t["table_name"]] = questions_for_table(t, cols_by_table.get(t["table_name"], []), questions_per_table)

total_templated = sum(len(v) for v in templated.values())
print(f"Templated {total_templated} questions across {len(templated)} tables")
for tname, qs in list(templated.items())[:5]:
    print(f"\n{tname}:")
    for q in qs:
        print(f"  - {q}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## (Optional) AI refinement
# MAGIC
# MAGIC When `use_ai=true`, ask the model to produce sharper questions per table from the
# MAGIC table comment + column list. One AI_QUERY per table. Falls back to the templated
# MAGIC questions on any parse failure so the pipeline never stalls.

# COMMAND ----------

questions_by_table = dict(templated)

if use_ai:
    import json
    from pyspark.sql.functions import expr

    for t in tbl_rows:
        tname = t["table_name"]
        cols = cols_by_table.get(tname, [])
        col_lines = "; ".join(
            f"{c['column_name']} ({c['data_type']}): {c.get('comment') or ''}".strip()
            for c in cols[:60]
        )
        prompt = (
            f"You are a healthcare analytics expert. Given this table, propose "
            f"{questions_per_table} concise business questions a metric view should answer. "
            f"Return ONLY a JSON array of strings.\n\n"
            f"Table: {tname}\nComment: {t.get('comment') or ''}\n"
            f"Domain: {t.get('domain') or ''} / {t.get('subdomain') or ''}\nColumns: {col_lines}"
        )
        prompt_sql = prompt.replace("'", "''")
        try:
            resp = spark.sql(
                f"SELECT ai_query('{model_endpoint}', '{prompt_sql}') AS r"
            ).collect()[0]["r"]
            arr = json.loads(resp[resp.index("["): resp.rindex("]") + 1])
            cleaned = [str(q).strip() for q in arr if str(q).strip()][:questions_per_table]
            if cleaned:
                questions_by_table[tname] = cleaned
        except Exception as e:  # noqa: BLE001 -- best-effort; keep templated fallback
            print(f"AI refine failed for {tname}: {e} -- keeping templated questions")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingest into semantic_layer_questions

# COMMAND ----------

from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig

config = SemanticLayerConfig(
    catalog_name=catalog_name,
    schema_name=schema_name,
    model_endpoint=model_endpoint,
)
gen = SemanticLayerGenerator(spark, config)
gen.create_tables()

all_questions = [q for qs in questions_by_table.values() for q in qs]
ingested = gen.ingest_questions(all_questions)
print(f"Ingested {ingested} questions into {catalog_name}.{schema_name}.semantic_layer_questions")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT status, COUNT(*) AS n
    FROM {catalog_name}.{schema_name}.semantic_layer_questions
    GROUP BY status ORDER BY status
"""))

display(spark.sql(f"""
    SELECT question_id, question_text, status, created_at
    FROM {catalog_name}.{schema_name}.semantic_layer_questions
    WHERE status = 'pending'
    ORDER BY created_at DESC
    LIMIT 50
"""))

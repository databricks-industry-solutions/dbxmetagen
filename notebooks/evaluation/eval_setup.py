# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluation Setup: Create Synthetic Tables & Expected Results
# MAGIC
# MAGIC Creates 7 FHIR-aligned healthcare tables with known ground truth for
# MAGIC evaluating dbxmetagen pipeline outputs (comments, PI, domain, ontology, FK).
# MAGIC
# MAGIC **After running this notebook**, run the dbxmetagen pipelines:
# MAGIC 1. Metadata generation with `table_names = {catalog}.{schema}.*` for modes: comment, pi, domain
# MAGIC 2. Full analytics pipeline with `ontology_bundle = fhir_r4` and `table_names = {catalog}.{schema}.*`
# MAGIC
# MAGIC Then run `eval_compare.py` to score the results.
# COMMAND ----------
# MAGIC %pip install -q faker
# MAGIC %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC dbutils.library.restartPython()
# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()

assert catalog_name, "catalog_name is required"
assert schema_name, "schema_name is required"

fq = lambda t: f"{catalog_name}.{schema_name}.{t}"

print(f"Target: {catalog_name}.{schema_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create synthetic data tables

# COMMAND ----------

import sys
sys.path.append("../../src")

from dbxmetagen.evaluation.ground_truth import (
    EVAL_TABLE_NAMES,
    GENERATORS,
    TABLE_SCHEMAS,
)

summary = []

# OVERWRITE (synthetic FHIR eval tables — one table per iteration of `EVAL_TABLE_NAMES`): Fully replaces `{catalog}.{schema}.{table_name}`
#   (patients, providers, encounters, diagnosis, medications, lab_results, insurance_claims); primary key surrogate is synthetic row identities
#   implied by regenerated data — all typed columns rebuilt from `TABLE_SCHEMAS` / `GENERATORS`.
# WHY: Supplies controlled ground-truth relational fixtures so metadata, PI, domain, ontology, and FK pipelines can be scored objectively in `eval_compare`.
# TRADEOFFS: `overwrite` + `overwriteSchema=true` resets schema to generator output (drops ad-hoc columns); fast idempotent setups vs MERGE/SCD —
#   not suitable if eval tables accumulate manual annotations you need to preserve.
for table_name in EVAL_TABLE_NAMES:
    rows = GENERATORS[table_name]()
    schema = TABLE_SCHEMAS[table_name]
    df = spark.createDataFrame(rows, schema=schema)
    # OVERWRITE: Same pattern as above — see eval ground-truth synthetic table comment.
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(table_name))
    count = df.count()
    summary.append((table_name, count, len(schema.fields)))
    print(f"  Created {fq(table_name)}: {count} rows, {len(schema.fields)} columns")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create expected results tables

# COMMAND ----------

from dbxmetagen.evaluation.ground_truth import (
    EXPECTED_PI,
    EXPECTED_DOMAINS,
    EXPECTED_ENTITIES,
    EXPECTED_COLUMN_PROPERTIES,
    EXPECTED_RELATIONSHIPS,
    EXPECTED_FK,
    EXPECTED_COMMENTS,
)

# --- eval_expected_pi ---
pi_rows = [
    {
        "table_name": tbl,
        "column_name": col,
        "expected_type": exp_type,
        "acceptable_alternatives": alts,
    }
    for (tbl, col), (exp_type, alts) in EXPECTED_PI.items()
]

# OVERWRITE (eval_expected_* — first of series): Fully replaces `{catalog}.{schema}.eval_expected_pi`; rows keyed by `(table_name, column_name)` with PI
#   expected_type plus comma-separated acceptable_alternatives derived from frozen `EXPECTED_PI` constants in `ground_truth.py`.
# WHY: Gives eval_compare deterministic labels for precision/recall on PI classifications without scraping external benchmarks.
# TRADEOFFS: Full overwrite aligns truth to code edits instantly; brittle if notebooks hand-edit UC rows — versioning lives in repo not table history;
#   `overwriteSchema` infers/avoids stale columns but can drop manual extensions same as synthetic table pattern.
spark.createDataFrame(pi_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_pi")
)
print(f"  eval_expected_pi: {len(pi_rows)} rows")

# --- eval_expected_domains ---
domain_rows = [
    {
        "table_name": tbl,
        "expected_domain": dom,
        "expected_subdomain": sub,
        "acceptable_domain_alternatives": dom_alts,
        "acceptable_subdomain_alternatives": sub_alts,
    }
    for tbl, (dom, sub, dom_alts, sub_alts) in EXPECTED_DOMAINS.items()
]
# OVERWRITE: Same pattern as above — see eval ground-truth table comment.
spark.createDataFrame(domain_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_domains")
)
print(f"  eval_expected_domains: {len(domain_rows)} rows")

# --- eval_expected_entities ---
entity_rows = [
    {"table_name": tbl, "expected_entity_type": etype, "confidence_tier": tier, "acceptable_alternatives": alts}
    for tbl, (etype, tier, alts) in EXPECTED_ENTITIES.items()
]
# OVERWRITE: Same pattern as above — see eval ground-truth table comment.
spark.createDataFrame(entity_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_entities")
)
print(f"  eval_expected_entities: {len(entity_rows)} rows")

# --- eval_expected_column_properties ---
prop_rows = [
    {
        "table_name": tbl,
        "column_name": col,
        "expected_property_role": prop.role,
        "expected_is_sensitive": prop.is_sensitive,
        "confidence_tier": prop.tier,
    }
    for (tbl, col), prop in EXPECTED_COLUMN_PROPERTIES.items()
]
# OVERWRITE: Same pattern as above — see eval ground-truth table comment.
spark.createDataFrame(prop_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_column_properties")
)
print(f"  eval_expected_column_properties: {len(prop_rows)} rows")

# --- eval_expected_relationships ---
rel_rows = [
    {
        "src_entity_type": src,
        "dst_entity_type": dst,
        "expected_relationship_name": name,
        "acceptable_alternatives": alts,
    }
    for src, dst, name, alts in EXPECTED_RELATIONSHIPS
]
# OVERWRITE: Same pattern as above — see eval ground-truth table comment.
spark.createDataFrame(rel_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_relationships")
)
print(f"  eval_expected_relationships: {len(rel_rows)} rows")

# --- eval_expected_fk ---
fk_rows = [
    {
        "src_table": src_t,
        "src_column": src_c,
        "dst_table": dst_t,
        "dst_column": dst_c,
    }
    for src_t, src_c, dst_t, dst_c in EXPECTED_FK
]
# OVERWRITE: Same pattern as above — see eval ground-truth table comment.
spark.createDataFrame(fk_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_fk")
)
print(f"  eval_expected_fk: {len(fk_rows)} rows")

# --- eval_expected_comments ---
comment_rows = [
    {
        "table_name": tbl,
        "column_name": col,
        "expected_keywords": ",".join(keywords),
    }
    for (tbl, col), keywords in EXPECTED_COMMENTS.items()
]
# OVERWRITE: Same pattern as above — see eval ground-truth table comment.
spark.createDataFrame(comment_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fq("eval_expected_comments")
)
print(f"  eval_expected_comments: {len(comment_rows)} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n=== Eval Setup Complete ===\n")
print(f"{'Table':<35} {'Rows':>6} {'Cols':>6}")
print("-" * 50)
for name, count, cols in summary:
    print(f"{name:<35} {count:>6} {cols:>6}")
print(f"\nExpected results tables written to {catalog_name}.{schema_name}")
print("\nNext steps:")
print(f"  1. Run metagen with table_names='{catalog_name}.{schema_name}.*' for modes: comment, pi, domain")
print(f"  2. Run full analytics pipeline with ontology_bundle='fhir_r4' and table_names='{catalog_name}.{schema_name}.*'")
print(f"  3. Run eval_compare notebook")

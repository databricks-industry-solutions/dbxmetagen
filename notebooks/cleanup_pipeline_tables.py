# Databricks notebook source
# MAGIC %md
# MAGIC # DESTRUCTIVE: Pipeline Table Cleanup
# MAGIC
# MAGIC **WARNING: This notebook DROPS all tables created by the dbxmetagen pipeline.**
# MAGIC
# MAGIC This is intended for development/testing only -- to reset state before a
# MAGIC clean re-run. It does NOT touch your source data tables, only the generated
# MAGIC metadata/analytics tables.
# MAGIC
# MAGIC **There is no undo.** Tables are permanently deleted (including history).
# MAGIC
# MAGIC Both the notebook widget AND the job parameter default `dry_run` to `true`.
# MAGIC You must explicitly set `dry_run=false` to actually drop tables:
# MAGIC ```
# MAGIC databricks bundle run cleanup_pipeline_job -t dev -p DEFAULT \
# MAGIC   --params dry_run=false
# MAGIC ```
# MAGIC
# MAGIC Tables deleted (when they exist):
# MAGIC - `metadata_generation_log` -- LLM-generated metadata results
# MAGIC - `metadata_control_*` -- concurrent processing control table(s)
# MAGIC - `table_knowledge_base` / `column_knowledge_base` -- knowledge bases
# MAGIC - `ontology_entities` / `ontology_relationships` / `ontology_column_properties` / `ontology_metrics`
# MAGIC - `discovery_diff_report` -- entity discovery changelog
# MAGIC - `graph_nodes` / `graph_edges` -- knowledge graph
# MAGIC - `fk_predictions` -- foreign key predictions
# MAGIC - `data_quality_scores` -- DQ scores
# MAGIC - `table_profiling_results` -- profiling stats
# MAGIC - `similarity_edges` -- embedding-based similarity
# MAGIC - `cluster_assignments` -- table clustering results

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run (safe default)")
dbutils.widgets.dropdown("include_control_table", "true", ["true", "false"], "Include Control Table")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name").strip()
schema = dbutils.widgets.get("schema_name").strip()
is_dry_run = dbutils.widgets.get("dry_run").lower() == "true"
include_control = dbutils.widgets.get("include_control_table").lower() == "true"

assert catalog, "catalog_name is required"
assert schema, "schema_name is required"

TABLES = [
    "metadata_generation_log",
    "table_knowledge_base",
    "column_knowledge_base",
    "ontology_entities",
    "ontology_relationships",
    "ontology_column_properties",
    "ontology_metrics",
    "discovery_diff_report",
    "graph_nodes",
    "graph_edges",
    "fk_predictions",
    "data_quality_scores",
    "table_profiling_results",
    "similarity_edges",
    "cluster_assignments",
]

if include_control:
    existing = [
        r.tableName
        for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    ]
    control_tables = [t for t in existing if t.startswith("metadata_control_")]
    TABLES.extend(control_tables)

fq_tables = [f"{catalog}.{schema}.{t}" for t in TABLES]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables targeted for deletion

# COMMAND ----------

existing_targets = []
for fq in fq_tables:
    try:
        spark.sql(f"DESCRIBE TABLE {fq}")
        existing_targets.append(fq)
    except Exception:
        pass

print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"dry_run:  {is_dry_run}")
print(f"\nFound {len(existing_targets)} of {len(fq_tables)} pipeline tables:")
for t in existing_targets:
    print(f"  [EXISTS] {t}")

missing = set(fq_tables) - set(existing_targets)
if missing:
    print(f"\nNot found (will skip): {len(missing)}")
    for t in sorted(missing):
        print(f"  [SKIP]   {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute deletion
# MAGIC
# MAGIC **WARNING: Set `dry_run` = `false` to actually drop tables. This is irreversible.**

# COMMAND ----------

if is_dry_run:
    print("DRY RUN -- no tables dropped. Set dry_run=false to execute.")
else:
    print("=" * 60)
    print("  LIVE MODE: DROPPING TABLES")
    print("=" * 60)
    dropped, failed = [], []
    for fq in existing_targets:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fq}")
            dropped.append(fq)
            print(f"  DROPPED: {fq}")
        except Exception as e:
            failed.append((fq, str(e)))
            print(f"  FAILED:  {fq} -- {e}")

    print(f"\nDropped {len(dropped)} tables.")
    if failed:
        print(f"Failed to drop {len(failed)} tables:")
        for t, err in failed:
            print(f"  - {t}: {err}")

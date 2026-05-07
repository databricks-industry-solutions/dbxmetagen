# Databricks notebook source
# MAGIC %md
# MAGIC # Update Graph with Quality Scores
# MAGIC 
# MAGIC This notebook updates graph_nodes with quality scores from data quality analysis.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MERGE: Updates `quality_score` and `updated_at` on `graph_nodes` rows where `node_type='table'` and `id` equals
#   `table_name`; source scores come from latest `data_quality_scores` snapshot per table (`ORDER BY created_at DESC LIMIT 1` correlated subquery).
# WHY: Surfaces freshest data-profiling/DQ rollup on table nodes so the graph mirrors operational quality posture for search and UI badges.
# TRADEOFFS: Correlated subquery-per-row can be heavier than window pre-aggregation; only touches table nodes — entity/other node types untouched;
#   if multiple snapshots share same `created_at`, choice is nondeterministic without tie-break columns.
spark.sql(f"""
MERGE INTO {catalog_name}.{schema_name}.graph_nodes AS target
USING (
    SELECT table_name, overall_score as quality_score
    FROM {catalog_name}.{schema_name}.data_quality_scores
    WHERE snapshot_id = (
        SELECT snapshot_id 
        FROM {catalog_name}.{schema_name}.data_quality_scores dq
        WHERE dq.table_name = data_quality_scores.table_name
        ORDER BY created_at DESC
        LIMIT 1
    )
) AS source
ON target.id = source.table_name AND target.node_type = 'table'
WHEN MATCHED THEN UPDATE SET
    target.quality_score = source.quality_score,
    target.updated_at = current_timestamp()
""")

print("Updated graph nodes with quality scores")



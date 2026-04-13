# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Health Check
# MAGIC 
# MAGIC Monitors dbxmetagen pipeline health:
# MAGIC - Tables stuck in `in_progress` beyond timeout
# MAGIC - High retry counts in metadata_generation_log
# MAGIC - Staleness of ontology, FK, and knowledge base tables
# MAGIC - Overall coverage: tables generated vs total in schema

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("stale_threshold_hours", "72", "Stale Threshold (hours)")
dbutils.widgets.text("stuck_timeout_minutes", "120", "Stuck Timeout (minutes)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
stale_hours = int(dbutils.widgets.get("stale_threshold_hours"))
stuck_minutes = int(dbutils.widgets.get("stuck_timeout_minutes"))

fq = f"{catalog_name}.{schema_name}"

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stuck Tables
# MAGIC
# MAGIC Tables with `_status = 'in_progress'` beyond the configured timeout.

# COMMAND ----------

stuck_count = 0
try:
    control_tables = [
        r.tableName
        for r in spark.sql(f"SHOW TABLES IN {fq} LIKE 'metadata_control_*'").collect()
    ]
    if not control_tables:
        print("No control tables found (metadata_control_*).")
    else:
        union_sql = " UNION ALL ".join(
            f"""
            SELECT table_name, _status, _run_id, _task_id, _updated_at,
                   ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(_updated_at)) / 60, 1) AS minutes_stuck
            FROM {fq}.{ct}
            WHERE _status = 'in_progress'
              AND _updated_at < current_timestamp() - INTERVAL {stuck_minutes} MINUTE
            """
            for ct in control_tables
        )
        stuck_df = spark.sql(f"{union_sql} ORDER BY _updated_at")
        stuck_count = stuck_df.count()
        print(f"Stuck tables: {stuck_count}")
        if stuck_count > 0:
            display(stuck_df)
        else:
            print("No tables stuck in in_progress state.")
except Exception as e:
    print(f"Control table query error: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## High Retry Tables
# MAGIC
# MAGIC Tables with the most generation attempts, indicating persistent failures.

# COMMAND ----------

try:
    retry_df = spark.sql(f"""
    SELECT
        table_name,
        mode,
        COUNT(*) AS attempt_count,
        COUNT(CASE WHEN _status = 'completed' THEN 1 END) AS success_count,
        COUNT(CASE WHEN _status = 'failed' THEN 1 END) AS failure_count,
        MAX(run_timestamp) AS last_attempt
    FROM {fq}.metadata_generation_log
    GROUP BY table_name, mode
    HAVING COUNT(*) > 3
    ORDER BY attempt_count DESC
    LIMIT 50
    """)
    display(retry_df)
except Exception as e:
    print(f"Log table not found or error: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table Freshness
# MAGIC
# MAGIC Last update timestamp and row count for key pipeline output tables.

# COMMAND ----------

output_tables = [
    "table_knowledge_base",
    "column_knowledge_base",
    "schema_knowledge_base",
    "graph_nodes",
    "graph_edges",
    "ontology_entities",
    "ontology_column_properties",
    "ontology_relationships",
    "fk_predictions",
    "similarity_edges",
    "cluster_assignments",
    "data_quality_scores",
]

freshness_rows = []
for tbl in output_tables:
    try:
        row = spark.sql(f"""
            SELECT
                '{tbl}' AS table_name,
                COUNT(*) AS row_count,
                MAX(COALESCE(updated_at, created_at)) AS last_updated
            FROM {fq}.{tbl}
        """).collect()[0]
        freshness_rows.append(row.asDict())
    except Exception:
        freshness_rows.append({"table_name": tbl, "row_count": 0, "last_updated": None})

freshness_df = spark.createDataFrame(freshness_rows)
freshness_df = freshness_df.selectExpr(
    "table_name",
    "row_count",
    "last_updated",
    f"CASE WHEN last_updated < current_timestamp() - INTERVAL {stale_hours} HOUR THEN 'STALE' ELSE 'OK' END AS status",
)
display(freshness_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Coverage Summary
# MAGIC
# MAGIC How many tables in the schema have been processed by each mode.

# COMMAND ----------

try:
    coverage_df = spark.sql(f"""
    WITH all_tables AS (
        SELECT table_name
        FROM {catalog_name}.information_schema.tables
        WHERE table_schema = '{schema_name}'
          AND table_type = 'MANAGED'
          AND table_name NOT LIKE 'metadata_%'
          AND table_name NOT LIKE 'ontology_%'
          AND table_name NOT LIKE 'fk_%'
          AND table_name NOT LIKE 'data_quality_%'
          AND table_name NOT IN ('graph_nodes', 'graph_edges', 'similarity_edges',
                                 'cluster_assignments', 'table_knowledge_base',
                                 'column_knowledge_base', 'schema_knowledge_base')
    ),
    generated AS (
        SELECT DISTINCT table_name, mode
        FROM {fq}.metadata_generation_log
        WHERE _status = 'completed'
    )
    SELECT
        (SELECT COUNT(*) FROM all_tables) AS total_tables,
        COUNT(DISTINCT CASE WHEN g_comment.table_name IS NOT NULL THEN a.table_name END) AS comment_generated,
        COUNT(DISTINCT CASE WHEN g_pi.table_name IS NOT NULL THEN a.table_name END) AS pi_generated,
        COUNT(DISTINCT CASE WHEN g_domain.table_name IS NOT NULL THEN a.table_name END) AS domain_generated,
        COUNT(DISTINCT CASE WHEN kb.table_name IS NOT NULL THEN a.table_name END) AS in_knowledge_base,
        COUNT(DISTINCT CASE WHEN oe.table_name IS NOT NULL THEN a.table_name END) AS ontology_classified
    FROM all_tables a
    LEFT JOIN generated g_comment ON a.table_name = g_comment.table_name AND g_comment.mode = 'comment'
    LEFT JOIN generated g_pi ON a.table_name = g_pi.table_name AND g_pi.mode = 'pi'
    LEFT JOIN generated g_domain ON a.table_name = g_domain.table_name AND g_domain.mode = 'domain'
    LEFT JOIN (
        SELECT DISTINCT table_name FROM {fq}.table_knowledge_base
    ) kb ON a.table_name = kb.table_name
    LEFT JOIN (
        SELECT DISTINCT EXPLODE(source_tables) AS table_name FROM {fq}.ontology_entities
    ) oe ON a.table_name = oe.table_name
    """)
    display(coverage_df)
except Exception as e:
    print(f"Coverage query error: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Quick health status.

# COMMAND ----------

print(f"Pipeline Health Check for {fq}")
print(f"  Lookback: stale_threshold={stale_hours}h, stuck_timeout={stuck_minutes}m")
if stuck_count > 0:
    print(f"  WARNING: {stuck_count} table(s) stuck in in_progress")
stale_count = freshness_df.filter("status = 'STALE'").count()
if stale_count > 0:
    print(f"  WARNING: {stale_count} output table(s) are stale (>{stale_hours}h since last update)")
empty_count = freshness_df.filter("row_count = 0").count()
if empty_count > 0:
    print(f"  WARNING: {empty_count} output table(s) are empty")
if stuck_count == 0 and stale_count == 0 and empty_count == 0:
    print("  All checks passed.")

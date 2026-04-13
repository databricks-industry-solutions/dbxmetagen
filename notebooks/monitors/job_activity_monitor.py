# Databricks notebook source
# MAGIC %md
# MAGIC # Job Activity Monitor
# MAGIC 
# MAGIC Queries system tables to track dbxmetagen job execution health:
# MAGIC - Run durations, success/failure rates per job
# MAGIC - DBU consumption by job and cluster type
# MAGIC - Anomaly detection (runs exceeding 2x historical average)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("lookback_days", "30", "Lookback Days")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
lookback_days = int(dbutils.widgets.get("lookback_days"))

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Job Run Summary
# MAGIC
# MAGIC Per-job: run count, success rate, avg/P95 duration, last run timestamp.

# COMMAND ----------

job_runs_df = spark.sql(f"""
SELECT
    j.name AS job_name,
    COUNT(*) AS total_runs,
    COUNT(CASE WHEN jr.result_state = 'SUCCESS' THEN 1 END) AS successful_runs,
    ROUND(COUNT(CASE WHEN jr.result_state = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*), 1) AS success_rate_pct,
    ROUND(AVG(jr.execution_duration) / 1000 / 60, 1) AS avg_duration_min,
    ROUND(PERCENTILE(jr.execution_duration / 1000 / 60, 0.95), 1) AS p95_duration_min,
    MAX(jr.end_time) AS last_run_at,
    COUNT(CASE WHEN jr.result_state NOT IN ('SUCCESS', 'SKIPPED') THEN 1 END) AS failed_runs
FROM system.lakeflow.job_run_timeline jr
JOIN system.lakeflow.jobs j ON jr.job_id = j.job_id
WHERE (j.name LIKE '%metagen%' OR j.name LIKE '%dbxmetagen%')
  AND jr.period_start_time >= current_date() - INTERVAL {lookback_days} DAY
GROUP BY j.name
ORDER BY total_runs DESC
""")

display(job_runs_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Task-Level Duration Breakdown
# MAGIC
# MAGIC Per-task within the analytics pipeline: avg duration, failure rate.

# COMMAND ----------

task_runs_df = spark.sql(f"""
SELECT
    j.name AS job_name,
    t.task_key,
    COUNT(*) AS task_runs,
    ROUND(AVG(t.execution_duration) / 1000 / 60, 1) AS avg_duration_min,
    ROUND(MAX(t.execution_duration) / 1000 / 60, 1) AS max_duration_min,
    COUNT(CASE WHEN t.result_state != 'SUCCESS' THEN 1 END) AS failures
FROM system.lakeflow.job_task_run_timeline t
JOIN system.lakeflow.jobs j ON t.job_id = j.job_id
WHERE (j.name LIKE '%metagen%' OR j.name LIKE '%analytics_pipeline%')
  AND t.period_start_time >= current_date() - INTERVAL {lookback_days} DAY
GROUP BY j.name, t.task_key
ORDER BY j.name, avg_duration_min DESC
""")

display(task_runs_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DBU Consumption by Job
# MAGIC
# MAGIC Aggregate billing usage attributed to dbxmetagen jobs.

# COMMAND ----------

dbu_df = spark.sql(f"""
SELECT
    usage_metadata.job_id,
    j.name AS job_name,
    sku_name,
    ROUND(SUM(usage_quantity), 2) AS total_dbus,
    COUNT(DISTINCT usage_date) AS active_days,
    ROUND(SUM(usage_quantity) / NULLIF(COUNT(DISTINCT usage_date), 0), 2) AS avg_dbus_per_day
FROM system.billing.usage u
LEFT JOIN system.lakeflow.jobs j ON u.usage_metadata.job_id = j.job_id
WHERE usage_metadata.job_id IS NOT NULL
  AND (j.name LIKE '%metagen%' OR j.name LIKE '%dbxmetagen%')
  AND usage_date >= current_date() - INTERVAL {lookback_days} DAY
GROUP BY usage_metadata.job_id, j.name, sku_name
ORDER BY total_dbus DESC
""")

display(dbu_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cost Trend (Weekly)
# MAGIC
# MAGIC Weekly DBU consumption across all dbxmetagen jobs.

# COMMAND ----------

trend_df = spark.sql(f"""
SELECT
    DATE_TRUNC('WEEK', usage_date) AS week,
    sku_name,
    ROUND(SUM(usage_quantity), 2) AS total_dbus
FROM system.billing.usage u
LEFT JOIN system.lakeflow.jobs j ON u.usage_metadata.job_id = j.job_id
WHERE usage_metadata.job_id IS NOT NULL
  AND (j.name LIKE '%metagen%' OR j.name LIKE '%dbxmetagen%')
  AND usage_date >= current_date() - INTERVAL {lookback_days} DAY
GROUP BY DATE_TRUNC('WEEK', usage_date), sku_name
ORDER BY week
""")

display(trend_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Anomaly Detection
# MAGIC
# MAGIC Runs that exceeded 2x the historical average duration for their job.

# COMMAND ----------

anomalies_df = spark.sql(f"""
WITH job_stats AS (
    SELECT
        jr.job_id,
        j.name AS job_name,
        AVG(jr.execution_duration) AS avg_duration,
        STDDEV(jr.execution_duration) AS stddev_duration
    FROM system.lakeflow.job_run_timeline jr
    JOIN system.lakeflow.jobs j ON jr.job_id = j.job_id
    WHERE (j.name LIKE '%metagen%' OR j.name LIKE '%dbxmetagen%')
      AND jr.result_state = 'SUCCESS'
      AND jr.period_start_time >= current_date() - INTERVAL {lookback_days * 2} DAY
    GROUP BY jr.job_id, j.name
)
SELECT
    js.job_name,
    jr.run_id,
    jr.start_time,
    ROUND(jr.execution_duration / 1000 / 60, 1) AS duration_min,
    ROUND(js.avg_duration / 1000 / 60, 1) AS avg_duration_min,
    ROUND(jr.execution_duration / NULLIF(js.avg_duration, 0), 1) AS ratio_to_avg,
    jr.result_state
FROM system.lakeflow.job_run_timeline jr
JOIN job_stats js ON jr.job_id = js.job_id
WHERE jr.execution_duration > 2 * js.avg_duration
  AND jr.period_start_time >= current_date() - INTERVAL {lookback_days} DAY
ORDER BY jr.start_time DESC
""")

display(anomalies_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cluster Utilization
# MAGIC
# MAGIC Cluster spin-up times and active compute for dbxmetagen jobs.

# COMMAND ----------

cluster_df = spark.sql(f"""
SELECT
    j.name AS job_name,
    jr.run_id,
    jr.start_time,
    ROUND(jr.setup_duration / 1000 / 60, 1) AS setup_min,
    ROUND(jr.execution_duration / 1000 / 60, 1) AS execution_min,
    ROUND(jr.cleanup_duration / 1000 / 60, 1) AS cleanup_min,
    jr.cluster_spec.existing_cluster_id,
    jr.result_state
FROM system.lakeflow.job_run_timeline jr
JOIN system.lakeflow.jobs j ON jr.job_id = j.job_id
WHERE (j.name LIKE '%metagen%' OR j.name LIKE '%dbxmetagen%')
  AND jr.period_start_time >= current_date() - INTERVAL {lookback_days} DAY
ORDER BY jr.start_time DESC
LIMIT 100
""")

display(cluster_df)

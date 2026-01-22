# Databricks notebook source
# MAGIC %md
# MAGIC # Update Graph with Quality Scores
# MAGIC 
# MAGIC This notebook updates graph_nodes with quality scores from data quality analysis.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# Update graph_nodes with quality scores
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

# COMMAND ----------

# Show nodes with quality scores
display(spark.sql(f"""
    SELECT id, node_type, quality_score, domain
    FROM {catalog_name}.{schema_name}.graph_nodes
    WHERE quality_score IS NOT NULL
    ORDER BY quality_score ASC
    LIMIT 20
"""))


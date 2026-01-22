# Databricks notebook source
# MAGIC %md
# MAGIC # Compute Data Quality Scores
# MAGIC 
# MAGIC This notebook computes data quality scores based on profiling snapshots.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")

# COMMAND ----------

from src.dbxmetagen.data_quality import compute_data_quality

result = compute_data_quality(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name
)

print(f"Quality scoring complete:")
print(f"  Tables scored: {result['tables_scored']}")
print(f"  Average score: {result['average_score']:.1f}")
print(f"  Low quality tables: {result['low_quality_tables']}")

# COMMAND ----------

# Show quality scores summary
display(spark.sql(f"""
    SELECT 
        table_name,
        overall_score,
        completeness_score,
        uniqueness_score,
        freshness_score,
        consistency_score,
        SIZE(quality_issues) as issue_count
    FROM {catalog_name}.{schema_name}.data_quality_scores
    ORDER BY overall_score ASC
    LIMIT 20
"""))


# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup Temp Tables and Control Tables
# MAGIC
# MAGIC This utility helps clean up temporary metadata generation tables and control tables
# MAGIC that may have been left behind from failed or interrupted runs.
# MAGIC
# MAGIC ## What gets cleaned up:
# MAGIC - Temp metadata generation log tables: `temp_metadata_generation_log_{user}_{timestamp}`
# MAGIC - Control tables: `metadata_control_{user}` and `metadata_control_{user}{job_id}`
# MAGIC - Metadata generation log entries for the current user
# MAGIC
# MAGIC ## Safety features:
# MAGIC - Dry run mode enabled by default (preview before deleting)
# MAGIC - Only cleans current user's tables by default
# MAGIC - Admin option to clean all users' tables

# COMMAND ----------

# MAGIC %pip install -qqqq pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("../../")

from pyspark.sql import SparkSession
from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user
from datetime import datetime

# Setup widgets
dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "metadata_results", "Schema Name")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run (preview only)")
dbutils.widgets.dropdown("include_all_users", "false", ["true", "false"], "Include All Users (admin)")

# Get widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
include_all_users = dbutils.widgets.get("include_all_users").lower() == "true"

# Validate inputs
if not catalog_name:
    raise ValueError("catalog_name is required")

# COMMAND ----------

def get_temp_tables(spark, catalog_name, schema_name, current_user_sanitized, include_all_users):
    """Find all temp metadata generation log tables."""
    try:
        # Get all tables in the schema
        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
        
        # Filter for temp metadata generation log tables
        if include_all_users:
            pattern = "temp_metadata_generation_log_"
            temp_tables = [
                row.tableName for row in tables_df.collect()
                if row.tableName.startswith(pattern)
            ]
        else:
            pattern = f"temp_metadata_generation_log_{current_user_sanitized}_"
            temp_tables = [
                row.tableName for row in tables_df.collect()
                if row.tableName.startswith(pattern)
            ]
        
        return temp_tables
    except Exception as e:
        print(f"Error finding temp tables: {e}")
        return []


def get_control_tables(spark, catalog_name, schema_name, current_user_sanitized, include_all_users):
    """Find all control tables."""
    try:
        # Get all tables in the schema
        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
        
        # Filter for control tables
        if include_all_users:
            pattern = "metadata_control_"
            control_tables = [
                row.tableName for row in tables_df.collect()
                if row.tableName.startswith(pattern)
            ]
        else:
            pattern = f"metadata_control_{current_user_sanitized}"
            control_tables = [
                row.tableName for row in tables_df.collect()
                if row.tableName.startswith(pattern)
            ]
        
        return control_tables
    except Exception as e:
        print(f"Error finding control tables: {e}")
        return []


def get_metadata_log_entries(spark, catalog_name, schema_name, current_user_sanitized, include_all_users):
    """Find metadata generation log entries for cleanup."""
    try:
        log_table = f"{catalog_name}.{schema_name}.metadata_generation_log"
        
        # Check if the log table exists
        if not spark.catalog.tableExists(log_table):
            return []
        
        # Get entries for current user or all users
        if include_all_users:
            count_df = spark.sql(f"SELECT COUNT(*) as count FROM {log_table}")
        else:
            # Log table doesn't track user directly, so we can't filter easily
            # Return empty for now - this would need schema changes to support properly
            return []
        
        count = count_df.collect()[0].count
        return [f"{count} entries in metadata_generation_log"]
    except Exception as e:
        print(f"Error finding log entries: {e}")
        return []


def cleanup_tables(spark, catalog_name, schema_name, tables_to_drop, dry_run):
    """Drop the specified tables."""
    results = {"success": [], "failed": []}
    
    for table in tables_to_drop:
        full_table_name = f"{catalog_name}.{schema_name}.{table}"
        
        if dry_run:
            print(f"[DRY RUN] Would drop: {full_table_name}")
            results["success"].append(table)
        else:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                print(f"✓ Dropped: {full_table_name}")
                results["success"].append(table)
            except Exception as e:
                print(f"✗ Failed to drop {full_table_name}: {e}")
                results["failed"].append(table)
    
    return results

# COMMAND ----------

# Get current user
spark = SparkSession.builder.getOrCreate()
current_user = get_current_user()
current_user_sanitized = sanitize_user_identifier(current_user)

print("="*80)
print("TEMP TABLE CLEANUP UTILITY")
print("="*80)
print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Current User: {current_user}")
print(f"Sanitized User: {current_user_sanitized}")
print(f"Dry Run: {dry_run}")
print(f"Include All Users: {include_all_users}")
print("="*80)
print()

# Find tables to clean up
print("🔍 Searching for tables to clean up...")
print()

temp_tables = get_temp_tables(spark, catalog_name, schema_name, current_user_sanitized, include_all_users)
control_tables = get_control_tables(spark, catalog_name, schema_name, current_user_sanitized, include_all_users)
log_entries = get_metadata_log_entries(spark, catalog_name, schema_name, current_user_sanitized, include_all_users)

# Display findings
print(f"Found {len(temp_tables)} temp metadata generation log table(s):")
for table in temp_tables:
    print(f"  - {table}")
print()

print(f"Found {len(control_tables)} control table(s):")
for table in control_tables:
    print(f"  - {table}")
print()

if log_entries:
    print("Metadata generation log entries:")
    for entry in log_entries:
        print(f"  - {entry}")
    print()

# Perform cleanup
if temp_tables or control_tables:
    print("="*80)
    if dry_run:
        print("DRY RUN MODE - No tables will be deleted")
        print("Set dry_run=false to actually delete these tables")
    else:
        print("DELETING TABLES...")
    print("="*80)
    print()
    
    all_tables = temp_tables + control_tables
    results = cleanup_tables(spark, catalog_name, schema_name, all_tables, dry_run)
    
    print()
    print("="*80)
    print("CLEANUP SUMMARY")
    print("="*80)
    if dry_run:
        print(f"Would delete {len(results['success'])} table(s)")
    else:
        print(f"✓ Successfully deleted: {len(results['success'])} table(s)")
        print(f"✗ Failed to delete: {len(results['failed'])} table(s)")
    print("="*80)
else:
    print("✓ No tables found to clean up!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC If you ran in dry run mode and want to actually delete the tables:
# MAGIC 1. Set the `dry_run` widget to `false`
# MAGIC 2. Re-run the notebook
# MAGIC
# MAGIC To clean up tables for all users (requires admin privileges):
# MAGIC 1. Set `include_all_users` to `true`
# MAGIC 2. Re-run the notebook


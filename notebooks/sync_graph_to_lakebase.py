# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Graph Data to Lakebase
# MAGIC Creates synced database tables that replicate `graph_nodes` and `graph_edges`
# MAGIC from Unity Catalog into a Lakebase Provisioned instance. Uses the Databricks SDK
# MAGIC `w.database.create_synced_database_table()` with SNAPSHOT scheduling.
# COMMAND ----------
# MAGIC %pip install --upgrade "databricks-sdk>=0.81.0"
# MAGIC %restart_python
# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters
# COMMAND ----------
dbutils.widgets.text("source_catalog", "", "Source Catalog")
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("lakebase_catalog", "dbxmetagen_graphrag", "Lakebase Catalog")
dbutils.widgets.text("lakebase_instance_name", "dbxmetagen", "Lakebase Instance Name")

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
lb_catalog = dbutils.widgets.get("lakebase_catalog")
lb_instance = dbutils.widgets.get("lakebase_instance_name")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Enable CDF on source tables
# COMMAND ----------
for tbl in ["graph_nodes", "graph_edges"]:
    fqn = f"{source_catalog}.{source_schema}.{tbl}"
    print(f"Enabling CDF on {fqn}")
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create synced tables
# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    SyncedTableSchedulingPolicy,
)

w = WorkspaceClient()

TABLES = [
    {
        "source": f"{source_catalog}.{source_schema}.graph_nodes",
        "dest": f"{lb_catalog}.public.graph_nodes",
        "pk": ["id"],
    },
    {
        "source": f"{source_catalog}.{source_schema}.graph_edges",
        "dest": f"{lb_catalog}.public.graph_edges",
        "pk": ["src", "dst", "relationship"],
    },
]

for t in TABLES:
    print(f"Syncing: {t['source']} -> {t['dest']}")
    try:
        synced = w.database.create_synced_database_table(
            synced_table=SyncedDatabaseTable(
                name=t["dest"],
                database_instance_name=lb_instance,
                spec=SyncedTableSpec(
                    source_table_full_name=t["source"],
                    primary_key_columns=t["pk"],
                    scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                    create_database_objects_if_missing=True,
                ),
            )
        )
        print(f"  Created: {synced.name}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            print(f"  Already exists, skipping")
        else:
            raise
# COMMAND ----------
# MAGIC %md
# MAGIC ## Check sync status
# COMMAND ----------
import time

for t in TABLES:
    status = w.database.get_synced_database_table(name=t["dest"])
    sync_status = status.data_synchronization_status
    print(f"{t['dest']}:")
    print(f"  State: {sync_status.detailed_state if sync_status else 'UNKNOWN'}")
    print(f"  Message: {sync_status.message if sync_status else 'N/A'}")

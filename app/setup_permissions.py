# Databricks notebook source
# MAGIC %md
# MAGIC # DBXMetaGen Permissions Setup
# MAGIC
# MAGIC This notebook sets up the necessary Unity Catalog permissions for the DBXMetaGen app service principal.

# COMMAND ----------

# Define widgets for job parameters
dbutils.widgets.text("catalog_name", "dbxmetagen", "Catalog Name")
dbutils.widgets.text("app_service_principal", "", "App Service Principal ID")
dbutils.widgets.text("schemas", "", "Target Schemas (comma-separated)")

# Get parameters from job
catalog_name = dbutils.widgets.get("catalog_name")
app_service_principal = dbutils.widgets.get("app_service_principal")
target_schemas_str = dbutils.widgets.get("schemas")

# Parse target schemas - handle both comma-separated strings and empty values
target_schemas = []
if target_schemas_str and target_schemas_str.strip():
    # Split by comma and clean up whitespace
    target_schemas = [
        schema.strip() for schema in target_schemas_str.split(",") if schema.strip()
    ]

print(f"Setting up permissions for:")
print(f"  Catalog: {catalog_name}")
print(f"  Service Principal ID: '{app_service_principal}'")
print(f"  Service Principal Length: {len(app_service_principal)}")
print(f"  Service Principal Type: {type(app_service_principal)}")
print(f"  Target Schemas: {target_schemas}")
print(f"  Number of Target Schemas: {len(target_schemas)}")

# Debug: Show the exact SQL that will be executed
print(
    f"🔍 Sample SQL: GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{app_service_principal}`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Catalog Access

# COMMAND ----------

# Grant USE CATALOG permission
try:
    sql_statement = (
        f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{app_service_principal}`"
    )
    print(f"🔍 Executing: {sql_statement}")
    spark.sql(sql_statement)
    print(f"✅ Granted USE CATALOG on {catalog_name}")
except Exception as e:
    print(f"❌ Error granting USE CATALOG: {str(e)}")
    print(
        f"   SQL was: GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{app_service_principal}`"
    )
    print(f"   Catalog: '{catalog_name}'")
    print(f"   Service Principal: '{app_service_principal}'")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Schema Permissions

# COMMAND ----------

# Grant CREATE SCHEMA permission
spark.sql(
    f"GRANT CREATE SCHEMA ON CATALOG `{catalog_name}` TO `{app_service_principal}`"
)
print(f"✅ Granted CREATE SCHEMA on {catalog_name}")

# Grant SELECT on target schemas
if target_schemas:
    print(
        f"📋 Granting SELECT privileges on {len(target_schemas)} specified schemas..."
    )
    for schema_name in target_schemas:
        try:
            # Check if schema exists first
            try:
                spark.sql(f"DESCRIBE SCHEMA `{catalog_name}`.`{schema_name}`")
                schema_exists = True
            except:
                schema_exists = False

            if schema_exists:
                spark.sql(
                    f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_service_principal}`"
                )
                spark.sql(
                    f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_service_principal}`"
                )
                print(f"✅ Granted SELECT on schema {catalog_name}.{schema_name}")
            else:
                print(
                    f"⚠️  Schema {catalog_name}.{schema_name} does not exist, skipping"
                )
        except Exception as e:
            print(
                f"❌ Error granting SELECT on schema {catalog_name}.{schema_name}: {e}"
            )
else:
    print("📋 No target schemas specified, granting SELECT on all existing schemas...")
    # Grant SELECT on all schemas (for existing schemas) - fallback behavior
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()
        for schema in schemas:
            schema_name = schema["namespace"]
            spark.sql(
                f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_service_principal}`"
            )
            spark.sql(
                f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{app_service_principal}`"
            )
            print(f"✅ Granted SELECT on schema {catalog_name}.{schema_name}")
    except Exception as e:
        print(f"⚠️  Could not grant SELECT on existing schemas: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Table Permissions

# COMMAND ----------

# Grant SELECT on tables in target schemas (for metadata generation)
if target_schemas:
    print(
        f"📋 Granting SELECT privileges on tables in {len(target_schemas)} specified schemas..."
    )
    for schema_name in target_schemas:
        try:
            # Check if schema exists first
            try:
                spark.sql(f"DESCRIBE SCHEMA `{catalog_name}`.`{schema_name}`")
                schema_exists = True
            except:
                schema_exists = False

            if schema_exists:
                try:
                    tables = spark.sql(
                        f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`"
                    ).collect()
                    for table in tables:
                        table_name = table["tableName"]
                        spark.sql(
                            f"GRANT SELECT ON TABLE `{catalog_name}`.`{schema_name}`.`{table_name}` TO `{app_service_principal}`"
                        )
                        print(
                            f"✅ Granted SELECT on table {catalog_name}.{schema_name}.{table_name}"
                        )
                except Exception as e:
                    print(
                        f"⚠️  Could not grant SELECT on tables in schema {schema_name}: {e}"
                    )
            else:
                print(
                    f"⚠️  Schema {catalog_name}.{schema_name} does not exist, skipping tables"
                )
        except Exception as e:
            print(
                f"❌ Error processing tables in schema {catalog_name}.{schema_name}: {e}"
            )
else:
    print(
        "📋 No target schemas specified, granting SELECT on tables in all existing schemas..."
    )
    # Grant SELECT on all tables (for metadata generation) - fallback behavior
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()
        for schema in schemas:
            schema_name = schema["namespace"]
            try:
                tables = spark.sql(
                    f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`"
                ).collect()
                for table in tables:
                    table_name = table["tableName"]
                    spark.sql(
                        f"GRANT SELECT ON TABLE `{catalog_name}`.`{schema_name}`.`{table_name}` TO `{app_service_principal}`"
                    )
                    print(
                        f"✅ Granted SELECT on table {catalog_name}.{schema_name}.{table_name}"
                    )
            except Exception as e:
                print(
                    f"⚠️  Could not grant SELECT on tables in schema {schema_name}: {e}"
                )
    except Exception as e:
        print(f"⚠️  Could not process table permissions: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Metadata Results Schema Permissions

# COMMAND ----------

# Grant ALL PRIVILEGES on metadata_results schema (for storing results)
metadata_schema = "metadata_results"
try:
    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{metadata_schema}`")
    print(f"✅ Created/verified schema {catalog_name}.{metadata_schema}")

    # Grant all privileges
    spark.sql(
        f"GRANT ALL PRIVILEGES ON SCHEMA `{catalog_name}`.`{metadata_schema}` TO `{app_service_principal}`"
    )
    print(f"✅ Granted ALL PRIVILEGES on schema {catalog_name}.{metadata_schema}")
except Exception as e:
    print(f"⚠️  Could not set up metadata_results schema: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Volume Permissions

# COMMAND ----------

# Grant CREATE VOLUME permission for file storage
try:
    spark.sql(
        f"GRANT CREATE VOLUME ON SCHEMA `{catalog_name}`.`{metadata_schema}` TO `{app_service_principal}`"
    )
    print(f"✅ Granted CREATE VOLUME on {catalog_name}.{metadata_schema}")
except Exception as e:
    print(f"⚠️  Could not grant CREATE VOLUME permission: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Function Permissions

# COMMAND ----------

# Grant EXECUTE permission on catalog (for functions)
try:
    spark.sql(f"GRANT EXECUTE ON CATALOG `{catalog_name}` TO `{app_service_principal}`")
    print(f"✅ Granted EXECUTE on catalog {catalog_name}")
except Exception as e:
    print(f"⚠️  Could not grant EXECUTE permission: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("🎉 Permissions setup completed!")
print(f"The service principal '{app_service_principal}' now has:")
print(f"  ✅ USE CATALOG on {catalog_name}")
print(f"  ✅ CREATE SCHEMA on {catalog_name}")
print(f"  ✅ SELECT on existing schemas and tables")
print(f"  ✅ ALL PRIVILEGES on {catalog_name}.metadata_results")
print(f"  ✅ CREATE VOLUME permissions")
print(f"  ✅ EXECUTE permissions on catalog")
print()
print("The DBXMetaGen app should now be able to:")
print("  - Read table metadata and data for analysis")
print("  - Create and manage metadata results")
print("  - Store generated files in volumes")
print("  - Execute necessary functions")

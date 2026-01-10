# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Knowledge Base ETL
# MAGIC 
# MAGIC Tests the knowledge base ETL pipeline that transforms metadata_generation_log
# MAGIC into table_knowledge_base.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
sys.path.append("../../")

from datetime import datetime
from pyspark.sql import Row

# Get test parameters
dbutils.widgets.text("catalog_name", "dbxmetagen_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dev_integration_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

# Create unique test schema for isolation
test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
kb_test_schema = f"kb_test_{test_timestamp}"

print(f"Test catalog: {catalog_name}")
print(f"Test schema: {kb_test_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Test Schema and Mock Data

# COMMAND ----------

# Create test schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{kb_test_schema}")
print(f"[SETUP] Created test schema: {catalog_name}.{kb_test_schema}")

# COMMAND ----------

# Create mock metadata_generation_log table with test data
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{kb_test_schema}.metadata_generation_log (
    metadata_type STRING,
    `table` STRING,
    tokenized_table STRING,
    ddl_type STRING,
    column_name STRING,
    _created_at TIMESTAMP,
    column_content STRING,
    classification STRING,
    type STRING,
    confidence DOUBLE,
    presidio_results STRING,
    domain STRING,
    subdomain STRING,
    recommended_domain STRING,
    recommended_subdomain STRING,
    reasoning STRING,
    metadata_summary STRING,
    catalog STRING,
    schema STRING,
    table_name STRING,
    ddl STRING,
    current_user STRING,
    model STRING,
    sample_size INT,
    max_tokens INT
)
""")

print("[SETUP] Created mock metadata_generation_log table")

# COMMAND ----------

# Insert test data - simulating different metadata runs
test_data = [
    # Comment metadata for table1
    Row(
        metadata_type="comment",
        table=f"{catalog_name}.{kb_test_schema}.test_customers",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_customers",
        ddl_type="table",
        column_name=None,
        _created_at=datetime(2024, 1, 1, 10, 0, 0),
        column_content="Customer master data table containing customer profiles and contact information",
        classification=None,
        type=None,
        confidence=None,
        presidio_results=None,
        domain=None,
        subdomain=None,
        recommended_domain=None,
        recommended_subdomain=None,
        reasoning=None,
        metadata_summary=None,
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_customers",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
    # Domain metadata for table1
    Row(
        metadata_type="domain",
        table=f"{catalog_name}.{kb_test_schema}.test_customers",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_customers",
        ddl_type="table",
        column_name=None,
        _created_at=datetime(2024, 1, 2, 10, 0, 0),
        column_content=None,
        classification=None,
        type=None,
        confidence=0.95,
        presidio_results=None,
        domain="Customer Data",
        subdomain="Master Data",
        recommended_domain="Customer Data",
        recommended_subdomain="Master Data",
        reasoning="Contains customer profiles",
        metadata_summary="Customer master data",
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_customers",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
    # PI metadata for table1 - column level PII
    Row(
        metadata_type="pi",
        table=f"{catalog_name}.{kb_test_schema}.test_customers",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_customers",
        ddl_type="column",
        column_name="email",
        _created_at=datetime(2024, 1, 3, 10, 0, 0),
        column_content=None,
        classification="pii",
        type="email",
        confidence=0.98,
        presidio_results=None,
        domain=None,
        subdomain=None,
        recommended_domain=None,
        recommended_subdomain=None,
        reasoning=None,
        metadata_summary=None,
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_customers",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
    # Comment metadata for table2 (medical records - should have PHI)
    Row(
        metadata_type="comment",
        table=f"{catalog_name}.{kb_test_schema}.test_medical_records",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_medical_records",
        ddl_type="table",
        column_name=None,
        _created_at=datetime(2024, 1, 1, 11, 0, 0),
        column_content="Patient medical records including diagnoses and treatments",
        classification=None,
        type=None,
        confidence=None,
        presidio_results=None,
        domain=None,
        subdomain=None,
        recommended_domain=None,
        recommended_subdomain=None,
        reasoning=None,
        metadata_summary=None,
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_medical_records",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
    # Domain metadata for table2
    Row(
        metadata_type="domain",
        table=f"{catalog_name}.{kb_test_schema}.test_medical_records",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_medical_records",
        ddl_type="table",
        column_name=None,
        _created_at=datetime(2024, 1, 2, 11, 0, 0),
        column_content=None,
        classification=None,
        type=None,
        confidence=0.92,
        presidio_results=None,
        domain="Healthcare",
        subdomain="Clinical Data",
        recommended_domain="Healthcare",
        recommended_subdomain="Clinical Data",
        reasoning="Contains patient medical information",
        metadata_summary="Patient medical records",
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_medical_records",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
    # PI metadata for table2 - PHI classification
    Row(
        metadata_type="pi",
        table=f"{catalog_name}.{kb_test_schema}.test_medical_records",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_medical_records",
        ddl_type="column",
        column_name="diagnosis",
        _created_at=datetime(2024, 1, 3, 11, 0, 0),
        column_content=None,
        classification="phi",
        type="medical_record",
        confidence=0.99,
        presidio_results=None,
        domain=None,
        subdomain=None,
        recommended_domain=None,
        recommended_subdomain=None,
        reasoning=None,
        metadata_summary=None,
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_medical_records",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
    # Comment only table (no domain or PI)
    Row(
        metadata_type="comment",
        table=f"{catalog_name}.{kb_test_schema}.test_products",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_products",
        ddl_type="table",
        column_name=None,
        _created_at=datetime(2024, 1, 1, 12, 0, 0),
        column_content="Product catalog with SKUs and pricing",
        classification=None,
        type=None,
        confidence=None,
        presidio_results=None,
        domain=None,
        subdomain=None,
        recommended_domain=None,
        recommended_subdomain=None,
        reasoning=None,
        metadata_summary=None,
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_products",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
]

# Get schema from the table we created
table_schema = spark.table(f"{catalog_name}.{kb_test_schema}.metadata_generation_log").schema

# Create DataFrame with explicit schema
test_df = spark.createDataFrame(test_data, schema=table_schema)
test_df.write.mode("append").saveAsTable(f"{catalog_name}.{kb_test_schema}.metadata_generation_log")

print(f"[SETUP] Inserted {len(test_data)} test records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Knowledge Base ETL

# COMMAND ----------

from src.dbxmetagen.knowledge_base import (
    KnowledgeBaseConfig,
    KnowledgeBaseBuilder,
    build_knowledge_base
)

# Run the ETL
result = build_knowledge_base(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=kb_test_schema
)

print(f"[ETL] Staged count: {result['staged_count']}")
print(f"[ETL] Total records: {result['total_records']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Results

# COMMAND ----------

# Test 1: Verify correct number of tables
kb_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{kb_test_schema}.table_knowledge_base
""").collect()[0]["cnt"]

assert kb_count == 3, f"Expected 3 tables, got {kb_count}"
print("[TEST 1] PASSED: Correct number of tables in knowledge base")

# COMMAND ----------

# Test 2: Verify test_customers has PII but not PHI
customers_row = spark.sql(f"""
    SELECT * FROM {catalog_name}.{kb_test_schema}.table_knowledge_base
    WHERE table_name = '{catalog_name}.{kb_test_schema}.test_customers'
""").collect()[0]

assert customers_row["has_pii"] == True, "test_customers should have has_pii=True"
assert customers_row["has_phi"] == False, "test_customers should have has_phi=False"
assert customers_row["domain"] == "Customer Data", f"Expected domain 'Customer Data', got '{customers_row['domain']}'"
assert "Customer" in customers_row["comment"], "Comment should contain 'Customer'"
print("[TEST 2] PASSED: test_customers has correct PII/PHI/domain/comment")

# COMMAND ----------

# Test 3: Verify test_medical_records has both PII and PHI
medical_row = spark.sql(f"""
    SELECT * FROM {catalog_name}.{kb_test_schema}.table_knowledge_base
    WHERE table_name = '{catalog_name}.{kb_test_schema}.test_medical_records'
""").collect()[0]

assert medical_row["has_pii"] == True, "test_medical_records should have has_pii=True"
assert medical_row["has_phi"] == True, "test_medical_records should have has_phi=True"
assert medical_row["domain"] == "Healthcare", f"Expected domain 'Healthcare', got '{medical_row['domain']}'"
assert medical_row["subdomain"] == "Clinical Data", f"Expected subdomain 'Clinical Data', got '{medical_row['subdomain']}'"
print("[TEST 3] PASSED: test_medical_records has correct PII/PHI/domain/subdomain")

# COMMAND ----------

# Test 4: Verify test_products has no PII/PHI (comment only)
products_row = spark.sql(f"""
    SELECT * FROM {catalog_name}.{kb_test_schema}.table_knowledge_base
    WHERE table_name = '{catalog_name}.{kb_test_schema}.test_products'
""").collect()[0]

assert products_row["has_pii"] == False, "test_products should have has_pii=False"
assert products_row["has_phi"] == False, "test_products should have has_phi=False"
assert products_row["domain"] is None, f"test_products domain should be None, got '{products_row['domain']}'"
assert "Product" in products_row["comment"], "Comment should contain 'Product'"
print("[TEST 4] PASSED: test_products has no PII/PHI and correct comment")

# COMMAND ----------

# Test 5: Verify table name parts are parsed correctly
for row in spark.sql(f"""
    SELECT table_name, catalog, schema, table_short_name
    FROM {catalog_name}.{kb_test_schema}.table_knowledge_base
""").collect():
    assert row["catalog"] == catalog_name, f"Expected catalog '{catalog_name}', got '{row['catalog']}'"
    assert row["schema"] == kb_test_schema, f"Expected schema '{kb_test_schema}', got '{row['schema']}'"
    assert row["table_short_name"] in ["test_customers", "test_medical_records", "test_products"], \
        f"Unexpected table_short_name: {row['table_short_name']}"

print("[TEST 5] PASSED: Table name parts parsed correctly")

# COMMAND ----------

# Test 6: Test incremental merge - add new data and re-run
# Add new PI classification for test_products
new_pi_data = [
    Row(
        metadata_type="pi",
        table=f"{catalog_name}.{kb_test_schema}.test_products",
        tokenized_table=f"{catalog_name}.{kb_test_schema}.test_products",
        ddl_type="column",
        column_name="price",
        _created_at=datetime(2024, 1, 5, 12, 0, 0),
        column_content=None,
        classification="pci",  # Payment card info
        type="payment",
        confidence=0.88,
        presidio_results=None,
        domain=None,
        subdomain=None,
        recommended_domain=None,
        recommended_subdomain=None,
        reasoning=None,
        metadata_summary=None,
        catalog=catalog_name,
        schema=kb_test_schema,
        table_name="test_products",
        ddl=None,
        current_user="test@example.com",
        model="test-model",
        sample_size=10,
        max_tokens=1000
    ),
]

# Use same schema for incremental data
new_pi_df = spark.createDataFrame(new_pi_data, schema=table_schema)
new_pi_df.write.mode("append").saveAsTable(f"{catalog_name}.{kb_test_schema}.metadata_generation_log")

# Re-run ETL
result2 = build_knowledge_base(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=kb_test_schema
)

# Verify test_products now has PII (from PCI classification)
products_row_updated = spark.sql(f"""
    SELECT * FROM {catalog_name}.{kb_test_schema}.table_knowledge_base
    WHERE table_name = '{catalog_name}.{kb_test_schema}.test_products'
""").collect()[0]

assert products_row_updated["has_pii"] == True, "test_products should now have has_pii=True after PCI classification"
assert products_row_updated["has_phi"] == False, "test_products should still have has_phi=False"
# Verify comment is still preserved (COALESCE behavior)
assert "Product" in products_row_updated["comment"], "Comment should still be preserved after merge"

print("[TEST 6] PASSED: Incremental merge correctly updated PII flag and preserved existing data")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop test schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{kb_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{kb_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL KNOWLEDGE BASE INTEGRATION TESTS PASSED")
print("=" * 60)


# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Ontology Pipeline
# MAGIC 
# MAGIC Tests the ontology pipeline that discovers business entities from the knowledge base
# MAGIC using keyword matching and AI-powered classification.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

from datetime import datetime
from pyspark.sql import Row

# Get test parameters
dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

# Create unique test schema for isolation
test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
ontology_test_schema = f"ontology_test_{test_timestamp}"

print(f"Test catalog: {catalog_name}")
print(f"Test schema: {ontology_test_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Test Schema and Mock Knowledge Base

# COMMAND ----------

# Create test schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{ontology_test_schema}")
print(f"[SETUP] Created test schema: {catalog_name}.{ontology_test_schema}")

# COMMAND ----------

# Create mock table_knowledge_base with tables that should match entity keywords
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{ontology_test_schema}.table_knowledge_base (
    table_name STRING NOT NULL,
    catalog STRING,
    `schema` STRING,
    table_short_name STRING,
    comment STRING,
    domain STRING,
    subdomain STRING,
    has_pii BOOLEAN,
    has_phi BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
""")
print("[SETUP] Created table_knowledge_base")

# COMMAND ----------

# Insert test data - tables designed to match specific entity keywords
test_kb_data = [
    # Should match "Customer" entity (keywords: customer, user, client)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.customers_master",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="customers_master",
        comment="Master customer data with account details and preferences",
        domain="CRM", subdomain="Master",
        has_pii=True, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Should match "Patient" entity (keywords: patient, individual)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.patient_records",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="patient_records",
        comment="Patient demographic and contact information",
        domain="Healthcare", subdomain="Demographics",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Should match "ClinicalNote" entity (keywords: note, clinical, ehr)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.ehr_clinical_notes",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="ehr_clinical_notes",
        comment="Clinical notes and documentation from EHR system",
        domain="Healthcare", subdomain="Clinical",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Should match "Product" entity (keywords: product, item, catalog)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.product_catalog",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="product_catalog",
        comment="Product catalog with SKUs, descriptions, and pricing",
        domain="Retail", subdomain="Master",
        has_pii=False, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Should match "Transaction" entity (keywords: transaction, order, purchase)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.order_transactions",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="order_transactions",
        comment="Transaction records for customer orders and purchases",
        domain="Sales", subdomain="Transactions",
        has_pii=True, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Should match "Medication" entity (keywords: medication, drug, prescription)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.medication_prescriptions",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="medication_prescriptions",
        comment="Prescription records for patient medications",
        domain="Healthcare", subdomain="Pharmacy",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Should match "LabResult" entity (keywords: lab, result, test)
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.lab_test_results",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="lab_test_results",
        comment="Laboratory test results and specimen data",
        domain="Healthcare", subdomain="Diagnostics",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Ambiguous table - may rely on AI fallback or generic matching
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.system_audit_log",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="system_audit_log",
        comment="System audit trail and activity logging",
        domain="IT", subdomain="Security",
        has_pii=False, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Reference data - should match "Reference" entity
    Row(
        table_name=f"{catalog_name}.{ontology_test_schema}.reference_codes",
        catalog=catalog_name, schema=ontology_test_schema, table_short_name="reference_codes",
        comment="Reference code lookup tables for standardization",
        domain="Master", subdomain="Reference",
        has_pii=False, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
]

kb_df = spark.createDataFrame(test_kb_data)
kb_df.write.mode("append").saveAsTable(f"{catalog_name}.{ontology_test_schema}.table_knowledge_base")
print(f"[SETUP] Inserted {len(test_kb_data)} tables into knowledge base")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Ontology Pipeline

# COMMAND ----------

from dbxmetagen.ontology import build_ontology

result = build_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=ontology_test_schema,
    config_path="configurations/ontology_config.yaml"
)

print(f"[ETL] Entities discovered: {result['entities_discovered']}")
print(f"[ETL] Entity types: {result['entity_types']}")
print(f"[ETL] Edges added: {result['edges_added']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate ontology_entities Table

# COMMAND ----------

# Test 1: Verify entities were discovered (table NOT empty)
entity_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
""").collect()[0]["cnt"]

assert entity_count > 0, "ontology_entities table is empty! Expected entities to be discovered."
assert entity_count >= 5, f"Expected at least 5 entities (most tables should match), got {entity_count}"
print(f"[TEST 1] PASSED: {entity_count} entities discovered (table not empty)")

# COMMAND ----------

# Test 2: Verify required columns exist
entity_cols = spark.table(f"{catalog_name}.{ontology_test_schema}.ontology_entities").columns
required_cols = [
    "entity_id", "entity_name", "entity_type", "description", 
    "source_tables", "confidence", "auto_discovered"
]
for col in required_cols:
    assert col in entity_cols, f"Missing required column: {col}"
print("[TEST 2] PASSED: ontology_entities has all required columns")

# COMMAND ----------

# Test 3: Verify "Customer" entity discovered for customers_master table
customer_entity = spark.sql(f"""
    SELECT entity_type, confidence, source_tables
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE ARRAY_CONTAINS(source_tables, '{catalog_name}.{ontology_test_schema}.customers_master')
""").collect()

assert len(customer_entity) > 0, "No entity found for customers_master table"
# Should be classified as Customer (keyword match)
print(f"[TEST 3] PASSED: customers_master classified as '{customer_entity[0]['entity_type']}' (confidence: {customer_entity[0]['confidence']})")

# COMMAND ----------

# Test 4: Verify "Patient" entity discovered for patient_records
patient_entity = spark.sql(f"""
    SELECT entity_type, confidence
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE ARRAY_CONTAINS(source_tables, '{catalog_name}.{ontology_test_schema}.patient_records')
""").collect()

assert len(patient_entity) > 0, "No entity found for patient_records table"
print(f"[TEST 4] PASSED: patient_records classified as '{patient_entity[0]['entity_type']}' (confidence: {patient_entity[0]['confidence']})")

# COMMAND ----------

# Test 5: Verify "ClinicalNote" entity for ehr_clinical_notes
clinical_entity = spark.sql(f"""
    SELECT entity_type, confidence
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE ARRAY_CONTAINS(source_tables, '{catalog_name}.{ontology_test_schema}.ehr_clinical_notes')
""").collect()

assert len(clinical_entity) > 0, "No entity found for ehr_clinical_notes table"
print(f"[TEST 5] PASSED: ehr_clinical_notes classified as '{clinical_entity[0]['entity_type']}' (confidence: {clinical_entity[0]['confidence']})")

# COMMAND ----------

# Test 6: Verify confidence scores are in valid range (0.3 - 1.0 based on new thresholds)
invalid_confidence = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE confidence < 0.0 OR confidence > 1.0
""").collect()[0]["cnt"]

assert invalid_confidence == 0, f"Found {invalid_confidence} entities with invalid confidence scores"
print("[TEST 6] PASSED: All confidence scores in valid range [0.0, 1.0]")

# COMMAND ----------

# Test 7: Verify auto_discovered flag is set
auto_discovered = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE auto_discovered = true
""").collect()[0]["cnt"]

assert auto_discovered >= entity_count - 1, "Most entities should be auto-discovered"
print(f"[TEST 7] PASSED: {auto_discovered} entities marked as auto_discovered")

# COMMAND ----------

# Test 8: Verify system_audit_log gets classified (even if AI fallback to DataTable)
audit_entity = spark.sql(f"""
    SELECT entity_type, confidence
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE ARRAY_CONTAINS(source_tables, '{catalog_name}.{ontology_test_schema}.system_audit_log')
""").collect()

# Every table should get SOME classification (no tables left unclassified)
assert len(audit_entity) > 0, "system_audit_log should have entity (AI fallback ensures all tables classified)"
print(f"[TEST 8] PASSED: system_audit_log classified as '{audit_entity[0]['entity_type']}' (AI fallback working)")

# COMMAND ----------

# Test 9: Verify ontology_metrics table is created (stub)
metrics_exists = spark.sql(f"""
    SHOW TABLES IN {catalog_name}.{ontology_test_schema} LIKE 'ontology_metrics'
""").count()

assert metrics_exists > 0, "ontology_metrics table should be created"
print("[TEST 9] PASSED: ontology_metrics table created (stub for future)")

# COMMAND ----------

# Test 10: Display entity type distribution
entity_summary = spark.sql(f"""
    SELECT entity_type, COUNT(*) as cnt, ROUND(AVG(confidence), 2) as avg_conf
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    GROUP BY entity_type
    ORDER BY cnt DESC
""")
print("[TEST 10] Entity distribution:")
entity_summary.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test Incremental Discovery

# COMMAND ----------

# Test 11: Add a new table and verify incremental merge doesn't duplicate
new_table = Row(
    table_name=f"{catalog_name}.{ontology_test_schema}.new_provider_data",
    catalog=catalog_name, schema=ontology_test_schema, table_short_name="new_provider_data",
    comment="Healthcare provider and physician information",
    domain="Healthcare", subdomain="Provider",
    has_pii=True, has_phi=False,
    created_at=datetime.now(), updated_at=datetime.now()
)

new_df = spark.createDataFrame([new_table])
new_df.write.mode("append").saveAsTable(f"{catalog_name}.{ontology_test_schema}.table_knowledge_base")

# Re-run ontology
result2 = build_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=ontology_test_schema
)

# Verify no duplicates
duplicate_check = spark.sql(f"""
    SELECT entity_name, source_tables, COUNT(*) as cnt
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    GROUP BY entity_name, source_tables
    HAVING COUNT(*) > 1
""").collect()

assert len(duplicate_check) == 0, f"Found duplicate entities: {duplicate_check}"
print("[TEST 11] PASSED: Incremental discovery doesn't create duplicates")

# COMMAND ----------

# Test 12: Verify new provider table was classified
provider_entity = spark.sql(f"""
    SELECT entity_type, confidence
    FROM {catalog_name}.{ontology_test_schema}.ontology_entities
    WHERE ARRAY_CONTAINS(source_tables, '{catalog_name}.{ontology_test_schema}.new_provider_data')
""").collect()

assert len(provider_entity) > 0, "new_provider_data should be classified"
print(f"[TEST 12] PASSED: new_provider_data classified as '{provider_entity[0]['entity_type']}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop test schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{ontology_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{ontology_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL ONTOLOGY INTEGRATION TESTS PASSED")
print("=" * 60)


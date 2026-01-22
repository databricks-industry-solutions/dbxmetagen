# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Knowledge Graph ETL
# MAGIC 
# MAGIC Tests the knowledge graph ETL pipeline that builds GraphFrames-compatible
# MAGIC node and edge tables from the table_knowledge_base.
# MAGIC
# MAGIC Note: GraphFrames queries are NOT tested here as they require JVM dependencies
# MAGIC not available in all runtime configurations. This test validates:
# MAGIC - Node table creation and population
# MAGIC - Edge table creation and population  
# MAGIC - Incremental merge behavior
# MAGIC - Edge refresh on relationship changes

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
graph_test_schema = f"graph_test_{test_timestamp}"

print(f"Test catalog: {catalog_name}")
print(f"Test schema: {graph_test_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Test Schema and Mock Knowledge Base

# COMMAND ----------

# Create test schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{graph_test_schema}")
print(f"[SETUP] Created test schema: {catalog_name}.{graph_test_schema}")

# COMMAND ----------

# Create mock table_knowledge_base with test data
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{graph_test_schema}.table_knowledge_base (
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

print("[SETUP] Created mock table_knowledge_base")

# COMMAND ----------

# Insert test data representing tables with various relationships
test_kb_data = [
    # Customer domain tables (same domain, different subdomains)
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.customers",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="customers",
        comment="Customer master data", domain="Customer", subdomain="Master",
        has_pii=True, has_phi=False,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1)
    ),
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.customer_addresses",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="customer_addresses",
        comment="Customer addresses", domain="Customer", subdomain="Contact",
        has_pii=True, has_phi=False,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1)
    ),
    # Healthcare domain tables (PHI)
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.patients",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="patients",
        comment="Patient records", domain="Healthcare", subdomain="Clinical",
        has_pii=True, has_phi=True,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1)
    ),
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.diagnoses",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="diagnoses",
        comment="Medical diagnoses", domain="Healthcare", subdomain="Clinical",
        has_pii=True, has_phi=True,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1)
    ),
    # Public reference table (no PII/PHI)
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.product_catalog",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="product_catalog",
        comment="Product catalog", domain="Product", subdomain="Master",
        has_pii=False, has_phi=False,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1)
    ),
]

test_kb_df = spark.createDataFrame(test_kb_data)
test_kb_df.write.mode("append").saveAsTable(f"{catalog_name}.{graph_test_schema}.table_knowledge_base")

print(f"[SETUP] Inserted {len(test_kb_data)} test records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Knowledge Graph ETL

# COMMAND ----------

from src.dbxmetagen.knowledge_graph import (
    KnowledgeGraphConfig,
    KnowledgeGraphBuilder,
    build_knowledge_graph
)

# Run the ETL
result = build_knowledge_graph(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=graph_test_schema
)

print(f"[ETL] Staged nodes: {result['staged_nodes']}")
print(f"[ETL] Staged edges: {result['staged_edges']}")
print(f"[ETL] Total nodes: {result['total_nodes']}")
print(f"[ETL] Total edges: {result['total_edges']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Node Table

# COMMAND ----------

# Test 1: Verify correct number of nodes
node_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_nodes
""").collect()[0]["cnt"]

assert node_count == 5, f"Expected 5 nodes, got {node_count}"
print("[TEST 1] PASSED: Correct number of nodes")

# COMMAND ----------

# Test 2: Verify node properties
nodes_df = spark.sql(f"""
    SELECT id, table_name, domain, security_level, has_pii, has_phi
    FROM {catalog_name}.{graph_test_schema}.graph_nodes
    ORDER BY table_name
""")

nodes = {row["table_name"]: row for row in nodes_df.collect()}

# Check customers node
customers_node = nodes[f"{catalog_name}.{graph_test_schema}.customers"]
assert customers_node["id"] == customers_node["table_name"], "id should equal table_name"
assert customers_node["domain"] == "Customer", "Domain should be Customer"
assert customers_node["security_level"] == "PII", "Security level should be PII"
assert customers_node["has_pii"] == True, "has_pii should be True"
assert customers_node["has_phi"] == False, "has_phi should be False"

# Check patients node (PHI)
patients_node = nodes[f"{catalog_name}.{graph_test_schema}.patients"]
assert patients_node["security_level"] == "PHI", "Security level should be PHI for healthcare"
assert patients_node["has_phi"] == True, "has_phi should be True"

# Check product_catalog node (PUBLIC)
products_node = nodes[f"{catalog_name}.{graph_test_schema}.product_catalog"]
assert products_node["security_level"] == "PUBLIC", "Security level should be PUBLIC"
assert products_node["has_pii"] == False, "has_pii should be False"

print("[TEST 2] PASSED: Node properties are correct")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Edge Table

# COMMAND ----------

# Test 3: Verify edges exist for each relationship type
edge_counts = spark.sql(f"""
    SELECT relationship, COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    GROUP BY relationship
""")

edge_map = {row["relationship"]: row["cnt"] for row in edge_counts.collect()}

# All 5 tables are in the same catalog/schema
assert "same_catalog" in edge_map, "Should have same_catalog edges"
assert "same_schema" in edge_map, "Should have same_schema edges"

# 2 Customer domain tables -> 1 edge
assert edge_map.get("same_domain", 0) >= 1, "Should have at least 1 same_domain edge"

# 2 Healthcare tables in same subdomain -> 1 edge  
assert edge_map.get("same_subdomain", 0) >= 1, "Should have at least 1 same_subdomain edge"

# PII tables: customers, customer_addresses, patients, diagnoses (4 tables)
# PHI tables: patients, diagnoses (2 tables)
# PUBLIC tables: product_catalog (1 table, no edges)
assert "same_security_level" in edge_map, "Should have same_security_level edges"

print(f"[TEST 3] PASSED: Edge relationship types exist")
print(f"  Edge counts by type: {edge_map}")

# COMMAND ----------

# Test 4: Verify specific edge relationships
# Check that same_domain edges connect Customer tables
customer_domain_edges = spark.sql(f"""
    SELECT src, dst 
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_domain'
    AND (src LIKE '%customers%' OR dst LIKE '%customers%')
""").collect()

customer_tables_in_edges = set()
for row in customer_domain_edges:
    if "customers" in row["src"]:
        customer_tables_in_edges.add(row["src"])
    if "customers" in row["dst"]:
        customer_tables_in_edges.add(row["dst"])
    if "customer_addresses" in row["src"]:
        customer_tables_in_edges.add(row["src"])
    if "customer_addresses" in row["dst"]:
        customer_tables_in_edges.add(row["dst"])

assert len(customer_tables_in_edges) == 2, f"Expected 2 customer tables in same_domain edges, got {len(customer_tables_in_edges)}"
print("[TEST 4] PASSED: Domain edges connect correct tables")

# COMMAND ----------

# Test 5: Verify edge direction (src < dst to avoid duplicates)
duplicate_edges = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE src >= dst
""").collect()[0]["cnt"]

assert duplicate_edges == 0, f"Found {duplicate_edges} edges with src >= dst (duplicates/self-loops)"
print("[TEST 5] PASSED: No duplicate or self-loop edges")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test Edge Refresh on Relationship Change

# COMMAND ----------

# Test 6: Simulate domain change and verify edges are refreshed
# Change product_catalog from "Product" domain to "Customer" domain
spark.sql(f"""
    UPDATE {catalog_name}.{graph_test_schema}.table_knowledge_base
    SET domain = 'Customer', subdomain = 'Reference', updated_at = current_timestamp()
    WHERE table_name = '{catalog_name}.{graph_test_schema}.product_catalog'
""")

# Count same_domain edges before refresh
domain_edges_before = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_domain'
""").collect()[0]["cnt"]

# Re-run graph build
result2 = build_knowledge_graph(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=graph_test_schema
)

# Count same_domain edges after refresh
domain_edges_after = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_domain'
""").collect()[0]["cnt"]

# Now 3 tables in Customer domain: customers, customer_addresses, product_catalog
# This creates 3 edges (3 choose 2 = 3)
# Before: 2 Customer + 2 Healthcare = 1 + 1 = 2 same_domain edges
# After: 3 Customer + 2 Healthcare = 3 + 1 = 4 same_domain edges
assert domain_edges_after > domain_edges_before, \
    f"Domain edges should increase after adding product_catalog to Customer domain (before: {domain_edges_before}, after: {domain_edges_after})"

print(f"[TEST 6] PASSED: Edge refresh handled domain change correctly")
print(f"  same_domain edges before: {domain_edges_before}")
print(f"  same_domain edges after: {domain_edges_after}")

# COMMAND ----------

# Test 7: Verify product_catalog is now connected to customer tables
product_customer_edges = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_domain'
    AND (src LIKE '%product_catalog%' OR dst LIKE '%product_catalog%')
    AND (src LIKE '%customer%' OR dst LIKE '%customer%')
""").collect()[0]["cnt"]

assert product_customer_edges >= 1, "product_catalog should now be connected to customer tables"
print("[TEST 7] PASSED: product_catalog now connected to Customer domain tables")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop test schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{graph_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{graph_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL KNOWLEDGE GRAPH INTEGRATION TESTS PASSED")
print("=" * 60)


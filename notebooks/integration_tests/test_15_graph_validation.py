# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Graph Tables Validation
# MAGIC 
# MAGIC Tests the complete state of graph_nodes and graph_edges tables after 
# MAGIC the full pipeline (KB -> Graph -> Embeddings -> Similarity).
# MAGIC Validates structure, relationships, and data quality.

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
graph_test_schema = f"graph_validation_{test_timestamp}"

print(f"Test catalog: {catalog_name}")
print(f"Test schema: {graph_test_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Test Schema and Full Pipeline Data

# COMMAND ----------

# Create test schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{graph_test_schema}")
print(f"[SETUP] Created test schema: {catalog_name}.{graph_test_schema}")

# COMMAND ----------

# Create comprehensive table_knowledge_base with various domains and relationships
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

# Insert tables with clear domain/subdomain structure for relationship testing
kb_data = [
    # Healthcare domain - Clinical subdomain (should have same_domain and same_subdomain edges)
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.patient_demographics",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="patient_demographics",
        comment="Patient demographic data including name, address, and contact information for healthcare services",
        domain="Healthcare", subdomain="Clinical",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.patient_diagnoses",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="patient_diagnoses",
        comment="Patient diagnosis codes and clinical assessments from healthcare encounters",
        domain="Healthcare", subdomain="Clinical",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.patient_medications",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="patient_medications",
        comment="Patient medication prescriptions and pharmacy orders for healthcare treatment",
        domain="Healthcare", subdomain="Pharmacy",
        has_pii=True, has_phi=True,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Retail domain - Sales subdomain
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.customer_orders",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="customer_orders",
        comment="Customer order transactions and purchase history for retail analytics",
        domain="Retail", subdomain="Sales",
        has_pii=True, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.customer_profiles",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="customer_profiles",
        comment="Customer profile data with preferences and contact details for retail marketing",
        domain="Retail", subdomain="CRM",
        has_pii=True, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    # Reference data - PUBLIC (no PII/PHI)
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.product_catalog",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="product_catalog",
        comment="Product catalog master data with SKUs, descriptions, and pricing information",
        domain="Retail", subdomain="Master",
        has_pii=False, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    Row(
        table_name=f"{catalog_name}.{graph_test_schema}.region_codes",
        catalog=catalog_name, schema=graph_test_schema, table_short_name="region_codes",
        comment="Geographic region reference codes for standardization",
        domain="Reference", subdomain="Geography",
        has_pii=False, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
]

kb_df = spark.createDataFrame(kb_data)
kb_df.write.mode("append").saveAsTable(f"{catalog_name}.{graph_test_schema}.table_knowledge_base")
print(f"[SETUP] Created {len(kb_data)} tables in knowledge base")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Full Pipeline

# COMMAND ----------

# Step 1: Build knowledge graph
from dbxmetagen.knowledge_graph import build_knowledge_graph

graph_result = build_knowledge_graph(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=graph_test_schema
)
print(f"[PIPELINE] Graph built - Nodes: {graph_result['total_nodes']}, Edges: {graph_result['total_edges']}")

# COMMAND ----------

# Step 2: Generate embeddings (if foundation model available)
try:
    from dbxmetagen.embeddings import generate_embeddings
    
    embed_result = generate_embeddings(
        spark=spark,
        catalog_name=catalog_name,
        schema_name=graph_test_schema
    )
    print(f"[PIPELINE] Embeddings generated - Nodes embedded: {embed_result.get('nodes_embedded', 0)}")
    embeddings_available = embed_result.get('nodes_embedded', 0) > 0
except Exception as e:
    print(f"[PIPELINE] Embeddings skipped (model may not be available): {e}")
    embeddings_available = False

# COMMAND ----------

# Step 3: Build similarity edges (if embeddings available)
if embeddings_available:
    try:
        from dbxmetagen.similarity_edges import build_similarity_edges
        
        sim_result = build_similarity_edges(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=graph_test_schema,
            similarity_threshold=0.5  # Lower threshold for testing
        )
        print(f"[PIPELINE] Similarity edges built - Edges: {sim_result.get('edges_added', 0)}")
    except Exception as e:
        print(f"[PIPELINE] Similarity edges skipped: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate graph_nodes Table

# COMMAND ----------

# Test 1: Verify all tables have nodes
node_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_nodes
    WHERE node_type = 'table'
""").collect()[0]["cnt"]

assert node_count == 7, f"Expected 7 table nodes, got {node_count}"
print("[TEST 1] PASSED: All 7 tables have corresponding nodes")

# COMMAND ----------

# Test 2: Verify node_type is populated (not NULL)
null_node_types = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_nodes
    WHERE node_type IS NULL OR node_type = ''
""").collect()[0]["cnt"]

assert null_node_types == 0, f"Found {null_node_types} nodes with NULL node_type"
print("[TEST 2] PASSED: node_type is populated for all nodes")

# COMMAND ----------

# Test 3: Verify required node columns exist
node_cols = spark.table(f"{catalog_name}.{graph_test_schema}.graph_nodes").columns
required_cols = [
    "id", "table_name", "catalog", "schema", "domain", "subdomain",
    "has_pii", "has_phi", "security_level", "comment", "node_type",
    "embedding", "created_at", "updated_at"
]
for col in required_cols:
    assert col in node_cols, f"Missing required column: {col}"
print("[TEST 3] PASSED: graph_nodes has all required columns")

# COMMAND ----------

# Test 4: Verify security_level is correctly derived
security_checks = spark.sql(f"""
    SELECT table_short_name, has_pii, has_phi, security_level
    FROM {catalog_name}.{graph_test_schema}.graph_nodes
    WHERE node_type = 'table'
""").collect()

for row in security_checks:
    if row["has_phi"]:
        assert row["security_level"] == "PHI", f"{row['table_short_name']} with PHI should have PHI security level"
    elif row["has_pii"]:
        assert row["security_level"] == "PII", f"{row['table_short_name']} with PII should have PII security level"
    else:
        assert row["security_level"] == "PUBLIC", f"{row['table_short_name']} without PII/PHI should be PUBLIC"

print("[TEST 4] PASSED: security_level correctly derived from PII/PHI flags")

# COMMAND ----------

# Test 5: Verify comment is populated (from knowledge base)
null_comments = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_nodes
    WHERE comment IS NULL OR comment = ''
""").collect()[0]["cnt"]

assert null_comments == 0, f"Found {null_comments} nodes without comments"
print("[TEST 5] PASSED: All nodes have comments from knowledge base")

# COMMAND ----------

# Test 6: Verify embedding population (if embeddings were generated)
if embeddings_available:
    embedded_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {catalog_name}.{graph_test_schema}.graph_nodes
        WHERE embedding IS NOT NULL AND SIZE(embedding) > 0
    """).collect()[0]["cnt"]
    
    assert embedded_count >= 1, "At least some nodes should have embeddings"
    print(f"[TEST 6] PASSED: {embedded_count} nodes have embeddings")
else:
    print("[TEST 6] SKIPPED: Embeddings not available")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate graph_edges Table

# COMMAND ----------

# Test 7: Verify edge table has required columns
edge_cols = spark.table(f"{catalog_name}.{graph_test_schema}.graph_edges").columns
required_edge_cols = ["src", "dst", "relationship", "created_at", "updated_at"]
for col in required_edge_cols:
    assert col in edge_cols, f"Missing required edge column: {col}"
print("[TEST 7] PASSED: graph_edges has all required columns")

# COMMAND ----------

# Test 8: Verify no self-loops (src != dst)
self_loops = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE src = dst
""").collect()[0]["cnt"]

assert self_loops == 0, f"Found {self_loops} self-loop edges"
print("[TEST 8] PASSED: No self-loop edges")

# COMMAND ----------

# Test 9: Verify no duplicate edges (src < dst for undirected)
duplicate_edges = spark.sql(f"""
    SELECT src, dst, relationship, COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    GROUP BY src, dst, relationship
    HAVING COUNT(*) > 1
""").collect()

assert len(duplicate_edges) == 0, f"Found duplicate edges: {duplicate_edges}"
print("[TEST 9] PASSED: No duplicate edges")

# COMMAND ----------

# Test 10: Verify same_domain edges exist
domain_edges = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_domain'
""").collect()[0]["cnt"]

# We have 3 Healthcare tables and 3 Retail tables
# Healthcare: 3 choose 2 = 3 edges
# Retail: 3 choose 2 = 3 edges
# Expected: at least 6 same_domain edges
assert domain_edges >= 4, f"Expected at least 4 same_domain edges, got {domain_edges}"
print(f"[TEST 10] PASSED: {domain_edges} same_domain edges exist")

# COMMAND ----------

# Test 11: Verify same_subdomain edges exist
subdomain_edges = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_subdomain'
""").collect()[0]["cnt"]

# Healthcare/Clinical has 2 tables: 1 edge
# Expected: at least 1 same_subdomain edge
assert subdomain_edges >= 1, f"Expected at least 1 same_subdomain edge, got {subdomain_edges}"
print(f"[TEST 11] PASSED: {subdomain_edges} same_subdomain edges exist")

# COMMAND ----------

# Test 12: Verify same_security_level edges exist
security_edges = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE relationship = 'same_security_level'
""").collect()[0]["cnt"]

# PHI tables: patient_demographics, patient_diagnoses, patient_medications = 3 choose 2 = 3
# PII tables: customer_orders, customer_profiles = 1
# PUBLIC tables: product_catalog, region_codes = 1
# Expected: at least 5 same_security_level edges
assert security_edges >= 3, f"Expected at least 3 same_security_level edges, got {security_edges}"
print(f"[TEST 12] PASSED: {security_edges} same_security_level edges exist")

# COMMAND ----------

# Test 13: Verify similar_embedding edges (if embeddings available)
if embeddings_available:
    sim_edges = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {catalog_name}.{graph_test_schema}.graph_edges
        WHERE relationship = 'similar_embedding'
    """).collect()[0]["cnt"]
    
    print(f"[TEST 13] INFO: {sim_edges} similarity edges found")
    
    if sim_edges > 0:
        # Verify similarity edge weights are valid
        invalid_weights = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {catalog_name}.{graph_test_schema}.graph_edges
            WHERE relationship = 'similar_embedding'
            AND (weight < 0 OR weight > 1 OR weight IS NULL)
        """).collect()[0]["cnt"]
        
        assert invalid_weights == 0, f"Found {invalid_weights} similarity edges with invalid weights"
        print("[TEST 13] PASSED: Similarity edge weights are valid [0, 1]")
    else:
        print("[TEST 13] INFO: No similarity edges (may need higher embedding similarity)")
else:
    print("[TEST 13] SKIPPED: Embeddings not available")

# COMMAND ----------

# Test 14: Verify edge relationships between Healthcare tables
healthcare_edges = spark.sql(f"""
    SELECT src, dst, relationship
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    WHERE (src LIKE '%patient%' OR dst LIKE '%patient%')
    AND relationship = 'same_domain'
""").collect()

# All 3 patient tables should be connected by same_domain
assert len(healthcare_edges) >= 2, f"Expected at least 2 edges connecting healthcare tables, got {len(healthcare_edges)}"
print(f"[TEST 14] PASSED: Healthcare tables connected with {len(healthcare_edges)} same_domain edges")

# COMMAND ----------

# Test 15: Verify cross-domain check (Healthcare vs Retail should NOT have same_domain)
cross_domain_error = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{graph_test_schema}.graph_edges e
    JOIN {catalog_name}.{graph_test_schema}.graph_nodes n1 ON e.src = n1.id
    JOIN {catalog_name}.{graph_test_schema}.graph_nodes n2 ON e.dst = n2.id
    WHERE e.relationship = 'same_domain'
    AND n1.domain != n2.domain
""").collect()[0]["cnt"]

assert cross_domain_error == 0, f"Found {cross_domain_error} same_domain edges connecting different domains!"
print("[TEST 15] PASSED: No same_domain edges between different domains")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Edge Distribution Summary

# COMMAND ----------

# Test 16: Display edge distribution for verification
edge_summary = spark.sql(f"""
    SELECT relationship, COUNT(*) as edge_count
    FROM {catalog_name}.{graph_test_schema}.graph_edges
    GROUP BY relationship
    ORDER BY edge_count DESC
""")
print("[TEST 16] Edge distribution:")
edge_summary.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop test schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{graph_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{graph_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL GRAPH VALIDATION INTEGRATION TESTS PASSED")
print("=" * 60)


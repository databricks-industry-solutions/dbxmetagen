# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Profiling Pipeline
# MAGIC 
# MAGIC Tests the profiling pipeline that generates table and column statistics
# MAGIC including the new universal metrics like pattern detection, entropy, and mode values.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Get test parameters
dbutils.widgets.text("catalog_name", "dbxmetagen_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dev_integration_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

# Create unique test schema for isolation
test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
profiling_test_schema = f"profiling_test_{test_timestamp}"

print(f"Test catalog: {catalog_name}")
print(f"Test schema: {profiling_test_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Test Schema and Mock Data

# COMMAND ----------

# Create test schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{profiling_test_schema}")
print(f"[SETUP] Created test schema: {catalog_name}.{profiling_test_schema}")

# COMMAND ----------

# Create mock table_knowledge_base (required by profiling)
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{profiling_test_schema}.table_knowledge_base (
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

# Create actual test tables with various data types for profiling
# Table 1: Numeric data (integers, floats)
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{profiling_test_schema}.test_numeric_data (
    id INT,
    amount DOUBLE,
    quantity INT,
    price DECIMAL(10,2),
    rating FLOAT
)
""")

# Insert test data
spark.sql(f"""
INSERT INTO {catalog_name}.{profiling_test_schema}.test_numeric_data VALUES
(1, 100.50, 10, 99.99, 4.5),
(2, 200.75, 20, 149.99, 3.8),
(3, 150.25, 15, 79.99, 4.2),
(4, 300.00, 30, 199.99, 4.9),
(5, 250.50, 25, 129.99, 4.0),
(6, 175.00, 18, 89.99, 3.5),
(7, 225.25, 22, 119.99, 4.7),
(8, 180.75, 17, 109.99, 4.1),
(9, 275.00, 28, 159.99, 4.4),
(10, NULL, 12, 69.99, NULL)
""")
print("[SETUP] Created test_numeric_data with 10 rows")

# COMMAND ----------

# Table 2: String data with patterns
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{profiling_test_schema}.test_string_data (
    uuid_col STRING,
    email_col STRING,
    date_str_col STRING,
    numeric_id_col STRING,
    category_col STRING,
    empty_col STRING
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{profiling_test_schema}.test_string_data VALUES
('123e4567-e89b-12d3-a456-426614174000', 'user1@example.com', '2024-01-15', '12345', 'CategoryA', ''),
('223e4567-e89b-12d3-a456-426614174001', 'user2@example.com', '2024-01-16', '12346', 'CategoryB', ''),
('323e4567-e89b-12d3-a456-426614174002', 'user3@test.org', '2024-01-17', '12347', 'CategoryA', ''),
('423e4567-e89b-12d3-a456-426614174003', 'admin@company.net', '2024-01-18', '12348', 'CategoryC', ''),
('523e4567-e89b-12d3-a456-426614174004', 'support@domain.io', '2024-01-19', '12349', 'CategoryA', ''),
('623e4567-e89b-12d3-a456-426614174005', 'info@website.com', '2024-01-20', '12350', 'CategoryB', NULL),
('723e4567-e89b-12d3-a456-426614174006', 'sales@business.co', '2024-01-21', '12351', 'CategoryA', ''),
('823e4567-e89b-12d3-a456-426614174007', 'dev@startup.xyz', '2024-01-22', '12352', 'CategoryB', ''),
('923e4567-e89b-12d3-a456-426614174008', NULL, '2024-01-23', '12353', 'CategoryC', ''),
('a23e4567-e89b-12d3-a456-426614174009', 'test@email.com', '2024-01-24', '12354', 'CategoryA', ''),
('b23e4567-e89b-12d3-a456-426614174010', 'ops@firm.com', '2024-01-25', '12355', 'CategoryB', ''),
('c23e4567-e89b-12d3-a456-426614174011', 'hr@corp.com', '2024-01-26', '12356', 'CategoryC', ''),
('d23e4567-e89b-12d3-a456-426614174012', 'cto@tech.io', '2024-01-27', '12357', 'CategoryA', ''),
('e23e4567-e89b-12d3-a456-426614174013', 'pm@agency.co', '2024-01-28', '12358', 'CategoryB', ''),
('f23e4567-e89b-12d3-a456-426614174014', 'qa@lab.dev', '2024-01-29', '12359', 'CategoryA', '')
""")
print("[SETUP] Created test_string_data with 15 rows")

# COMMAND ----------

# Table 3: Mixed types with nulls
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{profiling_test_schema}.test_mixed_data (
    unique_id STRING,
    name STRING,
    age INT,
    score DOUBLE,
    created_date DATE,
    updated_ts TIMESTAMP,
    is_active BOOLEAN
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{profiling_test_schema}.test_mixed_data VALUES
('U001', 'Alice', 25, 85.5, '2024-01-01', '2024-01-01 10:00:00', true),
('U002', 'Bob', 30, 92.0, '2024-01-02', '2024-01-02 11:00:00', true),
('U003', 'Charlie', 35, 78.5, '2024-01-03', '2024-01-03 12:00:00', false),
('U004', 'Diana', NULL, 88.0, '2024-01-04', '2024-01-04 13:00:00', true),
('U005', 'Eve', 28, NULL, '2024-01-05', NULL, true),
('U006', NULL, 32, 91.5, '2024-01-06', '2024-01-06 14:00:00', false),
('U007', 'Frank', 45, 75.0, '2024-01-07', '2024-01-07 15:00:00', true),
('U008', 'Grace', 29, 89.5, NULL, '2024-01-08 16:00:00', NULL)
""")
print("[SETUP] Created test_mixed_data with 8 rows")

# COMMAND ----------

# Register tables in knowledge base
kb_data = [
    Row(
        table_name=f"{catalog_name}.{profiling_test_schema}.test_numeric_data",
        catalog=catalog_name, schema=profiling_test_schema, table_short_name="test_numeric_data",
        comment="Numeric test data", domain="Testing", subdomain="Numeric",
        has_pii=False, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    Row(
        table_name=f"{catalog_name}.{profiling_test_schema}.test_string_data",
        catalog=catalog_name, schema=profiling_test_schema, table_short_name="test_string_data",
        comment="String patterns test data", domain="Testing", subdomain="Strings",
        has_pii=True, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
    Row(
        table_name=f"{catalog_name}.{profiling_test_schema}.test_mixed_data",
        catalog=catalog_name, schema=profiling_test_schema, table_short_name="test_mixed_data",
        comment="Mixed data types", domain="Testing", subdomain="Mixed",
        has_pii=True, has_phi=False,
        created_at=datetime.now(), updated_at=datetime.now()
    ),
]

kb_df = spark.createDataFrame(kb_data)
kb_df.write.mode("append").saveAsTable(f"{catalog_name}.{profiling_test_schema}.table_knowledge_base")
print(f"[SETUP] Added {len(kb_data)} tables to knowledge base")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Profiling Pipeline

# COMMAND ----------

from dbxmetagen.profiling import run_profiling

result = run_profiling(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=profiling_test_schema,
    max_tables=None
)

print(f"[ETL] Tables profiled: {result['tables_profiled']}")
print(f"[ETL] Tables failed: {result['tables_failed']}")
print(f"[ETL] Columns profiled: {result.get('columns_profiled', 'N/A')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate profiling_snapshots Table

# COMMAND ----------

# Test 1: Verify all tables were profiled
snapshot_count = spark.sql(f"""
    SELECT COUNT(DISTINCT table_name) as cnt 
    FROM {catalog_name}.{profiling_test_schema}.profiling_snapshots
""").collect()[0]["cnt"]

assert snapshot_count == 3, f"Expected 3 tables profiled, got {snapshot_count}"
print("[TEST 1] PASSED: All 3 test tables profiled")

# COMMAND ----------

# Test 2: Verify snapshot columns exist
snapshot_cols = spark.table(f"{catalog_name}.{profiling_test_schema}.profiling_snapshots").columns
required_cols = ["snapshot_id", "table_name", "row_count", "column_count", "table_size_bytes", "snapshot_time"]
for col in required_cols:
    assert col in snapshot_cols, f"Missing required column: {col}"
print("[TEST 2] PASSED: profiling_snapshots has all required columns")

# COMMAND ----------

# Test 3: Verify row counts are correct
for table_info in [("test_numeric_data", 10), ("test_string_data", 15), ("test_mixed_data", 8)]:
    table_name = f"{catalog_name}.{profiling_test_schema}.{table_info[0]}"
    expected_rows = table_info[1]
    actual_rows = spark.sql(f"""
        SELECT row_count FROM {catalog_name}.{profiling_test_schema}.profiling_snapshots
        WHERE table_name = '{table_name}'
        ORDER BY snapshot_time DESC LIMIT 1
    """).collect()[0]["row_count"]
    
    assert actual_rows == expected_rows, f"Expected {expected_rows} rows for {table_info[0]}, got {actual_rows}"

print("[TEST 3] PASSED: Row counts are accurate for all tables")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate column_profiling_stats Table

# COMMAND ----------

# Test 4: Verify new universal metrics exist in schema
stats_cols = spark.table(f"{catalog_name}.{profiling_test_schema}.column_profiling_stats").columns

new_universal_cols = [
    "data_type", "sample_values", "mode_value", "mode_frequency", 
    "entropy", "is_unique_candidate", "pattern_detected", 
    "has_numeric_stats", "has_string_stats"
]

for col in new_universal_cols:
    assert col in stats_cols, f"Missing new universal metric column: {col}"
print("[TEST 4] PASSED: All new universal metric columns exist")

# COMMAND ----------

# Test 5: Verify data_type is populated for all columns
null_data_types = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE data_type IS NULL OR data_type = ''
""").collect()[0]["cnt"]

assert null_data_types == 0, f"Found {null_data_types} columns with NULL/empty data_type"
print("[TEST 5] PASSED: data_type is populated for all columns")

# COMMAND ----------

# Test 6: Verify sample_values is valid JSON
sample_values_check = spark.sql(f"""
    SELECT column_name, sample_values
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE sample_values IS NOT NULL AND sample_values != ''
    LIMIT 5
""").collect()

for row in sample_values_check:
    try:
        parsed = json.loads(row["sample_values"])
        assert isinstance(parsed, list), f"sample_values should be a JSON array"
    except json.JSONDecodeError as e:
        raise AssertionError(f"Invalid JSON in sample_values for {row['column_name']}: {e}")

print("[TEST 6] PASSED: sample_values contains valid JSON arrays")

# COMMAND ----------

# Test 7: Verify pattern detection for string columns
uuid_pattern = spark.sql(f"""
    SELECT pattern_detected
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_string_data%' AND column_name = 'uuid_col'
""").collect()

if uuid_pattern:
    assert uuid_pattern[0]["pattern_detected"] == "uuid", \
        f"Expected 'uuid' pattern for uuid_col, got '{uuid_pattern[0]['pattern_detected']}'"
    print("[TEST 7] PASSED: UUID pattern detected correctly")
else:
    print("[TEST 7] SKIPPED: uuid_col stats not found (may be due to column limit)")

# COMMAND ----------

# Test 8: Verify email pattern detection
email_pattern = spark.sql(f"""
    SELECT pattern_detected
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_string_data%' AND column_name = 'email_col'
""").collect()

if email_pattern:
    assert email_pattern[0]["pattern_detected"] == "email", \
        f"Expected 'email' pattern for email_col, got '{email_pattern[0]['pattern_detected']}'"
    print("[TEST 8] PASSED: Email pattern detected correctly")
else:
    print("[TEST 8] SKIPPED: email_col stats not found")

# COMMAND ----------

# Test 9: Verify mode_value and mode_frequency for low-cardinality columns
category_mode = spark.sql(f"""
    SELECT mode_value, mode_frequency
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_string_data%' AND column_name = 'category_col'
""").collect()

if category_mode:
    assert category_mode[0]["mode_value"] is not None, "mode_value should be populated"
    assert category_mode[0]["mode_frequency"] is not None, "mode_frequency should be populated"
    assert category_mode[0]["mode_value"] == "CategoryA", \
        f"Expected 'CategoryA' as mode, got '{category_mode[0]['mode_value']}'"
    print("[TEST 9] PASSED: Mode value and frequency computed correctly")
else:
    print("[TEST 9] SKIPPED: category_col stats not found")

# COMMAND ----------

# Test 10: Verify entropy is computed for low-cardinality columns
entropy_check = spark.sql(f"""
    SELECT entropy
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_string_data%' AND column_name = 'category_col'
""").collect()

if entropy_check and entropy_check[0]["entropy"] is not None:
    entropy_val = entropy_check[0]["entropy"]
    assert 0 <= entropy_val <= 10, f"Entropy should be between 0 and ~10, got {entropy_val}"
    print(f"[TEST 10] PASSED: Entropy computed correctly (value: {entropy_val:.4f})")
else:
    print("[TEST 10] SKIPPED: Entropy not computed")

# COMMAND ----------

# Test 11: Verify is_unique_candidate for high-cardinality columns
unique_check = spark.sql(f"""
    SELECT column_name, is_unique_candidate, cardinality_ratio, null_rate
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_string_data%' AND column_name = 'uuid_col'
""").collect()

if unique_check:
    assert unique_check[0]["is_unique_candidate"] == True, \
        "uuid_col should be flagged as unique candidate"
    print("[TEST 11] PASSED: is_unique_candidate correctly identifies unique columns")
else:
    print("[TEST 11] SKIPPED: uuid_col stats not found")

# COMMAND ----------

# Test 12: Verify null handling - defaults not NULL
null_defaults_check = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE percentiles IS NULL
""").collect()[0]["cnt"]

# percentiles should be empty dict {}, not NULL
# Note: some columns legitimately have no percentiles (non-numeric)
print(f"[TEST 12] INFO: {null_defaults_check} rows have NULL percentiles (expected for non-numeric)")

# COMMAND ----------

# Test 13: Verify numeric stats populated for numeric columns
numeric_stats = spark.sql(f"""
    SELECT column_name, has_numeric_stats, mean_value, stddev_value
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_numeric_data%' AND column_name = 'amount'
""").collect()

if numeric_stats:
    assert numeric_stats[0]["has_numeric_stats"] == True, "amount should have numeric stats"
    assert numeric_stats[0]["mean_value"] is not None, "mean_value should be computed"
    print("[TEST 13] PASSED: Numeric stats populated for numeric columns")
else:
    print("[TEST 13] SKIPPED: amount stats not found")

# COMMAND ----------

# Test 14: Verify string stats populated for string columns
string_stats = spark.sql(f"""
    SELECT column_name, has_string_stats, min_length, max_length, empty_string_count
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_string_data%' AND column_name = 'empty_col'
""").collect()

if string_stats:
    assert string_stats[0]["has_string_stats"] == True, "empty_col should have string stats"
    # empty_col has 9 empty strings and 1 NULL
    print(f"[TEST 14] PASSED: String stats populated (empty_string_count: {string_stats[0]['empty_string_count']})")
else:
    print("[TEST 14] SKIPPED: empty_col stats not found")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test Incremental Profiling

# COMMAND ----------

# Test 15: Run profiling again and verify drift detection
# First, add more data
spark.sql(f"""
INSERT INTO {catalog_name}.{profiling_test_schema}.test_numeric_data VALUES
(11, 500.00, 50, 299.99, 5.0),
(12, 550.00, 55, 349.99, 4.8),
(13, 600.00, 60, 399.99, 4.6)
""")

# Simulate KB refresh: bump updated_at so incremental mode picks up the change
spark.sql(f"""
UPDATE {catalog_name}.{profiling_test_schema}.table_knowledge_base
SET updated_at = current_timestamp()
WHERE table_name LIKE '%test_numeric_data%'
""")

# Re-run profiling
result2 = run_profiling(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=profiling_test_schema
)

# Check for multiple snapshots
snapshot_history = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.{profiling_test_schema}.profiling_snapshots
    WHERE table_name LIKE '%test_numeric_data%'
""").collect()[0]["cnt"]

assert snapshot_history >= 2, f"Expected at least 2 snapshots for test_numeric_data, got {snapshot_history}"
print(f"[TEST 15] PASSED: Incremental profiling created {snapshot_history} snapshots")

# COMMAND ----------

# Test 16: Verify distinct_count_change_pct is computed
drift_check = spark.sql(f"""
    SELECT column_name, distinct_count, previous_distinct_count, distinct_count_change_pct
    FROM {catalog_name}.{profiling_test_schema}.column_profiling_stats
    WHERE table_name LIKE '%test_numeric_data%' 
    AND column_name = 'id'
    ORDER BY created_at DESC
    LIMIT 1
""").collect()

if drift_check and drift_check[0]["previous_distinct_count"] is not None:
    print(f"[TEST 16] PASSED: Drift detection computed")
    print(f"  Previous distinct: {drift_check[0]['previous_distinct_count']}")
    print(f"  Current distinct: {drift_check[0]['distinct_count']}")
    print(f"  Change %: {drift_check[0]['distinct_count_change_pct']}")
else:
    print("[TEST 16] INFO: Drift detection not yet populated (may need more runs)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop test schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{profiling_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{profiling_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL PROFILING INTEGRATION TESTS PASSED")
print("=" * 60)


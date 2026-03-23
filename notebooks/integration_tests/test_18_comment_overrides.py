# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Comment Overrides
# MAGIC
# MAGIC Verifies that `override_metadata_from_csv` correctly replaces `column_content`
# MAGIC values for comment mode using real Spark DataFrames.
# MAGIC
# MAGIC Covers:
# MAGIC - Column-level override (column-only, table+column, all params)
# MAGIC - Table-level override (table-only, catalog+schema+table)
# MAGIC - Rows NOT matching the override are left unchanged
# MAGIC - Empty/blank comments in CSV are skipped

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys, os, tempfile, csv

sys.path.append("../../src")

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from dbxmetagen.config import MetadataConfig
from dbxmetagen.overrides import override_metadata_from_csv, apply_overrides_with_loop

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: build a comment-mode DataFrame (post-split_and_hardcode_df shape)

# COMMAND ----------

def make_comment_df(rows):
    """Create a comment-mode DF with the columns that exist after split_and_hardcode_df."""
    schema = StructType([
        StructField("table", StringType(), True),
        StructField("tokenized_table", StringType(), True),
        StructField("ddl_type", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("column_content", StringType(), True),
        StructField("catalog", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table_name", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def make_config(**overrides):
    defaults = dict(
        skip_yaml_loading=True,
        catalog_name="cat",
        schema_name="sch",
        mode="comment",
        allow_manual_override=True,
        override_csv_path="metadata_overrides.csv",
    )
    defaults.update(overrides)
    return MetadataConfig(**defaults)


def write_csv(rows):
    """Write override CSV to a temp file, return path."""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
    writer = csv.DictWriter(f, fieldnames=["catalog", "schema", "table", "column", "comment", "classification", "type"])
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    f.close()
    return f.name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Column-level override with all four params

# COMMAND ----------

print("--- Test 1: column-level override (all params) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "column", "customer_id", "Original comment", "cat", "sch", "orders"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "order_date", "Date of order", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "cat", "schema": "sch", "table": "orders", "column": "customer_id",
     "comment": "OVERRIDDEN customer id", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
rows = {r["column_name"]: r["column_content"] for r in result.collect()}

assert rows["customer_id"] == "OVERRIDDEN customer id", f"Expected override, got: {rows['customer_id']}"
assert rows["order_date"] == "Date of order", f"Unrelated column changed: {rows['order_date']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Column-level override with table+column only (no catalog/schema)

# COMMAND ----------

print("--- Test 2: column-level override (table+column only) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "column", "customer_id", "Original", "cat", "sch", "orders"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "amount", "Amount col", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "", "schema": "", "table": "orders", "column": "customer_id",
     "comment": "Partial override", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
rows = {r["column_name"]: r["column_content"] for r in result.collect()}

assert rows["customer_id"] == "Partial override", f"Expected override, got: {rows['customer_id']}"
assert rows["amount"] == "Amount col", f"Unrelated column changed: {rows['amount']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Column-only override (no table/schema/catalog)

# COMMAND ----------

print("--- Test 3: column-only override (matches across tables) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "column", "ssn", "Old SSN comment", "cat", "sch", "orders"),
    Row("cat.sch.users", "cat.sch.users", "column", "ssn", "Old SSN in users", "cat", "sch", "users"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "amount", "Amount col", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "", "schema": "", "table": "", "column": "ssn",
     "comment": "Social Security Number", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
rows = result.collect()
ssn_rows = [r for r in rows if r["column_name"] == "ssn"]
other_rows = [r for r in rows if r["column_name"] != "ssn"]

for r in ssn_rows:
    assert r["column_content"] == "Social Security Number", f"SSN not overridden in {r['table_name']}: {r['column_content']}"
for r in other_rows:
    assert r["column_content"] == "Amount col", f"Unrelated col changed: {r['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Table-level override

# COMMAND ----------

print("--- Test 4: table-level override (catalog+schema+table) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "table", "None", "Old table comment", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "cat", "schema": "sch", "table": "orders", "column": "",
     "comment": "New table description", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
row = result.collect()[0]

assert row["column_content"] == "New table description", f"Table comment not overridden: {row['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Table-level override with table-only (no catalog/schema)

# COMMAND ----------

print("--- Test 5: table-level override (table-only) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "table", "None", "Old table comment", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "", "schema": "", "table": "orders", "column": "",
     "comment": "Table-only override", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
row = result.collect()[0]

assert row["column_content"] == "Table-only override", f"Table comment not overridden: {row['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Blank/empty comment in CSV is skipped

# COMMAND ----------

print("--- Test 6: blank comment is skipped ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "column", "customer_id", "Original", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "cat", "schema": "sch", "table": "orders", "column": "customer_id",
     "comment": "", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
row = result.collect()[0]

assert row["column_content"] == "Original", f"Blank comment should be skipped, got: {row['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Multiple overrides in a single CSV

# COMMAND ----------

print("--- Test 7: multiple overrides in one CSV ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "table", "None", "Old table comment", "cat", "sch", "orders"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "customer_id", "Old cust comment", "cat", "sch", "orders"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "order_date", "Old date comment", "cat", "sch", "orders"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "amount", "Old amount comment", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "cat", "schema": "sch", "table": "orders", "column": "",
     "comment": "New table desc", "classification": "", "type": ""},
    {"catalog": "", "schema": "", "table": "", "column": "customer_id",
     "comment": "New cust desc", "classification": "", "type": ""},
    {"catalog": "", "schema": "", "table": "", "column": "amount",
     "comment": "", "classification": "", "type": ""},  # blank -> skip
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
rows = {r["column_name"]: r["column_content"] for r in result.collect()}

assert rows["None"] == "New table desc", f"Table override failed: {rows['None']}"
assert rows["customer_id"] == "New cust desc", f"Customer override failed: {rows['customer_id']}"
assert rows["order_date"] == "Old date comment", f"Unmatched col changed: {rows['order_date']}"
assert rows["amount"] == "Old amount comment", f"Blank CSV comment should be skipped: {rows['amount']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Case-insensitive column name matching

# COMMAND ----------

print("--- Test 8: case-insensitive column name matching ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "column", "SSN", "Old SSN comment", "cat", "sch", "orders"),
    Row("cat.sch.orders", "cat.sch.orders", "column", "Customer_ID", "Old cust comment", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "", "schema": "", "table": "", "column": "ssn",
     "comment": "Social Security Number (case test)", "classification": "", "type": ""},
    {"catalog": "", "schema": "", "table": "Orders", "column": "customer_id",
     "comment": "Customer ID (case test)", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
rows = {r["column_name"]: r["column_content"] for r in result.collect()}

assert rows["SSN"] == "Social Security Number (case test)", f"Case-insensitive column match failed: {rows['SSN']}"
assert rows["Customer_ID"] == "Customer ID (case test)", f"Case-insensitive table+column match failed: {rows['Customer_ID']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: Case-insensitive table-level override

# COMMAND ----------

print("--- Test 9: case-insensitive table-level override ---")

df = make_comment_df([
    Row("cat.sch.Orders", "cat.sch.Orders", "table", "None", "Old table comment", "cat", "sch", "Orders"),
])

csv_path = write_csv([
    {"catalog": "Cat", "schema": "Sch", "table": "orders", "column": "",
     "comment": "Table override (case test)", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
row = result.collect()[0]

assert row["column_content"] == "Table override (case test)", f"Case-insensitive table override failed: {row['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10: Column comment with commas (quoted in CSV)

# COMMAND ----------

print("--- Test 10: column comment with commas (quoted) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "column", "amount", "Old amount comment", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "", "schema": "", "table": "orders", "column": "amount",
     "comment": "Sales data, including returns", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
row = result.collect()[0]

assert row["column_content"] == "Sales data, including returns", f"Comma in comment not preserved: {row['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11: Table comment with commas (quoted in CSV)

# COMMAND ----------

print("--- Test 11: table comment with commas (quoted) ---")

df = make_comment_df([
    Row("cat.sch.orders", "cat.sch.orders", "table", "None", "Old table comment", "cat", "sch", "orders"),
])

csv_path = write_csv([
    {"catalog": "cat", "schema": "sch", "table": "orders", "column": "",
     "comment": "Order tracking, including returns, refunds", "classification": "", "type": ""},
])

config = make_config()
result = override_metadata_from_csv(df, csv_path, config)
row = result.collect()[0]

assert row["column_content"] == "Order tracking, including returns, refunds", f"Comma in table comment not preserved: {row['column_content']}"
os.unlink(csv_path)
print("PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n=== All comment override integration tests PASSED ===")

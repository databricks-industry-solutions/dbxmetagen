# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Domain Classification Pipeline
# MAGIC
# MAGIC Tests the two-stage domain classification pipeline, `_enforce_value` validation
# MAGIC against real and synthetic configs, and robustness under large (50-domain) configs.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
sys.path.append("../../src")

import os
from datetime import datetime

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
domain_test_schema = f"domain_test_{test_timestamp}"

print(f"Test catalog: {catalog_name}")
print(f"Test schema: {domain_test_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Test Schema and Tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{domain_test_schema}")
print(f"[SETUP] Created test schema: {catalog_name}.{domain_test_schema}")

# COMMAND ----------

# Healthcare-themed table
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{domain_test_schema}.patient_encounters (
    patient_id BIGINT,
    encounter_id BIGINT,
    admission_date DATE,
    discharge_date DATE,
    diagnosis_code STRING,
    attending_physician STRING,
    department STRING,
    insurance_plan STRING
) COMMENT 'Patient encounter records from the EHR system'
""")
spark.sql(f"""
INSERT INTO {catalog_name}.{domain_test_schema}.patient_encounters VALUES
    (1, 100, '2025-01-01', '2025-01-03', 'J18.9', 'Dr. Smith', 'Internal Medicine', 'PPO-Gold'),
    (2, 101, '2025-01-02', '2025-01-02', 'K21.0', 'Dr. Jones', 'Gastroenterology', 'HMO-Basic')
""")
print("[SETUP] Created patient_encounters table")

# COMMAND ----------

# Finance-themed table
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{domain_test_schema}.general_ledger_entries (
    entry_id BIGINT,
    account_number STRING,
    debit_amount DECIMAL(18,2),
    credit_amount DECIMAL(18,2),
    posting_date DATE,
    journal_id STRING,
    cost_center STRING,
    fiscal_year INT
) COMMENT 'General ledger journal entries for accounting'
""")
spark.sql(f"""
INSERT INTO {catalog_name}.{domain_test_schema}.general_ledger_entries VALUES
    (1, '4000-100', 5000.00, 0.00, '2025-01-15', 'JE-001', 'CC-100', 2025),
    (2, '4000-100', 0.00, 5000.00, '2025-01-15', 'JE-001', 'CC-200', 2025)
""")
print("[SETUP] Created general_ledger_entries table")

# COMMAND ----------

# Ambiguous table
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{domain_test_schema}.system_metrics_log (
    metric_id BIGINT,
    metric_name STRING,
    metric_value DOUBLE,
    recorded_at TIMESTAMP,
    source_system STRING
) COMMENT 'System performance metrics and operational telemetry'
""")
spark.sql(f"""
INSERT INTO {catalog_name}.{domain_test_schema}.system_metrics_log VALUES
    (1, 'cpu_utilization', 72.5, current_timestamp(), 'monitoring-agent'),
    (2, 'memory_usage_mb', 4096.0, current_timestamp(), 'monitoring-agent')
""")
print("[SETUP] Created system_metrics_log table")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Domain Config

# COMMAND ----------

from dbxmetagen.domain_classifier import (
    load_domain_config,
    classify_table_domain,
    _enforce_value,
)

repo_root = os.path.abspath(os.path.join(os.path.dirname("__file__"), "../.."))
standalone_path = os.path.join(repo_root, "configurations", "domain_config_healthcare.yaml")
config = load_domain_config(config_path=standalone_path)

domain_keys = list(config["domains"].keys())
print(f"[SETUP] Loaded {len(domain_keys)} domains: {domain_keys}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 1: Healthcare table classified with valid domain key

# COMMAND ----------

table_meta_patient = {
    "column_contents": {c.name: c.dataType.simpleString() for c in spark.table(f"{catalog_name}.{domain_test_schema}.patient_encounters").schema},
    "table_comments": "Patient encounter records from the EHR system",
}

result_patient = classify_table_domain(
    f"{catalog_name}.{domain_test_schema}.patient_encounters",
    table_meta_patient,
    config,
    two_stage=True,
)

assert result_patient["domain"] in domain_keys, (
    f"Domain '{result_patient['domain']}' not in config keys {domain_keys}"
)
print(f"[TEST 1] PASSED: patient_encounters -> domain='{result_patient['domain']}' (valid key)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 2: Finance table classified with valid domain key

# COMMAND ----------

table_meta_finance = {
    "column_contents": {c.name: c.dataType.simpleString() for c in spark.table(f"{catalog_name}.{domain_test_schema}.general_ledger_entries").schema},
    "table_comments": "General ledger journal entries for accounting",
}

result_finance = classify_table_domain(
    f"{catalog_name}.{domain_test_schema}.general_ledger_entries",
    table_meta_finance,
    config,
    two_stage=True,
)

assert result_finance["domain"] in domain_keys, (
    f"Domain '{result_finance['domain']}' not in config keys {domain_keys}"
)
print(f"[TEST 2] PASSED: general_ledger_entries -> domain='{result_finance['domain']}' (valid key)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 3: Result dict has all required keys

# COMMAND ----------

REQUIRED_KEYS = [
    "catalog", "schema", "table", "domain", "subdomain", "confidence",
    "reasoning", "metadata_summary", "recommended_domain", "recommended_subdomain",
]

for key in REQUIRED_KEYS:
    assert key in result_patient, f"Missing key '{key}' in result dict"
print(f"[TEST 3] PASSED: result dict has all {len(REQUIRED_KEYS)} required keys")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 4: Confidence in valid range

# COMMAND ----------

for name, res in [("patient", result_patient), ("finance", result_finance)]:
    assert 0.0 <= res["confidence"] <= 1.0, (
        f"{name} confidence {res['confidence']} out of range [0, 1]"
    )
print("[TEST 4] PASSED: all confidence values in [0.0, 1.0]")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 5: Domain and subdomain are exact YAML config keys

# COMMAND ----------

for name, res in [("patient", result_patient), ("finance", result_finance)]:
    d = res["domain"]
    assert d in domain_keys, f"{name}: domain '{d}' not an exact config key"

    sd = res.get("subdomain")
    if sd and sd != "None":
        sd_keys = list(config["domains"].get(d, {}).get("subdomains", {}).keys())
        assert sd in sd_keys, f"{name}: subdomain '{sd}' not in {d}'s subdomain keys {sd_keys}"

print("[TEST 5] PASSED: all domain/subdomain values are exact YAML config keys")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 6: _enforce_value snaps display names to config keys (real config)

# COMMAND ----------

all_sd_keys = []
for dk in domain_keys:
    subs = config["domains"][dk].get("subdomains", {})
    all_sd_keys.extend(list(subs.keys()))

display_pairs = [
    ("Clinical", "clinical", domain_keys),
    ("Quality & Safety", "quality_safety", domain_keys),
    ("Patient Care", "patient_care", all_sd_keys),
    ("Diagnosis & Conditions", "diagnosis_condition", all_sd_keys),
    ("Medications & Orders", "medication_orders", all_sd_keys),
    ("Clinical Documentation", "clinical_documentation", all_sd_keys),
]

for display, expected_key, allowed in display_pairs:
    val, _ = _enforce_value(display, allowed)
    assert val == expected_key, f"_enforce_value('{display}') -> '{val}', expected '{expected_key}'"

print(f"[TEST 6] PASSED: _enforce_value correctly snaps {len(display_pairs)} display names to config keys")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 7-10: 50-Domain Stress Test

# COMMAND ----------

STRESS_DOMAINS = {}
for i in range(50):
    key = f"domain_{i:02d}"
    STRESS_DOMAINS[key] = {
        "name": f"Domain {i:02d} Display",
        "description": f"This is the description for business domain number {i}.",
        "keywords": [f"kw_{i}a", f"kw_{i}b", f"kw_{i}c"],
        "subdomains": {
            f"{key}_sub_a": {
                "name": f"Sub A of Domain {i:02d}",
                "description": f"First subdomain of domain {i}",
                "keywords": [f"sub_{i}_a1"],
            },
            f"{key}_sub_b": {
                "name": f"Sub B of Domain {i:02d}",
                "description": f"Second subdomain of domain {i}",
                "keywords": [f"sub_{i}_b1"],
            },
        },
    }

stress_config = {"domains": STRESS_DOMAINS}
stress_domain_keys = list(STRESS_DOMAINS.keys())

print(f"[SETUP] Built synthetic config with {len(stress_domain_keys)} domains, {len(stress_domain_keys) * 2} subdomains")

# COMMAND ----------

# Test 7: Exact match for all 50 domain keys
for k in stress_domain_keys:
    val, exact = _enforce_value(k, stress_domain_keys)
    assert val == k and exact is True, f"Exact match failed for '{k}': got '{val}'"

print("[TEST 7] PASSED: all 50 domain keys resolve via exact match")

# COMMAND ----------

# Test 8: Classify a table against the 50-domain config (LLM call)
result_stress = classify_table_domain(
    f"{catalog_name}.{domain_test_schema}.patient_encounters",
    table_meta_patient,
    stress_config,
    two_stage=True,
)

assert result_stress["domain"] in stress_domain_keys or result_stress["domain"] == "unknown", (
    f"Stress test returned invalid domain: '{result_stress['domain']}'"
)
assert 0.0 <= result_stress["confidence"] <= 1.0
print(f"[TEST 8] PASSED: LLM classified table as '{result_stress['domain']}' with 50-domain config (confidence={result_stress['confidence']:.2f})")

# COMMAND ----------

# Test 9: Display-name variants resolve correctly for stress config
for i in [0, 10, 25, 37, 49]:
    key = f"domain_{i:02d}"
    display = f"Domain {i:02d} Display"
    val, _ = _enforce_value(display, stress_domain_keys)
    assert val == key, f"Display name '{display}' resolved to '{val}', expected '{key}'"

print("[TEST 9] PASSED: display-name variants resolve correctly for sampled stress domains")

# COMMAND ----------

# Test 10: Garbage input does NOT match any of the 50 domains
garbage_inputs = [
    "completely_unrelated_term", "banana_split_sundae", "12345",
    "the_quick_brown_fox", "___---___",
]
for g in garbage_inputs:
    val, exact = _enforce_value(g, stress_domain_keys)
    assert val == "unknown", f"Garbage '{g}' matched '{val}' in 50-domain config"

print(f"[TEST 10] PASSED: {len(garbage_inputs)} garbage inputs correctly rejected from 50-domain config")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 11: Unreadable Table Returns Fallback Dict

# COMMAND ----------

error_result = classify_table_domain(
    f"{catalog_name}.{domain_test_schema}.nonexistent_table_xyz",
    {},
    config,
    two_stage=True,
)

assert error_result["domain"] == "unknown", f"Expected 'unknown', got '{error_result['domain']}'"
assert error_result["confidence"] == 0.0
for key in ["reasoning", "metadata_summary", "subdomain"]:
    assert key in error_result, f"Error result missing key '{key}'"

print("[TEST 11] PASSED: nonexistent table returns valid fallback dict (domain='unknown', confidence=0.0)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{domain_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{domain_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL DOMAIN CLASSIFICATION INTEGRATION TESTS PASSED")
print("=" * 60)

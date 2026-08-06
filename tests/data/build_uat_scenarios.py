# Databricks notebook source
# MAGIC %md
# MAGIC # dbxmetagen UAT Scenario Builder
# MAGIC
# MAGIC Builds purpose-built test schemas in a target catalog (set `catalog_name`),
# MAGIC where **each schema stresses one capability or a known failure mode** of dbxmetagen.
# MAGIC Use it as a repeatable pre-release sanity check: build the data, run metadata
# MAGIC generation + the analytics pipeline over each scenario, then verify against the
# MAGIC known-answer runbook (`docs/UAT_SANITY_CHECK.md`) and the app walkthrough
# MAGIC (`docs/UAT_APP_WALKTHROUGH.md`).
# MAGIC
# MAGIC **Scenarios (one schema each):**
# MAGIC
# MAGIC | Schema | Capability under test |
# MAGIC |--------|----------------------|
# MAGIC | `uat_star_retail`      | STAR schema → metric views / Genie / clear FKs |
# MAGIC | `uat_snowflake_health` | SNOWFLAKE + bridge → multi-hop FK / join generation |
# MAGIC | `uat_datamart`         | Pre-aggregated marts → SIMPLE single-table metric views |
# MAGIC | `uat_fk_hard`          | FK stressors: generic id↔id, named-but-non-joining, role-prefix, dedup |
# MAGIC | `uat_pii`              | PII/PHI/PCI detection + false-positive traps |
# MAGIC | `uat_domains`          | Domain classification incl. ambiguous / misnamed tables |
# MAGIC | `uat_types`            | Databricks data-type coverage (BINARY/VARIANT/TIMESTAMP/complex) |
# MAGIC | `uat_ddl_edges`        | DDL/comment edge cases + sampling edges (wide/empty/1-row) |
# MAGIC
# MAGIC Tables are **managed Delta with empty comments** (so generation has something to
# MAGIC fill). Data is deterministic (seeded Faker + literal seed rows). The builder is
# MAGIC idempotent (`overwrite`). Each schema gets a `_scenario_readme` table describing
# MAGIC its intent, and gold-expectation tables are written to `<catalog>.uat_eval` in the
# MAGIC `eval_expected_*` shape consumed by the eval_e2e scoring harness.

# COMMAND ----------

# MAGIC %pip install -qqqq faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Target Catalog (required)")
dbutils.widgets.text("eval_schema", "uat_eval", "Gold Expectations Schema")
dbutils.widgets.text(
    "skip_scenarios", "", "Skip Scenarios (comma-sep, e.g. uat_pii,uat_types)"
)
dbutils.widgets.text("base_rows", "500", "Bulk row count for synthetic tables")

catalog_name = dbutils.widgets.get("catalog_name").strip()
eval_schema = dbutils.widgets.get("eval_schema").strip()
skip_scenarios = {
    s.strip() for s in dbutils.widgets.get("skip_scenarios").split(",") if s.strip()
}
base_rows = int(dbutils.widgets.get("base_rows").strip() or "500")

assert catalog_name, "catalog_name is required"

# COMMAND ----------

import random
from datetime import date, datetime, timedelta

from faker import Faker
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Determinism: seed everything so re-runs and gold expectations stay stable.
SEED = 424242
random.seed(SEED)
Faker.seed(SEED)
fake = Faker("en_US")

print(f"Target catalog: {catalog_name}")
print(f"Gold schema:    {catalog_name}.{eval_schema}")
print(f"Bulk rows:      {base_rows}")
if skip_scenarios:
    print(f"Skipping:       {', '.join(sorted(skip_scenarios))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def ensure_schema(schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema}`")


def drop_schema(schema: str) -> None:
    """Drop the whole scenario schema so a re-run starts clean (idempotent)."""
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema}` CASCADE")


def fq(schema: str, table: str) -> str:
    return f"`{catalog_name}`.`{schema}`.`{table}`"


def write_pandas(pdf, schema: str, table: str) -> None:
    """Write a pandas DataFrame as a managed Delta table (comments left empty)."""
    ensure_schema(schema)
    sdf = spark.createDataFrame(pdf)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog_name}.{schema}.{table}"
    )
    print(f"  {schema}.{table}: {sdf.count()} rows, {len(sdf.columns)} cols")


def assert_fk_joins(schema: str, child: str, child_col: str, parent: str,
                    parent_col: str, expect_orphans: bool = False) -> None:
    """Assert FK integrity (or deliberate lack of it) via LEFT ANTI JOIN."""
    orphans = spark.sql(
        f"SELECT COUNT(*) AS c FROM {fq(schema, child)} c "
        f"LEFT ANTI JOIN {fq(schema, parent)} p ON c.`{child_col}` = p.`{parent_col}`"
    ).collect()[0]["c"]
    label = f"{schema}.{child}.{child_col} -> {parent}.{parent_col}"
    if expect_orphans:
        assert orphans > 0, f"FK-DECOY {label} expected orphans but found none"
        print(f"  [ok] decoy {label}: {orphans} orphans (join must fail)")
    else:
        assert orphans == 0, f"FK {label} has {orphans} orphan rows"
        print(f"  [ok] {label}: clean join")


def luhn_check_digit(number_without_check: str) -> str:
    """Return the Luhn check digit that makes number_without_check valid.

    In the completed number the check digit is at the rightmost position, so the
    body's rightmost digit lands at an *even*-from-right (doubled) position. Thus
    the body's `digits[-1::-2]` slice is the one that must be doubled, and
    `digits[-2::-2]` is summed straight. (Doubling the wrong slice yields a
    Luhn-valid number only ~10% of the time.)
    """
    digits = [int(d) for d in number_without_check]
    to_double = digits[-1::-2]
    straight = digits[-2::-2]
    total = sum(straight)
    for d in to_double:
        dd = d * 2
        total += dd - 9 if dd > 9 else dd
    return str((10 - (total % 10)) % 10)


def valid_credit_card() -> str:
    """A Luhn-valid 16-digit test card (Visa-like 4-prefix), grouped with dashes."""
    body = "4" + "".join(str(random.randint(0, 9)) for _ in range(14))
    full = body + luhn_check_digit(body)
    return f"{full[0:4]}-{full[4:8]}-{full[8:12]}-{full[12:16]}"


def invalid_credit_card() -> str:
    """A Luhn-INVALID 16-digit number (bumps the check digit so it never validates)."""
    body = "4" + "".join(str(random.randint(0, 9)) for _ in range(14))
    good = luhn_check_digit(body)
    bad = str((int(good) + 1) % 10)
    full = body + bad
    return f"{full[0:4]}-{full[4:8]}-{full[8:12]}-{full[12:16]}"


# Accumulators for gold-expectation tables (scenario builders append here).
gold_fk = []          # (schema, src_table, src_col, dst_table, dst_col, expect_is_fk)
gold_pi = []          # (schema, table, column, expected_type)  expected_type in {pi,phi,pci,None}
gold_domain = []      # (schema, table, expected_domain, expected_subdomain, notes)
scenario_readmes = {} # schema -> list[(topic, detail)]


def readme(schema: str, topic: str, detail: str) -> None:
    scenario_readmes.setdefault(schema, []).append((topic, detail))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1 — `uat_star_retail` (STAR: metric views / Genie / clear FKs)

# COMMAND ----------


def build_star_retail():
    schema = "uat_star_retail"
    drop_schema(schema)
    import pandas as pd

    n_cust, n_prod, n_store, n_days = 200, 80, 12, 120
    start = date(2024, 1, 1)

    dim_customer = pd.DataFrame([{
        "customer_id": i,
        "customer_name": fake.name(),
        "segment": random.choice(["Consumer", "SMB", "Enterprise"]),
        "country": fake.country(),
        "signup_date": fake.date_between(start_date="-3y", end_date="today"),
    } for i in range(1, n_cust + 1)])

    dim_product = pd.DataFrame([{
        "product_id": i,
        "product_name": fake.catch_phrase(),
        "category": random.choice(["Electronics", "Home", "Apparel", "Grocery"]),
        "unit_price": round(random.uniform(5, 500), 2),
        "is_active": random.random() > 0.1,
    } for i in range(1, n_prod + 1)])

    dim_store = pd.DataFrame([{
        "store_id": i,
        "store_name": f"{fake.city()} Store",
        "region": random.choice(["West", "East", "Central", "South"]),
    } for i in range(1, n_store + 1)])

    dim_date = pd.DataFrame([{
        "date_id": (start + timedelta(days=d)).strftime("%Y%m%d"),
        "calendar_date": start + timedelta(days=d),
        "day_of_week": (start + timedelta(days=d)).strftime("%A"),
        "month": (start + timedelta(days=d)).month,
    } for d in range(n_days)])
    date_ids = dim_date["date_id"].tolist()

    fct_sales = pd.DataFrame([{
        "transaction_id": i,
        "customer_id": random.randint(1, n_cust),
        "product_id": random.randint(1, n_prod),
        "store_id": random.randint(1, n_store),
        "date_id": random.choice(date_ids),
        "amount": round(random.uniform(1, 2000), 2),
        "quantity": random.randint(1, 10),
        "is_refund": random.random() < 0.08,
    } for i in range(1, base_rows * 4 + 1)])

    write_pandas(dim_customer, schema, "dim_customer")
    write_pandas(dim_product, schema, "dim_product")
    write_pandas(dim_store, schema, "dim_store")
    write_pandas(dim_date, schema, "dim_date")
    write_pandas(fct_sales, schema, "fct_sales")

    assert_fk_joins(schema, "fct_sales", "customer_id", "dim_customer", "customer_id")
    assert_fk_joins(schema, "fct_sales", "product_id", "dim_product", "product_id")
    assert_fk_joins(schema, "fct_sales", "store_id", "dim_store", "store_id")
    assert_fk_joins(schema, "fct_sales", "date_id", "dim_date", "date_id")

    for c, ct, p in [("customer_id", "dim_customer", "customer_id"),
                    ("product_id", "dim_product", "product_id"),
                    ("store_id", "dim_store", "store_id"),
                    ("date_id", "dim_date", "date_id")]:
        gold_fk.append((schema, "fct_sales", c, ct, p, True))
    for t in ["dim_customer", "dim_product", "dim_store", "dim_date", "fct_sales"]:
        gold_domain.append((schema, t, "Retail", "Sales", "clear retail star schema"))

    readme(schema, "capability", "STAR schema: metric-view generation, Genie spaces, clear FK prediction")
    readme(schema, "expect", "fct_sales -> 4 dims all predicted as FK (high confidence)")
    readme(schema, "expect", "SemanticLayer detects STAR; metric views use multi-dim joins; is_refund enables sign-flip measure")


if "uat_star_retail" not in skip_scenarios:
    build_star_retail()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2 — `uat_snowflake_health` (SNOWFLAKE + bridge: multi-hop FKs)

# COMMAND ----------


def build_snowflake_health():
    schema = "uat_snowflake_health"
    drop_schema(schema)
    import pandas as pd

    n_hs, n_fac, n_pat = 4, 20, 150

    dim_health_system = pd.DataFrame([{
        "health_system_id": i,
        "health_system_name": f"{fake.last_name()} Health System",
        "hq_state": fake.state_abbr(),
    } for i in range(1, n_hs + 1)])

    dim_facility = pd.DataFrame([{
        "facility_id": i,
        "facility_name": f"{fake.city()} Medical Center",
        "health_system_id": random.randint(1, n_hs),  # facility -> health_system
        "bed_count": random.randint(20, 800),
    } for i in range(1, n_fac + 1)])

    dim_patient = pd.DataFrame([{
        "patient_id": i,
        "patient_name": fake.name(),
        "birth_year": random.randint(1940, 2010),
        "sex": random.choice(["M", "F"]),
    } for i in range(1, n_pat + 1)])

    # Bridge table: patient <-> facility (many-to-many). src∩dst membership makes the
    # schema-profiler see a bridge and classify SNOWFLAKE.
    patient_facility_bridge = pd.DataFrame([{
        "patient_id": random.randint(1, n_pat),
        "facility_id": random.randint(1, n_fac),
        "primary_flag": random.random() < 0.5,
    } for _ in range(n_pat * 2)]).drop_duplicates(subset=["patient_id", "facility_id"])

    fct_encounter = pd.DataFrame([{
        "encounter_id": i,
        "patient_id": random.randint(1, n_pat),
        "facility_id": random.randint(1, n_fac),
        "encounter_date": fake.date_between(start_date="-2y", end_date="today"),
        "encounter_type": random.choice(["Inpatient", "Outpatient", "ER"]),
        "length_of_stay_days": random.randint(0, 21),
    } for i in range(1, base_rows * 2 + 1)])

    write_pandas(dim_health_system, schema, "dim_health_system")
    write_pandas(dim_facility, schema, "dim_facility")
    write_pandas(dim_patient, schema, "dim_patient")
    write_pandas(patient_facility_bridge, schema, "patient_facility_bridge")
    write_pandas(fct_encounter, schema, "fct_encounter")

    assert_fk_joins(schema, "dim_facility", "health_system_id", "dim_health_system", "health_system_id")
    assert_fk_joins(schema, "fct_encounter", "patient_id", "dim_patient", "patient_id")
    assert_fk_joins(schema, "fct_encounter", "facility_id", "dim_facility", "facility_id")
    assert_fk_joins(schema, "patient_facility_bridge", "patient_id", "dim_patient", "patient_id")
    assert_fk_joins(schema, "patient_facility_bridge", "facility_id", "dim_facility", "facility_id")

    gold_fk.append((schema, "dim_facility", "health_system_id", "dim_health_system", "health_system_id", True))
    gold_fk.append((schema, "fct_encounter", "patient_id", "dim_patient", "patient_id", True))
    gold_fk.append((schema, "fct_encounter", "facility_id", "dim_facility", "facility_id", True))
    for t in ["dim_health_system", "dim_facility", "dim_patient",
              "patient_facility_bridge", "fct_encounter"]:
        gold_domain.append((schema, t, "Healthcare", "Clinical Operations", "snowflake health schema"))

    readme(schema, "capability", "SNOWFLAKE detection + multi-hop FK / nested join generation")
    readme(schema, "expect", "fct_encounter -> facility -> health_system multi-hop chain; bridge triggers SNOWFLAKE")
    readme(schema, "expect", "patient_name flagged PII in pi mode (see uat_pii for the focused PII test)")


if "uat_snowflake_health" not in skip_scenarios:
    build_snowflake_health()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3 — `uat_datamart` (pre-aggregated: SIMPLE metric views, no joins)

# COMMAND ----------


def build_datamart():
    schema = "uat_datamart"
    drop_schema(schema)
    import pandas as pd

    regions = ["West", "East", "Central", "South"]
    cats = ["Electronics", "Home", "Apparel", "Grocery"]

    sales_summary_monthly = pd.DataFrame([{
        "month_start": date(2024, m, 1),
        "region": r,
        "category": c,
        "total_revenue": round(random.uniform(10000, 500000), 2),
        "order_count": random.randint(50, 5000),
        "avg_order_value": round(random.uniform(20, 400), 2),
    } for m in range(1, 13) for r in regions for c in cats])

    revenue_rollup_by_region = pd.DataFrame([{
        "region": r,
        "fiscal_year": 2024,
        "revenue": round(random.uniform(500000, 5000000), 2),
        "returns": round(random.uniform(1000, 50000), 2),
        "net_revenue": round(random.uniform(450000, 4900000), 2),
    } for r in regions])

    write_pandas(sales_summary_monthly, schema, "sales_summary_monthly")
    write_pandas(revenue_rollup_by_region, schema, "revenue_rollup_by_region")

    for t in ["sales_summary_monthly", "revenue_rollup_by_region"]:
        gold_domain.append((schema, t, "Retail", "Sales Analytics", "pre-aggregated mart"))

    readme(schema, "capability", "DATA_MART detection: pre-aggregated tables, no FKs")
    readme(schema, "expect", "SemanticLayer classifies DATA_MART/SIMPLE; single-table metric views; NO fabricated joins")
    readme(schema, "expect", "0 FK predictions (no join keys present)")


if "uat_datamart" not in skip_scenarios:
    build_datamart()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4 — `uat_fk_hard` (FK stressors: keep/drop known answers)

# COMMAND ----------


def build_fk_hard():
    schema = "uat_fk_hard"
    drop_schema(schema)
    import pandas as pd

    n = 300

    # (a) generic id<->id: orders.id and products.id are BOTH bare `id` — must be DROPPED.
    # (b) legit customer_id -> dim_customer.id : one non-generic side — must SURVIVE.
    dim_customer = pd.DataFrame([{"id": i, "customer_name": fake.name()} for i in range(1, 101)])
    products = pd.DataFrame([{"id": i, "sku": fake.bothify("SKU-####")} for i in range(1, 61)])

    # (d) named-like-FK-but-doesn't-join: orders.region_id matches regions.region_id by
    # name+dtype, but the VALUE RANGES are disjoint (orders use 900-999, regions use 1-20)
    # so join_rate ~ 0 and referential integrity fails -> must be rejected / tagged join_key.
    regions = pd.DataFrame([{"region_id": i, "region_name": fake.state()} for i in range(1, 21)])

    orders = pd.DataFrame([{
        "id": i,
        "customer_id": random.randint(1, 100),        # (b) real FK to dim_customer.id
        "fk_customer_id": random.randint(1, 100),      # (c) role-prefixed, same target
        "region_id": random.randint(900, 999),         # (d) decoy: never matches regions
        "order_total": round(random.uniform(10, 900), 2),
    } for i in range(1, n + 1)])

    write_pandas(dim_customer, schema, "dim_customer")
    write_pandas(products, schema, "products")
    write_pandas(regions, schema, "regions")
    write_pandas(orders, schema, "orders")

    # (e) near-duplicate staging table for dedup / duplicate-suppression testing.
    write_pandas(dim_customer.copy(), schema, "dim_customer_staging")

    assert_fk_joins(schema, "orders", "customer_id", "dim_customer", "id")
    assert_fk_joins(schema, "orders", "fk_customer_id", "dim_customer", "id")
    assert_fk_joins(schema, "orders", "region_id", "regions", "region_id", expect_orphans=True)

    # Gold: which pairs SHOULD be predicted as FK (True) vs must NOT (False).
    gold_fk.append((schema, "orders", "customer_id", "dim_customer", "id", True))
    gold_fk.append((schema, "orders", "fk_customer_id", "dim_customer", "id", True))
    gold_fk.append((schema, "orders", "region_id", "regions", "region_id", False))  # named but doesn't join
    gold_fk.append((schema, "orders", "id", "products", "id", False))               # both-generic id<->id

    readme(schema, "capability", "FK prediction hard cases: keep the real ones, drop the traps")
    readme(schema, "keep", "orders.customer_id -> dim_customer.id (non-generic side, real join)")
    readme(schema, "keep", "orders.fk_customer_id -> dim_customer.id (role-prefixed, real join)")
    readme(schema, "drop", "orders.region_id <-> regions.region_id (name+dtype match, DISJOINT values, join_rate~0)")
    readme(schema, "drop", "orders.id <-> products.id (both bare generic 'id', no corroboration)")
    readme(schema, "note", "dim_customer_staging is a near-duplicate for dedup/suppression testing")


if "uat_fk_hard" not in skip_scenarios:
    build_fk_hard()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5 — `uat_pii` (PII/PHI/PCI + false-positive traps)

# COMMAND ----------


def build_pii():
    schema = "uat_pii"
    drop_schema(schema)
    import pandas as pd

    n = base_rows

    # patients: genuine PII + PHI. patient_name/email/phone/ssn/address -> PI;
    # mrn/icd10_code -> PHI. dob -> PI.
    patients = pd.DataFrame([{
        "patient_id": i,
        "patient_name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "ssn": fake.ssn(),                      # real-format US SSN (fake value)
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=95),
        "home_address": fake.address().replace("\n", ", "),
        "mrn": f"MRN-{random.randint(2018, 2024)}-{random.randint(0, 99999):05d}",  # PHI
        "icd10_code": random.choice(["E11.9", "I10", "J45.909", "M54.5", "F41.1"]),  # PHI dx code
        # trap: sequential 9-digit that LOOKS like an SSN but is a survey id -> must NOT flag
        "survey_id": 100000000 + i,
    } for i in range(1, n + 1)])

    # payments: PCI. card_number (Luhn-valid) + cvv + expiry + iban + swift -> PCI.
    payments = pd.DataFrame([{
        "payment_id": i,
        "cardholder_name": fake.name(),
        "card_number": valid_credit_card(),     # Luhn-VALID -> should flag PCI
        "cvv": f"{random.randint(100, 999)}",
        "expiry": f"{random.randint(1, 12):02d}/{random.randint(26, 31)}",
        "iban": fake.iban(),
        "swift": fake.swift(),
        # trap: Luhn-INVALID 16-digit -> must NOT be flagged as a card
        "order_ref": invalid_credit_card(),
        # trap: plain numeric amount -> not PI
        "amount": round(random.uniform(1, 5000), 2),
    } for i in range(1, n + 1)])

    write_pandas(patients, schema, "patients")
    write_pandas(payments, schema, "payments")

    pi_map = {
        ("patients", "patient_name"): "pi",
        ("patients", "email"): "pi",
        ("patients", "phone"): "pi",
        ("patients", "ssn"): "pi",
        ("patients", "date_of_birth"): "pi",
        ("patients", "home_address"): "pi",
        ("patients", "mrn"): "phi",
        ("patients", "icd10_code"): "phi",
        ("patients", "survey_id"): "None",       # TRAP: looks like SSN, must stay None
        ("patients", "patient_id"): "None",
        ("payments", "cardholder_name"): "pi",
        ("payments", "card_number"): "pci",
        ("payments", "cvv"): "pci",
        ("payments", "iban"): "pci",
        ("payments", "swift"): "pci",
        ("payments", "order_ref"): "None",       # TRAP: Luhn-invalid, must stay None
        ("payments", "amount"): "None",          # TRAP: plain numeric
        ("payments", "payment_id"): "None",
        ("payments", "expiry"): "pci",
    }
    for (t, c), typ in pi_map.items():
        gold_pi.append((schema, t, c, typ))

    gold_domain.append((schema, "patients", "Healthcare", "Patient Administration", "PII/PHI"))
    gold_domain.append((schema, "payments", "Finance", "Payments", "PCI"))

    readme(schema, "capability", "PI/PHI/PCI detection recall AND precision (false-positive traps)")
    readme(schema, "flag", "patients: name/email/phone/ssn/dob/address=PI, mrn/icd10=PHI")
    readme(schema, "flag", "payments: card_number/cvv/expiry/iban/swift=PCI (card is Luhn-valid)")
    readme(schema, "trap", "patients.survey_id (sequential 9-digit) must NOT flag as SSN")
    readme(schema, "trap", "payments.order_ref (Luhn-INVALID) must NOT flag as card; amount stays None")


if "uat_pii" not in skip_scenarios:
    build_pii()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6 — `uat_domains` (domain classification incl. ambiguous / misnamed)

# COMMAND ----------


def build_domains():
    schema = "uat_domains"
    drop_schema(schema)
    import pandas as pd

    # Clear-domain tables (one per distinct domain) for meaningful domain+subdomain.
    retail_orders = pd.DataFrame([{
        "order_id": i, "sku": fake.bothify("SKU-####"),
        "quantity": random.randint(1, 5), "unit_price": round(random.uniform(5, 200), 2),
        "channel": random.choice(["web", "store", "mobile"]),
    } for i in range(1, 101)])

    clinical_labs = pd.DataFrame([{
        "result_id": i, "test_code": random.choice(["CBC", "BMP", "LFT", "A1C"]),
        "result_value": round(random.uniform(0.1, 300), 1),
        "ref_range_low": 0.0, "ref_range_high": round(random.uniform(50, 300), 1),
        "abnormal_flag": random.choice(["H", "L", "N"]),
    } for i in range(1, 101)])

    gl_transactions = pd.DataFrame([{
        "journal_id": i, "gl_account": fake.bothify("####-###"),
        "debit": round(random.uniform(0, 10000), 2), "credit": round(random.uniform(0, 10000), 2),
        "posting_date": fake.date_this_year(),
    } for i in range(1, 101)])

    # Ambiguous: mixes patient + invoice columns -> classifier should be uncertain,
    # penalize confidence and/or emit recommended_domain.
    service_requests = pd.DataFrame([{
        "request_id": i,
        "patient_name": fake.name(),
        "invoice_amount": round(random.uniform(50, 5000), 2),
        "vendor": fake.company(),
        "status": random.choice(["open", "closed", "pending"]),
    } for i in range(1, 101)])

    # Misnamed: table called "inventory" but columns are clearly finance/GL.
    inventory = pd.DataFrame([{
        "account_number": fake.bothify("ACCT-######"),
        "gl_code": fake.bothify("###.##"),
        "transaction_amount": round(random.uniform(10, 100000), 2),
        "fiscal_period": random.choice(["2024Q1", "2024Q2", "2024Q3", "2024Q4"]),
    } for i in range(1, 101)])

    write_pandas(retail_orders, schema, "retail_orders")
    write_pandas(clinical_labs, schema, "clinical_labs")
    write_pandas(gl_transactions, schema, "gl_transactions")
    write_pandas(service_requests, schema, "service_requests")
    write_pandas(inventory, schema, "inventory")

    gold_domain.append((schema, "retail_orders", "Retail", "Sales", "clear retail"))
    gold_domain.append((schema, "clinical_labs", "Healthcare", "Clinical", "clear healthcare"))
    gold_domain.append((schema, "gl_transactions", "Finance", "Accounting", "clear finance/GL"))
    gold_domain.append((schema, "service_requests", "AMBIGUOUS", "", "mixed patient+invoice; expect lower confidence / recommended_domain"))
    gold_domain.append((schema, "inventory", "Finance", "Accounting", "MISNAMED: columns are GL/finance, not inventory -> classifier should trust columns"))

    readme(schema, "capability", "Domain+subdomain classification, confidence penalties, misnamed-table handling")
    readme(schema, "expect", "retail_orders=Retail, clinical_labs=Healthcare, gl_transactions=Finance (high confidence)")
    readme(schema, "ambiguous", "service_requests mixes domains -> lower confidence and/or recommended_domain populated")
    readme(schema, "misnamed", "inventory has GL columns -> should classify Finance from columns, not the table name")


if "uat_domains" not in skip_scenarios:
    build_domains()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 7 — `uat_types` (Databricks data-type coverage)
# MAGIC
# MAGIC Built via SQL DDL + INSERT (not createDataFrame) so VARIANT / complex / BINARY /
# MAGIC TIMESTAMP_NTZ types are exact. Exercises the special-cased read path in
# MAGIC `processing.read_table_with_type_conversion` (BINARY->base64, VARIANT->to_json,
# MAGIC TIMESTAMP*->string) and non-orderable profiling.

# COMMAND ----------


def build_types():
    schema = "uat_types"
    drop_schema(schema)
    ensure_schema(schema)
    t = f"{catalog_name}.{schema}.type_coverage"

    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"""
        CREATE TABLE {t} (
            pk_id            BIGINT,
            business_key     STRING,
            category         VARCHAR(50),
            is_active        BOOLEAN,
            small_count      INT,
            float_rate       FLOAT,
            double_metric    DOUBLE,
            decimal_amount   DECIMAL(18,2),
            event_date       DATE,
            event_ts         TIMESTAMP,
            event_ts_ntz     TIMESTAMP_NTZ,
            file_data        BINARY,
            json_payload     VARIANT,
            tags             ARRAY<STRING>,
            attributes       MAP<STRING, STRING>,
            nested_record    STRUCT<field1: STRING, field2: INT, field3: DECIMAL(10,2)>
        ) USING DELTA
    """)

    # Three literal rows exercising each type (incl. a NULL row for null handling).
    spark.sql(f"""
        INSERT INTO {t} VALUES
        (1, 'BK-001', 'alpha', true, 10, CAST(1.5 AS FLOAT), 2.71828, 1234.56,
         DATE'2024-03-15', TIMESTAMP'2024-03-15 08:30:00', TIMESTAMP_NTZ'2024-03-15 08:30:00',
         CAST('binblob-1' AS BINARY), PARSE_JSON('{{"a": 1, "nested": {{"b": [1,2,3]}}}}'),
         ARRAY('red', 'green'), MAP('k1', 'v1', 'k2', 'v2'),
         NAMED_STRUCT('field1', 'x', 'field2', 7, 'field3', CAST(9.99 AS DECIMAL(10,2)))),
        (2, 'BK-002', 'beta', false, 20, CAST(3.14 AS FLOAT), 1.41421, 9999.99,
         DATE'2024-06-01', TIMESTAMP'2024-06-01 12:00:00', TIMESTAMP_NTZ'2024-06-01 12:00:00',
         CAST('binblob-2' AS BINARY), PARSE_JSON('{{"a": 2, "list": ["x","y"]}}'),
         ARRAY('blue'), MAP('k1', 'v9'),
         NAMED_STRUCT('field1', 'y', 'field2', 8, 'field3', CAST(0.01 AS DECIMAL(10,2)))),
        (3, 'BK-003', NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """)
    print(f"  {schema}.type_coverage: 3 rows, 16 typed cols (incl. BINARY/VARIANT/complex + NULL row)")

    gold_domain.append((schema, "type_coverage", "ANY", "", "type-coverage sanity table (no strong domain)"))
    readme(schema, "capability", "Data-type coverage: BINARY->base64, VARIANT->JSON, TIMESTAMP*->string, ARRAY/MAP/STRUCT, DECIMAL, NULLs")
    readme(schema, "expect", "Generation succeeds with no read/type errors; complex columns get sensible comments")
    readme(schema, "expect", "Profiling handles non-orderable types (ARRAY/MAP/STRUCT/VARIANT) without SQL errors")


if "uat_types" not in skip_scenarios:
    build_types()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 8 — `uat_ddl_edges` (DDL/comment + sampling edge cases)

# COMMAND ----------


def build_ddl_edges():
    schema = "uat_ddl_edges"
    drop_schema(schema)
    ensure_schema(schema)

    # (1) Comment/data characters that break naive DDL regeneration:
    #     apostrophes, double quotes, semicolons, unicode/emoji. Pre-existing comments
    #     included so the review/apply path must round-trip them.
    t_chars = f"{catalog_name}.{schema}.tricky_chars"
    spark.sql(f"DROP TABLE IF EXISTS {t_chars}")
    spark.sql(f"""
        CREATE TABLE {t_chars} (
            id INT COMMENT 'it''s the primary key; do not drop',
            note STRING COMMENT 'has "double quotes" and a semicolon; and emoji 🚀',
            label STRING
        ) USING DELTA COMMENT 'table with it''s tricky punctuation; and "quotes"'
    """)
    spark.sql(f"""
        INSERT INTO {t_chars} VALUES
        (1, 'value with ; semicolon', 'it''s fine'),
        (2, 'quote " inside', 'emoji 🚀 ok'),
        (3, 'plain', 'plain')
    """)

    # (2) Wide table: > 20 columns to force chunk_df chunking (columns_per_call default 20).
    wide_cols = ", ".join(f"col_{i:02d} STRING" for i in range(1, 31))
    t_wide = f"{catalog_name}.{schema}.wide_table"
    spark.sql(f"DROP TABLE IF EXISTS {t_wide}")
    spark.sql(f"CREATE TABLE {t_wide} ({wide_cols}) USING DELTA")
    vals = ", ".join(f"'v{i:02d}'" for i in range(1, 31))
    spark.sql(f"INSERT INTO {t_wide} VALUES ({vals})")

    # (3) Empty table (0 rows) and (4) single-row table -> sampling edges.
    t_empty = f"{catalog_name}.{schema}.empty_table"
    spark.sql(f"DROP TABLE IF EXISTS {t_empty}")
    spark.sql(f"CREATE TABLE {t_empty} (id INT, name STRING) USING DELTA")

    t_single = f"{catalog_name}.{schema}.single_row_table"
    spark.sql(f"DROP TABLE IF EXISTS {t_single}")
    spark.sql(f"CREATE TABLE {t_single} (id INT, name STRING) USING DELTA")
    spark.sql(f"INSERT INTO {t_single} VALUES (1, 'only row')")

    print(f"  {schema}: tricky_chars(3), wide_table(1x30col), empty_table(0), single_row_table(1)")

    gold_domain.append((schema, "tricky_chars", "ANY", "", "DDL round-trip edge case"))
    readme(schema, "capability", "DDL/comment round-trip + sampling edges")
    readme(schema, "expect", "tricky_chars: apostrophes/quotes/semicolons/emoji survive generation + apply-DDL")
    readme(schema, "expect", "wide_table (30 cols): chunk_df splits into >1 chunk; all columns get comments")
    readme(schema, "expect", "empty_table (0 rows) and single_row_table (1 row) generate without sampling errors")


if "uat_ddl_edges" not in skip_scenarios:
    build_ddl_edges()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `_scenario_readme` + gold expectation tables

# COMMAND ----------

import pandas as pd

# Per-schema _scenario_readme tables.
for schema, rows in scenario_readmes.items():
    ensure_schema(schema)
    pdf = pd.DataFrame(rows, columns=["topic", "detail"])
    spark.createDataFrame(pdf).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog_name}.{schema}._scenario_readme")
print(f"Wrote _scenario_readme to {len(scenario_readmes)} schemas")

# Gold expectations -> <catalog>.uat_eval (eval_expected_* shape, with a `scenario` column
# so the eval_e2e scoring harness can filter per schema).
ensure_schema(eval_schema)


def _write_gold(rows, cols, table):
    if not rows:
        return
    spark.createDataFrame(pd.DataFrame(rows, columns=cols)).write.mode(
        "overwrite"
    ).option("overwriteSchema", "true").saveAsTable(
        f"{catalog_name}.{eval_schema}.{table}"
    )
    print(f"  gold: {eval_schema}.{table} ({len(rows)} rows)")


_write_gold(
    gold_fk,
    ["scenario", "src_table", "src_column", "dst_table", "dst_column", "expected_is_fk"],
    "uat_expected_fk",
)
_write_gold(
    gold_pi,
    ["scenario", "table_name", "column_name", "expected_type"],
    "uat_expected_pi",
)
_write_gold(
    gold_domain,
    ["scenario", "table_name", "expected_domain", "expected_subdomain", "notes"],
    "uat_expected_domains",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

built = sorted(set(scenario_readmes.keys()) | {r[0] for r in gold_domain})
print("=" * 60)
print("  UAT SCENARIOS BUILT")
print("=" * 60)
for s in built:
    if s in skip_scenarios:
        continue
    tables = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{s}`").collect()
    n = len([t for t in tables if not t.tableName.startswith("_")])
    print(f"  {catalog_name}.{s}: {n} tables")
print("-" * 60)
print(f"  Gold expectations: {catalog_name}.{eval_schema}")
print(f"    uat_expected_fk:      {len(gold_fk)} rows")
print(f"    uat_expected_pi:      {len(gold_pi)} rows")
print(f"    uat_expected_domains: {len(gold_domain)} rows")
print("=" * 60)
print("Next: run metadata generation + analytics pipeline per scenario;")
print("verify with docs/UAT_SANITY_CHECK.md and docs/UAT_APP_WALKTHROUGH.md")

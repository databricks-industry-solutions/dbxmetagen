# Databricks notebook source
# MAGIC %md
# MAGIC # Challenging Life-Sciences EMR schema builder
# MAGIC
# MAGIC Builds ONE realistic, deliberately-messy healthcare schema from **Synthea**
# MAGIC synthetic patient data (Apache-2.0, synthetic -> no real PHI). This is not a
# MAGIC purpose-built failure zoo like `build_uat_scenarios.py`; it is meant to look
# MAGIC like a real customer's warehouse: a silver/raw layer with cryptic source-system
# MAGIC names, a gold mart layer, and a handful of realistic-but-poor modeling decisions
# MAGIC that stress metric-view generation, Genie, and PI detection at once.
# MAGIC
# MAGIC **Source:** Synthea sample CSVs (~108 patients, 278 providers, 5.5k encounters,
# MAGIC ~130k observations, ~113k claim transactions -- all Massachusetts). Downloaded at
# MAGIC runtime; nothing is committed to the repo.
# MAGIC
# MAGIC **Layers built into `<catalog>.<schema>` (default `challenging_ls`):**
# MAGIC
# MAGIC | Layer | Tables |
# MAGIC |-------|--------|
# MAGIC | silver / raw (cryptic, PII bait, no comments/FKs) | `raw_pat`, `raw_enc`, `raw_prov`, `raw_org`, `raw_obs`, `raw_cond`, `raw_med`, `raw_clm_txn` |
# MAGIC | gold / marts (clean star + persona filter cols) | `dim_patient`, `dim_provider`, `dim_organization`, `dim_date`, `fct_encounter` |
# MAGIC | messy traps (realistic-but-stupid) | `mart_activity_all`, `enc_summary`, `enc_summary_v2` |
# MAGIC
# MAGIC **Persona filtering (the point):** the gold layer carries TWO different filter
# MAGIC columns at TWO different grains, so different users filter on different columns:
# MAGIC - `territory` (city-cluster) -- a **sales/field rep** filters to their territory.
# MAGIC - `region` (MA region rolled up from county) -- a **director** filters to their region.
# MAGIC
# MAGIC **Messy traps (each documented in `_readme`):**
# MAGIC 1. `mart_activity_all` -- `UNION ALL` of DAILY encounter counts and MONTHLY cost
# MAGIC    rollups into one table: mixed-grain `period`, and an `amt` column that means a
# MAGIC    count on some rows and dollars on others.
# MAGIC 2. `enc_summary` -- silently `INNER JOIN`s encounters to provider, dropping rows
# MAGIC    with a null provider FK (row-loss trap).
# MAGIC 3. Ambiguous column names reused across tables: `val`, `amt`, `num`, `desc`.
# MAGIC 4. `enc_summary_v2` -- near-duplicate of `enc_summary` with one column renamed
# MAGIC    (`prov` -> `provider_name`) and one added (dedup / lineage trap).
# MAGIC
# MAGIC **PI bait:** the raw layer keeps Synthea's `ssn`/`drivers`/`passport`/name columns
# MAGIC (synthetic) so PI mode has something realistic to detect; gold excludes them.
# MAGIC
# MAGIC **Usage:** set `catalog_name`, Run All. Idempotent (drops + rebuilds the schema).

# COMMAND ----------

# MAGIC %pip install -qqqq requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Target Catalog (required)")
dbutils.widgets.text("schema_name", "challenging_ls", "Target Schema")
dbutils.widgets.text(
    "synthea_url",
    "https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip",
    "Synthea sample CSV zip URL",
)

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip() or "challenging_ls"
synthea_url = dbutils.widgets.get("synthea_url").strip()
assert catalog_name, "catalog_name is required"

print(f"Target: {catalog_name}.{schema_name}")
print(f"Source: {synthea_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download + stage Synthea CSVs

# COMMAND ----------

import io
import os
import zipfile

import requests
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Stage the CSVs into a UC Volume. On serverless / shared UC compute the driver's
# local /tmp and DBFS root are NOT writable, but a Volume path (/Volumes/...) is a
# real governed mount readable by executors. Create the schema + a staging volume,
# then extract the zip straight onto the volume.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`_synthea_staging`")
_vol_dir = f"/Volumes/{catalog_name}/{schema_name}/_synthea_staging"
os.makedirs(_vol_dir, exist_ok=True)

print(f"Downloading Synthea sample data -> {_vol_dir} ...")
resp = requests.get(synthea_url, timeout=300)
resp.raise_for_status()
with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
    zf.extractall(_vol_dir)
_csvs = sorted(f for f in os.listdir(_vol_dir) if f.endswith(".csv"))
print(f"Extracted {len(_csvs)} CSVs: {', '.join(_csvs)}")


def read_csv(name: str):
    """Read a Synthea CSV (header, inferred schema) from the staging volume."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"{_vol_dir}/{name}.csv")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def fq(table: str) -> str:
    return f"`{catalog_name}`.`{schema_name}`.`{table}`"


def write(df, table: str, comment: str = "") -> None:
    """Write a managed Delta table. Comments left empty on RAW (so dbxmetagen has
    something to fill); a light table-level comment is allowed on gold."""
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog_name}.{schema_name}.{table}")
    )
    n = spark.table(fq(table)).count()
    ncol = len(spark.table(fq(table)).columns)
    print(f"  {table}: {n} rows, {ncol} cols")


_readmes: list[tuple[str, str]] = []


def readme(topic: str, detail: str) -> None:
    _readmes.append((topic, detail))


# Massachusetts county -> region rollup (director-grain filter column). Deterministic.
_COUNTY_REGION = {
    "Suffolk County": "Greater Boston",
    "Middlesex County": "Greater Boston",
    "Norfolk County": "Greater Boston",
    "Essex County": "Northeast",
    "Worcester County": "Central MA",
    "Hampden County": "Western MA",
    "Hampshire County": "Western MA",
    "Franklin County": "Western MA",
    "Berkshire County": "Western MA",
    "Bristol County": "Southeast",
    "Plymouth County": "Southeast",
    "Barnstable County": "Cape & Islands",
    "Dukes County": "Cape & Islands",
    "Nantucket County": "Cape & Islands",
}


def region_expr(county_col: str):
    """CASE expression mapping a county column to an MA region; else 'Other'."""
    e = F
    col = F.col(county_col)
    expr = F.when(col.isNull(), F.lit("Unknown"))
    for county, region in _COUNTY_REGION.items():
        expr = expr.when(col == F.lit(county), F.lit(region))
    return expr.otherwise(F.lit("Other"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset built tables (idempotent)
# MAGIC
# MAGIC Drops the tables this builder creates (not the whole schema -- the staging
# MAGIC volume populated above lives in this schema and must survive until the build
# MAGIC reads from it). `write()` uses overwrite anyway; this just clears any tables
# MAGIC removed in a later version so re-runs stay clean.

# COMMAND ----------

_BUILT_TABLES = [
    "raw_pat", "raw_prov", "raw_org", "raw_enc", "raw_obs", "raw_cond", "raw_med",
    "raw_clm_txn", "dim_patient", "dim_provider", "dim_organization", "dim_date",
    "fct_encounter", "mart_activity_all", "enc_summary", "enc_summary_v2", "_readme",
]
for _t in _BUILT_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {fq(_t)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver / raw layer (cryptic names, PII bait, no comments/FKs)

# COMMAND ----------

# raw_pat: keep Synthea's identifier columns (synthetic) as PI-detection bait.
pat = read_csv("patients")
raw_pat = pat.selectExpr(
    "Id AS pat_id", "BIRTHDATE AS dob", "DEATHDATE AS dod", "SSN AS ssn",
    "DRIVERS AS drivers_lic", "PASSPORT AS passport", "FIRST AS fname",
    "LAST AS lname", "MARITAL AS marital_cd", "RACE AS race", "ETHNICITY AS ethnicity",
    "GENDER AS sex", "ADDRESS AS addr", "CITY AS city", "STATE AS st",
    "COUNTY AS cnty", "ZIP AS zip", "HEALTHCARE_EXPENSES AS val", "INCOME AS num",
)
write(raw_pat, "raw_pat")
readme("raw_pat", "Silver: patients as loaded from source. Cryptic names; keeps "
       "synthetic SSN/drivers_lic/passport/name as PI-detection bait. `val`=lifetime "
       "healthcare expense, `num`=income (ambiguously named on purpose).")

prov = read_csv("providers")
raw_prov = prov.selectExpr(
    "Id AS prov_id", "ORGANIZATION AS org_id", "NAME AS nm", "GENDER AS sex",
    "SPECIALITY AS spec", "ADDRESS AS addr", "CITY AS city", "STATE AS st",
    "ZIP AS zip", "ENCOUNTERS AS num",
)
write(raw_prov, "raw_prov")
readme("raw_prov", "Silver: providers. `nm`=provider name, `num`=lifetime encounter "
       "count (ambiguous).")

org = read_csv("organizations")
raw_org = org.selectExpr(
    "Id AS org_id", "NAME AS nm", "ADDRESS AS addr", "CITY AS city",
    "STATE AS st", "ZIP AS zip", "REVENUE AS val", "UTILIZATION AS num",
)
write(raw_org, "raw_org")
readme("raw_org", "Silver: organizations/facilities. `val`=revenue, `num`=utilization.")

enc = read_csv("encounters")
raw_enc = enc.selectExpr(
    "Id AS enc_id", "START AS start_ts", "STOP AS stop_ts", "PATIENT AS pat_id",
    "ORGANIZATION AS org_id", "PROVIDER AS prov_id", "PAYER AS payer_id",
    "ENCOUNTERCLASS AS enc_cls", "CODE AS code", "DESCRIPTION AS `desc`",
    "BASE_ENCOUNTER_COST AS base_amt", "TOTAL_CLAIM_COST AS amt",
    "PAYER_COVERAGE AS cov_amt", "REASONDESCRIPTION AS reason_desc",
)
# Realistic mess: null out the provider FK on a slice of encounters (unassigned /
# system-generated / backfilled encounters that never got a provider). Deterministic
# via a hash bucket (~8%). This is what makes the enc_summary INNER-JOIN row-loss trap
# actually lose rows -- the raw Synthea sample happens to have a provider on every row.
raw_enc = raw_enc.withColumn(
    "prov_id",
    F.when(F.abs(F.hash(F.col("enc_id"))) % F.lit(12) == F.lit(0), F.lit(None))
    .otherwise(F.col("prov_id")),
)
write(raw_enc, "raw_enc")
readme("raw_enc", "Silver: encounters. `amt`=total claim cost, `base_amt`=base cost, "
       "`desc`=encounter description, `enc_cls`=class. ~8% of rows have a NULL "
       "prov_id (unassigned encounters) -- realistic, and the reason enc_summary's "
       "INNER JOIN loses rows.")

obs = read_csv("observations")
raw_obs = obs.selectExpr(
    "PATIENT AS pat_id", "ENCOUNTER AS enc_id", "DATE AS obs_ts", "CODE AS code",
    "DESCRIPTION AS `desc`", "VALUE AS val", "UNITS AS units", "TYPE AS obs_type",
)
write(raw_obs, "raw_obs")
readme("raw_obs", "Silver: observations (labs/vitals). `val` is a STRING (numeric AND "
       "text results mixed), `desc`=observation name.")

cond = read_csv("conditions")
raw_cond = cond.selectExpr(
    "PATIENT AS pat_id", "ENCOUNTER AS enc_id", "START AS start_dt", "STOP AS stop_dt",
    "CODE AS code", "DESCRIPTION AS `desc`",
)
write(raw_cond, "raw_cond")
readme("raw_cond", "Silver: conditions/diagnoses. `desc`=condition name.")

med = read_csv("medications")
raw_med = med.selectExpr(
    "PATIENT AS pat_id", "ENCOUNTER AS enc_id", "START AS start_ts", "STOP AS stop_ts",
    "CODE AS code", "DESCRIPTION AS `desc`", "BASE_COST AS base_amt",
    "TOTALCOST AS amt", "DISPENSES AS num",
)
write(raw_med, "raw_med")
readme("raw_med", "Silver: medications. `amt`=total cost, `num`=dispenses, `desc`=drug.")

# claims_transactions is the big one (~113k rows) -- keep a realistic subset of cols.
clm = read_csv("claims_transactions")
_clm_cols = [c for c in ("ID", "CLAIMID", "PATIENTID", "TYPE", "AMOUNT", "PAYMENTS",
                         "OUTSTANDING", "FROMDATE", "PLACEOFSERVICE", "PROCEDURECODE")
             if c in clm.columns]
raw_clm_txn = clm.select(*[F.col(c) for c in _clm_cols])
write(raw_clm_txn, "raw_clm_txn")
readme("raw_clm_txn", "Silver: claim transactions (largest table). Loaded near-verbatim "
       "from source with original SCREAMING_CASE column names.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold / mart layer (clean star + persona filter columns)

# COMMAND ----------

# dim_patient: no direct identifiers; carry county + region (director filter grain).
dim_patient = (
    spark.table(fq("raw_pat"))
    .select(
        F.col("pat_id").alias("patient_key"),
        F.year(F.col("dob")).alias("birth_year"),
        F.col("sex"),
        F.col("race"),
        F.col("ethnicity"),
        F.col("city"),
        F.col("cnty").alias("county"),
        region_expr("cnty").alias("region"),
        F.col("zip"),
    )
)
write(dim_patient, "dim_patient", "Patient dimension (de-identified).")
readme("dim_patient", "Gold: de-identified patient dim. `region` = MA region rolled up "
       "from county -> the DIRECTOR persona filters on this.")

# City -> territory (sales/field-rep grain). A territory groups nearby cities; here we
# derive it deterministically from the first letter bucket of the city so reps have a
# stable, smaller-than-region slice. (Realistic stand-in for a sales territory map.)
_territory_expr = F.concat(F.lit("T-"), F.substring(F.upper(F.col("city")), 1, 1))

dim_provider = (
    spark.table(fq("raw_prov"))
    .select(
        F.col("prov_id").alias("provider_key"),
        F.col("org_id").alias("organization_key"),
        F.col("nm").alias("provider_name"),
        F.col("spec").alias("specialty"),
        F.col("city"),
        _territory_expr.alias("territory"),
    )
)
write(dim_provider, "dim_provider", "Provider dimension with sales territory.")
readme("dim_provider", "Gold: provider dim. `territory` = city-cluster -> the SALES/FIELD "
       "REP persona filters on this (different column + finer grain than region).")

dim_organization = (
    spark.table(fq("raw_org"))
    .select(
        F.col("org_id").alias("organization_key"),
        F.col("nm").alias("organization_name"),
        F.col("city"),
        F.col("val").cast("double").alias("annual_revenue"),
        F.col("num").cast("double").alias("utilization"),
    )
)
write(dim_organization, "dim_organization", "Facility/organization dimension.")

# fct_encounter: the central fact. Joins to patient (for region) and provider (for
# territory), so BOTH persona filter columns are available on the grain a user queries.
enc_base = spark.table(fq("raw_enc"))
dp = spark.table(fq("dim_patient")).select("patient_key", "region", "county")
dpr = spark.table(fq("dim_provider")).select(
    F.col("provider_key"), F.col("territory"), F.col("specialty")
)
fct_encounter = (
    enc_base.alias("e")
    .join(dp.alias("p"), F.col("e.pat_id") == F.col("p.patient_key"), "left")
    .join(dpr.alias("pr"), F.col("e.prov_id") == F.col("pr.provider_key"), "left")
    .select(
        F.col("e.enc_id").alias("encounter_key"),
        F.col("e.pat_id").alias("patient_key"),
        F.col("e.prov_id").alias("provider_key"),
        F.col("e.org_id").alias("organization_key"),
        F.to_date(F.col("e.start_ts")).alias("encounter_date"),
        F.col("e.enc_cls").alias("encounter_class"),
        F.col("e.desc").alias("encounter_desc"),
        F.col("e.amt").cast("double").alias("total_claim_cost"),
        F.col("e.base_amt").cast("double").alias("base_cost"),
        F.col("e.cov_amt").cast("double").alias("payer_coverage"),
        F.col("pr.territory").alias("territory"),   # sales-rep filter grain
        F.col("p.region").alias("region"),          # director filter grain
        F.col("p.county").alias("county"),
        F.col("pr.specialty").alias("provider_specialty"),
    )
)
write(fct_encounter, "fct_encounter",
      "Encounter fact. Carries BOTH `territory` (sales-rep filter) and `region` "
      "(director filter) so different personas filter on different columns.")
readme("fct_encounter", "Gold: central encounter fact. Query this filtered by "
       "`territory` (rep) OR `region` (director). Measures: total_claim_cost, "
       "base_cost, payer_coverage; count of encounters.")

# dim_date derived from the encounter date range.
dim_date = (
    fct_encounter.select(F.col("encounter_date").alias("date_key"))
    .where(F.col("encounter_date").isNotNull())
    .distinct()
    .select(
        "date_key",
        F.year("date_key").alias("year"),
        F.quarter("date_key").alias("quarter"),
        F.month("date_key").alias("month"),
        F.date_format("date_key", "yyyy-MM").alias("year_month"),
    )
)
write(dim_date, "dim_date", "Date dimension over the encounter range.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Messy traps (realistic-but-stupid modeling)

# COMMAND ----------

# TRAP 1: mart_activity_all -- UNION ALL of DAILY encounter counts and MONTHLY cost
# rollups. Mixed grain in one table; `amt` means a COUNT on daily rows and DOLLARS on
# monthly rows. `period` is a date on some rows, a year-month string on others.
daily = (
    spark.table(fq("fct_encounter"))
    .where(F.col("encounter_date").isNotNull())
    .groupBy(F.col("encounter_date").cast("string").alias("period"), F.col("region"))
    .agg(F.count(F.lit(1)).cast("double").alias("amt"))
    .select("period", "region", F.lit("daily_encounter_count").alias("grain"), "amt")
)
monthly = (
    spark.table(fq("fct_encounter"))
    .where(F.col("encounter_date").isNotNull())
    .groupBy(F.date_format("encounter_date", "yyyy-MM").alias("period"), F.col("region"))
    .agg(F.sum("total_claim_cost").alias("amt"))
    .select("period", "region", F.lit("monthly_cost_usd").alias("grain"), "amt")
)
write(daily.unionByName(monthly), "mart_activity_all")
readme("mart_activity_all", "TRAP: UNION ALL of DAILY encounter COUNTS and MONTHLY "
       "cost DOLLARS. `amt` and `period` mean different things per `grain`. A metric "
       "view that SUM(amt) here is meaningless -- the tool should flag the mixed grain.")

# TRAP 2: enc_summary -- silent INNER JOIN drops encounters with a null provider FK.
enc_summary = (
    spark.table(fq("raw_enc")).alias("e")
    .join(spark.table(fq("raw_prov")).alias("p"), F.col("e.prov_id") == F.col("p.prov_id"), "inner")
    .groupBy(F.col("p.nm").alias("prov"), F.col("p.spec").alias("spec"))
    .agg(
        F.count(F.lit(1)).alias("num"),
        F.sum(F.col("e.amt").cast("double")).alias("amt"),
    )
)
write(enc_summary, "enc_summary")
_total_enc = spark.table(fq("raw_enc")).count()
_kept_enc = (
    spark.table(fq("raw_enc")).alias("e")
    .join(spark.table(fq("raw_prov")).alias("p"), F.col("e.prov_id") == F.col("p.prov_id"), "inner")
    .count()
)
readme("enc_summary", f"TRAP: INNER JOIN silently drops encounters with null/unknown "
       f"provider ({_total_enc - _kept_enc} of {_total_enc} rows lost). `num`=encounter "
       f"count, `amt`=summed cost (ambiguous names). Totals here won't reconcile with "
       f"fct_encounter.")

# TRAP 3 is column naming (val/amt/num/desc reused everywhere) -- documented above.

# TRAP 4: enc_summary_v2 -- near-duplicate of enc_summary, one column renamed + one added.
enc_summary_v2 = (
    spark.table(fq("enc_summary"))
    .withColumnRenamed("prov", "provider_name")
    .withColumn("avg_amt", F.col("amt") / F.col("num"))
)
write(enc_summary_v2, "enc_summary_v2")
readme("enc_summary_v2", "TRAP: near-duplicate of enc_summary (`prov`->`provider_name`, "
       "adds `avg_amt`). Lineage/dedup trap -- two tables that look like the same thing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Readme table

# COMMAND ----------

readme_df = spark.createDataFrame(_readmes, schema="table_or_topic STRING, note STRING")
write(readme_df, "_readme")

print("\nBuilt schema:", f"{catalog_name}.{schema_name}")
for t, n in _readmes:
    print(f"  - {t}: {n[:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup temp staging volume

# COMMAND ----------

spark.sql(f"DROP VOLUME IF EXISTS `{catalog_name}`.`{schema_name}`.`_synthea_staging`")
print("Done.")

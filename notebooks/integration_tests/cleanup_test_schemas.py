# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup: leaked integration-test schemas
# MAGIC
# MAGIC The ETL tests each create a timestamped schema (`kb_test_<ts>`, `fk_test_<ts>`, ...)
# MAGIC and drop it in their last cell, so a test that fails part-way leaves its schema
# MAGIC behind. This runs as the job's final task (`run_if: ALL_DONE`) and drops a schema
# MAGIC only when ALL of these hold:
# MAGIC
# MAGIC - its name matches one of those tests' exact `<prefix>_YYYYMMDD_HHMMSS` patterns
# MAGIC - it was created by the identity running this job (never anyone else's)
# MAGIC - it was created after this run started (so its tests are finished; the job
# MAGIC   allows one run at a time), or more than `max_age_hours` ago
# MAGIC
# MAGIC `dry_run=true` only lists what it would drop.

# COMMAND ----------

import re

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("run_started_at", "", "This run's start (ISO-8601 UTC; empty = age rule only)")
dbutils.widgets.text("max_age_hours", "24", "Also drop matching schemas older than this")
dbutils.widgets.text("dry_run", "false", "List only")

catalog_name = dbutils.widgets.get("catalog_name").strip()
run_started_at = dbutils.widgets.get("run_started_at").strip()
max_age_hours = int(dbutils.widgets.get("max_age_hours").strip() or "24")
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

# Prefixes of the per-run schemas the integration notebooks create. Keep in sync
# when adding a test that creates its own timestamped schema.
PREFIXES = (
    "kb_test", "graph_test", "profiling_test", "ontology_test", "graph_validation",
    "domain_test", "fk_test", "sl_test", "vi_test", "dq_test", "sv_test", "ckb_test",
)
NAME_RE = re.compile(r"^(?:" + "|".join(PREFIXES) + r")_\d{8}_\d{6}(?:_[0-9a-f]{6})?$")

assert re.fullmatch(r"[A-Za-z0-9_]+", catalog_name), f"unexpected catalog name {catalog_name!r}"
assert max_age_hours >= 1, "max_age_hours must be at least 1"

# COMMAND ----------

age_rule = f"created < current_timestamp() - INTERVAL {max_age_hours} HOURS"
if run_started_at:
    # Validate the job-provided value before inlining it.
    assert re.fullmatch(r"[0-9T:.\-+Z ]+", run_started_at), f"unexpected run_started_at {run_started_at!r}"
    when = f"(created >= to_timestamp('{run_started_at}') OR {age_rule})"
else:
    when = age_rule

candidates = spark.sql(f"""
    SELECT schema_name, created, created_by
    FROM `{catalog_name}`.information_schema.schemata
    WHERE created_by = current_user() AND {when}
""").collect()

to_drop = sorted(r["schema_name"] for r in candidates if NAME_RE.match(r["schema_name"]))
print(f"{len(to_drop)} leaked test schema(s) in {catalog_name}" + (" (dry run)" if dry_run else ""))

failed = []
for name in to_drop:
    print(f"  {'would drop' if dry_run else 'dropping'} {catalog_name}.{name}")
    if dry_run:
        continue
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{name}` CASCADE")
    except Exception as e:  # keep going; report at the end
        failed.append((name, str(e)[:200]))

if failed:
    raise RuntimeError(f"could not drop {len(failed)} schema(s): {failed}")

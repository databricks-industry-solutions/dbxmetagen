# Databricks notebook source
# MAGIC %md
# MAGIC # FK prediction eval against UAT gold
# MAGIC
# MAGIC Scores `fk_predictions` for each UAT scenario schema against the gold rows the
# MAGIC scenario builder (`tests/data/build_uat_scenarios.py`) writes to
# MAGIC `<catalog>.<eval_schema>.uat_expected_fk`:
# MAGIC `expected_is_fk = true` rows are real FKs, `false` rows are traps that must not be predicted.
# MAGIC
# MAGIC Matching is undirected; direction is reported separately. Rows stored with
# MAGIC `is_fk = false` but confidence >= `display_threshold` are reported too, because
# MAGIC confidence-ranked readers (ERD recommender, review UI) still surface them.
# MAGIC
# MAGIC Per-scenario and pooled results are appended to `<results_schema>.fk_eval_results`
# MAGIC with a `run_label`, so a baseline and a candidate can be compared over time.
# MAGIC
# MAGIC Prerequisite: each scenario was processed by the metadata-generation job and then the
# MAGIC full analytics pipeline writing to `results_schema`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog holding the UAT scenario schemas")
dbutils.widgets.text("eval_schema", "uat_eval", "Schema holding uat_expected_fk")
dbutils.widgets.text("results_schema", "", "catalog.schema holding fk_predictions (pipeline output)")
dbutils.widgets.text(
    "scenarios",
    "uat_star_retail,uat_snowflake_health,uat_fk_hard,uat_fk_suffixless,uat_fk_patterns",
    "Scenario schemas to score (comma-separated)",
)
dbutils.widgets.text("display_threshold", "0.7", "Confidence at which an is_fk=false row still surfaces")
dbutils.widgets.text("run_label", "", "Label for this run (e.g. baseline, data_driven)")
dbutils.widgets.text("write_results", "true", "Append results to fk_eval_results (true/false)")

catalog_name = dbutils.widgets.get("catalog_name").strip()
eval_schema = dbutils.widgets.get("eval_schema").strip()
results_schema = dbutils.widgets.get("results_schema").strip()
scenarios = [s.strip() for s in dbutils.widgets.get("scenarios").split(",") if s.strip()]
display_threshold = float(dbutils.widgets.get("display_threshold").strip() or "0.7")
run_label = dbutils.widgets.get("run_label").strip()
write_results = dbutils.widgets.get("write_results").strip().lower() == "true"

assert catalog_name, "catalog_name is required"
assert results_schema and results_schema.count(".") == 1, "results_schema must be catalog.schema"

# COMMAND ----------

from datetime import datetime, timezone

from dbxmetagen.fk_eval import format_report, pooled, score_fk

gold_tbl = f"`{catalog_name}`.`{eval_schema}`.uat_expected_fk"
pred_tbl = f"{results_schema}.fk_predictions"


def _lit(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


scores = []
for sc in scenarios:
    gold = [r.asDict() for r in spark.sql(
        f"SELECT * FROM {gold_tbl} WHERE scenario = {_lit(sc)}"
    ).collect()]
    if not gold:
        print(f"SKIP {sc}: no gold rows in {gold_tbl}")
        continue
    prefix = f"{catalog_name}.{sc}."
    preds = [r.asDict() for r in spark.sql(f"""
        SELECT src_table, src_column, dst_table, dst_column, is_fk, final_confidence
        FROM {pred_tbl}
        WHERE src_table LIKE {_lit(prefix + '%')} AND dst_table LIKE {_lit(prefix + '%')}
    """).collect()]
    scores.append(score_fk(sc, gold, preds, display_threshold=display_threshold))

assert scores, "nothing scored: check catalog_name, eval_schema and scenarios"
print(format_report(scores))

# COMMAND ----------

if write_results:
    now = datetime.now(timezone.utc)
    rows = [dict(s.as_row(), run_label=run_label, results_schema=results_schema, run_ts=now)
            for s in scores + [pooled(scores)]]
    schema = ("scenario STRING, gold INT, predicted INT, tp INT, fn INT, fp INT, trap_fp INT, "
              "reversed INT, high_conf_non_fk INT, precision DOUBLE, recall DOUBLE, "
              "direction_accuracy DOUBLE, run_label STRING, results_schema STRING, run_ts TIMESTAMP")
    spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(
        f"{results_schema}.fk_eval_results"
    )
    print(f"Appended {len(rows)} rows to {results_schema}.fk_eval_results (run_label={run_label!r})")

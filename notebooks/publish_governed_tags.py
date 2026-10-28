# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Governed Domain Tags (UCG-3 / UCG-4)
# MAGIC
# MAGIC Opt-in, downstream-reader step. Reads already-persisted domain/subdomain predictions
# MAGIC (`table_knowledge_base`, else `metadata_generation_log`) and publishes **one native UC
# MAGIC governed tag** per table whose value slash-encodes the subdomain (e.g. `banking/retail_banking`),
# MAGIC so tables join UC **Domains** / the semantic layer.
# MAGIC
# MAGIC **Safe by default:** no-op unless `enable_governed_tags=true` AND `governed_domain_tag_name`
# MAGIC is set AND the governed tag + `ASSIGN`/`APPLY TAG` privilege exist. Never edits the ungoverned
# MAGIC `domain`/`subdomain` tags (those stay the default, written by metadata generation).

# COMMAND ----------

# MAGIC # Uncomment when running outside a DAB-deployed job:
# MAGIC # %pip install /Workspace/Users/<you>/.bundle/dbxmetagen/<target>/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated; blank = whole schema)")
dbutils.widgets.text("enable_governed_tags", "false", "Enable governed tags (true/false)")
dbutils.widgets.text("governed_domain_tag_name", "", "Governed domain tag KEY (blank = no-op)")
dbutils.widgets.text("governed_domain_separator", "/", "Subdomain separator in the value")
dbutils.widgets.text("governed_tag_max_len", "256", "Max governed-tag value length")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_names = dbutils.widgets.get("table_names")

# COMMAND ----------

import sys
sys.path.append("../src")  # git-clone / DAB layout; wheel is also on the environment

from types import SimpleNamespace
from dbxmetagen import governed_tags

# Lightweight config from widgets (this standalone job only needs the governed-tag knobs).
config = SimpleNamespace(
    catalog_name=catalog_name,
    schema_name=schema_name,
    table_names=table_names,
    enable_governed_tags=str(dbutils.widgets.get("enable_governed_tags")).strip().lower() == "true",
    governed_domain_tag_name=dbutils.widgets.get("governed_domain_tag_name").strip(),
    governed_domain_separator=dbutils.widgets.get("governed_domain_separator") or "/",
    governed_tag_max_len=int(dbutils.widgets.get("governed_tag_max_len") or 256),
)

run_id = ""
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    pass

# COMMAND ----------

# Injected runners: query returns list[dict]; run_sql executes DDL/INSERT. Shared with the app
# (which passes its warehouse execute_sql) via governed_tags.run_sync.
def query_fn(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]

def run_sql(sql):
    return spark.sql(sql)

summary = governed_tags.run_sync(query_fn, run_sql, config, run_id=run_id)
print("Governed tag sync summary:", summary)

if summary.get("status") == "noop":
    print(f"No-op: {summary.get('reason')}. (Set enable_governed_tags=true and governed_domain_tag_name "
          f"to the verified UC Domains tag key to enable.)")
elif summary.get("capability_missing"):
    print("Governed tag / ASSIGN privilege appears absent — nothing applied. "
          + governed_tags.GOVERNED_TAG_HINT)

# COMMAND ----------

dbutils.notebook.exit(str(summary))

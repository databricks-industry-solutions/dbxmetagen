# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys

sys.path.append("../")
import os

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.processing import split_table_names
from src.dbxmetagen.user_utils import sanitize_email
from src.dbxmetagen.error_handling import exponential_backoff
from src.dbxmetagen.ddl_regenerator import (
    process_metadata_file,
    load_metadata_file,
    replace_comment_in_ddl,
    replace_pii_tags_in_ddl,
)

current_user = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)
os.environ["DATABRICKS_TOKEN"] = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
)
dbutils.widgets.text("reviewed_file_name", "")
dbutils.widgets.text("mode", "comment")
dbutils.widgets.text("current_user_override", "")
file_name = dbutils.widgets.get("reviewed_file_name")
mode = dbutils.widgets.get("mode")
current_user_override = dbutils.widgets.get("current_user_override")

# Use override if provided, otherwise use the detected current user
if current_user_override and current_user_override.strip():
    current_user = current_user_override.strip()
    print(f"Using current_user override: {current_user}")
else:
    print(f"Using detected current_user: {current_user}")

review_variables = {
    "reviewed_file_name": file_name,
    "current_user": current_user,
    "mode": mode,
}

# COMMAND ----------


def main(kwargs, input_file):
    # Get Spark version in a serverless-compatible way
    try:
        # Try traditional approach first (works on regular clusters)
        spark_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
        print(f"Using traditional spark version: {spark_version}")
    except Exception as e:
        # Fallback for serverless compute
        print(
            f"Traditional spark version lookup failed (likely serverless): {str(e)[:100]}..."
        )
        # On serverless, Excel is not supported due to networking restrictions
        # Use version string without "ml" to trigger Excel blocking in compatibility check
        spark_version = spark.version
        print(f"Using serverless fallback: {spark_version} (Excel disabled)")

    config = MetadataConfig(**kwargs)
    if "ml" not in spark_version and "excel" in (
        config.review_input_file_type,
        config.reviewable_output_format,
    ):
        raise ValueError(
            "Excel writes in dbxmetagen are not supported on standard runtimes. Please change your output file type to tsv or sql if appropriate."
        )
    process_metadata_file(config, input_file)


# COMMAND ----------

main(review_variables, file_name)

# COMMAND ----------

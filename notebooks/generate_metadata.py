# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI-Assisted Metadata Utility (a.k.a `dbxmetagen`)
# COMMAND ----------
# MAGIC %md
# MAGIC #`dbxmetagen` Overview
# MAGIC ### This is a utility to help generate high quality descriptions for tables and columns to enhance enterprise search and data governance, identify and classify PI, improve Databricks Genie performance for Text-2-SQL, and generally help curate a high quality metadata layer and data dictionary for enterprise data.
# MAGIC
# MAGIC While Databricks does offer high quality [AI Generated Documentation](https://docs.databricks.com/en/comments/ai-comments.html), and PI identification these are not always customizable to customers' needs or integrable into devops workflows without additional effort. Prompts and model choice are not adjustable by customers, and there are a variety of customization options that customers have asked for. This utility, `dbxmetagen`, helps generate table and column descriptions at scale, as well as identifying and classifying various forms of sensitive information. Eventually Databricks utilities will undoubtedly be more flexible, but this solution accelerator can allow customers to close the gap in a customizable fashion until then.
# MAGIC
# MAGIC Please review the readme for full details and documentation.
# MAGIC
# MAGIC ###Disclaimer
# MAGIC AI generated comments are not always accurate and comment DDLs should be reviewed prior to modifying your tables. Databricks strongly recommends human review of AI-generated comments to check for inaccuracies. While the model has been guided to avoids generating harmful or inappropriate descriptions, you can mitigate this risk by setting up [AI Guardrails](https://docs.databricks.com/en/ai-gateway/index.html#ai-guardrails) in the AI Gateway where you connect your LLM.
# COMMAND ----------
# MAGIC %md
# MAGIC # Library installs

# COMMAND ----------
# MAGIC %pip install -q -r ../minimal_requirements.txt
# COMMAND ----------
dbutils.widgets.text("mode", "comment", "Mode (comment, pi, domain)")
dbutils.widgets.text("include_deterministic_pi", "true", "Run Presidio deterministic PI detection")
dbutils.widgets.text("spacy_model_names", "en_core_web_md", "spaCy model (en_core_web_md or en_core_web_lg)")
_mode = dbutils.widgets.get("mode").strip().lower()
_include_det_pi = dbutils.widgets.get("include_deterministic_pi").strip().lower() == "true"
if _mode == "pi" and _include_det_pi:
    _spacy_model = dbutils.widgets.get("spacy_model_names").strip()
    if _spacy_model == "en_core_web_lg":
        %pip install -q spacy==3.8.7 presidio-analyzer==2.2.358 https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl
    elif _spacy_model == "en_core_web_md":
        %pip install -q spacy==3.8.7 presidio-analyzer==2.2.358 https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.8.0/en_core_web_md-3.8.0-py3-none-any.whl
    else:
        raise ValueError(f"Invalid spaCy model: {_spacy_model}")
dbutils.library.restartPython()
# COMMAND ----------
# MAGIC %md
# MAGIC # Library imports, widgets, and environment
# COMMAND ----------
import sys

sys.path.append(
    "../src"
)  # For git-clone or DAB deployment; pip-installed package works without this
from dbxmetagen.main import main
from dbxmetagen.databricks_utils import (
    setup_widgets,
    setup_notebook_variables,
)
from dbxmetagen.config import MetadataConfig

setup_widgets(dbutils)
notebook_variables = setup_notebook_variables(dbutils)
# COMMAND ----------
main(notebook_variables)

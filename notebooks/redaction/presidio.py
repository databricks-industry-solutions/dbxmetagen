# Databricks notebook source
# MAGIC %pip install -r ../../requirements.txt
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.deterministic_pi import *
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine
from presidio_analyzer.dict_analyzer_result import DictAnalyzerResult

import pandas as pd
from pprint import pprint
from typing import Iterator

def format_presidio_batch_results(
    results: Iterator[DictAnalyzerResult], score_threshold: float = 0
) -> List:
    # Results must be a dict with col1_name and col2_name as keys.
    # col1 should be doc_id
    # col2 should be text
    col1, col2 = tuple(results)
    doc_ids = col1.value
    original_texts = col2.value
    recognizer_results = col2.recognizer_results

    output = []
    for i, res_doc in enumerate(recognizer_results):
        for j, res_ent in enumerate(res_doc):
            ans = res_ent.to_dict()
            ans["doc_id"] = doc_ids[i]
            ans["entity"] = original_texts[i][res_ent.start : res_ent.end]
            if ans.get("score", 0) > score_threshold:
                output.append(ans)
    return output

# COMMAND ----------

df = spark.table("dbxmetagen.eval_data.jsl_48docs")
display(df)

# COMMAND ----------

# text_dict = dict(df.select("doc_id", "text").limit(3).toPandas().values)
text_dict = df.select("doc_id", "text").distinct().toPandas().to_dict(orient="list")
text_dict

# COMMAND ----------

score_threshold = 0.5
#analyzer = AnalyzerEngine(default_score_threshold=score_threshold)
analyzer = get_analyzer_engine(add_pci=False, default_score_threshold=score_threshold)
batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)
results = batch_analyzer.analyze_dict(
    text_dict,
    language="en",
    keys_to_skip=["doc_id"],
    score_threshold=score_threshold,
    batch_size=16,
    n_process=3,
)
results_copy = results

# COMMAND ----------

output = format_presidio_batch_results(results, score_threshold=score_threshold)
output

# COMMAND ----------

df_results = spark.createDataFrame(pd.DataFrame(output))
df_results.write.mode("overwrite").saveAsTable("dbxmetagen.eval_data.jsl_48docs_by_presidio_OOTB")
display(df_results)

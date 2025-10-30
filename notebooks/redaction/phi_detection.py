# Databricks notebook source
# MAGIC %md
# MAGIC # PII Masking in unstructured medical data
# MAGIC
# MAGIC ### Presidio Query
# MAGIC Uses Presidio for identifying PII in text
# MAGIC
# MAGIC
# MAGIC ### AI Query for PHI Masking
# MAGIC This prompting attempts to identify and label all instances of PHI for a cell of medical text,. In post-processing, we then identify the positions of these PHI entities for downstream masking. Notably, if there are multiple of the same entities with different labels, (ie, "Beal Street" as a "person" label as well as a "street" label), the positions will be duplicated.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession

from src.dbxmetagen.redaction import (
    LABEL_ENUMS,
    make_presidio_batch_udf,
    make_prompt,
    format_entity_response_object_udf,
    align_entities_udf,
)
from src.dbxmetagen.redaction.config import PHI_PROMPT_SKELETON

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    defaultValue="dbxmetagen.eval_data.jsl_48docs",
    label="0. Source Table",
    name="medical_text_table"
)

dbutils.widgets.text(
    defaultValue="text",
    label="1. Text Column",
    name="medical_text_col"
)

dbutils.widgets.dropdown(
    name='endpoint',
    defaultValue='databricks-gpt-oss-120b',
    choices=sorted([
        'databricks-gpt-oss-20b',
        'databricks-gpt-oss-120b',
        'databricks-gemma-3-12b',
        'databricks-llama-4-maverick',
        'databricks-meta-llama-3-3-70b-instruct',
        'databricks-meta-llama-3-1-8b-instruct'
    ]),
    label='2. AI Endpoint'
)

dbutils.widgets.text("presidio_score_threshold", "0.5", "3. Presidio Score Threshold")

med_text_table = dbutils.widgets.get("medical_text_table")
med_text_col = dbutils.widgets.get("medical_text_col")
endpoint = dbutils.widgets.get("endpoint")
score_threshold = float(dbutils.widgets.get("presidio_score_threshold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set Spark configuration for batch processing
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10)
num_cores = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

# Load source data (used for both Presidio and AI detection)
source_df = (
    spark.table(med_text_table)
    .select("doc_id", col(med_text_col))
    .distinct()
)

display(source_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Presidio-based PHI Detection

# COMMAND ----------

# Process data with Presidio
text_df = source_df.repartition(num_cores).withColumn(
    "presidio_results", 
    make_presidio_batch_udf(score_threshold=score_threshold)(col("doc_id"), col("text"))
)

# Parse results into structured format
text_df = text_df.withColumn(
    "presidio_results_struct", 
    from_json(
        "presidio_results", 
        "array<struct<entity:string, score:double, start:integer, end:integer, doc_id:string>>"
    )
)

# Save results
text_df.write.mode('overwrite').saveAsTable("dbxmetagen.eval_data.presidio_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI-based PHI Detection

# COMMAND ----------

display(spark.table("dbxmetagen.eval_data.presidio_results"))

# COMMAND ----------

# Create the prompt for AI detection
prompt = make_prompt(PHI_PROMPT_SKELETON, labels=LABEL_ENUMS)

# Prepare data with prompts
# NOTE: the `modelParameters` acceptable values will change based on the chosen model. 
# See: https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query#model-params for more details

# Create a temporary view from the source data
source_df.createOrReplaceTempView("source_data_temp")

query = f"""
  WITH data_with_prompting AS (
      SELECT doc_id, text,
            REPLACE('{prompt}', '{{{{med_text}}}}', CAST(text AS STRING)) AS prompt
      FROM source_data_temp
  )
  SELECT *,
        ai_query(
          endpoint => '{endpoint}',
          request => prompt,
          failOnError => false,
          returnType => 'STRUCT<result: ARRAY<STRUCT<entity: STRING, entity_type: STRING>>>',
          modelParameters => named_struct('reasoning_effort', 'low')
        ) AS response
  FROM data_with_prompting
"""

ai_text_df = (
    spark
    .sql(query)
    .repartition(num_cores)
    .withColumn(
        "ai_query_results", 
        format_entity_response_object_udf(col("response.result"), col("text"))
    )
)

(
  ai_text_df
    .write
    .mode('overwrite')
    .option("mergeSchema", "true")
    .saveAsTable("dbxmetagen.eval_data.ai_results")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

# Display Presidio results
presidio_results = spark.read.table("dbxmetagen.eval_data.presidio_results")
display(presidio_results)

# COMMAND ----------

# Display AI results
ai_results = (
    spark.read.table("dbxmetagen.eval_data.ai_results")
    .withColumn(
        "ai_results_struct", 
        from_json(
            "ai_query_results", 
            "array<struct<entity:string, score:double, start:integer, end:integer, doc_id:string>>"
        )
    )
)
display(ai_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Results

# COMMAND ----------

# Join Presidio and AI results
all_results = presidio_results.drop("text").join(ai_results, "doc_id")
all_results.write.mode('overwrite').option("mergeSchema", "true").saveAsTable("dbxmetagen.eval_data.ai_vs_presidio")
display(all_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Alignment
# MAGIC
# MAGIC Align entities from both detection methods to create a consensus view with confidence scores.

# COMMAND ----------

# Read combined results and align entities
df = spark.read.table("dbxmetagen.eval_data.ai_vs_presidio")

df = df.withColumn(
    "aligned_entities",
    align_entities_udf()(col("ai_results_struct"), col("presidio_results_struct"), col("doc_id"))
)

# Save aligned results
df.write.mode('overwrite').saveAsTable("dbxmetagen.eval_data.aligned_entities3")

# COMMAND ----------

# Display aligned results
display(df)

# COMMAND ----------



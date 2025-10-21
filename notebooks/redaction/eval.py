# Databricks notebook source
df = spark.table("dbxmetagen.eval_data.jsl_48docs")
display(df)

# COMMAND ----------

df_results = spark.table("dbxmetagen.eval_data.jsl_48docs_by_presidio_OOTB")
display(df_results)

# COMMAND ----------

from pyspark.sql.functions import lower, trim, col, asc_nulls_last

df_join = df.join(
    df_results,
    (lower(trim(df_results.entity)) == lower(trim(df.chunk)))
    & (df_results.doc_id == df.doc_id)
    & (df_results.start == df.begin),
    how="outer",
).drop("text").orderBy(asc_nulls_last(df.doc_id), asc_nulls_last(df.begin))
display(df_join)

# COMMAND ----------

import spacy

text_dict = df.select("doc_id", "text").distinct().toPandas().to_dict(orient="list")
corpus = '\n'.join(text_dict['text'])
all_tokens = len(corpus)

# COMMAND ----------

pos_actual = df.count()
pos_pred = df_results.count()
tp = df_join.where(col("chunk").isNotNull() & col("entity").isNotNull()).count()
fp = pos_pred - tp

neg_actual = all_tokens - pos_actual
tn = neg_actual - fp
fn = pos_actual - tp
neg_pred = tn + fn

recall = tp/pos_actual
precision = tp/pos_pred
specificity = tn/neg_actual
npv = tn/neg_pred

neg_actual, pos_actual, neg_pred, pos_pred, tn, tp, fp, fn, recall, precision, specificity, npv

# COMMAND ----------

# MAGIC %md
# MAGIC |          | Neg_actual |  Pos_actual |        |
# MAGIC |----------|------------|-------------|--------|
# MAGIC | Neg_pred | 249546     |  772        | 250318 |
# MAGIC | Pos_pred |    890     |  707        |   1597 |
# MAGIC |          | 250436     | 1479        |        |

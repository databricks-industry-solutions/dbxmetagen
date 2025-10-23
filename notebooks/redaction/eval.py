# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql.functions import lower, trim, col, asc_nulls_last, abs as spark_abs, contains, explode

# COMMAND ----------

df = spark.table("dbxmetagen.eval_data.jsl_48docs")
display(df)

# COMMAND ----------

df_results = spark.table("dbxmetagen.eval_data.aligned_entities3")
display(df_results)

# COMMAND ----------

presidio_results_exploded = df_results.select("doc_id", "presidio_results_struct").withColumn("presidio_results_exploded", explode("presidio_results_struct")).select("presidio_results_exploded.*")

ai_results_exploded = df_results.select("doc_id", "ai_results_struct").withColumn("ai_results_exploded", explode("ai_results_struct")).select("doc_id", "ai_results_exploded.entity", "ai_results_exploded.score", "ai_results_exploded.start", "ai_results_exploded.end")

aligned_results_exploded = df_results.select("doc_id", "aligned_entities").withColumn("aligned_entities", explode("aligned_entities")).select("doc_id", "aligned_entities.entity", "aligned_entities.start", "aligned_entities.end")

# COMMAND ----------

def evaluate_df(df1, df2):
    df_join = df1.join(
        df2,
        (contains(df1.chunk, df2.entity) | contains(df2.entity, df1.chunk))
        & (df2.doc_id == df1.doc_id)
        & (df2.start <= df1.begin)
        & (df2.end >= df1.end-1),
        how="outer"
    ).drop("text").orderBy(asc_nulls_last(df.doc_id), asc_nulls_last(df.begin))
    return df_join

presidio_join_df = evaluate_df(df, presidio_results_exploded)
ai_join_df = evaluate_df(df, ai_results_exploded)
aligned_join_df = evaluate_df(df, aligned_results_exploded)

# COMMAND ----------

import spacy

text_dict = df.select("doc_id", "text").distinct().toPandas().to_dict(orient="list")
corpus = '\n'.join(text_dict['text'])
all_tokens = len(corpus)

# COMMAND ----------

display(df_results)

# COMMAND ----------

pos_actual = df.count()
pos_pred = presidio_results_exploded.count()
tp = presidio_join_df.where(col("chunk").isNotNull() & col("entity").isNotNull()).count()
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

contingency_data = {
    "": ["Neg_pred", "Pos_pred"],
    "Neg_actual": [tn, fp],
    "Pos_actual": [fn, tp],
    "Total": [tn + fp, fn + tp]
}
contingency_df = pd.DataFrame(contingency_data)
contingency_df.loc["Total"] = [
    "Total",
    contingency_df["Neg_actual"].sum(),
    contingency_df["Pos_actual"].sum(),
    contingency_df["Total"].sum()
]

display(contingency_df)

accuracy = (tp + tn) / (pos_actual + neg_actual)

summary_df = pd.DataFrame({
    "Metric": ["Accuracy", "Precision", "Specificity", "NPV", "Recall"],
    "Value": [accuracy, precision, specificity, npv, recall]
})

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC |          | Neg_actual |  Pos_actual |        |
# MAGIC |----------|------------|-------------|--------|
# MAGIC | Neg_pred | 249546     |  772        | 250318 |
# MAGIC | Pos_pred |    890     |  707        |   1597 |
# MAGIC |          | 250436     | 1479        |        |

# COMMAND ----------

pos_actual = df.count()
pos_pred = ai_results_exploded.count()
tp = ai_join_df.where(col("chunk").isNotNull() & col("entity").isNotNull()).count()
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

contingency_data = {
    "": ["Neg_pred", "Pos_pred"],
    "Neg_actual": [tn, fp],
    "Pos_actual": [fn, tp],
    "Total": [tn + fp, fn + tp]
}
contingency_df = pd.DataFrame(contingency_data)
contingency_df.loc["Total"] = [
    "Total",
    contingency_df["Neg_actual"].sum(),
    contingency_df["Pos_actual"].sum(),
    contingency_df["Total"].sum()
]

display(contingency_df)


accuracy = (tp + tn) / (pos_actual + neg_actual)

summary_df = pd.DataFrame({
    "Metric": ["Accuracy", "Precision", "Specificity", "NPV", "Recall"],
    "Value": [accuracy, precision, specificity, npv, recall]
})

display(summary_df)

# COMMAND ----------

pos_actual = df.count()
pos_pred = aligned_results_exploded.count()
tp = aligned_join_df.where(col("chunk").isNotNull() & col("entity").isNotNull()).count()
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

contingency_data = {
    "": ["Neg_pred", "Pos_pred"],
    "Neg_actual": [tn, fp],
    "Pos_actual": [fn, tp],
    "Total": [tn + fp, fn + tp]
}
contingency_df = pd.DataFrame(contingency_data)
contingency_df.loc["Total"] = [
    "Total",
    contingency_df["Neg_actual"].sum(),
    contingency_df["Pos_actual"].sum(),
    contingency_df["Total"].sum()
]

display(contingency_df)


accuracy = (tp + tn) / (pos_actual + neg_actual)

summary_df = pd.DataFrame({
    "Metric": ["Accuracy", "Precision", "Specificity", "NPV", "Recall"],
    "Value": [accuracy, precision, specificity, npv, recall]
})

display(summary_df)

# COMMAND ----------

### False negatives
display(aligned_join_df.where(aligned_join_df.entity.isNull()).select("chunk").groupBy("chunk").count())

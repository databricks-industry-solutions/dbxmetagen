# Databricks notebook source
df = spark.table("dbxmetagen.eval_data.jsl_48docs")
display(df)

# COMMAND ----------

df_results = spark.table("dbxmetagen.eval_data.ai_vs_presidio")
display(df_results)

# COMMAND ----------

from pyspark.sql.functions import explode
presidio_results_exploded = df_results.select("doc_id", "presidio_results_struct").withColumn("presidio_results_exploded", explode("presidio_results_struct")).select("presidio_results_exploded.*")
ai_results_exploded = df_results.select("doc_id", "ai_results_struct").withColumn("ai_results_exploded", explode("ai_results_struct")).select("doc_id", "ai_results_exploded.entity", "ai_results_exploded.score", "ai_results_exploded.start", "ai_results_exploded.end")

# COMMAND ----------

display(ai_results_exploded)

# COMMAND ----------

from pyspark.sql.functions import lower, trim, col, asc_nulls_last

def evaluate_df(df, df_results):
    df_join = df.join(
        df_results,
        (lower(trim(df_results.entity)) == lower(trim(df.chunk)))
        & (df_results.doc_id == df.doc_id)
        & (df_results.start == df.begin),
        how="outer",
    ).drop("text").orderBy(asc_nulls_last(df.doc_id), asc_nulls_last(df.begin))
    return df_join

presidio_join_df = evaluate_df(df, presidio_results_exploded)
ai_join_df = evaluate_df(df, ai_results_exploded)

# COMMAND ----------

display(presidio_join_df)

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

# TODO:
1. Combine the results for presidio and AI
2. Check for entity mismatches and look at some of the misses - e.g. time, date, other things?
3. Eval the combined results
4. Add custom recognizers and improve presidio
5. ?


## Finally evaluate the combined results

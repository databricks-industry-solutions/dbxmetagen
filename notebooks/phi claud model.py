# Databricks notebook source
# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %pip install -qqqq -U openai pydantic==2.9.2 presidio-analyzer
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, concat

# COMMAND ----------

df = spark.table("dbxmetagen.default.phi_test_data")

# COMMAND ----------

display(df)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

df = spark.table("dbxmetagen.default.phi_test_data")
df.repartition(50).write.format("delta").mode("overwrite").saveAsTable("dbxmetagen.default.phi_test_data_restructured")
df_stream = spark.readStream.format("delta").option("maxBytesPerTrigger", "50K").table("dbxmetagen.default.phi_test_data_restructured")

from presidio_analyzer import AnalyzerEngine
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd
import json

analyzer = AnalyzerEngine()
broadcasted_analyzer = spark.sparkContext.broadcast(analyzer)

def extract_entities(text: str) -> str:
    analyzer = broadcasted_analyzer.value
    results = analyzer.analyze(text=text, language="en")
    formatted = [
        {
            "score": round(r.score, 2),
            "text": text[r.start:r.end],
            "start": r.start,
            "end": r.end,
            "pii_category": r.entity_type
        }
        for r in results
    ]
    return json.dumps({"extracted_entities": formatted})

def extract_entities_series(series: pd.Series) -> pd.Series:
    return series.apply(extract_entities)

extract_entities_udf = pandas_udf(extract_entities_series, returnType=StringType())
df_stream = df.withColumn("presdio_pii_entities", extract_entities_udf(df["medical_text_with_phi"]))

# COMMAND ----------

# MAGIC %tb

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

df = spark.table("dbxmetagen.default.phi_test_data")
df.repartition(50).write.format("delta").mode("overwrite").saveAsTable("dbxmetagen.default.phi_test_data_restructured")
df_stream = spark.readStream.format("delta").option("maxBytesPerTrigger", "50K").table("dbxmetagen.default.phi_test_data_restructured")

from presidio_analyzer import AnalyzerEngine
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd
import json

analyzer = AnalyzerEngine()
broadcasted_analyzer = spark.sparkContext.broadcast(analyzer)

def extract_entities(text: str) -> str:
    analyzer = broadcasted_analyzer.value
    results = analyzer.analyze(text=text, language="en")
    formatted = [
        {
            "score": round(r.score, 2),
            "text": text[r.start:r.end],
            "start": r.start,
            "end": r.end,
            "pii_category": r.entity_type
        }
        for r in results
    ]
    return json.dumps({"extracted_entities": formatted})

def extract_entities_series(series: pd.Series) -> pd.Series:
    return series.apply(extract_entities)

extract_entities_udf = pandas_udf(extract_entities_series, returnType=StringType())
df_stream = df.withColumn("presdio_pii_entities", extract_entities_udf(df["medical_text_with_phi"]))


prompt = """You are a PHI detection system. Analyze the following medical text and identify all personally identifiable information (PHI) that must be redacted for HIPAA compliance.

            Return a JSON list of entities with this format:
            [{{"text": "found_text", "label": "phi_category", "start": start_pos, "end": end_pos, "score": 0.9}}]

            PHI categories to detect:
            - person (names)
            - phone number
            - email address
            - social security number
            - medical record number
            - date of birth
            - street address
            - geographic identifier        

            Return only the JSON list, no additional text. It needs to be a list of dictionaries, with the exact keys referenced.
            
            Text to analyze:"""


# In AI query, can provide a different format for the structured output that might be easier - Brennan
# https://www.luc.edu/its/aboutus/itspoliciesguidelines/hipaainformation/the18hipaaidentifiers/
df_transformed = df_stream.select(
    "*",
    expr(f"""ai_query(
        'databricks-claude-sonnet-4-5',
        '{prompt}' || medical_text_with_phi,
        responseFormat => '{{
            "type": "json_schema",
            "json_schema": {{
                "name": "pii_extraction",
                "schema": {{
                    "type": "object",
                    "properties": {{
                        "extracted_entities": {{
                            "type": "array",
                            "items": {{
                                "type": "object",
                                "properties": {{
                                    "text": {{"type": "string"}},
                                    "pii_category": {{
                                        "type": "string",
                                        "enum": [
                                            "Person",
                                            "Phone",
                                            "Email",
                                            "SSN",
                                            "Medical Record Number",
                                            "Date of Birth",
                                            "Street Address",
                                            "Geographic Identifier"
                                        ]
                                    }},
                                    "start": {{"type": "integer"}},
                                    "end": {{"type": "integer"}},
                                    "score": {{"type": "number"}}
                                }},
                                "required": [
                                    "text",
                                    "pii_category",
                                    "start",
                                    "end",
                                    "score"
                                ]
                            }}
                        }}
                    }},
                    "required": ["extracted_entities"]
                }},
                "strict": true
            }}
        }}'
    )""").alias("extracted_entities_list")
)

# COMMAND ----------

df_transformed.write.saveAsTable("dbxmetagen.default.redaction_evals")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
import pandas as pd

@F.pandas_udf(
    StructType([
        StructField("accuracy", DoubleType()),
        StructField("precision", DoubleType()),
        StructField("recall", DoubleType()),
        StructField("f1", DoubleType()),
    ])
)
def compute_metrics_udf(phi_ground_truth_entities, predicted_entities):
    def parse_entities(entities):
        if isinstance(entities, str):
            try:
                entities_list = json.loads(entities)
                if isinstance(entities_list, dict):
                    entities_list = entities_list.get("extracted_entities")

            except Exception as e:
                raise ValueError(f"Error parsing entities: {e}")                    
        return set((ent.get('text', ' '), ent.get('label', ' '), ent.get('pii_category', ' ').lower())
                   for ent in entities_list)
    
    metrics = []
    for gt, pred in zip(phi_ground_truth_entities, predicted_entities):                
        print(f"Ground truth: {gt}")
        print(f"Predicted: {pred}")
        pred_labels = parse_entities(pred) # current issue is here
        gt_labels = parse_entities(gt)
        #raise ValueError(pred_labels)


        all_labels = gt_labels.union(pred_labels)
        gt_vec = [1 if lbl in gt_labels else 0 for lbl in all_labels]
        pred_vec = [1 if lbl in pred_labels else 0 for lbl in all_labels]

        try:
            accuracy = accuracy_score(gt_vec, pred_vec)
            precision = precision_score(gt_vec, pred_vec, zero_division=0)
            recall = recall_score(gt_vec, pred_vec, zero_division=0)
            f1 = f1_score(gt_vec, pred_vec, zero_division=0)
        except Exception:
            accuracy = precision = recall = f1 = 0.0
        metrics.append((accuracy, precision, recall, f1))
    return pd.DataFrame(metrics, columns=['accuracy', 'precision', 'recall', 'f1'])

df = spark.read.table("dbxmetagen.default.redaction_evals")
df = df.withColumn(
    "presidio_metrics",
    compute_metrics_udf(
        F.col("phi_ground_truth_entities"), 
        F.col("presdio_pii_entities")
    )
)

df = df.withColumn(
    "extracted_metrics",
    compute_metrics_udf(
        F.col("phi_ground_truth_entities"), 
        F.col("extracted_entities_list")
    )
)

#Show metrics per row
df.select("presidio_metrics", "extracted_metrics")

display(df)
df_metrics = df.select(
    F.expr("presidio_metrics.accuracy").alias("presidio_accuracy"),
    F.expr("presidio_metrics.precision").alias("presidio_precision"),
    F.expr("presidio_metrics.recall").alias("presidio_recall"),
    F.expr("presidio_metrics.f1").alias("presidio_f1"),
    F.expr("extracted_metrics.accuracy").alias("extracted_accuracy"),
    F.expr("extracted_metrics.precision").alias("extracted_precision"),
    F.expr("extracted_metrics.recall").alias("extracted_recall"),
    F.expr("extracted_metrics.f1").alias("extracted_f1")
)

df_summary = df_metrics.agg(
    *[F.avg(c).alias(f"avg_{c}") for c in df_metrics.columns]
)
display(df_summary)



# COMMAND ----------

from pyspark.sql.functions import concat, lit

df = spark.read.table("dbxmetagen.default.redaction_evals")
df = df.selectExpr(
    """
    ai_query(
        'databricks-claude-sonnet-4-5',
        concat(
            "Please compare the two columns and return the number of correct matches to the ground truths from the extracted entities, as well as the total number of extracted entities and the total number of ground truth entities. Please output the result as a json string: {'number_correct': 0, 'total_extracted': 0, 'total_ground_truth': 0}",
            phi_redacted_ground_truth,
            extracted_entities_list
        )
    ) as phi_claude_metrics
    """
)
display(df)

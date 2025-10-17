# Databricks notebook source
# MAGIC %pip install -qqqq -U openai pydantic==2.9.2 presidio-analyzer
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import re
import pandas as pd
from pyspark.sql.functions import pandas_udf, col, expr, lit, concat
from pyspark.sql.types import LongType, StringType
from pyspark.sql import SparkSession

# COMMAND ----------

dbutils.widgets.text(
    defaultValue="dbxmetagen.default.phi_test_data",
    label="0. Table",
    name="medical_text_table"
)

dbutils.widgets.text(
    defaultValue="medical_text_with_phi",
    label="1. Column containing PHI",
    name="medical_text_col"
)

dbutils.widgets.dropdown(
  name='endpoint',
  defaultValue='databricks-gpt-oss-120b',
  choices = sorted([ 'databricks-gpt-oss-20b',
                    'databricks-gpt-oss-120b',
                    'databricks-gemma-3-12b',
                    'databricks-llama-4-maverick',
                    'databricks-meta-llama-3-3-70b-instruct',
                    'databricks-meta-llama-3-1-8b-instruct']),
  label = '1. Endpoint'
)

med_text_table = dbutils.widgets.get("medical_text_table")
med_text_col = dbutils.widgets.get("medical_text_col")
endpoint = dbutils.widgets.get("endpoint")

# COMMAND ----------

df = spark.table("dbxmetagen.default.phi_test_data")

# COMMAND ----------

display(df)

# COMMAND ----------

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

from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

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

# ==== BTBeal AI Query 
prompt = """
You are an expert in Protected Health Information (PHI) detection who will help identify all PHI entities in a piece of medical text.

Qualifying PHI includes:
1. Names;
2. All geographical subdivisions smaller than a State, including street address, city, county, precinct, zip code, and their equivalent geocodes, except for the initial three digits of a zip code, if according to the current publicly available data from the Bureau of the Census: (1) The geographic unit formed by combining all zip codes with the same three initial digits contains more than 20,000 people; and (2) The initial three digits of a zip code for all such geographic units containing 20,000 or fewer people is changed to 000.
3. All elements of dates (except year) for dates directly related to an individual, including birth date, admission date, discharge date, date of death; and all ages over 89 and all elements of dates (including year) indicative of such age, except that such ages and elements may be aggregated into a single category of age 90 or older;
4. Phone numbers;
5. Fax numbers;
6. Electronic mail addresses;
7. Social Security numbers;
8. Medical record numbers;
9. Health plan beneficiary numbers;
10. Account numbers;
11. Certificate/license numbers;
12. Vehicle identifiers and serial numbers, including license plate numbers;
13. Device identifiers and serial numbers;
14. Web Universal Resource Locators (URLs);
15. Internet Protocol (IP) address numbers;
16. Biometric identifiers, including finger and voice prints;
17. Full face photographic images and any comparable images; and
18. Any other unique identifying number, characteristic, or code (note this does not mean the unique code assigned by the investigator to code the data)
There are also additional standards and criteria to protect individuals from re-identification. Any code used to replace the identifiers in data sets cannot be derived from any information related to the individual and the master codes, nor can the method to derive the codes be disclosed. For example, a subjects initials cannot be used to code their data because the initials are derived from their name. Additionally, the researcher must not have actual knowledge that the research subject could be re-identified from the remaining identifiers in the PHI used in the research study. In other words, the information would still be considered identifiable if there was a way to identify the individual even though all of the 18 identifiers were removed.

You will identify all PHI with the following enums as the "label":

["person",
"phone",
"email",
"SSN",
"medical record number",
"date of birth",
"street address",
"geographic identifier"]

Respond with a list of dictionaries such as [{"text": "Brennan Beal", "label": "Person"}, {"text": "123-45-6789", "label": "SSN"}]

Note that if there are multiple of the same entities, you should list them multiple times. For example, if the text suggests "The patient, Brennan, notes that is feeling unwell. Brennan presents with a moderate fever of 100.5F," you should list the entity "brennan" twice. 

The text is listed here: 
<MedicalText>
{med_text}
<MedicalText/>

EXAMPLE: 
MedicalText: "MRN: 222345 -- I saw patient Brennan Beal today at 11:30am, who presents with a sore throat and temperature of 103F"
response: [{"text": "Brennan Beal", "label": "person"]}, {"text": "222345", "label": "medical record number"}]
"""

query = f"""
  WITH data_with_prompting AS (
      SELECT *,
            REPLACE('{prompt}', '{{med_text}}', CAST({med_text_col} AS STRING)) AS prompt
      FROM {med_text_table}
  )
  SELECT *,
        ai_query(
          endpoint => '{endpoint}',
          request => prompt,
          failOnError => false,
          returnType => 'STRUCT<result: ARRAY<STRUCT<text: STRING, label: STRING>>>',
          modelParameters => named_struct('reasoning_effort', 'low')
        ) AS response
  FROM data_with_prompting
  """

@pandas_udf(StringType())
def add_positions_to_entities_udf(identified_entities_series, sentences):
    new_entity_series = []
    for entity_list, sentence in zip(identified_entities_series, sentences):

        entities = json.loads(entity_list)
        unique_entities_set = set([(entity['text'], entity['label']) for entity in entities])

        new_entity_list = []

        for entity in unique_entities_set:
            pattern = re.escape(entity[0])
            positions = [(m.start(), m.end() - 1) for m in re.finditer(pattern, sentence)]

            for position in positions:
                new_entity_list.append({'text': entity[0], 'label': entity[1], 'start': position[0], 'end': position[1]})
        
        new_entity_series.append(json.dumps(new_entity_list))
    return pd.Series(new_entity_series)

ai_query_df = (
  spark
    .sql(query)
    .withColumn(
        "phi_ai_query_entities", 
        add_positions_to_entities_udf(
            F.col("response.result"), F.col("medical_text_with_phi")
        )
    )
)

# COMMAND ----------

display(ai_query_df)

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

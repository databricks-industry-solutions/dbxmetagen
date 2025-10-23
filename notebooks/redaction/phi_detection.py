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
# MAGIC #%pip install -qqqq -U openai pydantic==2.9.2 presidio-analyzer
# MAGIC #dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json
import re
import pandas as pd
from pyspark.sql.functions import pandas_udf, col, expr, lit, concat
from pyspark.sql.types import LongType, StringType
from pyspark.sql import SparkSession
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.deterministic_pi import *
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine
from presidio_analyzer.dict_analyzer_result import DictAnalyzerResult

import pandas as pd
from pprint import pprint
from typing import Iterator
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.functions import pandas_udf, col
import pandas as pd


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

dbutils.widgets.text("presidio_score_threshold", "0.5", "Presidio Score Threshold")

med_text_table = dbutils.widgets.get("medical_text_table")
med_text_col = dbutils.widgets.get("medical_text_col")
endpoint = dbutils.widgets.get("endpoint")
score_threshold = dbutils.widgets.get("presidio_score_threshold")

# COMMAND ----------

eligible_entity_types = [
    "PERSON",
    "PHONE",
    "EMAIL",
    "SSN",
    "MEDICAL_RECORD",
    "DATE_OF_BIRTH",
    "STREET_ADDRESS",
    "GEOGRAPHIC_IDENTIFIER"
]

LABEL_ENUMS = '["PERSON","PHONE_NUMBER","NRP","UK_NHS","AU_ACN","LOCATION","DATE_TIME","AU_MEDICARE","MEDICAL_RECORD_NUMBER","AU_TFN","EMAIL_ADDRESS","US_SSN","VIN","IP","DRIVER_LICENSE","BIRTH_DATE","APPOINTMENT_DATE_TIME",]'

prompt_skeleton = """
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

{label_enums}

Respond with a list of dictionaries such as [{{"entity": "Alice Anderson", "entity_type": "PERSON"}}, {{"entity": "123-45-6789", "entity_type": "US_SSN"}}]

Note that if there are multiple of the same entities, you should list them multiple times. For example, if the text suggests "The patient, Brennan, notes that is feeling unwell. Brennan presents with a moderate fever of 100.5F," you should list the entity "brennan" twice. 

The text is listed here: 
<MedicalText>
{{med_text}}
<MedicalText/>

EXAMPLE: 
MedicalText: "MRN: 222345 -- I saw patient Alice Anderson today at 11:30am, who presents with a sore throat and temperature of 103F"
response: [{{"entity": "Alice Anderson", "entity_type": "PERSON"}}, {{"entity": "222345", "entity_type": "MEDICAL_RECORED_NUMBER"}}]
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Presidio functions

# COMMAND ----------

### Presidio batch functions
def format_presidio_batch_results(
    results: Iterator[DictAnalyzerResult], score_threshold: float = 0.5
) -> list:
    col1, col2 = tuple(results)
    doc_ids = col1.value
    original_texts = col2.value
    recognizer_results = col2.recognizer_results

    output = []
    for i, res_doc in enumerate(recognizer_results):
        findings = []
        for res_ent in res_doc:
            ans = res_ent.to_dict()
            ans["doc_id"] = doc_ids[i]
            ans["entity"] = original_texts[i][res_ent.start : res_ent.end]
            if ans.get("score", 0) > score_threshold:
                findings.append(ans)
        output.append(json.dumps(findings))
    return output


def make_presidio_batch_udf(score_threshold=0.5):
    @pandas_udf("string")
    def analyze_udf(batch_iter: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
        analyzer = get_analyzer_engine(add_pci=False, default_score_threshold=score_threshold)
        batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)
        for doc_ids, texts in batch_iter:
            text_dict = pd.DataFrame({"doc_id": doc_ids, "text": texts}).to_dict(orient="list")
            results = batch_analyzer.analyze_dict(
                text_dict, language="en", keys_to_skip=["doc_id"],
                score_threshold=score_threshold, batch_size=20, n_process=1
            )
            output = format_presidio_batch_results(results, score_threshold=score_threshold)
            yield pd.Series(output)
    return analyze_udf


def make_prompt(prompt_skeleton, labels=eligible_entity_types):
    return prompt_skeleton.format(label_enums=labels)

@pandas_udf(StringType())
def format_entity_response_object_udf(identified_entities_series, sentences):
    new_entity_series = []
    for entity_list, sentence in zip(identified_entities_series, sentences):
        entities = json.loads(entity_list)
        unique_entities_set = set([(entity['entity'], entity['entity_type']) for entity in entities])

        new_entity_list = []

        for entity in unique_entities_set:
            pattern = re.escape(entity[0])
            positions = [(m.start(), m.end() - 1) for m in re.finditer(pattern, sentence)]

            for position in positions:
                new_entity_list.append(
                    {
                        'entity': entity[0], 
                        'entity_type': entity[1], 
                        'start': position[0], 
                        'end': position[1],
                        'score': None,
                        'analysis_explanation': None,
                        'recognition_metadata': {}
                    }
                )

        new_entity_series.append(json.dumps(new_entity_list))

    return pd.Series(new_entity_series)

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10)
num_cores = 10

# COMMAND ----------


_df = spark.table("dbxmetagen.eval_data.jsl_48docs").select("doc_id", "text").distinct()
#df = df.limit(5)
text_df = _df.repartition(num_cores).withColumn("presidio_results", make_presidio_batch_udf(score_threshold=0.5)(col("doc_id"), col("text")))
text_df = text_df.withColumn("presidio_results_struct", from_json("presidio_results", "array<struct<entity:string, score:double, start:integer, end:integer, doc_id:string>>"))
text_df.write.mode('overwrite').saveAsTable("dbxmetagen.eval_data.presidio_results")

# COMMAND ----------

prompt = make_prompt(prompt_skeleton, labels=LABEL_ENUMS)

query = f"""
  WITH data_with_prompting AS (
      SELECT DISTINCT doc_id, text,
            REPLACE('{prompt}', '{{med_text}}', CAST({med_text_col} AS STRING)) AS prompt
      FROM {med_text_table}
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
        format_entity_response_object_udf(
            col("response.result"), col("text")
        )
    )
)

ai_text_df.write.mode('overwrite').saveAsTable("dbxmetagen.eval_data.ai_results")

# COMMAND ----------

presidio_results = spark.read.table("dbxmetagen.eval_data.presidio_results")
display(presidio_results)

# COMMAND ----------

ai_results = spark.read.table("dbxmetagen.eval_data.ai_results").withColumn("ai_results_struct", from_json("ai_query_results", "array<struct<entity:string, score:double, start:integer, end:integer, doc_id:string>>"))
display(ai_results)

# COMMAND ----------

all_results = presidio_results.drop("text").join(ai_results, "doc_id")
all_results.write.mode('overwrite').option("mergeSchema", "true").saveAsTable("dbxmetagen.eval_data.ai_vs_presidio")
display(all_results)

# COMMAND ----------



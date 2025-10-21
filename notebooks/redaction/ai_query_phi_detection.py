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

# List of eligible entity types to be discovered by `ai_query()` or presidio
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

# COMMAND ----------

df = spark.table("dbxmetagen.default.phi_test_data")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI Query for PHI Masking
# MAGIC This prompting attempts to identify and label all instances of PHI for a cell of medical text,. In post-processing, we then identify the positions of these PHI entities for downstream masking. Notably, if there are multiple of the same entities with different labels, (ie, "Beal Street" as a "person" label as well as a "street" label), the positions will be duplicated.

# COMMAND ----------

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

Respond with a list of dictionaries such as [{"entity": "Brennan Beal", "entity_type": "PERSON"}, {"entity": "123-45-6789", "entity_type": "SSN"}]

Note that if there are multiple of the same entities, you should list them multiple times. For example, if the text suggests "The patient, Brennan, notes that is feeling unwell. Brennan presents with a moderate fever of 100.5F," you should list the entity "brennan" twice. 

The text is listed here: 
<MedicalText>
{med_text}
<MedicalText/>

EXAMPLE: 
MedicalText: "MRN: 222345 -- I saw patient Brennan Beal today at 11:30am, who presents with a sore throat and temperature of 103F"
response: [{"entity": "Brennan Beal", "entity_type": "PERSON"]}, {"entity": "222345", "entity_type": "MEDICAL_RECORED_NUMBER"}]
"""

def make_prompt(prompt_skeleton, labels=eligible_entity_types):
    return prompt_skeleton.format(label_enums=labels)

prompt = make_prompt(prompt_skeleton, labels)

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
          returnType => 'STRUCT<result: ARRAY<STRUCT<entity: STRING, entity_type: STRING>>>',
          modelParameters => named_struct('reasoning_effort', 'low')
        ) AS response
  FROM data_with_prompting
  """


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

df_for_eval = (
  spark
    .sql(query)
    .withColumn(
        "phi_ai_query_entities", 
        format_entity_response_object_udf(
            col("response.result"), col("medical_text_with_phi")
        )
    )
)

# COMMAND ----------

display(df_for_eval)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_for_eval.write.saveAsTable("dbxmetagen.default.redaction_evals")

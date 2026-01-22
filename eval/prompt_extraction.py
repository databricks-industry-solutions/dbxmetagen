"""
Prompt extraction and model function creation for dbxmetagen evaluation.

This module creates model functions compatible with mlflow.evaluate() by
extracting prompts from dbxmetagen's existing logic without modifying it.
"""

import mlflow
from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)
from typing import Dict, Any, Callable
import json
import sys
import os

# Add parent directory to path to import dbxmetagen
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.prompts import CommentPrompt, PIPrompt
from src.dbxmetagen.config import MetadataConfig

# Enable MLflow autologging for OpenAI
mlflow.openai.autolog()


def create_spark_df_from_sample(sample_data: list, column_types: Dict[str, str]):
    """
    Create a PySpark DataFrame from sample data for prompt generation.

    Args:
        sample_data: List of dictionaries with sample rows
        column_types: Dict mapping column names to SQL types

    Returns:
        PySpark DataFrame
    """
    spark = SparkSession.builder.getOrCreate()

    # Define schema based on column types
    type_mapping = {
        "INT": IntegerType(),
        "STRING": StringType(),
        "DECIMAL(10,2)": DecimalType(10, 2),
        "DATE": DateType(),
        "TIMESTAMP": TimestampType(),
    }

    fields = []
    for col_name in sample_data[0].keys():
        spark_type = type_mapping.get(
            column_types.get(col_name, "STRING"), StringType()
        )
        fields.append(StructField(col_name, spark_type, True))

    schema = StructType(fields)

    return spark.createDataFrame(sample_data, schema)


def create_model_function(
    mode: str,
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 8192,
) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """
    Create a model function compatible with mlflow.evaluate().

    Args:
        mode: Generation mode ('comment' or 'pi')
        model_endpoint: Databricks model serving endpoint name
        temperature: Temperature for LLM generation
        max_tokens: Maximum tokens for generation

    Returns:
        Function with signature: fn(inputs: Dict) -> Dict
    """

    # Get Databricks credentials for OpenAI client
    mlflow_creds = mlflow.utils.databricks_utils.get_databricks_host_creds()
    client = OpenAI(
        api_key=mlflow_creds.token, base_url=f"{mlflow_creds.host}/serving-endpoints"
    )

    @mlflow.trace
    def model_fn(inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate metadata using dbxmetagen prompts.

        Args:
            inputs: Dict with:
                - table_name: str
                - columns: List[str]
                - column_types: Dict[str, str]
                - sample_data: List[Dict]
                - metadata: Dict (optional)
                - mode: str

        Returns:
            Dict with 'response' key containing generated metadata
        """
        try:
            # Create minimal config (no YAML loading)
            config = MetadataConfig(
                skip_yaml_loading=True,
                catalog_name="eval",
                schema_name="eval",
                table_names=inputs["table_name"],
                mode=mode,
                temperature=temperature,
                max_tokens=max_tokens,
                model_endpoint=model_endpoint,
            )

            # Create PySpark DataFrame from sample data
            df = create_spark_df_from_sample(
                inputs["sample_data"], inputs["column_types"]
            )

            # Extract prompt using dbxmetagen's existing logic
            if mode == "comment":
                prompt_obj = CommentPrompt(config, df, inputs["table_name"])
            elif mode == "pi":
                prompt_obj = PIPrompt(config, df, inputs["table_name"])
            else:
                raise ValueError(f"Unsupported mode: {mode}")

            # Get the prompt template (this uses dbxmetagen's existing prompt logic)
            prompt_template = prompt_obj.create_prompt_template()
            messages = prompt_template[mode]

            # Call LLM via Databricks serving endpoint
            response = client.chat.completions.create(
                model=model_endpoint,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )

            # Extract response
            response_text = response.choices[0].message.content

            # Try to parse as JSON (dbxmetagen returns structured JSON)
            try:
                parsed_response = json.loads(response_text)
            except json.JSONDecodeError:
                # If not valid JSON, return raw text
                parsed_response = {
                    "raw_response": response_text,
                    "parse_error": "Could not parse JSON",
                }

            return {
                "response": parsed_response,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "model": model_endpoint,
            }

        except Exception as e:
            return {
                "response": {"error": str(e)},
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "model": model_endpoint,
            }

    return model_fn


# Pre-configured model functions for common scenarios
def create_comment_claude_model() -> Callable:
    """Create model function for comment mode with Claude."""
    return create_model_function(
        mode="comment", model_endpoint="databricks-claude-3-7-sonnet", temperature=0.1
    )


def create_comment_llama_model() -> Callable:
    """Create model function for comment mode with Llama."""
    return create_model_function(
        mode="comment",
        model_endpoint="databricks-meta-llama-3-1-405b-instruct",
        temperature=0.1,
    )


def create_pi_claude_model() -> Callable:
    """Create model function for PI mode with Claude."""
    return create_model_function(
        mode="pi", model_endpoint="databricks-claude-3-7-sonnet", temperature=0.1
    )


def create_pi_llama_model() -> Callable:
    """Create model function for PI mode with Llama."""
    return create_model_function(
        mode="pi",
        model_endpoint="databricks-meta-llama-3-1-405b-instruct",
        temperature=0.1,
    )


if __name__ == "__main__":
    # Test model function creation
    print("Testing model function creation...")

    # Create a test model
    model_fn = create_comment_claude_model()

    # Test input
    test_input = {
        "table_name": "test_table",
        "columns": ["id", "name"],
        "column_types": {"id": "INT", "name": "STRING"},
        "sample_data": [{"id": 1, "name": "Test"}],
        "mode": "comment",
    }

    print(f"Created model function: {model_fn}")
    print("Model function ready for use with mlflow.evaluate()")

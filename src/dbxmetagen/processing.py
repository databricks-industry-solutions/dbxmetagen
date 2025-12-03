"""Processing utilities shared among modules."""

import os
import shutil
import re
from abc import ABC
from datetime import datetime
from typing import List, Dict, Any, Tuple
import logging
import mlflow
import nest_asyncio
import pandas as pd
from pydantic import BaseModel, Field, Extra, ValidationError, ConfigDict
from pydantic.dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import (
    col,
    struct,
    to_timestamp,
    current_timestamp,
    lit,
    when,
    sum as spark_sum,
    max as spark_max,
    concat_ws,
    collect_list,
    collect_set,
    udf,
    trim,
    split,
    expr,
    split_part,
    regexp_replace,
    base64,
    to_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
    DoubleType,
    BinaryType,
)
from openai import OpenAI
from openai.types.chat.chat_completion import (
    Choice,
    ChatCompletion,
    ChatCompletionMessage,
)
import pandas as pd

try:
    from mlflow.types.llm import TokenUsageStats, ChatResponse
except ImportError:
    TokenUsageStats = None
    ChatResponse = None
from grpc._channel import _InactiveRpcError, _MultiThreadedRendezvous
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.sampling import determine_sampling_ratio
from src.dbxmetagen.prompts import Prompt, PIPrompt, CommentPrompt, PromptFactory
from src.dbxmetagen.error_handling import exponential_backoff, validate_csv
from src.dbxmetagen.comment_summarizer import TableCommentSummarizer
from src.dbxmetagen.metadata_generator import (
    Response,
    PIResponse,
    CommentResponse,
    PIColumnContent,
    MetadataGeneratorFactory,
    PIIdentifier,
    MetadataGenerator,
    CommentGenerator,
)
from src.dbxmetagen.overrides import (
    override_metadata_from_csv,
    apply_overrides_with_joins,
    build_condition,
    get_join_conditions,
)
from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user
from src.dbxmetagen.domain_classifier import load_domain_config, classify_table_domain

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Suppress verbose PySpark Connect logging to prevent GRPC stacktraces in notebooks
logging.getLogger("pyspark.sql.connect.client.logging").setLevel(logging.CRITICAL)


def extract_concise_error(error: Exception) -> str:
    """
    Extract a concise error message from an exception, removing JVM stacktraces.
    For tag policy violations, extracts just the relevant tag information.

    Args:
        error: The exception to extract the message from

    Returns:
        A concise error message string
    """
    error_str = str(error)
    if "INVALID_PARAMETER_VALUE" in error_str and "Tag value" in error_str:
        match = re.search(
            r"Tag value (\S+) is not an allowed value for tag policy key (\S+)",
            error_str,
        )
        if match:
            tag_value, tag_key = match.groups()
            return f"Tag policy violation: '{tag_key}' cannot be set to '{tag_value}'"
    return (
        error_str.split("JVM stacktrace:")[0].strip()
        if "JVM stacktrace:" in error_str
        else error_str
    )


class DDLGenerator(ABC):
    """DDLGenerator class."""

    def __init__(self):
        pass


class Input(BaseModel):
    """Input class."""

    ### Currently not implemented.
    model_config = ConfigDict(extra="forbid")

    table_name: str

    @classmethod
    def from_df(cls, df: DataFrame) -> Dict[str, Any]:
        """From DataFrame class."""
        return {
            "table_name": f"{catalog_name}.{schema_name}.{table_name}",
            "column_contents": cls.df.toPandas().to_dict(orient="list"),
        }


def tag_table(table_name: str, tags: Dict[str, str]) -> None:
    """
    Tags a table with the provided tags.

    Args:
        table_name (str): The name of the table to tag.
        tags (Dict[str, str]): A dictionary of tags to apply to the table.
    """
    spark = SparkSession.builder.getOrCreate()
    for key, value in tags.items():
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{key}' = '{value}');")


def write_to_log_table(log_data: Dict[str, Any], log_table_name: str) -> None:
    """
    Writes log data to a specified log table.

    Args:
        log_data (Dict[str, Any]): The log data to write.
        log_table_name (str): The name of the log table.
    """
    spark = SparkSession.builder.getOrCreate()
    log_df = spark.createDataFrame([log_data])
    log_df.write.format("delta").option("mergeSchema", "true").mode(
        "append"
    ).saveAsTable(log_table_name)


def count_df_columns(df: DataFrame) -> int:
    """
    Count the number of columns in a spark dataframe and return.
    """
    return len(df.columns)


def chunk_df(df: DataFrame, columns_per_call: int = 5) -> List[DataFrame]:
    """
    Splits a DataFrame into multiple DataFrames, each containing a specified number of columns.

    Args:
        df (DataFrame): The input DataFrame to split.
        columns_per_chunk (int, optional): The number of columns per chunk. Defaults to 10.

    Returns:
        List[DataFrame]: A list of DataFrames, each containing a subset of the original columns.
    """
    col_names = df.columns
    n_cols = count_df_columns(df)
    num_chunks = (n_cols + columns_per_call - 1) // columns_per_call

    dataframes = []
    for i in range(num_chunks):
        chunk_col_names = col_names[i * columns_per_call : (i + 1) * columns_per_call]
        chunk_df_var = df.select(chunk_col_names)
        dataframes.append(chunk_df_var)

    return dataframes


def get_extended_metadata_for_column(config, table_name, column_name):
    """Get extended metadata for a column."""
    spark = SparkSession.builder.getOrCreate()
    query = f"""DESCRIBE EXTENDED {config.catalog_name}.{config.schema_name}.{table_name} `{column_name}`;"""
    return spark.sql(query)


def convert_special_types_to_string(df: DataFrame) -> DataFrame:
    """
    Convert BINARY and VARIANT columns to string format for processing.
    
    - BINARY columns are encoded as base64 strings
    - VARIANT columns are converted to JSON strings
    
    Args:
        df (DataFrame): The DataFrame to convert.
    
    Returns:
        DataFrame: DataFrame with BINARY and VARIANT columns converted to strings.
    """
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        # Handle BINARY type - encode as base64
        if isinstance(col_type, BinaryType):
            print(f"Converting BINARY column '{col_name}' to base64 string")
            df = df.withColumn(col_name, base64(col(col_name)))
        
        # Handle VARIANT type - convert to JSON string
        # VARIANT type is represented as a string typename in Databricks
        elif str(col_type).upper() == "VARIANT":
            print(f"Converting VARIANT column '{col_name}' to JSON string")
            # For VARIANT type, we need to cast to string
            # Databricks automatically converts VARIANT to JSON when cast to string
            df = df.withColumn(col_name, col(col_name).cast("string"))
    
    return df


def sample_df(df: DataFrame, nrows: int, sample_size: int = 5) -> DataFrame:
    """
    Sample dataframe to a given size and filter out rows with lots of nulls.
    
    Automatically handles special data types (BINARY, VARIANT) by converting them to strings.

    Args:
        df (DataFrame): The DataFrame to be analyzed.
        nrows (int): number of rows in dataframe
        sample_size (int): The number of rows to sample.

    Returns:
        DataFrame: A DataFrame with columns to generate metadata for.
    """
    # Convert special types (BINARY, VARIANT) to strings before sampling
    df = convert_special_types_to_string(df)
    
    if nrows < sample_size:
        return df.limit(sample_size)

    larger_sample = sample_size * 100
    sampling_ratio = determine_sampling_ratio(nrows, larger_sample)
    sampled_df = df.sample(withReplacement=False, fraction=sampling_ratio)
    null_counts_per_row = sampled_df.withColumn(
        "null_count",
        sum(when(col(c).isNull(), 1).otherwise(0) for c in sampled_df.columns),
    )
    threshold = len(sampled_df.columns) // 2
    filtered_df = null_counts_per_row.filter(col("null_count") < threshold).drop(
        "null_count"
    )
    result_rows = filtered_df.count()
    if result_rows < sample_size:
        print(
            "Not enough non-NULL rows, returning available rows, despite large proportion of NULLs. Result rows:",
            result_rows,
            "vs sample size:",
            sample_size,
        )
        return df.limit(sample_size)

    print(f"Filtering {result_rows} result rows down to {sample_size} rows...")
    return filtered_df.limit(sample_size)


def append_table_row(
    rows: List[Row],
    full_table_name: str,
    response: Dict[str, Any],
    tokenized_full_table_name: str,
) -> List[Row]:
    """
    Appends a table row to the list of rows.

    Args:
        rows (List[Row]): The list of rows to append to.
        full_table_name (str): The full name of the table.
        response (Dict[str, Any]): The response dictionary containing table information.

    Returns:
        List[Row]: The updated list of rows with the new table row appended.
    """
    row = Row(
        table=full_table_name,
        tokenized_table=tokenized_full_table_name,
        ddl_type="table",
        column_name="None",
        column_content=str(
            response.table
        ),  # Ensure string type for serverless compatibility
    )
    rows.append(row)
    return rows


def append_domain_table_row(
    rows: List[Row],
    full_table_name: str,
    domain_result: Dict[str, Any],
    tokenized_full_table_name: str,
) -> List[Row]:
    """
    Appends a domain classification row to the list of rows.

    Args:
        rows (List[Row]): The list of rows to append to.
        full_table_name (str): The full name of the table.
        domain_result (Dict[str, Any]): The domain classification result.
        tokenized_full_table_name (str): The tokenized table name.

    Returns:
        List[Row]: The updated list of rows with the new domain row appended.
    """
    domain_comment = (
        f"Domain: {domain_result['domain']}"
        + (
            f" | Subdomain: {domain_result['subdomain']}"
            if domain_result.get("subdomain")
            else ""
        )
        + f" | Confidence: {domain_result['confidence']:.2f}"
        + f" | Reasoning: {domain_result['reasoning']}"
    )

    row = Row(
        table=full_table_name,
        tokenized_table=tokenized_full_table_name,
        ddl_type="table",
        column_name="None",
        column_content=domain_comment,
        domain=domain_result["domain"],
        subdomain=domain_result.get("subdomain", "None"),
        confidence=float(domain_result["confidence"]),
        recommended_domain=domain_result.get("recommended_domain", "None"),
        recommended_subdomain=domain_result.get("recommended_subdomain", "None"),
        reasoning=domain_result["reasoning"],
        metadata_summary=domain_result["metadata_summary"],
    )
    rows.append(row)
    return rows


def append_column_rows(
    config: MetadataConfig,
    rows: List[Row],
    full_table_name: str,
    response: Dict[str, Any],
    tokenized_full_table_name: str,
) -> List[Row]:
    """
    Appends column rows to the list of rows.

    Args:
        rows (List[Row]): The list of rows to append to.
        full_table_name (str): The full name of the table.
        response (Dict[str, Any]): The response dictionary containing column information.

    Returns:
        List[Row]: The updated list of rows with the new column rows appended.
    """
    # Parse presidio results for PI mode
    presidio_map = {}
    if (
        config.mode == "pi"
        and hasattr(response, "presidio_results")
        and response.presidio_results
    ):
        try:
            import json

            # Security: Log only metadata, not actual presidio results (may reference sensitive data)
            presidio_data = json.loads(response.presidio_results)
            num_results = len(presidio_data.get("deterministic_results", []))
            logger.debug(f"Parsing presidio_results with {num_results} column results")

            for result in presidio_data.get("deterministic_results", []):
                col = result.get("column")
                presidio_map[col] = json.dumps(result)
            print(f"Mapped presidio results for {len(presidio_map)} columns")
        except Exception as e:
            # Security: Do not log raw presidio data
            print(f"Failed to parse presidio results: {e}")
            logger.debug("Presidio parsing error details: %s", str(e)[:200])

    for i, (column_name, column_content) in enumerate(
        zip(response.columns, response.column_contents)
    ):
        if (
            isinstance(column_content, dict)
            or isinstance(column_content, PIColumnContent)
        ) and config.mode == "pi":
            if isinstance(column_content, PIColumnContent):
                column_content = column_content.model_dump()

            # Add presidio results for this column
            presidio_results = presidio_map.get(column_name, None)

            row = Row(
                table=full_table_name,
                tokenized_table=tokenized_full_table_name,
                ddl_type="column",
                column_name=column_name,
                classification=column_content.get("classification"),
                type=column_content.get("type"),
                confidence=column_content.get("confidence"),
                presidio_results=presidio_results,
            )
        elif isinstance(column_content, str) and config.mode == "comment":
            row = Row(
                table=full_table_name,
                tokenized_table=tokenized_full_table_name,
                ddl_type="column",
                column_name=column_name,
                column_content=column_content,
            )
        else:
            raise ValueError(
                f"Invalid column contents type: {type(column_content)} for column {column_name}"
            )
        rows.append(row)
    return rows


def define_row_schema(config):
    """
    Defines the schema for the row DataFrame.

    Args:
        config (MetadataConfig): The configuration object.

    Returns:
        StructType: The schema for the row DataFrame.
    """
    df_schema = None
    if config.mode == "pi":
        df_schema = StructType(
            [
                StructField("table", StringType(), True),
                StructField("tokenized_table", StringType(), True),
                StructField("ddl_type", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("classification", StringType(), True),
                StructField("type", StringType(), True),
                StructField("confidence", DoubleType(), True),
                StructField("presidio_results", StringType(), True),
            ]
        )
    elif config.mode == "comment":
        df_schema = StructType(
            [
                StructField("table", StringType(), True),
                StructField("tokenized_table", StringType(), True),
                StructField("ddl_type", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("column_content", StringType(), True),
            ]
        )
    elif config.mode == "domain":
        df_schema = StructType(
            [
                StructField("table", StringType(), True),
                StructField("tokenized_table", StringType(), True),
                StructField("ddl_type", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("column_content", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("subdomain", StringType(), True),
                StructField("confidence", StringType(), True),
                StructField("recommended_domain", StringType(), True),
                StructField("recommended_subdomain", StringType(), True),
                StructField("reasoning", StringType(), True),
                StructField("metadata_summary", StringType(), True),
            ]
        )
    return df_schema


def rows_to_df(rows: List[Row], config: MetadataConfig) -> DataFrame:
    """
    Converts a list of rows to a Spark DataFrame.

    Args:
        rows (List[Row]): The list of rows to convert.

    Returns:
        DataFrame: The Spark DataFrame created from the list of rows.
    """
    spark = SparkSession.builder.getOrCreate()
    if len(rows) == 0:
        return None
    else:
        schema = define_row_schema(config)
        try:
            df = spark.createDataFrame(rows, schema)
        except Exception as e:
            logger.debug("ERROR in DataFrame creation: %s", e)

            # TODO: evaluate for performance improvement
            for i, row in enumerate(rows):
                try:
                    test_df = spark.createDataFrame([row], schema)
                except Exception as row_error:
                    # Security: Do not log actual row data (may contain PII/PHI/PCI)
                    print(
                        f"Problematic row at index {i}: Schema mismatch - check field types"
                    )
                    logger.debug("Row error details: %s", str(row_error)[:200])
            raise

        if config.mode == "comment" and "column_content" in df.columns:
            df = df.withColumn("column_content", col("column_content").cast("string"))

        df = df.withColumn("_created_at", current_timestamp())
        return df


def add_ddl_to_column_comment_df(df: DataFrame, ddl_column: str) -> DataFrame:
    """
    Adds a DDL statement to a DataFrame for column comment.

    Args:
        df (DataFrame): The DataFrame to add the DDL statement to.
        ddl_column (str): The name of the DDL column.

    Returns:
        DataFrame: The updated DataFrame with the DDL statement added.
    """
    if "column_content" in df.columns:
        df = df.withColumn(
            "column_content",
            regexp_replace(col("column_content").cast("string"), "''", "'"),
        )

    result_df = df.withColumn(
        ddl_column,
        generate_column_comment_ddl("tokenized_table", "column_name", "column_content"),
    )

    if "column_content" in result_df.columns:
        result_df = result_df.withColumn(
            "column_content", col("column_content").cast("string")
        )

    return result_df


def add_ddl_to_table_comment_df(df: DataFrame, ddl_column: str) -> DataFrame:
    """
    Adds a DDL statement to a DataFrame for table comment.

    Args:
        df (DataFrame): The DataFrame to add the DDL statement to.
        ddl_column (str): The name of the DDL column.

    Returns:
        DataFrame: The updated DataFrame with the DDL statement added.
    """
    if df is not None and "column_content" in df.columns:
        df = df.withColumn(
            "column_content",
            regexp_replace(col("column_content").cast("string"), "''", "'"),
        )

    if df is not None:
        result_df = df.withColumn(
            ddl_column, generate_table_comment_ddl("tokenized_table", "column_content")
        )

        if "column_content" in result_df.columns:

            result_df = result_df.withColumn(
                "column_content", col("column_content").cast("string")
            )

        return result_df
    else:
        return df


def add_table_ddl_to_pi_df(config, df: DataFrame, ddl_column: str) -> DataFrame:
    """
    Adds a DDL statement to a DataFrame for PI information.

    Args:
        config: MetadataConfig with tag name configuration
        df (DataFrame): The DataFrame to add the DDL statement to.
        ddl_column (str): The name of the DDL column.

    Returns:
        DataFrame: The updated DataFrame with the DDL statement added.
    """
    # Create UDF with config-specific tag names
    generate_table_pi_ddl = udf(
        _create_table_pi_information_ddl_func(config), StringType()
    )

    return df.withColumn(
        ddl_column,
        generate_table_pi_ddl("tokenized_table", "classification", "type"),
    )


def add_column_ddl_to_pi_df(config, df: DataFrame, ddl_column: str) -> DataFrame:
    """
    Adds a DDL statement to a DataFrame for PI information.

    Args:
        config: MetadataConfig with tag name configuration
        df (DataFrame): The DataFrame to add the DDL statement to.
        ddl_column (str): The name of the DDL column.

    Returns:
        DataFrame: The updated DataFrame with the DDL statement added.
    """
    # Create UDF with config-specific tag names
    generate_pi_ddl = udf(_create_pi_information_ddl_func(config), StringType())

    if not config.tag_none_fields:
        df = df.filter(col("type") != "None")
    df = df.withColumn(
        ddl_column,
        generate_pi_ddl("tokenized_table", "column_name", "classification", "type"),
    )
    return df


def df_to_sql_file(
    df: DataFrame,
    catalog_name: str,
    dest_schema_name: str,
    sql_column: str,
    filename: str,
) -> str:
    """
    Writes a DataFrame to a SQL file using Spark-native operations (no collect()).

    Args:
        df (DataFrame): The DataFrame to write.
        catalog_name (str): The catalog name.
        dest_schema_name (str): The destination schema name.
        table_name (str): The table name.
        volume_name (str): The volume name.
        sql_column (str): The name of the SQL column.
        filename (str): The name of the file.

    Returns:
        str: The path to the SQL file.
    """
    print("Converting dataframe to SQL file using Spark-native operations...")
    selected_column_df = df.select(sql_column)
    uc_volume_path = f"/Volumes/{catalog_name}/{dest_schema_name}/{filename}.sql"

    temp_path = f"/Volumes/{catalog_name}/{dest_schema_name}/temp_{filename}"
    selected_column_df.coalesce(1).write.mode("overwrite").text(temp_path)

    part_files = [f for f in os.listdir(temp_path) if f.startswith("part-")]
    if part_files:
        shutil.move(os.path.join(temp_path, part_files[0]), uc_volume_path)
        shutil.rmtree(temp_path)

    return uc_volume_path


class DataFrameToExcelError(Exception):
    """Custom exception for DataFrame to Excel export errors."""


def ensure_directory_exists(directory_path: str) -> None:
    """
    Ensures that the specified directory exists, creating it if necessary.

    Args:
        directory_path (str): The directory to check or create.

    Raises:
        DataFrameToExcelError: If directory creation fails.
    """
    try:
        if not os.path.exists(directory_path):
            os.mkdir(directory_path)
            logger.info(f"Created directory: {directory_path}")
    except Exception as e:
        logger.error(f"Failed to create directory {directory_path}: {e}")
        raise DataFrameToExcelError(f"Directory creation failed: {e}")


def df_column_to_excel_file(
    df: pd.DataFrame, filename: str, base_path: str, excel_column: str
) -> str:
    """
    Exports a specified column from a DataFrame to an Excel file.

    Args:
        df (pd.DataFrame): The DataFrame to export.
        filename (str): The name of the output Excel file (without extension).
        volume_name (str): Volume name (used in path).
        excel_column (str): The column to export.

    Returns:
        str: The path to the created Excel file.

    Raises:
        DataFrameToExcelError: If export fails.
    """
    logger.info("Starting export of DataFrame column to Excel.")
    try:
        if excel_column not in df.columns:
            logger.error(f"Column '{excel_column}' not found in DataFrame.")
            raise DataFrameToExcelError(
                f"Column '{excel_column}' does not exist in DataFrame."
            )

        output_dir = base_path
        ensure_directory_exists(output_dir)
        excel_file_path = os.path.join(output_dir, f"{filename}.xlsx")
        local_path = f"/local_disk0/tmp/{filename}.xlsx"
        df[[excel_column]].to_excel(local_path, index=False, engine="openpyxl")

        # Use Databricks SDK WorkspaceClient for UC volume compatibility
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()

            with open(local_path, "rb") as src_file:
                excel_content = src_file.read()

            # Upload using WorkspaceClient (handles UC volumes properly)
            w.files.upload(excel_file_path, excel_content, overwrite=True)

        except Exception:
            # Fallback to direct file write
            with open(local_path, "rb") as src_file:
                with open(excel_file_path, "wb") as dest_file:
                    dest_file.write(src_file.read())
        logger.info(
            f"Successfully wrote column '{excel_column}' to Excel file: {excel_file_path}"
        )

        if not os.path.isfile(excel_file_path):
            logger.error(f"Excel file was not created: {excel_file_path}")
            raise DataFrameToExcelError(
                f"Excel file was not created: {excel_file_path}"
            )

        print(f"Excel file created at: {excel_file_path}")
        return excel_file_path

    except Exception as e:
        logger.error(f"Error exporting DataFrame to Excel: {e}")
        raise DataFrameToExcelError(f"Failed to export DataFrame to Excel: {e}")


def populate_log_table(df, config, current_user, base_path):
    """
    Populates the log table with the necessary columns.

    Args:
        df (DataFrame): The DataFrame to populate.
        config (MetadataConfig): The configuration.
        current_user (str): The current user.
        base_path (str): The base path.

    Returns:
        DataFrame: The result DataFrame.
    """
    # For serverless compatibility, ensure consistent data types without forcing string conversion
    # This maintains compatibility with existing table schemas

    # CRITICAL FIX: Explicitly cast all literal columns for serverless compatibility
    result_df = None
    if df is not None:
        result_df = (
            df.withColumn("current_user", lit(current_user).cast("string"))
            .withColumn("model", lit(config.model).cast("string"))
            .withColumn("sample_size", lit(config.sample_size).cast("int"))
            .withColumn("max_tokens", lit(config.max_tokens).cast("int"))
            .withColumn("temperature", lit(config.temperature).cast("double"))
            .withColumn("columns_per_call", lit(config.columns_per_call).cast("int"))
            .withColumn("status", lit("No Volume specified...").cast("string"))
        )
        # CRITICAL SERVERLESS FIX: Ensure column_content stays as string after adding log columns
        if config.mode == "comment" and "column_content" in result_df.columns:
            result_df = result_df.withColumn(
                "column_content", col("column_content").cast("string")
            )
    else:
        logger.info(
            "df is none during processing. Expected for table df for pi, and column df for domain."
        )

    return result_df


def get_control_table(config: MetadataConfig) -> str:
    """
    Returns the control table name based on the provided configuration.

    Args:
        config (MetadataConfig): Configuration object containing setup and model parameters.

    Returns:
        str: The control table name.
    """
    spark = SparkSession.builder.getOrCreate()
    if config.job_id and (
        config.cleanup_control_table == "true" or config.cleanup_control_table == True
    ):
        formatted_control_table = config.control_table.format(
            sanitize_user_identifier(get_current_user())
        ) + str(config.job_id)
    else:
        formatted_control_table = config.control_table.format(
            sanitize_user_identifier(get_current_user())
        )
    return formatted_control_table


def mark_as_deleted(table_name: str, config: MetadataConfig) -> None:
    """
    Updates the _deleted_at and _updated_at columns to the current timestamp for the specified table.

    Args:
        table_name (str): The name of the table to update.
        config (MetadataConfig): Configuration object containing setup and model parameters.
    """
    spark = SparkSession.builder.getOrCreate()
    formatted_control_table = get_control_table(config)
    control_table = (
        f"{config.catalog_name}.{config.schema_name}.{formatted_control_table}"
    )
    update_query = f"""
    UPDATE {control_table}
    SET _deleted_at = current_timestamp(),
        _updated_at = current_timestamp()
    WHERE table_name = '{table_name}'
    """
    spark.sql(update_query)
    print(f"Marked {table_name} as deleted in the control table...")


def run_log_table_ddl(config):
    """Run the unified log table DDL."""
    spark = SparkSession.builder.getOrCreate()
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {config.catalog_name}.{config.schema_name}.metadata_generation_log (
        metadata_type STRING,
        table STRING, 
        tokenized_table STRING, 
        ddl_type STRING, 
        column_name STRING,
        _created_at TIMESTAMP,
        column_content STRING,
        classification STRING,
        type STRING,
        confidence DOUBLE,
        presidio_results STRING,
        domain STRING,
        subdomain STRING,
        recommended_domain STRING,
        recommended_subdomain STRING,
        reasoning STRING,
        metadata_summary STRING,
        catalog STRING, 
        schema STRING, 
        table_name STRING, 
        ddl STRING, 
        current_user STRING, 
        model STRING, 
        sample_size INT, 
        max_tokens INT, 
        temperature DOUBLE, 
        columns_per_call INT, 
        status STRING
    )"""
    )


def output_df_pandas_to_tsv(df, output_file):
    pandas_df = df.toPandas()
    write_header = not os.path.exists(output_file)
    pandas_df.to_csv(output_file, sep="\t", header=write_header, index=False, mode="a")


def _export_table_to_tsv(df, config):
    """
    Reads a table from Databricks, writes it as a TSV file to a volume, and drops the original table.

    Args:
        df: Spark DataFrame to export.
        config: Configuration object containing catalog_name, schema_name, mode, current_user, volume_name, etc.

    Returns:
        str: Table name if operation was successful, False otherwise.
    """
    try:
        required_attrs = ["catalog_name", "schema_name", "mode", "volume_name"]
        for attr in required_attrs:
            if not hasattr(config, attr) or not getattr(config, attr):
                raise ValueError(f"Missing or empty required config attribute: {attr}")

        if df is None:
            raise ValueError("Input DataFrame is None.")
        if not hasattr(df, "count") or not callable(getattr(df, "count")):
            raise TypeError("Input is not a valid Spark DataFrame.")

        current_user = get_current_user()  # FIX: Define current_user
        current_user_sanitized = sanitize_user_identifier(current_user)
        date = datetime.now().strftime("%Y%m%d")
        if not hasattr(config, "log_timestamp") or not config.log_timestamp:
            config.log_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        filename = f"review_metadata_{config.mode}_{config.log_timestamp}.tsv"
        local_path = f"/local_disk0/tmp/{filename}"
        # Use unique temp table name for concurrent job safety
        table_name = config.get_temp_metadata_log_table_name()
        volume_path = (
            f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}"
        )
        folder_path = (
            f"{volume_path}/{current_user_sanitized}/{date}/exportable_run_logs/"
        )
        output_file = (
            f"{folder_path}review_metadata_{config.mode}_{config.log_timestamp}.tsv"
        )

        try:
            create_folder_if_not_exists(folder_path)
        except Exception as e:
            print(f"Error creating output directory '{folder_path}': {str(e)}")
            return False

        try:
            row_count = df.count()
        except Exception as e:
            print(f"Error counting rows in DataFrame: {str(e)}")
            return False
        if row_count == 0:
            print("Warning: Table is empty")

        print(f"Writing to TSV file: {output_file}")

        try:
            df.write.mode("append").saveAsTable(table_name)
        except Exception as e:
            print(f"Error writing Spark DataFrame to table '{table_name}': {str(e)}")
            return False

        try:
            # Write to local temp file first
            output_df_pandas_to_tsv(df, local_path)

            # Use Databricks SDK WorkspaceClient for proper Unity Catalog volume writing
            try:
                from databricks.sdk import WorkspaceClient

                w = WorkspaceClient()

                # Read the local file content
                with open(local_path, "rb") as src_file:
                    file_content = src_file.read()

                # Upload to Unity Catalog volume using WorkspaceClient (creates directories automatically)
                w.files.upload(output_file, file_content, overwrite=True)
                print(f"Successfully wrote TSV file to: {output_file}")

            except Exception as upload_error:
                print(f"WorkspaceClient upload failed: {upload_error}")
                # Fallback: try direct file write
                print("Attempting direct file write fallback...")
                with open(local_path, "r", encoding="utf-8") as src_file:
                    tsv_content = src_file.read()
                with open(output_file, "w", encoding="utf-8") as dest_file:
                    dest_file.write(tsv_content)
                print(f"Successfully wrote TSV file via fallback: {output_file}")

        except Exception as e:
            print(f"Error writing DataFrame to TSV file '{output_file}': {str(e)}")
            return False

        print("Export completed successfully...")
        return table_name

    except ValueError as ve:
        print(f"ValueError: {str(ve)}")
        return False
    except TypeError as te:
        print(f"TypeError: {str(te)}")
        return False
    except Exception as e:
        print(f"Unexpected error during full log export process: {str(e)}")
        return False


def eval_disable_medical_information_value(config: MetadataConfig) -> bool:
    return (
        config.disable_medical_information_value == "true"
        or config.disable_medical_information_value
        or config.disable_medical_information_value == "True"
    )


class ExportError(Exception):
    """Custom exception for export errors."""


def create_folder_if_not_exists(path: str) -> None:
    """Create directory if it doesn't exist. For Unity Catalog volumes, directories are created automatically."""
    try:
        if not os.path.exists(path):
            os.makedirs(path)
            logger.info(f"Created directory: {path}")
    except Exception as e:
        logger.error(f"Failed to create directory {path}: {e}")
        raise ExportError(f"Directory creation failed: {e}") from e


def export_df_to_excel(df: pd.DataFrame, output_file: str, export_folder: str) -> None:
    try:
        local_path = f"/local_disk0/tmp/{output_file}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if not os.path.exists(local_path):
            df.to_excel(local_path, index=False)
        else:
            with pd.ExcelWriter(
                local_path, engine="openpyxl", mode="a", if_sheet_exists="overlay"
            ) as writer:
                df.to_excel(
                    writer,
                    sheet_name="Sheet1",
                    startrow=writer.sheets["Sheet1"].max_row,
                    header=False,
                    index=False,
                )
        # Use Databricks SDK WorkspaceClient for UC volume compatibility
        volume_output_path = os.path.join(export_folder, output_file)
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()

            with open(local_path, "rb") as src_file:
                excel_content = src_file.read()

            # Upload using WorkspaceClient (handles UC volumes properly)
            w.files.upload(volume_output_path, excel_content, overwrite=True)

        except Exception:
            # Fallback to direct file write
            with open(local_path, "rb") as src_file:
                with open(volume_output_path, "wb") as dest_file:
                    dest_file.write(src_file.read())
        logger.info(f"Excel file created at: {output_file}")
    except Exception as e:
        logger.error(f"Failed to export DataFrame to Excel: {e}")
        raise ExportError(f"Failed to export DataFrame to Excel: {e}")


def _export_table_to_excel(df: Any, config: Any) -> str:
    """
    Reads a table from Databricks, writes it as an Excel file to a volume, and drops the original table.

    Args:
        df: DataFrame to export (Spark or pandas)
        config: Configuration object containing catalog_name, schema_name, mode, current_user, and volume_name

    Returns:
        str: The path to the Excel file if successful

    Raises:
        ExportError: If export fails
    """
    date = datetime.now().strftime("%Y%m%d")
    if not hasattr(config, "log_timestamp") or not config.log_timestamp:
        config.log_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    timestamp = config.log_timestamp

    try:
        # Use unique temp table name for concurrent job safety
        table_name = config.get_temp_metadata_log_table_name()
        volume_path = (
            f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}"
        )
        export_folder = f"{volume_path}/{current_user}/{date}/exportable_run_logs/"
        create_folder_if_not_exists(export_folder)
        output_filename = f"review_metadata_{config.mode}_{config.log_timestamp}.xlsx"
        output_file = f"{export_folder}{output_filename}"

        if hasattr(df, "count") and callable(df.count):
            if df.count() == 0:
                print("Warning: Table is empty")
                logger.warning("Table is empty")
        elif isinstance(df, pd.DataFrame) and df.empty:
            print("Warning: Table is empty")
            logger.warning("Table is empty")

        if hasattr(df, "toPandas") and callable(df.toPandas):
            pdf = df.toPandas()
        elif isinstance(df, pd.DataFrame):
            pdf = df
        else:
            logger.error("Unsupported DataFrame type")
            raise ExportError("Unsupported DataFrame type")

        print(f"Writing to Excel file: {output_filename}")
        logger.info(f"Writing to Excel file: {output_filename}")
        export_df_to_excel(pdf, output_filename, export_folder)

        print("Export completed successfully")
        logger.info("Export completed successfully")
        return output_file

    except Exception as e:
        print(f"Error during full log export process: {str(e)}")
        logger.error(f"Error during full log export process: {str(e)}")
        raise ExportError(f"Error during full log export process: {str(e)}")


def log_metadata_generation(
    df: DataFrame, config: MetadataConfig, table_name: str, volume_name: str
) -> None:
    """
    Log the metadata generation to the unified log table.
    """
    run_log_table_ddl(config)
    df = df.withColumn("metadata_type", lit(config.mode))

    print(f"[log_metadata_generation] DataFrame columns: {df.columns}")

    df.write.mode("append").option("mergeSchema", "true").saveAsTable(
        f"{config.catalog_name}.{config.schema_name}.metadata_generation_log"
    )
    mark_as_deleted(table_name, config)


# TODO: Figure out where this is used and if it is needed
def set_classification_to_null(df: DataFrame, config: MetadataConfig) -> DataFrame:
    """
    Set the classification to null.
    """
    if config.mode == "pi":
        df = df.withColumn("classification", lit(None))
    return df


def set_protected_classification(df: DataFrame, config: MetadataConfig) -> DataFrame:
    """
    Set the classification to protected.
    """
    if not df:
        return None

    if config.mode == "pi":
        df = df.withColumn(
            "classification",
            when(
                (col("type") == "pii")
                | (col("type") == "pci")
                | (col("type") == "medical_information")
                | (col("type") == "phi"),
                lit("protected"),
            ).otherwise(col("classification")),
        )
    return df


def replace_medical_information_with_phi(
    df: DataFrame, config: MetadataConfig
) -> DataFrame:
    """
    Replace the medical information with phi.
    """
    if not df:
        return None

    if config.mode == "pi" and eval_disable_medical_information_value(config):
        df = df.withColumn(
            "type",
            when((col("type") == "medical_information"), lit("phi")).otherwise(
                col("type")
            ),
        )
    return df


def filter_and_write_ddl(
    df: DataFrame,
    config: MetadataConfig,
    base_path: str,
    full_table_name: str,
    current_user: str,
    current_date: str,
) -> DataFrame:
    """Filter the DataFrame based on the table name and write the DDL statements to a SQL file.
    Args:
        df (DataFrame): The DataFrame containing the DDL statements.
        config: MetadataConfig
        base_path: str
        full_table_name: str
        current_user: str
        current_date: str
    """
    print(
        "Filtering dataframe based on table name to write DDL to SQL file in volume..."
    )

    table_name = re.sub(r"[^\w\s/]", "_", full_table_name)
    file_root = f"{table_name}_{config.mode}"

    try:
        write_ddl_to_volume_spark_native(
            df, file_root, base_path, config.ddl_output_format
        )
        df = df.withColumn("status", lit("Success"))
    except ValueError as ve:
        print(f"Error: {ve}")
        df = df.withColumn("status", lit("Failed: Invalid output format"))
    except IOError as ioe:
        print(
            f"Error writing DDL to volume: {ioe}. Check if Volume exists and if your permissions are correct."
        )
        df = df.withColumn("status", lit("Failed: IO Error"))
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = df.withColumn("status", lit("Failed: Unexpected error"))
    finally:
        log_metadata_generation(df, config, full_table_name, base_path)
        if config.reviewable_output_format == "excel":
            _export_table_to_excel(df, config)
        elif config.reviewable_output_format == "tsv":
            _export_table_to_tsv(df, config)
        else:
            raise ValueError(
                "Invalid output format for reviewable_output_format. Please choose either 'excel' or 'tsv'."
            )


def write_ddl_to_volume_spark_native(
    df: DataFrame, file_name: str, base_path: str, output_format: str
):
    """
    Write DDL statements to volume using collect() - simpler approach for compatibility.
    """
    try:
        create_folder_if_not_exists(base_path)
    except Exception as e:
        print(
            f"Error creating folder: {e}. Check if Volume exists and if your permissions are correct."
        )

    if output_format in ["sql", "tsv"]:
        nrows = df.count()
        try:
            df.select("ddl").toPandas()
        except Exception as e:
            print(f"Error: {e}")
            raise ValueError(f"Error: {e}")
        try:
            df.select("ddl").coalesce(1)
        except Exception as e:
            print(f"Error: {e}")
            raise ValueError(f"Error: {e}")

        # Use collect() - the original approach that worked on traditional clusters
        ddl_statements = df.select("ddl").collect()

        # Write using simple Python file I/O
        full_path = os.path.join(base_path, f"{file_name}.{output_format}")
        with open(full_path, "w") as file:
            for statement in ddl_statements:
                file.write(f"{statement[0]}\n")

    elif output_format == "excel":
        # For Excel, convert to list first then to pandas
        ddl_statements = df.select("ddl").collect()
        ddl_list = [row.ddl for row in ddl_statements]

        pdf = pd.DataFrame(ddl_list, columns=["ddl"])
        df_column_to_excel_file(pdf, file_name, base_path, "ddl")
    else:
        raise ValueError(
            "Invalid output format. Please choose either 'sql', 'tsv' or 'excel'."
        )


def write_ddl_to_volume(file_name, base_path, ddl_statements, output_format):
    """Legacy function kept for backward compatibility"""
    try:
        create_folder_if_not_exists(base_path)
    except Exception as e:
        print(
            f"Error creating folder: {e}. Check if Volume exists and if your permissions are correct."
        )
    if output_format in ["sql", "tsv"]:
        full_path = os.path.join(base_path, f"{file_name}.{output_format}")
        with open(full_path, "w") as file:
            for statement in ddl_statements:
                file.write(f"{statement[0]}\n")
    elif output_format == "excel":
        ddl_list = [row.ddl for row in ddl_statements]
        df = pd.DataFrame(ddl_list, columns=["ddl"])
        df_column_to_excel_file(df, file_name, base_path, "ddl")
    else:
        raise ValueError(
            "Invalid output format. Please choose either 'sql', 'tsv' or 'excel'."
        )


def create_and_persist_ddl(
    df: DataFrame, config: MetadataConfig, table_name: str
) -> None:
    """
    Writes the DDL statements from the DataFrame to a volume as SQL files.

    Args:
        df (DataFrame): The DataFrame containing the DDL statements.
        catalog (str): The catalog name.
        dest_schema (str): The destination schema name.
        table_name (str): A list of table names.
        volume_name (str): The volume name.
    """
    print("Running create and persist ddl...")
    current_user = get_current_user()
    current_user_sanitized = sanitize_user_identifier(current_user)
    current_date = datetime.now().strftime("%Y%m%d")
    if config.volume_name:
        base_path = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/{current_user_sanitized}/{current_date}"
        table_df = df[f"{config.mode}_table_df"]
        table_df = populate_log_table(table_df, config, current_user, base_path)
        modified_path = re.sub(r"[^\w\s/]", "_", base_path)
        column_df = df[f"{config.mode}_column_df"]
        column_df = populate_log_table(column_df, config, current_user, base_path)
        modified_path = re.sub(r"[^\w\s/]", "_", base_path)

        # Ensure schema compatibility before union for serverless environments
        if config.mode == "comment":
            # Handle case where table_df might be None (no table-level content)
            if table_df is not None:
                table_df = table_df.withColumn(
                    "column_content", col("column_content").cast("string")
                )
            if column_df is not None:
                column_df = column_df.withColumn(
                    "column_content", col("column_content").cast("string")
                )

            # Handle union with potential None DataFrames
            if table_df is not None and column_df is not None:
                # CRITICAL FIX: Ensure explicit column ordering before union for serverless
                # This prevents Spark Connect from doing implicit casting
                if config.mode == "comment":
                    expected_columns = [
                        "table",
                        "tokenized_table",
                        "ddl_type",
                        "column_name",
                        "column_content",
                        "_created_at",
                        "catalog",
                        "schema",
                        "table_name",
                        "ddl",
                        "current_user",
                        "model",
                        "sample_size",
                        "max_tokens",
                        "temperature",
                        "columns_per_call",
                        "status",
                    ]
                elif config.mode == "pi":
                    expected_columns = [
                        "table",
                        "tokenized_table",
                        "ddl_type",
                        "column_name",
                        "classification",
                        "type",
                        "confidence",
                        "_created_at",
                        "catalog",
                        "schema",
                        "table_name",
                        "ddl",
                        "current_user",
                        "model",
                        "sample_size",
                        "max_tokens",
                        "temperature",
                        "columns_per_call",
                        "status",
                    ]
                elif config.mode == "domain":
                    expected_columns = [
                        "table",
                        "tokenized_table",
                        "ddl_type",
                        "column_name",
                        "column_content",
                        "domain",
                        "subdomain",
                        "confidence",
                        "recommended_domain",
                        "recommended_subdomain",
                        "reasoning",
                        "metadata_summary",
                        "_created_at",
                        "catalog",
                        "schema",
                        "table_name",
                        "ddl",
                        "current_user",
                        "model",
                        "sample_size",
                        "max_tokens",
                        "temperature",
                        "columns_per_call",
                        "status",
                    ]
                else:
                    raise ValueError(f"Unsupported mode: {config.mode}")

                table_df_ordered = table_df.select(*expected_columns)
                column_df_ordered = column_df.select(*expected_columns)

                table_count = table_df_ordered.count()
                column_count = column_df_ordered.count()
                unioned_df = table_df_ordered.union(column_df_ordered)
            elif table_df is not None:
                unioned_df = table_df
            elif column_df is not None:
                unioned_df = column_df
            else:
                raise ValueError("Both table and column DataFrames are None")
        elif config.mode == "pi":
            # Handle case where table_df might be None (no table-level content)
            if table_df is not None:
                # Ensure confidence column is properly cast to DOUBLE
                table_df = table_df.withColumn(
                    "confidence", col("confidence").cast("double")
                )
                # Ensure classification and type are properly cast to STRING
                table_df = table_df.withColumn(
                    "classification", col("classification").cast("string")
                )
                table_df = table_df.withColumn("type", col("type").cast("string"))
            if column_df is not None:
                # Ensure confidence column is properly cast to DOUBLE
                column_df = column_df.withColumn(
                    "confidence", col("confidence").cast("double")
                )
                # Ensure classification and type are properly cast to STRING
                column_df = column_df.withColumn(
                    "classification", col("classification").cast("string")
                )
                column_df = column_df.withColumn("type", col("type").cast("string"))

            # Handle union with potential None DataFrames
            if table_df is not None and column_df is not None:

                # CRITICAL FIX: Ensure explicit column ordering before union for serverless
                # This prevents Spark Connect from doing implicit casting
                expected_columns = [
                    "table",
                    "tokenized_table",
                    "ddl_type",
                    "column_name",
                    "classification",
                    "type",
                    "confidence",
                    "presidio_results",
                    "_created_at",
                    "catalog",
                    "schema",
                    "table_name",
                    "ddl",
                    "current_user",
                    "model",
                    "sample_size",
                    "max_tokens",
                    "temperature",
                    "columns_per_call",
                    "status",
                ]

                # Explicitly select columns in the same order for both DataFrames
                table_df_ordered = table_df.select(*expected_columns)
                column_df_ordered = column_df.select(*expected_columns)

                unioned_df = table_df_ordered.union(column_df_ordered)

            elif table_df is not None:
                unioned_df = table_df
            elif column_df is not None:
                unioned_df = column_df
            else:
                raise ValueError("Both table and column DataFrames are None")
        elif config.mode == "domain":
            # Domain mode: table-level only, no column-level DataFrame
            if table_df is not None:
                # Ensure domain columns are properly cast
                table_df = table_df.withColumn(
                    "column_content", col("column_content").cast("string")
                )
                table_df = table_df.withColumn("domain", col("domain").cast("string"))
                table_df = table_df.withColumn(
                    "subdomain", col("subdomain").cast("string")
                )
                table_df = table_df.withColumn(
                    "confidence", col("confidence").cast("double")
                )
                table_df = table_df.withColumn(
                    "recommended_domain", col("recommended_domain").cast("string")
                )
                table_df = table_df.withColumn(
                    "recommended_subdomain", col("recommended_subdomain").cast("string")
                )
                table_df = table_df.withColumn(
                    "reasoning", col("reasoning").cast("string")
                )
                table_df = table_df.withColumn(
                    "metadata_summary", col("metadata_summary").cast("string")
                )
                unioned_df = table_df
            else:
                raise ValueError("Domain mode requires table_df to be non-None")
        else:
            raise ValueError(
                f"Invalid mode: {config.mode}. Expected 'comment', 'pi', or 'domain'."
            )
        filter_and_write_ddl(
            unioned_df, config, modified_path, table_name, current_user, current_date
        )
    else:
        print(
            "Volume name provided as None in configuration. Not writing DDL to volume..."
        )
        table_df = populate_log_table(
            df["comment_table_df"], config, current_user, base_path
        )
        log_metadata_generation(table_df, config, table_name, base_path)
        column_df = populate_log_table(
            df["comment_column_df"], config, current_user, base_path
        )
        log_metadata_generation(column_df, config, table_name, base_path)


# TODO: Update this to use get_generated_metadata_data_unaware() if sample size is 0 so Presidio can still use data.
def get_domain_classification(
    config: MetadataConfig, full_table_name: str
) -> Dict[str, Any]:
    """
    Generates domain classification for a given table.

    Note: Domain classification only uses the first N columns (defined by columns_per_call)
    to avoid massive prompts. In testing, along with table comments this is typically sufficient to determine business domain.

    Args:
        config: Configuration object
        full_table_name: Full table name (catalog.schema.table)

    Returns:
        Dict containing domain classification results
    """

    spark = SparkSession.builder.getOrCreate()

    domain_config = load_domain_config(config.domain_config_path)

    df = spark.read.table(full_table_name)
    total_columns = len(df.columns)

    # Limit columns for domain classification to avoid massive prompts
    # Use only the first chunk of columns (defined by columns_per_call)
    chunked_dfs = chunk_df(df, config.columns_per_call)
    first_chunk_df = chunked_dfs[0] if chunked_dfs else df
    columns_used = len(first_chunk_df.columns)

    logger.info(
        f"Domain classification for {full_table_name}: Using {columns_used}/{total_columns} columns (limited by columns_per_call={config.columns_per_call})"
    )

    # Sample rows from the limited column set
    sampled_df = sample_df(first_chunk_df, first_chunk_df.count(), config.sample_size)

    prompt = PromptFactory.create_prompt(config, sampled_df, full_table_name)
    prompt_messages = prompt.create_prompt_template()

    # Check prompt length to avoid excessive token usage
    check_token_length_against_num_words(prompt_messages, config)

    table_metadata = {
        "column_contents": prompt.prompt_content.get("column_contents", {}),
    }

    if config.add_metadata:
        table_metadata["column_metadata"] = prompt.prompt_content.get(
            "column_contents", {}
        ).get("column_metadata", {})
        table_metadata["table_tags"] = prompt.prompt_content.get(
            "column_contents", {}
        ).get("table_tags", "")
        table_metadata["table_constraints"] = prompt.prompt_content.get(
            "column_contents", {}
        ).get("table_constraints", "")
        table_metadata["table_comments"] = prompt.prompt_content.get(
            "column_contents", {}
        ).get("table_comments", "")

    # Classify the table
    classification_result = classify_table_domain(
        table_name=full_table_name,
        table_metadata=table_metadata,
        domain_config=domain_config,
        model_endpoint=config.model,
        temperature=config.temperature,
        max_tokens=config.max_tokens,
    )

    return classification_result


def get_generated_metadata(
    config: MetadataConfig, full_table_name: str
) -> List[Tuple[PIResponse, CommentResponse]]:
    """
    Generates metadata for a given table.
    Wraps get_generated_metadata_data_aware() to allow
    different handling when data is allowed versus disallowed.
    Currently no difference is implemented between the two routes,
    but in the future can be if needed.

    The intent here is to allow Presidio to be used,
    or skip the query to the table if data is not allowed.

    Currently, the table is still queried but with a
    limit of 0 rows if sample size is 0.

    Args:
        catalog (str): The catalog name.
        schema (str): The schema name.
        table_name (str): The table name.
        model (str): model name
        prompt_template (str): prompt template

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the generated metadata.
    """
    spark = SparkSession.builder.getOrCreate()

    if int(config.sample_size) == 0:
        responses = get_generated_metadata_data_aware(spark, config, full_table_name)
    elif int(config.sample_size) >= 1:
        responses = get_generated_metadata_data_aware(spark, config, full_table_name)
    else:
        raise ValueError(f"Invalid sample size: {config.sample_size}")
    return responses


def get_generated_metadata_data_aware(
    spark: SparkSession, config: MetadataConfig, full_table_name: str
):
    """
    Generates metadata for a given table.

    Args:
        spark (SparkSession): The Spark session.
        config (MetadataConfig): The configuration.
        full_table_name (str): The full table name.

    Returns:
        List[Tuple[PIResponse, CommentResponse]]: A list of tuples containing the generated metadata.
    """
    df = spark.read.table(full_table_name)
    responses = []
    nrows = df.count()
    chunked_dfs = chunk_df(df, config.columns_per_call)
    for i, chunk in enumerate(chunked_dfs):
        sampled_chunk = sample_df(chunk, nrows, config.sample_size)
        prompt = PromptFactory.create_prompt(config, sampled_chunk, full_table_name)
        prompt_messages = prompt.create_prompt_template()
        check_token_length_against_num_words(prompt_messages, config)
        if config.registered_model_name != "default":
            chat_response = call_registered_model(config)
        else:
            chat_response = MetadataGeneratorFactory.create_generator(config)
        response, _ = chat_response.get_responses(
            prompt_messages, prompt.prompt_content
        )
        # Store presidio results with the response for PI mode
        if hasattr(prompt, "deterministic_results"):
            response.presidio_results = prompt.deterministic_results
        responses.append(response)
    return responses


def check_token_length_against_num_words(prompt: str, config: MetadataConfig):
    """
    This function is not intended to catch every instance of overflowing token length, but to avoid significant overflow. Specifically, we compare the number of words in the prompt to the maximum number of tokens allowed in the model. If the number of words exceeds the maximum, an error is raised. This is potentially quite a conservative metric.
    """
    num_words = len(str(prompt).split())
    if num_words > config.max_prompt_length:
        raise ValueError(
            "Number of words in prompt exceeds max_tokens. Please reduce the number of columns or increase max_tokens."
        )
    else:
        return num_words


def review_and_generate_metadata(
    config: MetadataConfig, full_table_name: str
) -> Tuple[DataFrame, DataFrame]:
    """
    Reviews and generates metadata for a list of tables based on the mode.

    Args:
        catalog (str): The catalog name.
        schema (str): The schema name.
        table_names (str): A list of table names.
        model (str): model name
        mode (str): Mode to determine whether to process 'pi', 'comment', or 'domain'

    Returns:
        Tuple[DataFrame, DataFrame]: DataFrames containing the generated metadata.
    """
    table_rows = []
    column_rows = []

    if config.mode == "domain":
        domain_result = get_domain_classification(config, full_table_name)
        tokenized_full_table_name = replace_catalog_name(config, full_table_name)
        table_rows = append_domain_table_row(
            table_rows,
            full_table_name,
            domain_result,
            tokenized_full_table_name,
        )
        return rows_to_df(column_rows, config), rows_to_df(table_rows, config)

    # Standard flow for comment and pi modes
    responses = get_generated_metadata(config, full_table_name)
    for response in responses:
        tokenized_full_table_name = replace_catalog_name(config, full_table_name)
        if config.mode == "comment":
            table_rows = append_table_row(
                table_rows, full_table_name, response, tokenized_full_table_name
            )
        column_rows = append_column_rows(
            config, column_rows, full_table_name, response, tokenized_full_table_name
        )
    return rows_to_df(column_rows, config), rows_to_df(table_rows, config)


def replace_catalog_name(config, full_table_name):
    """
    Replaces __CATALOG_NAME__ in a string with the actual catalog name from a fully scoped table name.

    Args:
        config (MetadataConfig): Configuration object containing setup parameters.
        full_table_name (str): The fully scoped table name.

    Returns:
        str: The string with the catalog name replaced.
    """
    catalog_tokenizable = config.catalog_tokenizable
    parts = full_table_name.split(".")
    if len(parts) != 3:
        raise ValueError("full_table_name must be in the format 'catalog.schema.table'")
    catalog_name, schema_name, table_name = parts
    if config.format_catalog:
        replaced_catalog_name = catalog_tokenizable.replace(
            "__CATALOG_NAME__", catalog_name
        ).format(env=config.env)
    else:
        replaced_catalog_name = catalog_tokenizable.replace(
            "__CATALOG_NAME__", catalog_name
        )
    logger.debug("Replaced catalog name: %s", replaced_catalog_name)
    return f"{replaced_catalog_name}.{schema_name}.{table_name}"


def log_missing_governance_tags(error_msg: str, ddl_statement: str) -> None:
    """
    Log the missing governance tags.
    """

    missing_tags = []
    failed_statements = []

    if "TAG_NOT_FOUND" in error_msg or "tag" in error_msg.lower():
        if "SET TAGS" in ddl_statement:

            tag_pattern = r"'(\w+)'\s*="
            tags = re.findall(tag_pattern, ddl_statement)
            for tag in tags:
                if tag not in missing_tags:
                    missing_tags.append(tag)
            print(f"  Missing governance tags detected: {', '.join(tags)}")
            print(f"   Statement: {ddl_statement}")
            failed_statements.append(
                {
                    "statement": ddl_statement,
                    "error": "Missing governance tags",
                    "tags": tags,
                }
            )
        else:
            print(f"  Error applying DDL: {ddl_statement}")
            failed_statements.append({"statement": ddl_statement, "error": error_msg})
    else:
        print(f" Error applying DDL: {error_msg}")
        failed_statements.append({"statement": ddl_statement, "error": error_msg})
    return missing_tags, failed_statements


def apply_comment_ddl(df: DataFrame, config: MetadataConfig) -> dict:
    """
    Applies the comment DDL statements stored in the DataFrame to the table.

    Args:
        df (DataFrame): The DataFrame containing the DDL statements.

    Returns:
        dict: Summary of DDL application including any missing tags
    """
    spark = SparkSession.builder.getOrCreate()
    ddl_statements = df.select("ddl").collect()
    missing_tags = []
    failed_statements = []
    success_count = 0

    for row in ddl_statements:
        # Security: DDL statements may contain sensitive data in comments/tags
        # Log only metadata, not full DDL content
        ddl_statement = row["ddl"]
        logger.debug("Applying DDL statement (%d characters)", len(ddl_statement))

        if not config.dry_run:
            try:
                spark.sql(ddl_statement)
                success_count += 1
                print(
                    f"Applied DDL statement successfully ({len(ddl_statement)} chars)"
                )
            except Exception as e:
                # Extract concise error message
                concise_error = extract_concise_error(e)
                logger.error("Error applying DDL: %s", concise_error)
                print(f"Failed to apply DDL statement: {concise_error}")
                # Security: Only log DDL structure, not full content (may contain sensitive data in comments)
                logger.debug(
                    "DDL statement first 100 chars: %s...", ddl_statement[:100]
                )

                # Track failed tags for summary
                if "Tag policy violation" in concise_error:
                    match = re.search(
                        r"'(\w+)' cannot be set to '(\S+)'", concise_error
                    )
                    if match:
                        tag_key, tag_value = match.groups()
                        missing_tags.append(f"{tag_key}={tag_value}")

                failed_statements.append(
                    {"statement": ddl_statement, "error": concise_error}
                )

    if missing_tags:
        logger.warning(
            "Failed to set the following tags due to tag policy restrictions: %s",
            ", ".join(missing_tags),
        )

    return {
        "success_count": success_count,
        "failed_count": len(failed_statements),
        "missing_tags": missing_tags,
        "failed_statements": failed_statements,
    }


def split_and_hardcode_df(df, config):
    """
    Splits the DataFrame and hardcodes the classification.

    Works for both table and column DataFrames.

    Does not handle all mode types, need to manage the conditional in process_and_add_ddl.
    """
    if df is not None:
        df = split_name_for_df(df)
        df = hardcode_classification(df, config)
        return df

    logger.info(
        "df is none during processing. Expected for table df for pi, and column df for domain."
    )
    return None


def process_and_add_ddl(config: MetadataConfig, table_name: str) -> DataFrame:
    """
    Processes the metadata, splits the DataFrame based on 'table' values,
    applies DDL functions, and returns a unioned DataFrame.

    Args:
        catalog (str): The catalog name data is being read from and written to.
        dest_schema (str): The destination schema name.
        table_names (str): A list of table names.
        model (str): The model name.

    Returns:
        DataFrame: The unioned DataFrame with DDL statements added.
    """
    column_df, table_df = review_and_generate_metadata(config, table_name)
    column_df = split_and_hardcode_df(column_df, config)
    table_df = split_and_hardcode_df(table_df, config)

    if config.allow_manual_override and column_df is not None:
        logger.info("Overriding metadata from CSV...")
        column_df = override_metadata_from_csv(
            column_df, config.override_csv_path, config
        )

    dfs = add_ddl_to_dfs(config, table_df, column_df, table_name)
    return dfs


def hardcode_classification(df, config):
    """
    Hardcodes the classification for the DataFrame.

    Args:
        df (DataFrame): The DataFrame to hardcode the classification for.
        config (MetadataConfig): The configuration object.

    Returns:
        DataFrame: The DataFrame with the classification hardcoded.
    """
    df = replace_medical_information_with_phi(df, config)
    df = set_protected_classification(df, config)
    return df


def split_name_for_df(df):
    """
    Splits the fully scoped table name for the DataFrame.

    Args:
        df (DataFrame): The DataFrame to split the fully scoped table name for.

    Returns:
        DataFrame: The DataFrame with the fully scoped table name split.
    """
    if df is not None:
        has_column_content = "column_content" in df.columns
        if has_column_content:
            original_column_content_type = str(
                df.select("column_content").schema.fields[0].dataType
            )

        df = split_fully_scoped_table_name(df, "table")

        if has_column_content and "column_content" in df.columns:
            df = df.withColumn("column_content", col("column_content").cast("string"))
            final_column_content_type = str(
                df.select("column_content").schema.fields[0].dataType
            )

        logger.info(
            "df columns after generating metadata in process_and_add_ddl %s", df.columns
        )
    return df


def add_ddl_to_dfs(config, table_df, column_df, table_name):
    """
    Adds DDL to the DataFrames.

    Args:
        config (MetadataConfig): The configuration object.
        table_df (DataFrame): The table DataFrame.
        column_df (DataFrame): The column DataFrame.
        table_name (str): The name of the table.

    Raises:
        ValueError: If the mode is invalid.

    Returns:
        dict: A dictionary containing the DataFrames.
    """
    dfs = {}
    if config.mode == "comment":

        if table_df is not None:
            summarized_table_df = summarize_table_content(table_df, config, table_name)
            summarized_table_df = split_name_for_df(summarized_table_df)
        else:
            summarized_table_df = None

        if column_df is not None and "column_content" in column_df.columns:
            column_df = column_df.withColumn(
                "column_content", col("column_content").cast("string")
            )

        dfs["comment_table_df"] = add_ddl_to_table_comment_df(
            summarized_table_df, "ddl"
        )
        dfs["comment_column_df"] = add_ddl_to_column_comment_df(column_df, "ddl")

        if config.apply_ddl:
            dfs["ddl_results"] = apply_ddl_to_tables(dfs, config)
    elif config.mode == "pi":
        dfs["pi_column_df"] = add_column_ddl_to_pi_df(config, column_df, "ddl")
        table_df = create_pi_table_df(dfs["pi_column_df"], table_name, config)
        if table_df is not None:
            table_df = set_protected_classification(table_df, config)
            table_df = add_table_ddl_to_pi_df(config, table_df, "ddl")
            dfs["pi_table_df"] = table_df
        else:
            logger.error(
                "[DEBUG] WARNING: table_df is None! No table-level DDL will be generated!"
            )
        if config.apply_ddl:
            dfs["ddl_results"] = apply_ddl_to_tables(dfs, config)
    elif config.mode == "domain":
        if table_df is not None:
            table_df = add_ddl_to_domain_table_df(table_df, "ddl", config)
            dfs["domain_table_df"] = table_df
        else:
            logger.error(
                "[DEBUG] WARNING: table_df is None! No domain DDL will be generated!"
            )
        dfs["domain_column_df"] = None
        if config.apply_ddl:
            dfs["ddl_results"] = apply_ddl_to_tables(dfs, config)
    else:
        raise ValueError("Invalid mode. Use 'pi', 'comment', or 'domain'.")
    return dfs


def add_ddl_to_domain_table_df(
    table_df: DataFrame, ddl_col_name: str, config
) -> DataFrame:
    """
    Adds DDL statements to domain classification DataFrame.

    Args:
        table_df (DataFrame): The DataFrame containing domain classifications.
        ddl_col_name (str): The name of the DDL column to add.
        config: Configuration object with tag names.

    Returns:
        DataFrame: The DataFrame with DDL statements added.
    """
    if table_df is None:
        return None

    if "domain" in table_df.columns:
        table_df = table_df.withColumn("domain", col("domain").cast("string"))
    if "subdomain" in table_df.columns:
        table_df = table_df.withColumn("subdomain", col("subdomain").cast("string"))

    generate_domain_ddl = udf(_create_table_domain_ddl_func(config), StringType())

    result_df = table_df.withColumn(
        ddl_col_name,
        generate_domain_ddl("tokenized_table", "domain", "subdomain"),
    )

    # Keep column_content for logging purposes
    if "column_content" in result_df.columns:
        result_df = result_df.withColumn(
            "column_content", col("column_content").cast("string")
        )

    return result_df


def apply_ddl_to_tables(dfs, config):
    """
    Applies DDL to the tables.

    Args:
        dfs (DataFrame): The DataFrame containing the DDL statements.
        config (MetadataConfig): The configuration object.

    Returns:
        dict: Summary of DDL application results
    """
    table_df = dfs.get(f"{config.mode}_table_df")
    column_df = dfs.get(f"{config.mode}_column_df")

    results = {"table_results": {}, "column_results": {}, "all_missing_tags": []}

    if table_df is not None:
        results["table_results"] = apply_comment_ddl(table_df, config)
        if results["table_results"]["missing_tags"]:
            results["all_missing_tags"].extend(results["table_results"]["missing_tags"])

    if column_df is not None:
        results["column_results"] = apply_comment_ddl(column_df, config)
        if results["column_results"]["missing_tags"]:
            for tag in results["column_results"]["missing_tags"]:
                if tag not in results["all_missing_tags"]:
                    results["all_missing_tags"].append(tag)

    print_ddl_summary(results, config)

    return results


def print_ddl_summary(results, config):
    """
    Prints a formatted summary of DDL application results.

    Args:
        results (dict): Results dictionary from apply_ddl_to_tables
        config (MetadataConfig): Configuration object
    """
    print("\n" + "=" * 80)
    print("DDL APPLICATION SUMMARY")
    print("=" * 80)

    total_success = 0
    total_failed = 0

    if results["table_results"]:
        total_success += results["table_results"]["success_count"]
        total_failed += results["table_results"]["failed_count"]
        print(
            f"Table DDL: {results['table_results']['success_count']} succeeded, {results['table_results']['failed_count']} failed"
        )

    if results["column_results"]:
        total_success += results["column_results"]["success_count"]
        total_failed += results["column_results"]["failed_count"]
        print(
            f"Column DDL: {results['column_results']['success_count']} succeeded, {results['column_results']['failed_count']} failed"
        )

    print(f"\nTotal: {total_success} succeeded, {total_failed} failed")

    # Show failures
    if total_failed > 0:
        print("\n" + "-" * 80)
        print("FAILED DDL STATEMENTS:")
        print("-" * 80)

        if results["table_results"] and results["table_results"]["failed_statements"]:
            for i, fail in enumerate(results["table_results"]["failed_statements"], 1):
                # Security: Do not print full DDL (may contain sensitive data in comments)
                stmt_preview = (
                    fail["statement"][:100] + "..."
                    if len(fail["statement"]) > 100
                    else fail["statement"]
                )
                print(f"\n#{i} Statement preview: {stmt_preview}")
                print(f"Error: {fail['error'][:200]}...")  # Truncate long errors

        if results["column_results"] and results["column_results"]["failed_statements"]:
            for i, fail in enumerate(results["column_results"]["failed_statements"], 1):
                # Security: Do not print full DDL (may contain sensitive data in comments)
                stmt_preview = (
                    fail["statement"][:100] + "..."
                    if len(fail["statement"]) > 100
                    else fail["statement"]
                )
                print(f"\n#{i} Statement preview: {stmt_preview}")
                print(f"Error: {fail['error'][:200]}...")

    # Show missing tags
    if results["all_missing_tags"]:
        print("\n" + "-" * 80)
        print("MISSING GOVERNANCE TAGS:")
        print("-" * 80)
        for tag in results["all_missing_tags"]:
            print(f"  - {tag}")
        print("\nTo create these tags, run:")
        for tag in results["all_missing_tags"]:
            print(f"  CREATE TAG {config.catalog_name}.{tag};")

    print("=" * 80 + "\n")


def create_pi_table_df(
    column_df: DataFrame, table_name: str, config: MetadataConfig
) -> DataFrame:
    """
    Creates a DataFrame for PI information at the table level.
    Can be expanded to indicate the type of PI, but for tables it's a
    little more complicated because they can contain multiple, or
    even have PI that results from multiple columns, such as PHI
    that are not present in individual columns.

    Args:
        column_df (DataFrame): The DataFrame containing PI information at the column level.
        table_name (str): The name of the table.
        config (MetadataConfig): The configuration object.
    Returns:
        DataFrame: A DataFrame with PI information at the table level.
    """
    # First check all rows to determine classification
    all_rows = column_df.filter(col("type").isNotNull())
    table_subclassification = determine_table_classification(all_rows)

    # Filter out None types only for confidence calculation
    pi_rows = column_df.filter((col("type").isNotNull()) & (col("type") != "None"))

    # If all columns are "None", still create a table row if configured to tag None fields
    if pi_rows.count() == 0:
        # Check if we should tag None fields (uses existing config variable)
        # This variable also controls column-level None tagging elsewhere
        if not config.tag_none_fields:
            return None

        # Use all_rows for creating the table entry with "None" classification
        max_confidence = all_rows.agg(spark_max("confidence")).first()[0]
        base_row = all_rows.limit(1)
    else:
        # Use first() instead of collect() for single aggregate values
        max_confidence = pi_rows.agg(spark_max("confidence")).first()[0]
        base_row = pi_rows.limit(1)

    print("\n\ntable_subclassification:\n", table_subclassification)
    if config.use_protected_classification_for_table:
        table_classification = get_protected_classification_for_table(
            table_subclassification
        )
    else:
        table_classification = table_subclassification
    print("\n\ntable_subclassification:\n", table_subclassification)
    table_name = table_name.split(".")[-1]
    print("table_classification", table_classification)
    print("table_subclassification", table_subclassification)

    pi_table_row = (
        base_row.drop("ddl_type")
        .drop("confidence")
        .drop("ddl")
        .withColumn("ddl_type", lit("table"))
        .withColumn("confidence", lit(max_confidence))
        .withColumn("column_name", lit("None"))
        .withColumn("type", lit(table_subclassification))
        .withColumn("classification", lit(table_classification))
        .withColumn("table_name", lit(table_name))
    )
    return pi_table_row


def determine_table_classification(pi_rows: DataFrame) -> str:
    """
    Determines the classification based on the values in the 'classification' column of the pi_rows DataFrame.

    Args:
        pi_rows (DataFrame): The DataFrame containing PI information.

    Returns:
        str: The determined classification.
    """
    classification_set = set(pi_rows.select(collect_set("type")).first()[0])

    if classification_set == {} or not classification_set:
        return "None"
    elif classification_set == {"None"}:  # No PI information. Only case, done.
        return "None"
    elif classification_set == {"pii"} or classification_set == {"pii", "None"}:
        return "pii"
    elif (
        "pci" in classification_set
        and not {"phi", "medical_information"} & classification_set
    ):
        return "pci"
    elif classification_set == {"medical_information"} or classification_set == {
        "medical_information",
        "None",
    }:
        return "medical_information"
    elif (
        ("phi" in classification_set)
        or ({"pii", "medical_information"}.issubset(classification_set))
    ) and "pci" not in classification_set:
        return "phi"
    elif "pci" in classification_set and (
        "phi" in classification_set or "medical_information" in classification_set
    ):
        return "all"
    else:
        return "Unknown"


def get_protected_classification_for_table(table_classification: str) -> str:
    """
    Determines the classification based on the values in the 'classification'
    column of the pi_rows DataFrame.
    """
    if table_classification is not None:
        return "protected"


def summarize_table_content(table_df, config, table_name):
    """Create a new completion class for this."""
    if table_df.count() > 1:
        summarizer = TableCommentSummarizer(config, table_df)
        summary = summarizer.summarize_comments(table_name)

        if summary is None:
            summary = "No table summary available"
        elif not isinstance(summary, str):
            summary = str(summary)

        summary_df = table_df.limit(1).withColumn("column_content", lit(summary))
        return summary_df
    if table_df.count() == 1:
        return table_df
    raise ValueError("No table rows found during summarization...")


def setup_ddl(config: MetadataConfig) -> None:
    """
    Creates a schema volume if it does not already exist.

    Args:
        setup_params (Dict[str, Any]): A dictionary containing setup parameters including:
            - catalog (str): The catalog name.
            - dest_schema (str): The destination schema name.
            - volume_name (str): The volume name.
    """
    spark = SparkSession.builder.getOrCreate()
    ### Add error handling here
    if config.schema_name:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {config.catalog_name}.{config.schema_name};"
        )
    volume_sql = f"CREATE VOLUME IF NOT EXISTS {config.catalog_name}.{config.schema_name}.{config.volume_name};"

    if config.volume_name:
        spark.sql(volume_sql)
        review_output_path = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/{sanitize_user_identifier(config.current_user)}/reviewed_outputs/"
        os.makedirs(review_output_path, exist_ok=True)


def create_tables(config: MetadataConfig) -> None:
    """
    Creates a schema volume if it does not already exist.

    Args:
        setup_params (Dict[str, Any]): A dictionary containing setup parameters including:
            - catalog (str): The catalog name.
            - dest_schema (str): The destination schema name.
            - control_table (str): The destination table used for tracking table queue.
    """
    spark = SparkSession.builder.getOrCreate()
    if config.control_table:
        formatted_control_table = get_control_table(config)
        logger.info("Formatted control table...", formatted_control_table)
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {config.catalog_name}.{config.schema_name}.{formatted_control_table} (table_name STRING, _updated_at TIMESTAMP, _deleted_at TIMESTAMP, _job_id STRING)"""
        )


def instantiate_metadata_objects(
    env, mode, catalog_name=None, schema_name=None, table_names=None, base_url=None
):
    """By default, variables from variables.yml will be used.
    If widget values are provided, they will override."""
    METADATA_PARAMS = {"table_names": table_names}
    if catalog_name and catalog_name != "":
        METADATA_PARAMS["catalog_name"] = catalog_name
    if schema_name and schema_name != "":
        METADATA_PARAMS["dest_schema"] = schema_name
    if mode and mode != "":
        METADATA_PARAMS["mode"] = mode
    if mode and mode != "":
        METADATA_PARAMS["env"] = env
    if base_url and base_url != "":
        METADATA_PARAMS["base_url"] = base_url
    return METADATA_PARAMS


def trim_whitespace_from_df(df: DataFrame) -> DataFrame:
    """
    Trims whitespace from all string columns in the DataFrame.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with trimmed string columns.
    """
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    return df


class TableProcessingError(Exception):
    """Custom exception for table processing failures."""


def generate_and_persist_metadata(config: Any) -> None:
    """
    Generates and persists comments for tables based on the provided setup and model parameters.

    Args:
        config: Configuration object containing setup and model parameters.
    """
    spark = SparkSession.builder.getOrCreate()
    logger = logging.getLogger("metadata_processing")
    logger.setLevel(logging.INFO)

    for table in config.table_names:
        log_dict = {}
        try:
            logger.info(f"[generate_and_persist_metadata] Processing table {table}...")

            if not spark.catalog.tableExists(table):
                msg = f"Table {table} does not exist. Deleting from control table and skipping..."
                logger.warning(f"[generate_and_persist_metadata] {msg}")
                mark_as_deleted(table, config)
                log_dict = {
                    "full_table_name": table,
                    "status": "Table does not exist",
                    "user": sanitize_user_identifier(config.current_user),
                    "mode": config.mode,
                    "apply_ddl": str(config.apply_ddl),
                    "_updated_at": str(datetime.now()),
                }
            else:
                df = process_and_add_ddl(config, table)
                logger.info(
                    f"[generate_and_persist_metadata] Generating and persisting ddl for {table}..."
                )
                create_and_persist_ddl(df, config, table)

                # Check if DDL application had any failures
                status = "Table processed"
                if config.apply_ddl and "ddl_results" in df:
                    ddl_results = df["ddl_results"]
                    if ddl_results.get("failed_count", 0) > 0:
                        missing_tags = ddl_results.get("missing_tags", [])
                        if missing_tags:
                            status = f"Partial success - Missing governance tags: {', '.join(missing_tags)}"
                        else:
                            status = f"Partial success - {ddl_results['failed_count']} DDL statements failed"

                log_dict = {
                    "full_table_name": table,
                    "status": status,
                    "user": sanitize_user_identifier(config.current_user),
                    "mode": config.mode,
                    "apply_ddl": str(config.apply_ddl),
                    "_updated_at": str(datetime.now()),
                }

        except TableProcessingError as tpe:
            logger.error(
                f"[generate_and_persist_metadata] TableProcessingError for {table}: {tpe}"
            )
            log_dict = {
                "full_table_name": table,
                "status": f"Processing failed: {tpe}",
                "user": sanitize_user_identifier(config.current_user),
                "mode": config.mode,
                "apply_ddl": config.apply_ddl,
                "_updated_at": str(datetime.now()),
            }
            raise  # Optionally re-raise if you want to halt further processing

        except Exception as e:
            # Extract concise error message
            concise_error = extract_concise_error(e)
            logger.error(
                "[generate_and_persist_metadata] Error for %s: %s", table, concise_error
            )

            log_dict = {
                "full_table_name": table,
                "status": f"Processing failed: {concise_error}",
                "user": sanitize_user_identifier(config.current_user),
                "mode": config.mode,
                "apply_ddl": config.apply_ddl,
                "_updated_at": str(datetime.now()),
            }
            raise

        finally:
            try:
                print("\n\n\nTrying to write to log table...\n\n\n")
                write_to_log_table(
                    log_dict,
                    f"{config.catalog_name}.{config.schema_name}.table_processing_log",
                )
                logger.info(
                    "[generate_and_persist_metadata] Log written for table %s.", table
                )
            except Exception as log_err:
                concise_log_err = extract_concise_error(log_err)
                logger.error(
                    "[generate_and_persist_metadata] Failed to write log for %s: %s",
                    table,
                    concise_log_err,
                )
            print(f"Finished processing table {table} and writing to log table.")


def setup_queue(config: MetadataConfig) -> List[str]:
    """
    Checks a control table for any records and returns a list of table names.
    If the queue table is empty, reads a CSV with table names based on the flag set in the config file.

    Args:
        config (MetadataConfig): Configuration object containing setup and model parameters.

    Returns:
        List[str]: A list of table names.
    """
    spark = SparkSession.builder.getOrCreate()
    formatted_control_table = get_control_table(config)
    control_table = (
        f"{config.catalog_name}.{config.schema_name}.{formatted_control_table}"
    )
    queued_table_names = set()
    if spark.catalog.tableExists(control_table):
        control_df = spark.sql(
            f"""SELECT table_name FROM {control_table} WHERE _deleted_at IS NULL"""
        )
        # Use local temp table instead of collect() for better performance
        control_df.createOrReplaceTempView("temp_queued_tables")
        queued_table_names = set()

        # Alternative: Use pandas for small datasets when you need Python collections
        if control_df.count() < 10000:  # Only for reasonably small datasets
            queued_table_names = {row["table_name"] for row in control_df.collect()}
        else:
            # For large datasets, consider using broadcast variables or temp tables
            print(
                f"Large dataset detected ({control_df.count()} rows). Using temp table approach."
            )
            queued_table_names = set()  # Handle large datasets differently
    config_table_string = config.table_names
    config_table_names = [
        name.strip() for name in config_table_string.split(",") if len(name.strip()) > 0
    ]
    # Expand schema wildcards in config table names as well
    config_table_names = expand_schema_wildcards(config_table_names)
    file_table_names = load_table_names_from_csv(config.source_file_path)
    combined_table_names = list(
        set().union(queued_table_names, config_table_names, file_table_names)
    )
    combined_table_names = ensure_fully_scoped_table_names(
        combined_table_names, config.catalog_name
    )
    return combined_table_names


def ensure_fully_scoped_table_names(
    table_names: List[str], default_catalog: str
) -> List[str]:
    """
    Ensures that table names are fully scoped with catalog and schema.

    Args:
        table_names (List[str]): A list of table names.
        default_catalog (str): The default catalog name to use if not specified.

    Returns:
        List[str]: A list of fully scoped table names.
    """
    fully_scoped_table_names = []
    for table_name in table_names:
        parts = table_name.split(".")
        if len(parts) == 2:
            fully_scoped_table_names.append(f"{default_catalog}.{table_name}")
        elif len(parts) == 3:
            fully_scoped_table_names.append(table_name)
        else:
            raise ValueError(f"Invalid table name format: {table_name}")
    return fully_scoped_table_names


def upsert_table_names_to_control_table(
    table_names: List[str], config: MetadataConfig
) -> None:
    """
    Upserts a list of table names into the control table, ensuring no duplicates are created.

    Args:
        table_names (List[str]): A list of table names to upsert.
        config (MetadataConfig): Configuration object containing setup and model parameters.
    """
    print(f"Upserting table names to control table {table_names}...")
    spark = SparkSession.builder.getOrCreate()
    formatted_control_table = get_control_table(config)
    control_table = (
        f"{config.catalog_name}.{config.schema_name}.{formatted_control_table}"
    )
    table_names = ensure_fully_scoped_table_names(table_names, config.catalog_name)
    table_names_df = spark.createDataFrame(
        [(name,) for name in table_names], ["table_name"]
    )
    table_names_df = trim_whitespace_from_df(table_names_df)

    # Check if control table exists before reading
    if spark.catalog.tableExists(control_table):
        existing_df = spark.read.table(control_table)
    else:
        # Create empty DataFrame with same schema if table doesn't exist yet
        existing_df = spark.createDataFrame([], table_names_df.schema)
    new_table_names_df = (
        table_names_df.join(existing_df, on="table_name", how="left_anti")
        .withColumn("_updated_at", current_timestamp())
        .withColumn("_deleted_at", lit(None).cast(TimestampType()))
        .withColumn("_job_id", lit(config.job_id))
    )
    if new_table_names_df.count() > 0:
        new_table_names_df.write.option("mergeSchema", "true").format("delta").mode(
            "append"
        ).saveAsTable(control_table)
        print(
            f"Inserted {new_table_names_df.count()} new table names into the control table {control_table}..."
        )
    else:
        print("No new table names to upsert.")


def load_table_names_from_csv(csv_file_path):
    """Check if the CSV file exists and load table names."""
    if not csv_file_path or not os.path.exists(csv_file_path):
        print(
            f"CSV file not found or empty path: {csv_file_path}, returning empty list"
        )
        return []

    try:
        df_pandas = pd.read_csv(csv_file_path)

        if "table_name" not in df_pandas.columns:
            print(
                f"Warning: 'table_name' column not found in CSV. Available columns: {list(df_pandas.columns)}"
            )
            return []

        table_names = df_pandas["table_name"].dropna().tolist()
        print(f"Successfully loaded {len(table_names)} table names from CSV")

        sanitized_names = sanitize_string_list(table_names)
        expanded_names = expand_schema_wildcards(sanitized_names)
        return expanded_names

    except Exception as e:
        print(f"CSV read failed: {e}")
        return []


def is_schema_wildcard(table_name: str) -> bool:
    """
    Check if a table name is a schema wildcard pattern (catalog.schema.*).

    Args:
        table_name (str): The table name to check.

    Returns:
        bool: True if the table name is a schema wildcard pattern.
    """
    return table_name.strip().endswith(".*") and table_name.count(".") == 2


def get_tables_in_schema(catalog_name: str, schema_name: str) -> List[str]:
    """
    Get all table names in a given catalog and schema.

    Args:
        catalog_name (str): The catalog name.
        schema_name (str): The schema name.

    Returns:
        List[str]: A list of fully qualified table names.
    """
    spark = SparkSession.builder.getOrCreate()

    try:
        # Use SHOW TABLES to get all tables in the schema
        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")

        # Extract table names and create fully qualified names
        table_names = []
        for row in tables_df.collect():
            table_name = row["tableName"]
            fully_qualified_name = f"{catalog_name}.{schema_name}.{table_name}"
            table_names.append(fully_qualified_name)

        print(f"Found {len(table_names)} tables in schema {catalog_name}.{schema_name}")
        return table_names

    except Exception as e:
        print(
            f"Error retrieving tables from schema {catalog_name}.{schema_name}: {str(e)}"
        )
        return []


def expand_schema_wildcards(table_names: List[str]) -> List[str]:
    """
    Expand schema wildcard patterns in a list of table names.

    Args:
        table_names (List[str]): List of table names, possibly containing wildcards.

    Returns:
        List[str]: Expanded list of table names with wildcards resolved.
    """
    expanded_names = []

    for table_name in table_names:
        if is_schema_wildcard(table_name):
            # Extract catalog and schema from the wildcard pattern
            parts = table_name.replace(".*", "").split(".")
            if len(parts) == 2:
                catalog_name, schema_name = parts
                # Get all tables in the schema
                schema_tables = get_tables_in_schema(catalog_name, schema_name)
                expanded_names.extend(schema_tables)
                print(f"Expanded {table_name} to {len(schema_tables)} tables")
            else:
                print(f"Warning: Invalid wildcard pattern {table_name}, skipping")
        else:
            # Regular table name, add as-is
            expanded_names.append(table_name)

    return expanded_names


def sanitize_string_list(string_list: List[str]):
    """Sanitize a list of strings.

    Args:
        string_list (List[str]): The list of strings to sanitize.

    Returns:
        List[str]: The sanitized list of strings.
    """
    sanitized_list = []
    for s in string_list:
        s = str(s)
        s = s.strip()
        s = " ".join(s.split())
        s = s.lower()
        s = "".join(
            c
            for c in s
            if c.isalnum() or c.isspace() or c == "." or c == "_" or c == "*"
        )
        sanitized_list.append(s)
    return sanitized_list


def split_fully_scoped_table_name(df: DataFrame, full_table_name_col: str) -> DataFrame:
    """
    Splits a fully scoped table name column into catalog, schema, and table columns.

    Args:
        df (DataFrame): The input DataFrame.
        full_table_name_col (str): The name of the column containing the fully scoped table name.

    Returns:
        DataFrame: The updated DataFrame with catalog, schema, and table columns added.
    """
    split_col = split(col(full_table_name_col), r"\.")
    df = (
        df.withColumn("catalog", split_col.getItem(0).cast("string"))
        .withColumn("schema", split_col.getItem(1).cast("string"))
        .withColumn("table_name", split_col.getItem(2).cast("string"))
    )
    return df


def split_table_names(table_names: str) -> List[str]:
    """Split a comma-separated string of table names into a list of table names.

    Args:
        table_names (str): The comma-separated string of table names.

    Returns:
        List[str]: The list of table names.
    """
    if not table_names:
        return []
    return table_names.split(",")


def replace_fully_scoped_table_column(df):
    """Replace the fully scoped table column with the table name.

    Args:
        df (DataFrame): The DataFrame to replace the fully scoped table column with the table name.

    Returns:
        DataFrame: The DataFrame with the fully scoped table column replaced with the table name.
    """
    return df.withColumn("table", split_part(col("table"), ".", -1))


def _create_table_comment_ddl_func():

    def table_comment_ddl(full_table_name: str, comment: str) -> str:
        if comment is not None:
            comment = comment.replace('""', "'")
            comment = comment.replace('"', "'")
        return f"""COMMENT ON TABLE {full_table_name} IS "{comment}";"""

    return table_comment_ddl


def _create_column_comment_ddl_func():
    def column_comment_ddl(full_table_name: str, column_name: str, comment: str) -> str:
        if comment is not None:
            comment = comment.replace('""', "'")
            comment = comment.replace('"', "'")

        dbr_number = os.environ.get("DATABRICKS_RUNTIME_VERSION")

        if dbr_number is None:
            # Default to newer syntax for serverless (assumes DBR 15+)
            ddl_statement = f"""COMMENT ON COLUMN {full_table_name}.`{column_name}` IS "{comment}";"""
        else:
            try:
                dbr_version = float(dbr_number)
                if dbr_version is None:
                    raise ValueError(f"Databricks runtime version is None")
                if dbr_version >= 16:
                    ddl_statement = f"""COMMENT ON COLUMN {full_table_name}.`{column_name}` IS "{comment}";"""
                elif dbr_version >= 14 and dbr_version < 16:
                    ddl_statement = f"""ALTER TABLE {full_table_name} ALTER COLUMN `{column_name}` COMMENT "{comment}";"""
                else:
                    raise ValueError(
                        f"Unsupported Databricks runtime version: {dbr_number}"
                    )
            except ValueError as e:
                ddl_statement = f"""COMMENT ON COLUMN {full_table_name}.`{column_name}` IS "{comment}";"""
        return ddl_statement

    return column_comment_ddl


def _create_table_pi_information_ddl_func(config: MetadataConfig):
    pi_class_tag = getattr(config, "pi_classification_tag_name", "data_classification")
    pi_subclass_tag = getattr(
        config, "pi_subclassification_tag_name", "data_subclassification"
    )

    def table_pi_information_ddl(
        table_name: str, classification: str, pi_type: str
    ) -> str:
        return f"ALTER TABLE {table_name} SET TAGS ('{pi_class_tag}' = '{classification}', '{pi_subclass_tag}' = '{pi_type}');"

    return table_pi_information_ddl


def _create_pi_information_ddl_func(config: MetadataConfig):
    pi_class_tag = getattr(config, "pi_classification_tag_name", "data_classification")
    pi_subclass_tag = getattr(
        config, "pi_subclassification_tag_name", "data_subclassification"
    )

    def pi_information_ddl(
        table_name: str, column_name: str, classification: str, pi_type: str
    ) -> str:
        return f"ALTER TABLE {table_name} ALTER COLUMN `{column_name}` SET TAGS ('{pi_class_tag}' = '{classification}', '{pi_subclass_tag}' = '{pi_type}');"

    return pi_information_ddl


def _create_table_domain_ddl_func(config: MetadataConfig):
    domain_tag = getattr(config, "domain_tag_name", "domain")
    subdomain_tag = getattr(config, "subdomain_tag_name", "subdomain")

    def table_domain_ddl(full_table_name: str, domain: str, subdomain: str) -> str:
        """
        Generate DDL for domain classification as table tags.

        Args:
            full_table_name: The full table name (catalog.schema.table)
            domain: Primary domain classification
            subdomain: Subdomain classification (can be None or empty)

        Returns:
            DDL statement to set table tags with domain information
        """
        if subdomain is None or subdomain == "None" or subdomain.strip() == "":
            return (
                f"ALTER TABLE {full_table_name} SET TAGS ('{domain_tag}' = '{domain}');"
            )
        return f"ALTER TABLE {full_table_name} SET TAGS ('{domain_tag}' = '{domain}', '{subdomain_tag}' = '{subdomain}');"

    return table_domain_ddl


generate_table_comment_ddl = udf(_create_table_comment_ddl_func(), StringType())
generate_column_comment_ddl = udf(_create_column_comment_ddl_func(), StringType())

# These UDFs require config and are created dynamically in their respective functions
# generate_table_pi_information_ddl
# generate_pi_information_ddl
# generate_table_domain_ddl

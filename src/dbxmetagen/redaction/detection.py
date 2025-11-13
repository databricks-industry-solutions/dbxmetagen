"""
Unified PHI/PII detection interface.

This module provides a consistent interface for running different detection
methods (Presidio, AI Query, NER) and returning standardized results.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json

from .presidio import make_presidio_batch_udf
from .ai_detector import make_prompt, format_entity_response_object_udf
from .gliner_detector import run_gliner_detection as _run_gliner_detection
from .config import LABEL_ENUMS, PHI_PROMPT_SKELETON, DEFAULT_PRESIDIO_SCORE_THRESHOLD


def run_presidio_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    num_cores: int = 10,
) -> DataFrame:
    """
    Run Presidio-based PHI detection on a DataFrame.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        score_threshold: Minimum confidence score (0.0-1.0)
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with 'presidio_results' and 'presidio_results_struct' columns

    Example:
        >>> result_df = run_presidio_detection(
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     score_threshold=0.5
        ... )
    """
    presidio_udf = make_presidio_batch_udf(score_threshold=score_threshold)

    result_df = (
        df.repartition(num_cores)
        .withColumn(
            "presidio_results",
            presidio_udf(col(doc_id_column), col(text_column)),
        )
        .withColumn(
            "presidio_results_struct",
            from_json(
                "presidio_results",
                "array<struct<entity:string, entity_type:string, score:double, start:integer, end:integer, doc_id:string>>",
            ),
        )
    )

    return result_df


def run_ai_query_detection(
    spark: SparkSession,
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    endpoint: str = "databricks-claude-sonnet-4",
    num_cores: int = 10,
    prompt_skeleton: str = PHI_PROMPT_SKELETON,
    labels: str = LABEL_ENUMS,
) -> DataFrame:
    """
    Run AI-based PHI detection using Databricks AI Query.

    Args:
        spark: Active SparkSession
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        endpoint: Databricks model serving endpoint name
        num_cores: Number of cores for repartitioning
        prompt_skeleton: Template prompt for AI detection
        labels: Entity type labels for detection

    Returns:
        DataFrame with 'ai_query_results' and 'ai_results_struct' columns

    Example:
        >>> result_df = run_ai_query_detection(
        ...     spark,
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     endpoint="databricks-meta-llama-3-3-70b-instruct"
        ... )
    """
    # Create prompt
    prompt = make_prompt(prompt_skeleton, labels=labels)

    # Create temporary view
    temp_view = "ai_detection_temp_view"
    df.createOrReplaceTempView(temp_view)

    # Run AI query
    query = f"""
    WITH data_with_prompting AS (
        SELECT *,
              REPLACE('{prompt}', '{{med_text}}', CAST({text_column} AS STRING)) AS prompt
        FROM {temp_view}
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

    result_df = (
        spark.sql(query)
        .repartition(num_cores)
        .withColumn(
            "ai_query_results",
            format_entity_response_object_udf(col("response.result"), col(text_column)),
        )
        .withColumn(
            "ai_results_struct",
            from_json(
                "ai_query_results",
                "array<struct<entity:string, score:double, start:integer, end:integer, doc_id:string>>",
            ),
        )
    )

    return result_df


def run_gliner_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    model_name: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
) -> DataFrame:
    """
    Run GLiNER NER model-based PHI detection.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        model_name: HuggingFace model identifier for GLiNER
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with 'gliner_results' and 'gliner_results_struct' columns

    Example:
        >>> result_df = run_gliner_detection(
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text"
        ... )
    """
    return _run_gliner_detection(df, doc_id_column, text_column, model_name, num_cores)


def run_detection(
    spark: SparkSession,
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    endpoint: Optional[str] = None,
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
) -> DataFrame:
    """
    Run PHI/PII detection using selected method(s).

    Unified interface for running one or more detection methods and
    returning results in a standardized format.

    Args:
        spark: Active SparkSession
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER NER detection
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model name for GLiNER
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with detection results from enabled method(s)

    Example:
        >>> # Run Presidio and AI Query
        >>> result_df = run_detection(
        ...     spark,
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     use_presidio=True,
        ...     use_ai_query=True,
        ...     use_gliner=False,
        ...     endpoint="databricks-claude-sonnet-4"
        ... )

        >>> # Run only Presidio
        >>> result_df = run_detection(
        ...     spark,
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     use_presidio=True,
        ...     use_ai_query=False,
        ...     use_gliner=False
        ... )
    """
    result_df = df

    if use_presidio:
        result_df = run_presidio_detection(
            result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            score_threshold=score_threshold,
            num_cores=num_cores,
        )

    if use_ai_query:
        if endpoint is None:
            endpoint = "databricks-claude-sonnet-4"

        result_df = run_ai_query_detection(
            spark,
            result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            endpoint=endpoint,
            num_cores=num_cores,
        )

    if use_gliner:
        result_df = run_gliner_detection(
            result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            model_name=gliner_model,
            num_cores=num_cores,
        )

    return result_df

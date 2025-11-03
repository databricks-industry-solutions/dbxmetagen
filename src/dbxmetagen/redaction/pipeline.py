"""
End-to-end PHI/PII detection and redaction pipelines.

This module orchestrates the complete workflow from detection through
alignment and redaction.
"""

from typing import Optional, Literal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from .detection import run_detection
from .alignment import align_entities_udf
from .redaction import create_redacted_table, RedactionStrategy
from .metadata import get_columns_by_tag


def run_detection_pipeline(
    spark: SparkSession,
    source_df: DataFrame,
    doc_id_column: str,
    text_column: str,
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    align_results: bool = True,
) -> DataFrame:
    """
    Run complete detection pipeline with optional alignment.

    Executes detection using selected method(s) and optionally aligns
    results from multiple methods into a consensus view.

    Args:
        spark: Active SparkSession
        source_df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER detection
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model for GLiNER
        num_cores: Number of cores for repartitioning
        align_results: If True and multiple methods enabled, align results

    Returns:
        DataFrame with detection results and optional aligned entities

    Example:
        >>> result_df = run_detection_pipeline(
        ...     spark,
        ...     source_df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     use_presidio=True,
        ...     use_ai_query=True,
        ...     use_gliner=False,
        ...     endpoint="databricks-claude-sonnet-4"
        ... )
    """
    # Run detection
    result_df = run_detection(
        spark=spark,
        df=source_df,
        doc_id_column=doc_id_column,
        text_column=text_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        endpoint=endpoint,
        score_threshold=score_threshold,
        gliner_model=gliner_model,
        num_cores=num_cores,
    )

    # Count how many methods were used
    methods_used = sum([use_presidio, use_ai_query, use_gliner])

    # Align results if multiple methods were used
    if align_results and methods_used >= 2:
        # For now, only align Presidio and AI Query (most common case)
        if (
            use_presidio
            and use_ai_query
            and "ai_results_struct" in result_df.columns
            and "presidio_results_struct" in result_df.columns
        ):
            result_df = result_df.withColumn(
                "aligned_entities",
                align_entities_udf()(
                    col("ai_results_struct"),
                    col("presidio_results_struct"),
                    col(doc_id_column),
                ),
            )

    return result_df


def run_redaction_pipeline(
    spark: SparkSession,
    source_table: str,
    text_column: str,
    output_table: str,
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    use_aligned: bool = True,
) -> DataFrame:
    """
    Run end-to-end detection and redaction pipeline.

    Complete workflow that:
    1. Loads source table
    2. Runs detection
    3. Aligns results (if multiple methods)
    4. Redacts text
    5. Saves redacted table

    Args:
        spark: Active SparkSession
        source_table: Fully qualified source table name
        text_column: Name of text column to redact
        output_table: Fully qualified output table name
        doc_id_column: Name of document ID column
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER detection
        redaction_strategy: Redaction strategy ('generic' or 'typed')
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model for GLiNER
        num_cores: Number of cores for repartitioning
        use_aligned: If True, use aligned entities for redaction

    Returns:
        DataFrame with redacted text

    Example:
        >>> result_df = run_redaction_pipeline(
        ...     spark,
        ...     source_table="catalog.schema.medical_notes",
        ...     text_column="note_text",
        ...     output_table="catalog.schema.medical_notes_redacted",
        ...     use_presidio=True,
        ...     use_ai_query=True,
        ...     use_gliner=False,
        ...     redaction_strategy="typed"
        ... )
    """
    # Load source data
    source_df = spark.table(source_table).select(doc_id_column, text_column).distinct()

    # Run detection pipeline
    detection_df = run_detection_pipeline(
        spark=spark,
        source_df=source_df,
        doc_id_column=doc_id_column,
        text_column=text_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        endpoint=endpoint,
        score_threshold=score_threshold,
        gliner_model=gliner_model,
        num_cores=num_cores,
        align_results=True,
    )

    # Determine which entities column to use for redaction
    if use_aligned and "aligned_entities" in detection_df.columns:
        entities_column = "aligned_entities"
    elif "presidio_results_struct" in detection_df.columns:
        entities_column = "presidio_results_struct"
    elif "ai_results_struct" in detection_df.columns:
        entities_column = "ai_results_struct"
    else:
        raise ValueError("No entity results found in detection output")

    # Apply redaction
    result_df = create_redacted_table(
        spark=spark,
        source_df=detection_df,
        text_column=text_column,
        entities_column=entities_column,
        output_table=output_table,
        strategy=redaction_strategy,
    )

    return result_df


def run_redaction_pipeline_by_tag(
    spark: SparkSession,
    source_table: str,
    output_table: str,
    tag_name: str = "data_classification",
    tag_value: str = "protected",
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
) -> DataFrame:
    """
    Run redaction pipeline on columns identified by Unity Catalog tags.

    Queries Unity Catalog to find columns with specified tag/value,
    then runs detection and redaction on those columns.

    Args:
        spark: Active SparkSession
        source_table: Fully qualified source table name
        output_table: Fully qualified output table name
        tag_name: Name of tag to filter by
        tag_value: Value of tag to match
        doc_id_column: Name of document ID column
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER detection
        redaction_strategy: Redaction strategy ('generic' or 'typed')
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model for GLiNER
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with redacted columns

    Example:
        >>> result_df = run_redaction_pipeline_by_tag(
        ...     spark,
        ...     source_table="catalog.schema.patient_data",
        ...     output_table="catalog.schema.patient_data_redacted",
        ...     tag_name="data_classification",
        ...     tag_value="protected"
        ... )
    """
    # Get columns with specified tag
    protected_columns = get_columns_by_tag(
        spark=spark, table_name=source_table, tag_name=tag_name, tag_value=tag_value
    )

    if not protected_columns:
        raise ValueError(
            f"No columns found with {tag_name}={tag_value} in {source_table}"
        )

    # Load source table
    source_df = spark.table(source_table)

    # For each protected column, run detection and redaction
    # For simplicity, process the first text column found
    # In a full implementation, you'd handle multiple columns
    text_column = protected_columns[0]

    result_df = run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column=text_column,
        output_table=output_table,
        doc_id_column=doc_id_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        redaction_strategy=redaction_strategy,
        endpoint=endpoint,
        score_threshold=score_threshold,
        gliner_model=gliner_model,
        num_cores=num_cores,
    )

    return result_df

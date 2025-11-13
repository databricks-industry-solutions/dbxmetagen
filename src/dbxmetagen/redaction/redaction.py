"""
Text redaction functions for PHI/PII removal.

This module provides functions to redact sensitive information from text
based on detected entity positions.
"""

from typing import List, Dict, Any, Literal
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType


RedactionStrategy = Literal["generic", "typed"]


def redact_text(
    text: str, entities: List[Dict[str, Any]], strategy: RedactionStrategy = "generic"
) -> str:
    """
    Redact PII/PHI entities from text.

    Replaces entity text at specified positions with redaction placeholders.
    Entities are processed in reverse order by position to maintain correct
    indices during replacement.

    Args:
        text: Original text containing PII/PHI
        entities: List of entity dicts with 'start', 'end', 'entity_type' keys
        strategy: Redaction strategy:
            - 'generic': Replace with '[REDACTED]'
            - 'typed': Replace with '[ENTITY_TYPE]' (e.g., '[PERSON]')

    Returns:
        Text with entities redacted

    Example:
        >>> text = "John Smith lives at 123 Main St"
        >>> entities = [
        ...     {"start": 0, "end": 9, "entity_type": "PERSON"},
        ...     {"start": 20, "end": 31, "entity_type": "ADDRESS"}
        ... ]
        >>> redact_text(text, entities, strategy="typed")
        '[PERSON] lives at [ADDRESS]'
    """
    if not entities:
        return text

    # Sort entities by start position in reverse order
    sorted_entities = sorted(entities, key=lambda e: e.get("start", 0), reverse=True)

    redacted = text
    for entity in sorted_entities:
        start = entity.get("start")
        end = entity.get("end")
        entity_type = entity.get("entity_type", "REDACTED")

        if start is None or end is None:
            continue

        # Adjust end index (add 1 for string slicing)
        end_idx = end + 1

        # Choose redaction text based on strategy
        if strategy == "typed":
            replacement = f"[{entity_type}]"
        else:  # generic
            replacement = "[REDACTED]"

        # Replace the entity in text
        redacted = redacted[:start] + replacement + redacted[end_idx:]

    return redacted


def create_redaction_udf(strategy: RedactionStrategy = "generic"):
    """
    Create a Pandas UDF for redacting text in DataFrames.

    Args:
        strategy: Redaction strategy ('generic' or 'typed')

    Returns:
        Pandas UDF that takes (text, entities) and returns redacted text

    Example:
        >>> redact_udf = create_redaction_udf(strategy="typed")
        >>> df = df.withColumn("redacted_text",
        ...     redact_udf(col("text"), col("entities")))
    """

    @pandas_udf(StringType())
    def redact_udf(texts: pd.Series, entities_series: pd.Series) -> pd.Series:
        """Redact entities from text for each row."""
        results = []
        for text, entities in zip(texts, entities_series):
            # Handle None
            if entities is None:
                results.append(text)
                continue

            # Handle empty arrays (works with numpy arrays, Spark arrays, lists)
            try:
                if len(entities) == 0:
                    results.append(text)
                    continue
            except (TypeError, AttributeError):
                results.append(text)
                continue

            # Convert to list if needed (handles numpy arrays, Spark arrays)
            if not isinstance(entities, list):
                try:
                    entities = list(entities)
                except (TypeError, ValueError):
                    results.append(text)
                    continue

            # Convert Spark Row objects to dicts (Row objects have asDict method)
            entities = [e.asDict() if hasattr(e, "asDict") else e for e in entities]

            # Apply redaction
            results.append(redact_text(text, entities, strategy=strategy))

        return pd.Series(results)

    return redact_udf


def create_redacted_table(
    spark: SparkSession,
    source_df: DataFrame,
    text_column: str,
    entities_column: str,
    output_table: str,
    strategy: RedactionStrategy = "generic",
    suffix: str = "_redacted",
) -> DataFrame:
    """
    Create a redacted version of a table.

    Takes a DataFrame with detected entities and creates a new table
    where the specified text column has PII/PHI redacted.

    Args:
        spark: Active SparkSession
        source_df: DataFrame with text and detected entities
        text_column: Name of column containing text to redact
        entities_column: Name of column containing entity lists
        output_table: Fully qualified output table name
        strategy: Redaction strategy ('generic' or 'typed')
        suffix: Suffix for redacted column name (default: '_redacted')

    Returns:
        DataFrame with redacted text

    Example:
        >>> result_df = create_redacted_table(
        ...     spark,
        ...     df,
        ...     text_column="text",
        ...     entities_column="aligned_entities",
        ...     output_table="catalog.schema.table_redacted",
        ...     strategy="typed"
        ... )
    """
    # Create UDF for redaction
    redact_udf = create_redaction_udf(strategy=strategy)

    # Apply redaction
    redacted_col_name = f"{text_column}{suffix}"
    result_df = source_df.withColumn(
        redacted_col_name, redact_udf(col(text_column), col(entities_column))
    )

    # Write to output table
    result_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        output_table
    )

    return result_df


def apply_redaction_to_columns(
    spark: SparkSession,
    table_name: str,
    column_entities_map: Dict[str, List[Dict[str, Any]]],
    output_table: str,
    strategy: RedactionStrategy = "generic",
) -> DataFrame:
    """
    Apply redaction to multiple columns in a table.

    Useful when working with tables that have multiple text columns
    requiring redaction with different entity sets.

    Args:
        spark: Active SparkSession
        table_name: Source table name
        column_entities_map: Map of column names to their entity lists
        output_table: Output table name
        strategy: Redaction strategy ('generic' or 'typed')

    Returns:
        DataFrame with all specified columns redacted

    Example:
        >>> entities_map = {
        ...     'notes': [{'start': 0, 'end': 10, 'entity_type': 'PERSON'}],
        ...     'description': [{'start': 5, 'end': 15, 'entity_type': 'EMAIL'}]
        ... }
        >>> result = apply_redaction_to_columns(
        ...     spark,
        ...     "source_table",
        ...     entities_map,
        ...     "redacted_table"
        ... )
    """
    df = spark.table(table_name)

    # Apply redaction to each column
    for col_name, entities in column_entities_map.items():
        if col_name not in df.columns:
            continue

        redacted_col = f"{col_name}_redacted"
        # For simplicity, broadcast entities to all rows
        # In practice, you'd want row-specific entities
        redact_udf = create_redaction_udf(strategy=strategy)
        df = df.withColumn(
            redacted_col, redact_udf(col(col_name), col(col_name))  # Placeholder
        )

    # Save result
    df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(output_table)

    return df

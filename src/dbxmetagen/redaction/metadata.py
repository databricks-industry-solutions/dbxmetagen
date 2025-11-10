"""
Unity Catalog metadata query functions for PHI/PII redaction.

This module provides functions to query Unity Catalog for column metadata,
particularly for identifying columns with specific classification tags.
"""

from typing import List, Dict, Optional
from pyspark.sql import SparkSession


def get_columns_by_tag(
    spark: SparkSession, table_name: str, tag_name: str, tag_value: str
) -> List[str]:
    """
    Query Unity Catalog to find columns with a specific tag value.

    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        tag_name: Name of the tag to filter by
        tag_value: Value of the tag to match

    Returns:
        List of column names that have the specified tag and value

    Example:
        >>> columns = get_columns_by_tag(
        ...     spark,
        ...     "catalog.schema.table",
        ...     "data_classification",
        ...     "protected"
        ... )
        >>> # Returns: ['ssn', 'email', 'phone']
    """
    query = f"""
    SELECT column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE table_catalog = split('{table_name}', '\\.')[0]
      AND table_schema = split('{table_name}', '\\.')[1]
      AND table_name = split('{table_name}', '\\.')[2]
      AND tag_name = '{tag_name}'
      AND tag_value = '{tag_value}'
    """

    result_df = spark.sql(query)
    columns = [row.column_name for row in result_df.collect()]

    return columns


def get_protected_columns(
    spark: SparkSession, table_name: str, tag_name: str = "data_classification"
) -> List[str]:
    """
    Get columns marked as protected in Unity Catalog.

    Convenience wrapper for get_columns_by_tag() that searches for
    the 'protected' classification.

    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        tag_name: Name of the classification tag (default: 'data_classification')

    Returns:
        List of column names marked as protected

    Example:
        >>> protected_cols = get_protected_columns(spark, "catalog.schema.table")
        >>> # Returns columns with data_classification='protected'
    """
    return get_columns_by_tag(spark, table_name, tag_name, "protected")


def get_table_metadata(
    spark: SparkSession, table_name: str
) -> Dict[str, Dict[str, str]]:
    """
    Get comprehensive metadata for all columns in a table.

    Retrieves column names, data types, and any associated tags from
    Unity Catalog.

    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        Dictionary mapping column names to their metadata (type, tags)

    Example:
        >>> metadata = get_table_metadata(spark, "catalog.schema.table")
        >>> # Returns: {'col1': {'type': 'string', 'tags': {...}}, ...}
    """
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Table name must be fully qualified (catalog.schema.table): {table_name}"
        )

    catalog, schema, table = parts

    # Get column information
    columns_query = f"""
    SELECT column_name, data_type
    FROM system.information_schema.columns
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
    """

    columns_df = spark.sql(columns_query)
    metadata = {
        row.column_name: {"type": row.data_type, "tags": {}}
        for row in columns_df.collect()
    }

    # Get tags for columns
    tags_query = f"""
    SELECT column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
    """

    tags_df = spark.sql(tags_query)
    for row in tags_df.collect():
        if row.column_name in metadata:
            metadata[row.column_name]["tags"][row.tag_name] = row.tag_value

    return metadata

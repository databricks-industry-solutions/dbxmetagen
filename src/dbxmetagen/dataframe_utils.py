"""
Utility functions for DataFrame transformations.
Extracted for easier testing without heavy dependencies.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, regexp_replace

logger = logging.getLogger(__name__)


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


def split_name_for_df(df: DataFrame) -> DataFrame:
    """
    Splits the fully scoped table name for the DataFrame.
    Also replaces double apostrophes with single apostrophes in column_content.

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
            df = df.withColumn(
                "column_content",
                regexp_replace(col("column_content").cast("string"), "''", "'")
            )
            final_column_content_type = str(
                df.select("column_content").schema.fields[0].dataType
            )

        logger.info(
            "df columns after generating metadata in process_and_add_ddl %s", df.columns
        )
    return df


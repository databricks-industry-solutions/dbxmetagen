from __future__ import annotations

import logging
from functools import reduce
from typing import List, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.column import Column

from dbxmetagen.config import MetadataConfig

logger = logging.getLogger(__name__)


def override_metadata_from_csv(
    df: DataFrame, csv_path: str, config: MetadataConfig
) -> DataFrame:
    """
    Overrides the type and classification in the DataFrame based on the CSV file.
    This would need to be optimized if a customer has a large number of overrides.

    Args:
        df (DataFrame): The input DataFrame.
        csv_path (str): The path to the CSV file.

    Returns:
        DataFrame: The updated DataFrame with overridden type and classification.
    """
    import os

    resolved_path = os.path.abspath(csv_path) if csv_path else None
    logger.info(
        "Override check: allow_manual_override=True, mode=%s, csv_path='%s', resolved='%s'",
        config.mode, csv_path, resolved_path,
    )

    if not csv_path or not os.path.exists(csv_path):
        logger.warning(
            "Override CSV not found at '%s' (resolved: '%s') -- skipping overrides. "
            "Ensure the file exists at this path relative to the notebook working directory.",
            csv_path, resolved_path,
        )
        return df

    csv_df = pd.read_csv(csv_path)
    csv_df = csv_df.where(pd.notna(csv_df), None)

    # CRITICAL SERVERLESS FIX: Ensure all CSV columns are treated as strings to prevent type inference
    csv_df = csv_df.astype(str)
    csv_df = csv_df.replace("None", None)

    csv_dict = csv_df.to_dict("records")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    if len(csv_df) > 0:
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType(
            [StructField(col_name, StringType(), True) for col_name in csv_df.columns]
        )
        csv_spark_df = spark.createDataFrame(csv_df, schema)
    else:
        csv_spark_df = spark.createDataFrame(csv_df)
    nrows = csv_spark_df.count()
    logger.info("Override CSV loaded: %d rows from '%s'", nrows, csv_path)

    if nrows == 0:
        logger.info("No override rows to apply, returning DataFrame unchanged.")
        return df
    elif nrows < 10000:
        df = apply_overrides_with_loop(df, csv_dict, config)
    else:
        raise ValueError(
            "CSV file is too large. Please implement a more efficient method for large datasets."
        )
    return df


def _is_blank(val):
    return val is None or (isinstance(val, float) and pd.isna(val))


def apply_overrides_with_loop(df, csv_dict, config):
    if not csv_dict:
        return df

    from pyspark.sql.functions import col, lit, when

    applied_count = 0
    skipped_count = 0

    if config.mode == "pi":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            column = row.get("column")
            classification_override = row.get("classification")
            type_override = row.get("type")

            if not column:
                skipped_count += 1
                logger.info("PI override: skipping row with no column specified")
                continue

            if _is_blank(classification_override) and _is_blank(type_override):
                skipped_count += 1
                logger.info("PI override: skipping null/blank overrides for column '%s'", column)
                continue

            if not _is_blank(classification_override):
                classification_override = str(classification_override)
            if not _is_blank(type_override):
                type_override = str(type_override)

            try:
                condition = build_condition(df, table, column, schema, catalog)

                if not _is_blank(classification_override):
                    df = df.withColumn(
                        "classification",
                        when(condition, lit(classification_override).cast("string")).otherwise(col("classification")),
                    )
                    logger.info(
                        "PI override applied: column='%s' -> classification='%s' (table=%s)",
                        column, classification_override, table or "*",
                    )

                if not _is_blank(type_override):
                    df = df.withColumn(
                        "type",
                        when(condition, lit(type_override).cast("string")).otherwise(col("type")),
                    )
                    logger.info(
                        "PI override applied: column='%s' -> type='%s' (table=%s)",
                        column, type_override, table or "*",
                    )

                applied_count += 1
            except ValueError as e:
                skipped_count += 1
                logger.warning("PI override skipped (bad condition): %s", e)

    elif config.mode == "comment":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            column = row.get("column")
            comment_override = row.get("comment")

            if _is_blank(comment_override):
                target = column or table or "unknown"
                skipped_count += 1
                logger.info("Comment override: skipping null/blank comment for target '%s'", target)
                continue

            comment_override = str(comment_override)

            try:
                if column:
                    condition = build_condition(df, table, column, schema, catalog)
                    df = df.withColumn(
                        "column_content",
                        when(condition, lit(comment_override).cast("string")).otherwise(col("column_content")),
                    )
                    logger.info(
                        "Comment override applied: column='%s' -> comment='%.60s...' (table=%s)",
                        column, comment_override, table or "*",
                    )
                else:
                    if not table:
                        skipped_count += 1
                        logger.warning("Comment override: skipping table-level row with no table name specified")
                        continue
                    base_condition = build_condition(df, table, None, schema, catalog)
                    condition = base_condition & (col("ddl_type") == "table")
                    df = df.withColumn(
                        "column_content",
                        when(condition, lit(comment_override).cast("string")).otherwise(col("column_content")),
                    )
                    logger.info(
                        "Comment override applied (table-level): table='%s' -> comment='%.60s...'",
                        table, comment_override,
                    )
                applied_count += 1
            except ValueError as e:
                skipped_count += 1
                logger.warning("Comment override skipped (bad condition): %s", e)

    elif config.mode == "domain":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            domain_override = row.get("domain")
            subdomain_override = row.get("subdomain")

            if not table:
                skipped_count += 1
                logger.warning("Domain override: skipping row with no table name specified")
                continue

            if _is_blank(domain_override) and _is_blank(subdomain_override):
                skipped_count += 1
                logger.info("Domain override: skipping null/blank overrides for table '%s'", table)
                continue

            if not _is_blank(domain_override):
                domain_override = str(domain_override)
            if not _is_blank(subdomain_override):
                subdomain_override = str(subdomain_override)

            try:
                condition = build_condition(df, table, None, schema, catalog)

                if not _is_blank(domain_override):
                    df = df.withColumn(
                        "domain",
                        when(condition, lit(domain_override).cast("string")).otherwise(col("domain")),
                    )
                    logger.info(
                        "Domain override applied: table='%s' -> domain='%s'", table, domain_override,
                    )

                if not _is_blank(subdomain_override):
                    df = df.withColumn(
                        "subdomain",
                        when(condition, lit(subdomain_override).cast("string")).otherwise(col("subdomain")),
                    )
                    logger.info(
                        "Domain override applied: table='%s' -> subdomain='%s'", table, subdomain_override,
                    )

                applied_count += 1
            except ValueError as e:
                skipped_count += 1
                logger.warning("Domain override skipped (bad condition): %s", e)

    else:
        raise ValueError("Invalid mode provided. Must be 'pi', 'comment', or 'domain'.")

    logger.info(
        "Override summary: mode=%s, applied=%d, skipped=%d, total_csv_rows=%d",
        config.mode, applied_count, skipped_count, len(csv_dict),
    )
    return df


def apply_overrides_with_joins(df: DataFrame, csv_spark_df: DataFrame) -> DataFrame:
    """
    Applies overrides using joins for large CSV files.

    Args:
        df (DataFrame): The input DataFrame.
        csv_spark_df (DataFrame): The CSV DataFrame.

    Returns:
        DataFrame: The updated DataFrame with overridden type and classification.

    NOT FULLY IMPLEMENTED
    """
    from pyspark.sql.functions import col, when
    join_conditions = get_join_conditions(df, csv_spark_df)
    if config.mode == "pi":
        df = (
            df.join(csv_spark_df, join_conditions, "left_outer")
            .withColumn(
                "classification",
                when(
                    col("classification_override").isNotNull(),
                    col("classification_override"),
                ).otherwise(col("classification")),
            )
            .withColumn(
                "type",
                when(col("type_override").isNotNull(), col("type_override")).otherwise(
                    col("type")
                ),
            )
            .drop("classification_override", "type_override")
        )
    elif config.mode == "comment":
        df = (
            df.join(csv_spark_df, join_conditions, "left_outer")
            .withColumn(
                "column_content",
                when(
                    col("comment_override").isNotNull(), col("comment_override")
                ).otherwise(col("column_content")),
            )
            .drop("comment_override")
        )
    else:
        raise ValueError("Invalid mode provided.")

    return df


def build_condition(df, table, column, schema, catalog):
    """
    Builds the condition for the DataFrame filtering.

    Supported parameter combinations:
    1. Only column name is provided (all other parameters are None or empty)
    2. All parameters (catalog, schema, table, column) are provided
    3. Table-level: (catalog, schema, table) provided, column is None (for domain/comment table overrides)

    Args:
        df (DataFrame): The input DataFrame.
        table (str): The table name.
        column (str): The column name (can be None for table-level conditions).
        schema (str): The schema name.
        catalog (str): The catalog name.

    Returns:
        Column: The condition column.

    Raises:
        ValueError: If the combination of inputs is not one of the supported patterns.
    """
    from pyspark.sql.functions import col
    table = table if table else None
    schema = schema if schema else None
    catalog = catalog if catalog else None

    only_column = column and not any([table, schema, catalog])
    all_params = all([column, table, schema, catalog])
    table_level = all([table, schema, catalog]) and not column

    if only_column:
        return col("column_name") == column
    elif all_params:
        return reduce(
            lambda x, y: x & y,
            [
                col("column_name") == column,
                col("table_name") == table,
                col("schema") == schema,
                col("catalog") == catalog,
            ],
        )
    elif table_level:
        return reduce(
            lambda x, y: x & y,
            [
                col("table_name") == table,
                col("schema") == schema,
                col("catalog") == catalog,
            ],
        )
    else:
        raise ValueError(
            f"Unsupported parameter combination: catalog={catalog!r}, schema={schema!r}, "
            f"table={table!r}, column={column!r}. Supported patterns:\n"
            "1. Only column name (column-level override across all tables)\n"
            "2. All parameters (catalog, schema, table, column)\n"
            "3. Table-level (catalog, schema, table) with column=None"
        )


def get_join_conditions(df: DataFrame, csv_spark_df: DataFrame) -> List[Column]:
    """
    Generates the join conditions for the DataFrame joins.

    Args:
        df (DataFrame): The input DataFrame.
        csv_spark_df (DataFrame): The CSV DataFrame.

    Returns:
        List[Column]: The list of join conditions.

    NOT IMPLEMENTED
    """
    join_condition = None

    if "column" in csv_spark_df.columns:
        join_condition = df["column_name"] == csv_spark_df["column"]
    if "table" in csv_spark_df.columns:
        table_condition = df["table_name"] == csv_spark_df["table"]
        join_condition = (
            join_condition & table_condition if join_condition else table_condition
        )
    if "schema" in csv_spark_df.columns:
        schema_condition = df["schema"] == csv_spark_df["schema"]
        join_condition = (
            join_condition & schema_condition if join_condition else schema_condition
        )
    if "catalog" in csv_spark_df.columns:
        catalog_condition = df["catalog"] == csv_spark_df["catalog"]
        join_condition = (
            join_condition & catalog_condition if join_condition else catalog_condition
        )
    return join_condition

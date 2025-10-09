from functools import reduce
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.column import Column
from typing import List, Dict
from src.dbxmetagen.config import MetadataConfig


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
    csv_df = pd.read_csv(csv_path)
    csv_df = csv_df.where(pd.notna(csv_df), None)

    # CRITICAL SERVERLESS FIX: Ensure all CSV columns are treated as strings to prevent type inference
    # Convert all columns to string type to prevent Spark Connect from inferring numeric types
    csv_df = csv_df.astype(str)
    # Replace 'None' strings back to actual None for proper null handling
    csv_df = csv_df.replace("None", None)

    csv_dict = csv_df.to_dict("records")
    spark = SparkSession.builder.getOrCreate()

    # Create DataFrame with explicit string schema to prevent type inference issues
    if len(csv_df) > 0:
        from pyspark.sql.types import StructType, StructField, StringType

        # Create explicit string schema for all columns
        schema = StructType(
            [StructField(col_name, StringType(), True) for col_name in csv_df.columns]
        )
        csv_spark_df = spark.createDataFrame(csv_df, schema)
    else:
        csv_spark_df = spark.createDataFrame(csv_df)
    nrows = csv_spark_df.count()
    print("Number of rows being overridden...", nrows)

    if nrows == 0:
        return df
    elif nrows < 10000:
        df = apply_overrides_with_loop(df, csv_dict, config)
    else:
        raise ValueError(
            "CSV file is too large. Please implement a more efficient method for large datasets."
        )
    return df


def apply_overrides_with_loop(df, csv_dict, config):
    if not csv_dict:
        return df

    if config.mode == "pi":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            column = row.get("column")
            classification_override = row["classification"]
            type_override = row["type"]

            if not column:
                continue

            # CRITICAL SERVERLESS FIX: Handle null/blank overrides properly
            # Skip if both overrides are None/null (no override needed)
            if (
                classification_override is None or pd.isna(classification_override)
            ) and (type_override is None or pd.isna(type_override)):
                print(f"Skipping null/blank overrides for column: {column}")
                continue

            # Convert to strings to prevent type inference issues
            if classification_override is not None and not pd.isna(
                classification_override
            ):
                classification_override = str(classification_override)
            if type_override is not None and not pd.isna(type_override):
                type_override = str(type_override)

            try:
                condition = build_condition(df, table, column, schema, catalog)

                # Apply classification override only if not null
                if classification_override is not None and not pd.isna(
                    classification_override
                ):
                    df = df.withColumn(
                        "classification",
                        when(
                            condition, lit(classification_override).cast("string")
                        ).otherwise(col("classification")),
                    )

                # Apply type override only if not null
                if type_override is not None and not pd.isna(type_override):
                    df = df.withColumn(
                        "type",
                        when(condition, lit(type_override).cast("string")).otherwise(
                            col("type")
                        ),
                    )
            except ValueError as e:
                print(f"Skipping row due to: {e}")

    elif config.mode == "comment":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            column = row.get("column")
            comment_override = row["comment"]

            if not column:
                continue

            # CRITICAL SERVERLESS FIX: Ensure comment_override is treated as string
            # Skip if comment_override is None/null (no override needed)
            if comment_override is None or pd.isna(comment_override):
                print(f"Skipping null/blank comment override for column: {column}")
                continue

            # Explicitly convert to string to prevent type inference issues
            comment_override = str(comment_override)

            try:
                condition = build_condition(df, table, column, schema, catalog)
                # CRITICAL: Cast the literal to string explicitly for serverless compatibility
                df = df.withColumn(
                    "column_content",
                    when(condition, lit(comment_override).cast("string")).otherwise(
                        col("column_content")
                    ),
                )
            except ValueError as e:
                print(f"Skipping row due to: {e}")

    elif config.mode == "domain":
        # Domain mode overrides work at table level only
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            domain_override = row.get("domain")
            subdomain_override = row.get("subdomain")

            # Domain overrides are table-level, so column should be None or empty
            if not table:
                print("Skipping row: table name is required for domain overrides")
                continue

            # Skip if both overrides are None/null
            if (domain_override is None or pd.isna(domain_override)) and (
                subdomain_override is None or pd.isna(subdomain_override)
            ):
                print(f"Skipping null/blank domain override for table: {table}")
                continue

            # Convert to strings
            if domain_override is not None and not pd.isna(domain_override):
                domain_override = str(domain_override)
            if subdomain_override is not None and not pd.isna(subdomain_override):
                subdomain_override = str(subdomain_override)

            try:
                # Build condition for table-level override (no column)
                condition = build_condition(df, table, None, schema, catalog)

                # Apply domain override
                if domain_override is not None and not pd.isna(domain_override):
                    df = df.withColumn(
                        "domain",
                        when(condition, lit(domain_override).cast("string")).otherwise(
                            col("domain")
                        ),
                    )

                # Apply subdomain override
                if subdomain_override is not None and not pd.isna(subdomain_override):
                    df = df.withColumn(
                        "subdomain",
                        when(
                            condition, lit(subdomain_override).cast("string")
                        ).otherwise(col("subdomain")),
                    )
            except ValueError as e:
                print(f"Skipping row due to: {e}")

    else:
        raise ValueError("Invalid mode provided. Must be 'pi', 'comment', or 'domain'.")

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
    3. Table-level: (catalog, schema, table) provided, column is None (for domain mode)

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
    table = table if table else None
    schema = schema if schema else None
    catalog = catalog if catalog else None

    # Pattern 1: Only column name (column-level override across all tables)
    only_column = column and not any([table, schema, catalog])

    # Pattern 2: All params including column (specific column in specific table)
    all_params = all([column, table, schema, catalog])

    # Pattern 3: Table-level (no column, for domain mode)
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
        # Table-level condition (for domain mode)
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
            "Unsupported parameter combination. Supported patterns:\n"
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

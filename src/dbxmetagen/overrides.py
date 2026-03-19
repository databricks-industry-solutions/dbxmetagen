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


def _resolve_csv_path(csv_path: str | None) -> str | None:
    """Resolve override CSV path, trying fallback locations for DAB job CWD mismatch."""
    import os

    if not csv_path:
        return None
    if os.path.isabs(csv_path):
        return csv_path if os.path.exists(csv_path) else None

    if os.path.exists(csv_path):
        return os.path.abspath(csv_path)

    fallbacks = [
        os.path.join("notebooks", csv_path),
        os.path.join("..", "notebooks", csv_path),
    ]
    for candidate in fallbacks:
        if os.path.exists(candidate):
            print(f"[override] CSV not at '{csv_path}', found at fallback '{candidate}'")
            return os.path.abspath(candidate)
    return None


def override_metadata_from_csv(
    df: DataFrame, csv_path: str, config: MetadataConfig, df_label: str = ""
) -> DataFrame:
    """
    Overrides the type and classification in the DataFrame based on the CSV file.
    This would need to be optimized if a customer has a large number of overrides.

    Args:
        df (DataFrame): The input DataFrame.
        csv_path (str): The path to the CSV file.
        config (MetadataConfig): The configuration object.
        df_label (str): Label for diagnostic output (e.g. "column_df", "table_df").

    Returns:
        DataFrame: The updated DataFrame with overridden type and classification.
    """
    import os

    tag = f"({df_label}) " if df_label else ""
    resolved = _resolve_csv_path(csv_path)
    print(f"[override] {tag}mode={config.mode}, csv_path='{csv_path}', resolved='{resolved}'")

    if resolved is None:
        print(
            f"[override] {tag}CSV not found at '{csv_path}' (cwd={os.getcwd()}) -- skipping overrides. "
            "Place the file next to the notebook or provide an absolute path."
        )
        return df

    csv_df = pd.read_csv(resolved, dtype=str, keep_default_na=False)
    csv_df = csv_df.replace("", None)

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
    print(f"[override] {tag}CSV loaded: {nrows} rows from '{resolved}'")

    if nrows == 0:
        print(f"[override] {tag}No override rows to apply, returning DataFrame unchanged.")
        return df
    elif nrows < 10000:
        df = apply_overrides_with_loop(df, csv_dict, config, df_label=df_label)
    else:
        raise ValueError(
            "CSV file is too large. Please implement a more efficient method for large datasets."
        )
    return df


def _is_blank(val):
    return val is None or (isinstance(val, float) and pd.isna(val)) or val == ""


def _count_matches(df, condition):
    """Count rows matching a Spark condition (triggers action)."""
    try:
        return df.filter(condition).count()
    except Exception as e:
        logger.warning("Could not verify row match count: %s", e)
        return -1


def _dump_column_names(df, tag=""):
    """Print distinct column_name values from the DataFrame for diagnostics."""
    if "column_name" not in df.columns:
        return
    try:
        names = sorted(
            r["column_name"] for r in df.select("column_name").distinct().collect()
        )
        print(f"[override] {tag}DataFrame column_name values: {names}")
    except Exception as e:
        logger.warning("Could not collect column_name values: %s", e)


def apply_overrides_with_loop(df, csv_dict, config, df_label=""):
    if not csv_dict:
        return df

    from pyspark.sql.functions import col, lit, when

    tag = f"({df_label}) " if df_label else ""
    applied_count = 0
    skipped_count = 0
    zero_match_count = 0

    _dump_column_names(df, tag=tag)

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
                continue

            if _is_blank(classification_override) and _is_blank(type_override):
                skipped_count += 1
                continue

            if not _is_blank(classification_override):
                classification_override = str(classification_override)
            if not _is_blank(type_override):
                type_override = str(type_override)

            try:
                condition = build_condition(df, table, column, schema, catalog)
                match_count = _count_matches(df, condition)
                if match_count == 0:
                    print(
                        f"[override] {tag}PI override: 0 rows match column='{column}' "
                        f"(table={table or '*'}) -- skipping"
                    )
                    zero_match_count += 1
                    continue

                if not _is_blank(classification_override):
                    df = df.withColumn(
                        "classification",
                        when(condition, lit(classification_override).cast("string")).otherwise(col("classification")),
                    )
                if not _is_blank(type_override):
                    df = df.withColumn(
                        "type",
                        when(condition, lit(type_override).cast("string")).otherwise(col("type")),
                    )
                applied_count += 1
                print(
                    f"[override] {tag}PI override: column='{column}' -> "
                    f"classification={classification_override!r}, type={type_override!r} "
                    f"({match_count} row(s) matched)"
                )
            except ValueError as e:
                skipped_count += 1
                print(f"[override] {tag}PI override SKIPPED for column='{column}': {e}")

    elif config.mode == "comment":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            column = row.get("column")
            comment_override = row.get("comment")

            if _is_blank(comment_override):
                skipped_count += 1
                continue

            comment_override = str(comment_override)

            try:
                if column:
                    condition = build_condition(df, table, column, schema, catalog)
                    match_count = _count_matches(df, condition)
                    if match_count == 0:
                        print(
                            f"[override] {tag}Comment override: 0 rows match column='{column}' "
                            f"(table={table or '*'}) -- skipping"
                        )
                        zero_match_count += 1
                        continue
                    df = df.withColumn(
                        "column_content",
                        when(condition, lit(comment_override).cast("string")).otherwise(col("column_content")),
                    )
                    applied_count += 1
                    print(
                        f"[override] {tag}Comment override: column='{column}' -> "
                        f"'{comment_override[:60]}' ({match_count} row(s) matched)"
                    )
                else:
                    if not table:
                        skipped_count += 1
                        continue
                    base_condition = build_condition(df, table, None, schema, catalog)
                    condition = base_condition & (col("ddl_type") == "table")
                    match_count = _count_matches(df, condition)
                    if match_count == 0:
                        print(
                            f"[override] {tag}Comment override (table-level): 0 rows match "
                            f"table='{table}' -- skipping"
                        )
                        zero_match_count += 1
                        continue
                    df = df.withColumn(
                        "column_content",
                        when(condition, lit(comment_override).cast("string")).otherwise(col("column_content")),
                    )
                    applied_count += 1
                    print(
                        f"[override] {tag}Comment override (table-level): table='{table}' -> "
                        f"'{comment_override[:60]}' ({match_count} row(s) matched)"
                    )
            except ValueError as e:
                skipped_count += 1
                target = column or table or "unknown"
                print(f"[override] {tag}Comment override SKIPPED for target='{target}': {e}")

    elif config.mode == "domain":
        for row in csv_dict:
            catalog = row.get("catalog")
            schema = row.get("schema")
            table = row.get("table")
            domain_override = row.get("domain")
            subdomain_override = row.get("subdomain")

            if not table:
                skipped_count += 1
                continue

            if _is_blank(domain_override) and _is_blank(subdomain_override):
                skipped_count += 1
                continue

            if not _is_blank(domain_override):
                domain_override = str(domain_override)
            if not _is_blank(subdomain_override):
                subdomain_override = str(subdomain_override)

            try:
                condition = build_condition(df, table, None, schema, catalog)
                match_count = _count_matches(df, condition)
                if match_count == 0:
                    print(
                        f"[override] {tag}Domain override: 0 rows match table='{table}' -- skipping"
                    )
                    zero_match_count += 1
                    continue

                if not _is_blank(domain_override):
                    df = df.withColumn(
                        "domain",
                        when(condition, lit(domain_override).cast("string")).otherwise(col("domain")),
                    )
                if not _is_blank(subdomain_override):
                    df = df.withColumn(
                        "subdomain",
                        when(condition, lit(subdomain_override).cast("string")).otherwise(col("subdomain")),
                    )
                applied_count += 1
                print(
                    f"[override] {tag}Domain override: table='{table}' -> "
                    f"domain={domain_override!r}, subdomain={subdomain_override!r} "
                    f"({match_count} row(s) matched)"
                )
            except ValueError as e:
                skipped_count += 1
                print(f"[override] {tag}Domain override SKIPPED for table='{table}': {e}")

    else:
        raise ValueError("Invalid mode provided. Must be 'pi', 'comment', or 'domain'.")

    print(
        f"[override] {tag}Summary: mode={config.mode}, applied={applied_count}, "
        f"skipped={skipped_count}, zero_matches={zero_match_count}, "
        f"total_csv_rows={len(csv_dict)}"
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
    Builds a match condition from whichever CSV fields are provided.

    Dynamically combines conditions for any non-empty subset of
    (catalog, schema, table, column).  At least one field must be given.

    CSV field -> DataFrame column mapping:
        column  -> col("column_name")
        table   -> col("table_name")
        schema  -> col("schema")
        catalog -> col("catalog")

    Args:
        df (DataFrame): The input DataFrame.
        table (str): The table name.
        column (str): The column name (can be None for table-level conditions).
        schema (str): The schema name.
        catalog (str): The catalog name.

    Returns:
        Column: The condition column.

    Raises:
        ValueError: If no fields are provided at all.
    """
    from pyspark.sql.functions import col, lower

    table = table if table else None
    schema = schema if schema else None
    catalog = catalog if catalog else None

    parts = []
    if column:
        parts.append(lower(col("column_name")) == column.lower())
    if table:
        parts.append(lower(col("table_name")) == table.lower())
    if schema:
        parts.append(lower(col("schema")) == schema.lower())
    if catalog:
        parts.append(lower(col("catalog")) == catalog.lower())

    if not parts:
        raise ValueError(
            f"No match fields provided: catalog={catalog!r}, schema={schema!r}, "
            f"table={table!r}, column={column!r}. "
            "At least one of (catalog, schema, table, column) must be non-empty."
        )

    return reduce(lambda x, y: x & y, parts)


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

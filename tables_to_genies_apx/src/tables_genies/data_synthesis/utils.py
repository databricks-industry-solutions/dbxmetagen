"""
Shared utility functions for synthetic data generation.
"""


def create_infrastructure(spark, catalog: str, schemas: list[str]):
    """
    Create catalog and schemas if they don't exist.
    
    Args:
        spark: SparkSession
        catalog: Catalog name
        schemas: List of schema names
    """
    print(f"Creating catalog: {catalog}")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as e:
        print(f"  Note: {e}")
    
    for schema in schemas:
        print(f"Creating schema: {catalog}.{schema}")
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        except Exception as e:
            print(f"  Note: {e}")


def save_table(spark, df_pandas, catalog: str, schema: str, table_name: str):
    """
    Convert pandas DataFrame to Spark DataFrame and save as table.
    
    Args:
        spark: SparkSession
        df_pandas: Pandas DataFrame
        catalog: Catalog name
        schema: Schema name
        table_name: Table name
    """
    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    print(f"  Saving {full_table_name} ({len(df_pandas):,} rows)")
    
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
    print(f"  ✓ Saved {full_table_name}")

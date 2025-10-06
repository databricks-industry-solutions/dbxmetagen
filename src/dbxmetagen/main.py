"""Entry point to dbxmetagen generate metadata."""

import os
import uuid
import mlflow
from pyspark.sql import SparkSession
from src.dbxmetagen.error_handling import validate_csv
from src.dbxmetagen.processing import (
    setup_ddl,
    create_tables,
    setup_queue,
    upsert_table_names_to_control_table,
    generate_and_persist_metadata,
    get_control_table,
)
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.deterministic_pi import ensure_spacy_model
from src.dbxmetagen.benchmarking import log_token_usage, setup_benchmarking


def get_dbr_version():
    """Note that this simply does not work in all situations."""
    dbr_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None)
    if dbr_version:
        print(f"Databricks Runtime Version: {dbr_version}")
    else:
        print("DATABRICKS_RUNTIME_VERSION environment variable not found.")
    return dbr_version


def main(kwargs):
    """Main function to generate metadata."""
    spark = SparkSession.builder.getOrCreate()
    dbr_version = None
    dbr_version = get_dbr_version()
    if not validate_csv("./metadata_overrides.csv"):
        raise Exception(
            """Invalid metadata_overrides.csv file. Please check the format of 
            your metadata_overrides configuration file..."""
        )

    config = MetadataConfig(**kwargs)
    experiment_name = setup_benchmarking(config)
    if (
        config.include_deterministic_pi or config.include_deterministic_pi == "true"
    ) and config.mode == "pi":
        ensure_spacy_model(config.spacy_model_names)

    if "client" in dbr_version and "excel" in (
        config.ddl_output_format,
        config.review_output_file_type,
    ):
        raise ValueError(
            "Serverless runtime is supported, but Excel writes are not supported."
        )

    if "ml" not in dbr_version and "excel" in (
        config.ddl_output_format,
        config.review_output_file_type,
    ):
        raise ValueError(
            "Excel writes in dbxmetagen are not supported on standard runtimes. Please change your output file type to tsv or sql if appropriate."
        )

    if not os.environ.get("DATABRICKS_HOST"):
        os.environ["DATABRICKS_HOST"] = config.base_url
    # os.environ["DATABRICKS_HOST"] = config.base_url
    setup_ddl(config)
    create_tables(config)
    config.table_names = setup_queue(config)
    if config.control_table:
        upsert_table_names_to_control_table(config.table_names, config)
    print("Running generate on...", config.table_names)
    generate_and_persist_metadata(config)
    # Get the unique temp table name for this specific job run
    temp_table = config.get_temp_metadata_log_table_name()
    control_table = get_control_table(config)
    control_table_full = f"{config.catalog_name}.{config.schema_name}.{control_table}"
    # Clean up this job's unique temp table using DROP (safe since each job has its own table)
    try:
        spark.sql(f"""DROP TABLE IF EXISTS {temp_table}""")
        print(f"Cleaned up temp table: {temp_table}")
    except Exception as e:
        print(f"Temp table cleanup failed: {e}")

    # For control table, use DELETE FROM since multiple jobs might share it (depending on config)
    try:
        if config.cleanup_control_table == "true" or config.cleanup_control_table:
            if config.job_id is not None:
                spark.sql(
                    f"""DELETE FROM {control_table_full} WHERE job_id = {config.job_id}"""
                )
            else:
                spark.sql(f"""DELETE FROM {control_table_full}""")
            print(f"Cleaned up control table: {control_table_full}")
    except Exception as e:
        # If table doesn't exist, that's fine - it means cleanup already happened
        print(f"Control table cleanup skipped (table may not exist): {e}")

    # Log token usage if benchmarking is enabled
    if experiment_name:
        log_token_usage(config, experiment_name)

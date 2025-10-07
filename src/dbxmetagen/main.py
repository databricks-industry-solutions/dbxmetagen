"""Entry point to dbxmetagen generate metadata."""

import os
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
    """Get Databricks Runtime version from environment."""
    dbr_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None)
    if dbr_version:
        print(f"Databricks Runtime Version: {dbr_version}")
    else:
        print("DATABRICKS_RUNTIME_VERSION environment variable not found.")
    return dbr_version


def validate_runtime_compatibility(dbr_version, config):
    """Validate runtime compatibility with output formats."""
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
            "Excel writes in dbxmetagen are not supported on standard runtimes. "
            "Please change your output file type to tsv or sql if appropriate."
        )


def setup_mode_dependencies(config):
    """Setup mode-specific dependencies and validate configurations."""
    if config.mode == "pi":
        # Setup for PI mode
        if config.include_deterministic_pi or config.include_deterministic_pi == "true":
            ensure_spacy_model(config.spacy_model_names)

    elif config.mode == "domain":
        # Setup for domain mode
        if not os.path.exists(config.domain_config_path):
            print(f"Warning: Domain config not found at {config.domain_config_path}")
            print("Domain classification will use fallback configuration")

    elif config.mode == "comment":
        # Comment mode has no special setup requirements
        pass

    else:
        raise ValueError(
            f"Invalid mode: {config.mode}. Must be 'comment', 'pi', or 'domain'."
        )


def setup_environment(config):
    """Setup Databricks environment variables."""
    if not os.environ.get("DATABRICKS_HOST"):
        os.environ["DATABRICKS_HOST"] = config.base_url


def initialize_infrastructure(config):
    """Initialize DDL directories, tables, and queue."""
    setup_ddl(config)
    create_tables(config)
    config.table_names = setup_queue(config)
    if config.control_table:
        upsert_table_names_to_control_table(config.table_names, config)
    print("Running generate on...", config.table_names)


def cleanup_resources(config, spark):
    """Cleanup temporary tables and control tables."""
    temp_table = config.get_temp_metadata_log_table_name()
    control_table = get_control_table(config)
    control_table_full = f"{config.catalog_name}.{config.schema_name}.{control_table}"

    # Clean up temp table
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
        print(f"Cleaned up temp table: {temp_table}")
    except Exception as e:
        print(f"Temp table cleanup failed: {e}")

    # Clean up control table
    try:
        if config.cleanup_control_table == "true" or config.cleanup_control_table:
            if config.job_id is not None:
                spark.sql(
                    f"DELETE FROM {control_table_full} WHERE job_id = {config.job_id}"
                )
            else:
                spark.sql(f"DELETE FROM {control_table_full}")
            print(f"Cleaned up control table: {control_table_full}")
    except Exception as e:
        print(f"Control table cleanup skipped (table may not exist): {e}")


def main(kwargs):
    """Main function to generate metadata."""
    # Initialize Spark and get runtime info
    spark = SparkSession.builder.getOrCreate()
    dbr_version = get_dbr_version()

    # Validate override CSV
    if not validate_csv("./metadata_overrides.csv"):
        raise ValueError(
            "Invalid metadata_overrides.csv file. Please check the format of "
            "your metadata_overrides configuration file."
        )

    # Initialize configuration and benchmarking
    config = MetadataConfig(**kwargs)
    experiment_name = setup_benchmarking(config)

    # Validate runtime compatibility
    validate_runtime_compatibility(dbr_version, config)

    # Setup mode-specific dependencies
    setup_mode_dependencies(config)

    # Setup environment and infrastructure
    setup_environment(config)
    initialize_infrastructure(config)

    # Generate metadata
    generate_and_persist_metadata(config)

    # Cleanup resources
    cleanup_resources(config, spark)

    # Log token usage if benchmarking is enabled
    if experiment_name:
        log_token_usage(config, experiment_name)

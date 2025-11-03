"""Entry point to dbxmetagen generate metadata."""

import os
from pyspark.sql import SparkSession
from dbxmetagen.error_handling import validate_csv
from dbxmetagen.processing import (
    setup_ddl,
    create_tables,
    setup_queue,
    upsert_table_names_to_control_table,
    generate_and_persist_metadata,
    get_control_table,
)
from dbxmetagen.config import MetadataConfig
from dbxmetagen.deterministic_pi import ensure_spacy_model
from dbxmetagen.benchmarking import log_token_usage, setup_benchmarking
from dbxmetagen.databricks_utils import (
    grant_user_permissions,
    grant_group_permissions,
)
from dbxmetagen.databricks_utils import get_dbr_version


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
        if config.include_deterministic_pi or config.include_deterministic_pi == "true":
            ensure_spacy_model(config.spacy_model_names)

    elif config.mode == "domain":
        if not os.path.exists(config.domain_config_path):
            print(f"Warning: Domain config not found at {config.domain_config_path}")
            print("Domain classification will use fallback configuration")

    elif config.mode == "comment":
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


def _grant_permissions_to_groups(config, catalog_name, schema_name, volume_name):
    """Grant permissions to groups specified in config."""
    if not hasattr(config, "permission_groups") or not config.permission_groups:
        return False

    if str(config.permission_groups).lower() == "none":
        print("No permission groups specified")
        return True

    groups = [g.strip() for g in str(config.permission_groups).split(",") if g.strip()]
    for group in groups:
        print(f"Granting permissions to group: {group}")
        grant_group_permissions(
            catalog_name=catalog_name,
            schema_name=schema_name,
            group_name=group,
            volume_name=volume_name,
        )
        print(f"✓ Granted schema and volume permissions to group '{group}'")
    return True


def _grant_permissions_to_users(config, catalog_name, schema_name, volume_name):
    """Grant permissions to additional users specified in config."""
    if not hasattr(config, "permission_users") or not config.permission_users:
        return False

    if str(config.permission_users).lower() == "none":
        print("No additional permission users specified")
        return True

    users = [u.strip() for u in str(config.permission_users).split(",") if u.strip()]
    for user in users:
        print(f"Granting permissions to user: {user}")
        grant_user_permissions(
            catalog_name=catalog_name,
            schema_name=schema_name,
            current_user=user,
            volume_name=volume_name,
        )
        print(f"✓ Granted schema and volume permissions to {user}")
    return True


def grant_permissions_on_created_objects(config):
    """Grant permissions to groups and users specified in config."""
    if not getattr(config, "grant_permissions_after_creation", True):
        print("Permission grants disabled in config")
        return

    catalog_name = config.catalog_name
    schema_name = config.schema_name
    volume_name = config.volume_name

    try:
        print(f"Granting permissions to job user: {config.current_user}")
        grant_user_permissions(
            catalog_name=catalog_name,
            schema_name=schema_name,
            current_user=config.current_user,
            volume_name=volume_name,
        )
        print(f"✓ Granted schema and volume permissions to {config.current_user}")

        groups_granted = _grant_permissions_to_groups(
            config, catalog_name, schema_name, volume_name
        )
        users_granted = _grant_permissions_to_users(
            config, catalog_name, schema_name, volume_name
        )

        if not groups_granted and not users_granted:
            print("No additional groups or users specified for permissions")

    except Exception as e:
        print(f"⚠️ Warning: Could not grant some permissions: {e}")
        print("This is non-fatal - metadata generation completed successfully")


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
        if (
            config.cleanup_control_table == "true"
            or config.cleanup_control_table == True
        ):
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

    # Validate required parameters early
    catalog_name = kwargs.get("catalog_name", "")
    table_names = kwargs.get("table_names", "")

    if not catalog_name or str(catalog_name).lower() in ["none", "null", ""]:
        raise ValueError(
            "REQUIRED PARAMETER MISSING: catalog_name\n\n"
            "Please provide a valid catalog name.\n"
            "The catalog name cannot be 'none', 'null', or empty.\n\n"
            "Set it via:\n"
            "  - Notebook widget: 'Catalog Name (required)'\n"
            "  - Job parameter: catalog_name\n"
            "  - variables.yml: catalog_name.default\n\n"
            "Example: my_catalog"
        )

    if not table_names or str(table_names).lower() in ["none", "null", ""]:
        raise ValueError(
            "REQUIRED PARAMETER MISSING: table_names\n\n"
            "Please provide table names to process.\n"
            "Specify one or more tables in the format: catalog.schema.table\n\n"
            "Set it via:\n"
            "  - Notebook widget: 'Table Names - comma-separated (required)'\n"
            "  - Job parameter: table_names\n\n"
            "Examples:\n"
            "  - Single table: my_catalog.my_schema.my_table\n"
            "  - Multiple tables: my_catalog.schema1.table1, my_catalog.schema2.table2"
        )

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

    # Grant permissions on created objects
    grant_permissions_on_created_objects(config)

    # Cleanup resources
    cleanup_resources(config, spark)

    # Log token usage if benchmarking is enabled
    if experiment_name:
        log_token_usage(config, experiment_name)

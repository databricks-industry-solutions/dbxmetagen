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
from src.dbxmetagen.databricks_utils import (
    grant_user_permissions,
    grant_group_permissions,
)


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
    if not os.environ.get("DATABRICKS_HOST") and config.base_url:
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
        return True

    groups = [g.strip() for g in str(config.permission_groups).split(",") if g.strip()]
    any_granted = False
    for group in groups:
        granted = grant_group_permissions(
            catalog_name=catalog_name,
            schema_name=schema_name,
            group_name=group,
            volume_name=volume_name,
        )
        if granted:
            print(f"Granted permissions to group: {group}")
            any_granted = True
    return any_granted


def _grant_permissions_to_users(config, catalog_name, schema_name, volume_name):
    """Grant permissions to additional users specified in config."""
    if not hasattr(config, "permission_users") or not config.permission_users:
        return False

    if str(config.permission_users).lower() == "none":
        return True

    users = [u.strip() for u in str(config.permission_users).split(",") if u.strip()]
    any_granted = False
    for user in users:
        granted = grant_user_permissions(
            catalog_name=catalog_name,
            schema_name=schema_name,
            current_user=user,
            volume_name=volume_name,
        )
        if granted:
            print(f"Granted permissions to user: {user}")
            any_granted = True
    return any_granted


def grant_permissions_on_created_objects(config):
    """Grant permissions to groups and users specified in config.

    This function attempts to grant permissions but failures are non-fatal.
    Permission grants can fail if the user/service principal doesn't have
    admin privileges, which is common and expected in many deployments.
    """
    if not getattr(config, "grant_permissions_after_creation", True):
        print(
            "Permission grants disabled in config (grant_permissions_after_creation=false)"
        )
        return

    catalog_name = config.catalog_name
    schema_name = config.schema_name
    volume_name = config.volume_name

    try:
        # Try to grant permissions - will be skipped if user lacks MANAGE
        job_user_granted = grant_user_permissions(
            catalog_name=catalog_name,
            schema_name=schema_name,
            current_user=config.current_user,
            volume_name=volume_name,
        )
        
        groups_granted = _grant_permissions_to_groups(
            config, catalog_name, schema_name, volume_name
        )
        users_granted = _grant_permissions_to_users(
            config, catalog_name, schema_name, volume_name
        )
        
        # Print summary message
        if job_user_granted or groups_granted or users_granted:
            print("Permission grants completed successfully")
        else:
            print("Skipped permission grants - current user lacks MANAGE on schema")

        if not groups_granted and not users_granted:
            print("No additional groups or users specified for permissions")

    except Exception as e:
        print("\n" + "=" * 80)
        print("⚠️  PERMISSION GRANT WARNING (NON-FATAL)")
        print("=" * 80)
        print(f"Could not grant some permissions: {e}")
        print("\nWhy this happens:")
        print("  - The job user/service principal may not have admin privileges")
        print("  - This is common and expected in production environments")
        print("\nWhy it's OK:")
        print("  - Metadata generation completed successfully")
        print("  - Generated metadata is available in the output catalog/schema")
        print("  - You can manually grant permissions if needed")
        print("\nTo disable these grant attempts:")
        print("  - Set 'grant_permissions_after_creation: false' in variables.yml")
        print("  - Or pass grant_permissions_after_creation=false to the job")
        print("\nTo manually grant permissions, run:")
        print(
            f"  GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `<user_or_group>`;"
        )
        print(
            f"  GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `<user_or_group>`;"
        )
        if volume_name:
            print(
                f"  GRANT READ VOLUME ON VOLUME {catalog_name}.{schema_name}.{volume_name} TO `<user_or_group>`;"
            )
        print("=" * 80 + "\n")


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
            if config.run_id is not None:
                # Selective cleanup based on status
                # By default, only delete completed entries (keep failed for retry)
                cleanup_failed = getattr(config, 'cleanup_failed_tables', False)
                
                if cleanup_failed:
                    # Delete all entries for this run (completed AND failed)
                    spark.sql(
                        f"DELETE FROM {control_table_full} WHERE _run_id = '{config.run_id}'"
                    )
                    print(
                        f"Cleaned up all control table rows for run_id {config.run_id}: {control_table_full}"
                    )
                else:
                    # Only delete completed entries, keep failed for potential retry
                    spark.sql(
                        f"DELETE FROM {control_table_full} WHERE _run_id = '{config.run_id}' AND _status = 'completed'"
                    )
                    print(
                        f"Cleaned up completed control table rows for run_id {config.run_id}: {control_table_full}"
                    )
                    # Log if there are failed entries remaining
                    failed_count = spark.sql(
                        f"SELECT COUNT(*) as cnt FROM {control_table_full} WHERE _run_id = '{config.run_id}' AND _status = 'failed'"
                    ).first().cnt
                    if failed_count > 0:
                        print(f"Note: {failed_count} failed table(s) retained for potential retry")
            elif config.job_id is not None:
                # Fallback to job_id for backward compatibility
                spark.sql(
                    f"DELETE FROM {control_table_full} WHERE _job_id = '{config.job_id}'"
                )
                print(
                    f"Cleaned up control table rows for job_id {config.job_id}: {control_table_full}"
                )
            else:
                spark.sql(f"DROP TABLE IF EXISTS {control_table_full}")
                print(f"Dropped control table: {control_table_full}")
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

    # Initialize configuration and benchmarking
    config = MetadataConfig(**kwargs)
    experiment_name = setup_benchmarking(config)

    # Validate override CSV (only if manual overrides are enabled)
    if getattr(config, "allow_manual_override", True):
        override_path = (
            config.override_csv_path
            if hasattr(config, "override_csv_path")
            else "metadata_overrides.csv"
        )
        # Only validate if file exists (it's optional)
        if os.path.exists(override_path):
            if not validate_csv(override_path):
                raise ValueError(
                    f"Invalid {override_path} file. Please check the format of "
                    "your metadata_overrides configuration file."
                )

    # Validate runtime compatibility
    validate_runtime_compatibility(dbr_version, config)

    # Setup mode-specific dependencies
    setup_mode_dependencies(config)

    # Use try/finally to ensure cleanup runs even on errors
    # The finally block ensures temp tables are cleaned up, but exceptions still propagate
    try:
        # Setup environment and infrastructure
        setup_environment(config)
        initialize_infrastructure(config)

        # Generate metadata
        generate_and_persist_metadata(config)

        # Grant permissions on created objects
        grant_permissions_on_created_objects(config)

        # Log token usage if benchmarking is enabled
        if experiment_name:
            log_token_usage(config, experiment_name)
    finally:
        # Always cleanup resources, even if there were errors above
        # cleanup_resources has internal error handling to prevent masking original exceptions
        cleanup_resources(config, spark)

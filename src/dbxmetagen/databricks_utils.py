"""Databricks environment setup utilities."""

import os
import json
import logging
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# from databricks.sdk.core import _InactiveRpcError
from grpc._channel import _InactiveRpcError, _MultiThreadedRendezvous

logger = logging.getLogger(__name__)

# Cache for schema permission checks to avoid repeated queries
_schema_permission_cache = {}


def setup_databricks_environment(dbutils_instance=None):
    """Set up Databricks environment variables and return current user."""
    current_user = None
    try:
        w = WorkspaceClient()
        current_user = w.current_user.me().user_name
        if w.config.host:
            os.environ["DATABRICKS_HOST"] = w.config.host.rstrip("/")
        print(f"✓ Successfully authenticated as: {current_user}")
    except Exception:
        print("Warning: Could not get user info from WorkspaceClient")

    # Always try to set token from dbutils if available (needed for API calls)
    if dbutils_instance:
        try:
            context_json = (
                dbutils_instance.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .safeToJson()
            )
            context = json.loads(context_json)
            attrs = context.get("attributes", {})

            if not current_user:
                current_user = attrs.get("user")
            if not os.environ.get("DATABRICKS_HOST"):
                api_url = attrs.get("api_url")
                if api_url:
                    os.environ["DATABRICKS_HOST"] = api_url.rstrip("/")

            api_token = attrs.get("api_token")
            if api_token:
                os.environ["DATABRICKS_TOKEN"] = api_token
        except Exception as e:
            print(f"Warning: Could not get context from dbutils: {e}")

    return current_user


def get_job_context(job_id, dbutils_instance=None):
    """Get job context information if running in a job."""
    try:
        if job_id:
            return job_id

        if dbutils_instance:
            context_json = (
                dbutils_instance.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .safeToJson()
            )
            context = json.loads(context_json)
            return context.get("tags", {}).get("jobId")

        return None
    except Exception as e:
        print(f"Error getting job context: {e}")
        return None


def get_task_id(dbutils_instance=None):
    """Get task run ID from Databricks context for concurrent task identification.
    
    Returns a unique identifier for this task execution, used for table claiming
    in concurrent processing scenarios.
    """
    try:
        if dbutils_instance:
            context_json = (
                dbutils_instance.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .safeToJson()
            )
            context = json.loads(context_json)
            # Try taskRunId first (available in job tasks)
            task_run_id = context.get("tags", {}).get("taskRunId")
            if task_run_id:
                return task_run_id
            # Fallback to multitaskParentRunId if available
            parent_run_id = context.get("tags", {}).get("multitaskParentRunId")
            if parent_run_id:
                return f"{parent_run_id}_interactive"
        # Generate a UUID as fallback for interactive/local runs
        import uuid
        return str(uuid.uuid4())
    except Exception as e:
        print(f"Error getting task ID: {e}")
        import uuid
        return str(uuid.uuid4())


def setup_widgets(dbutils):
    """Setup widgets for the notebook."""
    dbutils.widgets.dropdown(
        "cleanup_control_table", "false", ["true", "false"], "Cleanup Control Table"
    )
    dbutils.widgets.dropdown("mode", "comment", ["comment", "pi", "domain"], "Mode")
    dbutils.widgets.text("env", "", "Environment")
    dbutils.widgets.text("catalog_name", "", "Output Catalog Name (required)")
    dbutils.widgets.text("schema_name", "", "Output Schema Name")
    dbutils.widgets.text("host", "", "Host URL (if different from current)")
    dbutils.widgets.text("table_names", "", "Table Names - comma-separated (required)")
    dbutils.widgets.text("current_user", "", "Current User")
    dbutils.widgets.dropdown("apply_ddl", "false", ["true", "false"], "Apply DDL")
    dbutils.widgets.text("columns_per_call", "")
    dbutils.widgets.text("sample_size", "")
    dbutils.widgets.text("job_id", "")
    dbutils.widgets.text("run_id", "")


def get_widgets(dbutils):
    """Get widgets for the notebook."""
    cleanup_control_table = dbutils.widgets.get("cleanup_control_table")
    mode = dbutils.widgets.get("mode")
    env = dbutils.widgets.get("env")
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_name = dbutils.widgets.get("schema_name")
    host_name = dbutils.widgets.get("host")
    table_names = dbutils.widgets.get("table_names")
    current_user = dbutils.widgets.get("current_user")
    apply_ddl = dbutils.widgets.get("apply_ddl")
    columns_per_call = dbutils.widgets.get("columns_per_call")
    sample_size = dbutils.widgets.get("sample_size")
    run_id = dbutils.widgets.get("run_id")
    notebook_variables = {
        "cleanup_control_table": cleanup_control_table,
        "mode": mode,
        "env": env,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "host_name": host_name,
        "table_names": table_names,
        "current_user": current_user,
        "apply_ddl": apply_ddl,
        "columns_per_call": columns_per_call,
        "sample_size": sample_size,
        "run_id": run_id,
    }
    return {k: v for k, v in notebook_variables.items() if v is not None and v != ""}


def get_current_user(dbutils_instance=None, current_user_param=None):
    """Get current user from parameter or detected user."""
    # Set up Databricks environment variables and get current user
    detected_user = setup_databricks_environment(dbutils_instance)
    if current_user_param and current_user_param.strip():
        current_user = current_user_param.strip()
        print(f"Using current_user parameter: {current_user}")
    else:
        current_user = detected_user
        print(f"Using detected current_user: {current_user}")
    return current_user


def get_notebook_path(dbutils_instance):
    """Get the current notebook path. Works across serverless, dedicated, and shared runtimes (DBR 13.3+)."""
    try:
        context_json = (
            dbutils_instance.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .safeToJson()
        )
        context = json.loads(context_json)
        return context.get("attributes", {}).get("notebook_path")
    except Exception as e:
        print(f"Could not get notebook path: {e}")
        return None


def setup_notebook_variables(dbutils):
    """Setup notebook variables and validate required parameters."""
    try:
        job_id = dbutils.widgets.get("job_id")
    except ValueError:
        job_id = None
    try:
        run_id = dbutils.widgets.get("run_id")
    except ValueError:
        run_id = None
    try:
        notebook_variables = get_widgets(dbutils)
    except Exception:
        notebook_variables = {}
    job_id = get_job_context(job_id, dbutils)
    task_id = get_task_id(dbutils)
    current_user = get_current_user(dbutils_instance=dbutils)
    notebook_path = get_notebook_path(dbutils)
    notebook_variables["job_id"] = job_id
    notebook_variables["run_id"] = run_id
    notebook_variables["task_id"] = task_id
    notebook_variables["current_user"] = current_user
    notebook_variables["notebook_path"] = notebook_path

    # Validate required parameters
    catalog_name = notebook_variables.get("catalog_name", "")
    table_names = notebook_variables.get("table_names", "")

    # Check if catalog_name is missing or set to 'none'
    if not catalog_name or catalog_name.lower() in ["none", "null", ""]:
        raise ValueError(
            "❌ REQUIRED PARAMETER MISSING: catalog_name\n\n"
            "Please provide a valid catalog name using the 'Catalog Name (required)' widget.\n"
            "The catalog name cannot be 'none', 'null', or empty.\n\n"
            "Example: my_catalog"
        )

    # Check if table_names is missing or set to 'none'
    if not table_names or table_names.lower() in ["none", "null", ""]:
        raise ValueError(
            "❌ REQUIRED PARAMETER MISSING: table_names\n\n"
            "Please provide table names using the 'Table Names - comma-separated (required)' widget.\n"
            "Specify one or more tables in the format: catalog.schema.table\n\n"
            "Examples:\n"
            "  - Single table: my_catalog.my_schema.my_table\n"
            "  - Multiple tables: my_catalog.schema1.table1, my_catalog.schema2.table2"
        )

    return notebook_variables


def user_can_manage_schema(catalog_name: str, schema_name: str) -> bool:
    """
    Check if the current user has MANAGE permission on the schema.
    Queries system tables to check permissions without triggering grant operations.
    Results are cached to avoid repeated queries during a session.
    
    Returns:
        True if user can manage the schema, False otherwise
    """
    cache_key = f"{catalog_name}.{schema_name}"
    
    # Return cached result if available
    if cache_key in _schema_permission_cache:
        return _schema_permission_cache[cache_key]
    
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Query system.information_schema.schema_privileges to check if user has MANAGE
        # This is a read-only operation that won't generate permission errors
        current_user = spark.sql("SELECT current_user()").collect()[0][0]
        
        # Check for MANAGE privilege on schema
        # Note: Users may also have inherited permissions from groups or catalog ownership
        result = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM system.information_schema.schema_privileges
            WHERE catalog_name = '{catalog_name}'
              AND schema_name = '{schema_name}'
              AND grantee = '{current_user}'
              AND privilege_type = 'MANAGE'
        """).collect()
        
        if result and result[0]["cnt"] > 0:
            _schema_permission_cache[cache_key] = True
            return True
            
        # Also check if user is owner of the schema (owners have implicit MANAGE)
        owner_result = spark.sql(f"""
            SELECT schema_owner FROM system.information_schema.schemata
            WHERE catalog_name = '{catalog_name}'
              AND schema_name = '{schema_name}'
              AND schema_owner = '{current_user}'
        """).collect()
        
        if owner_result and len(owner_result) > 0:
            _schema_permission_cache[cache_key] = True
            return True
        
        _schema_permission_cache[cache_key] = False
        return False
        
    except Exception as e:
        # If we can't check permissions (e.g., no access to system tables), 
        # assume we can try and let it fail gracefully
        logger.debug(f"Could not check schema permissions: {e}")
        _schema_permission_cache[cache_key] = True  # Optimistically assume we can try
        return True


def grant_user_permissions(
    catalog_name: str,
    schema_name: str,
    current_user: str,
    volume_name: str = None,
    table_name: str = None,
) -> bool:
    """
    Grant read permissions to a user on created schema and volume.

    Args:
        catalog_name: Catalog name
        schema_name: Schema name
        current_user: Email/username of the current user
        volume_name: Optional volume name
        table_name: Deprecated - use schema-level SELECT instead
    
    Returns:
        True if permissions were granted, False if skipped or failed
    """

    spark = SparkSession.builder.getOrCreate()

    # Check if we have permission to grant before attempting
    # This avoids noisy JVM stacktraces on serverless/shared compute
    if not user_can_manage_schema(catalog_name, schema_name):
        return False

    try:
        spark.sql(
            f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{current_user}`"
        )
        spark.sql(
            f"GRANT CREATE TABLE ON SCHEMA {catalog_name}.{schema_name} TO `{current_user}`"
        )
        spark.sql(
            f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{current_user}`"
        )

        if volume_name:
            spark.sql(
                f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {catalog_name}.{schema_name}.{volume_name} TO `{current_user}`"
            )
        
        return True

    except Exception as e:
        logger.error(f"Warning: Could not grant permissions to {current_user}: {e}")
        return False


def grant_group_permissions(
    catalog_name: str,
    schema_name: str,
    group_name: str = "account users",
    volume_name: str = None,
    table_pattern: str = None,
) -> bool:
    """
    Grant read permissions to a group on created schema and volume.
    This is more scalable than per-user grants.

    Args:
        catalog_name: Catalog name
        schema_name: Schema name
        group_name: Group name (default: "account users" for all users)
        volume_name: Optional volume name
        table_pattern: Deprecated - use schema-level SELECT instead
    
    Returns:
        True if permissions were granted, False if skipped or failed
    """
    spark = SparkSession.builder.getOrCreate()

    # Check if we have permission to grant before attempting
    # This avoids noisy JVM stacktraces on serverless/shared compute
    if not user_can_manage_schema(catalog_name, schema_name):
        return False

    try:
        spark.sql(
            f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{group_name}`"
        )
        spark.sql(
            f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{group_name}`"
        )

        if volume_name:
            spark.sql(
                f"GRANT READ VOLUME ON VOLUME {catalog_name}.{schema_name}.{volume_name} TO `{group_name}`"
            )
        
        return True

    except Exception as e:
        print(f"Warning: Could not grant permissions to group {group_name}: {e}")
        return False

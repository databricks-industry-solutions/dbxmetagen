"""Databricks environment setup utilities."""

import os
import json
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# from databricks.sdk.core import _InactiveRpcError
from grpc._channel import _InactiveRpcError


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


def setup_widgets(dbutils):
    """Setup widgets for the notebook."""
    dbutils.widgets.dropdown("cleanup_control_table", "false", ["true", "false"])
    dbutils.widgets.dropdown("mode", "comment", ["comment", "pi"])
    dbutils.widgets.text("env", "")
    dbutils.widgets.text("catalog_name", "")
    dbutils.widgets.text("schema_name", "")
    dbutils.widgets.text("host", "")
    dbutils.widgets.text("table_names", "")
    dbutils.widgets.text("current_user", "")
    dbutils.widgets.text("apply_ddl", "")
    dbutils.widgets.text("columns_per_call", "")
    dbutils.widgets.text("sample_size", "")
    dbutils.widgets.text("job_id", "")


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
    }
    return {k: v for k, v in notebook_variables.items() if v is not None and v != ""}


# def get_host_name(host_name=None):
#     """Get host name from environment or parameter."""
#     if not host_name:
#         host_name = os.environ.get("DATABRICKS_HOST")
#     print("host_name", host_name)
#     print("DATABRICKS_HOST", os.environ.get("DATABRICKS_HOST"))
#     return host_name


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
    """Setup notebook variables."""
    try:
        job_id = dbutils.widgets.get("job_id")
    except ValueError as e:
        job_id = None
    try:
        notebook_variables = get_widgets(dbutils)
    except Exception:
        notebook_variables = {}
    job_id = get_job_context(job_id, dbutils)
    current_user = get_current_user(dbutils_instance=dbutils)
    notebook_path = get_notebook_path(dbutils)
    notebook_variables["job_id"] = job_id
    notebook_variables["current_user"] = current_user
    notebook_variables["notebook_path"] = notebook_path
    return notebook_variables


# notebook_variables = {
#     "catalog_name": catalog_name,
#     "schema_name": schema_name,
#     "host_name": host_name,
#     "table_names": table_names,
#     "mode": mode,
#     "env": env,
#     "current_user": current_user,
#     "cleanup_control_table": cleanup_control_table,
#     "job_id": job_id,
# }

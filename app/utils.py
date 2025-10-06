"""
Utility functions for DBX MetaGen Streamlit app
"""

import os
import json
import yaml
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    JobSettings,
    NotebookTask,
    JobCluster,
    ClusterSpec,
)
import time


def get_current_user_email():
    current_user = st.session_state.workspace_client.current_user.me()
    user_email = current_user.user_name
    username = user_email.split("@")[0]  # Get username part bef
    return user_email


class JobManager:
    """Manages Databricks job creation and monitoring"""

    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client

    def create_metadata_job(
        self,
        job_name: str,
        tables: List[str],
        config: Dict[str, Any],
        cluster_config: Dict[str, Any],
    ) -> Tuple[int, int]:
        """
        Create and run a metadata generation job

        Returns:
            Tuple of (job_id, run_id)
        """
        try:
            # Prepare job configuration
            job_settings = JobSettings(
                name=job_name,
                tasks=[
                    {
                        "task_key": "generate_metadata",
                        "notebook_task": NotebookTask(
                            notebook_path="./notebooks/generate_metadata",
                            base_parameters={
                                "table_names": ",".join(tables),
                                "mode": config.get("mode", "comment"),
                                "env": "app",
                                "cleanup_control_table": "true",
                            },
                        ),
                        "job_cluster_key": "metadata_cluster",
                        "libraries": [{"whl": "../dist/*.whl"}],
                    }
                ],
                job_clusters=[
                    JobCluster(
                        job_cluster_key="metadata_cluster",
                        new_cluster=ClusterSpec(
                            spark_version=cluster_config.get(
                                "spark_version", "16.4.x-cpu-ml-scala2.12"
                            ),
                            node_type_id=cluster_config.get(
                                "node_type_id", "Standard_D3_v2"
                            ),
                            autoscale={
                                "min_workers": cluster_config.get("min_workers", 1),
                                "max_workers": cluster_config.get("max_workers", 2),
                            },
                            spark_conf={
                                "spark.databricks.cluster.profile": (
                                    "singleNode"
                                    if cluster_config.get("min_workers", 1) == 0
                                    else None
                                )
                            },
                        ),
                    )
                ],
                email_notifications={
                    "on_failure": [self.client.current_user.me().user_name],
                    "on_success": [self.client.current_user.me().user_name],
                },
                timeout_seconds=7200,  # 2 hours
            )

            # Create the job
            job = self.client.jobs.create(**job_settings)

            # Run the job immediately
            run = self.client.jobs.run_now(
                job_id=job.job_id,
                notebook_params={
                    "table_names": ",".join(tables),
                    "mode": config.get("mode", "comment"),
                    "env": "app",
                },
            )

            return job.job_id, run.run_id

        except Exception as e:
            st.error(f"Failed to create job: {str(e)}")
            raise

    def get_run_status(self, run_id: int) -> Dict[str, Any]:
        """Get the status of a job run"""
        try:
            run_details = self.client.jobs.get_run(run_id)

            status_info = {
                "state": (
                    run_details.state.life_cycle_state.value
                    if run_details.state
                    else "UNKNOWN"
                ),
                "result_state": (
                    run_details.state.result_state.value
                    if run_details.state and run_details.state.result_state
                    else None
                ),
                "start_time": (
                    datetime.fromtimestamp(run_details.start_time / 1000)
                    if run_details.start_time
                    else None
                ),
                "end_time": (
                    datetime.fromtimestamp(run_details.end_time / 1000)
                    if run_details.end_time
                    else None
                ),
                "run_page_url": run_details.run_page_url,
                "setup_duration": run_details.setup_duration,
                "execution_duration": run_details.execution_duration,
                "cleanup_duration": run_details.cleanup_duration,
            }

            return status_info

        except Exception as e:
            st.error(f"Failed to get run status: {str(e)}")
            return {"state": "ERROR", "error": str(e)}

    def cancel_run(self, run_id: int) -> bool:
        """Cancel a running job"""
        try:
            self.client.jobs.cancel_run(run_id)
            return True
        except Exception as e:
            st.error(f"Failed to cancel run: {str(e)}")
            return False


class ConfigManager:
    """Manages configuration loading and validation"""

    @staticmethod
    def load_default_variables() -> Dict[str, Any]:
        """Load default configuration from variables.yml"""
        try:
            with open("../variables.yml", "r") as f:
                variables = yaml.safe_load(f)

            # Extract default values
            defaults = {}
            for key, config in variables.get("variables", {}).items():
                defaults[key] = config.get("default")

            return defaults
        except FileNotFoundError:
            st.error(
                "variables.yml not found, please fix access to variables.yml for app"
            )
            return ConfigManager.get_builtin_defaults()
        except Exception as e:
            st.error(f"Error loading variables.yml: {str(e)}")
            return ConfigManager.get_builtin_defaults()

    # TODO: duplicating defaults in multiple places, need to consolidate, and fetch from variables.yml
    @staticmethod
    def get_builtin_defaults() -> Dict[str, Any]:
        """Get built-in default configuration"""
        return {
            "catalog_name": os.getenv("APP_NAME", "dbxmetagen"),
            "host": os.getenv("HOST"),
            "allow_data": True,
            "sample_size": 5,
            "mode": "comment",
            "disable_medical_information_value": True,
            "allow_data_in_comments": True,
            "add_metadata": True,
            "include_deterministic_pi": True,
            "apply_ddl": False,
            "ddl_output_format": "sql",
            "reviewable_output_format": "tsv",
            "model": "databricks-claude-3-7-sonnet",
            "max_tokens": 4096,
            "temperature": 0.1,
            "columns_per_call": 5,
            "schema_name": "metadata_results",
            "volume_name": "generated_metadata",
        }

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []

        # Required fields
        required_fields = ["catalog_name", "host"]
        for field in required_fields:
            if not config.get(field):
                errors.append(f"Missing required field: {field}")

        # Validate host format
        host = config.get("host", "")
        if host and not (host.startswith("https://") or host.startswith("http://")):
            errors.append("Host must start with https:// or http://")

        # Validate numeric fields
        numeric_fields = {
            "sample_size": (0, 100),
            "max_tokens": (1, 32768),
            "temperature": (0.0, 2.0),
            "columns_per_call": (1, 50),
        }

        for field, (min_val, max_val) in numeric_fields.items():
            value = config.get(field)
            if value is not None:
                try:
                    num_value = float(value)
                    if not (min_val <= num_value <= max_val):
                        errors.append(
                            f"{field} must be between {min_val} and {max_val}"
                        )
                except (ValueError, TypeError):
                    errors.append(f"{field} must be a number")

        return errors


class TableManager:
    """Manages table validation and processing"""

    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client

    def validate_table_names(
        self, table_names: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate table names and return valid/invalid lists

        Returns:
            Tuple of (valid_tables, invalid_tables_with_reasons)
        """
        valid_tables = []
        invalid_tables = []

        for table_name in table_names:
            try:
                # Basic format validation
                parts = table_name.split(".")
                if len(parts) != 3:
                    invalid_tables.append(
                        f"{table_name} - Invalid format (must be catalog.schema.table)"
                    )
                    continue

                catalog, schema, table = parts
                if not all([catalog.strip(), schema.strip(), table.strip()]):
                    invalid_tables.append(
                        f"{table_name} - Empty catalog, schema, or table name"
                    )
                    continue

                # Try to check if table exists (optional, may require permissions)
                try:
                    # This is a basic existence check - you might want to enhance this
                    # based on your specific Databricks setup and permissions
                    valid_tables.append(table_name)
                except Exception:
                    # If we can't verify existence, assume it's valid for now
                    valid_tables.append(table_name)

            except Exception as e:
                invalid_tables.append(f"{table_name} - Error: {str(e)}")

        return valid_tables, invalid_tables

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a table"""
        try:
            # This would use the Databricks SDK to get table information
            # Implementation depends on your specific needs and permissions
            return {
                "name": table_name,
                "schema": "placeholder",
                "comment": "Table comment would be here",
                "columns": [],
            }
        except Exception as e:
            st.error(f"Error getting table info for {table_name}: {str(e)}")
            return None


class ResultsManager:
    """Manages results viewing and file processing"""

    @staticmethod
    def process_tsv_file(file_content: str) -> pd.DataFrame:
        """Process TSV file content and return DataFrame"""
        try:
            import io

            df = pd.read_csv(io.StringIO(file_content), sep="\t")
            return df
        except Exception as e:
            st.error(f"Error processing TSV file: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def process_csv_file(file_content: str) -> pd.DataFrame:
        """Process CSV file content and return DataFrame"""
        try:
            import io

            df = pd.read_csv(io.StringIO(file_content))
            return df
        except Exception as e:
            st.error(f"Error processing CSV file: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def format_sql_for_display(sql_content: str) -> str:
        """Format SQL content for better display"""
        # Basic SQL formatting - you might want to enhance this
        formatted = sql_content

        # Add some basic formatting
        sql_keywords = ["ALTER TABLE", "COMMENT", "SET", "ADD", "COLUMN"]
        for keyword in sql_keywords:
            formatted = formatted.replace(keyword, f"\n{keyword}")

        return formatted.strip()

    @staticmethod
    def create_download_link(
        data: str, filename: str, mime_type: str = "text/plain"
    ) -> str:
        """Create a download link for data"""
        import base64

        b64 = base64.b64encode(data.encode()).decode()
        return f'<a href="data:{mime_type};base64,{b64}" download="{filename}">Download {filename}</a>'


def format_duration(seconds: Optional[int]) -> str:
    """Format duration in seconds to human readable format"""
    if seconds is None:
        return "N/A"

    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds}s"
    else:
        hours = seconds // 3600
        remaining_minutes = (seconds % 3600) // 60
        return f"{hours}h {remaining_minutes}m"


def get_status_emoji(status: str) -> str:
    """Get emoji for job status"""
    status_emojis = {
        "PENDING": "⏳",
        "RUNNING": "🔄",
        "TERMINATING": "⏹️",
        "TERMINATED": "🛑",
        "SKIPPED": "⏭️",
        "INTERNAL_ERROR": "💥",
        "SUCCESS": "✅",
        "FAILED": "❌",
        "TIMEDOUT": "⏰",
        "CANCELED": "🚫",
    }
    return status_emojis.get(status, "❓")


def safe_get_env_var(var_name: str, default: str = "") -> str:
    """Safely get environment variable with default"""
    return os.environ.get(var_name, default)

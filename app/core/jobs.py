import os
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import (
    NotebookTask,
    JobEnvironment,
    Task,
    PerformanceTarget,
    JobEmailNotifications,
)
from databricks.sdk.service.compute import Environment

from core.config import DatabricksClientManager
from core.user_context import UserContextManager, AppConfig

logger = logging.getLogger(__name__)


class JobManager:
    """Handles Databricks job creation and monitoring - clean and simple"""

    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    # Step 2 Create & run job triggers this
    # TODO: Want to modify this to reuse the job if it exists.
    def create_metadata_job(
        self,
        job_name: str,
        tables: List[str],
        cluster_size: str,
        config: Dict[str, Any],
        user_email: Optional[str] = None,
    ) -> Tuple[int, int]:
        """
        Create and run a metadata generation job

        This method creates a new job each time.
        Currently kept as fallback in case SPN approach fails.
        New code should use _create_and_run_spn_job() instead.
        """
        logger.info(f"Creating metadata job: {job_name}")

        # Step 1: Validate inputs
        self._validate_inputs(job_name, tables, cluster_size, config)

        # Step 2: Prepare job configuration
        job_parameters = self._build_job_parameters(tables, config)
        notebook_path = self._resolve_notebook_path(config)

        # Step 3: Check if job already exists, if so reuse it
        existing_job_id = self._find_job_by_name(job_name)
        if existing_job_id:
            logger.info(
                f"Job '{job_name}' already exists with ID {existing_job_id}, triggering new run"
            )
            job_id = existing_job_id
        else:
            # Create new job
            job_id = self._create_job(
                job_name, notebook_path, job_parameters, user_email
            )

        # Step 4: Start job run
        run_id = self._start_job_run(job_id, job_parameters)

        # Step 5: Add to session state for tracking
        if "job_runs" not in st.session_state:
            st.session_state.job_runs = {}

        st.session_state.job_runs[run_id] = {
            "job_id": job_id,
            "job_name": job_name,
            "run_id": run_id,
            "tables": tables,
            "config": config,
            "status": "RUNNING",
            "created_at": datetime.now().isoformat(),
            "start_time": datetime.now(),
        }

        logger.info(f"Job created successfully - job_id: {job_id}, run_id: {run_id}")
        return job_id, run_id

    def get_run_status(self, run_id: int) -> Dict[str, Any]:
        """Get the status of a job run"""
        try:
            run_details = self.workspace_client.jobs.get_run(run_id)
            return {
                "status": run_details.state.life_cycle_state.value,
                "result_state": (
                    run_details.state.result_state.value
                    if run_details.state.result_state
                    else None
                ),
                "start_time": run_details.start_time,
                "end_time": run_details.end_time,
                "run_page_url": run_details.run_page_url,
            }
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}

    def refresh_job_status(self):
        """Refresh status of all tracked jobs"""
        if not st.session_state.get("job_runs"):
            st.info("No jobs to track")
            return

        try:
            for run_id, job_info in st.session_state.job_runs.items():
                try:
                    run_status = self.workspace_client.jobs.get_run(int(run_id))
                    old_status = job_info.get("status", "UNKNOWN")
                    new_status = run_status.state.life_cycle_state.value

                    job_info["status"] = new_status
                    job_info["last_updated"] = datetime.now().isoformat()

                    if run_status.state.result_state:
                        job_info["result_state"] = run_status.state.result_state.value

                    if old_status != new_status:
                        logger.info(
                            f"Run {run_id} status changed: {old_status} -> {new_status}"
                        )

                except Exception as e:
                    logger.error(f"Failed to get status for run {run_id}: {str(e)}")
                    job_info["status"] = f"ERROR: {str(e)}"

            st.success(
                f"✅ Refreshed status for {len(st.session_state.job_runs)} job runs"
            )

        except Exception as e:
            logger.error(f"Failed to refresh job status: {str(e)}")
            st.error(f"❌ Failed to refresh job status: {str(e)}")

    # === Private helper methods (small and composable) ===

    # Step 3 Create & run job triggers this, not actually needed because we're using serverless clusters.
    def _validate_inputs(
        self,
        job_name: str,
        tables: List[str],
        cluster_size: str,
        config: Dict[str, Any],
    ):
        """Validate all inputs for job creation"""
        if not job_name:
            raise ValueError("Job name is required")
        if not tables:
            raise ValueError("No tables specified for processing")
        if not config:
            raise ValueError("Configuration is required")

        valid_sizes = [
            "Small (1-2 workers)",
            "Medium (2-4 workers)",
            "Large (4-8 workers)",
        ]
        if cluster_size not in valid_sizes:
            raise ValueError(
                f"Invalid cluster size: {cluster_size}. Must be one of {valid_sizes}"
            )

    def _get_worker_config(self, cluster_size: str) -> Dict[str, int]:
        """Map cluster size to worker configuration"""
        worker_map = {
            "Small (1-2 workers)": {"min": 1, "max": 2},
            "Medium (2-4 workers)": {"min": 2, "max": 4},
            "Large (4-8 workers)": {"min": 4, "max": 8},
        }
        return worker_map[cluster_size]

    # Step 3 create & run job triggers this
    def _build_job_parameters(
        self, tables: List[str], config: Dict[str, Any], job_user: str = None
    ) -> Dict[str, str]:
        """Build job parameters for notebook execution"""

        # Get the job execution user (no hardcoded defaults)
        # If job_user is provided (e.g., for SPN mode), skip the get_job_user() call
        if job_user is None:
            job_user = UserContextManager.get_job_user(
                use_obo=config.get("use_obo", False)
            )
        st.info(f"Job user: {job_user}")
        catalog_name = AppConfig.get_catalog_name()

        job_params = {
            "table_names": tables if isinstance(tables, str) else ",".join(tables),
            "env": "app",
            "cleanup_control_table": "true",
            # Core settings
            "catalog_name": catalog_name,
            "host": config.get("host", ""),
            "schema_name": config.get("schema_name", "metadata_results"),
            "volume_name": config.get("volume_name", "generated_metadata"),
            # Data settings
            "allow_data": str(config.get("allow_data", False)).lower(),
            "sample_size": str(config.get("sample_size", 5)),
            "mode": config.get("mode", "comment"),
            "allow_data_in_comments": str(
                config.get("allow_data_in_comments", True)
            ).lower(),
            # Advanced settings
            "model": config.get("model", "databricks-claude-3-7-sonnet"),
            "max_tokens": str(config.get("max_tokens", 8192)),
            "temperature": str(config.get("temperature", 0.1)),
            "columns_per_call": str(config.get("columns_per_call", 5)),
            "apply_ddl": str(config.get("apply_ddl", False)).lower(),
            "ddl_output_format": config.get("ddl_output_format", "sql"),
            "reviewable_output_format": config.get("reviewable_output_format", "tsv"),
            # Additional flags
            "disable_medical_information_value": str(
                config.get("disable_medical_information_value", True)
            ).lower(),
            "add_metadata": str(config.get("add_metadata", True)).lower(),
            "include_deterministic_pi": str(
                config.get("include_deterministic_pi", True)
            ).lower(),
            # Pass actual current user to override config
            "current_user": job_user,
        }

        # Add domain config path if in domain mode
        if config.get("mode") == "domain":
            job_params["domain_config_path"] = config.get(
                "domain_config_path", ".configurations/domain_config.yaml"
            )

        return job_params

    def _detect_node_type(self, config: Dict[str, Any]) -> str:
        """Auto-detect appropriate node type based on workspace"""
        workspace_url = config.get("host", "")
        if "azure" in workspace_url.lower():
            return "Standard_D3_v2"
        elif "aws" in workspace_url.lower():
            return "i3.xlarge"
        elif "gcp" in workspace_url.lower():
            return "n1-standard-4"
        else:
            return "Standard_D3_v2"  # Default to Azure

    def _resolve_notebook_path(self, config: Dict[str, Any]) -> str:
        """Resolve the notebook path for job execution"""
        # 1) Explicit override
        explicit_path = os.environ.get("NOTEBOOK_PATH") or config.get("notebook_path")
        if explicit_path:
            logger.info(f"Using explicit NOTEBOOK_PATH override: {explicit_path}")
            return explicit_path

        # 2) With OBO, use the deploying user for notebook paths (not the app user)
        try:
            # Get deploying user from session state (loaded from deploying_user.yml)
            deploying_user = st.session_state.get("deploying_user")

            st.info(f"Deploying user: {deploying_user}")

            if deploying_user:  # TODO: marke this spot
                bundle_target = st.session_state.get("app_env", "dev")
                logger.info(f"Using deploying user: {deploying_user}")
                logger.info(f"Using bundle target: {bundle_target}")
                # st.info(f"Using bundle target: {bundle_target}")
                # path = f"/Workspace/Users/{deploying_user}/.bundle/dbxmetagen/{bundle_target}/files/notebooks/generate_metadata"
                # st.info(
                #     f"Resolved notebook path for deploying user {deploying_user}: {path}"
                # )
                logger.info(
                    f"Resolved notebook path for deploying user {deploying_user}: {path}"
                )
                return path
            else:
                logger.info("DEPLOY_USER_NAME not set, continuing to fallback")

        except Exception as e:
            logger.warning(f"Could not resolve deploying user path: {e}")

        # 3) Use user context manager (no hardcoded fallbacks)
        try:
            app_name = AppConfig.get_app_name()
            bundle_target = AppConfig.get_bundle_target()
            use_shared = config.get("use_shared_bundle_location", False)

            path = UserContextManager.get_notebook_path(
                notebook_name="generate_metadata",
                bundle_name=app_name,
                bundle_target=bundle_target,
                use_shared=use_shared,
            )
            logger.info(f"Using constructed path: {path}")
            return path
        except ValueError as e:
            logger.error(f"Failed to construct notebook path: {e}")
            raise ValueError(
                f"Cannot determine notebook path. Ensure user context is properly configured: {e}"
            )

    def _resolve_sync_notebook_path(self) -> str:
        """Resolve the notebook path for DDL sync job"""
        try:
            app_name = AppConfig.get_app_name()
            bundle_target = AppConfig.get_bundle_target()

            path = UserContextManager.get_notebook_path(
                notebook_name="sync_reviewed_ddl",
                bundle_name=app_name,
                bundle_target=bundle_target,
                use_shared=False,
            )

            logger.info(f"Resolved sync notebook path: {path}")
            return path

        except Exception as e:
            logger.error(f"Failed to construct sync notebook path: {e}")
            raise ValueError(
                f"Cannot determine sync notebook path. Ensure user context is properly configured: {e}"
            )

    def _find_job_by_name(self, job_name: str) -> Optional[int]:
        """Find a job by name and return its ID"""
        try:
            logger.info(f"Searching for job with name: '{job_name}'")
            jobs_list = self.workspace_client.jobs.list()

            found_jobs = []
            for job in jobs_list:
                if job.settings and job.settings.name:
                    found_jobs.append(f"'{job.settings.name}' (ID: {job.job_id})")
                    if job.settings.name == job_name:
                        logger.info(
                            f"✅ Found matching job '{job_name}' with ID: {job.job_id}"
                        )
                        return job.job_id

            logger.warning(
                f"❌ Job '{job_name}' not found. Available jobs: {found_jobs[:5]}"
            )  # Show first 5 for debugging
            return None
        except Exception as e:
            logger.error(f"Error finding job '{job_name}': {e}")
            return None

    def _get_app_deployment_type(self) -> str:
        """Get app deployment type from configuration"""
        # TODO: When OBO (On-Behalf-Of) authentication becomes available for jobs.jobs scope
        # for non-account admins, add support for OBO deployment type.
        # For now, only SPN is supported to avoid permission issues.
        return os.getenv("APP_DEPLOYMENT_TYPE", "SPN")

    def _update_job_permissions(self, job_id: int, current_user: str):
        """Update job permissions to include current app user without removing existing permissions"""
        try:
            from databricks.sdk.service.jobs import (
                JobAccessControlRequest,
                JobPermissionLevel,
            )

            # Get existing permissions
            existing_permissions = self.workspace_client.jobs.get_permissions(
                job_id=str(job_id)
            )

            # Create new ACL list starting with existing permissions
            acl = []

            # Add existing permissions
            if existing_permissions.access_control_list:
                for existing_acl in existing_permissions.access_control_list:
                    acl.append(
                        JobAccessControlRequest(
                            user_name=existing_acl.user_name,
                            group_name=existing_acl.group_name,
                            service_principal_name=existing_acl.service_principal_name,
                            permission_level=existing_acl.permission_level,
                        )
                    )

            # Add current user with CAN_VIEW permission if not already present
            user_already_has_permission = any(
                acl_item.user_name == current_user for acl_item in acl
            )

            if not user_already_has_permission:
                acl.append(
                    JobAccessControlRequest(
                        user_name=current_user,
                        permission_level=JobPermissionLevel.CAN_VIEW,
                    )
                )
                logger.info(
                    f"Added CAN_VIEW permission for {current_user} to job {job_id}"
                )

            # Update permissions
            self.workspace_client.jobs.set_permissions(
                job_id=str(job_id), access_control_list=acl
            )

        except Exception as e:
            logger.error(f"Failed to update job permissions for job {job_id}: {e}")
            # Don't raise - job can still run without this

    def _schedule_post_job_grants(self, run_id: int, app_user: str, catalog_name: str):
        """Schedule post-job permission grants for the app user"""
        # Store information for post-job processing
        if "pending_grants" not in st.session_state:
            st.session_state.pending_grants = {}

        st.session_state.pending_grants[run_id] = {
            "app_user": app_user,
            "catalog_name": catalog_name,
            "scheduled_at": datetime.now().isoformat(),
        }

        logger.info(f"Scheduled post-job grants for run {run_id}, user {app_user}")

    def _execute_post_job_grants(self, run_id: int):
        """Execute post-job permission grants using SQL warehouse"""
        if "pending_grants" not in st.session_state:
            return

        grant_info = st.session_state.pending_grants.get(run_id)
        if not grant_info:
            return

        try:
            app_user = grant_info["app_user"]
            catalog_name = grant_info["catalog_name"]

            # TODO: Implement SQL warehouse-based grants
            # This will use the SQL warehouse that the app has CAN_USE permission on
            # to grant read access to all schemas and new tables created by the job

            # Grant SELECT on all schemas in the catalog
            grant_queries = [
                f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{app_user}`",
                f"GRANT SELECT ON CATALOG `{catalog_name}` TO `{app_user}`",
            ]

            # TODO: Execute these grants using SQL warehouse when implementation is ready
            logger.info(
                f"Would execute post-job grants for {app_user} on catalog {catalog_name}"
            )
            logger.info(f"Grant queries prepared: {grant_queries}")

            # Remove from pending grants
            del st.session_state.pending_grants[run_id]

        except Exception as e:
            logger.error(f"Failed to execute post-job grants for run {run_id}: {e}")

    def _find_existing_cluster(self) -> Optional[str]:
        """Find an existing cluster that can be reused"""
        try:
            clusters = self.workspace_client.clusters.list()
            for cluster in clusters:
                if (
                    cluster.state
                    and cluster.state.value in ["RUNNING", "TERMINATED"]
                    and hasattr(cluster, "cluster_name")
                    and "shared" in cluster.cluster_name.lower()
                ):
                    logger.info(
                        f"Found existing cluster: {cluster.cluster_name} ({cluster.cluster_id})"
                    )
                    return cluster.cluster_id
        except Exception as e:
            logger.warning(f"Could not list clusters: {e}")
        return None

    def _create_job(
        self,
        job_name: str,
        notebook_path: str,
        job_parameters: Dict[str, str],
        user_email: Optional[str] = None,
    ) -> int:
        """Create the Databricks job"""
        # Validate critical parameters
        if not notebook_path:
            raise ValueError("Notebook path is empty - cannot create job")
        if not job_parameters.get("table_names"):
            raise ValueError("No table names provided - cannot create job")

        existing_cluster_id = self._find_existing_cluster()

        try:
            job = self.workspace_client.jobs.create(
                environments=[
                    JobEnvironment(
                        environment_key="default_python",
                        spec=Environment(environment_version="2"),
                    )
                ],
                performance_target=PerformanceTarget.PERFORMANCE_OPTIMIZED,
                name=job_name,
                tasks=[
                    Task(
                        description="Generate metadata for tables",
                        task_key="generate_metadata",
                        notebook_task=NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters=job_parameters,
                        ),
                        existing_cluster_id=existing_cluster_id,
                        timeout_seconds=14400,
                    )
                ],
                email_notifications=(
                    JobEmailNotifications(
                        on_failure=[user_email] if user_email else [],
                        on_success=[user_email] if user_email else [],
                    )
                    if user_email
                    else None
                ),
                max_concurrent_runs=1,
            )

            # Verify job creation
            created_job = self.workspace_client.jobs.get(job.job_id)
            task_count = (
                len(created_job.settings.tasks) if created_job.settings.tasks else 0
            )
            logger.info(f"Job created successfully with {task_count} tasks")

            return job.job_id

        except Exception as e:
            error_msg = f"Job creation failed: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _start_job_run(self, job_id: int, job_parameters: Dict[str, str]) -> int:
        """Start a job run and return the run ID"""
        try:
            run = self.workspace_client.jobs.run_now(
                job_id=job_id,
                notebook_params=job_parameters,
            )
            logger.info(f"Job run started successfully with run ID: {run.run_id}")
            return run.run_id

        except Exception as e:
            error_msg = f"Job run failed to start: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    # === Job creation UI methods ===

    # Step 1 Create & run job triggers this
    def create_and_run_metadata_job(self, tables: List[str]):
        """Create and run a metadata generation job using SPN deployment type"""
        try:
            # Recheck authentication before starting job operations
            if not DatabricksClientManager.recheck_authentication():
                st.error(
                    "❌ Authentication check failed. Please refresh the page and try again."
                )
                return

            # Get deployment type from app configuration
            # app_deployment_type = self._get_app_deployment_type()

            # if app_deployment_type == "SPN":
            #    self._create_and_run_spn_job(tables, "metadata")
            # else:
            # Fallback to legacy approach if needed (for debugging/troubleshooting)
            # st.warning(
            #     f"⚠️ Using legacy job creation approach for deployment type: {app_deployment_type}"
            # )
            job_name = "dbxmetagen_app_job"
            job_id, run_id = self.create_metadata_job(
                job_name=job_name,
                tables=tables,
                cluster_size="Medium (2-4 workers)",
                config=st.session_state.config,
            )
            return job_id, run_id

        except Exception as e:
            st.error(f"❌ Job execution failed: {str(e)}")
            logger.error(f"Job execution failed: {str(e)}", exc_info=True)

            # Show debug information for troubleshooting
            st.markdown("**Debug Information:**")
            st.write(f"- Tables: {len(tables)} total")
            st.write(
                f"- Config available: {'Yes' if st.session_state.config else 'No'}"
            )
            st.write(
                f"- Workspace client: {'Yes' if st.session_state.get('workspace_client') else 'No'}"
            )

            import traceback

            st.code(traceback.format_exc())

    def create_sync_metadata_job(self, df, filename: str):
        """Create a job to sync reviewed metadata"""
        try:
            from pandas import DataFrame

            job_parameters = {
                "catalog_name": AppConfig.get_catalog_name(),
                "schema_name": st.session_state.config.get(
                    "schema_name", "metadata_results"
                ),
                "volume_name": st.session_state.config.get(
                    "volume_name", "generated_metadata"
                ),
                "reviewed_metadata_file": filename,
                "apply_changes": "true",
                "job_id": st.session_state.job_id,
            }

            job_name = (
                f"sync_reviewed_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            # Get notebook path
            app_name = AppConfig.get_app_name()
            bundle_target = AppConfig.get_bundle_target()
            use_shared = st.session_state.config.get(
                "use_shared_bundle_location", False
            )

            notebook_path = UserContextManager.get_notebook_path(
                notebook_name="sync_reviewed_ddl",
                bundle_name=app_name,
                bundle_target=bundle_target,
                use_shared=use_shared,
            )

            job = self.workspace_client.jobs.create(
                environments=[
                    JobEnvironment(
                        environment_key="default_python",
                        spec=Environment(environment_version="2"),
                    )
                ],
                performance_target=PerformanceTarget.PERFORMANCE_OPTIMIZED,
                name=job_name,
                tasks=[
                    Task(
                        task_key="sync_metadata",
                        notebook_task=NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters=job_parameters,
                        ),
                        timeout_seconds=14400,
                    )
                ],
                max_concurrent_runs=1,
            )

            st.success(f"✅ Sync job created! Job ID: {job.job_id}")
            st.info(
                "📋 Upload your reviewed metadata file to the volume and then run this job."
            )

        except Exception as e:
            st.error(f"❌ Error creating sync job: {str(e)}")
            logger.error(f"Error creating sync job: {str(e)}")

    def create_and_run_sync_job(
        self, filename: str, mode: str = "comment"
    ) -> Tuple[int, int]:
        """Create and run a DDL sync job"""
        logger.info(f"Creating sync job for file: {filename}, mode: {mode}")

        # Use consistent job name
        job_name = "dbxmetagen_app_sync_job"

        # Prepare job parameters for sync notebook
        job_parameters = {
            "reviewed_file_name": filename,
            "mode": mode,
            "env": "app",
            "table_names": "from_metadata_file",  # Sync job reads tables from metadata file
        }

        # Check if job already exists, if so reuse it
        existing_job_id = self._find_job_by_name(job_name)
        if existing_job_id:
            logger.info(
                f"Job '{job_name}' already exists with ID {existing_job_id}, triggering new run"
            )
            job_id = existing_job_id
        else:
            # Create new job - need sync notebook path
            notebook_path = self._resolve_sync_notebook_path()
            job_id = self._create_job(job_name, notebook_path, job_parameters)

        # Start job run
        run_id = self._start_job_run(job_id, job_parameters)

        # Add to session state for tracking
        if "job_runs" not in st.session_state:
            st.session_state.job_runs = {}

        st.session_state.job_runs[run_id] = {
            "job_id": job_id,
            "job_name": job_name,
            "run_id": run_id,
            "filename": filename,
            "mode": mode,
            "status": "RUNNING",
            "created_at": datetime.now().isoformat(),
            "start_time": datetime.now(),
        }

        logger.info(
            f"Sync job created successfully - job_id: {job_id}, run_id: {run_id}"
        )
        return job_id, run_id

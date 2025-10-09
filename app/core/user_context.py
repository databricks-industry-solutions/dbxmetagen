"""
User Context Manager for DBX MetaGen

Manages the three distinct user types without hardcoded fallbacks:
1. deploying_user: Who deploys the asset bundle
2. current_user: Who is logged into the app
3. job_user: Who runs jobs and writes files (app service principal or OBO user)
"""

import os
import logging
from typing import Optional, Dict, Any
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.jobs import (
    JobAccessControlRequest,
    JobPermissionLevel,
)
from deprecated import deprecated

logger = logging.getLogger(__name__)


class UserContextManager:
    """Manages user context for different operations without hardcoded fallbacks"""

    @staticmethod
    def get_deploying_user() -> str:
        """
        Get the user who deployed the asset bundle.
        This determines bundle path location.

        Returns:
            str: Deploying user's email/username

        Raises:
            ValueError: If deploying_user cannot be determined
        """
        # Get deploying user from session state (loaded from deploying_user.yml)
        deploying_user = st.session_state.get("deploying_user")

        if not deploying_user:
            raise ValueError(
                "Deploying user not configured. This should be loaded from deploying_user.yml during deployment. "
                "Please ensure the app was deployed correctly."
            )

        return deploying_user.strip()

    @staticmethod
    def get_current_user() -> str:
        """
        Get the user currently logged into the app.

        Returns:
            str: Current user's email/username

        Raises:
            ValueError: If current user cannot be determined
        """
        # Try session state first
        current_user = st.session_state.get("app_user")

        if not current_user:
            # Try to get from workspace client
            workspace_client = st.session_state.get("workspace_client")
            if workspace_client:
                try:
                    user_info = workspace_client.current_user.me()
                    current_user = user_info.user_name
                except Exception as e:
                    logger.error(
                        f"Failed to get current user from workspace client: {e}"
                    )

        if not current_user:
            raise ValueError(
                "Current user not available. Ensure user is properly authenticated."
            )

        return current_user.strip()

    @staticmethod
    def get_job_user(use_obo: bool = False) -> str:
        """
        Get the user who should run jobs and write files.

        Args:
            use_obo: If True, use On-Behalf-Of (current user).
                    If False, use app service principal.

        Returns:
            str: Job execution user's email/username

        Raises:
            ValueError: If job user cannot be determined
        """
        if use_obo:
            # Use current user for OBO operations
            return UserContextManager.get_current_user()
        else:
            # Use app service principal
            app_user = os.getenv("APP_SERVICE_PRINCIPAL")
            if not app_user:
                app_user = st.session_state.get("app_service_principal")

            # Fallback: try to get from deploying user if service principal not configured
            if not app_user:
                try:
                    app_user = UserContextManager.get_deploying_user()
                    logger.info(
                        f"Using deploying user as service principal fallback: {app_user}"
                    )
                except ValueError:
                    pass

            if not app_user:
                raise ValueError(
                    "App service principal not configured. Set APP_SERVICE_PRINCIPAL "
                    "environment variable for non-OBO operations, or enable OBO mode."
                )

            return app_user.strip()

    @staticmethod
    def get_bundle_base_path(use_shared: bool = False) -> str:
        """
        Get the base path for bundle resources.

        Args:
            use_shared: If True, use /Shared/ location.
                       If False, use deploying user's home directory.

        Returns:
            str: Base path for bundle resources
        """
        if use_shared:
            return "/Shared"
        else:
            deploying_user = UserContextManager.get_deploying_user()
            return f"/Users/{deploying_user}"

    @staticmethod
    def get_notebook_path(
        notebook_name: str,
        bundle_name: str,
        bundle_target: str = "dev",
        use_shared: bool = False,
    ) -> str:
        """
        Generate notebook path for job execution.

        Args:
            notebook_name: Name of notebook file (without .py extension)
            bundle_name: Name of the bundle/app
            bundle_target: Bundle target environment (dev/prod)
            use_shared: Whether to use /Shared/ or user directory

        Returns:
            str: Full notebook path
        """
        base_path = UserContextManager.get_bundle_base_path(use_shared)
        return f"{base_path}/.bundle/{bundle_name}/{bundle_target}/files/notebooks/{notebook_name}"


class AppConfig:
    """Application configuration helper"""

    @staticmethod
    def get_app_name() -> str:
        """Get application name from environment or config"""
        app_name = os.getenv("APP_NAME")
        if not app_name:
            # Try to get from bundle config or default
            app_name = st.session_state.get("app_name", "dbxmetagen")
        return app_name

    @staticmethod
    def get_catalog_name() -> str:
        """Get catalog name from config"""
        catalog_name = st.session_state.get("config", {}).get("catalog_name")
        if not catalog_name:
            raise ValueError(
                "Catalog name not configured. Set catalog_name in application config."
            )
        return catalog_name

    @staticmethod
    def get_bundle_target() -> str:
        """Get bundle target environment"""
        return os.getenv("BUNDLE_TARGET", "dev")

    @deprecated
    @staticmethod
    def set_app_permissions_for_job(job_id: str, user_email: str):
        """Set app service principal permissions on a job"""

        try:
            w = WorkspaceClient()
            app_name = os.getenv("DATABRICKS_APP_NAME")
            if not app_name:
                logger.warning("DATABRICKS_APP_NAME not set, using fallback")
                app_name = AppConfig.get_app_name()

            app = w.apps.get(name=app_name)
            app_service_principal_id = app.service_principal_client_id

            acl = [
                JobAccessControlRequest(
                    user_name=user_email, permission_level=JobPermissionLevel.IS_OWNER
                ),
                JobAccessControlRequest(
                    user_name=app_service_principal_id,
                    permission_level=JobPermissionLevel.CAN_MANAGE_RUN,
                ),
            ]

            # Set permissions on the job, replacing any previous ACL
            w.jobs.set_permissions(job_id=job_id, access_control_list=acl)
            logger.info(
                f"Set permissions on job {job_id} for user {user_email} and app SP {app_service_principal_id}"
            )

        except Exception as e:
            logger.error(f"Failed to set job permissions: {e}")
            raise

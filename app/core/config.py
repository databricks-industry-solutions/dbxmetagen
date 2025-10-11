"""
Core configuration and state management module.
Handles YAML config loading, session state, and Databricks client setup.
"""

import time
import streamlit as st
import yaml
import os
import logging


def st_debug(message: str):
    """
    Display debug messages only when debug mode is enabled.
    Uses logger instead of UI to keep interface clean.
    """
    logger = logging.getLogger(__name__)
    logger.debug(message)
    # Only show in UI if debug mode is explicitly enabled
    if st.session_state.get("config", {}).get("debug_mode", False):
        st.caption(f"🔍 {message}")


from typing import Dict, Any, Optional, List
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages application configuration and state."""

    def __init__(self):
        self.initialize_session_state()
        # Load config immediately to ensure deploying_user is set before authentication
        if not st.session_state.config:
            logger.info(
                "🔍 [ConfigManager.__init__] Loading config immediately for initial auth"
            )
            st.session_state.config = self.load_default_config()

    def initialize_session_state(self):
        """Initialize all session state variables in one place"""
        defaults = {
            "config": {},
            "job_runs": {},
            "workspace_client": None,
            "auto_refresh": False,
            "refresh_interval": 30,
            "selected_tables": [],
            "current_metadata": None,
            "job_creation_status": "idle",
            "performance_metrics": {},
        }

        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

        # Log current deploying_user state for debugging
        logger.info(
            "[initialize_session_state] deploying_user in session_state: %s",
            st.session_state.get("deploying_user", "NOT_SET"),
        )

    def load_default_config(self) -> Dict[str, Any]:
        """Load configuration from cached or variables.yml file."""
        # Check for cached config
        cached_config = self._get_cached_config()
        if cached_config is not None:
            logger.info(
                "[load_default_config] Using cached configuration, deploying_user: %s",
                st.session_state.get("deploying_user", "NOT_SET"),
            )
            return cached_config

        # Load variables.yml and set it up as config
        logger.info("[load_default_config] Loading fresh config from YAML files")
        config = self._load_variables_yml()
        return self._cache_and_return_config(config)

    def _load_yaml_file(
        self, path: str, required: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Load a YAML file and return its contents."""
        if not os.path.exists(path):
            if required:
                raise FileNotFoundError(f"{path} not found")
            return None

        try:
            with open(path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Failed to load {path}: {e}")
            return None

    def _extract_variables(self, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract default values from Databricks variables.yml structure."""
        if not raw_config or "variables" not in raw_config:
            raise ValueError(
                "Invalid variables.yml structure - missing 'variables' key"
            )

        config = {}
        for key, value_config in raw_config["variables"].items():
            if isinstance(value_config, dict) and "default" in value_config:
                config[key] = value_config["default"]
            else:
                config[key] = value_config
        return config

    def _load_variables_yml(self) -> Dict[str, Any]:
        """Load configuration from variables.yml and merge with advanced and deployment configs."""
        raw_config = self._load_yaml_file("./variables.yml", required=True)
        config = self._extract_variables(raw_config)
        logger.info("Loaded %s base variables", len(config))

        advanced_config = self._load_yaml_file("./variables.advanced.yml")
        if advanced_config:
            advanced_vars = self._extract_variables(advanced_config)
            config.update(advanced_vars)
            logger.info("Merged %s advanced variables", len(advanced_vars))

        deploying_config = self._load_yaml_file("./deploying_user.yml")
        if deploying_config and "deploying_user" in deploying_config:
            config.update(deploying_config)
            st.session_state.deploying_user = deploying_config["deploying_user"]
            # st.info(
            #     f"Deploying user in _load_variables_yml: {deploying_config['deploying_user']}"
            # )
            logger.info(
                "[_load_variables_yml] SET deploying_user to: %s",
                deploying_config["deploying_user"],
            )
            logger.info(
                "[_load_variables_yml] VERIFY session_state.deploying_user = %s",
                st.session_state.deploying_user,
            )
        else:
            st.session_state.deploying_user = "unknown"
            logger.warning(
                "[_load_variables_yml] deploying_user.yml not found or missing deploying_user key - set to 'unknown'"
            )

        return config

    def _load_env_yml(self, filename: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Load and merge environment-specific overrides."""
        env_config = self._load_yaml_file(filename)
        if env_config:
            config.update(env_config)
            st.session_state.app_env = env_config.get("app_env", "unknown")
            logger.info(f"Merged environment config from {filename}")
        return config

    def _get_cached_config(self) -> Optional[Dict[str, Any]]:
        """Check if we have a valid cached configuration and return it."""
        if st.session_state.config and isinstance(st.session_state.config, dict):
            return st.session_state.config
        return None

    def _apply_host_override(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Override host from environment if available."""
        env_host = os.environ.get("DATABRICKS_HOST")
        if env_host:
            config["host"] = env_host
            logger.info("Using DATABRICKS_HOST environment variable")
        return config

    def _cache_and_return_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Cache configuration in session state and return."""
        config = self._apply_host_override(config)
        st.session_state.config = config
        logger.info(f"Configuration loaded with {len(config)} keys")
        return config


class DatabricksClientManager:
    """Manages Databricks workspace client setup and authentication."""

    # Class constants for better maintainability
    REQUIRED_ENV_VAR = "DATABRICKS_HOST"
    TOKEN_ENV_VARS = ["DATABRICKS_TOKEN", "DATABRICKS_ACCESS_TOKEN"]
    FORWARDED_TOKEN_HEADERS = ["x-forwarded-access-token", "x-forwarded-user-token"]
    QUERY_TOKEN_PARAMS = ["token", "access_token"]

    @staticmethod
    def setup_client() -> bool:
        """
        Initialize Databricks workspace client with user impersonation.

        Returns:
            bool: True if setup successful, False otherwise
        """
        # Skip if already initialized
        if DatabricksClientManager._is_client_already_initialized():
            logger.info("Workspace client already initialized")
            return True

        try:
            host = DatabricksClientManager._validate_environment()
            if not host:
                return False

            client = DatabricksClientManager._create_authenticated_client(host)
            if not client:
                return False

            user_info = DatabricksClientManager._test_client_connection(client)
            if not user_info:
                return False

            DatabricksClientManager._store_client_in_session(client, user_info)
            return True

        except Exception as e:
            DatabricksClientManager._handle_setup_error(e)
            return False

    @staticmethod
    def _is_client_already_initialized() -> bool:
        """Check if workspace client is already initialized."""
        return st.session_state.get("workspace_client") is not None

    @staticmethod
    def recheck_authentication() -> bool:
        """
        Recheck authentication before critical operations like job runs.
        This enables future OBO (On-Behalf-Of) authentication support.

        Returns:
            bool: True if authentication is valid, False otherwise
        """
        logger.info("🔄 Rechecking authentication for job operation...")

        # For now, just verify the existing client is still valid
        if not st.session_state.get("workspace_client"):
            logger.warning("❌ No active client found, attempting re-authentication")
            return DatabricksClientManager.setup_client()

        try:
            # Test current client connection
            client = st.session_state.workspace_client
            user_info = client.current_user.me()
            logger.info(
                f"✅ Authentication recheck successful - user: {user_info.user_name}"
            )
            return True

        except Exception as e:
            logger.warning(f"⚠️ Authentication recheck failed: {str(e)}")
            logger.info("🔄 Attempting re-authentication...")

            # Clear the failed client and try again
            if "workspace_client" in st.session_state:
                del st.session_state.workspace_client

            return DatabricksClientManager.setup_client()

    @staticmethod
    def _validate_environment() -> Optional[str]:
        """
        Validate required environment variables.

        Returns:
            str: Databricks host URL if valid, None otherwise
        """
        host = os.environ.get(DatabricksClientManager.REQUIRED_ENV_VAR)

        if not host:
            error_msg = f"❌ {DatabricksClientManager.REQUIRED_ENV_VAR} environment variable not set"
            logger.error(error_msg)
            st.error(error_msg)
            return None

        # Normalize host URL by adding protocol if missing
        normalized_host = DatabricksClientManager._normalize_host_url(host)

        if not DatabricksClientManager._is_valid_host_url(normalized_host):
            error_msg = f"❌ Invalid host URL format: {host}"
            logger.error(error_msg)
            st.error(error_msg)
            return None

        logger.info(f"Using Databricks host: {normalized_host}")
        return normalized_host

    @staticmethod
    def _normalize_host_url(host: str) -> str:
        """
        Normalize host URL by adding https:// protocol if missing.

        Args:
            host: Raw host URL that may or may not include protocol

        Returns:
            str: Normalized host URL with protocol
        """
        host = host.strip()
        if not host.startswith(("https://", "http://")):
            host = f"https://{host}"
        return host

    @staticmethod
    def _is_valid_host_url(host: str) -> bool:
        """Validate host URL format."""
        return (
            host.startswith(("https://", "http://"))
            and len(host.strip()) > 10
            and "." in host
        )

    @staticmethod
    def _create_authenticated_client(host: str) -> Optional[WorkspaceClient]:
        """
        Create Databricks client using On-Behalf-Of (OBO) authentication.

        Args:
            host: Databricks workspace host URL

        Returns:
            WorkspaceClient: Authenticated client or None if failed
        """
        try:
            # Hybrid authentication: Try OBO first, fallback to service principal
            logger.info("🔍 Starting hybrid authentication (OBO + Service Principal)")

            # First, try to identify the actual app user from headers/context
            actual_user = DatabricksClientManager._identify_app_user()
            deploying_user = st.session_state.get("deploying_user", "NOT_SET")
            logger.info(
                "🔍 [get_client] deploying_user from session_state: %s", deploying_user
            )

            # Try OBO token authentication
            user_token = DatabricksClientManager._extract_user_token()

            if user_token:
                logger.info("✅ Found OBO user token, attempting OBO authentication")
                try:
                    client = WorkspaceClient(host=host, token=user_token)
                    test_user = client.current_user.me()
                    logger.info(
                        f"✅ OBO authentication successful - user: {test_user.user_name}"
                    )

                    # Store app-wide user info
                    if hasattr(st, "session_state"):
                        st.session_state.auth_method = "OBO (x-forwarded-access-token)"
                        st.session_state.app_user = test_user.user_name
                        logger.info(
                            f"🔍 App-wide user tracking - User: {test_user.user_name}, Deployer: {st.session_state.get('deploying_user', 'unknown')}"
                        )

                    return client

                except Exception as obo_error:
                    logger.warning(
                        f"⚠️ OBO token found but authentication failed: {str(obo_error)}"
                    )
                    logger.info("🔄 Falling back to service principal authentication")
            else:
                logger.info(
                    "ℹ️ No OBO token found - using service principal authentication"
                )

            # Fallback to service principal authentication (but track actual user)
            logger.info("🔄 Using service principal authentication with user tracking")
            client = WorkspaceClient(host=host)

            # Test the service principal client
            sp_user = client.current_user.me()
            logger.info(
                f"✅ Service principal authentication successful - SP: {sp_user.user_name}"
            )

            # Store app-wide user info (service principal + actual user if identified)
            if hasattr(st, "session_state"):
                st.session_state.auth_method = "Service Principal (hybrid)"
                st.session_state.service_principal = sp_user.user_name
                st.session_state.app_user = actual_user if actual_user else "unknown"
                # deploying_user already set by _load_variables_yml - don't overwrite it

                logger.info(
                    f"🔍 App-wide user tracking - SP: {sp_user.user_name}, User: {actual_user}, Deployer: {st.session_state.get('deploying_user', 'unknown')}"
                )

            return client

        except Exception as e:
            logger.error(f"❌ Failed to create OBO Databricks client: {str(e)}")
            logger.info("🔄 Attempting fallback authentication for debugging...")

            # Try fallback authentication
            try:
                logger.info("🔄 Attempting minimal fallback authentication...")
                client = WorkspaceClient(host=host)
                if hasattr(st, "session_state"):
                    st.session_state.auth_method = "Fallback (minimal config)"
                return client
            except Exception as fallback_error:
                logger.error(
                    f"❌ All authentication methods failed: {str(fallback_error)}"
                )
                if hasattr(st, "session_state"):
                    st.session_state.auth_method = "Failed"
                return None

    @staticmethod
    def _test_client_connection(client: WorkspaceClient) -> Optional[Dict[str, str]]:
        """
        Test client connection and get user information.

        Args:
            client: Databricks workspace client

        Returns:
            dict: User information if successful, None if failed
        """
        try:
            logger.info("Testing Databricks client connection...")
            current_user = client.current_user.me()

            user_info = {
                "user_name": current_user.user_name,
                "display_name": getattr(
                    current_user, "display_name", current_user.user_name
                ),
                "user_id": getattr(current_user, "id", "unknown"),
            }

            logger.info(f"Successfully connected as: {user_info['user_name']}")
            return user_info

        except Exception as e:
            error_msg = f"Failed to authenticate with Databricks: {str(e)}"
            logger.error(error_msg)
            st.error(f"❌ {error_msg}")
            return None

    @staticmethod
    def _store_client_in_session(client: WorkspaceClient, user_info: Dict[str, str]):
        """
        Store successful client and user info in session state.

        Args:
            client: Authenticated Databricks client
            user_info: User information dictionary
        """
        st.session_state.workspace_client = client
        st.session_state.databricks_user_info = user_info

        logger.info(f"Stored client in session for user: {user_info['user_name']}")

        # Optional: Show success message to user
        st.success(f"✅ Connected to Databricks as {user_info['display_name']}")

    @staticmethod
    def _handle_setup_error(error: Exception):
        """Handle setup errors with proper logging and user feedback."""
        error_msg = f"Failed to setup Databricks client: {str(error)}"
        logger.error(error_msg, exc_info=True)
        st.error(f"❌ Connection Error: {str(error)}")

    @staticmethod
    def _extract_user_token() -> Optional[str]:
        """
        Extract user token from various sources with detailed debugging.

        Returns:
            str: User token if found, None otherwise
        """
        logger.info("🔍 Starting user token extraction...")

        token_sources = [
            DatabricksClientManager._try_streamlit_context_token,
            DatabricksClientManager._try_query_params_token,
            DatabricksClientManager._try_environment_token,
        ]

        for source_func in token_sources:
            logger.info(f"🔍 Trying token source: {source_func.__name__}")
            try:
                token = source_func()
                if token:
                    logger.info(f"✅ Token found via {source_func.__name__}")
                    logger.info(f"🔍 Token length: {len(token)} characters")
                    logger.info(
                        f"🔍 Token prefix: {token[:10]}..."
                        if len(token) > 10
                        else f"🔍 Token: {token}"
                    )
                    return token
                else:
                    logger.info(f"❌ No token from {source_func.__name__}")
            except Exception as e:
                logger.error(
                    f"❌ Token extraction failed for {source_func.__name__}: {str(e)}"
                )
                continue

        logger.error("❌ No user token found from any source")
        return None

    @staticmethod
    def _identify_app_user() -> Optional[str]:
        """
        Identify the actual app user from headers/context, even without OBO token.

        Returns:
            str: User identifier if found, None otherwise
        """
        logger.info("🔍 Attempting to identify actual app user...")

        # Try to get user info from Streamlit request headers
        try:
            from streamlit.runtime.scriptrunner import get_script_run_ctx

            ctx = get_script_run_ctx()
            if not ctx or not hasattr(ctx, "session_info"):
                logger.info("❌ No Streamlit context for user identification")
                return None

            session_info = ctx.session_info
            if not hasattr(session_info, "headers"):
                logger.info("❌ No headers available for user identification")
                return None

            headers = session_info.headers

            # Look for user identification headers (common in Databricks Apps)
            user_headers = [
                "x-forwarded-user",
                "x-original-user",
                "x-databricks-user",
                "x-user-email",
                "remote-user",
            ]

            for header_name in user_headers:
                user_value = headers.get(header_name)
                if user_value:
                    logger.info(
                        f"✅ Found user identifier in header {header_name}: {user_value}"
                    )
                    return user_value

            logger.info("❌ No user identification headers found")
            return None

        except Exception as e:
            logger.error(f"❌ Error identifying app user: {str(e)}")
            return None

    @staticmethod
    def _try_streamlit_context_token() -> Optional[str]:
        """Extract token from Streamlit request context with detailed debugging."""
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        logger.info("🔍 Getting Streamlit script run context...")
        ctx = get_script_run_ctx()

        if not ctx:
            logger.info("❌ No Streamlit script run context available")
            return None

        if not hasattr(ctx, "session_info"):
            logger.info("❌ Script context has no session_info")
            return None

        logger.info("✅ Found script context with session_info")
        session_info = ctx.session_info

        if not hasattr(session_info, "headers"):
            logger.info("❌ Session info has no headers")
            return None

        headers = session_info.headers
        logger.info(f"✅ Found headers dictionary with {len(headers)} headers")

        logger.info("🔍 Available headers:")
        for header_name, header_value in headers.items():
            if "token" in header_name.lower() or "auth" in header_name.lower():
                value_preview = (
                    f"{header_value[:10]}..."
                    if len(header_value) > 10
                    else header_value
                )
                logger.info(f"  - {header_name}: {value_preview}")
            else:
                value_preview = (
                    header_value
                    if len(header_value) < 50
                    else f"{header_value[:47]}..."
                )
                logger.info(f"  - {header_name}: {value_preview}")

        # Try forwarded token headers
        logger.info(
            f"🔍 Looking for forwarded token headers: {DatabricksClientManager.FORWARDED_TOKEN_HEADERS}"
        )
        for header_name in DatabricksClientManager.FORWARDED_TOKEN_HEADERS:
            logger.info(f"🔍 Checking header: {header_name}")
            token = headers.get(header_name)
            if token:
                logger.info(f"✅ Found token in header: {header_name}")
                return token
            else:
                logger.info(f"❌ No token in header: {header_name}")

        # Try authorization header
        logger.info("🔍 Checking authorization header...")
        auth_header = headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            logger.info("✅ Found Bearer token in authorization header")
            return auth_header.replace("Bearer ", "")
        elif auth_header:
            logger.info(
                f"❌ Authorization header exists but doesn't start with 'Bearer ': {auth_header[:10]}..."
            )
        else:
            logger.info("❌ No authorization header found")

        logger.info("❌ No forwarded tokens found in any headers")
        return None

    @staticmethod
    def _try_query_params_token() -> Optional[str]:
        """Extract token from query parameters."""
        try:
            query_params = st.query_params
            for param_name in DatabricksClientManager.QUERY_TOKEN_PARAMS:
                token = query_params.get(param_name)
                if token:
                    return token
        except Exception:
            pass
        return None

    @staticmethod
    def _try_environment_token() -> Optional[str]:
        """Extract token from environment variables."""
        for env_var in DatabricksClientManager.TOKEN_ENV_VARS:
            token = os.environ.get(env_var)
            if token:
                return token
        return None

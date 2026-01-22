"""Unit tests for UserContextManager and AppConfig in app/core/user_context.py"""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch

# Mock streamlit and databricks before importing app modules
sys.modules["streamlit"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.core"] = MagicMock()
sys.modules["databricks.sdk.service"] = MagicMock()
sys.modules["databricks.sdk.service.jobs"] = MagicMock()

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))


class TestUserContextManagerDeployingUser:
    """Test UserContextManager deploying user methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.user_context import UserContextManager

        self.UserContextManager = UserContextManager

        # Mock streamlit session state
        import streamlit as st

        st.session_state = {}

    @patch("core.user_context.st")
    def test_get_deploying_user_from_session_state(self, mock_st):
        """Test getting deploying user from session state"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = "deployer@example.com"
        mock_st.session_state = mock_session

        result = self.UserContextManager.get_deploying_user()

        assert result == "deployer@example.com"

    @patch("core.user_context.st")
    def test_get_deploying_user_strips_whitespace(self, mock_st):
        """Test that deploying user email has whitespace stripped"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = "  deployer@example.com  "
        mock_st.session_state = mock_session

        result = self.UserContextManager.get_deploying_user()

        assert result == "deployer@example.com"

    @patch("core.user_context.st")
    def test_get_deploying_user_missing_raises_error(self, mock_st):
        """Test that missing deploying user raises ValueError"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = None
        mock_st.session_state = mock_session

        with pytest.raises(ValueError, match="Deploying user not configured"):
            self.UserContextManager.get_deploying_user()


class TestUserContextManagerCurrentUser:
    """Test UserContextManager current user methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.user_context import UserContextManager

        self.UserContextManager = UserContextManager

    @patch("core.user_context.st")
    def test_get_current_user_from_session_state(self, mock_st):
        """Test getting current user from session state"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = "current.user@example.com"
        mock_st.session_state = mock_session

        result = self.UserContextManager.get_current_user()

        assert result == "current.user@example.com"

    @patch("core.user_context.st")
    def test_get_current_user_from_workspace_client(self, mock_st):
        """Test getting current user from workspace client"""
        mock_client = Mock()
        mock_user = Mock()
        mock_user.user_name = "workspace.user@example.com"
        mock_client.current_user.me.return_value = mock_user

        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.side_effect = lambda key, default=None: (
            mock_client if key == "workspace_client" else default
        )
        mock_st.session_state = mock_session

        result = self.UserContextManager.get_current_user()

        assert result == "workspace.user@example.com"

    @patch("core.user_context.st")
    def test_get_current_user_missing_raises_error(self, mock_st):
        """Test that missing current user raises ValueError"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = None
        mock_st.session_state = mock_session

        with pytest.raises(ValueError, match="Current user not available"):
            self.UserContextManager.get_current_user()


class TestUserContextManagerJobUser:
    """Test UserContextManager job user methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.user_context import UserContextManager

        self.UserContextManager = UserContextManager

    @patch("core.user_context.st")
    @patch("core.user_context.UserContextManager.get_current_user")
    def test_get_job_user_with_obo(self, mock_get_current_user, mock_st):
        """Test getting job user with OBO (On-Behalf-Of) mode"""
        mock_get_current_user.return_value = "current.user@example.com"

        result = self.UserContextManager.get_job_user(use_obo=True)

        assert result == "current.user@example.com"
        mock_get_current_user.assert_called_once()

    @patch.dict(os.environ, {"APP_SERVICE_PRINCIPAL": "sp-123@service.principal"})
    def test_get_job_user_service_principal_from_env(self):
        """Test getting job user from APP_SERVICE_PRINCIPAL environment variable"""
        result = self.UserContextManager.get_job_user(use_obo=False)

        assert result == "sp-123@service.principal"

    @patch.dict(os.environ, {}, clear=True)
    @patch("core.user_context.st")
    def test_get_job_user_service_principal_from_session_state(self, mock_st):
        """Test getting job user from session state"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = "sp-456@service.principal"
        mock_st.session_state = mock_session

        result = self.UserContextManager.get_job_user(use_obo=False)

        assert result == "sp-456@service.principal"

    @patch.dict(os.environ, {}, clear=True)
    @patch("core.user_context.st")
    @patch("core.user_context.UserContextManager.get_deploying_user")
    def test_get_job_user_fallback_to_deploying_user(
        self, mock_get_deploying_user, mock_st
    ):
        """Test that job user falls back to deploying user when SP not configured"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = None
        mock_st.session_state = mock_session
        mock_get_deploying_user.return_value = "deployer@example.com"

        result = self.UserContextManager.get_job_user(use_obo=False)

        assert result == "deployer@example.com"

    @patch.dict(os.environ, {}, clear=True)
    @patch("core.user_context.st")
    @patch("core.user_context.UserContextManager.get_deploying_user")
    def test_get_job_user_missing_raises_error(self, mock_get_deploying_user, mock_st):
        """Test that missing job user raises ValueError"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = None
        mock_st.session_state = mock_session
        mock_get_deploying_user.side_effect = ValueError("No deploying user")

        with pytest.raises(ValueError, match="App service principal not configured"):
            self.UserContextManager.get_job_user(use_obo=False)


class TestUserContextManagerPaths:
    """Test UserContextManager path generation methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.user_context import UserContextManager

        self.UserContextManager = UserContextManager

    @patch("core.user_context.UserContextManager.get_deploying_user")
    def test_get_bundle_base_path_user_directory(self, mock_get_deploying_user):
        """Test getting bundle base path from user directory"""
        mock_get_deploying_user.return_value = "user@example.com"

        result = self.UserContextManager.get_bundle_base_path(use_shared=False)

        assert result == "/Users/user@example.com"

    def test_get_bundle_base_path_shared(self):
        """Test getting bundle base path from shared directory"""
        result = self.UserContextManager.get_bundle_base_path(use_shared=True)

        assert result == "/Shared"

    @patch("core.user_context.UserContextManager.get_deploying_user")
    def test_get_notebook_path_user_bundle(self, mock_get_deploying_user):
        """Test generating notebook path in user bundle"""
        mock_get_deploying_user.return_value = "user@example.com"

        result = self.UserContextManager.get_notebook_path(
            notebook_name="test_notebook",
            bundle_name="my_app",
            bundle_target="dev",
            use_shared=False,
        )

        assert (
            result
            == "/Users/user@example.com/.bundle/my_app/dev/files/notebooks/test_notebook"
        )

    @patch("core.user_context.UserContextManager.get_deploying_user")
    def test_get_notebook_path_shared_bundle(self, mock_get_deploying_user):
        """Test generating notebook path in shared bundle"""
        result = self.UserContextManager.get_notebook_path(
            notebook_name="test_notebook",
            bundle_name="my_app",
            bundle_target="prod",
            use_shared=True,
        )

        assert result == "/Shared/.bundle/my_app/prod/files/notebooks/test_notebook"


class TestAppConfig:
    """Test AppConfig helper methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.user_context import AppConfig

        self.AppConfig = AppConfig

    @patch.dict(os.environ, {"APP_NAME": "test_app"})
    def test_get_app_name_from_env(self):
        """Test getting app name from environment variable"""
        result = self.AppConfig.get_app_name()

        assert result == "test_app"

    @patch.dict(os.environ, {}, clear=True)
    @patch("core.user_context.st")
    def test_get_app_name_from_session_state(self, mock_st):
        """Test getting app name from session state"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = "session_app"
        mock_st.session_state = mock_session

        result = self.AppConfig.get_app_name()

        assert result == "session_app"

    @patch.dict(os.environ, {}, clear=True)
    @patch("core.user_context.st")
    def test_get_app_name_default(self, mock_st):
        """Test default app name when not configured"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = (
            "dbxmetagen"  # Returns default when key not found
        )
        mock_st.session_state = mock_session

        result = self.AppConfig.get_app_name()

        assert result == "dbxmetagen"

    @patch("core.user_context.st")
    def test_get_catalog_name_from_config(self, mock_st):
        """Test getting catalog name from config"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = {"catalog_name": "my_catalog"}
        mock_st.session_state = mock_session

        result = self.AppConfig.get_catalog_name()

        assert result == "my_catalog"

    @patch("core.user_context.st")
    def test_get_catalog_name_missing_raises_error(self, mock_st):
        """Test that missing catalog name raises ValueError"""
        # Use MagicMock for session_state to support .get()
        mock_session = MagicMock()
        mock_session.get.return_value = {}  # Empty config
        mock_st.session_state = mock_session

        with pytest.raises(ValueError, match="Catalog name not configured"):
            self.AppConfig.get_catalog_name()

    @patch.dict(os.environ, {"BUNDLE_TARGET": "prod"})
    def test_get_bundle_target_from_env(self):
        """Test getting bundle target from environment variable"""
        result = self.AppConfig.get_bundle_target()

        assert result == "prod"

    @patch.dict(os.environ, {}, clear=True)
    def test_get_bundle_target_default(self):
        """Test default bundle target"""
        result = self.AppConfig.get_bundle_target()

        assert result == "dev"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

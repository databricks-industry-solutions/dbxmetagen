"""
DBXMetaGen Streamlit Application
"""

import streamlit as st
import logging
import sys
from core.config import ConfigManager, DatabricksClientManager
from ui.components import UIComponents

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.StreamHandler(sys.stderr),
    ],
)
logger = logging.getLogger(__name__)

logger.info("🚀 DBX MetaGen App starting up...")

st.set_page_config(
    page_title="DBX MetaGen",
    page_icon="🏷️",
    layout="wide",
    initial_sidebar_state="collapsed",
)


class DBXMetaGenApp:
    """Main application class - now much cleaner and focused."""

    def __init__(self):
        """Initialize the application with authentication on session start."""
        self.config_manager = ConfigManager()
        self.ui_components = UIComponents()

        # Initialize authentication on session start for better user experience
        logger.info("🔄 Initializing authentication on session start...")
        self.client_ready = DatabricksClientManager.setup_client()

        if self.client_ready:
            logger.info("✅ DBX MetaGen App initialized with successful authentication")
        else:
            logger.warning("⚠️ DBX MetaGen App initialized but authentication failed")
            st.warning("⚠️ Authentication failed. Some features may be limited.")

    def ensure_client_ready(self) -> bool:
        """Ensure Databricks client is initialized (lazy initialization)."""
        if self.client_ready is None:
            logger.info("🔄 Performing lazy client initialization...")
            self.client_ready = DatabricksClientManager.setup_client()

            if self.client_ready:
                logger.info("✅ Lazy client initialization successful")
            else:
                logger.error("❌ Lazy client initialization failed")

        return self.client_ready

    def run(self):
        """Main app execution - clean and organized."""
        st.title("🏷️ DBXMetaGen")
        st.markdown(
            "### AI-Powered Metadata Generation & PI Identification & Classification for Databricks Tables"
        )

        self.ui_components.render_sidebar_config()

        # Navigation with state management - no tab jumping!
        if "current_section" not in st.session_state:
            st.session_state.current_section = "📋 Tables & Jobs"

        selected_section = st.radio(
            "Navigate:",
            [
                "📋 Tables & Jobs",
                # "📊 Results",
                "✏️ Review Metadata",
                "❓ Help",
            ],
            index=[
                "📋 Tables & Jobs",
                # "📊 Results",
                "✏️ Review Metadata",
                "❓ Help",
            ].index(st.session_state.current_section),
            key="main_navigation",
            horizontal=True,
        )

        # Update session state
        st.session_state.current_section = selected_section

        st.markdown("---")

        # Render the selected section
        if selected_section == "📋 Tables & Jobs":
            self.ui_components.render_unified_table_management()
            st.markdown("---")
            self.ui_components.render_job_status_section()

        elif selected_section == "📊 Results":
            self.ui_components.render_results_viewer()

        elif selected_section == "✏️ Review Metadata":
            self.ui_components.render_metadata_review()

        elif selected_section == "❓ Help":
            self.ui_components.render_help()

    def render_debug_info(self):
        """Render debug information in sidebar."""
        st.sidebar.markdown("---")
        st.sidebar.subheader("🔧 Debug Information")

        if st.sidebar.button("Show Debug Info"):
            st.sidebar.write("**System Status:**")

            # Try lazy initialization for debug info
            client_ready = self.ensure_client_ready()

            if st.session_state.get("workspace_client"):
                try:
                    user_info = st.session_state.workspace_client.current_user.me()
                    st.sidebar.write("- Workspace Client: ✅ Connected")
                    st.sidebar.write(f"  - Client User: {user_info.user_name}")
                    st.sidebar.write(
                        f"  - Auth Type: {st.session_state.get('auth_method', 'Unknown')}"
                    )

                    # Show hybrid user tracking info
                    if st.session_state.get("app_user"):
                        st.sidebar.write(
                            f"  - App User: {st.session_state.get('app_user')}"
                        )
                    if st.session_state.get("service_principal"):
                        st.sidebar.write(
                            f"  - Service Principal: {st.session_state.get('service_principal')}"
                        )
                    if st.session_state.get("deploying_user"):
                        st.sidebar.write(
                            f"  - Deploying User: {st.session_state.get('deploying_user')}"
                        )
                    if st.session_state.get("app_env"):
                        st.sidebar.write(
                            f"  - App Environment: {st.session_state.get('app_env')}"
                        )

                except Exception as e:
                    st.sidebar.write(
                        "- Workspace Client: ⚠️ Connected but can't get user info"
                    )
                    st.sidebar.write(f"  - Error: {str(e)}")
            else:
                st.sidebar.write("- Workspace Client: ❌ Not initialized")
                st.sidebar.write(f"  - Setup Result: {client_ready}")
                if not client_ready:
                    st.sidebar.write(
                        f"  - Auth Method: {st.session_state.get('auth_method', 'Unknown')}"
                    )

            if st.session_state.get("config"):
                st.sidebar.write(f"- Config: ✅ {len(st.session_state.config)} keys")
            else:
                st.sidebar.write("- Config: ❌ Not loaded")

            # Log viewing instructions
            st.sidebar.markdown("---")
            st.sidebar.write("**📋 View Full Logs:**")
            st.sidebar.write("Add `/logz` to your app URL")

            logger.info("🔍 Debug info requested by user")


# Run the app
if __name__ == "__main__":
    try:
        # Initialize and run the app
        app = DBXMetaGenApp()
        app.run()

        # Add debug info to sidebar
        app.render_debug_info()

    except Exception as e:
        logger.error("❌ Application failed to start: %s", str(e))
        st.error(f"❌ Application Error: {str(e)}")

        # Show error details directly (no expander to avoid nesting issues)
        st.code(str(e))
        if st.button("Show Full Exception"):
            st.exception(e)

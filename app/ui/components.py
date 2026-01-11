"""
UI components module.
Contains all Streamlit rendering functions and UI components.
"""

import streamlit as st
import pandas as pd
import yaml
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import base64

from core.config import ConfigManager, DatabricksClientManager
from core.jobs import JobManager
from core.user_context import AppConfig
from core.data_ops import DataOperations, MetadataProcessor


logger = logging.getLogger(__name__)


class UIComponents:
    """Contains all UI rendering methods."""

    def __init__(self):
        self.config_manager = ConfigManager()
        self.data_ops = DataOperations()
        self.metadata_processor = MetadataProcessor()
        self._job_manager = None

    def _ensure_client_ready(self) -> bool:
        """Ensure Databricks client is initialized (lazy initialization)."""
        if not st.session_state.get("workspace_client"):
            logger.info("Performing lazy client initialization for UI action...")
            client_ready = DatabricksClientManager.setup_client()
            if client_ready:
                logger.info("Lazy client initialization successful")
            else:
                logger.error("Lazy client initialization failed")
            return client_ready
        return True

    @property
    def job_manager(self):
        """Get job manager with lazy client initialization"""
        if self._job_manager is None:
            if self._ensure_client_ready() and st.session_state.get("workspace_client"):
                self._job_manager = JobManager(st.session_state.workspace_client)
        return self._job_manager

    def render_sidebar_config(self):
        """Render configuration sidebar."""
        st.sidebar.header("Configuration")

        if not st.session_state.config:
            st.session_state.config = self.config_manager.load_default_config()

        with st.sidebar.form("config_form"):
            st.subheader("Target Settings")

            catalog_name = st.text_input(
                "Output Catalog Name",
                value=st.session_state.config.get(
                    "catalog_name", AppConfig.get_app_name()
                ),
                help="Unity Catalog name for storing results",
            )

            schema_name = st.text_input(
                "Schema Name",
                value=st.session_state.config.get("schema_name", "metadata_results"),
                help="Schema name for metadata tables",
            )

            st.subheader("Processing Options")

            # allow_data = st.checkbox(
            #     "Allow Data In Comments",
            #     value=st.session_state.config.get("allow_data", True),
            #     help="Include actual data samples in LLM processing",
            # )

            sample_size = st.number_input(
                "Sample Size",
                min_value=0,
                max_value=100,
                value=st.session_state.config.get("sample_size", 10),
                help="Number of rows to sample per column (0 = no sampling)",
            )

            columns_per_call = st.number_input(
                "Columns Per Call",
                min_value=1,
                max_value=100,
                value=st.session_state.config.get("columns_per_call", 10),
                help="Number of columns to process per call",
            )

            mode = st.selectbox(
                "Processing Mode",
                options=["comment", "pi", "domain"],
                index=["comment", "pi", "domain"].index(
                    st.session_state.config.get("mode", "comment")
                ),
                help="What to generate: comments, PII classification, or domain.",
            )

            if mode == "domain":
                domain_config_path = st.text_input(
                    "Domain Config Path",
                    value=st.session_state.config.get(
                        "domain_config_path", "configurations/domain_config.yaml"
                    ),
                    help="Path to the domain configuration YAML file",
                )

            st.subheader("Execution Settings")

            cluster_size = "Serverless"

            apply_ddl = st.checkbox(
                "Apply DDL (CAUTION)",
                value=st.session_state.config.get("apply_ddl", False),
                help="WARNING: This will directly modify your tables!",
            )

            if st.form_submit_button("Save Configuration", type="primary"):
                # Update session state config
                config_update = {
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "sample_size": sample_size,
                    "columns_per_call": columns_per_call,
                    "mode": mode,
                    "cluster_size": cluster_size,
                    "apply_ddl": apply_ddl,
                    "review_apply_ddl": apply_ddl,
                }

                # Add domain config path if in domain mode
                if mode == "domain":
                    config_update["domain_config_path"] = domain_config_path

                st.session_state.config.update(config_update)

                st.sidebar.success("Configuration saved!")

    def render_unified_table_management(self):
        """Render the unified table management interface."""
        st.header("Table Management")

        tables = self._render_table_input_section()

        if tables:
            self._render_table_action_buttons(tables)
        else:
            self._show_no_tables_warning()

    def _render_table_input_section(self) -> List[str]:
        """Render table input section and return list of tables."""
        col1, col2 = st.columns([2, 2])

        with col1:
            st.subheader("Enter Table Names")
            table_names_input = st.text_area(
                "Table Names (one per line)",
                value="\n".join(st.session_state.get("selected_tables", [])),
                height=150,
                help="Enter fully qualified table names: catalog.schema.table",
            )

        with col2:
            st.subheader("Or Upload CSV")
            uploaded_file = st.file_uploader(
                "Upload CSV File",
                type=["csv"],
                help="CSV file with 'table_name' column",
            )

            if uploaded_file:
                csv_tables = self.data_ops.process_uploaded_csv(uploaded_file)
                if csv_tables:
                    # Update session state and return CSV tables directly
                    st.session_state.selected_tables = csv_tables
                    st.success(f"Loaded {len(csv_tables)} tables from CSV")
                    return csv_tables

        tables = self._parse_and_store_tables(table_names_input)

        return tables

    def _parse_and_store_tables(self, table_names_input: str) -> List[str]:
        """Parse table names and store in session state."""
        tables = [
            name.strip() for name in table_names_input.split("\n") if name.strip()
        ]
        st.session_state.selected_tables = tables
        return tables

    def _render_table_action_buttons(self, tables: List[str]):
        """Render action buttons for table operations."""
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Validate Tables", type="secondary"):
                self._validate_tables(tables)

        with col2:
            if st.button("Save List", type="secondary"):
                self._save_table_list(tables)

        with col3:
            self._render_job_creation_button(tables)

    def _render_job_creation_button(self, tables: List[str]):
        """Render job creation button with direct execution"""
        if st.button("Create & Run Job", type="primary"):
            if not self.job_manager:
                st.error(
                    "[ERROR] Databricks client not initialized. Please check connection."
                )
                return
            with st.spinner("Creating and running job..."):
                self.job_manager.create_and_run_metadata_job(tables)

    def _show_no_tables_warning(self):
        """Show warning when no tables are provided."""
        st.info("Enter table names above or upload a CSV file to get started")

    def _validate_tables(self, tables: List[str]):
        """Validate table names and accessibility."""
        with st.spinner("Validating tables..."):
            # Format validation
            valid_tables, invalid_tables = self.data_ops.validate_table_names(tables)
            self.data_ops.display_table_validation_results(valid_tables, invalid_tables)

            # Accessibility validation for valid tables
            if valid_tables:
                st.subheader("Checking Table Access...")
                validation_results = self.data_ops.validate_tables(valid_tables)

                accessible = validation_results["accessible"]
                inaccessible = validation_results["inaccessible"]
                errors = validation_results["errors"]

                if accessible:
                    st.success(f"[OK] {len(accessible)} tables accessible")
                    with st.expander("Accessible Tables"):
                        for table in accessible:
                            st.write(f"[OK] {table}")

                if inaccessible:
                    st.error(f"[ERROR] {len(inaccessible)} tables inaccessible")
                    with st.expander("Inaccessible Tables"):
                        for table in inaccessible:
                            st.write(f"[ERROR] {table}")

                if errors:
                    st.error("Validation Errors:")
                    for error in errors:
                        st.write(f"{error}")

    def _save_table_list(self, tables: List[str]):
        """Save table list as downloadable CSV."""
        csv_content = self.data_ops.save_table_list(tables)
        if csv_content:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"table_names_{timestamp}.csv"

            st.download_button(
                label="Download CSV",
                data=csv_content,
                file_name=filename,
                mime="text/csv",
            )

    def render_job_status_section(self):
        """Render job status monitoring section."""
        st.header("Job Status")

        if not st.session_state.get("job_runs"):
            st.info("No jobs to display. Create a job above to track its status.")
            return

        self._render_job_status_header()

        self._display_job_status_working()

    @st.fragment
    def _render_job_status_header(self):
        """Render job status header with refresh button (isolated fragment to prevent scroll issues)"""
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader("Job Status")
        with col2:
            if st.button("Refresh Now", help="Manually refresh job status"):
                if not self.job_manager:
                    st.error(
                        "[ERROR] Databricks client not initialized. Please check connection."
                    )
                    return
                with st.spinner("Refreshing job status..."):
                    self.job_manager.refresh_job_status()
                st.success("[OK] Status refreshed!")

    def _display_job_status_working(self):
        """Display job status using working format (run_id as key)"""
        for run_id, job_info in st.session_state.job_runs.items():
            status = job_info["status"]

            with st.expander(
                f"{job_info['job_name']} (Run ID: {run_id})",
                expanded=status == "RUNNING",
            ):
                # Job details
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Job ID:** {job_info.get('job_id', 'Unknown')}")
                    st.write(f"**Status:** {status}")
                    st.write(f"**Tables:** {len(job_info.get('tables', []))}")

                with col2:
                    st.write(
                        f"**Cluster Size:** {job_info.get('cluster_size', 'Unknown')}"
                    )
                    st.write(f"**Created:** {job_info.get('created_at', 'Unknown')}")
                    if job_info.get("result_state"):
                        st.write(f"**Result:** {job_info['result_state']}")

                # Status indicator based on job state
                if status == "SUCCESS":
                    st.progress(1.0)
                    st.success("Job completed successfully")
                elif status == "RUNNING":
                    st.info("Job is running...")
                elif status == "FAILED":
                    st.error("Job failed")
                elif status == "TERMINATED":
                    result = job_info.get("result_state", "UNKNOWN")
                    if result == "SUCCESS":
                        st.progress(1.0)
                        st.success("Job completed successfully")
                    else:
                        st.error(f"Job terminated: {result}")

                # Table list
                tables = job_info.get("tables", [])
                if tables:
                    st.markdown(f"**Processing {len(tables)} Tables:**")
                    for i, table in enumerate(tables[:10], 1):
                        st.write(f"{i}. {table}")
                    if len(tables) > 10:
                        st.write(f"... and {len(tables) - 10} more tables")

    def render_metadata_review(self):
        """Render metadata review interface with editing capabilities."""
        st.header("Review Metadata")

        # Configuration inputs (same as Results Viewer)
        col1, col2, col3 = st.columns(3)

        with col1:
            catalog = st.text_input(
                "Catalog",
                value=st.session_state.config.get(
                    "catalog_name", AppConfig.get_app_name()
                ),
                key="review_catalog",
            )

        with col2:
            schema = st.text_input(
                "Schema",
                value=st.session_state.config.get("schema_name", "metadata_results"),
                key="review_schema",
            )

        with col3:
            volume = st.text_input(
                "Volume",
                value=st.session_state.config.get("volume_name", "generated_metadata"),
                key="review_volume",
            )

        # File selection section
        st.subheader("Select File to Review")

        # Button to scan for available files
        if st.button("Scan for Available Files", key="scan_files_button"):
            with st.spinner("Scanning for metadata files..."):
                available_files = (
                    self.metadata_processor.get_available_files_from_volume(
                        catalog, schema, volume
                    )
                )
                if available_files:
                    st.session_state.available_review_files = available_files
                    st.session_state.selected_review_file = None  # Reset selection
                    st.success(f"[OK] Found {len(available_files)} metadata files")
                else:
                    st.session_state.available_review_files = []
                    st.warning("No metadata files found in the specified location")

        # File selection dropdown (only show if files are available)
        if st.session_state.get("available_review_files"):
            available_files = st.session_state.available_review_files

            # Create display options for selectbox
            file_options = []
            for file_info in available_files:
                size_mb = (
                    file_info["size"] / (1024 * 1024) if file_info["size"] > 0 else 0
                )
                display_name = (
                    f"{file_info['name']} ({file_info['type']}, {size_mb:.1f}MB)"
                )
                file_options.append(display_name)

            # Add timestamp info to help identify files
            selected_index = st.selectbox(
                "Select file to review:",
                range(len(file_options)),
                format_func=lambda i: file_options[i],
                key="file_selection_dropdown",
                help="Files are sorted with most recent first",
            )

            # Store selected file info
            if selected_index is not None:
                st.session_state.selected_review_file = available_files[selected_index]

        load_button_text = (
            "Load Selected File"
            if st.session_state.get("selected_review_file")
            else "Load Most Recent File"
        )

        if st.button(load_button_text, key="load_metadata_review_btn"):
            with st.spinner("Loading metadata from volume..."):
                # Use selected file if available, otherwise load most recent
                selected_file_path = None
                if st.session_state.get("selected_review_file"):
                    selected_file_path = st.session_state.selected_review_file["path"]

                df = self.metadata_processor.load_metadata_from_volume(
                    catalog, schema, volume, selected_file_path
                )
                if df is not None:
                    st.session_state.review_metadata = df
                    st.session_state.review_metadata_original_path = {
                        "catalog": catalog,
                        "schema": schema,
                        "volume": volume,
                    }

        if st.session_state.get("review_metadata") is not None:
            df = st.session_state.review_metadata

            st.subheader("Edit Metadata")

            has_domain = "domain" in df.columns
            has_pii = (
                "pii_classification" in df.columns or "classification" in df.columns
            )

            if has_domain:
                st.info(
                    "Edit the Domain and Subdomain fields. DDL will be auto-generated when you save or apply changes."
                )
            elif has_pii:
                st.info(
                    "Edit the PII Classification fields. DDL will be auto-generated when you save or apply changes."
                )
            else:
                st.info(
                    "Edit the Description and PII Classification fields. DDL will be auto-generated when you save or apply changes."
                )

            column_config = {
                "table": st.column_config.TextColumn("Table", disabled=True),
                "table_name": st.column_config.TextColumn("Table", disabled=True),
                "column": st.column_config.TextColumn("Column", disabled=True),
                "column_name": st.column_config.TextColumn("Column", disabled=True),
                "ddl": st.column_config.TextColumn(
                    "DDL (Auto-generated)", disabled=True, width="large"
                ),
            }

            # Add editable columns based on metadata type
            if "column_content" in df.columns:
                column_config["column_content"] = st.column_config.TextColumn(
                    "Description"
                )
            if "pii_classification" in df.columns:
                column_config["pii_classification"] = st.column_config.TextColumn(
                    "PII Classification"
                )
            if "classification" in df.columns:
                column_config["classification"] = st.column_config.TextColumn(
                    "Classification"
                )
            if "type" in df.columns:
                column_config["type"] = st.column_config.TextColumn("Type")
            if "domain" in df.columns:
                column_config["domain"] = st.column_config.TextColumn("Domain")
            if "subdomain" in df.columns:
                column_config["subdomain"] = st.column_config.TextColumn("Subdomain")
            if "recommended_domain" in df.columns:
                column_config["recommended_domain"] = st.column_config.TextColumn(
                    "Recommended Domain", disabled=True
                )
            if "recommended_subdomain" in df.columns:
                column_config["recommended_subdomain"] = st.column_config.TextColumn(
                    "Recommended Subdomain", disabled=True
                )

            # Use data_editor for editing capabilities
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                key="metadata_editor",
                column_config=column_config,
            )

            st.subheader("Save & Apply Changes")

            col1, col2 = st.columns(2)

            with col1:
                if st.button("Save Reviewed Metadata"):
                    self._save_reviewed_metadata(edited_df)

            with col2:
                if st.button("Apply to Tables"):
                    self._apply_metadata(edited_df)

    def _save_reviewed_metadata(self, df: pd.DataFrame):
        """Save reviewed metadata back to volume with _reviewed suffix."""
        if not st.session_state.get("review_metadata_original_path"):
            st.error("[ERROR] Original file path not found. Please reload the metadata.")
            return

        try:
            st.info("Generating DDL from edited metadata...")
            updated_df = self.metadata_processor._generate_ddl_from_comments(df)
            st.session_state.review_metadata = (
                updated_df  # Update session state with new DDL
            )
            path_info = st.session_state.review_metadata_original_path

            # Get current user and date for path construction
            if not st.session_state.get("workspace_client"):
                st.error("[ERROR] Workspace client not initialized")
                return

            current_user = st.session_state.workspace_client.current_user.me().user_name
            sanitized_user = (
                current_user.replace("@", "_").replace(".", "_").replace("-", "_")
            )
            current_date = datetime.now().strftime("%Y%m%d")

            # Construct the output path with _reviewed suffix
            volume_path = f"/Volumes/{path_info['catalog']}/{path_info['schema']}/{path_info['volume']}"
            output_dir = f"{volume_path}/{sanitized_user}/reviewed_outputs/"

            # Find the original file name pattern and add _reviewed
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            mode = st.session_state.config.get("mode", "comment")
            output_filename = f"{mode}_metadata_reviewed_{timestamp}.tsv"
            output_path = f"{output_dir}{output_filename}"

            with st.spinner(f"Saving reviewed metadata to {output_path}..."):
                tsv_content = updated_df.to_csv(sep="\t", index=False)

                try:
                    tsv_bytes = tsv_content.encode("utf-8")
                    st.session_state.workspace_client.files.upload(
                        output_path, tsv_bytes, overwrite=True
                    )
                except Exception as e:
                    # Fallback: direct file write
                    st.warning(f"WorkspaceClient upload failed: {str(e)}")
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(tsv_content)

                st.success(f"Reviewed metadata saved to: {output_path}")
                st.info(f"File: {output_filename}")

                # Also provide download option
                st.download_button(
                    label="Download Reviewed Metadata",
                    data=tsv_content.encode("utf-8"),
                    file_name=output_filename,
                    mime="text/tab-separated-values",
                )

        except Exception as e:
            st.error(f"[ERROR] Error saving reviewed metadata: {str(e)}")
            logger.error(f"Error saving reviewed metadata: {str(e)}")

    def _apply_metadata(self, df: pd.DataFrame):
        """Apply metadata changes to tables."""

        if not DatabricksClientManager.recheck_authentication():
            st.error(
                "[ERROR] Authentication check failed. Please refresh the page and try again."
            )
            return

        if not self.job_manager:
            st.error("[ERROR] Databricks client not initialized. Please check connection.")
            return

        with st.spinner("Applying metadata to tables..."):
            results = self.metadata_processor.apply_metadata_to_tables(
                df, self.job_manager
            )

            if results["success"]:
                st.success(
                    f"[OK] Applied metadata to {results['applied']} tables and columns..."
                )
            else:
                st.error(
                    f"[ERROR] Failed to apply metadata: {results.get('error', 'Unknown error')}"
                )

                if results.get("errors"):
                    st.markdown("**Error Details:**")
                    for error in results["errors"]:
                        st.write(f"- {error}")

    def render_help(self):
        """Render help and documentation."""
        st.header("Help & Documentation")

        with st.expander("Getting Started", expanded=True):
            st.markdown(
                """
            1. **Configure Settings**: Use the sidebar to set your Databricks catalog, host, and processing options
            2. **Select Tables**: Either manually enter table names or upload a CSV file with table names
            3. **Create Job**: Configure and create a metadata generation job
            4. **Monitor Progress**: Watch job status and get notified when complete
            5. **View Results**: Browse generated metadata, download files, and review table comments
            """
            )

        with st.expander("Configuration Options"):
            st.markdown(
                """
            - **Catalog Name**: Target catalog for storing metadata results
            - **Allow Data**: Whether to include actual data samples in LLM processing
            - **Sample Size**: Number of data rows to sample per column (0 = no data sampling)
            - **Mode**: Choose between:
              - **comment**: Generate descriptive comments for tables and columns
              - **pi**: Identify and classify personally identifiable information (PII/PHI/PCI)
              - **domain**: Classify tables into business domains (e.g., finance, sales, HR)
            - **Domain Config Path**: (Domain mode only) Path to domain configuration YAML file
            - **Apply DDL**: **WARNING** - This will directly modify your tables
            """
            )

        with st.expander("File Formats"):
            st.markdown(
                """
            - **table_names.csv**: CSV file with 'table_name' column containing fully qualified table names
            - **Generated TSV**: Tab-separated file with metadata results
            - **Generated SQL**: DDL statements to apply metadata to tables
            """
            )

        with st.expander("Troubleshooting"):
            st.markdown(
                """
            - **Authentication Issues**: Ensure Databricks token and host are properly configured
            - **Table Access**: Verify you have read access to the tables you want to process
            - **Job Failures**: Check job logs in Databricks workspace for detailed error information
            - **Large Tables**: Consider reducing sample size for very large tables
            """
            )

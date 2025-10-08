"""
Data operations module.
Handles table validation, CSV processing, metadata operations.
"""

from datetime import datetime
import streamlit as st
import pandas as pd
import re
import logging
from io import StringIO
from typing import List, Tuple, Dict, Any, Optional

logger = logging.getLogger(__name__)


def st_debug(message: str):
    """
    Display debug messages only when debug mode is enabled.
    Uses logger instead of UI to keep interface clean.
    """
    logger.debug(message)
    # Only show in UI if debug mode is explicitly enabled
    if st.session_state.get("config", {}).get("debug_mode", False):
        st.caption(f"🔍 {message}")


class DataOperations:
    """Handles data processing operations for metadata generation."""

    def __init__(self):
        pass

    def validate_table_names(
        self, table_names: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate table names format and accessibility.

        Returns:
            Tuple of (valid_tables, invalid_tables)
        """
        valid_tables = []
        invalid_tables = []

        table_name_pattern = re.compile(
            r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$"
        )

        for table_name in table_names:
            table_name = table_name.strip()
            if not table_name:
                continue

            if table_name_pattern.match(table_name):
                valid_tables.append(table_name)
            else:
                invalid_tables.append(table_name)
                logger.warning(f"Invalid table name format: {table_name}")

        return valid_tables, invalid_tables

    def display_table_validation_results(
        self, valid_tables: List[str], invalid_tables: List[str]
    ):
        """Display validation results to the user."""
        if valid_tables:
            st.success(f"✅ {len(valid_tables)} valid table names")
            with st.expander(f"Valid Tables ({len(valid_tables)})"):
                for table in valid_tables:
                    st.write(f"✅ {table}")

        if invalid_tables:
            st.error(f"❌ {len(invalid_tables)} invalid table names")
            with st.expander(f"Invalid Tables ({len(invalid_tables)})"):
                for table in invalid_tables:
                    st.write(f"❌ {table}")
                st.write("**Expected format:** `catalog.schema.table`")

    def validate_tables(self, tables: List[str]) -> Dict[str, List[str]]:
        """
        Validate that tables exist and are accessible.

        Returns:
            Dictionary with 'accessible', 'inaccessible', and 'errors' keys
        """
        if not st.session_state.get("workspace_client"):
            return {
                "accessible": [],
                "inaccessible": tables,
                "errors": ["Workspace client not initialized"],
            }

        accessible = []
        inaccessible = []
        errors = []

        try:
            for table in tables:
                try:
                    # Try to get table info
                    catalog, schema, table_name = table.split(".")
                    table_info = st.session_state.workspace_client.tables.get(
                        f"{catalog}.{schema}.{table_name}"
                    )

                    if table_info:
                        accessible.append(table)
                        logger.info(f"✅ Table accessible: {table}")
                    else:
                        inaccessible.append(table)
                        logger.warning(f"❌ Table not found: {table}")

                except Exception as e:
                    inaccessible.append(table)
                    error_msg = f"Error checking {table}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)

        except Exception as e:
            error_msg = f"Failed to validate tables: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)

        return {
            "accessible": accessible,
            "inaccessible": inaccessible,
            "errors": errors,
        }

    def process_uploaded_csv(self, uploaded_file) -> List[str]:
        """
        Process uploaded CSV file to extract table names.

        Returns:
            List of table names
        """
        try:
            # Read CSV file
            content = uploaded_file.read()

            # Handle both string and bytes
            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Parse CSV
            df = pd.read_csv(StringIO(content))

            # Look for table name column
            possible_columns = ["table_name", "table", "name", "table_names"]
            table_column = None

            for col in possible_columns:
                if col in df.columns:
                    table_column = col
                    break

            if table_column is None:
                # If no standard column found, use first column
                if len(df.columns) > 0:
                    table_column = df.columns[0]
                    st.warning(f"Using first column '{table_column}' as table names")
                else:
                    st.error("No columns found in CSV file")
                    return []

            # Extract table names and clean them
            table_names = df[table_column].dropna().astype(str).str.strip().tolist()

            # Remove empty strings
            table_names = [name for name in table_names if name and name != "nan"]

            st.success(f"✅ Loaded {len(table_names)} table names from CSV")
            logger.info(
                f"Loaded {len(table_names)} table names from {uploaded_file.name}"
            )

            return table_names

        except Exception as e:
            error_msg = f"Failed to process CSV file: {str(e)}"
            st.error(f"❌ {error_msg}")
            logger.error(error_msg)
            return []

    def save_table_list(self, tables: List[str]) -> Optional[bytes]:
        """
        Save table list as CSV for download.

        Returns:
            CSV content as bytes, or None if error
        """
        try:
            df = pd.DataFrame({"table_name": tables})
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            logger.info(f"Created CSV with {len(tables)} table names")
            return csv_buffer.getvalue().encode("utf-8")

        except Exception as e:
            error_msg = f"Failed to create CSV: {str(e)}"
            st.error(f"❌ {error_msg}")
            logger.error(error_msg)
            return None


class MetadataProcessor:
    """Handles metadata processing operations."""

    def __init__(self):
        pass

    def get_available_files_from_volume(
        self, catalog: str, schema: str, volume: str
    ) -> List[Dict[str, str]]:
        """Get list of available metadata files from Unity Catalog volume."""
        if not st.session_state.get("workspace_client"):
            st.error("❌ Workspace client not initialized")
            return []

        try:
            # Build path
            current_user = st.session_state.workspace_client.current_user.me().user_name
            sanitized_user = (
                current_user.replace("@", "_").replace(".", "_").replace("-", "_")
            )
            current_date = str(datetime.now().strftime("%Y%m%d"))
            full_directory_path = f"/Volumes/{catalog}/{schema}/{volume}/{sanitized_user}/{current_date}/exportable_run_logs/"

            st.info(f"🔍 Looking for files in: {full_directory_path}")

            # List files
            files = list(
                st.session_state.workspace_client.files.list_directory_contents(
                    full_directory_path
                )
            )

            # Find both TSV and Excel files
            metadata_files = []
            for f in files:
                if f.name.endswith((".tsv", ".xlsx")):
                    # Parse creation time if available
                    file_info = {
                        "name": f.name,
                        "path": f"{full_directory_path}{f.name}",
                        "size": getattr(f, "file_size", 0),
                        "type": "Excel" if f.name.endswith(".xlsx") else "TSV",
                    }
                    # Try to extract timestamp from filename for sorting
                    import re

                    timestamp_match = re.search(r"(\d{8}_\d{6})", f.name)
                    if timestamp_match:
                        file_info["timestamp"] = timestamp_match.group(1)
                    else:
                        file_info["timestamp"] = "00000000_000000"

                    metadata_files.append(file_info)

            # Sort by timestamp (most recent first)
            metadata_files.sort(key=lambda x: x["timestamp"], reverse=True)

            return metadata_files

        except Exception as e:
            st.error(f"❌ Error accessing volume directory: {str(e)}")
            return []

    def load_metadata_from_volume(
        self, catalog: str, schema: str, volume: str, selected_file_path: str = None
    ) -> Optional[pd.DataFrame]:
        """Load metadata files from Unity Catalog volume - with file selection support."""
        if not st.session_state.get("workspace_client"):
            st.error("❌ Workspace client not initialized")
            return None

        try:
            # If no specific file path provided, get available files and use most recent
            if not selected_file_path:
                available_files = self.get_available_files_from_volume(
                    catalog, schema, volume
                )
                if not available_files:
                    st.warning("No metadata files found in the directory")
                    return None

                # Use most recent file as default
                file_path = available_files[0]["path"]
                st.info(f"📄 Loading most recent file: {available_files[0]['name']}")
            else:
                file_path = selected_file_path
                st.info(
                    f"📄 Loading selected file: {selected_file_path.split('/')[-1]}"
                )
            raw_content = st.session_state.workspace_client.files.download(file_path)

            # Extract content - try different methods since DownloadResponse varies
            content = None
            if hasattr(raw_content, "read"):
                # If it has a read method, use it directly
                content_bytes = raw_content.read()
                content = (
                    content_bytes.decode("utf-8")
                    if isinstance(content_bytes, bytes)
                    else str(content_bytes)
                )
                st_debug("✅ Used read() method")
            elif hasattr(raw_content, "contents"):
                # If it has contents attribute, extract from there
                actual_content = raw_content.contents
                if hasattr(actual_content, "read"):
                    content_bytes = actual_content.read()
                    content = (
                        content_bytes.decode("utf-8")
                        if isinstance(content_bytes, bytes)
                        else str(content_bytes)
                    )
                    st_debug("✅ Used contents.read() method")
                else:
                    content = str(actual_content)
                    st_debug("✅ Used str(contents)")
            else:
                # Last resort - try context manager or convert to string
                try:
                    with raw_content as stream:
                        content = stream.read().decode("utf-8")
                    st_debug("✅ Used context manager")
                except Exception:
                    content = str(raw_content)
                    st_debug("⚠️ Fallback to string conversion")

            if not content:
                raise Exception("Failed to extract content from DownloadResponse")

            st_debug(f"✅ Successfully read {len(content)} characters")

            # Parse TSV
            df = pd.read_csv(StringIO(content), sep="\t")
            st_debug(
                f"🔍 Loaded DataFrame: {df.shape} shape, columns: {list(df.columns)}"
            )

            if len(df) == 0:
                st.warning("⚠️ DataFrame is empty!")
                return None

            st.success(f"✅ Loaded {len(df)} records from {file_path.split('/')[-1]}")
            return df

        except Exception as e:
            st.error(f"❌ Error loading metadata: {str(e)}")
            logger.error(f"Error in load_metadata_from_volume: {str(e)}")

            try:
                # Try parent directories to help debug
                path_parts = (
                    file_path.rstrip(file_path.split("/")[-1]).rstrip("/").split("/")
                )
                for i in range(len(path_parts) - 1, 2, -1):
                    parent_dir = "/".join(path_parts[: i + 1])
                    try:
                        parent_files = list(
                            st.session_state.workspace_client.files.list_directory_contents(
                                parent_dir
                            )
                        )
                        st_debug(
                            f"✅ Found parent directory: {parent_dir} with {len(parent_files)} items"
                        )
                        break
                    except:
                        continue
            except:
                pass

            return None

    def _load_file_from_volume(
        self, catalog: str, schema: str, volume: str, filename: str
    ) -> Optional[str]:
        """Load a specific file from Unity Catalog volume."""
        try:
            volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
            file_path = f"{volume_path}/{filename}"

            content = st.session_state.workspace_client.files.download(file_path)
            return content.decode("utf-8")

        except Exception as e:
            logger.error(f"Failed to load file {filename}: {str(e)}")
            return None

    def apply_metadata_to_tables(
        self, df: pd.DataFrame, job_manager=None
    ) -> Dict[str, Any]:
        """Generate DDL from metadata (comments or PI) and trigger Databricks job to execute it."""
        if not st.session_state.get("workspace_client"):
            return {"success": False, "error": "Workspace client not initialized"}

        if not job_manager:
            return {"success": False, "error": "Job manager not provided"}

        results = {"success": False, "applied": 0, "failed": 0, "errors": []}

        try:
            debug_mode = st.session_state.config.get("debug_mode", False)
            if debug_mode:
                st_debug(f"🔍 DataFrame columns: {list(df.columns)}")
                st_debug(f"🔍 DataFrame shape: {df.shape}")

            st.info("🔄 Generating DDL from edited metadata...")
            updated_df = self._generate_ddl_from_comments(df)

            # Save the updated DataFrame to Unity Catalog volume first
            saved_filename = self._save_updated_metadata_for_job(updated_df)
            if not saved_filename:
                results["error"] = "Failed to save metadata file for job execution"
                return results

            # Trigger Databricks job to execute DDL using sync_reviewed_ddl.py notebook
            st.info("🚀 Triggering Databricks job to execute DDL statements...")
            job_result = self._trigger_ddl_sync_job(saved_filename, job_manager)

            if job_result and job_result.get("success"):
                results["success"] = True
                results["applied"] = len(
                    [
                        row
                        for _, row in updated_df.iterrows()
                        if row.get("ddl")
                        and pd.notna(row.get("ddl"))
                        and str(row.get("ddl")).strip()
                    ]
                )

                # Update session state with the updated DataFrame
                st.session_state.review_metadata = updated_df

                st.success("🎉 Successfully triggered DDL execution job!")
                st.info(f"📋 Job ID: {job_result.get('job_id')}")
                if job_result.get("run_id"):
                    st.info(f"🔄 Run ID: {job_result.get('run_id')}")
            else:
                error_msg = job_result.get("error", "Unknown error triggering job")
                results["error"] = error_msg
                results["errors"].append(error_msg)
                st.error(f"❌ Failed to trigger DDL execution job: {error_msg}")

        except Exception as e:
            error_msg = f"Failed to apply metadata: {str(e)}"
            results["error"] = error_msg
            results["errors"].append(error_msg)
            logger.error(error_msg)

        return results

    def _generate_ddl_from_comments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate DDL statements from metadata fields (works for both comment and PI metadata)."""
        updated_df = df.copy()

        # Determine if this is PI or comment metadata
        is_pi_metadata = "classification" in df.columns and "type" in df.columns
        is_domain_metadata = "domain" in df.columns

        # Determine column names (handle different possible column name variations)
        table_col = "table_name" if "table_name" in df.columns else "table"
        column_col = "column_name" if "column_name" in df.columns else "column"

        # Handle different metadata types
        if is_pi_metadata:
            # st.info(
            #    "🔍 Detected PI metadata - generating DDL with data_classification and data_subclassification tags"
            # )
            # For PI metadata, use classification and type columns
            metadata_cols = ["classification", "type"]
            desc_cols = []
            pii_cols = []
        elif is_domain_metadata:
            st.info("🔍 Detected domain metadata - generating DDL with domain tags")
            metadata_cols = ["domain"]
            desc_cols = []
        else:
            # st.info(
            #    "🔍 detected comment metadata - generating ddl from description and pii fields"
            # )
            # for comment metadata, use traditional approach
            desc_cols = [
                col
                for col in df.columns
                if col.lower() in ["description", "column_content"]
            ]
            pii_cols = [col for col in df.columns if "pii" in col.lower()]
            metadata_cols = desc_cols + pii_cols

        st.info(
            f"🔍 Generating DDL - table: {table_col}, column: {column_col}, metadata_cols: {metadata_cols}"
        )

        for index, row in updated_df.iterrows():
            table_name = row[table_col]
            column_name = row[column_col]

            # Check if this is a table-level comment (column_name is null/nan)
            is_table_comment = pd.isna(column_name) or str(
                column_name
            ).lower().strip() in ["nan", "null", ""]

            # Build DDL based on metadata type
            if is_pi_metadata:
                # Handle PI metadata - generate tags DDL
                classification = row.get("classification", "")
                type_value = row.get("type", "")

                # Only generate DDL if we have meaningful PI classification
                if (
                    pd.notna(classification)
                    and str(classification).strip()
                    and str(classification).strip().lower() not in ["none", "null", ""]
                ) or (
                    pd.notna(type_value)
                    and str(type_value).strip()
                    and str(type_value).strip().lower() not in ["none", "null", ""]
                ):
                    # Build tags for PI classification using proper tag structure
                    tags = []

                    # Map classification to data_classification tag
                    if pd.notna(classification) and str(classification).strip():
                        classification_val = str(classification).strip()
                        # Map to 'Protected' or 'None' for data_classification
                        # Any PI-related classification should be 'Protected'
                        protected_values = [
                            "protected",
                            "pii",
                            "phi",
                            "pci",
                            "sensitive",
                            "confidential",
                            "personal",
                            "medical",
                            "medical_information",
                        ]
                        if classification_val.lower() in protected_values:
                            data_classification = "Protected"
                        else:
                            data_classification = "None"
                        tags.append(f"'data_classification' = '{data_classification}'")

                    # Map type to data_subclassification tag
                    if pd.notna(type_value) and str(type_value).strip():
                        type_val = str(type_value).strip()
                        # Ensure valid data_subclassification values
                        valid_subclassifications = [
                            "None",
                            "pii",
                            "pci",
                            "phi",
                            "medical_information",
                        ]

                        type_mapping = {
                            "medical_information": "Medical Information",
                            "healthcare": "PHI",
                            "payment": "PCI",
                            "credit_card": "PCI",
                            "personal": "PII",
                            "personally_identifiable": "PII",
                        }

                        # Check direct match first
                        if type_val in valid_subclassifications:
                            tags.append(f"'data_subclassification' = '{type_val}'")
                        # Check case-insensitive match
                        elif type_val.upper() in [
                            s.upper() for s in valid_subclassifications
                        ]:
                            matched_val = next(
                                s
                                for s in valid_subclassifications
                                if s.upper() == type_val.upper()
                            )
                            tags.append(f"'data_subclassification' = '{matched_val}'")
                        # Check mapping
                        elif type_val.lower() in type_mapping:
                            mapped_val = type_mapping[type_val.lower()]
                            tags.append(f"'data_subclassification' = '{mapped_val}'")
                        else:
                            # For non-standard values, still include them
                            tags.append(f"'data_subclassification' = '{type_val}'")

                    if tags:
                        tags_string = ", ".join(tags)
                        if is_table_comment:
                            # Table-level tags DDL
                            ddl_statement = (
                                f"ALTER TABLE {table_name} SET TAGS ({tags_string})"
                            )
                            # st.info(
                            #    f"🔍 Generated TABLE TAGS DDL for {table_name}: {ddl_statement}"
                            # )
                        else:
                            # Column-level tags DDL
                            ddl_statement = f"ALTER TABLE {table_name} ALTER COLUMN `{column_name}` SET TAGS ({tags_string})"
                            # st.info(
                            #    f"🔍 Generated COLUMN TAGS DDL for {table_name}.{column_name}: {ddl_statement}"
                            # )

                        updated_df.at[index, "ddl"] = ddl_statement
                    else:
                        target = (
                            table_name
                            if is_table_comment
                            else f"{table_name}.{column_name}"
                        )
                        # st.info(
                        #    f"⚠️ No valid PI classification for {target} - skipping DDL generation"
                        # )
                else:
                    target = (
                        table_name
                        if is_table_comment
                        else f"{table_name}.{column_name}"
                    )
                    # st.info(
                    #     f"⚠️ No PI classification for {target} - skipping DDL generation"
                    # )
            elif is_domain_metadata:
                # Handle domain metadata - generate domain tags DDL
                domain = row.get("domain", "")
                subdomain = row.get("subdomain", "")

                # Only generate DDL if we have a valid domain
                if (
                    pd.notna(domain)
                    and str(domain).strip()
                    and str(domain).strip().lower() not in ["none", "null", ""]
                ):
                    # Build tags for domain classification
                    tags = []
                    domain_val = str(domain).strip()
                    tags.append(f"'domain' = '{domain_val}'")

                    # Add subdomain if present and valid
                    if (
                        pd.notna(subdomain)
                        and str(subdomain).strip()
                        and str(subdomain).strip().lower() not in ["none", "null", ""]
                    ):
                        subdomain_val = str(subdomain).strip()
                        tags.append(f"'subdomain' = '{subdomain_val}'")

                    # Generate table-level tags DDL (domain is always table-level)
                    tags_string = ", ".join(tags)
                    ddl_statement = f"ALTER TABLE {table_name} SET TAGS ({tags_string})"
                    updated_df.at[index, "ddl"] = ddl_statement
                else:
                    st.info(
                        f"⚠️ No valid domain for {table_name} - skipping DDL generation"
                    )
            else:
                # Handle comment metadata - original logic
                comment_parts = []

                # Apply Description if it exists and is not empty
                for desc_col in desc_cols:
                    if (
                        desc_col in row
                        and pd.notna(row[desc_col])
                        and str(row[desc_col]).strip()
                    ):
                        comment_parts.append(str(row[desc_col]).strip())

                # Add PII classification if available and not empty
                for pii_col in pii_cols:
                    if (
                        pii_col in row
                        and pd.notna(row[pii_col])
                        and str(row[pii_col]).strip()
                    ):
                        pii_value = str(row[pii_col]).strip()
                        comment_parts.append(f"PII: {pii_value}")

                # Create the combined comment and generate DDL
                if comment_parts:
                    combined_comment = " | ".join(comment_parts)
                    escaped_comment = combined_comment.replace(
                        "'", "''"
                    )  # Escape single quotes

                    if is_table_comment:
                        # Table-level comment DDL
                        ddl_statement = (
                            f"ALTER TABLE {table_name} COMMENT '{escaped_comment}'"
                        )
                        # st.info(
                        #     f"🔍 Generated TABLE DDL for {table_name}: {ddl_statement}"
                        # )
                    else:
                        # Column-level comment DDL
                        ddl_statement = f"ALTER TABLE {table_name} ALTER COLUMN `{column_name}` COMMENT '{escaped_comment}'"
                        st.info(
                            f"🔍 Generated COLUMN DDL for {table_name}.{column_name}: {ddl_statement}"
                        )

                    updated_df.at[index, "ddl"] = ddl_statement
                else:
                    target = (
                        table_name
                        if is_table_comment
                        else f"{table_name}.{column_name}"
                    )
                    # st.info(
                    #     f"⚠️ No comment content for {target} - skipping DDL generation"
                    # )

        return updated_df

    def _save_updated_metadata_for_job(self, df: pd.DataFrame) -> str:
        """Save updated metadata to Unity Catalog volume for job execution."""
        try:
            if not st.session_state.get("review_metadata_original_path"):
                st.error(
                    "❌ Original file path not found. Cannot save for job execution."
                )
                return None

            path_info = st.session_state.review_metadata_original_path

            # Get current user and date for path construction
            current_user = st.session_state.workspace_client.current_user.me().user_name
            sanitized_user = (
                current_user.replace("@", "_").replace(".", "_").replace("-", "_")
            )
            current_date = datetime.now().strftime("%Y%m%d")

            # Construct the output path
            volume_path = f"/Volumes/{path_info['catalog']}/{path_info['schema']}/{path_info['volume']}"
            output_dir = (
                f"{volume_path}/{sanitized_user}/{current_date}/exportable_run_logs/"
            )

            # Generate unique filename for job execution
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reviewed_metadata_for_job_{timestamp}.tsv"
            output_path = f"{output_dir}{filename}"

            # Save as TSV format
            st.info(f"💾 Saving metadata for job execution: {output_path}")
            tsv_content = df.to_csv(sep="\t", index=False)

            # Use WorkspaceClient to save the file
            try:
                tsv_bytes = tsv_content.encode("utf-8")
                st.session_state.workspace_client.files.upload(
                    output_path, tsv_bytes, overwrite=True
                )
                st.success(f"✅ Saved metadata file: {filename}")
                return filename
            except Exception as upload_error:
                st.error(f"❌ Failed to save file: {upload_error}")
                return None

        except Exception as e:
            st.error(f"❌ Error saving metadata for job: {str(e)}")
            logger.error(f"Error in _save_updated_metadata_for_job: {str(e)}")
            return None

    # STep 10: this is what gets triggered when the user clicks the "Apply Metadata" button
    def _trigger_ddl_sync_job(self, filename: str, job_manager) -> Dict[str, Any]:
        """Trigger a Databricks job to execute DDL using sync_reviewed_ddl.py notebook."""
        try:
            # Prepare job parameters for the sync_reviewed_ddl.py notebook
            job_params = {
                "reviewed_file_name": filename,
                "mode": "comment",  # Default mode, could be made configurable
            }

            st_debug(f"🔧 Job parameters: {job_params}")

            try:
                job_id, run_id = job_manager.create_and_run_sync_job(
                    filename=filename, mode="comment"
                )

                return {
                    "success": True,
                    "job_id": job_id,
                    "run_id": run_id,
                    "message": "DDL sync job triggered successfully",
                }

            except AttributeError:
                # Fallback: use generic job creation if sync-specific method doesn't exist
                return {
                    "success": False,
                    "error": "DDL sync job creation not implemented in job manager",
                }

        except Exception as e:
            error_msg = f"Failed to trigger DDL sync job: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def review_uploaded_metadata(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Review uploaded metadata file."""
        try:
            content = uploaded_file.read()

            # Handle both string and bytes
            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Try to parse as TSV first, then CSV
            try:
                df = pd.read_csv(StringIO(content), sep="\t")
            except Exception as e:
                logger.error(f"❌ Failed to parse TSV file: {str(e)}")
                df = pd.read_csv(StringIO(content))

            # Validate required columns
            required_columns = ["table_name", "column_name"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                st.error(f"❌ Missing required columns: {missing_columns}")
                return None

            st.success(f"✅ Loaded metadata file with {len(df)} rows")
            logger.info(f"Loaded metadata file: {uploaded_file.name}")

            return df

        except Exception as e:
            error_msg = f"Failed to process metadata file: {str(e)}"
            st.error(f"❌ {error_msg}")
            logger.error(error_msg)
            return None

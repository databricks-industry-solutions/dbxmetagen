"""Configuration class for dbxmetagen."""

import uuid
from datetime import datetime
import yaml
from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user


def _parse_bool(value):
    """Convert string/bool to actual boolean with proper validation.

    Args:
        value: Value to convert (str, bool, int, or None)

    Returns:
        bool: Parsed boolean value

    Raises:
        TypeError: If value is not str, bool, int, or None
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        # Strip whitespace to handle " true ", "false  ", etc.
        return value.strip().lower() in ("true", "1", "yes")
    if value is None:
        return False
    if isinstance(value, int):
        return bool(value)

    # Raise informative error for unexpected types
    raise TypeError(
        f"Cannot convert {type(value).__name__} to bool. "
        f"Expected str, bool, int, or None. Got: {value!r}"
    )


class MetadataConfig:
    """Configuration class for dbxmetagen."""

    ACRO_CONTENT = {}
    SETUP_PARAMS = {
        "yaml_file_path": "../variables.yml",
        "yaml_variable_names": [
            "env",
            "host",
            "catalog_name",
            "schema_name",
            "catalog_tokenizable",
            "disable_medical_information_value",
            "format_catalog",
            "model",
            "mode",
            "registered_model_name",
            "model_type",
            "volume_name",
            "table_names_source",
            "source_file_path",
            "control_table",
            "apply_ddl",
            "ddl_output_format",
            "allow_data",
            "dry_run",
            "pi_classification_rules",
            "allow_manual_override",
            "override_csv_path",
            "tag_none_fields",
            "pi_column_field_names",
            "max_prompt_length",
            "columns_per_call",
            "sample_size",
            "max_tokens",
            "word_limit_per_cell",
            "limit_prompt_based_on_cell_len",
            "temperature",
            "add_metadata",
            "acro_content",
            "allow_data_in_comments",
            "include_datatype_from_metadata",
            "include_possible_data_fields_in_metadata",
            "review_input_file_type",
            "review_output_file_type",
            "review_apply_ddl",
            "include_deterministic_pi",
            "reviewable_output_format",
            "spacy_model_names",
            "include_existing_table_comment",
            "column_with_reviewed_ddl",
            "current_user",
            "use_protected_classification_for_table",
            "domain_config_path",
            "grant_permissions_after_creation",
            "permission_groups",
            "permission_users",
            "run_id",
            "solo_medical_identifier",
            "include_previously_failed_tables",
            "claim_timeout_minutes",
            "cleanup_failed_tables",
            "node_type",
        ],
        "yaml_advanced_file_path": "../variables.advanced.yml",
        "yaml_advanced_variable_names": [
            "enable_benchmarking",
            "benchmark_table_name",
            "pi_classification_tag_name",
            "pi_subclassification_tag_name",
            "domain_tag_name",
            "subdomain_tag_name",
            "chat_completion_type",
            "custom_endpoint_url",
            "custom_endpoint_secret_scope",
            "custom_endpoint_secret_key",
            "build_knowledge_graph",
        ],
    }
    MODEL_PARAMS = {}

    def __init__(self, **kwargs):
        self.setup_params = self.__class__.SETUP_PARAMS
        self.model_params = self.__class__.MODEL_PARAMS
        self.log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Runtime-determined attributes with defaults
        self.job_id = None  # From job context
        self.run_id = None  # From job run context ({{run.id}}), fallback to UUID
        self.task_id = None  # From task context, for table claiming
        self.base_url = None  # From workspace config or 'host' YAML param
        self.notebook_path = None  # From dbutils
        self.env = None  # From widgets or bundle

        # Check if YAML loading should be skipped (for unit tests)
        skip_yaml = kwargs.pop("skip_yaml_loading", False)

        for key, value in self.setup_params.items():
            setattr(self, key, value)

        for key, value in self.model_params.items():
            setattr(self, key, value)

        # Extract yaml_file_path from kwargs BEFORE loading (if provided)
        # This allows integration tests to override the YAML file path
        if "yaml_file_path" in kwargs:
            self.yaml_file_path = kwargs["yaml_file_path"]

        # Load YAML first as defaults (unless skipped for tests)
        if not skip_yaml:
            yaml_variables = self.load_yaml()
            for key, value in yaml_variables.items():
                setattr(self, key, value)

            # Load advanced YAML variables
            yaml_advanced_variables = self.load_yaml(
                file_path=self.yaml_advanced_file_path,
                variable_names=self.yaml_advanced_variable_names,
            )
            for key, value in yaml_advanced_variables.items():
                setattr(self, key, value)

        # Then override with any passed parameters
        for key, value in kwargs.items():
            setattr(self, key, value)

        # self.instantiate_environments()
        # Parse boolean fields properly (string "false" should be False, not True)
        self.allow_data = _parse_bool(getattr(self, "allow_data", True))
        self.apply_ddl = _parse_bool(getattr(self, "apply_ddl", False))
        self.dry_run = _parse_bool(getattr(self, "dry_run", False))
        self.cleanup_control_table = _parse_bool(
            getattr(self, "cleanup_control_table", False)
        )
        self.grant_permissions_after_creation = _parse_bool(
            getattr(self, "grant_permissions_after_creation", True)
        )

        # Handle review_apply_ddl if present
        if hasattr(self, "review_apply_ddl"):
            self.review_apply_ddl = _parse_bool(self.review_apply_ddl)

        # Map 'host' YAML parameter to 'base_url' attribute for backward compatibility
        if hasattr(self, "host") and self.host and not self.base_url:
            self.base_url = self.host

        if not self.allow_data:
            self.allow_data_in_comments = False
            self.sample_size = 0
            self.filter_data_from_metadata = True
            self.include_possible_data_fields_in_metadata = False

        self.columns_per_call = int(getattr(self, "columns_per_call", 5))
        self.sample_size = int(getattr(self, "sample_size", 5))
        self.allow_data_in_comments = _parse_bool(
            getattr(self, "allow_data_in_comments", True)
        )
        self.include_possible_data_fields_in_metadata = _parse_bool(
            getattr(self, "include_possible_data_fields_in_metadata", True)
        )

        # Parse retry/concurrent task options
        self.include_previously_failed_tables = _parse_bool(
            getattr(self, "include_previously_failed_tables", False)
        )
        self.cleanup_failed_tables = _parse_bool(
            getattr(self, "cleanup_failed_tables", False)
        )
        self.claim_timeout_minutes = int(
            getattr(self, "claim_timeout_minutes", 60)
        )

        # Fallback for run_id if not provided via kwargs/YAML
        if not self.run_id:
            self.run_id = str(uuid.uuid4())

    def get_temp_metadata_log_table_name(self) -> str:
        """
        Generate unique temp metadata generation log table name for this job run.
        Ensures concurrent jobs don't interfere with each other's temp tables.

        Returns:
            str: Full table name in format: catalog.schema.temp_metadata_generation_log_user_timestamp
        """

        current_user = sanitize_user_identifier(get_current_user())
        table_name = f"{self.catalog_name}.{self.schema_name}.temp_metadata_generation_log_{current_user}_{self.log_timestamp}"
        return table_name

    def load_yaml(self, file_path=None, variable_names=None):
        """Load YAML file."""
        import os

        file_path = file_path or self.yaml_file_path
        variable_names = variable_names or self.yaml_variable_names

        try:
            with open(file_path, "r") as file:
                variables = yaml.safe_load(file)
            if not variables or "variables" not in variables:
                return {}
            selected_variables = {
                key: variables["variables"][key]["default"]
                for key in variable_names
                if key in variables["variables"]
            }
            return selected_variables
        except FileNotFoundError:
            # variables.advanced.yml is optional, but variables.yml is required
            if "advanced" in str(file_path):
                print(f"Note: Optional config file not found: {file_path}")
                return {}
            else:
                raise FileNotFoundError(
                    f"❌ Required configuration file not found: {file_path}\n"
                    f"Current working directory: {os.getcwd()}\n"
                    f"Expected path: {os.path.abspath(file_path) if os.path.exists('.') else 'N/A'}\n\n"
                    f"This usually means:\n"
                    f"  - Running from wrong directory (should run from notebooks/ or have correct relative path)\n"
                    f"  - For integration tests: Pass all required config parameters explicitly\n"
                    f"  - For job runs: Ensure yaml_file_path points to deployed location"
                )

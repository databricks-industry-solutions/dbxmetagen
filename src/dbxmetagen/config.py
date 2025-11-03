"""Configuration class for dbxmetagen."""

import uuid
from datetime import datetime
import yaml
from dbxmetagen.user_utils import sanitize_user_identifier, get_current_user


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
        self.run_id = uuid.uuid4()

        for key, value in self.setup_params.items():
            setattr(self, key, value)

        for key, value in self.model_params.items():
            setattr(self, key, value)

        # Load YAML first as defaults
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
        self.allow_data = bool(self.allow_data)

        if not self.allow_data:
            self.allow_data_in_comments = False
            self.sample_size = 0
            self.filter_data_from_metadata = True
            self.include_possible_data_fields_in_metadata = False

        self.columns_per_call = int(self.columns_per_call)
        self.sample_size = int(self.sample_size)
        self.allow_data_in_comments = bool(self.allow_data_in_comments)
        self.include_possible_data_fields_in_metadata = bool(
            self.include_possible_data_fields_in_metadata
        )

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
            return {}

"""Shared test fixtures for dbxmetagen unit tests."""

import sys
import os
import types
from unittest.mock import MagicMock, patch
import pytest

# Ensure src/ is on the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Stub heavy Databricks/Spark deps before any dbxmetagen imports trigger them
for _mod_name in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.window", "pyspark.sql.column", "pyspark.sql.utils",
    "databricks_langchain",
]:
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()

# Stub external deps that processing.py and other heavy modules import.
# These are never installed in the test env, so they must be globally stubbed
# (same pattern as pyspark above). Internal dbxmetagen.* submodules are NOT
# stubbed here to avoid clobbering real implementations.
for _mod_name in [
    "mlflow", "mlflow.types", "mlflow.types.llm", "mlflow.tracing",
    "mlflow.tracing.fluent",
    "nest_asyncio",
    "openai", "openai.types", "openai.types.chat",
    "openai.types.chat.chat_completion",
    "openai.types.chat.chat_completion_message",
    "grpc", "grpc._channel",
]:
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()

# grpc._channel exception types must be real classes (not MagicMock)
# so that `except _InactiveRpcError` works correctly
_gc = sys.modules["grpc._channel"]
_gc._InactiveRpcError = type("_InactiveRpcError", (Exception,), {})
_gc._MultiThreadedRendezvous = type("_MultiThreadedRendezvous", (Exception,), {})


# Internal dbxmetagen submodule stubs -- these must be SCOPED (not global)
# because other tests import the real modules (e.g. dbxmetagen.prompts.Prompt).
_INTERNAL_STUB_NAMES = [
    "dbxmetagen.sampling", "dbxmetagen.prompts", "dbxmetagen.error_handling",
    "dbxmetagen.comment_summarizer", "dbxmetagen.metadata_generator",
    "dbxmetagen.user_utils", "dbxmetagen.domain_classifier",
]

_SENTINEL = object()


def install_processing_stubs():
    """Install temporary sys.modules stubs for internal dbxmetagen submodules
    so dbxmetagen.processing can import. External deps (mlflow, openai, etc.)
    are already globally stubbed above.

    Only creates stub modules for entries NOT already in sys.modules.
    Returns a dict for later restoration via uninstall_processing_stubs.
    """
    saved = {}
    installed = set()
    for mod in _INTERNAL_STUB_NAMES:
        saved[mod] = sys.modules.get(mod, _SENTINEL)
        if mod not in sys.modules:
            sys.modules[mod] = types.ModuleType(mod)
            installed.add(mod)

    # Only populate attributes on stubs we actually created, to avoid
    # corrupting real modules that are already imported.
    if "dbxmetagen.sampling" in installed:
        sys.modules["dbxmetagen.sampling"].determine_sampling_ratio = MagicMock()
    if "dbxmetagen.prompts" in installed:
        pr = sys.modules["dbxmetagen.prompts"]
        pr.Prompt = MagicMock()
        pr.PIPrompt = MagicMock()
        pr.CommentPrompt = MagicMock()
        pr.PromptFactory = MagicMock()
    if "dbxmetagen.error_handling" in installed:
        er = sys.modules["dbxmetagen.error_handling"]
        er.exponential_backoff = MagicMock()
        er.validate_csv = MagicMock()
    if "dbxmetagen.comment_summarizer" in installed:
        sys.modules["dbxmetagen.comment_summarizer"].TableCommentSummarizer = MagicMock()
    if "dbxmetagen.metadata_generator" in installed:
        mg = sys.modules["dbxmetagen.metadata_generator"]
        for attr in ("Response", "PIResponse", "CommentResponse", "PIColumnContent",
                     "MetadataGeneratorFactory", "PIIdentifier", "MetadataGenerator",
                     "CommentGenerator"):
            setattr(mg, attr, MagicMock())
    if "dbxmetagen.user_utils" in installed:
        uu = sys.modules["dbxmetagen.user_utils"]
        uu.sanitize_user_identifier = MagicMock()
        uu.get_current_user = MagicMock()
    if "dbxmetagen.domain_classifier" in installed:
        dc = sys.modules["dbxmetagen.domain_classifier"]
        dc.load_domain_config = MagicMock()
        dc.classify_table_domain = MagicMock()

    return saved


def uninstall_processing_stubs(saved):
    """Restore sys.modules entries overwritten by install_processing_stubs."""
    import dbxmetagen as _pkg
    for mod, original in saved.items():
        attr = mod.rsplit(".", 1)[-1]
        if original is _SENTINEL:
            sys.modules.pop(mod, None)
            if hasattr(_pkg, attr):
                delattr(_pkg, attr)
        else:
            sys.modules[mod] = original
            setattr(_pkg, attr, original)
    sys.modules.pop("dbxmetagen.processing", None)
    if hasattr(_pkg, "processing"):
        delattr(_pkg, "processing")


@pytest.fixture
def mock_spark():
    """Mock SparkSession with common methods stubbed."""
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.createDataFrame.return_value = MagicMock()
    spark.table.return_value = MagicMock()
    spark.read.table.return_value = MagicMock()
    return spark


@pytest.fixture
def mock_spark_session(mock_spark):
    """Patch SparkSession.builder.getOrCreate to return mock_spark."""
    with patch("pyspark.sql.SparkSession") as mock_cls:
        mock_cls.builder.getOrCreate.return_value = mock_spark
        yield mock_spark


@pytest.fixture
def base_config_kwargs():
    """Minimal kwargs to create a MetadataConfig without YAML loading."""
    return {
        "skip_yaml_loading": True,
        "catalog_name": "test_catalog",
        "schema_name": "test_schema",
        "table_names": "test_catalog.test_schema.test_table",
        "mode": "comment",
        "model": "test-model",
        "volume_name": "test_volume",
        "apply_ddl": False,
        "allow_data": True,
        "sample_size": 5,
        "columns_per_call": 5,
        "temperature": 0.1,
        "max_tokens": 4096,
        "max_prompt_length": 100000,
        "ddl_output_format": "tsv",
        "current_user": "test_user@example.com",
        "control_table": True,
        "cleanup_control_table": False,
        "word_limit_per_cell": 50,
        "limit_prompt_based_on_cell_len": True,
        "allow_data_in_comments": True,
        "include_datatype_from_metadata": False,
        "include_possible_data_fields_in_metadata": True,
        "include_existing_table_comment": False,
        "add_metadata": True,
        "reviewable_output_format": "tsv",
        "review_input_file_type": "tsv",
        "review_output_file_type": "tsv",
        "review_apply_ddl": False,
        "grant_permissions_after_creation": False,
        "include_deterministic_pi": False,
        "tag_none_fields": True,
        "federation_mode": False,
    }


@pytest.fixture
def test_config(base_config_kwargs):
    """Create a MetadataConfig with YAML loading skipped."""
    from dbxmetagen.config import MetadataConfig
    return MetadataConfig(**base_config_kwargs)


@pytest.fixture
def sample_table_rows():
    """Sample table row data for testing."""
    return [
        {"col_a": "foo", "col_b": 1, "col_c": "2024-01-01"},
        {"col_a": "bar", "col_b": 2, "col_c": "2024-01-02"},
        {"col_a": "baz", "col_b": 3, "col_c": "2024-01-03"},
    ]

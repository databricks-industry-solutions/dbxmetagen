"""Tests for federation_mode config behavior."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.config import MetadataConfig


def _make_config(**overrides):
    base = {
        "skip_yaml_loading": True,
        "catalog_name": "test",
        "schema_name": "test",
        "table_names": "test.test.t1",
        "mode": "comment",
        "model": "test-model",
        "volume_name": "v",
        "apply_ddl": True,
        "federation_mode": False,
    }
    base.update(overrides)
    return MetadataConfig(**base)


class TestFederationMode:

    def test_federation_mode_forces_apply_ddl_false(self):
        config = _make_config(federation_mode=True, apply_ddl=True)
        assert config.federation_mode is True
        assert config.apply_ddl is False

    def test_federation_mode_false_preserves_apply_ddl(self):
        config = _make_config(federation_mode=False, apply_ddl=True)
        assert config.federation_mode is False
        assert config.apply_ddl is True

    def test_federation_mode_string_true(self):
        config = _make_config(federation_mode="true")
        assert config.federation_mode is True
        assert config.apply_ddl is False

    def test_federation_mode_string_false(self):
        config = _make_config(federation_mode="false")
        assert config.federation_mode is False

    def test_get_extended_metadata_returns_none_in_federation(self):
        from unittest.mock import patch
        config = _make_config(federation_mode=True)
        from dbxmetagen.processing import get_extended_metadata_for_column
        result = get_extended_metadata_for_column(config, "table1", "col1")
        assert result is None

    def test_get_extended_metadata_calls_sql_normally(self):
        from unittest.mock import patch, MagicMock
        config = _make_config(federation_mode=False)
        mock_spark = MagicMock()
        with patch("dbxmetagen.processing.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            from dbxmetagen.processing import get_extended_metadata_for_column
            get_extended_metadata_for_column(config, "table1", "col1")
            mock_spark.sql.assert_called_once()

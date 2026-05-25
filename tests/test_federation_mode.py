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

    # apply_tags flag tests

    def test_apply_tags_defaults_to_apply_ddl(self):
        config = _make_config(apply_ddl=True, federation_mode=False)
        assert config.apply_tags is True

    def test_apply_tags_defaults_false_when_apply_ddl_false(self):
        config = _make_config(apply_ddl=False, federation_mode=False)
        assert config.apply_tags is False

    def test_federation_apply_tags_independent_of_apply_ddl(self):
        config = _make_config(federation_mode=True, apply_ddl=True, apply_tags=True)
        assert config.apply_ddl is False
        assert config.apply_tags is True

    def test_federation_apply_tags_false_by_default(self):
        config = _make_config(federation_mode=True, apply_ddl=False)
        assert config.apply_tags is False

    # SET TAG ON DDL generation tests

    def test_table_pi_ddl_federation_uses_set_tag_on(self):
        from dbxmetagen.processing import _create_table_pi_information_ddl_func
        config = _make_config(federation_mode=True, mode="pi")
        fn = _create_table_pi_information_ddl_func(config)
        ddl = fn("cat.sch.tbl", "pi", "ssn")
        assert ddl.startswith("SET TAG ON TABLE cat.sch.tbl")
        assert "data_classification = 'pi'" in ddl
        assert "data_subclassification = 'ssn'" in ddl
        assert "ALTER TABLE" not in ddl

    def test_table_pi_ddl_non_federation_uses_alter_table(self):
        from dbxmetagen.processing import _create_table_pi_information_ddl_func
        config = _make_config(federation_mode=False, mode="pi")
        fn = _create_table_pi_information_ddl_func(config)
        ddl = fn("cat.sch.tbl", "pi", "ssn")
        assert ddl.startswith("ALTER TABLE cat.sch.tbl SET TAGS")
        assert "SET TAG ON" not in ddl

    def test_column_pi_ddl_federation_uses_set_tag_on_column(self):
        from dbxmetagen.processing import _create_pi_information_ddl_func
        config = _make_config(federation_mode=True, mode="pi")
        fn = _create_pi_information_ddl_func(config)
        ddl = fn("cat.sch.tbl", "my_col", "phi", "mrn")
        assert "SET TAG ON COLUMN cat.sch.tbl.`my_col`" in ddl
        assert "ALTER TABLE" not in ddl

    def test_domain_ddl_federation_uses_set_tag_on(self):
        from dbxmetagen.processing import _create_table_domain_ddl_func
        config = _make_config(federation_mode=True, mode="domain")
        fn = _create_table_domain_ddl_func(config)
        ddl = fn("cat.sch.tbl", "healthcare", "clinical")
        assert ddl.startswith("SET TAG ON TABLE cat.sch.tbl")
        assert "domain = 'healthcare'" in ddl
        assert "subdomain = 'clinical'" in ddl

    def test_domain_ddl_federation_no_subdomain(self):
        from dbxmetagen.processing import _create_table_domain_ddl_func
        config = _make_config(federation_mode=True, mode="domain")
        fn = _create_table_domain_ddl_func(config)
        ddl = fn("cat.sch.tbl", "healthcare", "")
        assert ddl.startswith("SET TAG ON TABLE cat.sch.tbl")
        assert "subdomain" not in ddl

    # DDL regenerator dual-format tests

    def test_extract_pi_tags_from_set_tag_on(self):
        from dbxmetagen.ddl_regenerator import get_pii_tags_from_ddl
        config = _make_config(mode="pi")
        ddl = "SET TAG ON TABLE cat.sch.tbl data_classification = 'pi', data_subclassification = 'ssn';"
        cls, sub = get_pii_tags_from_ddl(ddl, config)
        assert cls == "pi"
        assert sub == "ssn"

    def test_replace_pii_tags_in_set_tag_on(self):
        from dbxmetagen.ddl_regenerator import replace_pii_tags_in_ddl
        config = _make_config(mode="pi")
        ddl = "SET TAG ON TABLE cat.sch.tbl data_classification = 'pi', data_subclassification = 'ssn';"
        result = replace_pii_tags_in_ddl(ddl, "phi", "mrn", config)
        assert "data_classification = 'phi'" in result
        assert "data_subclassification = 'mrn'" in result

"""Tests for the fatal output-access preflight (dbxmetagen.output_access)."""

import types
from unittest.mock import MagicMock, patch

import pytest

from dbxmetagen.output_access import (
    OutputAccessError,
    is_output_write_denied,
    verify_output_access,
)

DENIED_MODIFY = (
    "[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have MODIFY "
    "on Table 'out_cat.out_sch.metadata_generation_log'."
)


def _cfg(**kw):
    base = dict(catalog_name="out_cat", schema_name="out_sch", control_table="",
                volume_name="")
    base.update(kw)
    return types.SimpleNamespace(**base)


def _spark(fail_on=None, exc=None):
    """Spark mock whose .sql raises `exc` for statements containing `fail_on`."""
    spark = MagicMock()
    calls = []

    def _sql(stmt):
        calls.append(stmt)
        if fail_on and fail_on in stmt:
            raise exc
        return MagicMock()

    spark.sql.side_effect = _sql
    spark.calls = calls
    return spark


@pytest.fixture(autouse=True)
def _stub_processing():
    with patch("dbxmetagen.processing.ensure_log_table",
               lambda spark, c, s: spark.sql(f"CREATE TABLE IF NOT EXISTS {c}.{s}.metadata_generation_log"),
               create=True), \
         patch("dbxmetagen.processing.get_control_table", lambda cfg: "ctrl_tbl", create=True):
        yield


class TestVerifyOutputAccess:
    @pytest.mark.parametrize("field", ["catalog_name", "schema_name"])
    @pytest.mark.parametrize("val", ["", None, "none", "NULL"])
    def test_missing_output_location_is_fatal(self, field, val):
        spark = _spark()
        with pytest.raises(OutputAccessError, match=field):
            verify_output_access(spark, _cfg(**{field: val}))
        assert spark.calls == []

    def test_unreachable_schema_is_fatal(self):
        spark = _spark("DESCRIBE SCHEMA", Exception("[SCHEMA_NOT_FOUND] out_sch"))
        with pytest.raises(OutputAccessError, match="USE SCHEMA"):
            verify_output_access(spark, _cfg())

    def test_read_only_output_is_fatal(self):
        spark = _spark("INSERT INTO out_cat.out_sch.metadata_generation_log",
                       Exception(DENIED_MODIFY))
        with pytest.raises(OutputAccessError) as ei:
            verify_output_access(spark, _cfg())
        msg = str(ei.value)
        assert "Permission denied" in msg and "MODIFY" in msg and "before any table" in msg

    def test_cannot_create_log_table_is_fatal(self):
        spark = _spark("CREATE TABLE", Exception("PERMISSION_DENIED: CREATE TABLE on schema"))
        with pytest.raises(OutputAccessError):
            verify_output_access(spark, _cfg())

    def test_control_table_probed_when_configured(self):
        spark = _spark("INSERT INTO out_cat.out_sch.ctrl_tbl", Exception(DENIED_MODIFY))
        with pytest.raises(OutputAccessError, match="control table"):
            verify_output_access(spark, _cfg(control_table="ctrl"))

    def test_probe_is_zero_row_blind_append(self):
        spark = _spark()
        verify_output_access(spark, _cfg(control_table="ctrl"))
        inserts = [c for c in spark.calls if c.startswith("INSERT")]
        assert len(inserts) == 2
        for stmt in inserts:
            assert "WHERE 1 = 0" in stmt
            assert " FROM out_cat" not in stmt  # never reads the target table

    def test_writable_output_passes(self, tmp_path):
        verify_output_access(_spark(), _cfg(volume_name="vol"), str(tmp_path))
        assert list(tmp_path.iterdir()) == []  # probe file cleaned up

    def test_unwritable_volume_is_fatal(self, tmp_path):
        with pytest.raises(OutputAccessError, match="WRITE VOLUME"):
            verify_output_access(_spark(), _cfg(volume_name="vol"),
                                 str(tmp_path / "does_not_exist"))


class TestIsOutputWriteDenied:
    def test_modify_denial_on_output_is_fatal(self):
        assert is_output_write_denied(Exception(DENIED_MODIFY), "out_cat", "out_sch")

    def test_backticked_names_match(self):
        e = Exception("PERMISSION_DENIED: User does not have MODIFY on `out_cat`.`out_sch`.`t`")
        assert is_output_write_denied(e, "out_cat", "out_sch")

    def test_source_read_denial_is_not_fatal(self):
        e = Exception("[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table 'src.raw.t'")
        assert not is_output_write_denied(e, "out_cat", "out_sch")

    def test_select_denial_inside_output_schema_is_not_fatal(self):
        e = Exception("[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table 'out_cat.out_sch.t'")
        assert not is_output_write_denied(e, "out_cat", "out_sch")

    def test_write_denial_on_other_schema_is_not_fatal(self):
        e = Exception("PERMISSION_DENIED: User does not have MODIFY on Table 'other.sch.t'")
        assert not is_output_write_denied(e, "out_cat", "out_sch")

    def test_non_permission_error_is_not_fatal(self):
        assert not is_output_write_denied(Exception("LLM timeout out_cat.out_sch MODIFY"),
                                          "out_cat", "out_sch")

    def test_output_access_error_always_fatal(self):
        assert is_output_write_denied(OutputAccessError("x"), "a", "b")

"""Tests for the fatal output-access preflight (dbxmetagen.output_access)."""

import types
from unittest.mock import MagicMock, patch

import pytest

from dbxmetagen.output_access import (
    OutputAccessError,
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


# ---------------------------------------------------------------------------
# main.initialize_infrastructure: an unusable output location stops the job
# during setup, before any table is queued or sent to the LLM.
# ---------------------------------------------------------------------------

# Verbatim error from a real run whose output catalog had been deleted.
CATALOG_GONE = (
    "[UNAUTHORIZED_ACCESS] Unauthorized access:\nPERMISSION_DENIED: Catalog "
    "'out_cat' is not accessible in current workspace SQLSTATE: 42501"
)


def _main():
    try:
        import dbxmetagen.main as m
    except Exception as e:  # heavy deps unavailable under some harnesses
        pytest.skip(f"dbxmetagen.main not importable: {e}")
    return m


class TestInitializeInfrastructureFatal:
    def _cfg(self):
        return _cfg(control_table="ctrl", volume_name="vol", current_user="u@example.com")

    def _run(self, m, spark, setup_ddl=None):
        queue = MagicMock(return_value=["src.s.t1"])
        with patch.object(m.SparkSession, "builder") as b, \
             patch.object(m, "setup_ddl", setup_ddl or MagicMock(return_value=None)), \
             patch.object(m, "create_tables", MagicMock()), \
             patch.object(m, "seed_customer_context", MagicMock()), \
             patch.object(m, "setup_queue", queue), \
             patch.object(m, "upsert_table_names_to_control_table", MagicMock()):
            b.getOrCreate.return_value = spark
            with pytest.raises(OutputAccessError) as ei:
                m.initialize_infrastructure(self._cfg())
        return ei.value, queue

    def test_deleted_output_catalog_stops_before_queue(self):
        m = _main()
        err, queue = self._run(m, _spark(), setup_ddl=MagicMock(side_effect=Exception(CATALOG_GONE)))
        assert "before any table" in str(err)
        assert "not accessible in current workspace" in str(err)
        queue.assert_not_called()

    def test_read_only_existing_schema_stops_before_queue(self):
        # Schema/volume/control table already exist, so the IF NOT EXISTS setup
        # statements succeed; only an actual write is denied.
        m = _main()
        denied = ("PERMISSION_DENIED: Principal 'job' does not have INSERT on Table "
                  "'out_cat.out_sch.metadata_generation_log'.")
        spark = _spark("INSERT INTO out_cat.out_sch.metadata_generation_log", Exception(denied))
        err, queue = self._run(m, spark)
        assert "Permission denied" in str(err) and "MODIFY" in str(err)
        queue.assert_not_called()

    def test_main_propagates_and_never_generates(self):
        # The fatal error must escape main() (job task FAILS) and generation must
        # not start. cleanup still runs.
        m = _main()
        gen = MagicMock()
        kwargs = {"catalog_name": "out_cat", "table_names": "src.s.t1", "mode": "comment"}
        cfg = self._cfg()
        cfg.apply_ddl, cfg.task_id, cfg.incremental, cfg.mode = False, None, False, "comment"
        cfg.allow_manual_override, cfg.base_url = False, None
        with patch.object(m, "MetadataConfig", return_value=cfg), \
             patch.object(m.SparkSession, "builder"), \
             patch.object(m, "setup_benchmarking", return_value=None), \
             patch.object(m, "setup_ddl", side_effect=Exception(CATALOG_GONE)), \
             patch.object(m, "generate_and_persist_metadata", gen), \
             patch.object(m, "cleanup_resources") as cleanup:
            with pytest.raises(OutputAccessError):
                m.main(kwargs)
        gen.assert_not_called()
        cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# Job DAG: when generation produced nothing, the final KB rebuild is skipped
# instead of running (and retrying) against an unusable output location.
# ---------------------------------------------------------------------------

_KB_REBUILD_JOBS = {
    "metagen_kb_enriched_serverless": ("build_knowledge_base_final", "build_column_kb_final"),
    "metagen_kb_enriched": ("build_knowledge_base_final", "build_column_kb_final"),
    "metagen_parallel_serverless": ("build_knowledge_base", "build_column_kb"),
    "metagen_parallel_kb_build": ("build_knowledge_base", "build_column_kb"),
}


@pytest.mark.parametrize("job,tasks", sorted(_KB_REBUILD_JOBS.items()))
def test_kb_rebuild_skips_when_nothing_was_generated(job, tasks):
    import os
    import yaml

    path = os.path.join(os.path.dirname(__file__), "..", "resources", "jobs", f"{job}.job.yml")
    spec = next(iter(yaml.safe_load(open(path))["resources"]["jobs"].values()))
    by_key = {t["task_key"]: t for t in spec["tasks"]}
    for key in tasks:
        t = by_key[key]
        deps = {d["task_key"] for d in t.get("depends_on", [])}
        # AT_LEAST_ONE_SUCCESS: a single pi/domain failure still rebuilds (the
        # reason it was ALL_DONE), but a failed generate_comments (whose failure
        # cascades to pi/domain as UPSTREAM_FAILED) leaves zero successes -> skip.
        assert t.get("run_if") == "AT_LEAST_ONE_SUCCESS", (job, key)
        assert {"generate_comments", "generate_pi", "generate_domain"} <= deps, (job, key)

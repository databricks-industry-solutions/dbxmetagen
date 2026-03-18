"""
Tests for functionality introduced or modified during the merge resolution.

Covers:
- get_task_id: returns None in non-concurrent (no user/UUID fallback)
- claim_table: combined mode-aware + cleanup + self-reclaim + timeout + status filter
- mark_table_completed / mark_table_failed / mark_as_deleted: mode-aware SQL
- Concurrent retry logic on Delta conflicts
"""

import pytest
import json
from unittest.mock import MagicMock, patch, call

from dbxmetagen.config import MetadataConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**overrides):
    """Create a MetadataConfig with sane defaults for testing."""
    defaults = dict(
        skip_yaml_loading=True,
        catalog_name="cat",
        schema_name="sch",
        mode="comment",
        task_id=None,
        run_id="run_1",
        control_table="ctrl_{}",
        cleanup_control_table=False,
        claim_timeout_minutes=60,
    )
    defaults.update(overrides)
    return MetadataConfig(**defaults)


def _mock_dbutils(context: dict):
    """Build a MagicMock dbutils that returns the given context JSON."""
    m = MagicMock()
    m.notebook.entry_point.getDbutils().notebook().getContext().safeToJson.return_value = json.dumps(context)
    return m


# ===========================================================================
# get_task_id
# ===========================================================================

class TestGetTaskIdMerged:
    """Validate the simplified get_task_id behavior chosen during merge.

    Intended behaviour:
    - Returns taskRunId when present (concurrent job task)
    - Returns parentRunId_interactive when only multitaskParentRunId present
    - Returns None in all other cases (non-concurrent mode signal)
    """

    def setup_method(self):
        from dbxmetagen.databricks_utils import get_task_id
        self.get_task_id = get_task_id

    # --- Happy paths ---

    def test_returns_task_run_id_from_context(self):
        ctx = {"tags": {"taskRunId": "abc123", "jobId": "999"}}
        assert self.get_task_id(_mock_dbutils(ctx)) == "abc123"

    def test_returns_parent_run_id_interactive_suffix(self):
        ctx = {"tags": {"multitaskParentRunId": "parent_42"}}
        assert self.get_task_id(_mock_dbutils(ctx)) == "parent_42_interactive"

    def test_prefers_task_run_id_over_parent(self):
        ctx = {"tags": {"taskRunId": "task_1", "multitaskParentRunId": "parent_2"}}
        assert self.get_task_id(_mock_dbutils(ctx)) == "task_1"

    # --- Non-concurrent / no context ---

    def test_returns_none_when_no_dbutils(self):
        assert self.get_task_id(None) is None

    def test_returns_none_when_tags_empty(self):
        ctx = {"tags": {}, "attributes": {"user": "u@example.com"}}
        assert self.get_task_id(_mock_dbutils(ctx)) is None

    def test_returns_none_when_no_tags_key(self):
        ctx = {"attributes": {"user": "u@example.com"}}
        assert self.get_task_id(_mock_dbutils(ctx)) is None

    def test_returns_none_on_exception(self):
        m = MagicMock()
        m.notebook.entry_point.getDbutils.side_effect = RuntimeError("boom")
        assert self.get_task_id(m) is None

    def test_returns_none_on_json_decode_error(self):
        m = MagicMock()
        m.notebook.entry_point.getDbutils().notebook().getContext().safeToJson.return_value = "not-json"
        assert self.get_task_id(m) is None

    # --- Edge: user in context does NOT cause a return value ---

    def test_user_in_context_not_returned(self):
        """Ensure we DON'T fall back to returning the user -- we return None instead."""
        ctx = {"tags": {}, "attributes": {"user": "someone@databricks.com"}}
        result = self.get_task_id(_mock_dbutils(ctx))
        assert result is None
        assert result != "someone@databricks.com"


# ===========================================================================
# claim_table -- merged logic
# ===========================================================================

class TestClaimTableMergedLogic:
    """
    Tests the merged claim_table that combines:
    - mode-aware claiming (_mode filter)
    - cleanup_control_table override
    - self-reclaim (same task_id can re-claim)
    - timeout (stale claims older than N minutes)
    - status filter (only queued/failed/completed)
    """

    def setup_method(self):
        from dbxmetagen.processing import claim_table
        self.claim_table = claim_table

    # --- Non-concurrent passthrough ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_no_task_id_always_returns_true(self, mock_ct, mock_spark):
        """Without task_id, claim_table should allow processing unconditionally."""
        config = MagicMock()
        config.task_id = None
        result = self.claim_table("cat.sch.tbl", config)
        assert result is True
        mock_spark.builder.getOrCreate().sql.assert_not_called()

    # --- Mode filter ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_where_clause_contains_mode(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="domain", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "_mode = 'domain'" in update_sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_mode_defaults_to_comment(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode=None, catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "_mode = 'comment'" in update_sql

    # --- Cleanup override ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_cleanup_mode_skips_claim_conditions(self, mock_ct, mock_spark):
        """cleanup_control_table=True should produce a simpler WHERE (no _claimed_by IS NULL, no INTERVAL)."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=True, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "_claimed_by IS NULL" not in update_sql
        assert "INTERVAL" not in update_sql
        assert "_mode = 'comment'" in update_sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_cleanup_mode_still_filters_by_mode(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="pi", catalog_name="c", schema_name="s",
                           cleanup_control_table=True, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "_mode = 'pi'" in update_sql

    # --- Self-reclaim ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_self_reclaim_condition_present(self, mock_ct, mock_spark):
        """Normal mode WHERE should include _claimed_by = '<task_id>' for self-reclaim."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "my_task"}]
        config = MagicMock(task_id="my_task", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "_claimed_by = 'my_task'" in update_sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_self_reclaim_returns_true(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "my_task"}]
        config = MagicMock(task_id="my_task", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        assert self.claim_table("c.s.tbl", config) is True

    # --- Timeout ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_timeout_interval_in_where(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=90)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "INTERVAL 90 MINUTES" in update_sql
        assert "_claimed_at <" in update_sql

    # --- Status filter ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_status_filter_in_normal_mode(self, mock_ct, mock_spark):
        """Normal mode should restrict claiming to queued/failed/completed statuses."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "'queued'" in update_sql
        assert "'failed'" in update_sql
        assert "'completed'" in update_sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_status_filter_absent_in_cleanup_mode(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=True, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "'queued'" not in update_sql
        assert "'failed'" not in update_sql

    # --- Verify query uses mode ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_verify_query_includes_mode(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="pi", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        verify_sql = spark.sql.call_args_list[1][0][0]
        assert "_mode = 'pi'" in verify_sql

    # --- Claim success / failure ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_returns_false_when_claimed_by_other(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "other_task"}]
        config = MagicMock(task_id="my_task", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        assert self.claim_table("c.s.tbl", config) is False

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_returns_false_when_no_rows(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = []
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        assert self.claim_table("c.s.tbl", config) is False

    # --- Sets status to in_progress ---

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sets_status_in_progress(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        self.claim_table("c.s.tbl", config)
        update_sql = spark.sql.call_args_list[0][0][0]
        assert "_status = 'in_progress'" in update_sql

    # --- Retry on concurrent modification ---

    @patch("time.sleep")
    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_retries_on_concurrent_append_exception(self, mock_ct, mock_spark, mock_sleep):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value

        ok_result = MagicMock()
        ok_result.collect.return_value = [{"_claimed_by": "t1"}]
        spark.sql.side_effect = [
            Exception("ConcurrentAppendException: conflict"),
            ok_result,  # claim_query succeeds on retry
            ok_result,  # verify_query
        ]
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        result = self.claim_table("c.s.tbl", config, max_retries=3)
        assert result is True
        assert mock_sleep.called

    @patch("time.sleep")
    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_gives_up_after_max_retries(self, mock_ct, mock_spark, mock_sleep):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.side_effect = Exception("DELTA_CONCURRENT conflict")
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        result = self.claim_table("c.s.tbl", config, max_retries=2)
        assert result is False

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_non_concurrent_exception_reraises(self, mock_ct, mock_spark):
        """Non-concurrent exceptions (not Delta conflicts) should propagate."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.side_effect = ValueError("unrelated error")
        config = MagicMock(task_id="t1", mode="comment", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=60)
        with pytest.raises(ValueError, match="unrelated error"):
            self.claim_table("c.s.tbl", config)


# ===========================================================================
# mark_table_completed -- mode-aware SQL
# ===========================================================================

class TestMarkTableCompletedModeAware:

    def setup_method(self):
        from dbxmetagen.processing import mark_table_completed
        self.mark_table_completed = mark_table_completed

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_contains_mode_filter(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode="domain")
        self.mark_table_completed("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "_mode = 'domain'" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_sets_completed_and_clears_claim(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode="comment")
        self.mark_table_completed("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "_status = 'completed'" in sql
        assert "_claimed_by = NULL" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_filters_by_run_id(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="run_xyz", mode="comment")
        self.mark_table_completed("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "run_xyz" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_mode_defaults_to_comment(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode=None)
        self.mark_table_completed("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "_mode = 'comment'" in sql


# ===========================================================================
# mark_table_failed -- mode-aware SQL + error escaping
# ===========================================================================

class TestMarkTableFailedModeAware:

    def setup_method(self):
        from dbxmetagen.processing import mark_table_failed
        self.mark_table_failed = mark_table_failed

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_contains_mode_filter(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode="pi")
        self.mark_table_failed("c.s.tbl", config, "err")
        sql = spark.sql.call_args[0][0]
        assert "_mode = 'pi'" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_sets_failed_status(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode="comment")
        self.mark_table_failed("c.s.tbl", config, "something broke")
        sql = spark.sql.call_args[0][0]
        assert "_status = 'failed'" in sql
        assert "something broke" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_escapes_single_quotes_in_error(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode="comment")
        self.mark_table_failed("c.s.tbl", config, "can't parse 'value'")
        sql = spark.sql.call_args[0][0]
        assert "can''t parse ''value''" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_no_error_message_omits_clause(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="r1", mode="comment")
        self.mark_table_failed("c.s.tbl", config, None)
        sql = spark.sql.call_args[0][0]
        assert "_error_message" not in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_filters_by_run_id(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", run_id="run_abc", mode="comment")
        self.mark_table_failed("c.s.tbl", config, "err")
        sql = spark.sql.call_args[0][0]
        assert "run_abc" in sql


# ===========================================================================
# mark_as_deleted -- mode-aware SQL
# ===========================================================================

class TestMarkAsDeletedModeAware:

    def setup_method(self):
        from dbxmetagen.processing import mark_as_deleted
        self.mark_as_deleted = mark_as_deleted

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_contains_mode_filter(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", mode="domain")
        self.mark_as_deleted("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "_mode = 'domain'" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_sql_clears_claim_and_sets_completed(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", mode="comment")
        self.mark_as_deleted("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "_status = 'completed'" in sql
        assert "_claimed_by = NULL" in sql
        assert "_deleted_at = current_timestamp()" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_mode_defaults_to_comment(self, mock_ct, mock_spark):
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        config = MagicMock(catalog_name="c", schema_name="s", mode=None)
        self.mark_as_deleted("c.s.tbl", config)
        sql = spark.sql.call_args[0][0]
        assert "_mode = 'comment'" in sql


# ===========================================================================
# claim_table: combined scenario tests
# ===========================================================================

class TestClaimTableCombinedScenarios:
    """Integration-like tests verifying that the combined WHERE clause is correct."""

    def setup_method(self):
        from dbxmetagen.processing import claim_table
        self.claim_table = claim_table

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_normal_mode_has_all_four_conditions(self, mock_ct, mock_spark):
        """Normal (non-cleanup) mode should have mode + unclaimed + self-reclaim + timeout + status."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="pi", catalog_name="c", schema_name="s",
                           cleanup_control_table=False, claim_timeout_minutes=45)
        self.claim_table("c.s.tbl", config)
        sql = spark.sql.call_args_list[0][0][0]
        assert "_mode = 'pi'" in sql
        assert "_claimed_by IS NULL" in sql
        assert "_claimed_by = 't1'" in sql
        assert "INTERVAL 45 MINUTES" in sql
        assert "'queued'" in sql
        assert "'failed'" in sql
        assert "'completed'" in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_cleanup_mode_only_has_mode_and_table(self, mock_ct, mock_spark):
        """Cleanup mode: only table_name + _mode, no claim/timeout/status conditions."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        spark.sql.return_value.collect.return_value = [{"_claimed_by": "t1"}]
        config = MagicMock(task_id="t1", mode="pi", catalog_name="c", schema_name="s",
                           cleanup_control_table=True, claim_timeout_minutes=45)
        self.claim_table("c.s.tbl", config)
        sql = spark.sql.call_args_list[0][0][0]
        assert "_mode = 'pi'" in sql
        assert "WHERE table_name = 'c.s.tbl'" in sql
        assert "_claimed_by IS NULL" not in sql
        assert "INTERVAL" not in sql
        assert "'queued'" not in sql

    @patch("dbxmetagen.processing.SparkSession")
    @patch("dbxmetagen.processing.get_control_table")
    def test_two_different_modes_same_table_both_succeed(self, mock_ct, mock_spark):
        """Verify that two tasks with different modes can both claim the same table."""
        mock_ct.return_value = "ctrl"
        spark = mock_spark.builder.getOrCreate.return_value
        
        for mode, tid in [("comment", "task_a"), ("domain", "task_b")]:
            spark.sql.reset_mock()
            spark.sql.return_value.collect.return_value = [{"_claimed_by": tid}]
            config = MagicMock(task_id=tid, mode=mode, catalog_name="c", schema_name="s",
                               cleanup_control_table=False, claim_timeout_minutes=60)
            result = self.claim_table("c.s.tbl", config)
            assert result is True
            update_sql = spark.sql.call_args_list[0][0][0]
            assert f"_mode = '{mode}'" in update_sql


# ===========================================================================
# uc-metadata-assistant: SettingsManager env-var catalog
# ===========================================================================

def _load_uc_assistant_module(module_filename):
    """Import a module from the uc-metadata-assistant app directory by filename."""
    import importlib.util, pathlib
    app_dir = pathlib.Path(__file__).resolve().parent.parent / "apps" / "uc-metadata-assistant"
    spec = importlib.util.spec_from_file_location(
        f"uc_assistant_{module_filename.removesuffix('.py')}",
        app_dir / module_filename,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestSettingsManagerEnvVarCatalog:
    """Verify SettingsManager uses UC_ASSISTANT_CATALOG env var."""

    def test_default_catalog_when_env_not_set(self, monkeypatch):
        monkeypatch.delenv("UC_ASSISTANT_CATALOG", raising=False)
        mod = _load_uc_assistant_module("settings_manager.py")
        sm = mod.SettingsManager(MagicMock())
        assert sm.settings_schema == "uc_metadata_assistant.settings"

    def test_custom_catalog_from_env(self, monkeypatch):
        monkeypatch.setenv("UC_ASSISTANT_CATALOG", "my_custom_catalog")
        mod = _load_uc_assistant_module("settings_manager.py")
        sm = mod.SettingsManager(MagicMock())
        assert sm.settings_schema == "my_custom_catalog.settings"
        assert sm.settings_table == "my_custom_catalog.settings.workspace_settings"

    def test_settings_table_derives_from_schema(self, monkeypatch):
        monkeypatch.setenv("UC_ASSISTANT_CATALOG", "cat_x")
        mod = _load_uc_assistant_module("settings_manager.py")
        sm = mod.SettingsManager(MagicMock())
        assert sm.settings_table == f"{sm.settings_schema}.workspace_settings"


class TestAutoSetupManagerEnvVarCatalog:
    """Verify AutoSetupManager uses UC_ASSISTANT_CATALOG and output_schema key."""

    def test_default_catalog_when_env_not_set(self, monkeypatch):
        monkeypatch.delenv("UC_ASSISTANT_CATALOG", raising=False)
        mod = _load_uc_assistant_module("setup_utils.py")
        mgr = mod.AutoSetupManager(MagicMock())
        assert mgr.default_config['output_catalog'] == "uc_metadata_assistant"

    def test_custom_catalog_from_env(self, monkeypatch):
        monkeypatch.setenv("UC_ASSISTANT_CATALOG", "prod_catalog")
        mod = _load_uc_assistant_module("setup_utils.py")
        mgr = mod.AutoSetupManager(MagicMock())
        assert mgr.default_config['output_catalog'] == "prod_catalog"

    def test_output_schema_key_exists(self, monkeypatch):
        monkeypatch.delenv("UC_ASSISTANT_CATALOG", raising=False)
        mod = _load_uc_assistant_module("setup_utils.py")
        mgr = mod.AutoSetupManager(MagicMock())
        assert 'output_schema' in mgr.default_config
        assert mgr.default_config['output_schema'] == 'generated_metadata'

    def test_config_does_not_use_schema_name_key(self, monkeypatch):
        """Verify the old buggy 'schema_name' key is NOT present in default_config."""
        monkeypatch.delenv("UC_ASSISTANT_CATALOG", raising=False)
        mod = _load_uc_assistant_module("setup_utils.py")
        mgr = mod.AutoSetupManager(MagicMock())
        assert 'schema_name' not in mgr.default_config

    def test_results_table_path_uses_output_schema(self, monkeypatch):
        monkeypatch.delenv("UC_ASSISTANT_CATALOG", raising=False)
        mod = _load_uc_assistant_module("setup_utils.py")
        mgr = mod.AutoSetupManager(MagicMock())
        cfg = mgr.default_config
        path = f"{cfg['output_catalog']}.{cfg['output_schema']}.{cfg['results_table']}"
        assert path == "uc_metadata_assistant.generated_metadata.metadata_results"

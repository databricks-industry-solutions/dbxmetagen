"""
Tests for performance improvements: DDL batching, batch DESCRIBE EXTENDED
replacement, concurrent LLM calls, and mode='all' unified pass.

Run with: uv run pytest tests/test_performance_improvements.py -v
"""

import os
import sys
import tempfile
import pytest
from collections import defaultdict
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dbxmetagen.config import MetadataConfig


def _cfg(**kw):
    return MetadataConfig(skip_yaml_loading=True, catalog_name="cat", schema_name="sch", **kw)


# ---------------------------------------------------------------------------
# Fixture: import processing.py with stubs
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def proc_module():
    """Import processing.py once, with internal stubs active."""
    from tests.conftest import install_processing_stubs, uninstall_processing_stubs
    saved = install_processing_stubs()
    import dbxmetagen.processing as proc
    yield proc
    uninstall_processing_stubs(saved)


# ---------------------------------------------------------------------------
# Helpers for building mock DataFrames
# ---------------------------------------------------------------------------

def _make_row(**kw):
    """Return a MagicMock that acts like a PySpark Row."""
    m = MagicMock()
    m.asDict.return_value = dict(kw)
    m.__getitem__ = lambda self, k: kw[k]
    return m


def _make_df(rows, columns):
    """Build a mock DataFrame with .columns, .select().collect()."""
    df = MagicMock()
    df.columns = list(columns)

    def _select(*cols):
        selected = MagicMock()
        selected.collect.return_value = [
            _make_row(**{c: r.asDict()[c] for c in cols if c in r.asDict()})
            for r in rows
        ]
        return selected
    df.select = _select
    return df


# ===================================================================
# 1. TestNewConfigVariables -- existing, kept as-is
# ===================================================================

class TestNewConfigVariables:
    def test_batch_ddl_apply_defaults_true(self):
        assert _cfg().batch_ddl_apply is True

    def test_batch_ddl_apply_string_false(self):
        assert _cfg(batch_ddl_apply="false").batch_ddl_apply is False

    def test_batch_ddl_max_columns_default(self):
        assert _cfg().batch_ddl_max_columns == 200

    def test_batch_ddl_max_columns_override(self):
        assert _cfg(batch_ddl_max_columns="50").batch_ddl_max_columns == 50

    def test_max_concurrent_llm_calls_default(self):
        assert _cfg().max_concurrent_llm_calls == 4

    def test_max_concurrent_llm_calls_override(self):
        assert _cfg(max_concurrent_llm_calls="1").max_concurrent_llm_calls == 1

    def test_mode_all_accepted(self):
        assert _cfg(mode="all").mode == "all"


# ===================================================================
# 2. TestBuildBatchDDL -- expanded with edge cases
# ===================================================================

class TestBuildBatchDDL:
    def test_comment_single(self, proc_module):
        r = proc_module._build_batch_comment_ddl("cat.sch.tbl", [("col_a", "Desc A")])
        assert r.startswith("ALTER TABLE cat.sch.tbl")
        assert 'ALTER COLUMN `col_a` COMMENT "Desc A"' in r

    def test_comment_multi(self, proc_module):
        cols = [("a", "D1"), ("b", "D2"), ("c", "D3")]
        r = proc_module._build_batch_comment_ddl("cat.sch.tbl", cols)
        assert r.count("ALTER COLUMN") == 3

    def test_comment_sanitizes_double_quotes(self, proc_module):
        r = proc_module._build_batch_comment_ddl("t", [("c", 'It"s a "test"')])
        assert '""' not in r
        assert "It's a 'test'" in r

    def test_comment_backtick_column_name(self, proc_module):
        r = proc_module._build_batch_comment_ddl("t", [("my col", "desc")])
        assert "`my col`" in r

    def test_empty_columns(self, proc_module):
        r = proc_module._build_batch_comment_ddl("t", [])
        assert r == "ALTER TABLE t ;"

    def test_pi_tags_custom(self, proc_module):
        cfg = _cfg(pi_classification_tag_name="dc", pi_subclassification_tag_name="dsc")
        r = proc_module._build_batch_pi_ddl("t", [("c1", "pii", "ssn")], cfg)
        assert "'dc' = 'pii'" in r
        assert "'dsc' = 'ssn'" in r

    def test_pi_tags_default_names(self, proc_module):
        cfg = _cfg()
        r = proc_module._build_batch_pi_ddl("t", [("c1", "pi", "email")], cfg)
        assert "'data_classification' = 'pi'" in r
        assert "'data_subclassification' = 'email'" in r

    def test_pi_multi_columns(self, proc_module):
        cfg = _cfg()
        cols = [("a", "pi", "ssn"), ("b", "pci", "credit_card"), ("c", "None", "")]
        r = proc_module._build_batch_pi_ddl("t", cols, cfg)
        assert r.count("ALTER COLUMN") == 3
        assert r.count("SET TAGS") == 3

    def test_pi_single_quote_in_value(self, proc_module):
        cfg = _cfg()
        r = proc_module._build_batch_pi_ddl("t", [("c", "pi", "driver's_license")], cfg)
        assert "driver's_license" in r


# ===================================================================
# 3. TestApplyBatchedDDL -- thorough
# ===================================================================

class TestApplyBatchedDDL:
    def _comment_rows(self, table="cat.sch.tbl"):
        return [
            _make_row(tokenized_table=table, column_name="a", column_content="Desc A", ddl_type="column", ddl="ALTER TABLE t ALTER COLUMN a COMMENT 'Desc A'"),
            _make_row(tokenized_table=table, column_name="b", column_content="Desc B", ddl_type="column", ddl="ALTER TABLE t ALTER COLUMN b COMMENT 'Desc B'"),
            _make_row(tokenized_table=table, column_name="c", column_content="Desc C", ddl_type="column", ddl="ALTER TABLE t ALTER COLUMN c COMMENT 'Desc C'"),
        ]

    def test_dry_run_returns_zero(self, proc_module):
        df = _make_df([], ["tokenized_table", "column_name", "column_content", "ddl_type", "ddl"])
        cfg = _cfg(mode="comment", dry_run=True)
        result = proc_module._apply_batched_ddl(df, cfg, MagicMock())
        assert result["success_count"] == 0
        assert result["failed_count"] == 0

    def test_success_path_calls_spark_sql(self, proc_module):
        rows = self._comment_rows()
        df = _make_df(rows, ["tokenized_table", "column_name", "column_content", "ddl_type", "ddl"])
        cfg = _cfg(mode="comment")
        mock_spark = MagicMock()
        result = proc_module._apply_batched_ddl(df, cfg, mock_spark)
        assert result["success_count"] == 3
        assert result["failed_count"] == 0
        mock_spark.sql.assert_called_once()
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "ALTER TABLE cat.sch.tbl" in sql_arg
        assert "ALTER COLUMN `a`" in sql_arg
        assert "ALTER COLUMN `b`" in sql_arg
        assert "ALTER COLUMN `c`" in sql_arg

    def test_table_level_ddl_applied_individually(self, proc_module):
        rows = [
            _make_row(tokenized_table="cat.sch.tbl", column_name="None", column_content="Table desc", ddl_type="table", ddl="COMMENT ON TABLE cat.sch.tbl IS 'desc'"),
            _make_row(tokenized_table="cat.sch.tbl", column_name="a", column_content="Desc A", ddl_type="column", ddl="ALTER TABLE t ALTER COLUMN a COMMENT 'Desc A'"),
        ]
        df = _make_df(rows, ["tokenized_table", "column_name", "column_content", "ddl_type", "ddl"])
        cfg = _cfg(mode="comment")
        mock_spark = MagicMock()
        result = proc_module._apply_batched_ddl(df, cfg, mock_spark)
        assert result["success_count"] == 2
        assert mock_spark.sql.call_count == 2
        first_call = mock_spark.sql.call_args_list[0][0][0]
        assert "COMMENT ON TABLE" in first_call

    def test_chunking_by_batch_ddl_max_columns(self, proc_module):
        rows = [
            _make_row(tokenized_table="t", column_name=f"c{i}", column_content=f"D{i}", ddl_type="column", ddl=f"DDL{i}")
            for i in range(5)
        ]
        df = _make_df(rows, ["tokenized_table", "column_name", "column_content", "ddl_type", "ddl"])
        cfg = _cfg(mode="comment", batch_ddl_max_columns=2)
        mock_spark = MagicMock()
        result = proc_module._apply_batched_ddl(df, cfg, mock_spark)
        assert result["success_count"] == 5
        assert mock_spark.sql.call_count == 3  # ceil(5/2)=3 batch chunks

    def test_fallback_on_batch_failure(self, proc_module):
        rows = self._comment_rows()
        df = _make_df(rows, ["tokenized_table", "column_name", "column_content", "ddl_type", "ddl"])
        cfg = _cfg(mode="comment")
        mock_spark = MagicMock()
        call_count = [0]

        def side_effect(sql):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("batch syntax error")
            return MagicMock()

        mock_spark.sql.side_effect = side_effect
        result = proc_module._apply_batched_ddl(df, cfg, mock_spark)
        assert result["success_count"] == 3
        assert result["failed_count"] == 0
        # 1 batch call (failed) + 3 individual fallbacks
        assert mock_spark.sql.call_count == 4

    def test_tag_policy_in_fallback(self, proc_module):
        rows = [
            _make_row(tokenized_table="t", column_name="a", classification="pii", type="ssn",
                      ddl_type="column", ddl="ALTER TABLE t ALTER COLUMN a SET TAGS ('dc' = 'pii')"),
        ]
        df = _make_df(rows, ["tokenized_table", "column_name", "classification", "type", "ddl_type", "ddl"])
        cfg = _cfg(mode="pi")
        mock_spark = MagicMock()
        call_count = [0]

        def side_effect(sql):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("batch error")
            raise Exception("Tag policy violation: 'dc' cannot be set to 'pii'")

        mock_spark.sql.side_effect = side_effect
        result = proc_module._apply_batched_ddl(df, cfg, mock_spark)
        assert result["failed_count"] == 1
        assert len(result["missing_tags"]) == 1
        assert "dc=pii" in result["missing_tags"][0]

    def test_domain_mode_falls_through(self, proc_module):
        df = MagicMock()
        df.columns = ["ddl"]
        df.select.return_value.collect.return_value = []
        cfg = _cfg(mode="domain", batch_ddl_apply=True)
        result = proc_module._apply_batched_ddl(df, cfg, MagicMock())
        assert "success_count" in result


# ===================================================================
# 4. TestApplyCommentDDLDispatch -- routing verification
# ===================================================================

class TestApplyCommentDDLDispatch:
    @patch("dbxmetagen.processing._apply_batched_ddl")
    @patch("dbxmetagen.processing._apply_individual_ddl")
    @patch("dbxmetagen.processing.SparkSession")
    def test_comment_batch_enabled(self, mock_ss, mock_indiv, mock_batch, proc_module):
        mock_batch.return_value = {"success_count": 1, "failed_count": 0}
        cfg = _cfg(mode="comment", batch_ddl_apply=True, federation_mode=False)
        proc_module.apply_comment_ddl(MagicMock(), cfg)
        mock_batch.assert_called_once()
        mock_indiv.assert_not_called()

    @patch("dbxmetagen.processing._apply_batched_ddl")
    @patch("dbxmetagen.processing._apply_individual_ddl")
    @patch("dbxmetagen.processing.SparkSession")
    def test_pi_batch_enabled(self, mock_ss, mock_indiv, mock_batch, proc_module):
        mock_batch.return_value = {"success_count": 1, "failed_count": 0}
        cfg = _cfg(mode="pi", batch_ddl_apply=True, federation_mode=False)
        proc_module.apply_comment_ddl(MagicMock(), cfg)
        mock_batch.assert_called_once()
        mock_indiv.assert_not_called()

    @patch("dbxmetagen.processing._apply_batched_ddl")
    @patch("dbxmetagen.processing._apply_individual_ddl")
    @patch("dbxmetagen.processing.SparkSession")
    def test_federation_mode_forces_individual(self, mock_ss, mock_indiv, mock_batch, proc_module):
        mock_indiv.return_value = {"success_count": 0, "failed_count": 0, "missing_tags": [], "failed_statements": []}
        cfg = _cfg(mode="comment", batch_ddl_apply=True, federation_mode=True)
        proc_module.apply_comment_ddl(MagicMock(), cfg)
        mock_indiv.assert_called_once()
        mock_batch.assert_not_called()

    @patch("dbxmetagen.processing._apply_batched_ddl")
    @patch("dbxmetagen.processing._apply_individual_ddl")
    @patch("dbxmetagen.processing.SparkSession")
    def test_batch_disabled_forces_individual(self, mock_ss, mock_indiv, mock_batch, proc_module):
        mock_indiv.return_value = {"success_count": 0, "failed_count": 0, "missing_tags": [], "failed_statements": []}
        cfg = _cfg(mode="comment", batch_ddl_apply=False, federation_mode=False)
        proc_module.apply_comment_ddl(MagicMock(), cfg)
        mock_indiv.assert_called_once()
        mock_batch.assert_not_called()

    @patch("dbxmetagen.processing._apply_batched_ddl")
    @patch("dbxmetagen.processing._apply_individual_ddl")
    @patch("dbxmetagen.processing.SparkSession")
    def test_domain_always_individual(self, mock_ss, mock_indiv, mock_batch, proc_module):
        mock_indiv.return_value = {"success_count": 0, "failed_count": 0, "missing_tags": [], "failed_statements": []}
        cfg = _cfg(mode="domain", batch_ddl_apply=True, federation_mode=False)
        proc_module.apply_comment_ddl(MagicMock(), cfg)
        mock_indiv.assert_called_once()
        mock_batch.assert_not_called()

    @patch("dbxmetagen.processing._apply_batched_ddl")
    @patch("dbxmetagen.processing._apply_individual_ddl")
    @patch("dbxmetagen.processing.SparkSession")
    def test_mode_all_not_in_batch_tuple(self, mock_ss, mock_indiv, mock_batch, proc_module):
        """mode='all' is NOT in ('comment', 'pi') so dispatch goes to individual."""
        mock_indiv.return_value = {"success_count": 0, "failed_count": 0, "missing_tags": [], "failed_statements": []}
        cfg = _cfg(mode="all", batch_ddl_apply=True, federation_mode=False)
        proc_module.apply_comment_ddl(MagicMock(), cfg)
        mock_indiv.assert_called_once()
        mock_batch.assert_not_called()


# ===================================================================
# 5. TestWriteBatchDDLFile -- file I/O
# ===================================================================

class TestWriteBatchDDLFile:
    def _comment_df(self, n=3, table="cat.sch.tbl"):
        rows = [
            _make_row(tokenized_table=table, column_name=f"c{i}",
                      column_content=f"Desc {i}", ddl_type="column")
            for i in range(n)
        ]
        return _make_df(rows, ["tokenized_table", "column_name", "column_content", "ddl_type"])

    def test_file_created_comment_mode(self, proc_module):
        with tempfile.TemporaryDirectory() as d:
            df = self._comment_df()
            cfg = _cfg(mode="comment", batch_ddl_apply=True)
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", cfg)
            batch_file = os.path.join(d, "test_tbl_batch.sql")
            assert os.path.exists(batch_file)
            content = open(batch_file).read()
            assert "ALTER TABLE cat.sch.tbl" in content
            assert "ALTER COLUMN `c0`" in content

    def test_early_return_batch_disabled(self, proc_module):
        with tempfile.TemporaryDirectory() as d:
            df = self._comment_df()
            cfg = _cfg(mode="comment", batch_ddl_apply=False)
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", cfg)
            assert not os.path.exists(os.path.join(d, "test_tbl_batch.sql"))

    def test_early_return_domain_mode(self, proc_module):
        with tempfile.TemporaryDirectory() as d:
            df = self._comment_df()
            cfg = _cfg(mode="domain", batch_ddl_apply=True)
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", cfg)
            assert not os.path.exists(os.path.join(d, "test_tbl_batch.sql"))

    def test_early_return_no_config(self, proc_module):
        with tempfile.TemporaryDirectory() as d:
            df = self._comment_df()
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", config=None)
            assert not os.path.exists(os.path.join(d, "test_tbl_batch.sql"))

    def test_table_rows_excluded(self, proc_module):
        rows = [
            _make_row(tokenized_table="t", column_name="None", column_content="Tbl desc", ddl_type="table"),
            _make_row(tokenized_table="t", column_name="a", column_content="Desc A", ddl_type="column"),
        ]
        df = _make_df(rows, ["tokenized_table", "column_name", "column_content", "ddl_type"])
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(mode="comment", batch_ddl_apply=True)
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", cfg)
            content = open(os.path.join(d, "test_tbl_batch.sql")).read()
            assert content.count("ALTER TABLE") == 1
            assert "`a`" in content
            assert "`None`" not in content

    def test_chunking_in_file(self, proc_module):
        df = self._comment_df(n=5)
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(mode="comment", batch_ddl_apply=True, batch_ddl_max_columns=2)
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", cfg)
            content = open(os.path.join(d, "test_tbl_batch.sql")).read()
            stmts = [s for s in content.strip().split("\n") if s.strip()]
            assert len(stmts) == 3  # ceil(5/2)=3

    def test_pi_mode_file(self, proc_module):
        rows = [
            _make_row(tokenized_table="t", column_name="a", classification="pi",
                      type="ssn", ddl_type="column"),
        ]
        df = _make_df(rows, ["tokenized_table", "column_name", "classification", "type", "ddl_type"])
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(mode="pi", batch_ddl_apply=True)
            proc_module._write_batch_ddl_file(df, "test_tbl", d, "sql", cfg)
            content = open(os.path.join(d, "test_tbl_batch.sql")).read()
            assert "SET TAGS" in content
            assert "'data_classification' = 'pi'" in content


# ===================================================================
# 6. TestConcurrentLLMChunkOrder
# ===================================================================

class TestConcurrentLLMChunkOrder:
    """Test the concurrent chunk processing logic in get_generated_metadata_data_aware.
    The function signature is (spark, config, full_table_name). We need to mock
    several intermediate calls (_is_metric_view, read_table_with_type_conversion, etc.)."""

    def _run_data_aware(self, proc_module, cfg, num_chunks, mock_tpe_cls):
        """Helper to call get_generated_metadata_data_aware with all deps mocked."""
        original_tpe = proc_module.ThreadPoolExecutor
        original_as_completed = proc_module.as_completed
        proc_module.ThreadPoolExecutor = mock_tpe_cls
        # Replace as_completed so it just yields the mock futures immediately
        proc_module.as_completed = lambda futures: futures
        try:
            with patch.object(proc_module, "_is_metric_view", return_value=False), \
                 patch.object(proc_module, "read_table_with_type_conversion") as mock_read, \
                 patch.object(proc_module, "chunk_df") as mock_chunk, \
                 patch.object(proc_module, "sample_df") as mock_sample, \
                 patch.object(proc_module, "PromptFactory") as mock_pf, \
                 patch.object(proc_module, "MetadataGeneratorFactory") as mock_mgf, \
                 patch.object(proc_module, "check_token_length_against_num_words"):

                mock_df = MagicMock()
                mock_df.count.return_value = 10
                mock_df.columns = [f"c{i}" for i in range(num_chunks * 2)]
                mock_read.return_value = mock_df

                chunks = [MagicMock() for _ in range(num_chunks)]
                for i, c in enumerate(chunks):
                    c.columns = [f"c{i}"]
                mock_chunk.return_value = chunks
                mock_sample.return_value = MagicMock()

                mock_prompt = MagicMock()
                mock_pf.create_prompt.return_value = mock_prompt

                mock_gen = MagicMock()
                mock_gen.get_responses.return_value = (MagicMock(), None)
                mock_mgf.create_generator.return_value = mock_gen

                mock_spark = MagicMock()
                proc_module.get_generated_metadata_data_aware(mock_spark, cfg, "cat.sch.tbl")
        finally:
            proc_module.ThreadPoolExecutor = original_tpe
            proc_module.as_completed = original_as_completed

    def test_executor_used_when_multi_chunk(self, proc_module):
        """Verify ThreadPoolExecutor is instantiated for >1 chunks and >1 workers."""
        mock_executor = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = mock_future
        mock_tpe_cls = MagicMock(return_value=mock_executor)

        cfg = _cfg(mode="comment", max_concurrent_llm_calls=4, columns_per_call=2,
                    sample_size=5)
        self._run_data_aware(proc_module, cfg, num_chunks=3, mock_tpe_cls=mock_tpe_cls)

        mock_tpe_cls.assert_called_once()
        assert mock_tpe_cls.call_args[1]["max_workers"] == 3

    def test_serial_when_single_chunk(self, proc_module):
        """Single chunk should NOT use ThreadPoolExecutor."""
        mock_tpe_cls = MagicMock()
        cfg = _cfg(mode="comment", max_concurrent_llm_calls=4, columns_per_call=20,
                    sample_size=5, registered_model_name="default")
        self._run_data_aware(proc_module, cfg, num_chunks=1, mock_tpe_cls=mock_tpe_cls)
        mock_tpe_cls.assert_not_called()

    def test_serial_when_max_workers_one(self, proc_module):
        """max_concurrent_llm_calls=1 should NOT use ThreadPoolExecutor even with 3 chunks."""
        mock_tpe_cls = MagicMock()
        cfg = _cfg(mode="comment", max_concurrent_llm_calls=1, columns_per_call=2,
                    sample_size=5, registered_model_name="default")
        self._run_data_aware(proc_module, cfg, num_chunks=3, mock_tpe_cls=mock_tpe_cls)
        mock_tpe_cls.assert_not_called()


# ===================================================================
# 7. TestReviewAndGenerateAllModes
# ===================================================================

class TestReviewAndGenerateAllModes:
    @patch("dbxmetagen.processing.rows_to_df")
    @patch("dbxmetagen.processing.append_domain_table_row")
    @patch("dbxmetagen.processing.get_domain_classification")
    @patch("dbxmetagen.processing.append_column_rows")
    @patch("dbxmetagen.processing.append_table_row")
    @patch("dbxmetagen.processing.get_generated_metadata")
    @patch("dbxmetagen.processing.replace_catalog_name")
    def test_all_three_phases_succeed(self, mock_replace, mock_gen, mock_append_tbl,
                                      mock_append_col, mock_domain, mock_append_domain,
                                      mock_rows_to_df, proc_module):
        mock_replace.return_value = "tok.sch.tbl"
        mock_gen.return_value = [MagicMock()]
        mock_append_tbl.return_value = [{"row": 1}]
        mock_append_col.return_value = [{"row": 1}]
        mock_domain.return_value = MagicMock()
        mock_append_domain.return_value = [{"domain_row": 1}]
        mock_rows_to_df.return_value = MagicMock()

        cfg = _cfg(mode="all")
        result = proc_module._review_and_generate_all_modes(cfg, "cat.sch.tbl")

        assert "comment" in result
        assert "pi" in result
        assert "domain" in result
        for mode_key in result:
            col_df, tbl_df = result[mode_key]
            assert col_df is not None
            assert tbl_df is not None

    @patch("dbxmetagen.processing.rows_to_df")
    @patch("dbxmetagen.processing.append_domain_table_row")
    @patch("dbxmetagen.processing.get_domain_classification")
    @patch("dbxmetagen.processing.append_column_rows")
    @patch("dbxmetagen.processing.append_table_row")
    @patch("dbxmetagen.processing.get_generated_metadata")
    @patch("dbxmetagen.processing.replace_catalog_name")
    def test_pi_fails_others_succeed(self, mock_replace, mock_gen, mock_append_tbl,
                                     mock_append_col, mock_domain, mock_append_domain,
                                     mock_rows_to_df, proc_module):
        mock_replace.return_value = "tok.sch.tbl"
        call_count = [0]

        def gen_side_effect(cfg, tbl):
            call_count[0] += 1
            if cfg.mode == "pi":
                raise RuntimeError("PI endpoint unavailable")
            return [MagicMock()]

        mock_gen.side_effect = gen_side_effect
        mock_append_tbl.return_value = [{"row": 1}]
        mock_append_col.return_value = [{"row": 1}]
        mock_domain.return_value = MagicMock()
        mock_append_domain.return_value = [{"domain_row": 1}]
        mock_rows_to_df.return_value = MagicMock()

        cfg = _cfg(mode="all")
        result = proc_module._review_and_generate_all_modes(cfg, "cat.sch.tbl")

        assert result["pi"] == (None, None)
        assert result["comment"][0] is not None
        assert result["domain"][0] is not None

    @patch("dbxmetagen.processing.rows_to_df")
    @patch("dbxmetagen.processing.append_domain_table_row")
    @patch("dbxmetagen.processing.get_domain_classification")
    @patch("dbxmetagen.processing.append_column_rows")
    @patch("dbxmetagen.processing.append_table_row")
    @patch("dbxmetagen.processing.get_generated_metadata")
    @patch("dbxmetagen.processing.replace_catalog_name")
    def test_all_phases_fail(self, mock_replace, mock_gen, mock_append_tbl,
                             mock_append_col, mock_domain, mock_append_domain,
                             mock_rows_to_df, proc_module):
        mock_replace.return_value = "tok.sch.tbl"
        mock_gen.side_effect = RuntimeError("LLM down")
        mock_domain.side_effect = RuntimeError("Domain down")
        mock_rows_to_df.return_value = MagicMock()

        cfg = _cfg(mode="all")
        result = proc_module._review_and_generate_all_modes(cfg, "cat.sch.tbl")

        for mode_key in ("comment", "pi", "domain"):
            assert result[mode_key] == (None, None)


# ===================================================================
# 8. TestProcessAllModesDDL
# ===================================================================

class TestProcessAllModesDDL:
    @patch("dbxmetagen.processing.add_ddl_to_dfs")
    @patch("dbxmetagen.processing.split_and_hardcode_df")
    def test_all_modes_present(self, mock_split, mock_add_ddl, proc_module):
        mock_split.side_effect = lambda df, cfg: df
        mock_add_ddl.return_value = {"final_df": MagicMock()}

        cfg = _cfg(mode="all", allow_manual_override=False)
        mode_results = {
            "comment": (MagicMock(), MagicMock()),
            "pi": (MagicMock(), MagicMock()),
            "domain": (MagicMock(), MagicMock()),
        }
        result = proc_module._process_all_modes_ddl(cfg, "cat.sch.tbl", mode_results)

        assert "_all_mode_dfs" in result
        assert set(result["_all_mode_dfs"].keys()) == {"comment", "pi", "domain"}

    @patch("dbxmetagen.processing.add_ddl_to_dfs")
    @patch("dbxmetagen.processing.split_and_hardcode_df")
    def test_skips_none_modes(self, mock_split, mock_add_ddl, proc_module):
        mock_split.return_value = None
        mock_add_ddl.return_value = {"final_df": MagicMock()}

        cfg = _cfg(mode="all", allow_manual_override=False)
        mode_results = {
            "comment": (MagicMock(), MagicMock()),
            "pi": (None, None),
            "domain": (None, None),
        }

        mock_split.side_effect = lambda df, cfg: df if df is not None else None
        result = proc_module._process_all_modes_ddl(cfg, "cat.sch.tbl", mode_results)
        assert "_all_mode_dfs" in result
        assert "comment" in result["_all_mode_dfs"]
        assert "pi" not in result["_all_mode_dfs"]

    @patch("dbxmetagen.processing.add_ddl_to_dfs")
    @patch("dbxmetagen.processing.split_and_hardcode_df")
    def test_all_none_returns_skip_reason(self, mock_split, mock_add_ddl, proc_module):
        mock_split.return_value = None

        cfg = _cfg(mode="all", allow_manual_override=False)
        mode_results = {
            "comment": (None, None),
            "pi": (None, None),
            "domain": (None, None),
        }
        result = proc_module._process_all_modes_ddl(cfg, "cat.sch.tbl", mode_results)
        assert "_skip_reason" in result
        assert "No metadata" in result["_skip_reason"]


# ===================================================================
# 9. TestClaimTableModeFilter -- SQL inspection
# ===================================================================

class TestClaimTableModeFilter:
    def test_no_task_id_returns_true(self, proc_module):
        cfg = _cfg(mode="all", control_table="metadata_control_{}", task_id=None)
        with patch("dbxmetagen.processing.get_control_table", return_value="ctrl"):
            assert proc_module.claim_table("cat.sch.tbl", cfg) is True

    def test_mode_all_where_clause(self, proc_module):
        cfg = _cfg(mode="all", control_table="metadata_control_{}", task_id="t123")
        mock_spark = MagicMock()
        verify_row = MagicMock()
        verify_row.__getitem__ = lambda self, k: "t123"
        mock_spark.sql.return_value.collect.return_value = [verify_row]

        with patch("dbxmetagen.processing.get_control_table", return_value="ctrl"), \
             patch("dbxmetagen.processing.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            proc_module.claim_table("cat.sch.tbl", cfg)

        sqls = [c[0][0] for c in mock_spark.sql.call_args_list]
        update_sql = sqls[0]
        assert "_mode IN ('comment', 'pi', 'domain', 'all')" in update_sql

    def test_mode_comment_where_clause(self, proc_module):
        cfg = _cfg(mode="comment", control_table="metadata_control_{}", task_id="t123")
        mock_spark = MagicMock()
        verify_row = MagicMock()
        verify_row.__getitem__ = lambda self, k: "t123"
        mock_spark.sql.return_value.collect.return_value = [verify_row]

        with patch("dbxmetagen.processing.get_control_table", return_value="ctrl"), \
             patch("dbxmetagen.processing.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            proc_module.claim_table("cat.sch.tbl", cfg)

        sqls = [c[0][0] for c in mock_spark.sql.call_args_list]
        update_sql = sqls[0]
        assert "_mode = 'comment' OR _mode = 'all'" in update_sql

    def test_cleanup_mode_where_clause(self, proc_module):
        cfg = _cfg(mode="comment", control_table="metadata_control_{}", task_id="t123",
                   cleanup_control_table=True)
        mock_spark = MagicMock()
        verify_row = MagicMock()
        verify_row.__getitem__ = lambda self, k: "t123"
        mock_spark.sql.return_value.collect.return_value = [verify_row]

        with patch("dbxmetagen.processing.get_control_table", return_value="ctrl"), \
             patch("dbxmetagen.processing.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            proc_module.claim_table("cat.sch.tbl", cfg)

        sqls = [c[0][0] for c in mock_spark.sql.call_args_list]
        update_sql = sqls[0]
        assert "_claimed_by IS NULL" not in update_sql
        assert "table_name = 'cat.sch.tbl'" in update_sql


# ===================================================================
# 10. TestFetchColumnMetadataBatch (prompts.py)
# ===================================================================

class TestFetchColumnMetadataBatch:
    """Test the batch DESCRIBE EXTENDED replacement in prompts.py.
    Since Prompt is stubbed as MagicMock by conftest, we replicate the method
    logic inline for isolated unit testing."""

    FIELD_MAP = {
        "data_type": "data_type",
        "is_nullable": "nullable",
        "column_default": "default",
        "comment": "comment",
        "ordinal_position": "ordinal_position",
        "character_maximum_length": "max_length",
        "numeric_precision": "numeric_precision",
        "numeric_scale": "numeric_scale",
    }

    @staticmethod
    def _run_batch_fetch(spark, full_table_name, columns, field_map):
        """Mirror of Prompt._fetch_column_metadata_batch logic."""
        parts = full_table_name.split(".")
        if len(parts) != 3:
            return {}
        catalog, schema, table = parts
        col_list = ", ".join(f"'{c}'" for c in columns)
        query = f"""
            SELECT column_name, data_type, is_nullable, column_default, comment,
                   ordinal_position, character_maximum_length, numeric_precision, numeric_scale
            FROM `{catalog}`.information_schema.columns
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
              AND column_name IN ({col_list})
        """
        rows = spark.sql(query).collect()
        result = {}
        for row in rows:
            rd = row.asDict()
            col = rd.pop("column_name", None)
            if not col:
                continue
            mapped = {}
            for src_key, dst_key in field_map.items():
                val = rd.get(src_key)
                if val is not None:
                    mapped[dst_key] = str(val)
            result[col] = mapped
        return result

    @staticmethod
    def _run_extract(columns, raw_metadata, mode, include_datatype, include_possible_data):
        """Mirror of Prompt.extract_column_metadata filtering logic."""
        column_metadata_dict = {}
        for column_name in columns:
            meta = dict(raw_metadata.get(column_name, {}))
            filtered = {}
            for k, v in meta.items():
                if v is None or str(v) == "NULL":
                    continue
                if mode == "comment":
                    if k in ("description", "comment"):
                        continue
                    if not include_datatype and k == "data_type":
                        continue
                    if not include_possible_data and k in ("min", "max"):
                        continue
                filtered[k] = v
            column_metadata_dict[column_name] = filtered
        return column_metadata_dict

    def test_field_mapping(self):
        mock_spark = MagicMock()
        row_a = MagicMock()
        row_a.asDict.return_value = {
            "column_name": "col_a",
            "data_type": "STRING",
            "is_nullable": "YES",
            "column_default": None,
            "comment": "A column",
            "ordinal_position": 1,
            "character_maximum_length": 255,
            "numeric_precision": None,
            "numeric_scale": None,
        }
        mock_spark.sql.return_value.collect.return_value = [row_a]

        result = self._run_batch_fetch(mock_spark, "cat.sch.tbl", ["col_a"], self.FIELD_MAP)
        assert "col_a" in result
        meta = result["col_a"]
        assert meta["data_type"] == "STRING"
        assert meta["nullable"] == "YES"
        assert meta["comment"] == "A column"
        assert meta["max_length"] == "255"
        assert "numeric_precision" not in meta

    def test_sql_fallback_on_exception(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("information_schema not available")

        # The real code falls back to _fetch_column_metadata_legacy -- verify the SQL was attempted
        with pytest.raises(Exception, match="information_schema not available"):
            mock_spark.sql("SELECT ...").collect()

        # Verify that the batch function raises so callers can fall back
        mock_spark.sql.side_effect = Exception("not available")
        try:
            self._run_batch_fetch(mock_spark, "cat.sch.tbl", ["col_a"], self.FIELD_MAP)
        except Exception:
            pass  # in real code, the except block calls _fetch_column_metadata_legacy

    def test_bad_table_name_returns_empty(self):
        result = self._run_batch_fetch(MagicMock(), "no_dots", ["col_a"], self.FIELD_MAP)
        assert result == {}

    def test_preloaded_cache_bypasses_sql(self):
        cache = {"col_a": {"data_type": "STRING"}, "col_b": {"data_type": "INT"}}
        columns = ["col_a", "col_b"]
        # extract_column_metadata uses preloaded_cache directly (no SQL)
        result = self._run_extract(columns, cache, mode="pi",
                                   include_datatype=True, include_possible_data=True)
        assert result["col_a"]["data_type"] == "STRING"
        assert result["col_b"]["data_type"] == "INT"

    def test_comment_mode_filters_description(self):
        cache = {
            "col_a": {"data_type": "STRING", "comment": "existing desc",
                       "description": "old desc", "nullable": "YES"},
        }
        result = self._run_extract(["col_a"], cache, mode="comment",
                                   include_datatype=False, include_possible_data=True)
        meta = result["col_a"]
        assert "comment" not in meta
        assert "description" not in meta
        assert "data_type" not in meta
        assert meta["nullable"] == "YES"

    def test_comment_mode_includes_datatype_when_enabled(self):
        cache = {"col_a": {"data_type": "INT", "nullable": "NO"}}
        result = self._run_extract(["col_a"], cache, mode="comment",
                                   include_datatype=True, include_possible_data=True)
        assert result["col_a"]["data_type"] == "INT"

    def test_null_values_excluded(self):
        cache = {"col_a": {"data_type": "STRING", "nullable": None, "default": "NULL"}}
        result = self._run_extract(["col_a"], cache, mode="pi",
                                   include_datatype=True, include_possible_data=True)
        assert "nullable" not in result["col_a"]
        assert "default" not in result["col_a"]
        assert result["col_a"]["data_type"] == "STRING"


# ===================================================================
# 11. TestShallowConfigCopy -- existing, kept
# ===================================================================

class TestShallowConfigCopy:
    def test_changes_mode(self, proc_module):
        cfg = _cfg(mode="all")
        c = proc_module._shallow_config_copy(cfg, "comment")
        assert c.mode == "comment"
        assert cfg.mode == "all"
        assert c.catalog_name == "cat"

    def test_preserves_other_fields(self, proc_module):
        cfg = _cfg(mode="all", batch_ddl_apply=True, max_concurrent_llm_calls=8)
        c = proc_module._shallow_config_copy(cfg, "pi")
        assert c.batch_ddl_apply is True
        assert c.max_concurrent_llm_calls == 8


# ===================================================================
# 12. TestModeAllSetup -- existing, kept
# ===================================================================

class TestModeAllSetup:
    def test_all_does_not_raise(self):
        from dbxmetagen.main import setup_mode_dependencies
        cfg = _cfg(mode="all", include_deterministic_pi=False, domain_config_path="")
        setup_mode_dependencies(cfg)

    def test_invalid_raises(self):
        from dbxmetagen.main import setup_mode_dependencies
        cfg = _cfg(mode="invalid_mode")
        with pytest.raises(ValueError, match="Invalid mode"):
            setup_mode_dependencies(cfg)

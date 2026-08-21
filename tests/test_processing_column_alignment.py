"""Tests for column-description alignment in metadata generation (issue #194).

LLM column descriptions used to be paired to columns BY POSITION
(`zip(response.columns, response.column_contents)`), so a single duplicated/dropped
entry silently misaligned an entire table. These tests cover the name-keyed
resolver (`_resolve_column_content_pairs`) and the refuse-on-mismatch behavior of
`append_column_rows`.
"""
import logging
import sys

import pytest

PROC_LOGGER = "dbxmetagen.processing"


@pytest.fixture(scope="module")
def proc_mod():
    """Import ``dbxmetagen.processing`` with the REAL ``metadata_generator`` loaded.

    ``append_column_rows`` does ``isinstance(x, PIColumnContent)``; the default
    conftest stub replaces ``PIColumnContent`` with a MagicMock *instance* (not a
    type), which would raise ``TypeError`` in that isinstance. Loading the real
    module first means ``install_processing_stubs`` leaves it alone (it only stubs
    modules not already imported).
    """
    from conftest import install_processing_stubs, uninstall_processing_stubs

    had_mg = "dbxmetagen.metadata_generator" in sys.modules
    import dbxmetagen.metadata_generator as mg  # noqa: F401  (real classes)

    saved = install_processing_stubs()
    sys.modules.pop("dbxmetagen.processing", None)
    import dbxmetagen.processing as processing

    try:
        yield processing, mg
    finally:
        uninstall_processing_stubs(saved)
        if not had_mg:
            sys.modules.pop("dbxmetagen.metadata_generator", None)
            import dbxmetagen as _pkg
            for attr in ("metadata_generator", "processing"):
                if hasattr(_pkg, attr):
                    delattr(_pkg, attr)


class _Resp:
    """Lightweight stand-in for a CommentResponse/PIResponse."""

    def __init__(self, columns, column_contents, source_columns=None, presidio_results=None):
        self.columns = list(columns)
        self.column_contents = list(column_contents)
        self._source_columns = source_columns
        self.presidio_results = presidio_results


def _cfg(base_config_kwargs, mode="comment"):
    from dbxmetagen.config import MetadataConfig

    return MetadataConfig(**{**base_config_kwargs, "mode": mode})


# --------------------------------------------------------------------------- #
# _resolve_column_content_pairs -- the pure name/position resolution logic
# --------------------------------------------------------------------------- #
class TestResolveColumnContentPairs:
    def test_happy_path_matches_positionally_and_by_name(self, proc_mod):
        proc, _ = proc_mod
        real = [f"col_{i:02d}" for i in range(4)]
        resp = _Resp(real, [f"d{i}" for i in range(4)], source_columns=real)
        pairs = proc._resolve_column_content_pairs(resp, "cat.sch.t", real)
        assert pairs == [("col_00", "d0"), ("col_01", "d1"),
                         ("col_02", "d2"), ("col_03", "d3")]

    def test_length_mismatch_refuses(self, proc_mod, caplog):
        """Issue #194 repro: 16 names, 17 contents (a dup) -> refuse, not shift."""
        proc, _ = proc_mod
        real = [f"col_{i:02d}" for i in range(16)]
        contents = [f"desc for {c}" for c in real]
        contents.insert(6, "desc for col_05 (near-duplicate)")  # 17 contents
        resp = _Resp(real, contents, source_columns=real)
        with caplog.at_level(logging.ERROR, logger=PROC_LOGGER):
            pairs = proc._resolve_column_content_pairs(resp, "cat.sch.wide", real)
        assert pairs is None
        assert any("count mismatch" in r.message and "cat.sch.wide" in r.message
                   for r in caplog.records)

    def test_duplicate_returned_name_refuses(self, proc_mod, caplog):
        proc, _ = proc_mod
        resp = _Resp(["a", "a", "b"], ["d0", "d1", "d2"], source_columns=["a", "b", "c"])
        with caplog.at_level(logging.ERROR, logger=PROC_LOGGER):
            pairs = proc._resolve_column_content_pairs(resp, "cat.sch.t", ["a", "b", "c"])
        assert pairs is None
        assert any("Duplicate returned column name" in r.message for r in caplog.records)

    def test_wrong_column_list_reports_phantom_and_undescribed(self, proc_mod, caplog):
        """Counts match but 5 of 7 returned names are not real columns."""
        proc, _ = proc_mod
        real = [f"c{i}" for i in range(11)]
        returned = ["c0", "c1", "ghost1", "ghost2", "ghost3", "ghost4", "ghost5"]
        contents = [f"d_{n}" for n in returned]
        resp = _Resp(returned, contents, source_columns=real)
        with caplog.at_level(logging.WARNING, logger=PROC_LOGGER):
            pairs = proc._resolve_column_content_pairs(resp, "cat.sch.t", real)
        # Only the two real matches are written, keyed on the real column names.
        assert pairs == [("c0", "d_c0"), ("c1", "d_c1")]
        msgs = " ".join(r.message for r in caplog.records)
        assert "not columns of the table" in msgs        # phantom reported
        assert "no description" in msgs                  # undescribed reported

    def test_case_and_whitespace_insensitive_match(self, proc_mod):
        proc, _ = proc_mod
        real = ["OrderId", "Amount"]
        resp = _Resp([" orderid ", "AMOUNT"], ["desc_id", "desc_amt"], source_columns=real)
        pairs = proc._resolve_column_content_pairs(resp, "cat.sch.t", real)
        # keyed on the canonical (real) names
        assert pairs == [("OrderId", "desc_id"), ("Amount", "desc_amt")]

    def test_no_source_columns_falls_back_to_positional(self, proc_mod):
        proc, _ = proc_mod
        resp = _Resp(["x", "y"], ["dx", "dy"], source_columns=None)
        pairs = proc._resolve_column_content_pairs(resp, "cat.sch.t", None)
        assert pairs == [("x", "dx"), ("y", "dy")]

    def test_no_source_columns_still_refuses_on_mismatch(self, proc_mod):
        proc, _ = proc_mod
        resp = _Resp(["x", "y", "z"], ["dx", "dy"], source_columns=None)
        assert proc._resolve_column_content_pairs(resp, "cat.sch.t", None) is None

    def test_missing_column_reported_when_model_drops_one(self, proc_mod, caplog):
        proc, _ = proc_mod
        real = ["a", "b", "c"]
        resp = _Resp(["a", "c"], ["da", "dc"], source_columns=real)
        with caplog.at_level(logging.WARNING, logger=PROC_LOGGER):
            pairs = proc._resolve_column_content_pairs(resp, "cat.sch.t", real)
        assert pairs == [("a", "da"), ("c", "dc")]  # 'b' left out, not misassigned
        assert any("no description" in r.message and "['b']" in r.message
                   for r in caplog.records)


# --------------------------------------------------------------------------- #
# append_column_rows -- wrapper wiring (row construction / refuse path)
# --------------------------------------------------------------------------- #
class TestAppendColumnRows:
    def test_refuse_writes_no_rows(self, proc_mod, base_config_kwargs):
        proc, _ = proc_mod
        cfg = _cfg(base_config_kwargs, mode="comment")
        resp = _Resp(["a", "b"], ["da"], source_columns=["a", "b"])  # mismatch
        out = proc.append_column_rows(cfg, [], "cat.sch.t", resp, "cat.sch.t")
        assert out == []

    def test_comment_rows_keyed_on_real_names(self, proc_mod, base_config_kwargs, monkeypatch):
        proc, _ = proc_mod
        # Make Row(...) return the kwargs dict so we can inspect the wiring
        monkeypatch.setattr(proc, "Row", lambda **kw: kw)
        cfg = _cfg(base_config_kwargs, mode="comment")
        real = ["OrderId", "Amount"]
        resp = _Resp(["orderid", "amount"], ["desc_id", "desc_amt"], source_columns=real)
        rows = proc.append_column_rows(cfg, [], "cat.sch.t", resp, "cat.sch.t")
        assert [(r["column_name"], r["column_content"]) for r in rows] == [
            ("OrderId", "desc_id"),
            ("Amount", "desc_amt"),
        ]

    def test_comment_phantom_names_not_written(self, proc_mod, base_config_kwargs, monkeypatch):
        proc, _ = proc_mod
        monkeypatch.setattr(proc, "Row", lambda **kw: kw)
        cfg = _cfg(base_config_kwargs, mode="comment")
        real = ["c0", "c1", "c2"]
        resp = _Resp(["c0", "ghost", "c2"], ["d0", "dg", "d2"], source_columns=real)
        rows = proc.append_column_rows(cfg, [], "cat.sch.t", resp, "cat.sch.t")
        names = [r["column_name"] for r in rows]
        assert names == ["c0", "c2"]           # ghost dropped, c1 undescribed
        assert "ghost" not in names

    def test_pi_rows_resolved_by_name(self, proc_mod, base_config_kwargs, monkeypatch):
        proc, _ = proc_mod
        monkeypatch.setattr(proc, "Row", lambda **kw: kw)
        cfg = _cfg(base_config_kwargs, mode="pi")
        real = ["ssn_col", "name_col"]
        # Model echoes DIFFERENT-CASE names in a DIFFERENT order than the table. Old positional
        # code would key rows on the model's cased echoes ("NAME_COL"/"SSN_COL"); name-keying must
        # key on the REAL columns ("ssn_col"/"name_col") and map content to them by name. This makes
        # the test discriminate (it fails on the old positional behavior), not just pass trivially.
        resp = _Resp(
            ["NAME_COL", "SSN_COL"],
            [{"classification": "pi", "type": "PERSON", "confidence": 0.80},
             {"classification": "pi", "type": "SSN", "confidence": 0.95}],
            source_columns=real,
        )
        rows = proc.append_column_rows(cfg, [], "cat.sch.t", resp, "cat.sch.t")
        by_name = {r["column_name"]: r for r in rows}
        assert set(by_name) == {"ssn_col", "name_col"}   # keyed on REAL names, not the model's case
        assert by_name["ssn_col"]["type"] == "SSN"
        assert by_name["name_col"]["type"] == "PERSON"

    def test_pi_mismatch_refused(self, proc_mod, base_config_kwargs):
        proc, _ = proc_mod
        cfg = _cfg(base_config_kwargs, mode="pi")
        contents = [{"classification": "pi", "type": "SSN", "confidence": 0.9}]
        resp = _Resp(["a", "b"], contents, source_columns=["a", "b"])  # 2 names, 1 content
        assert proc.append_column_rows(cfg, [], "cat.sch.t", resp, "cat.sch.t") == []


# --------------------------------------------------------------------------- #
# _source_columns PrivateAttr on the real response model
# --------------------------------------------------------------------------- #
class TestSourceColumnsPrivateAttr:
    def test_settable_and_hidden_from_schema_and_dump(self):
        from dbxmetagen.metadata_generator import CommentResponse

        r = CommentResponse.model_validate(
            {"table": "t", "columns": ["c1", "c2"], "column_contents": ["d1", "d2"]}
        )
        # default None, settable, readable
        assert r._source_columns is None
        r._source_columns = ["c1", "c2"]
        assert r._source_columns == ["c1", "c2"]
        # not part of the serialized data nor the JSON schema sent to the model
        assert "_source_columns" not in r.model_dump()
        assert "_source_columns" not in CommentResponse.model_json_schema().get("properties", {})

    def test_does_not_trip_extra_forbid(self):
        """Setting the private attr after construction must not conflict with extra=forbid."""
        from dbxmetagen.metadata_generator import PIResponse

        r = PIResponse.model_validate(
            {"table": "t", "columns": ["c1"],
             "column_contents": [{"classification": "pi", "type": "SSN", "confidence": 0.9}]}
        )
        r._source_columns = ["c1"]
        assert r._source_columns == ["c1"]

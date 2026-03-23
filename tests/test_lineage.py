"""Unit tests for lineage caching, prompt formatting, and include_lineage gating."""

import sys
import os
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.conftest import install_processing_stubs, uninstall_processing_stubs


# ---------------------------------------------------------------------------
# fetch_lineage caching
# ---------------------------------------------------------------------------

class TestFetchLineageCache:
    """Verify that fetch_lineage caches results per table_name."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self._saved = install_processing_stubs()
        import dbxmetagen.processing as proc
        self.proc = proc
        # Reset module-level state before each test
        proc._lineage_cache.clear()
        proc._lineage_unavailable = False
        proc._ext_metadata_unavailable = True  # skip extended_table_metadata path
        yield
        proc._lineage_cache.clear()
        proc._lineage_unavailable = False
        proc._ext_metadata_unavailable = False
        uninstall_processing_stubs(self._saved)

    def _mock_spark_with_lineage(self, upstream, downstream):
        spark = MagicMock()
        call_count = {"n": 0}

        def sql_side_effect(query):
            call_count["n"] += 1
            result = MagicMock()
            if "source_table_catalog" in query:
                result.collect.return_value = [(u,) for u in upstream]
            else:
                result.collect.return_value = [(d,) for d in downstream]
            return result

        spark.sql.side_effect = sql_side_effect
        return spark, call_count

    def test_cache_hit_avoids_sql(self):
        spark, call_count = self._mock_spark_with_lineage(
            ["cat.sch.src1"], ["cat.sch.dst1"]
        )
        r1 = self.proc.fetch_lineage(spark, "cat.sch.tbl")
        assert r1 is not None
        assert r1["upstream_tables"] == ["cat.sch.src1"]
        first_sql_count = call_count["n"]

        r2 = self.proc.fetch_lineage(spark, "cat.sch.tbl")
        assert r2 == r1
        assert call_count["n"] == first_sql_count  # no additional SQL

    def test_cache_stores_none_when_no_lineage(self):
        spark, call_count = self._mock_spark_with_lineage([], [])
        r1 = self.proc.fetch_lineage(spark, "cat.sch.tbl")
        assert r1 is None
        first_sql_count = call_count["n"]

        r2 = self.proc.fetch_lineage(spark, "cat.sch.tbl")
        assert r2 is None
        assert call_count["n"] == first_sql_count

    def test_cache_stores_none_for_bad_table_name(self):
        spark = MagicMock()
        r = self.proc.fetch_lineage(spark, "no_dots")
        assert r is None
        assert "no_dots" in self.proc._lineage_cache

    def test_different_tables_cached_independently(self):
        spark, _ = self._mock_spark_with_lineage(["cat.sch.src"], [])
        r1 = self.proc.fetch_lineage(spark, "cat.sch.tbl_a")
        r2 = self.proc.fetch_lineage(spark, "cat.sch.tbl_b")
        assert "cat.sch.tbl_a" in self.proc._lineage_cache
        assert "cat.sch.tbl_b" in self.proc._lineage_cache


# ---------------------------------------------------------------------------
# Prompt lineage formatting
# ---------------------------------------------------------------------------

class TestFormatLineageSection:
    def test_both_upstream_and_downstream(self):
        from dbxmetagen.prompts import _format_lineage_section
        lineage = {
            "upstream_tables": ["cat.sch.src1", "cat.sch.src2"],
            "downstream_tables": ["cat.sch.dst1"],
        }
        result = _format_lineage_section(lineage)
        assert "Upstream Tables (data sources): cat.sch.src1, cat.sch.src2" in result
        assert "Downstream Tables (consumers): cat.sch.dst1" in result

    def test_upstream_only(self):
        from dbxmetagen.prompts import _format_lineage_section
        result = _format_lineage_section({"upstream_tables": ["a.b.c"], "downstream_tables": []})
        assert "Upstream Tables" in result
        assert "Downstream Tables" not in result

    def test_empty_lineage(self):
        from dbxmetagen.prompts import _format_lineage_section
        result = _format_lineage_section({"upstream_tables": [], "downstream_tables": []})
        assert result == ""


class TestCommentPromptLineageFormatting:
    def test_lineage_appears_as_labeled_section(self):
        from dbxmetagen.prompts import CommentPrompt
        content = {
            "table_name": "cat.sch.tbl",
            "column_contents": {"columns": ["col_a"]},
            "lineage": {
                "upstream_tables": ["cat.sch.source"],
                "downstream_tables": ["cat.sch.target"],
            },
        }
        result = CommentPrompt._build_user_content(content, {})
        assert "Upstream Tables (data sources): cat.sch.source" in result
        assert "Downstream Tables (consumers): cat.sch.target" in result
        assert "'lineage'" not in result  # dict repr should not appear

    def test_no_lineage_key_no_section(self):
        from dbxmetagen.prompts import CommentPrompt
        content = {"table_name": "cat.sch.tbl", "column_contents": {}}
        result = CommentPrompt._build_user_content(content, {})
        assert "Upstream Tables" not in result
        assert "Downstream Tables" not in result


class TestPIPromptLineageFormatting:
    def test_lineage_appears_as_labeled_section(self):
        from dbxmetagen.prompts import PIPrompt
        prompt = MagicMock(spec=PIPrompt)
        prompt.deterministic_results = []
        prompt._build_pi_user_content = PIPrompt._build_pi_user_content.__get__(prompt, PIPrompt)
        content = {
            "table_name": "cat.sch.tbl",
            "lineage": {"upstream_tables": ["cat.sch.up"], "downstream_tables": []},
        }
        result = prompt._build_pi_user_content(content, {})
        assert "Upstream Tables (data sources): cat.sch.up" in result
        assert "'lineage'" not in result


class TestCommentNoDataPromptLineageFormatting:
    def test_lineage_appears_as_labeled_section(self):
        from dbxmetagen.prompts import CommentNoDataPrompt
        content = {
            "table_name": "cat.sch.tbl",
            "lineage": {"upstream_tables": [], "downstream_tables": ["cat.sch.down"]},
        }
        result = CommentNoDataPrompt._build_nodata_user_content(content, {})
        assert "Downstream Tables (consumers): cat.sch.down" in result
        assert "'lineage'" not in result


# ---------------------------------------------------------------------------
# include_lineage=false suppression
# ---------------------------------------------------------------------------

class TestIncludeLineageSuppression:
    def test_add_lineage_not_called_when_disabled(self):
        from dbxmetagen.config import MetadataConfig
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="cat",
            schema_name="sch",
            mode="comment",
            include_lineage=False,
        )
        assert config.include_lineage is False

    def test_add_lineage_enabled_when_true(self):
        from dbxmetagen.config import MetadataConfig
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="cat",
            schema_name="sch",
            mode="comment",
            include_lineage=True,
        )
        assert config.include_lineage is True

    def test_add_lineage_enabled_when_string_true(self):
        from dbxmetagen.config import MetadataConfig
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="cat",
            schema_name="sch",
            mode="comment",
            include_lineage="true",
        )
        assert config.include_lineage is True

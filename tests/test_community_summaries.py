"""Tests for community_summaries module."""

from unittest.mock import MagicMock, call, patch

from dbxmetagen.community_summaries import (
    build_prompt, community_id, discover_communities, generate_summaries_batch,
)


class TestCommunityId:
    def test_deterministic(self):
        assert community_id("finance", "accounting") == community_id("finance", "accounting")

    def test_case_insensitive(self):
        assert community_id("Finance", "Accounting") == community_id("finance", "accounting")

    def test_different_domains_differ(self):
        assert community_id("finance", "accounting") != community_id("clinical", "accounting")

    def test_length(self):
        assert len(community_id("x", "y")) == 12


class TestBuildPrompt:
    def test_includes_domain_and_tables(self):
        comm = {
            "domain": "finance",
            "subdomain": "accounting",
            "table_names": ["ledger", "journal"],
            "comments": ["General ledger entries", ""],
        }
        prompt = build_prompt(comm)
        assert "finance" in prompt
        assert "accounting" in prompt
        assert "- ledger: General ledger entries" in prompt
        assert "- journal" in prompt

    def test_truncates_long_comments_to_200_chars(self):
        comm = {
            "domain": "d",
            "subdomain": "s",
            "table_names": ["t"],
            "comments": ["x" * 300],
        }
        prompt = build_prompt(comm)
        assert "x" * 200 in prompt
        assert "x" * 201 not in prompt


class TestDiscoverCommunities:
    def test_returns_communities_from_spark(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            "domain": "clinical", "subdomain": "diagnostics",
            "table_names": ["lab_results", "imaging"],
            "comments": ["Lab data", "DICOM refs"],
            "table_count": 2,
        }[k]

        mock_df = MagicMock()
        mock_df.collect.return_value = [row]
        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_df

        result = discover_communities(mock_spark, "cat", "sch", min_tables=2)
        assert len(result) == 1
        assert result[0]["domain"] == "clinical"
        assert result[0]["table_count"] == 2

    def test_returns_empty_for_no_communities(self):
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_df

        result = discover_communities(mock_spark, "cat", "sch")
        assert result == []


class TestGenerateSummariesBatch:
    @patch("dbxmetagen.community_summaries.discover_communities", return_value=[])
    def test_returns_zero_when_no_communities(self, mock_discover):
        mock_spark = MagicMock()
        result = generate_summaries_batch(mock_spark, "cat", "sch")
        assert result == 0
        mock_spark.sql.assert_not_called()

    @patch("dbxmetagen.community_summaries.discover_communities")
    def test_writes_table_for_one_community(self, mock_discover):
        mock_discover.return_value = [{
            "community_id": "abc123",
            "domain": "finance",
            "subdomain": "accounting",
            "table_names": ["ledger", "journal"],
            "comments": ["General ledger", "Journal entries"],
            "table_count": 2,
        }]
        mock_result_df = MagicMock()
        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_result_df
        mock_spark.createDataFrame.return_value = MagicMock()

        result = generate_summaries_batch(mock_spark, "cat", "sch", model="test-model")
        assert result == 1

        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("CREATE TABLE" in s for s in sql_calls)
        assert any("AI_QUERY" in s for s in sql_calls)
        mock_result_df.write.format().mode().option().saveAsTable.assert_called_once()

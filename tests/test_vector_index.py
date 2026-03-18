"""Tests for vector_index module -- config properties, endpoint readiness, index retry."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from dbxmetagen.vector_index import VectorIndexConfig, VectorIndexBuilder


# ── VectorIndexConfig ─────────────────────────────────────────────────


class TestVectorIndexConfig:

    def test_fq_documents(self):
        c = VectorIndexConfig(catalog_name="cat", schema_name="sch")
        assert c.fq_documents == "cat.sch.metadata_documents"

    def test_fq_index(self):
        c = VectorIndexConfig(catalog_name="cat", schema_name="sch")
        assert c.fq_index == "cat.sch.metadata_vs_index"

    def test_fq_helper(self):
        c = VectorIndexConfig(catalog_name="cat", schema_name="sch")
        assert c.fq("my_table") == "cat.sch.my_table"

    def test_custom_suffix(self):
        c = VectorIndexConfig(catalog_name="cat", schema_name="sch", index_suffix="custom_idx")
        assert c.fq_index == "cat.sch.custom_idx"

    def test_custom_documents_table(self):
        c = VectorIndexConfig(catalog_name="cat", schema_name="sch", documents_table="docs")
        assert c.fq_documents == "cat.sch.docs"


# ── ensure_endpoint ───────────────────────────────────────────────────


class TestEnsureEndpoint:

    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_existing_endpoint_uses_sdk_wait(self, mock_wc_cls):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        ep = MagicMock()
        ep.endpoint_status.state = "ONLINE"
        ep.endpoint_status.message = None
        ep.num_indexes = 2
        mock_wc.vector_search_endpoints.get_endpoint.return_value = ep
        mock_wc.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.return_value = ep

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        result = builder.ensure_endpoint()

        assert result == "dbxmetagen-vs"
        mock_wc.vector_search_endpoints.create_endpoint.assert_not_called()
        mock_wc.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.assert_called_once_with("dbxmetagen-vs")

    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_creates_endpoint_when_missing(self, mock_wc_cls):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        mock_wc.vector_search_endpoints.get_endpoint.side_effect = Exception("not found")
        ep_online = MagicMock()
        ep_online.num_indexes = 0
        mock_wc.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.return_value = ep_online

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        result = builder.ensure_endpoint()

        assert result == "dbxmetagen-vs"
        mock_wc.vector_search_endpoints.create_endpoint.assert_called_once()
        mock_wc.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.assert_called_once()

    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_propagates_timeout_from_sdk_wait(self, mock_wc_cls):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        ep = MagicMock()
        ep.endpoint_status.state = "PROVISIONING"
        ep.endpoint_status.message = "still starting"
        ep.num_indexes = 0
        mock_wc.vector_search_endpoints.get_endpoint.return_value = ep
        mock_wc.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.side_effect = \
            TimeoutError("timed out")

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        with pytest.raises(TimeoutError):
            builder.ensure_endpoint()


# ── ensure_index ──────────────────────────────────────────────────────


class TestEnsureIndex:

    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_returns_existing_index(self, mock_wc_cls):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        idx = MagicMock()
        idx.status.ready = True
        idx.status.message = "ok"
        idx.status.indexed_row_count = 500
        mock_wc.vector_search_indexes.get_index.return_value = idx

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        result = builder.ensure_index()

        assert result == "cat.sch.metadata_vs_index"
        mock_wc.vector_search_indexes.create_index.assert_not_called()

    @patch("dbxmetagen.vector_index.time")
    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_retries_on_not_ready(self, mock_wc_cls, mock_time):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        mock_wc.vector_search_indexes.get_index.side_effect = Exception("not found")
        mock_wc.vector_search_indexes.create_index.side_effect = [
            Exception("BadRequest: Vector search endpoint dbxmetagen-vs is not ready yet."),
            None,  # succeeds on second attempt
        ]
        mock_time.sleep = MagicMock()

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        result = builder.ensure_index()

        assert result == "cat.sch.metadata_vs_index"
        assert mock_wc.vector_search_indexes.create_index.call_count == 2
        mock_time.sleep.assert_called_once_with(30)

    @patch("dbxmetagen.vector_index.time")
    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_raises_after_max_retries(self, mock_wc_cls, mock_time):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        mock_wc.vector_search_indexes.get_index.side_effect = Exception("not found")
        mock_wc.vector_search_indexes.create_index.side_effect = \
            Exception("BadRequest: Vector search endpoint dbxmetagen-vs is not ready yet.")
        mock_time.sleep = MagicMock()

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        with pytest.raises(Exception, match="not ready"):
            builder.ensure_index()
        assert mock_wc.vector_search_indexes.create_index.call_count == 5

    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_raises_non_retryable_error_immediately(self, mock_wc_cls):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        mock_wc.vector_search_indexes.get_index.side_effect = Exception("not found")
        mock_wc.vector_search_indexes.create_index.side_effect = \
            Exception("PermissionDenied: insufficient privileges")

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        with pytest.raises(Exception, match="insufficient privileges"):
            builder.ensure_index()
        assert mock_wc.vector_search_indexes.create_index.call_count == 1

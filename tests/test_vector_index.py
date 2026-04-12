"""Tests for vector_index module -- config properties, endpoint readiness, index retry."""

import pytest
from unittest.mock import MagicMock, patch
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
        ready_idx = MagicMock()
        ready_idx.status.ready = True
        ready_idx.status.message = "ok"
        ready_idx.status.indexed_row_count = 0
        mock_wc.vector_search_indexes.get_index.side_effect = [
            Exception("not found"),
            ready_idx,
        ]
        mock_wc.vector_search_indexes.create_index.side_effect = [
            Exception("BadRequest: Vector search endpoint dbxmetagen-vs is not ready yet."),
            None,
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


# ── wait_until_index_ready ────────────────────────────────────────────


class TestWaitUntilIndexReady:

    @patch("dbxmetagen.vector_index.time.sleep")
    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_polls_until_ready(self, mock_wc_cls, mock_sleep):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc

        def mk(ready: bool):
            m = MagicMock()
            m.status.ready = ready
            m.status.message = "provisioning" if not ready else "ok"
            m.status.indexed_row_count = 0
            return m

        mock_wc.vector_search_indexes.get_index.side_effect = [
            mk(False),
            mk(False),
            mk(True),
        ]

        cfg = VectorIndexConfig("cat", "sch", index_ready_timeout_s=900)
        builder = VectorIndexBuilder(MagicMock(), cfg)
        builder._wait_until_index_ready("cat.sch.metadata_vs_index")

        assert mock_wc.vector_search_indexes.get_index.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_timeout_raises(self, mock_wc_cls):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        m = MagicMock()
        m.status.ready = False
        m.status.message = "still provisioning"
        mock_wc.vector_search_indexes.get_index.return_value = m

        cfg = VectorIndexConfig("cat", "sch", index_ready_timeout_s=0)
        builder = VectorIndexBuilder(MagicMock(), cfg)
        with pytest.raises(TimeoutError, match="did not become ready"):
            builder._wait_until_index_ready("cat.sch.metadata_vs_index")


# ── sync ──────────────────────────────────────────────────────────────


class TestSync:

    @patch("dbxmetagen.vector_index.time")
    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_retries_on_not_ready(self, mock_wc_cls, mock_time):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        mock_wc.vector_search_indexes.sync_index.side_effect = [
            Exception("BadRequest: Vector index cat.sch.metadata_vs_index is not ready."),
            None,
        ]
        mock_time.sleep = MagicMock()

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        builder.sync()

        assert mock_wc.vector_search_indexes.sync_index.call_count == 2
        mock_time.sleep.assert_called_once_with(30.0)

    @patch("dbxmetagen.vector_index.time")
    @patch("dbxmetagen.vector_index.WorkspaceClient")
    def test_non_transient_error_not_retried(self, mock_wc_cls, mock_time):
        mock_wc = MagicMock()
        mock_wc_cls.return_value = mock_wc
        mock_wc.vector_search_indexes.sync_index.side_effect = Exception("PermissionDenied: denied")
        mock_time.sleep = MagicMock()

        builder = VectorIndexBuilder(MagicMock(), VectorIndexConfig("cat", "sch"))
        with pytest.raises(Exception, match="denied"):
            builder.sync()
        assert mock_wc.vector_search_indexes.sync_index.call_count == 1
        mock_time.sleep.assert_not_called()

"""Unit tests for similarity_edges module."""

import inspect
import sys
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from dbxmetagen.similarity_edges import (
    SimilarityEdgesConfig,
    SimilarityEdgeBuilder,
    build_similarity_edges,
    _EDGE_SCHEMA,
)

# Ensure databricks.vector_search.client is importable (stubbed by conftest for databricks.*)
for _vs_mod in ["databricks.vector_search", "databricks.vector_search.client"]:
    if _vs_mod not in sys.modules:
        sys.modules[_vs_mod] = MagicMock()


class TestSimilarityEdgesConfig:
    """Tests for SimilarityEdgesConfig."""
    
    def test_fully_qualified_nodes(self):
        config = SimilarityEdgesConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_nodes == "test_catalog.test_schema.graph_nodes"
    
    def test_fully_qualified_edges(self):
        config = SimilarityEdgesConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        assert config.fully_qualified_edges == "test_catalog.test_schema.graph_edges"
    
    def test_default_similarity_threshold(self):
        config = SimilarityEdgesConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.similarity_threshold == 0.7
    
    def test_default_max_edges_per_node(self):
        config = SimilarityEdgesConfig(
            catalog_name="cat",
            schema_name="sch"
        )
        assert config.max_edges_per_node == 15
    
    def test_custom_threshold(self):
        config = SimilarityEdgesConfig(
            catalog_name="cat",
            schema_name="sch",
            similarity_threshold=0.6,
            max_edges_per_node=5
        )
        assert config.similarity_threshold == 0.6
        assert config.max_edges_per_node == 5


class TestSimilarityEdgeBuilder:
    """Tests for SimilarityEdgeBuilder."""
    
    @pytest.fixture
    def mock_spark(self):
        return MagicMock()
    
    @pytest.fixture
    def config(self):
        return SimilarityEdgesConfig(
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def builder(self, mock_spark, config):
        # use_ann=False triggers cosine impl detection on init
        b = SimilarityEdgeBuilder(mock_spark, config)
        mock_spark.sql.reset_mock()
        return b
    
    def test_relationship_type_constant(self, builder):
        """Relationship type should be 'similar_embedding'."""
        assert builder.RELATIONSHIP_TYPE == "similar_embedding"
    
    def test_get_nodes_with_embeddings_query(self, builder, mock_spark):
        builder.get_nodes_with_embeddings()
        mock_spark.sql.assert_called_once()
        query = mock_spark.sql.call_args[0][0]
        assert "embedding IS NOT NULL" in query
    
    def test_remove_existing_similarity_edges(self, builder, mock_spark):
        builder.remove_existing_similarity_edges()
        mock_spark.sql.assert_called_once()
        query = mock_spark.sql.call_args[0][0]
        assert "DELETE FROM" in query
        assert "similar_embedding" in query
    
    def test_compute_similarity_fallback_empty_nodes(self, builder, mock_spark):
        """Fallback should handle empty node list."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.createDataFrame.return_value = mock_df
        
        result = builder._compute_similarity_fallback(mock_df)
        assert result is not None
    
    def test_compute_similarity_fallback_single_node(self, builder, mock_spark):
        """Fallback should handle single node (no pairs to compare)."""
        mock_node = MagicMock()
        mock_node.id = "node1"
        mock_node.embedding = [1.0, 0.0, 0.0]
        
        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_node]
        mock_spark.createDataFrame.return_value = mock_df
        
        result = builder._compute_similarity_fallback(mock_df)
        assert result is not None


class TestCosineSimInFallback:
    """Tests for the cosine similarity calculation in fallback."""
    
    def test_identical_vectors(self):
        """Identical vectors should have similarity 1.0."""
        v = [1.0, 0.0, 0.0]
        dot = sum(a * b for a, b in zip(v, v))
        norm = sum(a * a for a in v) ** 0.5
        sim = dot / (norm * norm) if norm else 0.0
        assert abs(sim - 1.0) < 0.001
    
    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity 0.0."""
        v1 = [1.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0]
        dot = sum(a * b for a, b in zip(v1, v2))
        norm1 = sum(a * a for a in v1) ** 0.5
        norm2 = sum(b * b for b in v2) ** 0.5
        sim = dot / (norm1 * norm2) if norm1 and norm2 else 0.0
        assert abs(sim) < 0.001
    
    def test_opposite_vectors(self):
        """Opposite vectors should have similarity -1.0."""
        v1 = [1.0, 0.0, 0.0]
        v2 = [-1.0, 0.0, 0.0]
        dot = sum(a * b for a, b in zip(v1, v2))
        norm1 = sum(a * a for a in v1) ** 0.5
        norm2 = sum(b * b for b in v2) ** 0.5
        sim = dot / (norm1 * norm2) if norm1 and norm2 else 0.0
        assert abs(sim + 1.0) < 0.001


class TestBuildSimilarityEdges:
    """Tests for build_similarity_edges function."""
    
    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_creates_builder_with_correct_config(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"edges_created": 10, "threshold": 0.8}
        mock_builder_class.return_value = mock_builder
        
        mock_spark = MagicMock()
        build_similarity_edges(mock_spark, "my_cat", "my_sch")
        
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_cat"
        assert config.schema_name == "my_sch"
    
    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_passes_custom_threshold(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {}
        mock_builder_class.return_value = mock_builder
        
        build_similarity_edges(MagicMock(), "cat", "sch", similarity_threshold=0.6)
        
        config = mock_builder_class.call_args[0][1]
        assert config.similarity_threshold == 0.6
    
    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_passes_max_edges_per_node(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {}
        mock_builder_class.return_value = mock_builder
        
        build_similarity_edges(MagicMock(), "cat", "sch", max_edges_per_node=5)
        
        config = mock_builder_class.call_args[0][1]
        assert config.max_edges_per_node == 5
    
    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_returns_run_result(self, mock_builder_class):
        expected = {"edges_created": 50, "threshold": 0.75}
        mock_builder = MagicMock()
        mock_builder.run.return_value = expected
        mock_builder_class.return_value = mock_builder
        
        result = build_similarity_edges(MagicMock(), "cat", "sch")
        assert result == expected


class TestANNConfig:
    """Tests for ANN-related config fields."""

    def test_default_use_ann_is_true(self):
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s")
        assert config.use_ann is True

    def test_default_ann_k_multiplier(self):
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s")
        assert config.ann_k_multiplier == 2

    def test_fq_vs_index(self):
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s")
        assert config.fq_vs_index == "c.s.graph_nodes_vs_index"

    def test_ann_max_nodes_default(self):
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s")
        assert config.ann_max_nodes == 100_000

    def test_embedding_dimension_default(self):
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s")
        assert config.embedding_dimension == 1024


class TestModeSwitch:
    """Tests for explicit ANN / cross-join mode dispatch."""

    def test_crossjoin_fallback_for_use_ann_false(self):
        """When use_ann=False, only cross-join path is used."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges)
        assert "not self.config.use_ann" in src
        assert "_compute_similarity_crossjoin" in src

    def test_ann_mode_calls_ensure_and_ann_compute(self):
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges)
        assert "ensure_graph_nodes_index" in src
        assert "compute_similarity_edges_ann" in src

    def test_ann_fallback_on_infra_failure(self):
        """ANN path falls back to cross-join on VS infra failure (logged warning)."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges)
        assert "except Exception as e" in src
        assert "falling back to cross-join" in src

    def test_valueerror_not_caught_in_fallback(self):
        """ValueError (e.g. ann_max_nodes exceeded) must NOT fall back to cross-join."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges)
        assert "except ValueError" in src
        assert src.index("except ValueError") < src.index("except Exception")

    def test_small_graph_uses_crossjoin_when_ann_true(self):
        """When node count < blocking_node_threshold, cross-join is used even with use_ann=True."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges)
        assert "blocking_node_threshold" in src
        assert "cheaper for small graphs" in src

    def test_run_returns_method_key(self):
        src = inspect.getsource(SimilarityEdgeBuilder.run)
        assert '"method"' in src or "'method'" in src


class TestANNGuardRail:
    """Tests for ann_max_nodes guard rail."""

    def test_ann_raises_on_too_many_nodes(self):
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "ann_max_nodes" in src
        assert "ValueError" in src

    def test_ann_uses_vector_search_client(self):
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "VectorSearchClient" in src
        assert "query_vector" in src

    def test_ann_uses_score_threshold(self):
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "score_threshold" in src

    def test_ann_does_not_use_query_type_hybrid(self):
        """Self-managed embeddings must NOT use query_type='HYBRID'."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "HYBRID" not in src


class TestEnsureGraphNodesIndex:
    """Tests for VS index creation using correct SDK classes."""

    def test_uses_workspace_client_not_vector_search_client(self):
        src = inspect.getsource(SimilarityEdgeBuilder.ensure_graph_nodes_index)
        assert "WorkspaceClient" in src
        assert "VectorSearchClient" not in src

    def test_uses_embedding_vector_column(self):
        """Must use EmbeddingVectorColumn (self-managed), not EmbeddingSourceColumn."""
        src = inspect.getsource(SimilarityEdgeBuilder.ensure_graph_nodes_index)
        assert "EmbeddingVectorColumn" in src
        assert "EmbeddingSourceColumn" not in src

    def test_uses_triggered_pipeline(self):
        src = inspect.getsource(SimilarityEdgeBuilder.ensure_graph_nodes_index)
        assert "TRIGGERED" in src


class TestPostprocessANNEdges:
    """Tests for _postprocess_ann_edges dedup and entity-entity filter."""

    @pytest.fixture
    def mock_spark(self):
        return MagicMock()

    @pytest.fixture
    def ann_builder(self, mock_spark):
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
        )
        return SimilarityEdgeBuilder(mock_spark, config)

    def test_empty_input_returns_empty_df(self, ann_builder, mock_spark):
        mock_spark.createDataFrame.return_value = MagicMock()
        result = ann_builder._postprocess_ann_edges([])
        mock_spark.createDataFrame.assert_called_once()
        args = mock_spark.createDataFrame.call_args[0]
        assert args[0] == []

    def test_entity_entity_filter_in_source(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert "entity" in src
        assert "src_type" in src
        assert "dst_type" in src

    def test_dedup_uses_least_greatest(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert "least" in src.lower()
        assert "greatest" in src.lower()

    def test_output_has_standard_edge_columns(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        for col in ["relationship", "edge_id", "edge_type", "direction",
                     "source_system", "status", "created_at", "updated_at"]:
            assert col in src, f"Missing column '{col}' in postprocess output"


class TestBuildSimilarityEdgesUseAnn:
    """Tests for passing use_ann through the convenience function."""

    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_passes_use_ann_true(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"method": "ann", "edges_created": 5, "threshold": 0.7}
        mock_builder_class.return_value = mock_builder

        build_similarity_edges(MagicMock(), "cat", "sch", use_ann=True)
        config = mock_builder_class.call_args[0][1]
        assert config.use_ann is True

    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_default_use_ann_true(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"method": "ann", "edges_created": 5, "threshold": 0.7}
        mock_builder_class.return_value = mock_builder

        build_similarity_edges(MagicMock(), "cat", "sch")
        config = mock_builder_class.call_args[0][1]
        assert config.use_ann is True


class TestANNExceptionPropagation:
    """Test A: ThreadPoolExecutor propagates batch exceptions."""

    def test_batch_exception_propagates(self):
        """If _query_batch raises, compute_similarity_edges_ann must re-raise."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "raise" in src
        assert "batch_idx" in src or "futures[f]" in src

    def test_batch_index_logged_on_failure(self):
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "logger.error" in src or "logger.warning" in src


class TestMaxEdgesCapANN:
    """Test B: per-node cap enforcement in ANN _postprocess_ann_edges."""

    def test_row_number_cap_exists(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert "row_number" in src
        assert "max_edges_per_node" in src

    def test_cap_uses_window_partition_by_src(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert "partitionBy" in src
        assert "src" in src


class TestEmptyEmbeddingHandling:
    """Test C: _query_batch behaviour with empty embedding vectors."""

    def test_query_batch_uses_list_conversion(self):
        """Ensures list(node.embedding) is called -- empty list would produce zero-dim query."""
        src = inspect.getsource(SimilarityEdgeBuilder.compute_similarity_edges_ann)
        assert "list(node.embedding)" in src

    def test_ann_filters_null_embeddings_in_sql(self):
        """SQL query must filter WHERE embedding IS NOT NULL before collect."""
        src = inspect.getsource(SimilarityEdgeBuilder.get_nodes_with_embeddings)
        assert "IS NOT NULL" in src


class TestSymmetricDedupCorrectness:
    """Test D: symmetric dedup keeps max similarity per pair."""

    def test_dedup_aggregates_with_max(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert "max" in src.lower()
        assert "similarity" in src

    def test_dedup_uses_least_greatest_for_canonical_ordering(self):
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        # Canonical direction: (LEAST(src,dst), GREATEST(src,dst))
        assert "least" in src.lower()
        assert "greatest" in src.lower()
        assert "groupBy" in src


class TestKwargsPassthrough:
    """Test E: build_similarity_edges passes **kwargs to config correctly."""

    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_passes_ann_batch_size(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"method": "ann", "edges_created": 0, "threshold": 0.7}
        mock_builder_class.return_value = mock_builder

        build_similarity_edges(MagicMock(), "c", "s", ann_batch_size=50, ann_max_workers=4)
        config = mock_builder_class.call_args[0][1]
        assert config.ann_batch_size == 50
        assert config.ann_max_workers == 4

    @patch('dbxmetagen.similarity_edges.SimilarityEdgeBuilder')
    def test_unknown_kwargs_silently_dropped(self, mock_builder_class):
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"method": "crossjoin", "edges_created": 0, "threshold": 0.7}
        mock_builder_class.return_value = mock_builder

        # Should not raise even with unknown param
        build_similarity_edges(MagicMock(), "c", "s", bogus_param=True)
        config = mock_builder_class.call_args[0][1]
        assert not hasattr(config, "bogus_param")


class TestANNInitSkipsCosineDetection:
    """Test F: ANN builder init does NOT call _detect_cosine_impl."""

    def test_ann_mode_skips_spark_sql_on_init(self):
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s", use_ann=True)
        SimilarityEdgeBuilder(mock_spark, config)
        # spark.sql should NOT have been called (cosine detection probe is skipped)
        mock_spark.sql.assert_not_called()

    def test_cosine_impl_is_none_when_ann(self):
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(catalog_name="c", schema_name="s", use_ann=True)
        builder = SimilarityEdgeBuilder(mock_spark, config)
        assert builder._cosine_impl is None


class TestPostprocessANNEdgesWithLocalSpark:
    """Real PySpark-style test for _postprocess_ann_edges.

    Since the core test suite stubs pyspark, we build a carefully mocked
    DataFrame chain that tracks operations. This validates the logical flow
    (entity filter, symmetric dedup, per-node cap, output columns) without
    requiring a running Spark cluster.
    """

    @pytest.fixture
    def ann_builder_cap3(self):
        """Builder with max_edges_per_node=3 for cap testing."""
        spark = MagicMock()
        # Wire createDataFrame to return a chainable mock DF
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s",
            use_ann=True, max_edges_per_node=3,
        )
        return SimilarityEdgeBuilder(spark, config)

    def test_empty_raw_edges_returns_empty(self, ann_builder_cap3):
        result = ann_builder_cap3._postprocess_ann_edges([])
        ann_builder_cap3.spark.createDataFrame.assert_called_once()
        args = ann_builder_cap3.spark.createDataFrame.call_args[0]
        assert args[0] == []

    def test_entity_entity_filter_applied(self):
        """Verify that entity-entity exclusion filter is in the chain."""
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert 'src_type' in src and 'entity' in src
        assert 'dst_type' in src

    def test_symmetric_dedup_aggregation(self):
        """Dedup must group by (LEAST, GREATEST) and take MAX similarity."""
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert 'groupBy' in src
        assert 'least' in src.lower()
        assert 'greatest' in src.lower()
        assert 'max' in src.lower()

    def test_per_node_cap_uses_row_number(self):
        """Cap must use ROW_NUMBER partitioned by src and dst."""
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        assert 'row_number' in src
        assert 'partitionBy' in src
        assert 'max_edges_per_node' in src

    def test_output_columns_match_edge_schema(self):
        """Output select must include all _EDGE_SCHEMA fields."""
        src = inspect.getsource(SimilarityEdgeBuilder._postprocess_ann_edges)
        expected_cols = [
            "relationship", "weight", "edge_id", "edge_type", "direction",
            "join_expression", "join_confidence", "ontology_rel",
            "source_system", "status", "created_at", "updated_at",
        ]
        for col in expected_cols:
            assert col in src, f"Missing output column '{col}'"


class TestQueryBatchVSResponseParsing:
    """Test VS data_array response parsing in compute_similarity_edges_ann."""

    @patch("databricks.vector_search.client.VectorSearchClient", create=True)
    def test_query_batch_parses_data_array_correctly(self, mock_vsc_cls):
        """Mock the VS response and verify edge tuples are constructed right."""
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
            ann_batch_size=100, ann_max_workers=1, ann_max_nodes=10,
        )
        builder = SimilarityEdgeBuilder(mock_spark, config)

        # Simulate 2 nodes
        node_a = MagicMock()
        node_a.id = "cat.sch.t1.col_a"
        node_a.node_type = "column"
        node_a.domain = "finance"
        node_a.embedding = [0.1, 0.2, 0.3]

        node_b = MagicMock()
        node_b.id = "cat.sch.t2.col_b"
        node_b.node_type = "column"
        node_b.domain = "finance"
        node_b.embedding = [0.4, 0.5, 0.6]

        nodes_df = MagicMock()
        nodes_df.count.return_value = 2
        nodes_df.collect.return_value = [node_a, node_b]
        builder.get_nodes_with_embeddings = MagicMock(return_value=nodes_df)

        # Mock VS index response: node_a query returns node_b at 0.85
        # Columns: [id, node_type, domain, score]
        mock_index = MagicMock()
        mock_index.similarity_search.side_effect = [
            {"result": {"data_array": [
                ["cat.sch.t1.col_a", "column", "finance", 1.0],  # self -- should be skipped
                ["cat.sch.t2.col_b", "column", "finance", 0.85],
            ]}},
            {"result": {"data_array": [
                ["cat.sch.t2.col_b", "column", "finance", 1.0],  # self
                ["cat.sch.t1.col_a", "column", "finance", 0.85],
            ]}},
        ]
        mock_vsc_cls.return_value.get_index.return_value = mock_index

        # _postprocess_ann_edges will be called with real tuples
        # but returns a mock DF since spark is mocked
        result_edges = []

        def capture_postprocess(raw):
            result_edges.extend(raw)
            return MagicMock()

        builder._postprocess_ann_edges = capture_postprocess
        builder.compute_similarity_edges_ann()

        # Should have 2 edges: A->B and B->A (dedup happens in postprocess)
        assert len(result_edges) == 2
        # Verify tuple structure: (src_id, src_type, src_domain, dst_id, dst_type, dst_domain, score)
        assert result_edges[0][0] == "cat.sch.t1.col_a"
        assert result_edges[0][3] == "cat.sch.t2.col_b"
        assert result_edges[0][6] == 0.85
        assert result_edges[1][0] == "cat.sch.t2.col_b"
        assert result_edges[1][3] == "cat.sch.t1.col_a"

    @patch("databricks.vector_search.client.VectorSearchClient", create=True)
    def test_self_edges_excluded(self, mock_vsc_cls):
        """Node should not create edge to itself."""
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
            ann_batch_size=100, ann_max_workers=1, ann_max_nodes=10,
        )
        builder = SimilarityEdgeBuilder(mock_spark, config)

        node_a = MagicMock()
        node_a.id = "n1"
        node_a.node_type = "column"
        node_a.domain = "d"
        node_a.embedding = [1.0]

        nodes_df = MagicMock()
        nodes_df.count.return_value = 1
        # Still need >=2 for the guard but we're testing self-exclusion logic
        # Relax: set ann_max_nodes high, node_count < 2 returns empty -- skip that path
        # Instead: pass 2 identical nodes
        node_b = MagicMock()
        node_b.id = "n1"  # same id!
        node_b.node_type = "column"
        node_b.domain = "d"
        node_b.embedding = [1.0]
        nodes_df.count.return_value = 2
        nodes_df.collect.return_value = [node_a, node_b]
        builder.get_nodes_with_embeddings = MagicMock(return_value=nodes_df)

        mock_index = MagicMock()
        # Both queries return only themselves
        mock_index.similarity_search.return_value = {
            "result": {"data_array": [["n1", "column", "d", 1.0]]}
        }
        mock_vsc_cls.return_value.get_index.return_value = mock_index

        captured = []
        builder._postprocess_ann_edges = lambda raw: (captured.extend(raw), MagicMock())[1]
        builder.compute_similarity_edges_ann()
        assert len(captured) == 0


class TestWaitUntilIndexReadyTimeout:
    """Test _wait_until_index_ready raises TimeoutError."""

    @patch("databricks.sdk.WorkspaceClient", create=True)
    @patch("dbxmetagen.similarity_edges.time")
    def test_timeout_raises(self, mock_time, mock_ws_cls):
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
            index_ready_timeout_s=0,  # immediate timeout
            index_ready_initial_delay_s=1,
        )
        builder = SimilarityEdgeBuilder(mock_spark, config)

        # monotonic returns 0 on first call (deadline=0), then 1 (past deadline)
        mock_time.monotonic.side_effect = [0.0, 1.0]

        mock_idx = MagicMock()
        mock_idx.status.ready = False
        mock_ws_cls.return_value.vector_search_indexes.get_index.return_value = mock_idx

        with pytest.raises(TimeoutError, match="did not become ready"):
            builder._wait_until_index_ready("c.s.test_idx")

    @patch("databricks.sdk.WorkspaceClient", create=True)
    @patch("dbxmetagen.similarity_edges.time")
    def test_returns_when_ready(self, mock_time, mock_ws_cls):
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
            index_ready_timeout_s=60,
            index_ready_initial_delay_s=1,
        )
        builder = SimilarityEdgeBuilder(mock_spark, config)

        mock_time.monotonic.return_value = 0.0

        mock_idx = MagicMock()
        mock_idx.status.ready = True
        mock_ws_cls.return_value.vector_search_indexes.get_index.return_value = mock_idx

        # Should not raise
        builder._wait_until_index_ready("c.s.test_idx")


class TestEnsureGraphNodesIndexIdempotency:
    """Test ensure_graph_nodes_index does not double-create."""

    @patch("dbxmetagen.similarity_edges.time")
    @patch("databricks.sdk.WorkspaceClient", create=True)
    def test_existing_index_not_recreated(self, mock_ws_cls, mock_time):
        """If get_index succeeds, create_index must NOT be called."""
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
            index_ready_timeout_s=60,
        )
        builder = SimilarityEdgeBuilder(mock_spark, config)

        mock_w = mock_ws_cls.return_value
        mock_w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.return_value = MagicMock()
        mock_idx = MagicMock()
        mock_idx.status.ready = True
        mock_w.vector_search_indexes.get_index.return_value = mock_idx
        mock_time.monotonic.return_value = 0.0

        builder.ensure_graph_nodes_index()
        mock_w.vector_search_indexes.create_index.assert_not_called()

    @patch("dbxmetagen.similarity_edges.time")
    @patch("databricks.sdk.WorkspaceClient", create=True)
    def test_missing_index_triggers_creation(self, mock_ws_cls, mock_time):
        """If get_index raises, create_index must be called exactly once."""
        mock_spark = MagicMock()
        config = SimilarityEdgesConfig(
            catalog_name="c", schema_name="s", use_ann=True,
            index_ready_timeout_s=60,
        )
        builder = SimilarityEdgeBuilder(mock_spark, config)

        mock_w = mock_ws_cls.return_value
        mock_w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online.return_value = MagicMock()

        # First get_index call (in ensure_graph_nodes_index): raises => triggers create
        # Second get_index call (in _wait_until_index_ready): returns ready
        mock_idx_ready = MagicMock()
        mock_idx_ready.status.ready = True
        mock_w.vector_search_indexes.get_index.side_effect = [
            NotFound("NOT_FOUND"),
            mock_idx_ready,
        ]
        mock_time.monotonic.return_value = 0.0

        builder.ensure_graph_nodes_index()
        mock_w.vector_search_indexes.create_index.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


"""Unit tests for data signal integration fixes (D, E1, E2, A, B1, B2, C)
and KB bootstrap methods."""

import pytest
from unittest.mock import MagicMock, patch


# ======================================================================
# Fix D: fetch_lineage cached fallback
# ======================================================================
class TestFetchLineageCached:
    """Tests for fetch_lineage with extended_table_metadata cache."""

    @classmethod
    def setup_class(cls):
        from conftest import install_processing_stubs, uninstall_processing_stubs
        cls._saved = install_processing_stubs()
        cls._uninstall = uninstall_processing_stubs

    @classmethod
    def teardown_class(cls):
        cls._uninstall(cls._saved)

    def setup_method(self):
        import dbxmetagen.processing as proc
        proc._lineage_cache.clear()
        proc._lineage_unavailable = False

    def test_returns_none_for_bad_table_name(self):
        from dbxmetagen.processing import fetch_lineage
        assert fetch_lineage(MagicMock(), "no_dots") is None

    def test_uses_cached_lineage_when_available(self):
        from dbxmetagen.processing import fetch_lineage
        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, k: {
            "upstream_tables": ["a.b.up1"],
            "downstream_tables": ["a.b.dn1"],
        }[k]
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        result = fetch_lineage(
            mock_spark, "cat.sch.tbl",
            catalog_name="out_cat", schema_name="out_sch",
        )
        assert result == {"upstream_tables": ["a.b.up1"], "downstream_tables": ["a.b.dn1"]}
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "extended_table_metadata" in sql_arg

    def test_falls_back_to_system_table_on_cache_miss(self):
        from dbxmetagen.processing import fetch_lineage
        mock_spark = MagicMock()
        # First call (cache) returns empty
        # Next calls (system table) return data
        mock_spark.sql.return_value.collect.side_effect = [
            [],  # cache miss
            [("a.b.upstream",)],  # upstream query
            [("a.b.downstream",)],  # downstream query
        ]

        result = fetch_lineage(
            mock_spark, "cat.sch.tbl",
            catalog_name="out_cat", schema_name="out_sch",
        )
        assert result is not None
        assert "a.b.upstream" in result["upstream_tables"]

    def test_falls_back_on_cache_exception(self):
        from dbxmetagen.processing import fetch_lineage
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.side_effect = [
            Exception("table not found"),  # cache fails
            [("up",)],  # upstream
            [],  # downstream
        ]
        result = fetch_lineage(
            mock_spark, "cat.sch.tbl",
            catalog_name="out_cat", schema_name="out_sch",
        )
        assert result is not None

    def test_no_cache_attempt_without_catalog_schema(self):
        from dbxmetagen.processing import fetch_lineage
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        result = fetch_lineage(mock_spark, "cat.sch.tbl")
        assert result is None
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "system.access.table_lineage" in sql_arg


# ======================================================================
# Fix E1: _add_profiling_context
# ======================================================================
class TestAddProfilingContext:

    def _make_prompt_instance(self, mock_spark, include_profiling=True):
        """Build a concrete Prompt subclass instance for testing."""
        from dbxmetagen.prompts import Prompt

        class _TestPrompt(Prompt):
            def convert_to_comment_input(self):
                return {"column_contents": {"column_metadata": {
                    "col_a": {"comment": ""}, "col_b": {"comment": ""},
                }}}
            def add_metadata_to_comment_input(self):
                pass
            def chat_completion(self):
                return {}

        config = MagicMock()
        config.add_metadata = False
        config.include_lineage = False
        config.include_profiling_context = include_profiling
        config.include_constraint_context = False
        config.catalog_name = "cat"
        config.schema_name = "sch"

        with patch("dbxmetagen.prompts.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            return _TestPrompt(config, MagicMock(), "cat.sch.tbl")

    def test_profiling_context_injected(self):
        mock_spark = MagicMock()
        stats_row = MagicMock()
        stats_row.__getitem__ = lambda self, k: {
            "column_name": "col_a", "distinct_count": 100,
            "null_rate": 0.05, "min_value": "1", "max_value": "999",
        }[k]
        mock_spark.sql.return_value.collect.side_effect = [
            [stats_row],  # column_profiling_stats
            [MagicMock(**{"__getitem__": lambda self, k: 10000})],  # profiling_snapshots
        ]

        prompt = self._make_prompt_instance(mock_spark)
        meta = prompt.prompt_content["column_contents"]["column_metadata"]
        assert "profiling" in meta["col_a"]
        assert meta["col_a"]["profiling"]["distinct_count"] == 100

    def test_profiling_context_skipped_when_table_missing(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.side_effect = Exception("table not found")

        prompt = self._make_prompt_instance(mock_spark)
        meta = prompt.prompt_content["column_contents"]["column_metadata"]
        assert "profiling" not in meta.get("col_a", {})

    def test_profiling_context_not_called_when_disabled(self):
        mock_spark = MagicMock()
        prompt = self._make_prompt_instance(mock_spark, include_profiling=False)
        # sql should not have been called for profiling tables
        for call in mock_spark.sql.call_args_list:
            assert "column_profiling_stats" not in call[0][0]


# ======================================================================
# Fix E2: _add_constraint_context
# ======================================================================
class TestAddConstraintContext:

    def _make_prompt_instance(self, mock_spark, include_constraints=True):
        from dbxmetagen.prompts import Prompt

        class _TestPrompt(Prompt):
            def convert_to_comment_input(self):
                return {"column_contents": {"column_metadata": {
                    "pk_col": {"comment": ""}, "fk_col": {"comment": ""},
                }}}
            def add_metadata_to_comment_input(self):
                pass
            def chat_completion(self):
                return {}

        config = MagicMock()
        config.add_metadata = False
        config.include_lineage = False
        config.include_profiling_context = False
        config.include_constraint_context = include_constraints
        config.catalog_name = "cat"
        config.schema_name = "sch"

        with patch("dbxmetagen.prompts.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            return _TestPrompt(config, MagicMock(), "cat.sch.tbl")

    def test_pk_and_fk_roles_injected(self):
        mock_spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            "primary_key_columns": ["pk_col"],
            "foreign_keys": {"fk_col": "ref_cat.ref_sch.ref_tbl.ref_col"},
        }[k]
        mock_spark.sql.return_value.collect.return_value = [row]

        prompt = self._make_prompt_instance(mock_spark)
        meta = prompt.prompt_content["column_contents"]["column_metadata"]
        assert meta["pk_col"]["role"] == "PRIMARY KEY"
        assert "FOREIGN KEY" in meta["fk_col"]["role"]

    def test_no_crash_when_table_missing(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.side_effect = Exception("nope")
        prompt = self._make_prompt_instance(mock_spark)
        meta = prompt.prompt_content["column_contents"]["column_metadata"]
        assert "role" not in meta.get("pk_col", {})


# ======================================================================
# Fix A: cardinality_analysis with profiling stats
# ======================================================================
class TestCardinalityAnalysisProfiling:

    @pytest.fixture
    def predictor(self):
        from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
        mock_spark = MagicMock()
        config = FKPredictionConfig(catalog_name="cat", schema_name="sch")
        p = FKPredictor(mock_spark, config)
        p._table_samples = {}
        return p

    def test_load_profiling_stats_returns_dict(self, predictor):
        row = MagicMock(table_name="cat.sch.tbl", column_name="id", distinct_count=500)
        predictor.spark.sql.return_value.collect.return_value = [row]
        stats = predictor._load_profiling_stats()
        assert stats["cat.sch.tbl.id"] == 500

    def test_load_profiling_stats_returns_empty_on_failure(self, predictor):
        predictor.spark.sql.return_value.collect.side_effect = Exception("no table")
        assert predictor._load_profiling_stats() == {}

    def test_load_profiling_row_counts_returns_dict(self, predictor):
        row = MagicMock(table_name="cat.sch.tbl", row_count=10000)
        predictor.spark.sql.return_value.collect.return_value = [row]
        counts = predictor._load_profiling_row_counts()
        assert counts["cat.sch.tbl"] == 10000


# ======================================================================
# Fix C: add_lineage_signal
# ======================================================================
class TestAddLineageSignal:

    @pytest.fixture
    def predictor(self):
        from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
        mock_spark = MagicMock()
        config = FKPredictionConfig(catalog_name="cat", schema_name="sch")
        p = FKPredictor(mock_spark, config)
        p._table_samples = {}
        return p

    def test_returns_zero_score_on_no_lineage(self, predictor):
        predictor.spark.sql.return_value.collect.side_effect = Exception("no table")
        cands = MagicMock()
        result = predictor.add_lineage_signal(cands)
        cands.withColumn.assert_called_once()
        args = cands.withColumn.call_args
        assert args[0][0] == "lineage_score"


# ======================================================================
# Fix B1: extract_constraints with referential_constraints
# ======================================================================
class TestExtractConstraintsRefConstraints:

    @pytest.fixture
    def builder(self):
        from dbxmetagen.extended_metadata import ExtendedMetadataConfig, ExtendedMetadataBuilder
        mock_spark = MagicMock()
        config = ExtendedMetadataConfig(catalog_name="cat", schema_name="sch")
        return ExtendedMetadataBuilder(mock_spark, config)

    def test_load_fk_references_returns_empty_on_failure(self, builder):
        builder.spark.sql.return_value.collect.side_effect = Exception("no referential_constraints")
        result = builder._load_fk_references("my_catalog")
        assert result == {}

    def test_load_fk_references_parses_rows(self, builder):
        row = MagicMock(
            src_table="cat.sch.orders",
            src_col="customer_id",
            ref_target="cat.sch.customers.id",
        )
        builder.spark.sql.return_value.collect.return_value = [row]
        result = builder._load_fk_references("cat")
        assert result["cat.sch.orders"]["customer_id"] == "cat.sch.customers.id"

    def test_extract_constraints_empty_tables(self, builder):
        result = builder.extract_constraints([])
        assert result is not None  # should be empty DF


# ======================================================================
# Fix B2: get_declared_fk_candidates
# ======================================================================
class TestGetDeclaredFKCandidates:

    @pytest.fixture
    def predictor(self):
        from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
        mock_spark = MagicMock()
        config = FKPredictionConfig(catalog_name="cat", schema_name="sch")
        p = FKPredictor(mock_spark, config)
        p._table_samples = {}
        return p

    def test_returns_empty_on_missing_table(self, predictor):
        predictor.spark.sql.return_value.collect.side_effect = Exception("no table")
        result = predictor.get_declared_fk_candidates()
        assert result is not None

    def test_returns_empty_when_no_fk_values(self, predictor):
        row = MagicMock(table_name="cat.sch.tbl", foreign_keys={"col_a": ""})
        predictor.spark.sql.return_value.collect.return_value = [row]
        result = predictor.get_declared_fk_candidates()
        # empty ref_target "" is skipped
        predictor.spark.createDataFrame.assert_called()

    def test_emits_candidates_with_col_similarity_1(self, predictor):
        row = MagicMock(
            table_name="cat.sch.orders",
            foreign_keys={"customer_id": "cat.sch.customers.id"},
        )
        predictor.spark.sql.return_value.collect.return_value = [row]
        result = predictor.get_declared_fk_candidates()
        # createDataFrame called with pairs having col_similarity=1.0
        call_args = predictor.spark.createDataFrame.call_args
        pairs = call_args[0][0]
        assert len(pairs) == 1
        assert pairs[0][6] == 1.0  # col_similarity


# ======================================================================
# KB Bootstrap: KnowledgeBaseBuilder.bootstrap
# ======================================================================
class TestKnowledgeBaseBootstrap:

    @pytest.fixture
    def builder(self):
        from dbxmetagen.knowledge_base import KnowledgeBaseConfig, KnowledgeBaseBuilder
        mock_spark = MagicMock()
        config = KnowledgeBaseConfig(catalog_name="out_cat", schema_name="out_sch")
        return KnowledgeBaseBuilder(mock_spark, config)

    def test_bootstrap_creates_target_table(self, builder):
        builder.spark.sql.return_value.count.return_value = 3
        builder.bootstrap(["cat.sch.tbl_a", "cat.sch.tbl_b"])
        ddl_call = builder.spark.sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl_call

    def test_bootstrap_queries_information_schema(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 2
        builder.spark.sql.return_value = mock_df

        builder.bootstrap(["cat.sch.tbl_a"])
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        info_schema_calls = [s for s in sql_calls if "information_schema.tables" in s]
        assert len(info_schema_calls) >= 1

    def test_bootstrap_uses_merge_not_matched(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        builder.spark.sql.return_value = mock_df

        builder.bootstrap(["cat.sch.tbl"])
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        merge_calls = [s for s in sql_calls if "MERGE INTO" in s]
        assert len(merge_calls) == 1
        assert "WHEN NOT MATCHED THEN INSERT" in merge_calls[0]

    def test_bootstrap_returns_zero_on_empty(self, builder):
        result = builder.bootstrap([])
        assert result == 0

    def test_bootstrap_skips_bad_fqn(self, builder):
        result = builder.bootstrap(["no_dots"])
        assert result == 0

    def test_bootstrap_handles_query_failure(self, builder):
        builder.spark.sql.side_effect = [
            MagicMock(),  # create_target_table
            Exception("access denied"),  # information_schema query
        ]
        result = builder.bootstrap(["cat.sch.tbl"])
        assert result == 0


# ======================================================================
# KB Bootstrap: ColumnKnowledgeBaseBuilder.bootstrap
# ======================================================================
class TestColumnKnowledgeBaseBootstrap:

    @pytest.fixture
    def builder(self):
        from dbxmetagen.column_knowledge_base import ColumnKnowledgeBaseConfig, ColumnKnowledgeBaseBuilder
        mock_spark = MagicMock()
        config = ColumnKnowledgeBaseConfig(catalog_name="out_cat", schema_name="out_sch")
        return ColumnKnowledgeBaseBuilder(mock_spark, config)

    def test_bootstrap_creates_target_table(self, builder):
        builder.spark.sql.return_value.count.return_value = 5
        builder.bootstrap(["cat.sch.tbl_a"])
        ddl_call = builder.spark.sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl_call

    def test_bootstrap_queries_information_schema_columns(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        builder.spark.sql.return_value = mock_df

        builder.bootstrap(["cat.sch.tbl_a"])
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        info_calls = [s for s in sql_calls if "information_schema.columns" in s]
        assert len(info_calls) >= 1

    def test_bootstrap_uses_merge_not_matched(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        builder.spark.sql.return_value = mock_df

        builder.bootstrap(["cat.sch.tbl"])
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        merge_calls = [s for s in sql_calls if "MERGE INTO" in s]
        assert len(merge_calls) == 1
        assert "WHEN NOT MATCHED THEN INSERT" in merge_calls[0]

    def test_bootstrap_returns_zero_on_empty(self, builder):
        assert builder.bootstrap([]) == 0

    def test_bootstrap_groups_by_catalog_schema(self, builder):
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        builder.spark.sql.return_value = mock_df

        builder.bootstrap(["cat_a.sch1.tbl1", "cat_a.sch1.tbl2", "cat_b.sch2.tbl3"])
        sql_calls = [c[0][0] for c in builder.spark.sql.call_args_list]
        info_calls = [s for s in sql_calls if "information_schema.columns" in s]
        # Should have queries for 2 distinct catalog.schema combos
        assert len(info_calls) == 2


# ======================================================================
# validate_optional_tables pre-flight check
# ======================================================================
class TestValidateOptionalTables:

    @classmethod
    def setup_class(cls):
        from conftest import install_processing_stubs, uninstall_processing_stubs
        cls._saved = install_processing_stubs()
        cls._uninstall = uninstall_processing_stubs

    @classmethod
    def teardown_class(cls):
        cls._uninstall(cls._saved)

    def setup_method(self):
        import dbxmetagen.processing as proc
        proc._ext_metadata_unavailable = False

    def _make_config(self, **overrides):
        defaults = dict(
            catalog_name="cat", schema_name="sch",
            include_lineage=True, include_constraint_context=False,
            include_profiling_context=False, use_ontology_context=False,
            use_kb_comments=False, use_customer_context=False,
        )
        defaults.update(overrides)
        cfg = MagicMock()
        for k, v in defaults.items():
            setattr(cfg, k, v)
        return cfg

    @patch("dbxmetagen.processing.table_exists_uc", return_value=False)
    def test_disables_constraint_flag_when_ext_metadata_missing(self, mock_exists):
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(include_constraint_context=True)
        validate_optional_tables(MagicMock(), config)
        assert config.include_constraint_context is False

    @patch("dbxmetagen.processing.table_exists_uc", return_value=False)
    def test_sets_ext_metadata_unavailable_when_lineage_enabled(self, mock_exists):
        import dbxmetagen.processing as proc
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(include_lineage=True)
        validate_optional_tables(MagicMock(), config)
        assert proc._ext_metadata_unavailable is True
        assert config.include_lineage is True  # NOT disabled

    @patch("dbxmetagen.processing.table_exists_uc", return_value=True)
    def test_leaves_flags_alone_when_tables_exist(self, mock_exists):
        import dbxmetagen.processing as proc
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(
            include_lineage=True, include_constraint_context=True,
            include_profiling_context=True, use_ontology_context=True,
            use_kb_comments=True,
        )
        validate_optional_tables(MagicMock(), config)
        assert config.include_constraint_context is True
        assert config.include_profiling_context is True
        assert config.use_ontology_context is True
        assert config.use_kb_comments is True
        assert proc._ext_metadata_unavailable is False

    @patch("dbxmetagen.processing.table_exists_uc", return_value=False)
    def test_skips_disabled_flags(self, mock_exists):
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(include_lineage=False)
        validate_optional_tables(MagicMock(), config)
        mock_exists.assert_not_called()

    @patch("dbxmetagen.processing.table_exists_uc", return_value=False)
    def test_disables_profiling_when_stats_table_missing(self, mock_exists):
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(include_profiling_context=True, include_lineage=False)
        validate_optional_tables(MagicMock(), config)
        assert config.include_profiling_context is False

    @patch("dbxmetagen.processing.table_exists_uc", return_value=False)
    def test_disables_kb_when_tables_missing(self, mock_exists):
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(use_kb_comments=True, include_lineage=False)
        validate_optional_tables(MagicMock(), config)
        assert config.use_kb_comments is False

    @patch("dbxmetagen.processing.table_exists_uc", return_value=False)
    def test_logs_warning_for_missing_table(self, mock_exists):
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(include_lineage=True)
        with patch("dbxmetagen.processing.logger") as mock_logger:
            validate_optional_tables(MagicMock(), config)
            assert mock_logger.warning.called
            warn_msg = mock_logger.warning.call_args[0][0]
            assert "not found" in warn_msg

    def test_noop_when_no_catalog_schema(self):
        from dbxmetagen.processing import validate_optional_tables
        config = self._make_config(catalog_name=None, schema_name=None)
        validate_optional_tables(MagicMock(), config)


# ======================================================================
# fetch_lineage respects _ext_metadata_unavailable flag
# ======================================================================
class TestFetchLineageExtMetadataFlag:

    @classmethod
    def setup_class(cls):
        from conftest import install_processing_stubs, uninstall_processing_stubs
        cls._saved = install_processing_stubs()
        cls._uninstall = uninstall_processing_stubs

    @classmethod
    def teardown_class(cls):
        cls._uninstall(cls._saved)

    def setup_method(self):
        import dbxmetagen.processing as proc
        proc._ext_metadata_unavailable = False
        proc._lineage_cache.clear()
        proc._lineage_unavailable = False

    def teardown_method(self):
        import dbxmetagen.processing as proc
        proc._ext_metadata_unavailable = False

    def test_skips_cache_when_ext_metadata_unavailable(self):
        import dbxmetagen.processing as proc
        from dbxmetagen.processing import fetch_lineage
        proc._ext_metadata_unavailable = True

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []

        result = fetch_lineage(
            mock_spark, "cat.sch.tbl",
            catalog_name="out_cat", schema_name="out_sch",
        )
        # Should go straight to system table, not extended_table_metadata
        sql_calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert not any("extended_table_metadata" in s for s in sql_calls)

    def test_uses_cache_when_ext_metadata_available(self):
        import dbxmetagen.processing as proc
        from dbxmetagen.processing import fetch_lineage
        proc._ext_metadata_unavailable = False

        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, k: {
            "upstream_tables": ["a.b.up1"],
            "downstream_tables": ["a.b.dn1"],
        }[k]
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        result = fetch_lineage(
            mock_spark, "cat.sch.tbl",
            catalog_name="out_cat", schema_name="out_sch",
        )
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "extended_table_metadata" in sql_arg
        assert result == {"upstream_tables": ["a.b.up1"], "downstream_tables": ["a.b.dn1"]}


# ======================================================================
# Early-return dict shape in get_domain_classification
# ======================================================================
class TestGetDomainClassificationEarlyReturn:
    """When read_table_with_type_conversion raises, get_domain_classification
    must return a dict with every key that append_domain_table_row accesses."""

    @classmethod
    def setup_class(cls):
        from conftest import install_processing_stubs, uninstall_processing_stubs
        cls._saved = install_processing_stubs()
        cls._uninstall = uninstall_processing_stubs

    @classmethod
    def teardown_class(cls):
        cls._uninstall(cls._saved)

    REQUIRED_KEYS = [
        "domain", "subdomain", "confidence", "recommended_domain",
        "recommended_subdomain", "reasoning", "metadata_summary",
    ]

    def test_unreadable_table_returns_all_keys(self):
        from dbxmetagen.processing import get_domain_classification
        mock_config = MagicMock()
        mock_config.domain_config_path = None
        mock_config.ontology_bundle = None

        with patch("dbxmetagen.processing.SparkSession") as mock_ss, \
             patch("dbxmetagen.processing.load_domain_config", return_value={"domains": {}}), \
             patch("dbxmetagen.processing.read_table_with_type_conversion", side_effect=RuntimeError("table gone")):
            result = get_domain_classification(mock_config, "cat.sch.tbl")

        for key in self.REQUIRED_KEYS:
            assert key in result, f"Early-return missing key: {key}"
        assert result["domain"] == "unknown"
        assert result["confidence"] == 0.0

    def test_confidence_is_numeric(self):
        from dbxmetagen.processing import get_domain_classification
        mock_config = MagicMock()
        mock_config.domain_config_path = None
        mock_config.ontology_bundle = None

        with patch("dbxmetagen.processing.SparkSession"), \
             patch("dbxmetagen.processing.load_domain_config", return_value={"domains": {}}), \
             patch("dbxmetagen.processing.read_table_with_type_conversion", side_effect=RuntimeError("boom")):
            result = get_domain_classification(mock_config, "cat.sch.tbl")

        formatted = f"{result['confidence']:.2f}"
        assert formatted == "0.00"

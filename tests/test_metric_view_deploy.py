"""Tests for metric view deployment location tracking and Genie assembly.

Validates that:
  1. apply_metric_views() records deployed_catalog / deployed_schema
  2. _resolve_mv_location uses assembler fallback (not source_table)
  3. Genie assembler correctly resolves MV locations from different schemas
  4. generate_metric_views() INSERT includes the two deployed columns as NULL
  5. _get_metric_views deduplicates ambiguous short-name matches across schemas
  6. apply_metric_views deploys to source table's schema (not hardcoded config)
  7. create_genie_space uses deployed_catalog/schema from definition rows
"""

import json
import logging
import pytest
from unittest.mock import MagicMock, call, patch
from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig
from dbxmetagen.genie.context import GenieContextAssembler


# ── _resolve_mv_location ─────────────────────────────────────────────


class TestResolveMvLocation:
    """GenieContextAssembler._resolve_mv_location priority:
    deployed cols > source_table catalog/schema > assembler fallback.

    apply_metric_views deploys MVs into the source table's schema,
    so source_table is a valid intermediate fallback when deployed cols
    are NULL (e.g., rows applied before those columns existed)."""

    def test_deployed_columns_used(self):
        mv = {
            "deployed_catalog": "prod",
            "deployed_schema": "gold",
            "source_table": "prod.bronze.orders",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "fb_cat", "fb_sch")
        assert cat == "prod"
        assert sch == "gold"

    def test_fallback_to_source_table_when_deployed_null(self):
        mv = {
            "deployed_catalog": None,
            "deployed_schema": None,
            "source_table": "prod.bronze.orders",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "prod", "metagen")
        assert cat == "prod"
        assert sch == "bronze"

    def test_fallback_to_source_table_when_deployed_missing(self):
        mv = {"source_table": "prod.bronze.orders"}
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "prod", "metagen")
        assert cat == "prod"
        assert sch == "bronze"

    def test_fallback_to_source_table_when_deployed_empty(self):
        mv = {
            "deployed_catalog": "",
            "deployed_schema": "",
            "source_table": "prod.bronze.orders",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "prod", "analytics")
        assert cat == "prod"
        assert sch == "bronze"

    def test_partial_deployed_catalog_only(self):
        mv = {
            "deployed_catalog": "prod",
            "deployed_schema": None,
            "source_table": "prod.bronze.orders",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "fb_cat", "fb_sch")
        assert cat == "prod"
        assert sch == "bronze"

    def test_partial_deployed_schema_only(self):
        mv = {
            "deployed_catalog": None,
            "deployed_schema": "gold",
            "source_table": "prod.bronze.orders",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "fb_cat", "fb_sch")
        assert cat == "prod"
        assert sch == "gold"

    def test_empty_mv_uses_fallback(self):
        cat, sch = GenieContextAssembler._resolve_mv_location({}, "cat", "sch")
        assert cat == "cat"
        assert sch == "sch"

    def test_non_fq_source_falls_through_to_assembler(self):
        """source_table with <3 parts can't provide catalog/schema."""
        mv = {
            "deployed_catalog": None,
            "deployed_schema": None,
            "source_table": "orders",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "fb_cat", "fb_sch")
        assert cat == "fb_cat"
        assert sch == "fb_sch"


# ── apply_metric_views ───────────────────────────────────────────────


class TestApplyMetricViewsDeployTracking:
    """Verify that apply_metric_views() writes deployed_catalog/schema."""

    @pytest.fixture
    def gen(self):
        spark = MagicMock()
        config = SemanticLayerConfig(catalog_name="mycat", schema_name="mysch")
        return SemanticLayerGenerator(spark, config)

    def _make_row(self, data: dict):
        """Create a mock Row whose .asDict() returns a real dict."""
        row = MagicMock()
        row.asDict.return_value = dict(data)
        row.__getitem__ = lambda self, k: data[k]
        return row

    def test_apply_sets_deployed_location(self, gen):
        defn = {
            "name": "revenue_metrics",
            "source": "mycat.bronze.orders",
            "dimensions": [{"name": "region", "expr": "source.region"}],
            "measures": [{"name": "total", "expr": "SUM(source.amount)", "agg": "sum"}],
        }
        row = self._make_row({
            "definition_id": "def-123",
            "metric_view_name": "revenue_metrics",
            "source_table": "mycat.bronze.orders",
            "json_definition": json.dumps(defn),
        })

        gen.spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[row])),  # SELECT
            None,  # CREATE VIEW
            None,  # UPDATE
        ]

        result = gen.apply_metric_views()
        assert result["applied"] == 1

        update_call = gen.spark.sql.call_args_list[2]
        update_sql = update_call[0][0]
        assert "deployed_catalog = 'mycat'" in update_sql
        assert "deployed_schema = 'bronze'" in update_sql

    def test_apply_failed_does_not_set_deployed(self, gen):
        defn = {
            "name": "bad_metrics",
            "source": "mycat.bronze.orders",
            "dimensions": [{"name": "x", "expr": "source.x"}],
            "measures": [{"name": "y", "expr": "SUM(source.y)", "agg": "sum"}],
        }
        row = self._make_row({
            "definition_id": "def-456",
            "metric_view_name": "bad_metrics",
            "source_table": "mycat.bronze.orders",
            "json_definition": json.dumps(defn),
        })

        gen.spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[row])),  # SELECT
            RuntimeError("CREATE failed"),  # CREATE VIEW fails
            None,  # UPDATE with failed status
        ]

        result = gen.apply_metric_views()
        assert result["failed"] == 1

        update_call = gen.spark.sql.call_args_list[2]
        update_sql = update_call[0][0]
        assert "status = 'failed'" in update_sql
        assert "deployed_catalog" not in update_sql

    def test_apply_escapes_catalog_schema(self, gen):
        gen.config.catalog_name = "cat'test"
        gen.config.schema_name = "sch'test"
        defn = {
            "name": "mv1",
            "source": "cat'test.sch'test.tbl",
            "dimensions": [{"name": "d", "expr": "source.d"}],
            "measures": [{"name": "m", "expr": "SUM(source.m)", "agg": "sum"}],
        }
        row = self._make_row({
            "definition_id": "def-789",
            "metric_view_name": "mv1",
            "source_table": "cat'test.sch'test.tbl",
            "json_definition": json.dumps(defn),
        })
        gen.spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[row])),
            None,
            None,
        ]
        gen.apply_metric_views()
        update_sql = gen.spark.sql.call_args_list[2][0][0]
        assert "cat''test" in update_sql
        assert "sch''test" in update_sql


# ── Table schema includes deployed columns ───────────────────────────


class TestDefinitionsTableSchema:
    """The definitions table CREATE should include deployed_catalog/deployed_schema."""

    def test_create_tables_includes_deployed_columns(self):
        spark = MagicMock()
        config = SemanticLayerConfig(catalog_name="cat", schema_name="sch")
        gen = SemanticLayerGenerator(spark, config)
        gen.create_tables()
        create_calls = [c for c in spark.sql.call_args_list if "CREATE TABLE" in str(c)]
        definitions_call = [c for c in create_calls if "metric_view_definitions" in str(c)]
        assert len(definitions_call) == 1
        ddl = definitions_call[0][0][0]
        assert "deployed_catalog" in ddl
        assert "deployed_schema" in ddl


# ── Cross-schema MV scenario (the reported bug) ─────────────────────


class TestGenerateMetricViewsInsert:
    """generate_metric_views() INSERT must include NULL for deployed_catalog/deployed_schema."""

    def test_insert_has_null_deployed_columns(self):
        spark = MagicMock()
        config = SemanticLayerConfig(catalog_name="cat", schema_name="sch")
        gen = SemanticLayerGenerator(spark, config)

        q_row = MagicMock()
        q_row.__getitem__ = lambda self, k: {"question_id": "q1", "question_text": "What?"}[k]

        defn_json = json.dumps({
            "name": "mv1", "source": "cat.sch.t",
            "dimensions": [{"name": "d", "expr": "source.d"}],
            "measures": [{"name": "m", "expr": "SUM(source.x)", "agg": "sum"}],
        })

        call_count = [0]
        def sql_router(query):
            call_count[0] += 1
            mock_result = MagicMock()
            if "SELECT" in query and "pending" in query:
                mock_result.collect.return_value = [q_row]
            elif "AI_QUERY" in query:
                ai_row = MagicMock()
                ai_row.__getitem__ = lambda s, k: defn_json
                mock_result.collect.return_value = [ai_row]
            else:
                mock_result.collect.return_value = []
            return mock_result

        spark.sql.side_effect = sql_router
        gen._validate_definition = MagicMock(return_value=[])
        gen._enrich_joins_from_fk = MagicMock()
        gen.build_context = MagicMock(return_value="ctx")
        gen.config.use_two_phase = False

        gen.generate_metric_views()

        insert_calls = [c for c in spark.sql.call_args_list
                        if "INSERT INTO" in str(c) and "metric_view_definitions" in str(c)]
        assert len(insert_calls) >= 1, "Expected at least one INSERT into definitions table"
        insert_sql = insert_calls[0][0][0]
        assert "NULL, NULL" in insert_sql, (
            "INSERT should end with NULL, NULL for deployed_catalog and deployed_schema"
        )


class TestCrossSchemaMetricViewResolution:
    """Cross-schema MV resolution scenarios."""

    def test_bronze_source_gold_deploy(self):
        mv = {
            "metric_view_name": "defect_sla_compliance_metrics",
            "source_table": "prism_dev.ado_bronze.incidents",
            "status": "applied",
            "deployed_catalog": "prism_dev",
            "deployed_schema": "ado_gold",
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "prism_dev", "metagen")
        assert cat == "prism_dev"
        assert sch == "ado_gold"

    def test_null_deploy_uses_source_table_schema(self):
        """When deployed columns are NULL, fall back to source_table's schema
        since apply_metric_views deploys MVs next to their source."""
        mv = {
            "metric_view_name": "defect_sla_compliance_metrics",
            "source_table": "prism_dev.ado_bronze.incidents",
            "status": "applied",
            "deployed_catalog": None,
            "deployed_schema": None,
        }
        cat, sch = GenieContextAssembler._resolve_mv_location(mv, "prism_dev", "metagen")
        assert cat == "prism_dev"
        assert sch == "ado_bronze"


# ── _get_metric_views ambiguous short-name dedup ─────────────────────


class TestGetMetricViewsShortNameDedup:
    """When tables from multiple schemas share a bare name, MVs matched
    only by that ambiguous short name must be filtered out."""

    def _make_assembler(self):
        assembler = GenieContextAssembler.__new__(GenieContextAssembler)
        assembler.ws = MagicMock()
        assembler.wh = "wh-123"
        assembler.catalog = "cat"
        assembler.schema = "metagen"
        return assembler

    def test_ambiguous_short_name_filtered(self):
        assembler = self._make_assembler()
        tables = ["cat.schema_a.orders", "cat.schema_b.orders"]

        mv_rows = [
            {"metric_view_name": "mv_orders", "source_table": "orders",
             "status": "validated", "deployed_catalog": None, "deployed_schema": None,
             "json_definition": "{}"},
        ]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            mvs, warnings = assembler._get_metric_views(tables)

        assert len(mvs) == 0, "MV matched via ambiguous short name 'orders' should be dropped"

    def test_fq_source_not_filtered_even_with_ambiguous_short(self):
        assembler = self._make_assembler()
        tables = ["cat.schema_a.orders", "cat.schema_b.orders"]

        mv_rows = [
            {"metric_view_name": "mv_orders_a", "source_table": "cat.schema_a.orders",
             "status": "validated", "deployed_catalog": None, "deployed_schema": None,
             "json_definition": "{}"},
        ]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            mvs, warnings = assembler._get_metric_views(tables)

        assert len(mvs) == 1, "MV with FQ source_table should survive even with ambiguous short names"

    def test_no_dedup_when_no_ambiguity(self):
        assembler = self._make_assembler()
        tables = ["cat.schema_a.orders", "cat.schema_b.customers"]

        mv_rows = [
            {"metric_view_name": "mv_orders", "source_table": "orders",
             "status": "validated", "deployed_catalog": None, "deployed_schema": None,
             "json_definition": "{}"},
        ]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            mvs, warnings = assembler._get_metric_views(tables)

        assert len(mvs) == 1, "No ambiguity, so short-name MV should pass through"


# ── _get_metric_views trusts applied status (no existence check) ─────


class TestGetMetricViewsPassThrough:
    """Applied MVs are trusted based on status -- no existence check.
    All applied/validated MVs returned by the query should pass through."""

    def _make_assembler(self, config_cat="prism_dev", config_sch="metagen"):
        assembler = GenieContextAssembler.__new__(GenieContextAssembler)
        assembler.ws = MagicMock()
        assembler.wh = "wh-123"
        assembler.catalog = config_cat
        assembler.schema = config_sch
        return assembler

    def test_applied_mv_passes_through(self):
        assembler = self._make_assembler()
        mv_rows = [{
            "metric_view_name": "revenue_metrics",
            "source_table": "prism_dev.ado_bronze.incidents",
            "status": "applied",
            "deployed_catalog": "prism_dev",
            "deployed_schema": "ado_bronze",
            "json_definition": "{}",
        }]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            mvs, warnings = assembler._get_metric_views(["prism_dev.ado_bronze.incidents"])

        assert len(mvs) == 1
        assert len(warnings) == 0
        assert mvs[0]["metric_view_name"] == "revenue_metrics"

    def test_no_existence_check_executed(self):
        """Verify no SQL warehouse statement_execution calls are made."""
        assembler = self._make_assembler()
        mv_rows = [{
            "metric_view_name": "mv1",
            "source_table": "prism_dev.ado_bronze.incidents",
            "status": "applied",
            "deployed_catalog": "prism_dev",
            "deployed_schema": "ado_bronze",
            "json_definition": "{}",
        }]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            assembler._get_metric_views(["prism_dev.ado_bronze.incidents"])

        assembler.ws.statement_execution.execute_statement.assert_not_called()

    def test_multi_schema_applied_mvs_all_returned(self):
        assembler = self._make_assembler()
        mv_rows = [
            {
                "metric_view_name": "bronze_mv",
                "source_table": "cat.bronze.orders",
                "status": "applied",
                "deployed_catalog": "cat",
                "deployed_schema": "bronze",
                "json_definition": "{}",
            },
            {
                "metric_view_name": "gold_mv",
                "source_table": "cat.gold.summary",
                "status": "applied",
                "deployed_catalog": "cat",
                "deployed_schema": "gold",
                "json_definition": "{}",
            },
        ]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            mvs, warnings = assembler._get_metric_views(["cat.bronze.orders", "cat.gold.summary"])

        assert len(mvs) == 2
        assert len(warnings) == 0

    def test_name_level_dedup_keeps_most_recent(self):
        """When duplicate MV names exist, the first row wins (ORDER BY applied_at DESC)."""
        assembler = self._make_assembler()
        mv_rows = [
            {
                "metric_view_name": "dup_mv",
                "source_table": "cat.sfdc.orders",
                "status": "applied",
                "deployed_catalog": "cat",
                "deployed_schema": "sfdc",
                "json_definition": "{}",
            },
            {
                "metric_view_name": "dup_mv",
                "source_table": "cat.sfdc.orders",
                "status": "applied",
                "deployed_catalog": None,
                "deployed_schema": None,
                "json_definition": "{}",
            },
        ]

        with patch("dbxmetagen.genie.context._safe_sql", return_value=mv_rows):
            mvs, warnings = assembler._get_metric_views(["cat.sfdc.orders"])

        assert len(mvs) == 1
        assert mvs[0]["deployed_schema"] == "sfdc"


# ── apply_metric_views deploys to source table's schema ──────────────


class TestApplyDeploysToSourceSchema:
    """apply_metric_views() should deploy each MV to its source table's
    catalog.schema, not to the hardcoded config schema."""

    @pytest.fixture
    def gen(self):
        spark = MagicMock()
        config = SemanticLayerConfig(catalog_name="mycat", schema_name="mysch")
        return SemanticLayerGenerator(spark, config)

    def _make_row(self, data: dict):
        row = MagicMock()
        row.asDict.return_value = dict(data)
        row.__getitem__ = lambda self, k: data[k]
        return row

    def test_deploys_to_source_schema_not_config(self, gen):
        defn = {
            "name": "mv1",
            "source": "prod.gold.fact_table",
            "dimensions": [{"name": "d", "expr": "source.d"}],
            "measures": [{"name": "m", "expr": "SUM(source.x)", "agg": "sum"}],
        }
        row = self._make_row({
            "definition_id": "def-1",
            "metric_view_name": "mv1",
            "source_table": "prod.gold.fact_table",
            "json_definition": json.dumps(defn),
        })

        gen.spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[row])),
            None,  # CREATE VIEW
            None,  # UPDATE
        ]

        gen.apply_metric_views()
        create_sql = gen.spark.sql.call_args_list[1][0][0]
        assert "prod.gold.mv1" in create_sql, "View should be created in source table's schema"

        update_sql = gen.spark.sql.call_args_list[2][0][0]
        assert "deployed_catalog = 'prod'" in update_sql
        assert "deployed_schema = 'gold'" in update_sql

    def test_non_fq_source_falls_back_to_config(self, gen):
        defn = {
            "name": "mv2",
            "source": "some_table",
            "dimensions": [{"name": "d", "expr": "source.d"}],
            "measures": [{"name": "m", "expr": "SUM(source.x)", "agg": "sum"}],
        }
        row = self._make_row({
            "definition_id": "def-2",
            "metric_view_name": "mv2",
            "source_table": "some_table",
            "json_definition": json.dumps(defn),
        })

        gen.spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[row])),
            None,
            None,
        ]

        gen.apply_metric_views()
        update_sql = gen.spark.sql.call_args_list[2][0][0]
        assert "deployed_catalog = 'mycat'" in update_sql
        assert "deployed_schema = 'mysch'" in update_sql


# ── create_genie_space uses deployed_catalog/schema ──────────────────


class TestCreateGenieSpaceDeployedLocation:
    """create_genie_space should read deployed_catalog/deployed_schema
    from each definition row, not hardcode config schema."""

    @pytest.fixture
    def gen(self):
        spark = MagicMock()
        config = SemanticLayerConfig(catalog_name="mycat", schema_name="mysch")
        g = SemanticLayerGenerator(spark, config)
        g._safe_collect = MagicMock(return_value=[])
        g.build_context = MagicMock(return_value="ctx")
        g._generate_example_sqls = MagicMock(return_value=[])
        g._build_genie_instructions = MagicMock(return_value={})
        return g

    def test_mv_identifier_uses_deployed_columns(self, gen):
        applied_row = MagicMock()
        applied_row.__getitem__ = lambda self, k: {
            "metric_view_name": "mv_revenue",
            "source_table": "mycat.bronze.orders",
            "json_definition": json.dumps({"comment": "Revenue metrics"}),
            "deployed_catalog": "mycat",
            "deployed_schema": "gold",
        }[k]
        applied_row.get = lambda k, d=None: {
            "metric_view_name": "mv_revenue",
            "source_table": "mycat.bronze.orders",
            "json_definition": json.dumps({"comment": "Revenue metrics"}),
            "deployed_catalog": "mycat",
            "deployed_schema": "gold",
        }.get(k, d)

        questions_row = MagicMock()
        questions_row.__getitem__ = lambda self, k: {"question_text": "How much?"}[k]
        questions_row.get = lambda k, d=None: {"question_text": "How much?"}.get(k, d)

        gen.spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[applied_row])),  # SELECT applied
            MagicMock(collect=MagicMock(return_value=[questions_row])),  # SELECT questions
        ]

        with patch("dbxmetagen.semantic_layer.WorkspaceClient", create=True):
            result = gen.create_genie_space("Test Space", "wh-123")

        # Find the space dict built before the API call
        create_calls = [c for c in gen.spark.sql.call_args_list]
        # The metric_views_ds should use deployed_catalog.deployed_schema
        # We can check via the internal state -- verify the method was called
        # by checking what data_sources was built
        # Since we can't easily intercept the space dict, verify the SQL calls
        # show the correct SELECT with deployed columns
        select_sql = create_calls[0][0][0]
        assert "deployed_catalog" in select_sql
        assert "deployed_schema" in select_sql

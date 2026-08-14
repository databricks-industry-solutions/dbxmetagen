"""Tests for api_server pure helpers and job param wiring.

Uses the same lightweight module mock pattern as test_genie_strip_joins.py
so api_server can import without real Databricks/FastAPI deps.
"""

import os
import sys
import types
import pytest
from unittest.mock import MagicMock

_MOCK_MODULES = [
    "fastapi", "fastapi.responses", "fastapi.staticfiles", "fastapi.middleware", "fastapi.middleware.cors",
    "starlette", "starlette.middleware", "starlette.middleware.base", "starlette.requests", "starlette.responses",
    "sqlalchemy", "sqlalchemy.orm",
    "uvicorn", "cachetools",
    "langchain_core", "langchain_core.tools", "langchain_core.messages",
    "langchain_databricks", "langchain_community", "langchain_community.chat_models",
    "langgraph", "langgraph.graph", "langgraph.graph.message",
    "langgraph.prebuilt", "requests", "pydantic",
]


class _AutoMockModule(types.ModuleType):
    def __getattr__(self, name):
        mock = MagicMock()
        setattr(self, name, mock)
        return mock


def _install_mock_modules():
    for mod_name in _MOCK_MODULES:
        if mod_name not in sys.modules:
            mod = _AutoMockModule(mod_name)
            if any(m.startswith(mod_name + ".") for m in _MOCK_MODULES):
                mod.__path__ = []
            sys.modules[mod_name] = mod
    if not isinstance(sys.modules.get("fastapi"), _AutoMockModule):
        return
    def _http_exc_init(self, status_code=None, detail=None, **kw):
        self.status_code = status_code
        self.detail = detail
    sys.modules["fastapi"].HTTPException = type(
        "HTTPException", (Exception,), {"__init__": _http_exc_init}
    )
    sys.modules["cachetools"].cached = lambda *a, **kw: (lambda fn: fn)


_install_mock_modules()

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")
sys.path.insert(0, APP_DIR)
os.environ.setdefault("CATALOG_NAME", "test_cat")
os.environ.setdefault("SCHEMA_NAME", "test_schema")
os.environ.setdefault("LAKEBASE_CATALOG", "lb_cat")
os.environ.setdefault("WAREHOUSE_ID", "wh123")

import api_server  # noqa: E402


# ---------------------------------------------------------------------------
# _safe_bundle_path
# ---------------------------------------------------------------------------
class TestSafeBundlePath:
    def test_valid_subpath(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, "healthcare")
        assert result is not None
        assert result.startswith(root)

    def test_rejects_path_traversal(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, "../../etc/passwd")
        assert result is None

    def test_rejects_absolute_escape(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, "/tmp/evil")
        assert result is None

    def test_dot_resolves_to_root(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, ".")
        assert result == os.path.normpath(root)


# ---------------------------------------------------------------------------
# _validate_filter
# ---------------------------------------------------------------------------
class TestValidateFilter:
    def test_accepts_normal_identifier(self):
        api_server._validate_filter("my_catalog.my_schema", "catalog")

    def test_accepts_none(self):
        api_server._validate_filter(None, "catalog")

    def test_accepts_empty(self):
        api_server._validate_filter("", "catalog")

    def test_rejects_sql_injection(self):
        HTTPException = sys.modules["fastapi"].HTTPException
        with pytest.raises(HTTPException) as exc_info:
            api_server._validate_filter("'; DROP TABLE--", "catalog")
        assert exc_info.value.status_code == 400

    def test_rejects_backtick(self):
        HTTPException = sys.modules["fastapi"].HTTPException
        with pytest.raises(HTTPException):
            api_server._validate_filter("cat`evil", "catalog")

    def test_accepts_hyphens_and_spaces(self):
        api_server._validate_filter("my-catalog name", "catalog")


# ---------------------------------------------------------------------------
# Job param merge (extra_params override)
# ---------------------------------------------------------------------------
class TestJobParamMerge:
    """Verifies that extra_params correctly override/extend base params.
    This replicates the pattern from POST /api/jobs/run."""

    def test_extra_params_override_base(self):
        params = {"mode": "comment", "sample_size": "5"}
        extra = {"sample_size": "10", "columns_per_call": "15"}
        params.update(extra)
        assert params["sample_size"] == "10"
        assert params["columns_per_call"] == "15"
        assert params["mode"] == "comment"

    def test_extra_params_add_new_keys(self):
        params = {"mode": "all"}
        extra = {"columns_per_call": "20", "batch_ddl_apply": "true"}
        params.update(extra)
        assert "columns_per_call" in params
        assert "batch_ddl_apply" in params

    def test_empty_extra_params_is_noop(self):
        params = {"mode": "comment", "sample_size": "5"}
        original = dict(params)
        params.update({})
        assert params == original


# ---------------------------------------------------------------------------
# _compute_mv_health -- metric view quality scoring
# ---------------------------------------------------------------------------
class TestComputeMvHealth:
    """Tests for the metric view health scoring function."""

    def _minimal_defn(self, **overrides):
        base = {
            "source": "cat.sch.t",
            "comment": "A metric view",
            "dimensions": [
                {"name": "d1", "expr": "col1", "comment": "dim 1"},
                {"name": "d2", "expr": "col2", "comment": "dim 2"},
                {"name": "d3", "expr": "col3", "comment": "dim 3"},
            ],
            "measures": [
                {"name": "m1", "expr": "SUM(x)", "comment": "measure 1"},
                {"name": "m2", "expr": "COUNT(*)", "comment": "measure 2"},
                {"name": "m3", "expr": "AVG(y)", "comment": "measure 3"},
            ],
        }
        base.update(overrides)
        return base

    def test_perfect_score(self):
        defn = self._minimal_defn()
        defn["measures"][0]["synonyms"] = ["total x"]
        defn["measures"][0]["expr"] = "SUM(x) FILTER(WHERE active)"
        result = api_server._compute_mv_health(defn)
        assert result["score"] == result["max"] == 10

    def test_no_measures_scores_zero_for_measures(self):
        defn = self._minimal_defn(measures=[])
        result = api_server._compute_mv_health(defn)
        assert result["dimensions"]["measures"]["score"] == 0
        high_issues = [i for i in result["issues"] if i["severity"] == "high"]
        assert any("No measures" in i["message"] for i in high_issues)

    def test_no_dimensions_scores_zero_for_dims(self):
        defn = self._minimal_defn(dimensions=[])
        result = api_server._compute_mv_health(defn)
        assert result["dimensions"]["dimensions"]["score"] == 0

    def test_no_comment_penalized(self):
        defn = self._minimal_defn(comment="")
        result = api_server._compute_mv_health(defn)
        assert result["dimensions"]["metadata"]["score"] < 2
        assert any("No top-level comment" in i["message"] for i in result["issues"])

    def test_uncommented_measures_flagged(self):
        defn = self._minimal_defn()
        defn["measures"][0]["comment"] = ""
        result = api_server._compute_mv_health(defn)
        assert result["dimensions"]["measures"]["score"] == 1

    def test_synonyms_boost_richness(self):
        defn = self._minimal_defn()
        result_no_syn = api_server._compute_mv_health(defn)
        defn["dimensions"][0]["synonyms"] = ["alt name"]
        result_syn = api_server._compute_mv_health(defn)
        assert result_syn["dimensions"]["richness"]["score"] > result_no_syn["dimensions"]["richness"]["score"]

    def test_unquoted_literal_detected(self):
        defn = self._minimal_defn()
        defn["measures"][0]["expr"] = "CASE WHEN status = Active THEN Active Status ELSE Inactive END"
        result = api_server._compute_mv_health(defn)
        lit_issues = [i for i in result["issues"] if "Unquoted" in i.get("message", "")]
        assert len(lit_issues) >= 1

    def test_score_within_range(self):
        defn = self._minimal_defn()
        result = api_server._compute_mv_health(defn)
        assert 0 <= result["score"] <= result["max"]

    def test_unused_join_detected(self):
        defn = self._minimal_defn()
        defn["joins"] = [
            {"name": "dim_patient", "source": "cat.sch.dim_patient", "on": "source.pk = dim_patient.pk"},
        ]
        result = api_server._compute_mv_health(defn)
        unused_issues = [i for i in result["issues"] if "never referenced" in i.get("message", "")]
        assert len(unused_issues) == 1
        assert "dim_patient" in unused_issues[0]["message"]

    def test_used_join_not_flagged(self):
        defn = self._minimal_defn()
        defn["joins"] = [
            {"name": "dim_patient", "source": "cat.sch.dim_patient", "on": "source.pk = dim_patient.pk"},
        ]
        defn["dimensions"].append({"name": "race", "expr": "dim_patient.race", "comment": "race"})
        result = api_server._compute_mv_health(defn)
        unused_issues = [i for i in result["issues"] if "never referenced" in i.get("message", "")]
        assert len(unused_issues) == 0

    def test_fact_to_fact_join_detected(self):
        defn = self._minimal_defn()
        defn["joins"] = [
            {"name": "fact_clinical_event", "source": "cat.sch.fact_clinical_event", "on": "source.ek = fact_clinical_event.ek"},
        ]
        defn["dimensions"].append({"name": "evt", "expr": "fact_clinical_event.type", "comment": "type"})
        result = api_server._compute_mv_health(defn)
        fact_issues = [i for i in result["issues"] if "Fact-to-fact" in i.get("message", "")]
        assert len(fact_issues) == 1
        assert "fact_clinical_event" in fact_issues[0]["message"]

    def test_dim_join_not_flagged_as_fact(self):
        defn = self._minimal_defn()
        defn["joins"] = [
            {"name": "dim_patient", "source": "cat.sch.dim_patient", "on": "source.pk = dim_patient.pk"},
        ]
        defn["dimensions"].append({"name": "race", "expr": "dim_patient.race", "comment": "race"})
        result = api_server._compute_mv_health(defn)
        fact_issues = [i for i in result["issues"] if "Fact-to-fact" in i.get("message", "")]
        assert len(fact_issues) == 0

    def test_unused_join_does_not_affect_score(self):
        defn = self._minimal_defn()
        defn["measures"][0]["synonyms"] = ["total x"]
        defn["measures"][0]["expr"] = "SUM(x) FILTER(WHERE active)"
        score_without = api_server._compute_mv_health(defn)["score"]
        defn["joins"] = [
            {"name": "unused_tbl", "source": "cat.sch.unused_tbl", "on": "source.id = unused_tbl.id"},
        ]
        score_with = api_server._compute_mv_health(defn)["score"]
        assert score_without == score_with


# ---------------------------------------------------------------------------
# HITL review endpoints exist
# ---------------------------------------------------------------------------
class TestHITLEndpointsExist:
    """Verify that HITL review endpoints are defined on the api_server module."""

    def test_patch_fk_predictions_exists(self):
        assert hasattr(api_server, "patch_fk_prediction")

    def test_reset_review_exists(self):
        assert hasattr(api_server, "reset_review")

    def test_fk_review_body_has_is_fk(self):
        body_cls = api_server.FKReviewBody
        assert "is_fk" in body_cls.__annotations__
        assert "src_column" in body_cls.__annotations__

    def test_reset_review_body_has_level(self):
        body_cls = api_server.ResetReviewBody
        assert "level" in body_cls.__annotations__
        assert "table_name" in body_cls.__annotations__


# ---------------------------------------------------------------------------
# Metric view materialization (Public Preview)
# ---------------------------------------------------------------------------
class TestMaterialization:
    def _defn(self, **over):
        d = {
            "name": "sales_mv",
            "source": "cat.sch.orders",
            "dimensions": [{"name": "order_date", "expr": "o_orderdate"}],
            "measures": [{"name": "total_revenue", "expr": "SUM(o_totalprice)"}],
        }
        d.update(over)
        return d

    def test_request_has_materialize_fields(self):
        ann = api_server.SemanticGenerateRequest.__annotations__
        assert "materialize" in ann
        assert "materialization_schedule" in ann

    def test_build_default_block(self):
        block = api_server._build_materialization(self._defn(), "every 6 hours")
        assert block["mode"] == "relaxed"
        assert block["schedule"] == "every 6 hours"
        assert block["materialized_views"] == [{"name": "sales_mv_baseline", "type": "unaggregated"}]

    def test_validate_default_passes(self):
        d = self._defn()
        d["materialization"] = api_server._build_materialization(d)
        assert api_server._validate_materialization(d) == []

    def test_validate_bad_mode(self):
        d = self._defn(materialization={"mode": "x", "materialized_views": [{"name": "b", "type": "unaggregated"}]})
        assert any("relaxed" in e for e in api_server._validate_materialization(d))

    def test_validate_aggregated_unknown_ref(self):
        d = self._defn(materialization={
            "mode": "relaxed",
            "materialized_views": [{"name": "agg", "type": "aggregated", "dimensions": ["nope"]}],
        })
        assert any("unknown dimension 'nope'" in e for e in api_server._validate_materialization(d))

    def test_yaml_emits_only_when_included(self):
        d = self._defn()
        d["materialization"] = api_server._build_materialization(d)
        assert "materialization" not in api_server._definition_to_yaml(dict(d))
        with_mat = api_server._definition_to_yaml(dict(d), include_materialization=True)
        assert "materialization" in with_mat
        assert "sales_mv_baseline" in with_mat

    def test_create_request_has_materialize_fields(self):
        ann = api_server.CreateDefinitionRequest.__annotations__
        assert "materialize" in ann
        assert "strip_materialization" in ann

    def test_apply_materialization_override_attaches(self):
        d = self._defn()
        req = api_server.CreateDefinitionRequest(
            target_catalog="c", target_schema="s", materialize=True,
        )
        assert api_server._apply_materialization_override(d, req) == []
        assert "materialization" in d

    def test_apply_materialization_override_strips(self):
        d = self._defn()
        d["materialization"] = api_server._build_materialization(d)
        req = api_server.CreateDefinitionRequest(
            target_catalog="c", target_schema="s", strip_materialization=True,
        )
        api_server._apply_materialization_override(d, req)
        assert "materialization" not in d

    def test_yaml_dry_run_passes_include_materialization(self, monkeypatch):
        calls = []

        def fake_execute_sql(sql, timeout=30):
            calls.append(sql)

        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        d = self._defn()
        d["materialization"] = api_server._build_materialization(d)
        err = api_server._yaml_dry_run(d, "cat", "sch", include_materialization=True)
        assert err is None
        assert any("materialization" in c for c in calls)


# ---------------------------------------------------------------------------
# FK vs join-key split (Phase 1)
# ---------------------------------------------------------------------------
class TestFkVsJoinKey:
    """The ERD 'confirm' path must default to join_key so a confirmed join never
    silently becomes an ALTER TABLE ADD CONSTRAINT; only an explicit foreign_key
    assertion is constraint-eligible. Constraint/tag DDL excludes join keys."""

    def test_normalize_kind_defaults_to_join_key(self):
        # No kind, empty, and unknown all fall back to join_key so a confirmed
        # join is never silently promoted to a constraint-eligible FK.
        assert api_server._normalize_fk_kind(None) == "join_key"
        assert api_server._normalize_fk_kind("") == "join_key"
        assert api_server._normalize_fk_kind("banana") == "join_key"

    def test_normalize_kind_honors_explicit_foreign_key(self):
        assert api_server._normalize_fk_kind("foreign_key") == "foreign_key"
        assert api_server._normalize_fk_kind("  Foreign_Key  ") == "foreign_key"

    def test_fk_add_body_defaults_kind_none(self):
        # The request model must not default to a constraint-eligible kind.
        body = api_server.FKAddBody(
            src_column="a", dst_column="b", src_table="c.s.a", dst_table="c.s.b",
        )
        assert body.kind is None
        assert api_server._normalize_fk_kind(body.kind) == "join_key"

    def _fk_body(self, **kw):
        base = dict(src_column="patient_key", dst_column="pat_id",
                    src_table="cat.sch.fct", dst_table="cat.sch.dim")
        base.update(kw)
        return api_server.FKAddBody(**base)

    def test_validate_fk_columns_accepts_bare_columns(self):
        # Valid bare columns must NOT raise.
        api_server._validate_fk_columns(self._fk_body())

    def test_validate_fk_columns_rejects_catalog_as_column(self):
        # The exact corruption: dst_column = the catalog name.
        import pytest
        with pytest.raises(api_server.HTTPException) as e:
            api_server._validate_fk_columns(self._fk_body(dst_column="cat"))
        assert e.value.status_code == 400

    def test_validate_fk_columns_rejects_schema_as_column(self):
        import pytest
        with pytest.raises(api_server.HTTPException):
            api_server._validate_fk_columns(self._fk_body(src_column="sch"))

    def test_validate_fk_columns_rejects_qualified_column(self):
        import pytest
        with pytest.raises(api_server.HTTPException):
            api_server._validate_fk_columns(self._fk_body(dst_column="dim.pat_id"))

    def test_validate_fk_columns_rejects_empty(self):
        import pytest
        with pytest.raises(api_server.HTTPException):
            api_server._validate_fk_columns(self._fk_body(src_column="  "))

    def test_add_fk_prediction_uses_merge_not_insert(self):
        # The write must be idempotent (MERGE), not an appending INSERT that
        # duplicates a pair on every re-save. The endpoint is decorator-mocked so
        # inspect.getsource won't work; read the module source text instead.
        import os as _os
        path = _os.path.join(APP_DIR, "api_server.py")
        with open(path) as f:
            src = f.read()
        # Scope to the fk-add function body.
        start = src.index("def add_fk_prediction(")
        body = src[start:start + 4000]
        assert "MERGE INTO" in body
        assert "WHEN MATCHED THEN UPDATE" in body
        assert "_validate_fk_columns(body)" in body

    def test_fetch_fk_rows_excludes_join_keys(self):
        import inspect as _inspect
        src = _inspect.getsource(api_server._fetch_fk_rows)
        assert "_FK_NOT_JOIN_KEY_SQL" in src

    def test_fetch_fk_rows_gates_is_fk(self):
        # The constraint/tag DDL builder must require is_fk (matches the library's
        # generate_ddl); AI-rejected pairs at final_confidence>=0.5 are not FKs.
        import inspect as _inspect
        src = _inspect.getsource(api_server._fetch_fk_rows)
        assert "is_fk = 'true'" in src

    def test_not_join_key_sql_is_null_safe(self):
        # Legacy NULL and foreign_key survive; only 'join_key' excluded.
        pred = api_server._FK_NOT_JOIN_KEY_SQL
        assert "relationship_kind IS NULL" in pred
        assert "join_key" in pred


class TestInjectFkJoinsComposite:
    """_inject_fk_joins must render a composite condition correctly in BOTH
    directions -- not reduce the reverse direction to a single column."""

    def _run(self, monkeypatch, fk_row, source):
        def fake_execute_sql(sql, timeout=30):
            if sql.strip().upper().startswith("DESCRIBE"):
                return [{"col_name": "relationship_kind"}]
            return [fk_row]
        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        api_server._fk_relationship_cols_ensured = True
        plan = [{"source": source, "question_indices": [0]}]
        out, _ = api_server._inject_fk_joins(plan, [source, "c.s.other"], "c", "s")
        return out[0].get("joins", [])

    def test_source_is_child_forward(self, monkeypatch):
        fk = {"src_table": "c.s.orders", "dst_table": "c.s.lines",
              "src_column": "c.s.orders.order_id", "dst_column": "c.s.lines.order_id",
              "pk_uniqueness": 0.9, "is_composite": True,
              "join_condition": "source.order_id = lines.order_id AND source.line_no = lines.line_no"}
        joins = self._run(monkeypatch, fk, "c.s.orders")
        assert joins[0]["on"] == "source.order_id = lines.order_id AND source.line_no = lines.line_no"

    def test_source_is_parent_reverse_keeps_all_columns(self, monkeypatch):
        # View sourced from the PARENT (lines); the joined child is orders. Every
        # column must survive -- not collapse to a single-column equality.
        fk = {"src_table": "c.s.orders", "dst_table": "c.s.lines",
              "src_column": "c.s.orders.order_id", "dst_column": "c.s.lines.order_id",
              "pk_uniqueness": 0.9, "is_composite": True,
              "join_condition": "source.order_id = lines.order_id AND source.line_no = lines.line_no"}
        joins = self._run(monkeypatch, fk, "c.s.lines")
        on = joins[0]["on"]
        assert " AND " in on                      # both columns present
        assert on == "orders.order_id = source.order_id AND orders.line_no = source.line_no"

    def test_constants_shared_with_library(self):
        # No drift: the app's kind constants ARE the shared fk_constants values.
        from dbxmetagen import fk_constants
        assert api_server._FK_JOIN_KEY == fk_constants.JOIN_KEY
        assert api_server._FK_FOREIGN_KEY == fk_constants.FOREIGN_KEY
        assert api_server._FK_NOT_JOIN_KEY_SQL == fk_constants.NOT_JOIN_KEY_SQL

    def test_ensure_column_returns_bool(self, monkeypatch):
        # #4: the guard must only latch when columns are confirmed present.
        # Table-absent (DESCRIBE throws) => False => guard stays unlatched.
        def boom(sql, timeout=15):
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        monkeypatch.setattr(api_server, "execute_sql", boom)
        assert api_server._ensure_column("c.s.missing", "x", "STRING") is False

        api_server._fk_relationship_cols_ensured = False
        api_server._ensure_fk_relationship_columns()
        assert api_server._fk_relationship_cols_ensured is False

    def test_ensure_columns_latches_when_present(self, monkeypatch):
        # Columns already present => _ensure_column True => guard latches True.
        def desc(sql, timeout=15):
            return [{"col_name": "relationship_kind"}, {"col_name": "is_composite"},
                    {"col_name": "join_condition"}]
        monkeypatch.setattr(api_server, "execute_sql", desc)
        api_server._fk_relationship_cols_ensured = False
        api_server._ensure_fk_relationship_columns()
        assert api_server._fk_relationship_cols_ensured is True


# ---------------------------------------------------------------------------
# FK-candidates evidence payload (Phase 2)
# ---------------------------------------------------------------------------
class TestFkCandidatesEvidence:
    """The join editor must receive the evidence signals (ri_score / join_rate /
    pk_uniqueness / col_similarity / reasoning), not just a bare confidence."""

    def test_num_or_none(self):
        assert api_server._num_or_none(None) is None
        assert api_server._num_or_none("") is None
        assert api_server._num_or_none("nan") is None
        assert api_server._num_or_none(0.12345) == 0.123
        assert api_server._num_or_none("0.5") == 0.5

    def test_candidates_query_selects_evidence(self):
        # get_fk_candidates is a decorated route (a MagicMock under the fastapi
        # mock, not inspectable), so assert the SELECT column list via the module
        # source file text instead.
        import os as _os
        path = _os.path.join(APP_DIR, "api_server.py")
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        # The fk-candidates SELECT must carry the evidence columns.
        assert "ri_score, join_rate, join_matched, pk_uniqueness, col_similarity" in text

    def test_normalize_candidate_passes_evidence(self):
        # Row stored in the requested direction: evidence passes through, rounded.
        row = {
            "src_table": "c.s.orders", "src_column": "c.s.orders.customer_id",
            "dst_table": "c.s.customers", "dst_column": "c.s.customers.id",
            "final_confidence": 0.9123, "ri_score": 0.987654, "join_rate": 0.5,
            "join_matched": 42, "pk_uniqueness": 1.0, "col_similarity": 0.8,
            "ai_reasoning": "name+RI match", "is_fk": True, "relationship_kind": "foreign_key",
        }
        c = api_server._normalize_fk_candidate(row, "c.s.orders")
        assert c["src_column"] == "customer_id" and c["dst_column"] == "id"
        assert c["ri_score"] == 0.988         # rounded to 3dp
        assert c["join_matched"] == 42
        assert c["pk_uniqueness"] == 1.0
        assert c["stored_reversed"] is False
        assert c["reasoning"] == "name+RI match"
        assert c["is_fk"] is True

    def test_normalize_candidate_flags_stored_reversed(self):
        # Row stored parent->child relative to the request => columns swap +
        # stored_reversed flag so the UI can caveat directional signals.
        row = {
            "src_table": "c.s.customers", "src_column": "c.s.customers.id",
            "dst_table": "c.s.orders", "dst_column": "c.s.orders.customer_id",
            "final_confidence": 0.8, "ri_score": None, "join_rate": None,
            "join_matched": None, "pk_uniqueness": None, "col_similarity": None,
            "ai_reasoning": None, "is_fk": None, "relationship_kind": None,
        }
        c = api_server._normalize_fk_candidate(row, "c.s.orders")
        assert c["src_column"] == "customer_id"   # normalized back to requested src
        assert c["dst_column"] == "id"
        assert c["stored_reversed"] is True
        assert c["ri_score"] is None               # absent signal stays None, not 0
        assert c["is_fk"] is None                  # unknown stays None, not False


# ---------------------------------------------------------------------------
# execute_sql chunk-following + truncation cap
# ---------------------------------------------------------------------------
class _FakeState:
    def __init__(self, value):
        self.value = value


class _FakeStatus:
    def __init__(self, value="SUCCEEDED"):
        self.state = _FakeState(value)
        self.error = None


class _FakeCol:
    def __init__(self, name):
        self.name = name


class _FakeSchema:
    def __init__(self, names):
        self.columns = [_FakeCol(n) for n in names]


class _FakeManifest:
    def __init__(self, names, total_row_count=None):
        self.schema = _FakeSchema(names)
        self.total_row_count = total_row_count


class _FakeResult:
    def __init__(self, data_array, next_chunk_index=None):
        self.data_array = data_array
        self.next_chunk_index = next_chunk_index


class _FakeResp:
    def __init__(self, cols, first_chunk, next_chunk_index=None, total_row_count=None):
        self.status = _FakeStatus("SUCCEEDED")
        self.manifest = _FakeManifest(cols, total_row_count=total_row_count)
        self.result = _FakeResult(first_chunk, next_chunk_index=next_chunk_index)
        self.statement_id = "stmt-1"


class _FakeStatementExecution:
    """Simulates a multi-chunk result set: chunk 0 comes on the initial response,
    chunks 1..N are served by get_statement_result_chunk_n."""
    def __init__(self, cols, chunks, total_row_count=None):
        self._cols = cols
        self._chunks = chunks
        self._total = total_row_count

    def execute_statement(self, **kw):
        nxt = 1 if len(self._chunks) > 1 else None
        return _FakeResp(self._cols, self._chunks[0], next_chunk_index=nxt, total_row_count=self._total)

    def get_statement_result_chunk_n(self, statement_id, chunk_index):
        data = self._chunks[chunk_index]
        nxt = chunk_index + 1 if chunk_index + 1 < len(self._chunks) else None
        return _FakeResult(data, next_chunk_index=nxt)


class _FakeClient:
    def __init__(self, se):
        self.statement_execution = se


class TestExecuteSqlChunking:
    def _patch(self, monkeypatch, se):
        monkeypatch.setattr(api_server, "_get_effective_client", lambda: _FakeClient(se))
        monkeypatch.setattr(api_server, "_auth_identity_label", lambda: "test")
        monkeypatch.setenv("WAREHOUSE_ID", "wh123")

    def test_single_chunk(self, monkeypatch):
        se = _FakeStatementExecution(["a"], [[["1"], ["2"]]])
        self._patch(monkeypatch, se)
        rows, truncated = api_server.execute_sql_meta("SELECT a FROM t")
        assert [r["a"] for r in rows] == ["1", "2"]
        assert truncated is False

    def test_follows_all_chunks(self, monkeypatch):
        # 3 chunks of 2 rows each -> all 6 rows returned in order.
        se = _FakeStatementExecution(
            ["a"], [[["1"], ["2"]], [["3"], ["4"]], [["5"], ["6"]]]
        )
        self._patch(monkeypatch, se)
        rows, truncated = api_server.execute_sql_meta("SELECT a FROM t")
        assert [r["a"] for r in rows] == ["1", "2", "3", "4", "5", "6"]
        assert truncated is False

    def test_caps_and_flags_truncation(self, monkeypatch):
        # Two chunks, but cap at 2 rows while total_row_count says 4 -> truncated.
        se = _FakeStatementExecution(
            ["a"], [[["1"], ["2"]], [["3"], ["4"]]], total_row_count=4
        )
        self._patch(monkeypatch, se)
        rows, truncated = api_server.execute_sql_meta("SELECT a FROM t", max_rows=2)
        assert [r["a"] for r in rows] == ["1", "2"]
        assert truncated is True

    def test_execute_sql_wrapper_drops_flag(self, monkeypatch):
        se = _FakeStatementExecution(["a"], [[["1"]]])
        self._patch(monkeypatch, se)
        rows = api_server.execute_sql("SELECT a FROM t")
        assert rows == [{"a": "1"}]


class TestReviewCombinedPagination:
    """review-combined has_more + SQL LIMIT/OFFSET via _review_combined_impl
    (the decorated endpoint isn't callable under the mocked fastapi harness)."""

    def _run(self, monkeypatch, total, page_tables, offset, limit):
        calls = {}

        def fake_execute_sql(query, *a, **kw):
            q = " ".join(query.split())
            if q.startswith("DESCRIBE TABLE"):
                return [{"col_name": "review_status"}]
            if "COUNT(*)" in q:
                return [{"cnt": total}]
            if "FROM tkb" in q and "LIMIT" in q:
                calls["table_query"] = q
                return [
                    {"table_name": f"c.s.{t}", "catalog": "c", "schema": "s",
                     "table_short_name": t, "comment": "", "domain": "", "subdomain": "",
                     "has_pii": False, "has_phi": False, "review_status": "unreviewed"}
                    for t in page_tables
                ]
            return []  # columns / ontology / fk / col_props

        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        res = api_server._review_combined_impl(
            "tkb", "ckb", "ent", "fk", "catalog='c' AND `schema`='s'", offset, limit,
        )
        return res, calls

    def test_first_page_has_more(self, monkeypatch):
        res, calls = self._run(monkeypatch, total=500,
                               page_tables=[f"t{i}" for i in range(200)], offset=0, limit=200)
        assert res["offset"] == 0 and res["limit"] == 200
        assert res["total_count"] == 500
        assert res["has_more"] is True
        assert "LIMIT 200 OFFSET 0" in calls["table_query"]

    def test_last_page_no_more(self, monkeypatch):
        res, _ = self._run(monkeypatch, total=500,
                           page_tables=[f"t{i}" for i in range(100)], offset=400, limit=200)
        assert res["has_more"] is False   # 400 + 100 == 500
        assert res["offset"] == 400

    def test_sql_uses_offset_and_limit(self, monkeypatch):
        _, calls = self._run(monkeypatch, total=10,
                             page_tables=[f"t{i}" for i in range(10)], offset=40, limit=20)
        assert "LIMIT 20 OFFSET 40" in calls["table_query"]

    def test_endpoint_clamps_offset_and_limit(self):
        # Mirror the clamp expressions in review_combined() (which can't be called
        # under the mocked-fastapi harness): offset floored at 0, limit in [1, MAX].
        cap = api_server._REVIEW_PAGE_MAX
        clamp_off = lambda o: max(0, int(o or 0))
        clamp_lim = lambda l: max(1, min(int(l or 200), cap))
        assert clamp_off(-5) == 0
        assert clamp_off(30) == 30
        assert clamp_lim(99999) == cap
        assert clamp_lim(0) == 200   # 0 is falsy -> default 200 (0 is meaningless)
        assert clamp_lim(1) == 1
        assert clamp_lim(200) == 200


# ---------------------------------------------------------------------------
# Suggest business context (item 3)
# ---------------------------------------------------------------------------
class TestSuggestBusinessContext:
    """Draft business context from table descriptions. Default source = live UC
    comments (system.information_schema); use_kb=true reads table_knowledge_base.
    Warn-not-block: no descriptions -> returns a message, not an error."""

    def _patch_llm(self, monkeypatch, text="We are a retailer."):
        import databricks_langchain
        fake_resp = MagicMock()
        fake_resp.content = text
        fake_llm = MagicMock()
        fake_llm.invoke.return_value = fake_resp
        monkeypatch.setattr(databricks_langchain, "ChatDatabricks",
                            MagicMock(return_value=fake_llm), raising=False)

    def test_uc_comments_source_default(self, monkeypatch):
        self._patch_llm(monkeypatch, "Retail analytics context.")
        def fake_execute_sql(sql, timeout=30):
            assert "information_schema.tables" in sql  # default source
            return [{"table_catalog": "c", "table_schema": "s",
                     "table_name": "orders", "comment": "Customer orders fact table"}]
        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        req = api_server.SuggestBusinessContextRequest(table_identifiers=["c.s.orders"])
        res = api_server._suggest_business_context_impl(req)
        assert res["source"] == "uc_comments"
        assert res["tables_used"] == 1
        assert res["business_context"] == "Retail analytics context."

    def test_kb_source_when_flagged(self, monkeypatch):
        self._patch_llm(monkeypatch)
        def fake_execute_sql(sql, timeout=30):
            assert "table_knowledge_base" in sql  # KB source
            return [{"table_name": "c.s.orders", "comment": "Orders", "domain": "sales"}]
        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        req = api_server.SuggestBusinessContextRequest(
            table_identifiers=["c.s.orders"], use_kb=True)
        res = api_server._suggest_business_context_impl(req)
        assert res["source"] == "knowledge_base"
        assert res["tables_used"] == 1

    def test_no_descriptions_returns_message_not_error(self, monkeypatch):
        self._patch_llm(monkeypatch)
        monkeypatch.setattr(api_server, "execute_sql", lambda sql, timeout=30: [])
        req = api_server.SuggestBusinessContextRequest(table_identifiers=["c.s.orders"])
        res = api_server._suggest_business_context_impl(req)
        assert res["tables_used"] == 0
        assert res["business_context"] == ""
        assert res.get("message")

    def test_no_tables_raises(self):
        req = api_server.SuggestBusinessContextRequest(table_identifiers=[])
        with pytest.raises(api_server.HTTPException) as exc:
            api_server._suggest_business_context_impl(req)
        assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# KPI data-enrichment blocks (item 22)
# ---------------------------------------------------------------------------
class TestKpiProfilingBlock:
    """_kpi_profiling_block reads CACHED column_profiling_stats (a local Delta
    table) — NOT the source tables — so it adds zero load to federated sources."""

    def test_empty_tables_returns_empty(self):
        assert api_server._kpi_profiling_block([]) == ""

    def test_reads_profiling_not_source_tables(self, monkeypatch):
        seen = {}
        def fake_execute_sql(sql, timeout=30):
            seen["sql"] = sql
            return [{"table_name": "c.s.orders", "column_name": "status", "data_type": "STRING",
                     "distinct_count": 3, "cardinality_ratio": 0.01, "null_rate": 0.0,
                     "sample_values": '["fulfilled", "pending", "refunded"]',
                     "min_value": None, "max_value": None}]
        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        out = api_server._kpi_profiling_block(["c.s.orders"])
        assert "column_profiling_stats" in seen["sql"]  # cached table, not the source
        assert "DATA PROFILE" in out
        # real categorical values surfaced for FILTER literals
        assert "fulfilled" in out and "pending" in out and "refunded" in out
        assert "distinct=3" in out

    def test_high_cardinality_omits_sample_values(self, monkeypatch):
        monkeypatch.setattr(api_server, "execute_sql", lambda sql, timeout=30: [
            {"table_name": "c.s.orders", "column_name": "amount", "data_type": "DECIMAL",
             "distinct_count": 100000, "cardinality_ratio": 0.99, "null_rate": 0.0,
             "sample_values": '["1.00","2.00"]', "min_value": "0.5", "max_value": "9999.0"}])
        out = api_server._kpi_profiling_block(["c.s.orders"])
        assert "values=[" not in out            # too high-cardinality to list as filter literals
        assert "range=[0.5..9999.0]" in out      # numeric range hint instead

    def test_query_failure_returns_empty(self, monkeypatch):
        monkeypatch.setattr(api_server, "execute_sql",
                            lambda sql, timeout=30: (_ for _ in ()).throw(Exception("no table")))
        assert api_server._kpi_profiling_block(["c.s.orders"]) == ""


class TestKpiColumnRolesBlock:
    def test_formats_roles(self, monkeypatch):
        monkeypatch.setattr(api_server, "execute_sql", lambda sql, timeout=20: [
            {"table_name": "c.s.orders", "column_name": "amount", "property_role": "measure", "linked_entity_type": None},
            {"table_name": "c.s.orders", "column_name": "region", "property_role": "dimension", "linked_entity_type": None},
        ])
        out = api_server._kpi_column_roles_block(["c.s.orders"])
        assert "COLUMN ROLES" in out
        assert "amount=measure" in out and "region=dimension" in out

    def test_empty_returns_empty(self, monkeypatch):
        monkeypatch.setattr(api_server, "execute_sql", lambda sql, timeout=20: [])
        assert api_server._kpi_column_roles_block(["c.s.orders"]) == ""


class TestBuildMvTestQueries:
    """Tests for the metric-view test-query generator (item 23)."""

    def _defn(self, **overrides):
        base = {
            "name": "mv_sales",
            "measures": [
                {"name": "total", "expr": "SUM(x)"},
                {"name": "cnt", "expr": "COUNT(*)"},
            ],
            "dimensions": [
                {"name": "region", "expr": "region"},
                {"name": "quarter", "expr": "quarter"},
            ],
        }
        base.update(overrides)
        return base

    def test_no_measures_returns_empty(self):
        assert api_server._build_mv_test_queries(self._defn(measures=[]), "`c`.`s`.`mv`") == []

    def test_generates_ungrouped_and_per_dim(self):
        qs = api_server._build_mv_test_queries(self._defn(), "`c`.`s`.`mv`")
        kinds = [q["kind"] for q in qs]
        assert kinds[0] == "ungrouped"
        assert kinds.count("single_dim") == 2
        assert "combined_dims" in kinds
        # every query wraps measures in MEASURE() and targets the fq view
        for q in qs:
            assert "MEASURE(`total`)" in q["sql"]
            assert "`c`.`s`.`mv`" in q["sql"]
            assert q["sql"].rstrip().endswith("LIMIT 10")

    def test_dim_cap(self):
        many_dims = [{"name": f"d{i}", "expr": f"d{i}"} for i in range(8)]
        qs = api_server._build_mv_test_queries(self._defn(dimensions=many_dims), "`c`.`s`.`mv`", max_dims=5)
        assert sum(1 for q in qs if q["kind"] == "single_dim") == 5

    def test_filter_query_emitted(self):
        qs = api_server._build_mv_test_queries(self._defn(filter="region = 'NA'"), "`c`.`s`.`mv`")
        filtered = [q for q in qs if q["kind"] == "filtered"]
        assert len(filtered) == 1
        assert "WHERE region = 'NA'" in filtered[0]["sql"]

    def test_no_combined_with_single_dim(self):
        qs = api_server._build_mv_test_queries(
            self._defn(dimensions=[{"name": "region", "expr": "region"}]), "`c`.`s`.`mv`")
        assert "combined_dims" not in [q["kind"] for q in qs]


class TestHealthFromTestResult:
    def test_zero_rows_warns(self):
        h = api_server._health_from_test_result("ungrouped", None, [])
        assert h["status"] == "warn"

    def test_all_null_measures_warns(self):
        h = api_server._health_from_test_result("ungrouped", None, [{"total": None, "cnt": None}])
        assert h["status"] == "warn"
        assert any("NULL" in n for n in h["notes"])

    def test_fanout_duplicate_dimension_warns(self):
        rows = [{"region": "NA", "total": 1}, {"region": "NA", "total": 2}]
        h = api_server._health_from_test_result("single_dim", "region", rows)
        assert h["status"] == "warn"
        assert any("fan-out" in n for n in h["notes"])

    def test_clean_result_ok(self):
        rows = [{"region": "NA", "total": 5}, {"region": "EU", "total": 3}]
        h = api_server._health_from_test_result("single_dim", "region", rows)
        assert h["status"] == "ok"


class TestIsFederatedCatalog:
    def test_empty_false(self):
        assert api_server._is_federated_catalog("") is False

    def test_federation_mode_env(self, monkeypatch):
        monkeypatch.setenv("FEDERATION_MODE", "true")
        assert api_server._is_federated_catalog("anycat") is True

    def test_foreign_catalog_type(self, monkeypatch):
        monkeypatch.setenv("FEDERATION_MODE", "false")
        monkeypatch.setattr(api_server, "execute_sql",
                            lambda sql, timeout=15: [{"catalog_type": "FOREIGN"}])
        assert api_server._is_federated_catalog("snowflake_cat") is True

    def test_managed_catalog_not_federated(self, monkeypatch):
        monkeypatch.setenv("FEDERATION_MODE", "false")
        monkeypatch.setattr(api_server, "execute_sql",
                            lambda sql, timeout=15: [{"catalog_type": "MANAGED_CATALOG"}])
        assert api_server._is_federated_catalog("main") is False

    def test_lookup_error_returns_false(self, monkeypatch):
        monkeypatch.setenv("FEDERATION_MODE", "false")
        def _boom(sql, timeout=15):
            raise RuntimeError("no access")
        monkeypatch.setattr(api_server, "execute_sql", _boom)
        assert api_server._is_federated_catalog("main") is False


class TestSelectMvTestQueries:
    """Hard-bound + federation policy for which drills actually run (item 23)."""

    def _queries(self, n_single=4):
        q = [{"kind": "ungrouped", "label": "grand"}]
        q += [{"kind": "single_dim", "label": f"d{i}"} for i in range(n_single)]
        q += [{"kind": "combined_dims", "label": "combo"}]
        return q

    def test_non_federated_runs_full_set_within_cap(self):
        q = self._queries(n_single=3)  # 1 + 3 + 1 = 5, under the cap
        sel, note = api_server._select_mv_test_queries(q, federated=False, allow_federated_full=False)
        assert len(sel) == 5
        assert note is None

    def test_hard_cap_clamps_large_query_count(self):
        # 1 grand + 20 single + 1 combined = 22, must clamp to _MV_TEST_MAX_QUERIES
        q = self._queries(n_single=20)
        sel, _ = api_server._select_mv_test_queries(q, federated=False, allow_federated_full=False)
        assert len(sel) == api_server._MV_TEST_MAX_QUERIES

    def test_federated_default_caps_small_and_runs(self):
        q = self._queries(n_single=4)
        sel, note = api_server._select_mv_test_queries(q, federated=True, allow_federated_full=False)
        # Runs (not skipped) but capped to the federated max: grand-total + 1 single-dim
        assert len(sel) == api_server._MV_TEST_FEDERATED_MAX
        assert [s["kind"] for s in sel] == ["ungrouped", "single_dim"]
        assert note and "federated" in note.lower()

    def test_federated_full_opt_in_runs_bounded_full_set(self):
        q = self._queries(n_single=4)  # 6 total, under cap
        sel, note = api_server._select_mv_test_queries(q, federated=True, allow_federated_full=True)
        assert len(sel) == 6
        assert note and "full set" in note.lower()

    def test_federated_full_still_respects_hard_cap(self):
        q = self._queries(n_single=20)
        sel, _ = api_server._select_mv_test_queries(q, federated=True, allow_federated_full=True)
        assert len(sel) == api_server._MV_TEST_MAX_QUERIES


class TestMvTestWorkerAndPayload:
    """Background worker + payload shaper (the endpoints themselves are decorator-mocked
    in this harness, so we test the plain functions they delegate to -- same convention
    as the other endpoint tests in this file)."""

    def test_worker_runs_drills_and_finalizes(self, monkeypatch):
        # cachetools is mocked in this harness, so back the task/result stores with real dicts.
        tasks, cache = {}, {}
        monkeypatch.setattr(api_server, "_mv_test_tasks", tasks)
        monkeypatch.setattr(api_server, "_mv_test_result_cache", cache)
        monkeypatch.setattr(api_server, "execute_sql",
                            lambda sql, timeout=45: [{"m1": 5, "d0": "A"}, {"m1": 3, "d0": "B"}])
        queries = [
            {"kind": "ungrouped", "label": "grand", "sql": "SELECT 1"},
            {"kind": "single_dim", "dimension": "d0", "label": "by d0", "sql": "SELECT 2"},
        ]
        tasks["t1"] = {
            "status": "running", "total": 2, "done": 0, "results": [],
            "definition_id": "def1", "cache_key": "ck1",
        }
        api_server._run_mv_test_queries_bg("t1", queries)
        task = tasks["t1"]
        assert task["status"] == "done"
        assert task["summary"]["total"] == 2
        assert task["summary"]["passed"] == 2
        assert task["summary"]["failed"] == 0
        # finished payload is cached for dedupe
        assert "ck1" in cache

    def test_worker_marks_failed_drill(self, monkeypatch):
        tasks = {}
        monkeypatch.setattr(api_server, "_mv_test_tasks", tasks)
        def _boom(sql, timeout=45):
            raise RuntimeError("bad sql")
        monkeypatch.setattr(api_server, "execute_sql", _boom)
        tasks["t2"] = {
            "status": "running", "total": 1, "done": 0, "results": [],
            "definition_id": "def1", "cache_key": None,
        }
        api_server._run_mv_test_queries_bg("t2", [{"kind": "ungrouped", "label": "g", "sql": "SELECT x"}])
        task = tasks["t2"]
        assert task["overall"] == "fail"
        assert task["summary"]["failed"] == 1
        assert task["results"][0]["error"]

    def test_wall_clock_bounds_runaway_query(self, monkeypatch):
        # A drill whose SQL ignores its own timeout must NOT keep the task alive
        # past the wall-clock: the worker fills it as timed-out and returns.
        import time as _time
        tasks = {}
        monkeypatch.setattr(api_server, "_mv_test_tasks", tasks)
        monkeypatch.setattr(api_server, "_mv_test_result_cache", {})
        monkeypatch.setattr(api_server, "_MV_TEST_WALL_TIMEOUT", 2)

        def _slow(sql, timeout=45):
            _time.sleep(30)   # ignores its own timeout (simulates a runaway pull)
            return [{"x": 1}]
        monkeypatch.setattr(api_server, "execute_sql", _slow)
        tasks["tw"] = {"status": "running", "total": 1, "done": 0, "results": [],
                       "definition_id": "d", "cache_key": None}
        t0 = _time.time()
        api_server._run_mv_test_queries_bg("tw", [{"kind": "ungrouped", "label": "g", "sql": "SELECT 1"}])
        elapsed = _time.time() - t0
        assert elapsed < 10          # ~2s wall-clock, not ~30s
        assert tasks["tw"]["status"] == "done"
        assert tasks["tw"]["overall"] == "fail"
        assert tasks["tw"]["summary"]["failed"] == 1

    def test_payload_shape(self):
        task = {"definition_id": "d", "metric_view": "`c`.`s`.`mv`", "federated": True,
                "federation_note": "note", "allow_federated_full": False, "status": "done",
                "total": 3, "done": 3, "overall": "ok",
                "summary": {"total": 3, "passed": 3, "failed": 0, "warned": 0}, "results": []}
        p = api_server._mv_test_task_payload(task)
        assert p["definition_id"] == "d"
        assert p["federated"] is True
        assert p["status"] == "done"
        assert p["summary"]["passed"] == 3

    def test_endpoints_and_model_exist(self):
        assert hasattr(api_server, "run_mv_test_queries")
        assert hasattr(api_server, "poll_mv_test_queries")
        assert "allow_federated_full" in api_server.MvTestQueryRequest.__annotations__


class TestOverlaySavedErdEdges:
    """_overlay_saved_erd must persist edge DELETIONS: a saved edge set is
    authoritative, but a saved ERD with no `edges` key leaves recommendations
    untouched (backward compatible with node-only saves)."""

    def _rec(self):
        return {
            "schema_type": "STAR",
            "nodes": [{"table": "c.s.fct", "role": "fact"},
                      {"table": "c.s.dim", "role": "dimension"}],
            "edges": [
                {"src": "c.s.fct", "dst": "c.s.dim", "on": "src.fk = dim.id", "source": "recommended"},
                {"src": "c.s.fct", "dst": "c.s.dim2", "on": "src.fk2 = dim2.id", "source": "recommended"},
            ],
        }

    def test_no_saved_leaves_edges_untouched(self):
        out = api_server._overlay_saved_erd(self._rec(), None)
        assert len(out["edges"]) == 2

    def test_saved_without_edges_key_preserves_recommendations(self):
        # A node-only save (pre-edges-field, or role-only edit) must NOT wipe edges.
        saved = {"nodes": [{"table": "c.s.fct", "role": "fact"}]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        assert len(out["edges"]) == 2

    def test_saved_edge_subset_drops_deleted_edge(self):
        # User kept only the first edge -> the second must not be re-derived back in.
        saved = {"nodes": [], "edges": [{"src": "c.s.fct", "dst": "c.s.dim", "on": "src.fk = dim.id"}]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        keys = {(e["src"], e["dst"]) for e in out["edges"]}
        assert keys == {("c.s.fct", "c.s.dim")}

    def test_multiple_edges_same_pair_delete_one(self):
        # Two edges between the SAME table pair on different columns (the fact-to-fact
        # case). Deleting ONE must not re-admit it just because the OTHER shares (src,dst).
        rec = {
            "nodes": [{"table": "c.s.f1", "role": "fact"}, {"table": "c.s.f2", "role": "fact"}],
            "edges": [
                {"src": "c.s.f1", "dst": "c.s.f2", "on": "src.a_id = f2.id", "source": "predicted"},
                {"src": "c.s.f1", "dst": "c.s.f2", "on": "src.b_id = f2.id", "source": "predicted"},
            ],
        }
        # User kept only the second column-pair edge.
        saved = {"edges": [{"src": "c.s.f1", "dst": "c.s.f2", "on": "src.b_id = f2.id"}]}
        out = api_server._overlay_saved_erd(rec, saved)
        ons = [e["on"] for e in out["edges"]]
        assert ons == ["src.b_id = f2.id"]   # the deleted a_id edge stays gone

    def test_on_whitespace_normalized_in_match(self):
        # Cosmetic whitespace differences between saved `on` and recommended `on`
        # must still match (so a kept edge isn't wrongly dropped).
        saved = {"edges": [{"src": "c.s.fct", "dst": "c.s.dim", "on": "src.fk  =  dim.id"}]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        assert [e["dst"] for e in out["edges"]] == ["c.s.dim"]

    def test_empty_edge_list_drops_all_edges(self):
        saved = {"nodes": [], "edges": []}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        assert out["edges"] == []

    def test_edge_match_is_case_insensitive(self):
        saved = {"edges": [{"src": "C.S.FCT", "dst": "C.S.DIM", "on": "SRC.FK = DIM.ID"}]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        assert len(out["edges"]) == 1

    def test_edge_direction_is_distinct(self):
        # A saved reverse-direction edge does NOT match the forward recommendation
        # (direction is part of the key), so it is not the SAME edge -- but because
        # the saved set is authoritative, the user-asserted reverse edge is ADDED
        # BACK (not silently dropped), and the unmatched forward recommendation is
        # not re-admitted.
        saved = {"edges": [{"src": "c.s.dim", "dst": "c.s.fct", "on": "src.fk = dim.id"}]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        pairs = {(e["src"], e["dst"]) for e in out["edges"]}
        assert pairs == {("c.s.dim", "c.s.fct")}   # only the saved reverse edge, kept

    def test_user_only_edge_survives_reload(self):
        # An edge the recommender never proposed (hand-drawn, or columns cleared so
        # its `on` no longer matches) MUST survive overlay -- previously it vanished.
        saved = {"edges": [
            {"src": "c.s.fct", "dst": "c.s.dim", "on": "src.fk = dim.id"},   # matches a rec edge
            {"src": "c.s.dim", "dst": "c.s.dim2", "on": ""},                 # user-only, no cols yet
        ]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        pairs = {(e["src"], e["dst"]) for e in out["edges"]}
        assert ("c.s.dim", "c.s.dim2") in pairs   # the user-only edge is kept
        assert ("c.s.fct", "c.s.dim") in pairs

    def test_node_roles_still_overlay(self):
        saved = {"nodes": [{"table": "c.s.dim", "role": "fact", "grain": "day"}],
                 "edges": [{"src": "c.s.fct", "dst": "c.s.dim", "on": "src.fk = dim.id"}]}
        out = api_server._overlay_saved_erd(self._rec(), saved)
        dim = next(n for n in out["nodes"] if n["table"] == "c.s.dim")
        assert dim["role"] == "fact"
        assert dim["grain"] == "day"
        assert dim["user_confirmed"] is True


# ---------------------------------------------------------------------------
# _compute_view_cap -- fact-grain metric-view count cap
# ---------------------------------------------------------------------------
class TestComputeViewCap:
    """The ERD recommendation is the fact-grain-aware floor for the cap; the old
    num_eligible//2 hard_cap crushed it (4 facts -> 2 overlapping views)."""

    def test_erd_recommendation_is_respected_for_facts(self):
        # 4 fact tables selected, ERD recommends 4. Old code capped to 2.
        recommended, hard_cap, effective = api_server._compute_view_cap(
            num_eligible=4, erd_recommended=4, max_views=None
        )
        assert recommended == 4
        assert hard_cap >= 4
        assert effective == 4

    def test_no_erd_falls_back_to_half_tables_heuristic(self):
        recommended, hard_cap, effective = api_server._compute_view_cap(
            num_eligible=4, erd_recommended=0, max_views=None
        )
        # Without an ERD we cannot know the fact count -> conservative heuristic.
        assert hard_cap == 2
        assert effective <= 2

    def test_user_max_views_clamped_to_hard_cap(self):
        _, hard_cap, effective = api_server._compute_view_cap(
            num_eligible=4, erd_recommended=4, max_views=99
        )
        assert effective == hard_cap

    def test_user_max_views_below_recommended_is_honored(self):
        _, _, effective = api_server._compute_view_cap(
            num_eligible=8, erd_recommended=6, max_views=3
        )
        assert effective == 3

    def test_capped_at_global_max(self):
        _, hard_cap, effective = api_server._compute_view_cap(
            num_eligible=200, erd_recommended=50, max_views=None
        )
        assert hard_cap <= 15
        assert effective <= 15

    def test_effective_never_below_one(self):
        _, _, effective = api_server._compute_view_cap(
            num_eligible=0, erd_recommended=0, max_views=None
        )
        assert effective >= 1


# ---------------------------------------------------------------------------
# _coverage_factor -- richness vs source-column-count
# ---------------------------------------------------------------------------
class TestCoverageFactor:
    def test_unknown_columns_is_noop(self):
        for missing in (None, 0, -1):
            cov = api_server._coverage_factor(3, 3, missing)
            assert cov["penalty"] == 0
            assert cov["level"] == "unknown"

    def test_thin_view_over_wide_table_penalized(self):
        # 3 dims + 3 measures over 40 columns -> ratio 0.15 -> heavy penalty.
        cov = api_server._coverage_factor(3, 3, 40)
        assert cov["level"] == "thin"
        assert cov["penalty"] == 10
        assert cov["thin_dims"] is True
        assert cov["thin_measures"] is True

    def test_comprehensive_view_no_penalty(self):
        cov = api_server._coverage_factor(30, 12, 40)
        assert cov["level"] == "comprehensive"
        assert cov["penalty"] == 0

    def test_penalty_lowers_complexity_score(self):
        defn = {
            "source": "c.s.fct",
            "joins": [{"name": "d", "source": "c.s.dim", "on": "source.k = d.k"}],
            "measures": [{"name": f"m{i}", "expr": "SUM(x)"} for i in range(3)],
            "dimensions": [{"name": f"dm{i}", "expr": "col"} for i in range(3)],
        }
        rich = api_server._score_definition_complexity(defn, available_cols=None)
        thin = api_server._score_definition_complexity(defn, available_cols=60)
        assert thin["complexity_score"] < rich["complexity_score"]
        assert thin["coverage_level"] == "thin"


# ---------------------------------------------------------------------------
# _mv_defn_tables -- source + join table extraction
# ---------------------------------------------------------------------------
class TestMvDefnTables:
    def test_collects_source_and_nested_joins(self):
        defn = {
            "source": "c.s.fct",
            "joins": [
                {"source": "c.s.dim_a", "joins": [{"source": "c.s.dim_b"}]},
                {"source": "c.s.dim_c"},
            ],
        }
        tables = api_server._mv_defn_tables(defn)
        assert tables == ["c.s.fct", "c.s.dim_a", "c.s.dim_b", "c.s.dim_c"]

    def test_dedups_preserving_order(self):
        defn = {"source": "c.s.fct", "joins": [{"source": "c.s.fct"}, {"source": "c.s.dim"}]}
        assert api_server._mv_defn_tables(defn) == ["c.s.fct", "c.s.dim"]

    def test_empty_defn(self):
        assert api_server._mv_defn_tables({}) == []


# ---------------------------------------------------------------------------
# _compute_mv_health -- coverage dimension + refinement actions
# ---------------------------------------------------------------------------
class TestMvHealthCoverage:
    def _defn(self):
        return {
            "source": "c.s.fct",
            "comment": "cv",
            "dimensions": [{"name": "d1", "expr": "c1", "comment": "x"}],
            "measures": [{"name": "m1", "expr": "SUM(x)", "comment": "x"}],
        }

    def test_coverage_absent_when_cols_unknown(self):
        r = api_server._compute_mv_health(self._defn(), available_cols=None)
        assert r["max"] == 10
        assert "coverage" not in r["dimensions"]

    def test_coverage_present_and_actions_emitted_for_thin_view(self):
        r = api_server._compute_mv_health(self._defn(), available_cols=40)
        assert r["max"] == 12
        assert "coverage" in r["dimensions"]
        actions = {i.get("action") for i in r["issues"]}
        assert "add_measures" in actions
        assert "add_dimensions" in actions
        assert "check_filters" in actions

    def test_check_filters_not_emitted_when_filter_present(self):
        defn = self._defn()
        defn["filter"] = "c1 IS NOT NULL"
        # Make it non-thin so only the filter action is in question.
        defn["dimensions"] = [{"name": f"d{i}", "expr": "c", "comment": "x"} for i in range(20)]
        defn["measures"] = [{"name": f"m{i}", "expr": "SUM(x)", "comment": "x"} for i in range(10)]
        r = api_server._compute_mv_health(defn, available_cols=25)
        actions = {i.get("action") for i in r["issues"]}
        assert "check_filters" not in actions


# ---------------------------------------------------------------------------
# improve focus directives
# ---------------------------------------------------------------------------
class TestImproveFocusDirectives:
    def test_all_three_actions_have_directives(self):
        for focus in ("add_measures", "add_dimensions", "check_filters"):
            assert focus in api_server._IMPROVE_FOCUS_DIRECTIVES
            assert api_server._IMPROVE_FOCUS_DIRECTIVES[focus].strip()

    def test_improve_request_accepts_focus(self):
        req = api_server.ImproveRequest(focus="add_measures")
        assert req.focus == "add_measures"


# ---------------------------------------------------------------------------
# KPI status filter / bulk delete constants
# ---------------------------------------------------------------------------
class TestKpiStatusValues:
    def test_invalid_is_a_recognized_status(self):
        assert "invalid" in api_server._KPI_STATUS_VALUES

    def test_expected_statuses_present(self):
        assert {"valid", "invalid", "empty", "unchecked", "skipped"} <= api_server._KPI_STATUS_VALUES


class TestDedupKpiSuggestions:
    """_dedup_kpi_suggestions: algorithmic dedup that fixes the repeated-suggest
    plateau (was prompt-reliant only). Drops matches vs existing KPIs and intra-batch
    near-dupes; keeps distinct ones; skips already-invalid suggestions."""

    def test_drops_exact_name_dupe_of_existing(self):
        kpis = [{"name": "Total Revenue", "formula": "SUM(amt)"}]
        out = api_server._dedup_kpi_suggestions(kpis, ["Total Revenue"])
        assert out == []

    def test_keeps_distinct_new_kpis(self):
        kpis = [
            {"name": "Total Revenue", "formula": "SUM(amt)"},
            {"name": "Order Count", "formula": "COUNT(order_id)"},
        ]
        out = api_server._dedup_kpi_suggestions(kpis, ["Average Discount"])
        assert len(out) == 2

    def test_drops_intra_batch_near_dupe(self):
        # Two suggestions with (near-)identical names in the same batch -> keep first.
        kpis = [
            {"name": "Total Revenue", "formula": "SUM(amount)"},
            {"name": "total revenue", "formula": "SUM(amount)"},
        ]
        out = api_server._dedup_kpi_suggestions(kpis, [])
        assert len(out) == 1
        assert out[0]["name"] == "Total Revenue"   # order preserved, first kept

    def test_skips_invalid_suggestions(self):
        kpis = [
            {"name": "Bad KPI", "formula": "SUM(nope)", "validation_status": "invalid"},
            {"name": "Good KPI", "formula": "COUNT(id)"},
        ]
        out = api_server._dedup_kpi_suggestions(kpis, [])
        assert [k["name"] for k in out] == ["Good KPI"]

    def test_empty_inputs(self):
        assert api_server._dedup_kpi_suggestions([], []) == []
        assert api_server._dedup_kpi_suggestions(None, None) == []


class TestValidateKpiFormulaCaps:
    """PQ-4: KPI formula validation must not fan out into a source-query storm.
    Dedup target tables, cap probes at _KPI_VALIDATE_MAX_TABLES, stop at first resolve."""

    def _run(self, monkeypatch, tables, ok_tables):
        probed = []
        def fake_execute_sql(sql, timeout=30):
            # crude: find which table this probe hit
            for t in tables:
                if t in sql:
                    probed.append(t)
                    return [{"kpi_val": 1}] if t in ok_tables else []
            return []
        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)
        status, err, resolved = api_server._validate_kpi_formula("SUM(x)", tables)
        return status, resolved, probed

    def test_stops_at_first_resolving_table(self, monkeypatch):
        # First table resolves -> the rest must NOT be probed.
        status, resolved, probed = self._run(
            monkeypatch, ["c.s.a", "c.s.b", "c.s.c"], ok_tables={"c.s.a"})
        assert status == "valid"
        assert probed == ["c.s.a"]   # only one probe

    def test_dedups_target_tables(self, monkeypatch):
        status, resolved, probed = self._run(
            monkeypatch, ["c.s.a", "c.s.a", "c.s.a"], ok_tables=set())
        assert probed == ["c.s.a"]   # deduped to a single probe

    def test_caps_probe_count(self, monkeypatch):
        many = [f"c.s.t{i}" for i in range(20)]  # none resolve
        status, resolved, probed = self._run(monkeypatch, many, ok_tables=set())
        assert len(probed) <= api_server._KPI_VALIDATE_MAX_TABLES

    def test_empty_is_skipped(self, monkeypatch):
        monkeypatch.setattr(api_server, "execute_sql", lambda *a, **k: [])
        assert api_server._validate_kpi_formula("", ["c.s.a"])[0] == "skipped"
        assert api_server._validate_kpi_formula("SUM(x)", [])[0] == "skipped"


# ---------------------------------------------------------------------------
# _parse_json_array
# ---------------------------------------------------------------------------
class TestParseJsonArray:
    def test_plain_array(self):
        out = api_server._parse_json_array('[{"name": "a", "expr": "SUM(x)"}]')
        assert out == [{"name": "a", "expr": "SUM(x)"}]

    def test_fenced_array(self):
        out = api_server._parse_json_array('```json\n[{"name": "a"}]\n```')
        assert out == [{"name": "a"}]

    def test_object_wrapped_measures(self):
        out = api_server._parse_json_array('{"measures": [{"name": "m1"}]}')
        assert out == [{"name": "m1"}]

    def test_object_wrapped_items(self):
        out = api_server._parse_json_array('{"items": [{"name": "d1"}]}')
        assert out == [{"name": "d1"}]

    def test_no_array_raises(self):
        import pytest as _pytest
        with _pytest.raises(ValueError):
            api_server._parse_json_array("no json here")


# ---------------------------------------------------------------------------
# _compute_mv_health: joined-dimension numeric-aggregation fan-out detector
# ---------------------------------------------------------------------------
class TestDimAggFanoutDetector:
    def _numeric_cols(self, monkeypatch, cols_by_table):
        # execute_sql is called once per joined table with a column-metadata query.
        def fake_execute_sql(query, *a, **k):
            for tbl, rows in cols_by_table.items():
                if tbl in query or tbl.split(".")[-1] in query:
                    return rows
            return []
        monkeypatch.setattr(api_server, "execute_sql", fake_execute_sql)

    def _dim_issues(self, health):
        return [i for i in health["issues"] if i.get("action") == "split_dim_measure"]

    def test_flags_sum_over_joined_dim_numeric(self, monkeypatch):
        self._numeric_cols(monkeypatch, {
            "c.s.dim_account": [{"column_name": "bed_count", "data_type": "INT"}],
        })
        defn = {
            "source": "c.s.fct_rx",
            "joins": [{"name": "acct", "source": "c.s.dim_account", "on": "source.acct_id = acct.id"}],
            "measures": [{"name": "beds", "expr": "SUM(acct.bed_count)"}],
            "dimensions": [{"name": "d", "expr": "acct.name"}],
        }
        health = api_server._compute_mv_health(defn)
        issues = self._dim_issues(health)
        assert len(issues) == 1
        assert issues[0]["severity"] == "medium"  # no FK evidence supplied

    def test_high_severity_with_fk_confirmation(self, monkeypatch):
        self._numeric_cols(monkeypatch, {
            "c.s.dim_account": [{"column_name": "bed_count", "data_type": "INT"}],
        })
        defn = {
            "source": "c.s.fct_rx",
            "joins": [{"name": "acct", "source": "c.s.dim_account", "on": "source.acct_id = acct.id"}],
            "measures": [{"name": "beds", "expr": "SUM(acct.bed_count)"}],
            "dimensions": [{"name": "d", "expr": "acct.name"}],
        }
        fk = [{"src_table": "c.s.fct_rx", "dst_table": "c.s.dim_account"}]
        health = api_server._compute_mv_health(defn, fk_rows=fk)
        issues = self._dim_issues(health)
        assert issues and issues[0]["severity"] == "high"

    def test_no_flag_for_source_column_aggregation(self, monkeypatch):
        self._numeric_cols(monkeypatch, {
            "c.s.dim_account": [{"column_name": "bed_count", "data_type": "INT"}],
        })
        # Aggregates a SOURCE column, not a join-alias column -> correct, no flag.
        defn = {
            "source": "c.s.fct_rx",
            "joins": [{"name": "acct", "source": "c.s.dim_account", "on": "source.acct_id = acct.id"}],
            "measures": [{"name": "total", "expr": "SUM(source.amount)"}],
            "dimensions": [{"name": "d", "expr": "acct.name"}],
        }
        health = api_server._compute_mv_health(defn)
        assert self._dim_issues(health) == []

    def test_no_flag_for_non_numeric_dim_column(self, monkeypatch):
        # A count over a categorical join column is not the fan-out pattern we flag
        # (COUNT is not additive here; and the column is not numeric).
        self._numeric_cols(monkeypatch, {
            "c.s.dim_account": [{"column_name": "region", "data_type": "STRING"}],
        })
        defn = {
            "source": "c.s.fct_rx",
            "joins": [{"name": "acct", "source": "c.s.dim_account", "on": "source.acct_id = acct.id"}],
            "measures": [{"name": "m", "expr": "SUM(acct.region)"}],
            "dimensions": [{"name": "d", "expr": "acct.name"}],
        }
        health = api_server._compute_mv_health(defn)
        assert self._dim_issues(health) == []

    def test_degrades_when_column_lookup_fails(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("no warehouse")
        monkeypatch.setattr(api_server, "execute_sql", boom)
        defn = {
            "source": "c.s.fct_rx",
            "joins": [{"name": "acct", "source": "c.s.dim_account", "on": "source.acct_id = acct.id"}],
            "measures": [{"name": "beds", "expr": "SUM(acct.bed_count)"}],
            "dimensions": [{"name": "d", "expr": "acct.name"}],
        }
        # Must not raise; just skips the detector.
        health = api_server._compute_mv_health(defn)
        assert self._dim_issues(health) == []


class TestGenieDeployHelpers:
    """Helpers backing the Genie MV-preservation fix and dropped-content surfacing."""

    def test_mv_names_extracts_trailing_name(self):
        ss = {"data_sources": {"metric_views": [
            {"identifier": "cat.sch.mv_sales"},
            {"identifier": "cat.sch.mv_returns"},
        ]}}
        assert api_server._mv_names_from_serialized_space(ss) == ["mv_sales", "mv_returns"]

    def test_mv_names_empty_when_no_metric_views(self):
        assert api_server._mv_names_from_serialized_space({"data_sources": {"tables": [{"identifier": "c.s.t"}]}}) == []
        assert api_server._mv_names_from_serialized_space({}) == []

    def test_mv_names_skips_blank_identifiers(self):
        ss = {"data_sources": {"metric_views": [{"identifier": ""}, {"identifier": "c.s.mv"}, {}]}}
        assert api_server._mv_names_from_serialized_space(ss) == ["mv"]

    def test_content_counts_all_categories(self):
        ss = {"instructions": {
            "join_specs": [{"a": 1}, {"b": 2}],
            "example_question_sqls": [{"q": 1}],
            "sql_snippets": {"measures": [{"m": 1}], "filters": [{"f": 1}, {"f": 2}], "expressions": []},
        }}
        counts = api_server._genie_content_counts(ss)
        assert counts == {"joins": 2, "example_sqls": 1, "snippets": 3}

    def test_content_counts_handles_empty_and_missing(self):
        assert api_server._genie_content_counts({}) == {"joins": 0, "example_sqls": 0, "snippets": 0}
        # legacy example_sql key + join_specs under data_sources
        ss = {"data_sources": {"join_specs": [{"a": 1}]}, "instructions": {"example_sql": [{"q": 1}]}}
        counts = api_server._genie_content_counts(ss)
        assert counts["joins"] == 1 and counts["example_sqls"] == 1


class TestGenieHealthScoreMetricViewsNA:
    """metric_views health dimension is N/A (excluded from max) for a tables-only space."""

    def _score(self, tables, mvs):
        ss = {
            "data_sources": {
                "tables": [{"identifier": t} for t in tables],
                "metric_views": [{"identifier": m} for m in mvs],
            },
            "instructions": {},
            "sample_questions": [],
        }
        return api_server._compute_health_score(ss)

    def test_tables_only_metric_views_na_and_max_18(self):
        h = self._score(["c.s.orders", "c.s.customers"], [])
        assert h["dimensions"]["metric_views"]["score"] is None
        assert h["dimensions"]["metric_views"]["max"] == 0
        assert h["max"] == 18  # 20 minus the excluded 2-pt metric_views dimension

    def test_mv_space_scores_metric_views_and_max_20(self):
        h = self._score([], ["c.s.mv1", "c.s.mv2"])
        assert h["dimensions"]["metric_views"]["score"] == 2
        assert h["dimensions"]["metric_views"]["max"] == 2
        assert h["max"] == 20

    def test_mixed_space_counts_metric_views_and_max_20(self):
        h = self._score(["c.s.orders"], ["c.s.mv1"])
        assert h["dimensions"]["metric_views"]["score"] == 1  # 1 MV present -> not N/A
        assert h["max"] == 20

    def test_empty_space_metric_views_stays_zero(self):
        # No tables AND no MVs -> not "tables-only", keep the 0/2 penalty (degenerate space).
        h = self._score([], [])
        assert h["dimensions"]["metric_views"]["score"] == 0
        assert h["dimensions"]["metric_views"]["max"] == 2
        assert h["max"] == 20


class TestExecuteSqlMetaPagination:
    """Regression: the result-chunk pagination loop must terminate on a malformed or
    non-advancing next_chunk_index instead of spinning forever. An unbounded loop pins
    a worker thread in production (and caused a ~20-min unit-test hang on a MagicMock
    whose next_chunk_index is perpetually truthy)."""

    from types import SimpleNamespace as _NS

    def _resp(self, data_array, next_chunk_index):
        NS = self._NS
        return NS(
            status=NS(state=NS(value="SUCCEEDED"), error=None),
            manifest=NS(schema=NS(columns=[NS(name="c")]), total_row_count=len(data_array)),
            statement_id="s1",
            result=NS(data_array=data_array, next_chunk_index=next_chunk_index),
        )

    def _ws(self, resp, chunk_fn):
        NS = self._NS
        return NS(statement_execution=NS(
            execute_statement=lambda **k: resp,
            get_statement=lambda sid: resp,
            get_statement_result_chunk_n=chunk_fn,
        ))

    def test_non_advancing_chunk_index_terminates(self, monkeypatch):
        calls = {"n": 0}
        def _chunk(sid, idx):
            calls["n"] += 1
            return self._resp([["z"]], idx).result
        resp = self._resp([["a"]], 0)  # next_chunk_index 0 does NOT advance past chunk 0
        monkeypatch.setattr(api_server, "_get_effective_client", lambda: self._ws(resp, _chunk))
        rows, truncated = api_server.execute_sql_meta("SELECT 1", warehouse_id="wh")
        assert rows == [{"c": "a"}]
        assert truncated is True
        assert calls["n"] == 0  # guard broke BEFORE fetching another chunk (no infinite loop)

    def test_non_int_chunk_index_terminates(self, monkeypatch):
        # A non-int next_chunk_index (e.g. a MagicMock in tests, or a malformed API value)
        resp = self._resp([["a"]], object())
        monkeypatch.setattr(api_server, "_get_effective_client",
                            lambda: self._ws(resp, lambda sid, idx: (_ for _ in ()).throw(AssertionError("should not fetch"))))
        rows, truncated = api_server.execute_sql_meta("SELECT 1", warehouse_id="wh")
        assert rows == [{"c": "a"}]
        assert truncated is True

    def test_advancing_chunks_are_followed(self, monkeypatch):
        NS = self._NS
        chunk1 = NS(data_array=[["b"]], next_chunk_index=None)
        seen = {"idx": None}
        def _chunk(sid, idx):
            seen["idx"] = idx
            return chunk1
        resp = self._resp([["a"]], 1)  # advances 0 -> 1, then chunk1 ends the stream
        monkeypatch.setattr(api_server, "_get_effective_client", lambda: self._ws(resp, _chunk))
        rows, truncated = api_server.execute_sql_meta("SELECT 1", warehouse_id="wh")
        assert rows == [{"c": "a"}, {"c": "b"}]
        assert truncated is False
        assert seen["idx"] == 1

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

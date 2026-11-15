"""Unit tests for UC Pages generation (pure logic + injected I/O; no Databricks connection).

API behaviours asserted here were verified live against the Pages API (see uc_pages module doc).
"""

import json

import pytest

from dbxmetagen import uc_pages as up
from dbxmetagen import uc_pages_pipeline as pp
from dbxmetagen import uc_pages_quality as uq
from dbxmetagen import uc_pages_store as us
from dbxmetagen.evaluation import uc_pages_eval as ev

DOMAINS = [
    {"name": "domains/d1", "domain_id": "d1", "tag_key": "clinical", "label": "clinical", "parent_domain_id": "", "description": ""},
    {"name": "domains/d2", "domain_id": "d2", "tag_key": "clinical/allergy", "label": "clinical/allergy", "parent_domain_id": "d1", "description": ""},
    {"name": "domains/d3", "domain_id": "d3", "tag_key": "finance", "label": "finance", "parent_domain_id": "", "description": ""},
]
CTX = {
    "tables": [
        {"table_name": "cat.emr.allergy", "comment": "Allergy assertions", "domain": "clinical", "subdomain": "allergy", "reviewed": "false"},
        {"table_name": "cat.emr.fact_encounter", "comment": "Encounters", "domain": "clinical", "subdomain": None, "reviewed": "true"},
    ],
    "columns": {"cat.emr.allergy": [{"column_name": "allergy_type", "data_type": "string", "comment": "Type", "classification": "None", "reviewed": "false"}]},
    "entities": [{"entity_id": "e1", "entity_name": "Allergy", "entity_type": "Allergy", "description": "An allergy", "source_tables": ["cat.emr.allergy"], "auto_discovered": "true"}],
    "fks": [{"src_table": "cat.emr.allergy", "src_column": "pat_id", "dst_table": "cat.emr.fact_encounter", "dst_column": "pat_id", "reviewed": "false"}],
    "customer_context": [{"context_id": "c1", "scope": "cat.emr.allergy", "scope_type": "table", "context_label": "", "context_text": "NKA rows are documented negatives."}],
    "acronyms": [{"acronym_id": "a1", "abbreviation": "NKA", "expansion": "No Known Allergies", "notes": ""}],
    "metric_views": [{"metric_view_name": "allergy_metrics", "fqn": "cat.res.allergy_metrics", "source_table": "cat.emr.allergy",
                      "json_definition": json.dumps({"measures": [{"name": "screened", "expr": "COUNT(1)"}]})}],
    "source_chunks": [{"chunk_id": "s1:0", "title": "Glossary", "heading": "NKA", "text": "NKA means no known allergies."}],
}


@pytest.fixture
def evidence():
    return up.build_evidence(CTX)


# ---------------------------------------------------------------- evidence

def test_evidence_authority_classes_and_human_first(evidence):
    by_type = {}
    for e in evidence:
        by_type.setdefault(e["type"], set()).add(e["authority"])
    assert by_type["customer_context"] == {"human"} and by_type["acronym"] == {"human"} and by_type["source_doc"] == {"human"}
    assert by_type["metric_view"] == {"governed"} and by_type["ontology_entity"] == {"inferred"}
    assert by_type["table_comment"] == {"inferred", "human"}  # reviewed table comment is human
    order = [up.AUTHORITY_ORDER[e["authority"]] for e in evidence]
    assert order == sorted(order, reverse=True)
    assert [e["id"] for e in evidence] == [f"E{i}" for i in range(1, len(evidence) + 1)]


def test_steward_locked_entity_is_human():
    ctx = {**CTX, "entities": [{**CTX["entities"][0], "auto_discovered": "false"}]}
    ent = [e for e in up.build_evidence(ctx) if e["type"] == "ontology_entity"][0]
    assert ent["authority"] == "human"


def test_evidence_level_strongest_wins(evidence):
    by = {e["id"]: e for e in evidence}
    inferred = [e["id"] for e in evidence if e["authority"] == "inferred"]
    human = [e["id"] for e in evidence if e["authority"] == "human"]
    assert up.evidence_level(inferred, by) == "inferred"
    assert up.evidence_level(inferred + human[:1], by) == "human"
    assert up.evidence_level(["E999"], by) == "none"


def test_render_evidence_subset_and_budget(evidence):
    txt = up.render_evidence(evidence, ids=["E1"])
    assert txt.startswith("[E1]") and "[E2]" not in txt
    assert "truncated" in up.render_evidence(evidence, budget_chars=60)


# ---------------------------------------------------------------- plan normalization

def _plan(*cands):
    return {"candidates": list(cands)}


def _cand(**over):
    c = {"display_name": "No Known Allergies", "kind": "term", "definition": "An allergy status value that…",
         "synonyms": ["NKA"], "related_tables": ["cat.emr.allergy"], "related_metric_views": ["cat.res.allergy_metrics"],
         "evidence": ["E1"], "recommend": True}
    c.update(over)
    return c


def test_plan_selects_human_backed_and_validates_assets(evidence):
    out = up.normalize_plan(_plan(_cand(related_tables=["CAT.EMR.ALLERGY", "cat.emr.invented"])), ctx=CTX, evidence=evidence,
                            domains=DOMAINS, default_domain="domains/d2")
    c = out[0]
    assert c["selected"] and c["evidence_level"] == "human" and c["domain"] == "domains/d2"
    assert c["related_tables"] == ["cat.emr.allergy"] and c["related_metric_views"] == ["cat.res.allergy_metrics"]
    assert "unknown_assets_dropped" in [f["code"] for f in c["flags"]]


def test_inferred_only_shown_but_unselected(evidence):
    inferred = [e["id"] for e in evidence if e["authority"] == "inferred"][:1]
    c = up.normalize_plan(_plan(_cand(evidence=inferred)), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    assert not c["selected"] and "inferred_only" in [f["code"] for f in c["flags"]]


def test_invalid_citations_and_low_evidence(evidence):
    c = up.normalize_plan(_plan(_cand(evidence=["E999"])), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    codes = [f["code"] for f in c["flags"]]
    assert "invalid_citations" in codes and "low_evidence" in codes and not c["selected"]


def test_table_shaped_only_for_identifiers(evidence):
    assert up.is_table_shaped("fact_encounter", ["cat.emr.fact_encounter"])
    assert not up.is_table_shaped("Encounter", ["cat.emr.encounter"])  # concept sharing a table name is fine
    assert not up.is_table_shaped("Allergy", ["cat.emr.allergy"])
    c = up.normalize_plan(_plan(_cand(display_name="fact_encounter", kind="entity")), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    assert not c["selected"] and "table_shaped" in [f["code"] for f in c["flags"]]


def test_data_product_requires_opt_in(evidence):
    c = up.normalize_plan(_plan(_cand(kind="data_product")), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    assert c["kind"] != "data_product" and "kind_not_enabled" in [f["code"] for f in c["flags"]]
    c = up.normalize_plan(_plan(_cand(kind="data_product")), ctx=CTX, evidence=evidence, domains=DOMAINS,
                          kinds=up.DEFAULT_KINDS + ["data_product"])[0]
    assert c["kind"] == "data_product"
    assert "data_product" not in up.DEFAULT_KINDS and "wiki" in up.KINDS["data_product"]["help"]


def test_duplicates_existing_page_and_draft(evidence):
    existing = [{"name": "pages/p1", "display_name": "no known allergies", "domain": "domains/d2"}]
    c = up.normalize_plan(_plan(_cand()), ctx=CTX, evidence=evidence, domains=DOMAINS, existing_pages=existing,
                          default_domain="domains/d2")[0]
    assert c["uc_page_name"] == "pages/p1" and "duplicate_existing_page" in [f["code"] for f in c["flags"]]
    c = up.normalize_plan(_plan(_cand()), ctx=CTX, evidence=evidence, domains=DOMAINS,
                          existing_drafts=[{"display_name": "No Known Allergies", "status": "drafted"}])[0]
    assert not c["selected"] and "duplicate_draft" in [f["code"] for f in c["flags"]]


def test_relations_resolve_to_candidate_ids(evidence):
    out = up.normalize_plan(_plan(_cand(display_name="Allergy", kind="entity"),
                                  _cand(broader="Allergy", related=["Encounter", "Allergy"])),
                            ctx=CTX, evidence=evidence, domains=DOMAINS)
    allergy, nka = out
    rel = {r["type"]: r for r in nka["relations"]}
    assert rel["broader"]["target_page_id"] == allergy["page_id"]
    assert [r for r in nka["relations"] if r["target"] == "Encounter"][0]["target_page_id"] == ""


def test_domain_fallback_from_kb(evidence):
    c = up.normalize_plan(_plan(_cand(domain="")), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    assert c["domain"] == "domains/d2"  # clinical/allergy from the table's classified domain


# ---------------------------------------------------------------- citations + write

def test_parse_citations_strips_and_maps():
    body = "## Definition\nNKA is a negative assertion. [E1, E2]\n\n- Uncited bullet\n| a | b |\n|---|---|\n## Rule\nNever filter. [E9]"
    r = up.parse_citations(body, {"E1", "E2"})
    assert "[E" not in r["body"] and r["invalid_ids"] == ["E9"]
    assert r["claims"][0] == {"section": "Definition", "claim": "NKA is a negative assertion.", "evidence_ids": ["E1", "E2"]}
    assert r["cited_lines"] == 1 and r["content_lines"] == 4


def test_finalize_builds_provenance_and_footer(evidence):
    cand = up.normalize_plan(_plan(_cand()), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    parsed = {"description": "A documented negative allergy assertion. [E1]", "synonyms": ["NKA", "nka"],
              "body": "## Definition\nA documented negative. [E1]", "open_questions": ["Which column?"]}
    page = up.finalize_written_page(cand, parsed, evidence, ["E1", "E2"])
    assert page["description"] == "A documented negative allergy assertion."
    assert "## Open questions for the steward" in page["body"] and "_Drafted with dbxmetagen" in page["body"]
    assert page["body_cited"].endswith("[E1]") and page["synonyms"] == ["NKA"]
    assert {p["section"] for p in page["provenance"]} >= {"Description", "Definition"}


def test_write_and_revise_with_fake_llm(evidence):
    cand = up.normalize_plan(_plan(_cand()), ctx=CTX, evidence=evidence, domains=DOMAINS)[0]
    calls = []

    def llm(prompts):
        calls.append(len(prompts))
        return [(json.dumps({"display_name": "No Known Allergies", "description": "A documented negative assertion.",
                             "synonyms": ["NKA"], "body": "## Definition\nA documented negative. [E1]"}), None) for _ in prompts]

    pages = up.write_pages(llm=llm, candidates=[cand], evidence=evidence)
    assert pages[0]["status"] == "drafted" and calls == [1]
    rev = up.revise_pages(llm=llm, pages=pages, issues_by_page={pages[0]["page_id"]: ["Invented synonym"]}, evidence=evidence)
    assert rev[0]["body"].startswith("## Definition")


def test_llm_retry_on_bad_output():
    seq = iter([[("not json", None)], [('{"body": "x"}', None)]])
    out = up._call_with_retry(lambda ps: next(seq), ["p"], ok=lambda t: bool(up.extract_json(t).get("body")))
    assert out == [('{"body": "x"}', None)]


# ---------------------------------------------------------------- quality

GOOD = {"display_name": "Availability Factor", "kind": "metric", "description": "Share of time a plant was technically able to produce.",
        "synonyms": ["AF"], "related_tables": ["cat.ops.availability"], "related_metric_views": ["cat.ops.availability_mv"],
        "body": "## Definition\n" + "Available hours divided by period hours. " * 12, "domain": "domains/d1"}


def test_quality_good_page_passes_form_checks():
    r = uq.deterministic_checks(GOOD, scope_tables=["cat.ops.availability"])
    failed = [k for k, v in r.items() if v["applicable"] and not v["pass"]]
    assert failed == []


@pytest.mark.parametrize("over,check", [
    ({"description": "This metric is important for reporting."}, "description_is_definition"),
    ({"description": "Availability factor is the availability factor of a plant."}, "description_not_circular"),
    ({"description": ""}, "has_description"),
    ({"body": GOOD["body"] + " Call 555-123-4567."}, "no_pii"),
    ({"body": GOOD["body"] + " See `cat.other.secret_table`."}, "assets_in_scope"),
    ({"related_metric_views": []}, "metric_links_metric_view"),
    ({"display_name": "fact_availability", "description": "A fact table."}, "not_table_shaped"),
])
def test_quality_catches_defects(over, check):
    page = {**GOOD, **over}
    r = uq.deterministic_checks(page, scope_tables=["cat.ops.availability", "cat.ops.fact_availability"])
    assert r[check]["applicable"] and not r[check]["pass"]


def test_genus_reuse_is_not_circular():
    page = {**GOOD, "display_name": "Raw Material", "description": "A material used as input in production."}
    assert uq.deterministic_checks(page)["description_not_circular"]["pass"]


def test_score_blocking_caps_and_judge_blend():
    checks = uq.deterministic_checks({**GOOD, "body": GOOD["body"] + " jane@example.org"}, scope_tables=["cat.ops.availability"])
    s = uq.score(checks)
    assert s["blocking"] == ["no_pii"] and s["score"] <= 40
    clean = uq.deterministic_checks(GOOD, scope_tables=["cat.ops.availability"])
    j = {"scores": {k: 5.0 for k in uq.JUDGE_CRITERIA}}
    assert uq.score(clean, j)["score"] == 100.0 and uq.score(clean, {"scores": {k: 1.0 for k in uq.JUDGE_CRITERIA}})["score"] == 50.0


def test_citation_checks_from_stats():
    page = {**GOOD, "citation_stats": {"cited_lines": 3, "content_lines": 10, "invalid_ids": ["E7"]}, "evidence_ids": []}
    r = uq.deterministic_checks(page, evidence_ids=[], evidence_by_id={})
    assert not r["citation_coverage"]["pass"] and not r["citations_valid"]["pass"] and not r["has_human_evidence"]["pass"]


def test_revision_issues_and_judge_parse():
    j = uq.parse_judge('```json\n{"scores":{"faithfulness":2,"definition_quality":9,"usefulness":"x","human_context_fidelity":null,"granularity":4},'
                       '"unsupported_claims":["NKDA invented"]}\n```')
    assert j["scores"]["faithfulness"] == 2 and j["scores"]["definition_quality"] == 5 and j["scores"]["usefulness"] is None
    issues = uq.revision_issues({"judge": j, "checks": uq.deterministic_checks(GOOD)})
    assert issues == ["NKDA invented"]


def test_links_not_applicable_for_uc_read_pages():
    page = {k: v for k, v in GOOD.items() if k not in ("related_tables", "related_metric_views")}
    assert not uq.deterministic_checks(page)["links_assets"]["applicable"]


# ---------------------------------------------------------------- store (SQL builders)

FQ = lambda t: f"`c`.`s`.`{t}`"  # noqa: E731


def test_upsert_sql_binds_everything():
    row = {"page_id": "p'1", "display_name": "O'Brien; DROP", "synonyms": ["a", "b"], "selected": True, "quality": {"x": 1}}
    sql, params = us.build_upsert_drafts([row], FQ)
    assert "O'Brien" not in sql and "DROP" not in sql
    assert params["synonyms_0"] == '["a", "b"]' and params["selected_0"] == "true" and json.loads(params["quality_0"]) == {"x": 1}
    assert "from_json(:synonyms_0, 'ARRAY<STRING>')" in sql and "MERGE INTO `c`.`s`.`page_drafts`" in sql
    sql2, _ = us.build_upsert_drafts([row], FQ, columns=["quality_score"])
    assert "UPDATE SET t.quality_score = s.quality_score WHEN NOT MATCHED" in sql2


def test_decode_draft_roundtrip():
    d = us.decode_draft({"synonyms": '["NKA"]', "flags": '[{"code":"x"}]', "quality": "", "selected": "true",
                         "uc_links": None, "quality_score": "81.5", "related_tables": None})
    assert d["synonyms"] == ["NKA"] and d["flags"] == [{"code": "x"}] and d["quality"] == {} and d["selected"] is True
    assert d["uc_links"] == {} and d["quality_score"] == 81.5 and d["related_tables"] == []


def test_apply_edit_only_editable_fields():
    d, changed = us.apply_edit({"display_name": "A", "status": "drafted"}, {"display_name": "B", "status": "published", "uc_page_name": "x"})
    assert changed == ["display_name"] and d["status"] == "drafted" and d["edited_by_user"] and "uc_page_name" not in d


def test_merge_drafts_unions():
    keep = {"display_name": "Allergy", "synonyms": ["allergy record"], "related_tables": ["t1"], "evidence_ids": ["E1"], "evidence_level": "inferred", "relations": []}
    other = {"display_name": "Allergy Assertion", "synonyms": ["assertion"], "related_tables": ["t2"], "evidence_ids": ["E2"], "evidence_level": "human", "relations": []}
    m = us.merge_drafts(keep, [other])
    assert m["synonyms"] == ["allergy record", "Allergy Assertion", "assertion"]
    assert m["related_tables"] == ["t1", "t2"] and m["evidence_level"] == "human"


def test_chunk_text_headings_and_limits():
    doc = "# Glossary\nIntro para.\n\n## NKA\n" + ("NKA means no known allergies. " * 80) + "\n\n## Encounter\nA visit."
    chunks = us.chunk_text(doc, max_chars=500)
    assert {c["heading"] for c in chunks} == {"Glossary", "NKA", "Encounter"}
    assert all(len(c["text"]) <= 650 for c in chunks) and [c["ordinal"] for c in chunks] == list(range(len(chunks)))


def test_parsed_document_to_text():
    parsed = json.dumps({"document": {"elements": [{"type": "title", "content": "Policy"}, {"type": "text", "content": "Body."}]}})
    assert us.parsed_document_to_text(parsed) == "## Policy\n\nBody."
    assert us.parsed_document_to_text("plain") == "plain"


def test_store_source_is_idempotent_and_bound():
    calls = []
    doc = "# A\nzq'secret body; DROP TABLE x"
    us.store_source(lambda s, p: calls.append((s, p)) or [], FQ, title="G", text=doc, uri="https://x", now="2026-01-01 00:00:00")
    sid = us.source_id_for("G", doc)
    assert sid == us.source_id_for("G", doc)  # deterministic id -> re-ingest replaces, never duplicates
    assert calls[0] == ("DELETE FROM `c`.`s`.`page_source_chunks` WHERE source_id = :sid", {"sid": sid})
    assert all("zq'secret" not in s for s, _ in calls)  # document text only ever travels as a bound value
    assert any("zq'secret" in str(v) for _, p in calls for v in p.values())


# ---------------------------------------------------------------- REST: publish + links

class FakeApi:
    """Mimics verified API behaviour: duplicate names allowed, idempotent links, PATCH needs a mask."""

    def __init__(self, uc_body=None, fail_create=False):
        self.calls, self.uc_body, self.fail_create, self.n = [], uc_body, fail_create, 0

    def __call__(self, method, path, query=None, body=None):
        self.calls.append((method, path, query, body))
        if path == "/.well-known/databricks-config":
            return {"account_id": "acct"}
        if method == "GET" and path == up.DOMAINS_PATH:
            return {"domains": [{"name": "domains/d1", "tag_key": "clinical"}]} if not (query or {}).get("page_token") else {}
        if method == "POST" and path == up.PAGES_PATH:
            if self.fail_create:
                raise RuntimeError("PERMISSION_DENIED")
            self.n += 1
            return {"name": f"pages/new{self.n}", "state": body["state"], **{k: body[k] for k in ("display_name", "description", "body", "synonyms")}}
        if method == "GET" and path.startswith("/api/2.0/knowledge/pages/"):
            return self.uc_body or {}
        if method == "PATCH":
            assert query and query.get("update_mask")
            return {"name": path.split("/knowledge/")[1], "state": "DRAFT", **body}
        if "asset-links" in path and method == "POST":
            return {"name": f"{path}/{body['asset_id']}"}
        if method == "DELETE":
            return {}
        raise AssertionError((method, path))


def _page(**over):
    p = {"page_id": "a", "display_name": "Allergy", "description": "d", "body": "## Definition\nx", "synonyms": ["s"],
         "domain": "domains/d1", "related_tables": ["cat.emr.allergy"], "related_metric_views": ["cat.res.mv"],
         "relations": [], "uc_page_name": "", "uc_links": {}}
    p.update(over)
    return p


def test_publish_create_then_links_including_page_to_page():
    api = FakeApi()
    pages = [_page(), _page(page_id="b", display_name="NKA", relations=[{"type": "broader", "target": "Allergy", "target_page_id": "a"}])]
    res = up.publish(api, pages, metastore_id="ms", account="acct")
    assert [r["status"] for r in res] == ["ok", "ok"]
    nka_links = res[1]["uc_links"]
    assert "RELATED_ASSETS|ASSET_TYPE_ACCOUNT_PAGE|new1" in nka_links
    assert "RELATED_ASSETS|ASSET_TYPE_UC_TABLE|cat.res.mv" in nka_links  # metric view links as a UC table
    assert res[0]["published_hash"] == up.content_hash("Allergy", "d", "## Definition\nx", ["s"])


def test_publish_reconciles_removed_links():
    api = FakeApi()
    prev = {"RELATED_ASSETS|ASSET_TYPE_UC_TABLE|cat.emr.old": "pages/p1/asset-link-categories/RELATED_ASSETS/asset-links/l1"}
    res = up.publish(api, [_page(uc_page_name="pages/p1", uc_links=prev)], metastore_id="ms", account="acct")
    assert ("DELETE", "/api/2.0/knowledge/pages/p1/asset-link-categories/RELATED_ASSETS/asset-links/l1", None, None) in api.calls
    assert "RELATED_ASSETS|ASSET_TYPE_UC_TABLE|cat.emr.old" not in res[0]["uc_links"]


def test_publish_detects_uc_side_edits():
    api = FakeApi(uc_body={"display_name": "Allergy", "description": "edited in UC", "body": "x", "synonyms": []})
    p = _page(uc_page_name="pages/p1", published_hash=up.content_hash("Allergy", "d", "## Definition\nx", ["s"]))
    assert up.publish(api, [p], metastore_id="ms")[0]["status"] == "edited_in_uc"
    assert up.publish(api, [p], metastore_id="ms", force=True)[0]["status"] == "ok"


def test_draft_republish_never_demotes_published():
    path, q, body = up.build_update(_page(uc_page_name="pages/p1"), "DRAFT")
    assert "state" not in q["update_mask"] and "state" not in body and "domain" not in body
    _, q2, _ = up.build_update(_page(uc_page_name="pages/p1"), "PUBLISHED")
    assert q2["update_mask"].endswith(",state")


def test_publish_errors_are_per_page():
    res = up.publish(FakeApi(fail_create=True), [_page(), _page(page_id="b", domain="")], metastore_id="ms")
    assert [r["status"] for r in res] == ["error", "error"] and "PERMISSION_DENIED" in res[0]["error"]


def test_probe_never_raises():
    def boom(*a, **k):
        raise RuntimeError("404")
    assert up.probe(boom)["available"] is False
    assert up.probe(lambda *a, **k: {"domains": []})["available"] is False
    assert up.probe(FakeApi())["available"] is True


# ---------------------------------------------------------------- context gathering

def test_gather_context_bound_graceful_and_patterns():
    seen = []

    def run_sql(sql, params):
        seen.append((sql, params))
        if "table_knowledge_base" in sql:
            return CTX["tables"]
        if "fk_predictions" in sql:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        if "scope_type = 'pattern'" in sql:
            return [{"context_id": "p1", "scope": "*allergy", "scope_type": "pattern", "context_text": "grain"}]
        if "metric_view_definitions" in sql:
            return [{"metric_view_name": "mv", "deployed_catalog": "c", "deployed_schema": "s", "source_table": "cat.emr.allergy"}]
        return []

    ctx = up.gather_context(run_sql, FQ, catalog="zz'cat", schema="emr", prompt="allergy")
    assert ctx["fks"] == [] and [c["context_id"] for c in ctx["customer_context"]] == ["p1"]
    assert ctx["metric_views"][0]["fqn"] == "c.s.mv"
    assert all("zz'cat" not in s for s, _ in seen)


# ---------------------------------------------------------------- eval harness

def test_concept_recall_matching():
    pages = [{"display_name": "Allergy and Intolerance", "synonyms": []}, {"display_name": "No Known Allergies (NKA)", "synonyms": ["NKA"]},
             {"display_name": "Data Latency Rule", "synonyms": []}]
    r = ev.concept_recall(pages, [{"name": "Allergy", "aliases": ["Allergy and Intolerance"]}, {"name": "No Known Allergies", "aliases": ["NKA"]},
                                  {"name": "Encounter"}])
    assert r["recall"] == round(2 / 3, 3) and r["missing"] == ["Encounter"] and r["unmatched_pages"] == ["Data Latency Rule"]


def test_fact_recall_scoring():
    judge = lambda ps: [(json.dumps({"facts": {"F1": {"status": "stated"}, "F2": {"status": "partial"}, "F3": {"status": "contradicted"}},  # noqa: E731
                                     "forbidden": {"X1": {"status": "asserted"}}}), None)]
    gold = {"required_facts": [{"id": "F1", "statement": "a"}, {"id": "F2", "statement": "b"}, {"id": "F3", "statement": "c"}],
            "forbidden": [{"id": "X1", "statement": "x"}]}
    r = ev.fact_recall([{"display_name": "p", "body": "b"}], gold, judge)
    assert r["fact_recall"] == 0.5 and r["contradictions"] == ["F3", "X1"]


def test_pipeline_plan_run_without_persistence():
    def run_sql(sql, params):
        return CTX["tables"] if "table_knowledge_base" in sql else []

    llm = lambda ps: [(json.dumps({"candidates": [_cand(evidence=["E1"])]}), None)]  # noqa: E731
    res = pp.plan_run(run_sql=run_sql, fq=FQ, llm=llm, api_do=FakeApi(), catalog="cat", schema="emr", prompt="allergy pages please",
                      now="2026-01-01 00:00:00", persist=False)
    assert res["error"] is None and res["run_id"] and res["candidates"][0]["run_id"] == res["run_id"]


# ---------------------------------------------------------------- judge panel, revision loop, baseline

def test_combine_judgments_panel():
    a = {"scores": {"faithfulness": 2.0, "definition_quality": 4.0}, "unsupported_claims": ["x"], "parsed": True}
    b = {"scores": {"faithfulness": 4.0, "definition_quality": None}, "unsupported_claims": ["x", "y"], "parsed": True}
    c = uq.combine_judgments([a, b, {"error": "timeout", "scores": {}}])
    assert c["scores"]["faithfulness"] == 3.0 and c["scores"]["definition_quality"] == 4.0
    assert c["unsupported_claims"] == ["x", "y"] and len(c["panel"]) == 2
    assert uq.combine_judgments([{"error": "e", "scores": {}}])["error"] == "e"


def test_overlong_body_triggers_revision():
    long_page = {**GOOD, "body": "## Definition\n" + "word " * 1200}
    issues = uq.revision_issues({"judge": {}, "checks": uq.deterministic_checks(long_page)})
    assert any("characters; cut it" in i for i in issues)


def test_write_run_keeps_better_revision(evidence):
    """Regression: a kept revision must be what write_run returns (it used to return the original)."""
    cand = {**up.normalize_plan(_plan(_cand()), ctx=CTX, evidence=evidence, domains=DOMAINS, default_domain="domains/d2")[0], "run_id": "r"}

    def writer(ps):
        return [(json.dumps({"display_name": "No Known Allergies", "description": "A documented negative allergy assertion.",
                             "body": "## Definition\n" + ("Revised." if "REVIEWER ISSUES" in p else "First.") + " [E1]"}), None) for p in ps]

    def judge(ps):
        out = []
        for p in ps:
            f = 5 if "Revised." in p else 2
            out.append((json.dumps({"scores": {k: (f if k == "faithfulness" else 4) for k in uq.JUDGE_CRITERIA},
                                    "unsupported_claims": [] if f >= 4 else ["unsupported"]}), None))
        return out

    res = pp.write_run(run_sql=None, fq=None, llm=writer, judge_llm=judge, page_ids=[cand["page_id"]], drafts=[cand],
                       run={"evidence": evidence}, now="n", persist=False)
    rev = res[0]["quality"]["revision"]
    assert rev["kept"] and rev["score_after"] > rev["score_before"] and "Revised." in res[0]["body"]


def test_baseline_steps_down_on_truncation(evidence):
    calls = []

    def llm(ps):
        n = int(ps[0].split("Write at most ")[1].split()[0])
        calls.append(n)
        if n > 3:
            return [('{"pages": [{"display_name": "A", "body": "trunc', None)]
        return [(json.dumps({"pages": [{"display_name": "A", "description": "d", "body": "b"}]}), None)]

    attempts = []
    pages = ev.baseline_pages(llm=llm, evidence=evidence, prompt="p", max_pages=12, attempts=attempts)
    assert calls == [12, 6, 3] and len(pages) == 1 and attempts[0]["parsed_pages"] == 0

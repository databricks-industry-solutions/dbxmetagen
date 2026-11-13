"""Generate Unity Catalog **Pages** (governed business-concept definitions) from a user prompt and
the context dbxmetagen has already accumulated, with claim-level provenance.

What a Page is (docs + field guidance + ISO 704, see docs/ notes): one governed definition of ONE
business concept within one Domain: an entity, a term/acronym, the *meaning* of a metric, a
business rule, or a process. Not a table (that is a wiki), not a grouping (that is a Domain), and
not the governed calculation (that is a metric view, which the Page links to). Data-product
overview Pages are an explicit opt-in.

Flow (each step persists to the editable ``page_drafts`` store, see ``uc_pages_store``):

1. **Plan**: gather evidence for the scope → ONE LLM call proposes candidate concepts (kind,
   one-line definition, cited evidence) → deterministic validation and triage flags (duplicate,
   inferred-only, table-shaped, low evidence) → candidates the user curates.
2. **Write**: per selected candidate, a kind-specific template prompt over focused evidence; every
   factual sentence must cite ``[E#]``. Citations become ``page_evidence`` rows and are stripped
   from the published body.
3. **Evaluate**: ``uc_pages_quality`` (deterministic checks + rubric judge).
4. **Publish**: create/update via the Pages REST API (Draft by default) and reconcile asset links
   (tables, metric views, related Pages, source URLs). Links are persisted because the API has no
   list endpoint. Edits made in UC since our last publish are detected and not overwritten.

Authority classes drive selection and trust:
* ``human``: customer context, acronyms, ingested source documents, steward-reviewed comments,
  steward-locked ontology entities.
* ``governed``: deployed (applied) metric views, steward-confirmed FKs.
* ``inferred``: AI-generated comments, auto-discovered entities, predicted FKs, classified domains.
A candidate grounded ONLY in inferred evidence is shown but unselected by default: Genie ranks a
Page above the tables it describes, so publishing machine guesses as Pages would launder them
into the most authoritative context in the estate.

All I/O is injected so the app, the job and the eval harness share one implementation:
``run_sql(sql, params) -> list[dict]`` (named ``:param`` binding), ``api_do(method, path,
query=None, body=None) -> dict`` and ``llm(prompts: list[str]) -> list[(text, error)]``.

Pages API behaviour verified live (2026-11-13): duplicate display names in a Domain are ALLOWED
(dedup is our job); asset links have NO list endpoint; a UC table link accepts the FQN (a UUID
resolves to the same link); metric views link as ``ASSET_TYPE_UC_TABLE``; page→page links take
the bare page id with the account UUID as scope; duplicate links are idempotent; PATCH requires
``update_mask``; synonyms are trimmed + sorted (case-sensitive dedupe), max 50.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

PAGES_PATH = "/api/2.0/knowledge/pages"
DOMAINS_PATH = "/api/2.0/domains"

VALID_STATES = ("DRAFT", "PUBLISHED")
MAX_DISPLAY_NAME = 200
MAX_SYNONYMS = 50
MAX_SYNONYM_LEN = 200
MAX_DESCRIPTION = 1000
MAX_BODY_CHARS = 60_000
MAX_CANDIDATES = 40
MAX_WRITE_BATCH = 25

DEFAULT_MAX_TABLES = 40
DEFAULT_MAX_COLUMNS_PER_TABLE = 25
DEFAULT_CONTEXT_CHARS = 120_000
KB_CANDIDATE_CAP = 2000

RunSql = Callable[[str, Dict[str, Any]], List[Dict[str, Any]]]
ApiDo = Callable[..., Any]
Llm = Callable[[List[str]], List[Tuple[str, Optional[str]]]]

PAGES_UNAVAILABLE_HINT = (
    "UC Pages / Domains are not reachable from this workspace with the current identity. Pages "
    "require Unity Catalog semantics (Pages + Domains) to be enabled, at least one Domain to exist, "
    "and the caller to hold Page/Domain permissions (listing needs MANAGE DISCOVER or MANAGE "
    "DOMAIN). Under OBO the user's token also needs the 'knowledge' API scope."
)

# ---------------------------------------------------------------------------
# Page kinds: what a Page can be, and the body template for each
# ---------------------------------------------------------------------------

KINDS: Dict[str, Dict[str, Any]] = {
    "entity": {
        "label": "Business entity",
        "help": "A real-world thing the business tracks, e.g. Customer, Encounter, Sales Agreement.",
        "sections": ["Definition", "Business use", "How it is represented in the data", "Relationships", "Examples"],
        "default": True,
    },
    "metric": {
        "label": "Metric meaning",
        "help": "What a measure means: scope, exclusions, interpretation. The calculation itself is "
                "governed by a metric view, which the Page links to rather than re-defines.",
        "sections": ["Definition", "Business use", "How it is calculated", "Where to find it", "Caveats"],
        "default": True,
    },
    "term": {
        "label": "Business term or acronym",
        "help": "A term, code, or acronym people must understand, e.g. NKA, OLO, FWA.",
        "sections": ["Definition", "Business use", "Examples"],
        "default": True,
    },
    "rule": {
        "label": "Business rule",
        "help": "A rule for interpreting data correctly, e.g. 'fiscal year starts Feb 1', 'never "
                "average margin', 'NKA rows are documented negatives, not missing data'.",
        "sections": ["Rule", "Why it matters", "How to apply it in the data", "Examples"],
        "default": True,
    },
    "process": {
        "label": "Process or lifecycle",
        "help": "A business process or lifecycle, e.g. order fulfillment, admission to discharge.",
        "sections": ["Definition", "Stages", "How it is represented in the data", "Business use"],
        "default": True,
    },
    "data_product": {
        "label": "Data product overview (opt-in)",
        "help": "A consumer-facing overview of a curated set of tables treated as a product: purpose, "
                "contents, how to use it. Off by default: Pages are designed for business concepts. "
                "Documentation of a single table belongs in a wiki, and grouping assets belongs in a "
                "Domain. Enable this only when a set of tables is a product with its own meaning, and "
                "link its concepts to their own Pages instead of repeating them.",
        "sections": ["Purpose", "What's included", "How to use it", "Quality and freshness", "Related concepts"],
        "default": False,
    },
}
DEFAULT_KINDS = [k for k, v in KINDS.items() if v["default"]]

AUTHORITY_ORDER = {"human": 3, "governed": 2, "inferred": 1}

# Calibrated on 40 real published Pages (2026-11-13): body p10/p50/p90 = 344/668/3315 chars;
# 37/40 have a one-sentence description; 36/40 have synonyms; "Definition" heads 26/40 bodies.
BODY_TARGET_CHARS = (300, 2500)

# ---------------------------------------------------------------------------
# Small pure helpers
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOP = {
    "the", "a", "an", "and", "or", "of", "for", "to", "in", "on", "with", "our", "we", "is", "are",
    "page", "pages", "create", "make", "build", "write", "about", "each", "all", "that", "this",
    "table", "tables", "data", "please", "concept", "concepts", "glossary", "term", "terms", "it",
    "be", "by", "as", "at", "from", "which", "what", "how", "key", "core", "main", "new",
}
_CITE_RE = re.compile(r"\s*\[(E\d+(?:\s*,\s*E\d+)*)\]")


def _tokens(text: Any) -> set:
    toks = set(_TOKEN_RE.findall(str(text or "").lower().replace("_", " ")))
    return {t for t in toks if len(t) > 1 and t not in _STOP}


def _clip(text: Any, n: int) -> str:
    s = str(text or "").strip()
    return s if len(s) <= n else s[: max(n - 1, 0)].rstrip() + "…"


def norm_name(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    if isinstance(v, str):
        t = v.strip()
        if t.startswith("["):
            try:
                parsed = json.loads(t)
                return parsed if isinstance(parsed, list) else [parsed]
            except Exception:
                pass
        return [x.strip() for x in t.split(",") if x.strip()] if t else []
    return [v]


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes", "t")


def normalize_state(state: Any) -> str:
    s = str(state or "DRAFT").strip().upper()
    return s if s in VALID_STATES else "DRAFT"


def normalize_kind(kind: Any, allowed: Optional[Sequence[str]] = None) -> str:
    k = norm_name(kind).replace(" ", "_")
    aliases = {"kpi": "metric", "measure": "metric", "acronym": "term", "business_term": "term",
               "concept": "entity", "business_rule": "rule", "lifecycle": "process", "data_product_overview": "data_product"}
    k = aliases.get(k, k)
    if k not in KINDS:
        k = "entity"
    if allowed is not None and k not in allowed:
        return ""
    return k


def sanitize_synonyms(values: Iterable[Any], display_name: str = "") -> List[str]:
    """Trim, drop blanks / the display name itself, dedupe case-insensitively, clamp to API limits."""
    seen = {norm_name(display_name)} if display_name else set()
    out: List[str] = []
    for v in values or []:
        s = str(v or "").strip()
        if not s or len(s) > MAX_SYNONYM_LEN:
            continue
        k = norm_name(s)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(s)
        if len(out) >= MAX_SYNONYMS:
            break
    return out


def _in_params(prefix: str, values: List[str]) -> Tuple[str, Dict[str, Any]]:
    params = {f"{prefix}{i}": v for i, v in enumerate(values)}
    return ", ".join(f":{k}" for k in params), params


def content_hash(display_name: Any, description: Any, body: Any, synonyms: Iterable[Any]) -> str:
    """Stable hash of the user-visible Page fields (for UC-side edit detection)."""
    payload = json.dumps([str(display_name or ""), str(description or ""), str(body or ""),
                          sorted(str(s) for s in (synonyms or []))], ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Domains + Pages REST (thin, injectable)
# ---------------------------------------------------------------------------

def _paginate(api_do: ApiDo, path: str, key: str, query: Optional[Dict[str, Any]] = None,
              limit: int = 5000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    q = dict(query or {})
    while True:
        resp = api_do("GET", path, query=q) or {}
        out.extend(resp.get(key) or [])
        tok = resp.get("next_page_token")
        if not tok or len(out) >= limit:
            return out[:limit]
        q = {**q, "page_token": tok}


def _domain_id(d: Dict[str, Any]) -> str:
    name = str(d.get("name") or "")
    return d.get("domain_id") or (name.split("/", 1)[1] if name.startswith("domains/") else name)


def list_domains(api_do: ApiDo) -> List[Dict[str, Any]]:
    """All Domains (and subdomains). ``label`` is the governed tag key, which slash-encodes the
    hierarchy (``Parent/Sub``); Domains have no separate display name. Accounts can hold >1000."""
    raw = _paginate(api_do, DOMAINS_PATH, "domains", {"page_size": 100})
    out = []
    for d in raw:
        did = _domain_id(d)
        out.append({
            "name": d.get("name") or f"domains/{did}",
            "domain_id": did,
            "tag_key": d.get("tag_key") or "",
            "label": d.get("tag_key") or did,
            "parent_domain_id": d.get("parent_domain_id") or "",
            "description": d.get("subtitle") or d.get("description") or "",
        })
    return sorted(out, key=lambda x: x["label"].lower())


def list_pages(api_do: ApiDo, domain: Optional[str] = None, limit: int = 2000) -> List[Dict[str, Any]]:
    """Page summaries (list responses omit body/description/synonyms; GET a page for those)."""
    q: Dict[str, Any] = {"page_size": 100}
    if domain:
        q["filter"] = f'domain = "{domain}"'
    return _paginate(api_do, PAGES_PATH, "pages", q, limit=limit)


def get_page(api_do: ApiDo, page_name: str) -> Dict[str, Any]:
    return api_do("GET", f"/api/2.0/knowledge/{page_name}") or {}


def probe(api_do: ApiDo) -> Dict[str, Any]:
    """Capability check: can this identity see Domains? Never raises."""
    try:
        domains = list_domains(api_do)
    except Exception as exc:
        return {"available": False, "reason": f"{PAGES_UNAVAILABLE_HINT} ({_clip(exc, 300)})", "domains": []}
    if not domains:
        return {"available": False, "domains": [],
                "reason": "No UC Domains exist yet. A Page must belong to a Domain; create one in "
                          "Catalog → Discover → Domains, then return here."}
    return {"available": True, "reason": None, "domains": domains}


def account_id(api_do: ApiDo) -> Optional[str]:
    """Account UUID (scope id for page→page and external-URL links)."""
    try:
        return (api_do("GET", "/.well-known/databricks-config") or {}).get("account_id")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Context gathering (reads ONLY already-persisted dbxmetagen output)
# ---------------------------------------------------------------------------

def _safe(run_sql: RunSql, sql: str, params: Dict[str, Any], what: str) -> List[Dict[str, Any]]:
    try:
        return run_sql(sql, params) or []
    except Exception as exc:
        logger.info("uc_pages: %s unavailable (%s)", what, _clip(exc, 200))
        return []


def rank_tables(rows: List[Dict[str, Any]], prompt: str, max_tables: int,
                pinned: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """Order KB rows by lexical relevance to the prompt; pinned tables first; stable otherwise."""
    pin = {p.lower() for p in (pinned or [])}
    q = _tokens(prompt)

    def score(r):
        name = str(r.get("table_name") or "")
        hay = _tokens(name) | _tokens(r.get("comment")) | _tokens(r.get("domain")) | _tokens(r.get("subdomain"))
        return (name.lower() in pin, len(q & _tokens(name.rsplit(".", 1)[-1])) * 3 + len(q & hay))

    indexed = list(enumerate(rows))
    indexed.sort(key=lambda ir: (score(ir[1]), -ir[0]), reverse=True)
    return [r for _, r in indexed[:max_tables]]


def rank_chunks(chunks: List[Dict[str, Any]], query: str, limit: int) -> List[Dict[str, Any]]:
    """Top source-document chunks for a query by lexical overlap (title/heading weighted)."""
    q = _tokens(query)
    if not q:
        return chunks[:limit]

    def score(c):
        head = _tokens(c.get("heading")) | _tokens(c.get("title"))
        return len(q & head) * 2 + len(q & _tokens(c.get("text")))

    scored = [(score(c), i, c) for i, c in enumerate(chunks)]
    scored = [s for s in scored if s[0] > 0] or scored
    scored.sort(key=lambda s: (-s[0], s[1]))
    return [c for _, _, c in scored[:limit]]


def gather_context(
    run_sql: RunSql,
    fq: Callable[[str], str],
    *,
    catalog: str,
    schema: str = "",
    tables: Optional[List[str]] = None,
    prompt: str = "",
    source_ids: Optional[List[str]] = None,
    max_tables: int = DEFAULT_MAX_TABLES,
    max_columns_per_table: int = DEFAULT_MAX_COLUMNS_PER_TABLE,
    max_source_chunks: int = 400,
    include: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """Collect accumulated context for a scope. Every source is optional (missing table = empty).

    ``fq(name)`` maps a dbxmetagen output table to its FQN in the results schema. ``tables`` are
    full names and pin the scope. ``source_ids`` restricts ingested documents (None = all active).
    """
    inc = {"columns": True, "ontology": True, "fks": True, "customer_context": True,
           "acronyms": True, "metric_views": True, "source_docs": True, **(include or {})}
    tables = [t.strip() for t in (tables or []) if t and t.strip()]

    where, params = ["catalog = :cat"], {"cat": catalog}
    if schema:
        where.append("`schema` = :sch")
        params["sch"] = schema
    if tables:
        ph, tp = _in_params("t", tables)
        where.append(f"table_name IN ({ph})")
        params.update(tp)
    kb_rows = _safe(run_sql,
                    f"SELECT table_name, comment, domain, subdomain, has_pii, has_phi, "
                    f"CAST(review_updated_at IS NOT NULL AS STRING) AS reviewed "
                    f"FROM {fq('table_knowledge_base')} WHERE {' AND '.join(where)} "
                    f"ORDER BY table_name LIMIT {KB_CANDIDATE_CAP}", params, "table_knowledge_base")
    chosen = rank_tables(kb_rows, prompt, max_tables, pinned=tables)
    names = [r["table_name"] for r in chosen if r.get("table_name")]
    ctx: Dict[str, Any] = {
        "scope": {"catalog": catalog, "schema": schema, "tables": tables},
        "tables": chosen, "table_candidates": len(kb_rows),
        "columns": {}, "entities": [], "fks": [], "customer_context": [], "acronyms": [],
        "metric_views": [], "source_chunks": [],
    }
    if inc["source_docs"]:
        if source_ids:
            sph, sp = _in_params("src", list(source_ids))
            src_where, src_params = f"s.source_id IN ({sph})", sp
        else:
            src_where, src_params = "1 = 1", {}
        ctx["source_chunks"] = _safe(run_sql,
                                     f"SELECT c.chunk_id, c.source_id, c.ordinal, c.heading, c.text, s.title, s.uri "
                                     f"FROM {fq('page_source_chunks')} c JOIN {fq('page_sources')} s "
                                     f"ON c.source_id = s.source_id WHERE s.active = true AND {src_where} "
                                     f"ORDER BY s.title, c.ordinal LIMIT {int(max_source_chunks)}",
                                     src_params, "page_source_chunks")
    if not names:
        return ctx
    ph, tp = _in_params("t", names)

    if inc["columns"]:
        rows = _safe(run_sql,
                     f"SELECT table_name, column_name, data_type, comment, classification, "
                     f"CAST(review_updated_at IS NOT NULL AS STRING) AS reviewed "
                     f"FROM {fq('column_knowledge_base')} WHERE table_name IN ({ph}) "
                     f"ORDER BY table_name, column_name", tp, "column_knowledge_base")
        cols: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            lst = cols.setdefault(r.get("table_name"), [])
            if len(lst) < max_columns_per_table:
                lst.append(r)
        ctx["columns"] = cols

    if inc["ontology"]:
        ctx["entities"] = _safe(run_sql,
                                f"SELECT entity_id, entity_name, entity_type, description, source_tables, "
                                f"CAST(auto_discovered AS STRING) AS auto_discovered "
                                f"FROM {fq('ontology_entities')} "
                                f"WHERE confidence >= 0.5 AND arrays_overlap(source_tables, array({ph})) "
                                f"ORDER BY confidence DESC LIMIT 200", tp, "ontology_entities")

    if inc["fks"]:
        ctx["fks"] = _safe(run_sql,
                           f"SELECT src_table, src_column, dst_table, dst_column, final_confidence, "
                           f"CAST(review_updated_at IS NOT NULL AS STRING) AS reviewed "
                           # CAST: is_fk is BOOLEAN on new tables but STRING on some legacy ones.
                           f"FROM {fq('fk_predictions')} WHERE lower(CAST(is_fk AS STRING)) = 'true' "
                           f"AND src_table != dst_table AND final_confidence >= 0.5 "
                           f"AND (src_table IN ({ph}) OR dst_table IN ({ph})) "
                           f"ORDER BY final_confidence DESC LIMIT 200", tp, "fk_predictions")

    if inc["customer_context"]:
        scopes = [catalog] + ([f"{catalog}.{schema}"] if schema else []) + names
        sph, sp = _in_params("s", scopes)
        ctx["customer_context"] = _safe(run_sql,
                                        f"SELECT context_id, scope, scope_type, context_label, context_text "
                                        f"FROM {fq('customer_context')} WHERE active = true "
                                        f"AND scope IN ({sph}) ORDER BY priority DESC LIMIT 200",
                                        sp, "customer_context")
        # Pattern scopes (e.g. '*allergy') apply to any table whose name matches.
        pats = _safe(run_sql,
                     f"SELECT context_id, scope, scope_type, context_label, context_text "
                     f"FROM {fq('customer_context')} WHERE active = true AND scope_type = 'pattern' LIMIT 200",
                     {}, "customer_context patterns")
        have = {r.get("context_id") for r in ctx["customer_context"]}
        for p in pats:
            pat = str(p.get("scope") or "").strip().lower()
            rx = re.compile("^" + re.escape(pat).replace(r"\*", ".*") + "$") if pat else None
            if rx and p.get("context_id") not in have and any(rx.match(n.lower()) or rx.match(n.lower().rsplit(".", 1)[-1]) for n in names):
                ctx["customer_context"].append(p)

    if inc["acronyms"]:
        scopes = [catalog] + ([f"{catalog}.{schema}"] if schema else [])
        sph, sp = _in_params("a", scopes)
        ctx["acronyms"] = _safe(run_sql,
                                f"SELECT acronym_id, abbreviation, expansion, notes FROM {fq('customer_acronyms')} "
                                f"WHERE active = true AND scope IN ({sph}) LIMIT 500",
                                sp, "customer_acronyms")

    if inc["metric_views"]:
        mvs = _safe(run_sql,
                    f"SELECT * FROM {fq('metric_view_definitions')} WHERE source_table IN ({ph}) "
                    f"AND status = 'applied' LIMIT 50", tp, "metric_view_definitions")
        for m in mvs:  # deployed FQN = deployed_catalog.deployed_schema.metric_view_name when recorded
            cat, sch = m.get("deployed_catalog"), m.get("deployed_schema")
            m["fqn"] = f"{cat}.{sch}.{m.get('metric_view_name')}" if cat and sch else ""
        ctx["metric_views"] = mvs
    return ctx


def _mv_measures(json_definition: Any) -> List[Dict[str, str]]:
    try:
        d = json.loads(json_definition) if isinstance(json_definition, str) else (json_definition or {})
    except Exception:
        return []
    out = []
    for m in (d.get("measures") or []):
        if isinstance(m, dict) and m.get("name"):
            out.append({"name": str(m["name"]), "expr": _clip(m.get("expr"), 200), "comment": _clip(m.get("comment"), 200)})
    return out[:25]


# ---------------------------------------------------------------------------
# Evidence: every input becomes a citable, attributable item
# ---------------------------------------------------------------------------

def build_evidence(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Turn gathered context into a stable, ordered list of evidence items. Pure.

    Item: ``{id, type, authority, ref, label, text, tables}``. Human sources come first so they
    lead the prompt. IDs are positional (``E1..En``) and only meaningful within one run.
    """
    items: List[Dict[str, Any]] = []

    def add(type_, authority, ref, label, text, tables=()):
        if str(text or "").strip():
            items.append({"type": type_, "authority": authority, "ref": str(ref or ""), "label": label,
                          "text": str(text).strip(), "tables": list(tables)})

    for r in ctx.get("customer_context") or []:
        add("customer_context", "human", r.get("context_id"),
            f"customer context [{r.get('scope_type')}: {r.get('scope')}]" + (f" {r['context_label']}" if r.get("context_label") else ""),
            _clip(r.get("context_text"), 2000),
            [r["scope"]] if r.get("scope_type") == "table" else [])
    for r in ctx.get("acronyms") or []:
        add("acronym", "human", r.get("acronym_id") or r.get("abbreviation"), "acronym",
            f"{r.get('abbreviation')} = {r.get('expansion')}" + (f" ({_clip(r['notes'], 200)})" if r.get("notes") else ""))
    for c in ctx.get("source_chunks") or []:
        add("source_doc", "human", c.get("chunk_id"),
            f"document '{c.get('title')}'" + (f" § {c['heading']}" if c.get("heading") else ""),
            _clip(c.get("text"), 1800))
    cols = ctx.get("columns") or {}
    for t in ctx.get("tables") or []:
        name = t.get("table_name")
        dom = "/".join(x for x in [t.get("domain"), t.get("subdomain")] if x and str(x).lower() != "none")
        flags = ", ".join(f for f, on in [("contains PII", _truthy(t.get("has_pii"))), ("contains PHI", _truthy(t.get("has_phi")))] if on)
        meta = "; ".join(x for x in [f"classified domain {dom}" if dom else "", flags] if x)
        add("table_comment", "human" if _truthy(t.get("reviewed")) else "inferred", name,
            f"table {name}" + (f" ({meta})" if meta else ""), t.get("comment") or "(no description)", [name])
        crow = cols.get(name) or []
        if crow:
            lines = []
            for c in crow:
                cls = c.get("classification")
                cls = f" [{cls}]" if cls and str(cls).lower() not in ("none", "null", "") else ""
                lines.append(f"- {c.get('column_name')} ({c.get('data_type') or '?'}){cls}: {_clip(c.get('comment'), 240)}")
            reviewed = bool(crow) and all(_truthy(c.get("reviewed")) for c in crow)
            add("column_comments", "human" if reviewed else "inferred", name, f"columns of {name}", "\n".join(lines), [name])
    for e in ctx.get("entities") or []:
        srcs = [str(s) for s in _as_list(e.get("source_tables"))]
        locked = str(e.get("auto_discovered")).lower() == "false"
        add("ontology_entity", "human" if locked else "inferred", e.get("entity_id") or e.get("entity_name"),
            f"ontology entity {e.get('entity_name') or e.get('entity_type')} ({e.get('entity_type')})",
            f"{_clip(e.get('description'), 400)} [tables: {', '.join(srcs[:6])}]", srcs)
    for f in ctx.get("fks") or []:
        add("relationship", "governed" if _truthy(f.get("reviewed")) else "inferred",
            f"{f.get('src_table')}.{f.get('src_column')}->{f.get('dst_table')}.{f.get('dst_column')}", "relationship",
            f"{f.get('src_table')}.{f.get('src_column')} references {f.get('dst_table')}.{f.get('dst_column')}",
            [f.get("src_table"), f.get("dst_table")])
    for m in ctx.get("metric_views") or []:
        ms = _mv_measures(m.get("json_definition"))
        txt = "; ".join(f"{x['name']} = {x['expr']}" + (f" ({x['comment']})" if x.get("comment") else "") for x in ms)
        add("metric_view", "governed", m.get("fqn") or m.get("metric_view_name"),
            f"metric view {m.get('fqn') or m.get('metric_view_name')} on {m.get('source_table')}",
            txt or "(no measures recorded)", [m.get("source_table")])
    items.sort(key=lambda x: -AUTHORITY_ORDER[x["authority"]])
    for i, it in enumerate(items, 1):
        it["id"] = f"E{i}"
    return items


def render_evidence(evidence: List[Dict[str, Any]], budget_chars: int = DEFAULT_CONTEXT_CHARS,
                    ids: Optional[Iterable[str]] = None) -> str:
    """Render evidence (optionally a subset) as a citable block; truncates at ``budget_chars``."""
    keep = set(ids) if ids is not None else None
    lines = []
    for e in evidence:
        if keep is not None and e["id"] not in keep:
            continue
        lines.append(f"[{e['id']}] ({e['authority']}; {e['label']})\n{e['text']}")
    text = "\n\n".join(lines)
    if len(text) > budget_chars:
        text = text[:budget_chars].rsplit("\n\n", 1)[0] + "\n\n…(evidence truncated)"
    return text


def evidence_level(ids: Iterable[str], evidence_by_id: Dict[str, Dict[str, Any]]) -> str:
    """Strongest authority among cited evidence: human > governed > inferred; none = 'none'."""
    best = 0
    for i in ids or []:
        e = evidence_by_id.get(i)
        if e:
            best = max(best, AUTHORITY_ORDER[e["authority"]])
    return {3: "human", 2: "governed", 1: "inferred"}.get(best, "none")


# ---------------------------------------------------------------------------
# Plan: propose candidate concepts
# ---------------------------------------------------------------------------

def _kinds_block(kinds: Sequence[str]) -> str:
    return "\n".join(f"- {k}: {KINDS[k]['help']}" for k in kinds)


def build_plan_prompt(user_prompt: str, evidence_text: str, *, table_names: List[str],
                      metric_views: List[str], existing_names: List[str], kinds: Sequence[str],
                      max_candidates: int = 15, audience: str = "") -> str:
    aud = f"Audience: {audience.strip()}\n" if (audience or "").strip() else ""
    return (
        "You are a data steward planning Unity Catalog **Pages**. A Page is the governed definition of "
        "ONE business concept that humans and AI agents (Genie) treat as authoritative.\n\n"
        f"USER REQUEST:\n{user_prompt.strip()}\n\n{aud}"
        f"Propose up to {max_candidates} candidate Pages that fulfil the request. Allowed kinds:\n"
        f"{_kinds_block(kinds)}\n\n"
        "GRANULARITY RULES:\n"
        "- One Page = one concept. Different names for the same meaning are ONE Page (the others are synonyms).\n"
        "- The same name with different meanings becomes separate, scope-qualified Pages, e.g. 'Account (billing)'.\n"
        "- A general concept and a meaningful subtype are separate Pages (e.g. 'Allergy' and 'No Known Allergies').\n"
        "- A concept that lives in several tables is ONE Page linked to all of them.\n"
        "- Do NOT propose a Page that merely describes one table; if a candidate is really a table, "
        "set recommend=false with skip_reason 'table-shaped: document this table in a wiki'.\n"
        "- An acronym is a synonym of its concept, not its own Page, unless the acronym itself is the concept people look up.\n"
        "- Business rules stated in human evidence (how to interpret data correctly) deserve their own 'rule' Page.\n"
        "- Grouping several concepts is a Domain's job, not a Page's.\n\n"
        "EVIDENCE RULES:\n"
        "- Cite the evidence ids [E#] that support each candidate. Prefer 'human' evidence; 'inferred' "
        "evidence is AI-generated metadata and may be wrong.\n"
        "- If sources disagree on what a concept means, set 'conflict' to a one-line description.\n"
        "- If the evidence is too thin to define the concept, set recommend=false with skip_reason 'low evidence'.\n"
        "- related_tables and related_metric_views MUST be copied exactly from the lists below.\n\n"
        f"TABLES IN SCOPE:\n{chr(10).join('- ' + t for t in table_names) or '- (none)'}\n\n"
        f"METRIC VIEWS IN SCOPE:\n{chr(10).join('- ' + m for m in metric_views) or '- (none)'}\n\n"
        f"EXISTING PAGES (do not duplicate; reuse the exact name if the concept is the same):\n"
        f"{chr(10).join('- ' + n for n in existing_names[:300]) or '- (none)'}\n\n"
        "Return ONLY JSON:\n"
        '{"candidates":[{"display_name":"...","kind":"entity|metric|term|rule|process|data_product",'
        '"definition":"one sentence: nearest broader kind + distinguishing characteristics",'
        '"synonyms":["..."],"related_tables":["catalog.schema.table"],"related_metric_views":["..."],'
        '"evidence":["E1"],"broader":"name of a broader candidate or empty","related":["other candidate names"],'
        '"recommend":true,"skip_reason":"","conflict":""}]}\n\n'
        f"EVIDENCE:\n{evidence_text}"
    )


def is_table_shaped(name: str, table_names: Iterable[str]) -> bool:
    """True when a Page name is a table IDENTIFIER (e.g. 'fact_encounter', 'dim_patient'), i.e. the
    Page would document a table, which belongs in a wiki. A natural-language concept that merely
    shares a table's name ('Diagnosis' vs table ``diagnosis``) is NOT table-shaped: concepts and
    their tables routinely share names (verified on the eval fixture)."""
    raw = str(name or "").strip()
    if not raw or not re.search(r"[_.]", raw) or " " in raw:
        return False
    n = raw.lower().strip("`")
    for t in table_names:
        full = str(t).lower()
        if n in (full, full.rsplit(".", 1)[-1]):
            return True
    return False


def normalize_plan(
    parsed: Dict[str, Any],
    *,
    ctx: Dict[str, Any],
    evidence: List[Dict[str, Any]],
    domains: List[Dict[str, Any]],
    existing_pages: Optional[List[Dict[str, Any]]] = None,
    existing_drafts: Optional[List[Dict[str, Any]]] = None,
    default_domain: Optional[str] = None,
    kinds: Optional[Sequence[str]] = None,
    max_candidates: int = 15,
) -> List[Dict[str, Any]]:
    """Validate the model's plan into curated candidates with triage flags. Pure.

    Grounding: tables / metric views must be in scope, evidence ids must exist (invented ones are
    dropped and flagged). Default selection = recommended AND evidence beyond 'inferred' AND not a
    duplicate AND not table-shaped. Every flag carries a human-readable message.
    """
    kinds = list(kinds or DEFAULT_KINDS)
    canon_t = {str(t.get("table_name") or "").lower(): t["table_name"] for t in ctx.get("tables") or [] if t.get("table_name")}
    mv_names = {}
    for m in ctx.get("metric_views") or []:
        for key in (m.get("fqn"), m.get("metric_view_name")):
            if key:
                mv_names[str(key).lower()] = m.get("fqn") or m.get("metric_view_name")
    ev_by_id = {e["id"]: e for e in evidence}
    default = resolve_domain(default_domain, domains) if default_domain else None
    existing_uc = {}
    for p in existing_pages or []:
        existing_uc.setdefault(norm_name(p.get("display_name")), []).append(p)
    existing_dr = {norm_name(d.get("display_name")): d for d in existing_drafts or [] if d.get("status") not in ("discarded",)}

    out: List[Dict[str, Any]] = []
    seen = set()
    for raw in (parsed or {}).get("candidates") or (parsed or {}).get("pages") or []:
        if not isinstance(raw, dict):
            continue
        name = _clip(raw.get("display_name") or raw.get("name"), MAX_DISPLAY_NAME)
        key = norm_name(name)
        if not key or key in seen:
            continue
        seen.add(key)
        flags: List[Dict[str, str]] = []
        kind = normalize_kind(raw.get("kind"), kinds)
        if not kind:
            flags.append({"code": "kind_not_enabled", "message": f"Kind '{raw.get('kind')}' is not enabled for this run; treated as entity."})
            kind = "entity" if "entity" in kinds else kinds[0]

        rel_t, dropped = [], []
        for t in _as_list(raw.get("related_tables")):
            k = str(t or "").strip().strip("`").lower()
            if k in canon_t:
                if canon_t[k] not in rel_t:
                    rel_t.append(canon_t[k])
            elif k:
                dropped.append(str(t))
        rel_mv = []
        for m in _as_list(raw.get("related_metric_views")):
            k = str(m or "").strip().strip("`").lower()
            if k in mv_names and mv_names[k] not in rel_mv:
                rel_mv.append(mv_names[k])
            elif k:
                dropped.append(str(m))
        if dropped:
            flags.append({"code": "unknown_assets_dropped", "message": f"Dropped {len(dropped)} asset reference(s) not in scope: {', '.join(dropped[:5])}"})

        ev_ids, bad_ev = [], []
        for e in _as_list(raw.get("evidence")):
            eid = str(e or "").strip().strip("[]").upper()
            (ev_ids if eid in ev_by_id else bad_ev).append(eid)
        ev_ids = list(dict.fromkeys(ev_ids))
        if bad_ev:
            flags.append({"code": "invalid_citations", "message": f"Ignored {len(bad_ev)} citation(s) to evidence that does not exist."})
        level = evidence_level(ev_ids, ev_by_id)
        if level == "none":
            flags.append({"code": "low_evidence", "message": "No valid evidence cited; the concept may not be supported by your metadata."})
        elif level == "inferred":
            flags.append({"code": "inferred_only", "message": "Supported only by AI-generated metadata. Confirm with a steward or add a source document before publishing."})

        dom = resolve_domain(raw.get("domain"), domains) if raw.get("domain") else None
        dom = dom or default or domain_from_tables(rel_t, ctx.get("tables") or [], domains)

        dup_uc = [p for p in existing_uc.get(key, []) if not dom or p.get("domain") == dom["name"]]
        if dup_uc:
            flags.append({"code": "duplicate_existing_page", "message": f"A Page named '{name}' already exists in this Domain; publishing will update it."})
        if key in existing_dr:
            flags.append({"code": "duplicate_draft", "message": f"You already have a draft for '{name}'."})
        table_names = list(canon_t.values())
        shaped = kind != "data_product" and (is_table_shaped(name, table_names)
                                              or "table-shaped" in str(raw.get("skip_reason") or "").lower())
        if shaped:
            flags.append({"code": "table_shaped", "message": "This looks like documentation of one table. Use a UC wiki for tables; Pages are for business concepts."})
        recommend = raw.get("recommend") is not False and not str(raw.get("skip_reason") or "").strip()
        if not recommend:
            flags.append({"code": "model_skipped", "message": str(raw.get("skip_reason") or "Not recommended by the planner.")})
        if str(raw.get("conflict") or "").strip():
            flags.append({"code": "conflict", "message": _clip(raw.get("conflict"), 300)})
        if not dom:
            flags.append({"code": "no_domain", "message": "Choose a Domain before publishing."})

        selected = recommend and level in ("human", "governed") and not shaped and key not in existing_dr
        out.append({
            "page_id": new_id(),
            "kind": kind,
            "display_name": name,
            "description": _clip(raw.get("definition") or raw.get("description"), MAX_DESCRIPTION),
            "synonyms": sanitize_synonyms(_as_list(raw.get("synonyms")), name),
            "domain": (dom or {}).get("name", ""),
            "domain_label": (dom or {}).get("label", ""),
            "related_tables": rel_t,
            "related_metric_views": rel_mv,
            "relations": ([{"type": "broader", "target": str(raw["broader"]).strip()}]
                          if str(raw.get("broader") or "").strip() else []),
            "related_names": [str(x).strip() for x in _as_list(raw.get("related")) if str(x).strip()],
            "evidence_ids": ev_ids,
            "evidence_level": level,
            "flags": flags,
            "selected": bool(selected),
            "status": "planned",
            "uc_page_name": (dup_uc[0].get("name") if dup_uc else ""),
        })
        if len(out) >= max_candidates:
            break
    # Resolve name-based relations to candidate page_ids where possible.
    by_name = {norm_name(c["display_name"]): c["page_id"] for c in out}
    for c in out:
        rels = []
        for r in c["relations"]:
            rels.append({**r, "target_page_id": by_name.get(norm_name(r["target"]), "")})
        for n in c.pop("related_names"):
            if norm_name(n) != norm_name(c["display_name"]):
                rels.append({"type": "related", "target": n, "target_page_id": by_name.get(norm_name(n), "")})
        c["relations"] = rels
    return out


def resolve_domain(raw: Any, domains: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Snap a domain reference (name, id, or tag-key label) to a real Domain."""
    s = str(raw or "").strip()
    if not s:
        return None
    low = s.lower()
    for d in domains:
        if low in (d["name"].lower(), d["domain_id"].lower(), d["label"].lower()):
            return d
    n = norm_name(s)
    for d in domains:
        if n and n == norm_name(d["label"]):
            return d
    leaf = [d for d in domains if norm_name(d["label"].rsplit("/", 1)[-1]) == n]
    return leaf[0] if len(leaf) == 1 else None


def domain_from_tables(related: List[str], kb_rows: List[Dict[str, Any]],
                       domains: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Fallback: majority dbxmetagen-classified domain/subdomain of the related tables."""
    kb = {str(r.get("table_name") or "").lower(): r for r in kb_rows}
    votes: Dict[str, int] = {}
    for t in related:
        r = kb.get(t.lower()) or {}
        for cand in (f"{r.get('domain')}/{r.get('subdomain')}", r.get("subdomain"), r.get("domain")):
            d = resolve_domain(cand, domains) if cand and "none" not in str(cand).lower() else None
            if d:
                votes[d["name"]] = votes.get(d["name"], 0) + 1
                break
    if not votes:
        return None
    best = max(votes, key=votes.get)
    return next(d for d in domains if d["name"] == best)


# ---------------------------------------------------------------------------
# Write: one kind-specific, fully cited Page per selected candidate
# ---------------------------------------------------------------------------

def focused_evidence_ids(candidate: Dict[str, Any], evidence: List[Dict[str, Any]], *, limit: int = 40,
                         doc_chunks: int = 6) -> List[str]:
    """Evidence for one Page: its cited items + everything about its tables/metric views + human
    context and the most relevant source-document chunks for the concept."""
    tabs = {t.lower() for t in candidate.get("related_tables") or []}
    mvs = {m.lower() for m in candidate.get("related_metric_views") or []}
    picked: List[str] = list(candidate.get("evidence_ids") or [])
    for e in evidence:
        if e["id"] in picked:
            continue
        et = {str(t).lower() for t in e.get("tables") or []}
        if (et & tabs) or (e["type"] == "metric_view" and e["ref"].lower() in mvs) or e["type"] in ("acronym",):
            picked.append(e["id"])
        elif e["type"] == "customer_context" and not et:  # catalog/schema-wide human context
            picked.append(e["id"])
    docs = [e for e in evidence if e["type"] == "source_doc" and e["id"] not in picked]
    query = " ".join([candidate.get("display_name", ""), " ".join(candidate.get("synonyms") or []), candidate.get("description", "")])
    for c in rank_chunks([{"heading": e["label"], "text": e["text"], "id": e["id"]} for e in docs], query, doc_chunks):
        picked.append(c["id"])
    return picked[:limit]


def build_write_prompt(candidate: Dict[str, Any], evidence_text: str, *, other_pages: List[str],
                       audience: str = "") -> str:
    kind = candidate.get("kind") or "entity"
    spec = KINDS.get(kind, KINDS["entity"])
    sections = "\n".join(f"## {s}" for s in spec["sections"])
    aud = f"Audience: {audience.strip()}\n" if (audience or "").strip() else ""
    metric_rule = (
        "- This is a METRIC Page. The governed calculation lives in the metric view. Explain what the "
        "measure means, its population/exclusions/time basis if evidenced, and reference the metric view "
        "and measure by exact name (e.g. `MEASURE(name)` in `catalog.schema.view`). Do not invent a formula; "
        "only state one if it appears in the evidence, and cite it.\n" if kind == "metric" else "")
    return (
        "You are a data steward writing ONE Unity Catalog Page: the authoritative definition of a "
        "business concept that people and Genie rely on.\n\n"
        f"CONCEPT: {candidate.get('display_name')}\nKIND: {kind} ({spec['label']}): {spec['help']}\n"
        f"PLANNED DEFINITION (improve it): {candidate.get('description') or '(none)'}\n"
        f"KNOWN SYNONYMS: {', '.join(candidate.get('synonyms') or []) or '(none)'}\n"
        f"RELATED TABLES: {', '.join(candidate.get('related_tables') or []) or '(none)'}\n"
        f"RELATED METRIC VIEWS: {', '.join(candidate.get('related_metric_views') or []) or '(none)'}\n"
        f"OTHER PAGES IN THIS SET (you may refer to them by name): {', '.join(other_pages) or '(none)'}\n{aud}\n"
        "WRITING RULES:\n"
        "- description: ONE sentence that defines the concept as its nearest broader kind plus what "
        "distinguishes it (e.g. 'A customer who has completed at least one purchase in the preceding 12 "
        "months.'). Not a purpose statement, does not repeat the concept name, at most 200 characters. "
        "Put detail in the body, not here.\n"
        "- body: Markdown using these sections in order; OMIT any section the evidence cannot support:\n"
        f"{sections}\n"
        "- Every factual sentence or bullet in the body MUST end with citations like [E3] or [E3, E7], "
        "using only the evidence ids below. Do not state anything you cannot cite.\n"
        "- Where human evidence (customer context, documents, acronyms) and inferred evidence disagree, "
        "keep the human MEANING, never assert the inferred version as fact, and add the conflict to "
        "open_questions (e.g. two different column names for the same thing: name neither as definitive).\n"
        "- Examples must be grounded in the evidence. Never invent workflows, scenarios, values or people. "
        "Omit the Examples section rather than make one up.\n"
        "- State rules exactly as strong as the evidence does: do not turn 'use X to scope' into 'always "
        "filter to X'.\n"
        "- Name exact tables, columns and metric views in backticks when they help someone find or query "
        "the concept. Include data caveats from the evidence (grain, null meaning, freshness, exclusions).\n"
        f"{metric_rule}"
        "- Never include data values about real people or records, personal information, or credentials.\n"
        f"- Be concise. Good Pages are short: aim for about 1000 characters of body, never more than "
        f"{BODY_TARGET_CHARS[1]}. Prefer short bullets over paragraphs. Cover each fact once; facts that "
        "belong to another Page in this set should be a one-line pointer to that Page by name, not repeated.\n"
        "- open_questions: at most 3 short questions a steward must answer where evidence is missing, "
        "conflicting, or only inferred (empty list if none).\n"
        "- synonyms: only abbreviations and alternative names that APPEAR in the evidence (not the concept "
        "name; never guess).\n\n"
        'Return ONLY JSON: {"display_name":"...","description":"...","synonyms":["..."],'
        '"body":"## Definition\\n... [E1]","open_questions":["..."]}\n\n'
        f"EVIDENCE:\n{evidence_text}"
    )


def parse_citations(body: str, valid_ids: Iterable[str]) -> Dict[str, Any]:
    """Split a cited body into a clean body + claim→evidence provenance. Pure.

    Returns ``{body, claims: [{section, claim, evidence_ids}], invalid_ids, cited_lines, content_lines}``.
    A "content line" is a non-empty, non-heading, non-table-separator line; coverage =
    cited_lines / content_lines.
    """
    valid = set(valid_ids or [])
    section = ""
    claims: List[Dict[str, Any]] = []
    invalid: List[str] = []
    clean_lines: List[str] = []
    cited = content = 0
    in_code = False
    for line in str(body or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
        m_head = re.match(r"^#{1,6}\s+(.*)$", stripped)
        if m_head and not in_code:
            section = _CITE_RE.sub("", m_head.group(1)).strip()
            clean_lines.append(_CITE_RE.sub("", line).rstrip())
            continue
        ids = []
        for grp in _CITE_RE.findall(line):
            ids.extend(x.strip() for x in grp.split(","))
        clean = _CITE_RE.sub("", line).rstrip()
        is_content = bool(stripped) and not in_code and not stripped.startswith("```") \
            and not re.match(r"^\|?\s*:?-{3,}", stripped) and stripped not in ("---", "***")
        if is_content:
            content += 1
        good = [i for i in dict.fromkeys(ids) if i in valid]
        invalid.extend(i for i in ids if i not in valid)
        if good and is_content:
            cited += 1
            claims.append({"section": section, "claim": clean.strip(" -*|"), "evidence_ids": good})
        clean_lines.append(clean)
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(clean_lines)).strip()
    return {"body": text, "claims": claims, "invalid_ids": sorted(set(invalid)),
            "cited_lines": cited, "content_lines": content}


def provenance_footer(evidence_ids: Iterable[str], evidence_by_id: Dict[str, Dict[str, Any]]) -> str:
    """Short, human-readable source note appended to the published body."""
    human_docs, ctx_notes, tables = [], 0, set()
    for i in evidence_ids:
        e = evidence_by_id.get(i)
        if not e:
            continue
        if e["type"] == "source_doc":
            title = e["label"].split("'")[1] if "'" in e["label"] else e["label"]
            if title not in human_docs:
                human_docs.append(title)
        elif e["type"] in ("customer_context", "acronym"):
            ctx_notes += 1
        elif e["type"] in ("table_comment", "column_comments"):
            tables.add(e["ref"])
    parts = []
    if human_docs:
        parts.append("documents: " + ", ".join(human_docs[:5]))
    if ctx_notes:
        parts.append(f"{ctx_notes} business context note(s)")
    if tables:
        parts.append(f"metadata for {len(tables)} table(s)")
    if not parts:
        return ""
    return "\n\n---\n_Drafted with dbxmetagen from " + "; ".join(parts) + ". Review before certifying._"


def finalize_written_page(candidate: Dict[str, Any], parsed: Dict[str, Any], evidence: List[Dict[str, Any]],
                          ids_offered: List[str]) -> Dict[str, Any]:
    """Merge the model's written page into the candidate; build provenance rows. Pure."""
    ev_by_id = {e["id"]: e for e in evidence}
    cit = parse_citations(parsed.get("body") or "", ids_offered)
    desc_cit = parse_citations(parsed.get("description") or "", ids_offered)
    description = _clip(desc_cit["body"].replace("\n", " "), MAX_DESCRIPTION)
    cited_ids = list(dict.fromkeys([i for c in cit["claims"] + desc_cit["claims"] for i in c["evidence_ids"]]
                                   + list(candidate.get("evidence_ids") or [])))
    questions = [str(q).strip() for q in _as_list(parsed.get("open_questions")) if str(q).strip()][:8]
    body = cit["body"]
    if questions:
        body += "\n\n## Open questions for the steward\n" + "\n".join(f"- {q}" for q in questions)
    body += provenance_footer(cited_ids, ev_by_id)
    name = _clip(parsed.get("display_name") or candidate["display_name"], MAX_DISPLAY_NAME)
    page = {
        **candidate,
        "display_name": name,
        "description": description or candidate.get("description", ""),
        "synonyms": sanitize_synonyms(list(candidate.get("synonyms") or []) + _as_list(parsed.get("synonyms")), name),
        "body": _clip(body, MAX_BODY_CHARS),
        # The model's body WITH [E#] citations: drives the provenance view and lets the judge and the
        # revision pass reason about which statement rests on which evidence.
        "body_cited": _clip(parsed.get("body") or "", MAX_BODY_CHARS),
        "offered_evidence_ids": list(ids_offered),
        "open_questions": questions,
        "evidence_ids": cited_ids,
        "evidence_level": evidence_level(cited_ids, ev_by_id),
        "citation_stats": {"cited_lines": cit["cited_lines"], "content_lines": cit["content_lines"],
                           "invalid_ids": cit["invalid_ids"] + desc_cit["invalid_ids"]},
        "status": "drafted",
    }
    provenance = []
    for c in desc_cit["claims"] + cit["claims"]:
        section = c["section"] or ("Description" if c in desc_cit["claims"] else "")
        for i in c["evidence_ids"]:
            e = ev_by_id[i]
            provenance.append({"evidence_id": i, "section": section, "claim": _clip(c["claim"], 1000),
                               "evidence_type": e["type"], "authority": e["authority"], "source_ref": e["ref"],
                               "label": e["label"], "excerpt": _clip(e["text"], 1500)})
    cited_in_claims = {p["evidence_id"] for p in provenance}
    for i in candidate.get("evidence_ids") or []:  # plan-level support not tied to a sentence
        if i in ev_by_id and i not in cited_in_claims:
            e = ev_by_id[i]
            provenance.append({"evidence_id": i, "section": "(plan)", "claim": "", "evidence_type": e["type"],
                               "authority": e["authority"], "source_ref": e["ref"], "label": e["label"],
                               "excerpt": _clip(e["text"], 1500)})
    page["provenance"] = provenance
    return page


def plan_pages(*, llm: Llm, ctx: Dict[str, Any], prompt: str, domains: List[Dict[str, Any]],
               existing_pages: List[Dict[str, Any]], existing_drafts: Optional[List[Dict[str, Any]]] = None,
               default_domain: Optional[str] = None, kinds: Optional[Sequence[str]] = None,
               max_candidates: int = 15, audience: str = "") -> Dict[str, Any]:
    """Run the planning step end to end (one LLM call). Returns ``{candidates, evidence, error}``."""
    kinds = list(kinds or DEFAULT_KINDS)
    evidence = build_evidence(ctx)
    mv_list = [m.get("fqn") or m.get("metric_view_name") for m in ctx.get("metric_views") or []]
    p = build_plan_prompt(prompt, render_evidence(evidence), table_names=[t["table_name"] for t in ctx.get("tables") or []],
                          metric_views=[m for m in mv_list if m], existing_names=[x.get("display_name") for x in existing_pages if x.get("display_name")],
                          kinds=kinds, max_candidates=max_candidates, audience=audience)
    text, err = (llm([p]) or [("", "no response")])[0]
    if err:
        return {"candidates": [], "evidence": evidence, "error": err}
    cands = normalize_plan(extract_json(text), ctx=ctx, evidence=evidence, domains=domains, existing_pages=existing_pages,
                           existing_drafts=existing_drafts, default_domain=default_domain, kinds=kinds,
                           max_candidates=max_candidates)
    return {"candidates": cands, "evidence": evidence, "error": None if cands else "The model proposed no usable Pages."}


def write_pages(*, llm: Llm, candidates: List[Dict[str, Any]], evidence: List[Dict[str, Any]],
                audience: str = "") -> List[Dict[str, Any]]:
    """Write each candidate (one batched LLM call). Failed pages come back with ``error`` set."""
    names = [c["display_name"] for c in candidates]
    prompts, offered = [], []
    for c in candidates:
        ids = focused_evidence_ids(c, evidence)
        offered.append(ids)
        prompts.append(build_write_prompt(c, render_evidence(evidence, ids=ids),
                                          other_pages=[n for n in names if n != c["display_name"]], audience=audience))
    responses = _call_with_retry(llm, prompts, ok=lambda t: bool(extract_json(t).get("body")))
    out = []
    for c, ids, (text, err) in zip(candidates, offered, responses):
        if err:
            out.append({**c, "error": err})
            continue
        parsed = extract_json(text)
        if not parsed.get("body"):
            out.append({**c, "error": "The model returned no page body."})
            continue
        out.append(finalize_written_page(c, parsed, evidence, ids))
    return out


def _call_with_retry(llm: Llm, prompts: List[str], ok: Callable[[str], bool], retries: int = 1) -> List[Tuple[str, Optional[str]]]:
    """Batched LLM call; prompts whose response errored or failed ``ok`` are retried (batched)."""
    if not prompts:
        return []
    results = list(llm(prompts))
    for _ in range(retries):
        bad = [i for i, (t, e) in enumerate(results) if e or not ok(t or "")]
        if not bad:
            break
        again = llm([prompts[i] for i in bad])
        for i, r in zip(bad, again):
            if not r[1] and ok(r[0] or ""):
                results[i] = r
    return results


def build_revision_prompt(page: Dict[str, Any], issues: List[str], evidence_text: str) -> str:
    """Ask the writer to fix specific reviewer findings, keeping citations and structure."""
    current = {"display_name": page.get("display_name"), "description": page.get("description"),
               "synonyms": page.get("synonyms") or [], "body": page.get("body_cited") or page.get("body"),
               "open_questions": page.get("open_questions") or []}
    return (
        "You wrote the Unity Catalog Page below. A strict reviewer found the issues listed. Revise the Page to "
        "fix EVERY issue: remove or correct unsupported statements, remove invented synonyms or examples, keep "
        "human evidence authoritative, and move unresolved conflicts into open_questions. Keep every remaining "
        "factual sentence cited with [E#] using only the evidence ids below. Do not add new unsupported content. "
        "Keep it concise.\n\n"
        "REVIEWER ISSUES:\n" + "\n".join(f"- {i}" for i in issues) + "\n\n"
        f"CURRENT PAGE (JSON):\n{json.dumps(current, ensure_ascii=False)}\n\n"
        'Return ONLY JSON: {"display_name":"...","description":"...","synonyms":["..."],"body":"...","open_questions":["..."]}\n\n'
        f"EVIDENCE:\n{evidence_text}"
    )


def revise_pages(*, llm: Llm, pages: List[Dict[str, Any]], issues_by_page: Dict[str, List[str]],
                 evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One revision pass for pages with reviewer issues. Returns revised pages (same order as
    ``pages``; a page whose revision failed is returned unchanged with ``revision_error``)."""
    todo = [p for p in pages if issues_by_page.get(p["page_id"])]
    prompts = [build_revision_prompt(p, issues_by_page[p["page_id"]],
                                     render_evidence(evidence, ids=p.get("offered_evidence_ids") or p.get("evidence_ids")))
               for p in todo]
    responses = _call_with_retry(llm, prompts, ok=lambda t: bool(extract_json(t).get("body")))
    revised = {}
    for p, (text, err) in zip(todo, responses):
        parsed = extract_json(text) if not err else {}
        if not parsed.get("body"):
            revised[p["page_id"]] = {**p, "revision_error": err or "no body"}
            continue
        base = {k: v for k, v in p.items() if k not in ("body", "body_cited", "provenance", "citation_stats")}
        base["synonyms"] = []  # the revision decides synonyms afresh (drops invented ones)
        revised[p["page_id"]] = finalize_written_page(base, parsed, evidence, p.get("offered_evidence_ids") or p.get("evidence_ids") or [])
    return [revised.get(p["page_id"], p) for p in pages]


def extract_json(text: str) -> Dict[str, Any]:
    """Parse a JSON object from LLM text, tolerating code fences and surrounding prose."""
    if not text:
        return {}
    t = str(text).strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t[3:]
        t = t.rsplit("```", 1)[0]
    try:
        v = json.loads(t)
        return v if isinstance(v, dict) else {}
    except Exception:
        pass
    s, e = t.find("{"), t.rfind("}")
    if s != -1 and e > s:
        try:
            v = json.loads(t[s:e + 1])
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Publishing (the ONLY external write; called only on explicit user action)
# ---------------------------------------------------------------------------

UPDATE_FIELDS = ("display_name", "description", "body", "synonyms")


def build_create_body(p: Dict[str, Any], state: str = "DRAFT", owner_id: Optional[str] = None) -> Dict[str, Any]:
    body = {
        "domain": p["domain"],
        "display_name": _clip(p["display_name"], MAX_DISPLAY_NAME),
        "description": p.get("description") or "",
        "body": p.get("body") or "",
        "synonyms": sanitize_synonyms(p.get("synonyms") or [], p["display_name"]),
        "state": normalize_state(state),
    }
    if owner_id:
        body["owner_ids"] = [str(owner_id)]
    return body


def build_update(p: Dict[str, Any], state: str = "DRAFT") -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    """(path, query, body) for PATCH. ``state`` is in the mask ONLY when publishing, so a Draft
    re-sync never demotes an already-PUBLISHED page."""
    fields = list(UPDATE_FIELDS)
    body = {k: v for k, v in build_create_body(p, state).items() if k in fields}
    if normalize_state(state) == "PUBLISHED":
        fields.append("state")
        body["state"] = "PUBLISHED"
    return f"/api/2.0/knowledge/{p['uc_page_name']}", {"update_mask": ",".join(fields)}, body


def validate_for_publish(p: Dict[str, Any]) -> Optional[str]:
    if not str(p.get("display_name") or "").strip():
        return "display_name is required"
    if len(p["display_name"]) > MAX_DISPLAY_NAME:
        return f"display_name exceeds {MAX_DISPLAY_NAME} characters"
    if not str(p.get("domain") or "").startswith("domains/"):
        return "a Domain is required"
    if not str(p.get("body") or "").strip():
        return "the Page has no body yet; write it before publishing"
    return None


def desired_links(p: Dict[str, Any], *, metastore_id: Optional[str], account: Optional[str],
                  page_names_by_id: Dict[str, str], source_urls: Iterable[str] = ()) -> List[Dict[str, str]]:
    """The asset links a Page should have. Pure. ``key`` identifies a link across syncs."""
    out = []
    if metastore_id:
        for t in list(p.get("related_tables") or []) + list(p.get("related_metric_views") or []):
            out.append({"key": f"RELATED_ASSETS|ASSET_TYPE_UC_TABLE|{t.lower()}", "category": "RELATED_ASSETS",
                        "asset_type": "ASSET_TYPE_UC_TABLE", "asset_scope_id": metastore_id, "asset_id": t})
    if account:
        for r in p.get("relations") or []:
            target = page_names_by_id.get(r.get("target_page_id") or "")
            if target:
                pid = target.split("/", 1)[1]
                out.append({"key": f"RELATED_ASSETS|ASSET_TYPE_ACCOUNT_PAGE|{pid}", "category": "RELATED_ASSETS",
                            "asset_type": "ASSET_TYPE_ACCOUNT_PAGE", "asset_scope_id": account, "asset_id": pid})
        for u in source_urls:
            if str(u).lower().startswith(("http://", "https://")):
                out.append({"key": f"SOURCE|ASSET_TYPE_EXTERNAL_URL|{u}", "category": "SOURCE",
                            "asset_type": "ASSET_TYPE_EXTERNAL_URL", "asset_scope_id": account, "asset_id": u})
    seen, uniq = set(), []
    for link in out:
        if link["key"] not in seen:
            seen.add(link["key"])
            uniq.append(link)
    return uniq


def reconcile_links(api_do: ApiDo, page_name: str, desired: List[Dict[str, str]],
                    recorded: Dict[str, str]) -> Tuple[Dict[str, str], List[str]]:
    """Create missing links and delete links we created that are no longer desired. Returns
    ``(recorded_after, errors)``. Links are immutable; the API has no list call, so ``recorded``
    (key → link resource name) is our only memory of what exists."""
    errors: List[str] = []
    after = dict(recorded or {})
    want = {d["key"]: d for d in desired}
    for key, link_name in list(after.items()):
        if key not in want:
            try:
                api_do("DELETE", f"/api/2.0/knowledge/{link_name}")
                after.pop(key)
            except Exception as exc:
                errors.append(f"remove {key.split('|')[-1]}: {_clip(exc, 160)}")
    for key, d in want.items():
        if key in after:
            continue
        try:
            r = api_do("POST", f"/api/2.0/knowledge/{page_name}/asset-link-categories/{d['category']}/asset-links",
                       body={"asset_type": d["asset_type"], "asset_scope_id": d["asset_scope_id"], "asset_id": d["asset_id"]}) or {}
            if r.get("name"):
                after[key] = r["name"]
        except Exception as exc:
            errors.append(f"{d['asset_id']}: {_clip(exc, 200)}")
    return after, errors


def publish(
    api_do: ApiDo,
    pages: List[Dict[str, Any]],
    *,
    state: str = "DRAFT",
    metastore_id: Optional[str] = None,
    account: Optional[str] = None,
    owner_id: Optional[str] = None,
    source_urls_by_page: Optional[Dict[str, List[str]]] = None,
    force: bool = False,
    known_page_names: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Create/update Pages, then reconcile their links in a second pass (so related-Page links can
    target Pages created in the same batch). Per-page errors are captured, never raised.

    Edit protection: an existing UC Page whose content no longer matches our ``published_hash`` was
    edited in UC since our last sync; it is skipped (status ``edited_in_uc``) unless ``force``.
    """
    results: Dict[str, Dict[str, Any]] = {}
    names_by_id: Dict[str, str] = dict(known_page_names or {})
    for p in pages or []:
        pid = p["page_id"]
        res = {"page_id": pid, "display_name": p.get("display_name"), "status": "error", "error": None,
               "uc_page_name": p.get("uc_page_name") or "", "link_errors": [], "uc_links": dict(p.get("uc_links") or {})}
        results[pid] = res
        err = validate_for_publish(p)
        if err:
            res["error"] = err
            continue
        try:
            if p.get("uc_page_name"):
                if p.get("published_hash") and not force:
                    cur = get_page(api_do, p["uc_page_name"])
                    if cur and content_hash(cur.get("display_name"), cur.get("description"), cur.get("body"),
                                            cur.get("synonyms")) != p["published_hash"]:
                        res.update(status="edited_in_uc", error="This Page was edited in Unity Catalog since dbxmetagen "
                                   "last published it. Review the UC version, then publish with overwrite to replace it.")
                        names_by_id[pid] = p["uc_page_name"]
                        continue
                path, query, body = build_update(p, state)
                page = api_do("PATCH", path, query=query, body=body) or {}
                res["action"] = "update"
            else:
                page = api_do("POST", PAGES_PATH, body=build_create_body(p, state, owner_id)) or {}
                res["action"] = "create"
            res["uc_page_name"] = page.get("name") or p.get("uc_page_name") or ""
            res["uc_state"] = page.get("state") or normalize_state(state)
            res["published_hash"] = content_hash(page.get("display_name", p["display_name"]), page.get("description", p.get("description")),
                                                 page.get("body", p.get("body")), page.get("synonyms", p.get("synonyms")))
            res["status"] = "ok"
            names_by_id[pid] = res["uc_page_name"]
        except Exception as exc:
            res["error"] = _clip(exc, 500)
    for p in pages or []:
        res = results[p["page_id"]]
        if res["status"] != "ok":
            continue
        want = desired_links(p, metastore_id=metastore_id, account=account, page_names_by_id=names_by_id,
                             source_urls=(source_urls_by_page or {}).get(p["page_id"], []))
        if (p.get("related_tables") or p.get("related_metric_views")) and not metastore_id:
            res["link_errors"].append("metastore id unknown; table links skipped")
        res["uc_links"], errs = reconcile_links(api_do, res["uc_page_name"], want, res["uc_links"])
        res["link_errors"].extend(errs)
        if res["link_errors"]:
            res["status"] = "partial"
    return [results[p["page_id"]] for p in pages or []]


def audit_rows(results: List[Dict[str, Any]], *, prompt: str, created_by: str, now: str,
               request_id: str) -> List[Dict[str, Any]]:
    """Rows for the additive ``uc_pages_log`` audit table (one per publish result). Pure."""
    return [{
        "request_id": request_id, "page_id": r.get("page_id") or "", "page_name": r.get("uc_page_name") or "",
        "display_name": r.get("display_name") or "", "action": r.get("action") or "",
        "state": r.get("uc_state") or "", "status": r.get("status") or "",
        "error": _clip(r.get("error"), 1000) if r.get("error") else "",
        "linked_assets": json.dumps(sorted(k.split("|", 2)[-1] for k in (r.get("uc_links") or {}))),
        "prompt": _clip(prompt, 2000), "created_by": created_by, "created_at": now,
    } for r in results]

"""ERD recommendation engine for the semantic layer.

Given the structural metadata dbxmetagen already produces -- FK predictions,
ontology entities/roles, per-column profiling stats, holistic coverage, existing
metric-view definitions, and KPIs -- this module recommends a star-schema ERD:
which tables are **facts/sources** vs **dimensions/bridges**, which **joins**
connect them, which columns are **measurable**, and a coverage-aware
**sufficiency** recommendation for how many more metric views to generate.

The core `recommend_erd()` is a **pure function**: it takes already-fetched rows
(plain dicts) and returns dataclasses, with no Spark/SQL/LLM/network dependency,
so it unit-tests with fixtures. The app backend is responsible for running the
SQL that produces the input rows and for JSON-serializing the output.

Design intentionally mirrors and extends `semantic_layer.profile_schema()`: it
reuses the fact/dim naming prefixes and the FK-bridge topology idea, but layers
profiling-based role inference (cardinality/uniqueness) and ontology entity_role
on top, and emits per-table roles rather than a single schema label.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

# Reuse the naming conventions already used by the LLM-context schema profiler so
# the recommender and the generator agree on what "looks like" a fact/dim/mart.
from dbxmetagen.semantic_layer import (
    _DIM_PREFIXES,
    _FACT_PREFIXES,
    _MART_KEYWORDS,
)

# --- Tunable thresholds (single place to adjust the heuristic) ---------------

# A dimension is small + highly unique on its key: low row cardinality relative
# to the fact, and at least one near-unique column (a natural PK).
DIM_MAX_CARDINALITY_RATIO = 0.5   # unused directly; kept for readability of intent
PK_UNIQUENESS_MIN = 0.9           # is_unique_candidate / cardinality_ratio ~ PK
FACT_MIN_OUTBOUND_FKS = 2         # a fact typically references >=2 dimensions
FK_CONFIDENCE_MIN = 0.5           # mirror /api/coverage/holistic's threshold
FK_CONFIRMED_MIN = 0.85           # mirror _inject_fk_joins auto-injection floor

# Sufficiency: how many metric views a schema "wants". A fact table typically
# warrants ~1 broad view; extra views cover distinct grains / uncovered KPIs.
VIEWS_PER_FACT = 1
KPIS_PER_VIEW = 3                 # a metric view's measures can satisfy several KPIs
MAX_RECOMMENDED_VIEWS = 15        # mirror the existing UI cap

# Questions/KPIs sufficiency: a fact grain warrants a few analytical questions;
# each business domain warrants at least one KPI.
QUESTIONS_PER_FACT = 3            # analytical questions a fact table typically supports
QUESTIONS_PER_DOMAIN = 1         # + breadth across distinct business domains
MAX_RECOMMENDED_QUESTIONS = 25
KPIS_PER_FACT = 2                 # a fact table usually anchors ~2 headline KPIs
MAX_RECOMMENDED_KPIS = 20


@dataclass
class ErdNode:
    table: str                       # fully-qualified table name
    role: str                        # fact | dimension | source | bridge
    confidence: float                # 0..1
    reasons: list[str] = field(default_factory=list)
    measurable_columns: list[str] = field(default_factory=list)
    grain: Optional[str] = None      # natural-key / id column if known


@dataclass
class ErdEdge:
    src: str                         # fq table (the referencing / fact side)
    dst: str                         # fq table (the referenced / dim side)
    on: str                          # join condition, e.g. "src.customer_id = dst.id"
    confidence: float                # 0..1
    source: str                      # predicted | confirmed | ontology
    reasons: list[str] = field(default_factory=list)


@dataclass
class Sufficiency:
    metric_views_current: int = 0
    metric_views_recommended: int = 0
    metric_views_gap: int = 0
    reasons: list[str] = field(default_factory=list)
    uncovered_tables: list[str] = field(default_factory=list)
    missing_kpis: list[str] = field(default_factory=list)


@dataclass
class GenSufficiency:
    """Coverage-aware 'generate more?' recommendation for one generator
    (questions or KPIs) over a profile+project scope."""
    kind: str = ""                   # "questions" | "kpis"
    current: int = 0
    recommended: int = 0
    gap: int = 0
    should_generate_more: bool = False
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ErdRecommendation:
    nodes: list[ErdNode] = field(default_factory=list)
    edges: list[ErdEdge] = field(default_factory=list)
    sufficiency: Sufficiency = field(default_factory=Sufficiency)
    schema_type: str = "SIMPLE"      # SIMPLE | STAR | SNOWFLAKE | DATA_MART

    def to_dict(self) -> dict:
        """JSON-serializable dict for the API layer."""
        return {
            "nodes": [asdict(n) for n in self.nodes],
            "edges": [asdict(e) for e in self.edges],
            "sufficiency": asdict(self.sufficiency),
            "schema_type": self.schema_type,
        }


def _short(table: str) -> str:
    return table.split(".")[-1].lower()


def _looks_like_fact(short: str) -> bool:
    return any(short.startswith(p) for p in _FACT_PREFIXES)


def _looks_like_dim(short: str) -> bool:
    return any(short.startswith(p) for p in _DIM_PREFIXES)


def _looks_like_mart(short: str) -> bool:
    return any(kw in short for kw in _MART_KEYWORDS)


def _coerce_list(val: Any) -> list:
    """target_tables / source_tables arrive as a real list or a JSON string."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else [parsed]
        except Exception:
            return [val]
    return [val]


def _build_edges(fk_rows: list[dict]) -> list[ErdEdge]:
    """Turn FK predictions into ERD edges (fact/src -> dim/dst), deduped.

    Confidence-gated at FK_CONFIDENCE_MIN. A steward-confirmed FK (is_fk true or
    confidence >= FK_CONFIRMED_MIN) is labeled `confirmed`, else `predicted`.
    """
    edges: list[ErdEdge] = []
    seen: set[tuple] = set()
    for fk in fk_rows or []:
        src_t = fk.get("src_table")
        dst_t = fk.get("dst_table")
        src_c = fk.get("src_column")
        dst_c = fk.get("dst_column")
        if not (src_t and dst_t and src_c and dst_c):
            continue
        conf = float(fk.get("final_confidence") or 0.0)
        is_fk = bool(fk.get("is_fk"))
        if not is_fk and conf < FK_CONFIDENCE_MIN:
            continue
        key = (src_t.lower(), dst_t.lower(), src_c.lower(), dst_c.lower())
        if key in seen:
            continue
        seen.add(key)
        confirmed = is_fk or conf >= FK_CONFIRMED_MIN
        reasons = []
        if is_fk:
            reasons.append("steward-confirmed FK")
        if fk.get("join_rate") is not None:
            reasons.append(f"join_rate={float(fk['join_rate']):.2f}")
        if fk.get("pk_uniqueness") is not None:
            reasons.append(f"pk_uniqueness={float(fk['pk_uniqueness']):.2f}")
        edges.append(ErdEdge(
            src=src_t,
            dst=dst_t,
            on=f"src.{src_c} = {_short(dst_t)}.{dst_c}",
            confidence=1.0 if is_fk else round(conf, 3),
            source="confirmed" if confirmed else "predicted",
            reasons=reasons,
        ))
    return edges


def _measurable_columns(profiling_rows: list[dict]) -> list[str]:
    """Numeric, low-null columns that aren't near-unique keys -> good measures."""
    out = []
    for c in profiling_rows or []:
        if not c.get("has_numeric_stats"):
            continue
        null_rate = float(c.get("null_rate") or 0.0)
        if null_rate > 0.5:
            continue
        # A near-unique numeric column is usually an ID, not a measure.
        if c.get("is_unique_candidate"):
            continue
        name = c.get("column_name")
        if name:
            out.append(name)
    return out


def _grain_column(profiling_rows: list[dict]) -> Optional[str]:
    """Best natural-key candidate: unique + low null."""
    best = None
    best_ratio = 0.0
    for c in profiling_rows or []:
        ratio = float(c.get("cardinality_ratio") or 0.0)
        low_null = float(c.get("null_rate") or 0.0) <= 0.05
        if (c.get("is_unique_candidate") or ratio >= PK_UNIQUENESS_MIN) and low_null:
            if ratio >= best_ratio:
                best_ratio = ratio
                best = c.get("column_name")
    return best


def _infer_role(
    table: str,
    profiling_rows: list[dict],
    outbound_fk_count: int,
    inbound_fk_count: int,
    ontology_role: Optional[str],
    is_bridge: bool,
) -> tuple[str, float, list[str]]:
    """Combine naming, FK topology, profiling and ontology into a role + why."""
    short = _short(table)
    reasons: list[str] = []
    score = {"fact": 0.0, "dimension": 0.0, "source": 0.0, "bridge": 0.0}

    # 1. Naming conventions. A strong hint WHEN PRESENT, but many real schemas
    #    (marts, denormalized/OBT tables) aren't named fct_/dim_ -- so naming is
    #    weighted comparably to the structural signals below, not above them.
    if _looks_like_fact(short):
        score["fact"] += 0.35
        reasons.append("fact naming prefix")
    if _looks_like_dim(short):
        score["dimension"] += 0.35
        reasons.append("dimension naming prefix")
    if _looks_like_mart(short):
        score["source"] += 0.3
        reasons.append("mart/summary naming")

    # 2. FK topology. Facts reference many dims (outbound); dims are referenced
    #    by many (inbound); a table that is both src and dst is a bridge.
    if is_bridge:
        score["bridge"] += 0.5
        reasons.append("bridges FK chains (both src and dst)")
    if outbound_fk_count >= FACT_MIN_OUTBOUND_FKS:
        score["fact"] += 0.35
        reasons.append(f"{outbound_fk_count} outbound FKs")
    if inbound_fk_count >= 1 and outbound_fk_count == 0:
        score["dimension"] += 0.35
        reasons.append(f"referenced by {inbound_fk_count} table(s), no outbound FKs")

    # 3. Profiling shape (naming-independent -- this is what lets an unlabeled
    #    mart / wide table be classified sensibly).
    grain = _grain_column(profiling_rows)
    measures = _measurable_columns(profiling_rows)
    n_measures = len(measures)
    n_cols = len(profiling_rows)
    if grain and n_measures <= 1:
        score["dimension"] += 0.2
        reasons.append(f"unique key '{grain}', few measures")
    if n_measures >= 3:
        score["fact"] += 0.3
        reasons.append(f"{n_measures} numeric measure-like columns")
    elif n_measures == 2:
        score["fact"] += 0.15
        reasons.append("2 numeric measure-like columns")
    # Wide denormalized / "one big table": many columns AND several measures,
    # with no outbound FKs (attributes are inline, not joined out). This reads
    # like a self-contained fact/source you single-source a view from -- NOT a
    # dimension, even though it has a key. Give it a distinct nudge so it isn't
    # mislabeled a dimension just because it has a unique key.
    if n_cols >= 12 and n_measures >= 3 and outbound_fk_count == 0:
        score["fact"] += 0.25
        reasons.append(f"wide denormalized table ({n_cols} cols, {n_measures} measures, inline attributes)")

    # 4. Ontology entity_role (steward/AI attribution) is a trusted nudge.
    if ontology_role:
        r = ontology_role.lower()
        if r in ("primary", "fact"):
            score["fact"] += 0.25
            reasons.append(f"ontology role '{ontology_role}'")
        elif r in ("referenced", "dimension", "contextual"):
            score["dimension"] += 0.25
            reasons.append(f"ontology role '{ontology_role}'")

    top = max(score.values())
    if top == 0.0:
        # No signal at all -> treat as a standalone source table.
        return "source", 0.3, ["no FK/profiling/ontology signal"]
    # Resolve ties with a MEANINGFUL order instead of dict-insertion order (which
    # always picked 'fact' first -- review finding #5). FK topology decides first
    # (outbound => fact-like, inbound-only => dimension-like), then measure count,
    # then a stable fallback. Only roles at the top score compete.
    tied = [r for r, v in score.items() if v == top]
    if len(tied) == 1:
        role = tied[0]
    else:
        def _tiebreak(r: str):
            if r == "bridge":
                return (3 if is_bridge else -1)
            if r == "fact":
                return (2 if outbound_fk_count >= 1 else 0) + (1 if n_measures >= 2 else 0)
            if r == "dimension":
                return (2 if (inbound_fk_count >= 1 and outbound_fk_count == 0) else 0) + (1 if grain else 0)
            if r == "source":
                return 1  # neutral standalone
            return 0
        role = max(tied, key=_tiebreak)
    confidence = round(min(top, 1.0), 3)
    return role, confidence, reasons


def _recommend_view_count(
    facts: list[str],
    uncovered_tables: list[str],
    missing_kpis: list[str],
    current_views: int,
) -> tuple[int, list[str]]:
    """Coverage-aware target: base on fact tables + uncovered work, NOT raw table
    count. Returns (recommended_total, reasons).

    Each uncovered fact/source anchors ~1 view. Missing KPIs are grouped a few
    per view (a metric view holds many measures), so they add ceil(n / KPIS_PER_
    VIEW) views rather than one-per-KPI. The total is floored at the fact base
    and capped at MAX_RECOMMENDED_VIEWS.
    """
    reasons: list[str] = []
    base = max(len(facts) * VIEWS_PER_FACT, 1)
    reasons.append(f"{len(facts)} fact table(s) x {VIEWS_PER_FACT} view")

    uncovered_need = len(uncovered_tables)
    if uncovered_tables:
        reasons.append(f"{len(uncovered_tables)} table(s) with no validated view")

    # Multiple KPIs can be satisfied by one view's measures -> group them.
    kpi_need = -(-len(missing_kpis) // KPIS_PER_VIEW) if missing_kpis else 0  # ceil div
    if missing_kpis:
        reasons.append(f"{len(missing_kpis)} KPI(s) with no implementing measure")

    recommended = min(
        max(base, current_views + uncovered_need + kpi_need),
        MAX_RECOMMENDED_VIEWS,
    )
    return recommended, reasons


def recommend_erd(
    tables: list[str],
    fk_rows: Optional[list[dict]] = None,
    ontology_rows: Optional[list[dict]] = None,
    profiling_by_table: Optional[dict[str, list[dict]]] = None,
    existing_defs: Optional[list[dict]] = None,
    kpi_coverage: Optional[dict] = None,
) -> ErdRecommendation:
    """Pure ERD + sufficiency recommendation over already-fetched metadata rows.

    Args:
      tables: fully-qualified table names in scope.
      fk_rows: rows from fk_predictions (src/dst_table, src/dst_column,
        final_confidence, is_fk, join_rate, pk_uniqueness, ...).
      ontology_rows: rows from ontology_entities (entity_type, entity_role,
        source_tables). Used to attribute a role to a table.
      profiling_by_table: {fq_table: [column_profiling_stats rows]} for role /
        measure / grain inference.
      existing_defs: metric_view_definitions rows (source_table, status) to
        compute what's already covered.
      kpi_coverage: output of _compute_kpi_coverage() -- {implemented, missing,
        total}. `missing` drives extra recommended views.
    """
    fk_rows = fk_rows or []
    ontology_rows = ontology_rows or []
    profiling_by_table = profiling_by_table or {}
    existing_defs = existing_defs or []
    kpi_coverage = kpi_coverage or {}

    lower_tables = {t.lower(): t for t in tables}

    # --- FK topology counts + bridge detection (mirrors profile_schema) -------
    outbound: dict[str, int] = {t.lower(): 0 for t in tables}
    inbound: dict[str, int] = {t.lower(): 0 for t in tables}
    src_set, dst_set = set(), set()
    for fk in fk_rows:
        conf = float(fk.get("final_confidence") or 0.0)
        if not fk.get("is_fk") and conf < FK_CONFIDENCE_MIN:
            continue
        st = (fk.get("src_table") or "").lower()
        dt = (fk.get("dst_table") or "").lower()
        if st in outbound:
            outbound[st] += 1
            src_set.add(st)
        if dt in inbound:
            inbound[dt] += 1
            dst_set.add(dt)
    bridges = src_set & dst_set

    # --- Ontology role per table (first attributed role wins) -----------------
    onto_role: dict[str, str] = {}
    for row in ontology_rows:
        role = row.get("entity_role")
        if not role:
            continue
        for st in _coerce_list(row.get("source_tables")):
            key = st.lower()
            if key in lower_tables and key not in onto_role:
                onto_role[key] = role

    # --- Build nodes ----------------------------------------------------------
    nodes: list[ErdNode] = []
    for t in tables:
        key = t.lower()
        prof = profiling_by_table.get(t) or profiling_by_table.get(key) or []
        role, conf, reasons = _infer_role(
            table=t,
            profiling_rows=prof,
            outbound_fk_count=outbound.get(key, 0),
            inbound_fk_count=inbound.get(key, 0),
            ontology_role=onto_role.get(key),
            is_bridge=key in bridges,
        )
        nodes.append(ErdNode(
            table=t,
            role=role,
            confidence=conf,
            reasons=reasons,
            measurable_columns=_measurable_columns(prof),
            grain=_grain_column(prof),
        ))

    edges = _build_edges(fk_rows)

    # --- Schema type (reuse profile_schema's classification rules) ------------
    fk_count = len(edges)
    n_tables = len(tables)
    mart_tables = [t for t in tables if _looks_like_mart(_short(t))]
    # A schema is "mart-dominated" when mart/summary tables are the majority of
    # what's in scope -- regardless of whether some are also fact-named. Such
    # tables are pre-joined/aggregated: usually one view each, few or no joins.
    mart_dominated = len(mart_tables) >= max(1, (n_tables + 1) // 2)
    if mart_dominated and fk_count == 0:
        schema_type = "DATA_MART"
    elif fk_count == 0 or (fk_count <= 1 and n_tables <= 3):
        schema_type = "SIMPLE"
    elif bridges:
        schema_type = "SNOWFLAKE"
    else:
        schema_type = "STAR"

    # --- Sufficiency ----------------------------------------------------------
    facts = [n.table for n in nodes if n.role == "fact"]
    if not facts:
        # No clear fact: sources/marts each anchor a view.
        facts = [n.table for n in nodes if n.role in ("source", "bridge")] or tables

    covered = {
        (d.get("source_table") or "").lower()
        for d in existing_defs
        if (d.get("status") or "") in ("validated", "applied")
    }
    uncovered_tables = [n.table for n in nodes
                        if n.role in ("fact", "source") and n.table.lower() not in covered]
    missing_kpis = list(kpi_coverage.get("missing") or [])
    # Count only realized views (validated/applied) so current_views is
    # consistent with `covered` -- 'created'/'failed' drafts must not inflate it
    # (which would spuriously shrink the recommended gap to zero).
    current_views = sum(
        1 for d in existing_defs if (d.get("status") or "") in ("validated", "applied")
    )

    recommended, reasons = _recommend_view_count(
        facts, uncovered_tables, missing_kpis, current_views
    )
    sufficiency = Sufficiency(
        metric_views_current=current_views,
        metric_views_recommended=recommended,
        metric_views_gap=max(recommended - current_views, 0),
        reasons=reasons,
        uncovered_tables=uncovered_tables,
        missing_kpis=missing_kpis,
    )

    return ErdRecommendation(
        nodes=nodes,
        edges=edges,
        sufficiency=sufficiency,
        schema_type=schema_type,
    )


def _distinct_domains(ontology_rows: list[dict], tables: list[str]) -> set[str]:
    """Distinct entity_types attributed to the in-scope tables (proxy for the
    number of business domains a question/KPI set should span)."""
    lower_tables = {t.lower() for t in tables}
    domains: set[str] = set()
    for row in ontology_rows or []:
        et = row.get("entity_type")
        if not et:
            continue
        if any(st.lower() in lower_tables for st in _coerce_list(row.get("source_tables"))):
            domains.add(et)
    return domains


def recommend_questions_kpis(
    tables: list[str],
    current_questions: int = 0,
    current_kpis: int = 0,
    kpi_coverage: Optional[dict] = None,
    fk_rows: Optional[list[dict]] = None,
    ontology_rows: Optional[list[dict]] = None,
    profiling_by_table: Optional[dict[str, list[dict]]] = None,
    existing_defs: Optional[list[dict]] = None,
) -> dict[str, GenSufficiency]:
    """Coverage-aware 'generate more?' recommendations for questions and KPIs.

    Reuses recommend_erd() to identify fact tables + join structure, and ontology
    entity_types as a proxy for business-domain breadth. Targets:
      - questions: QUESTIONS_PER_FACT per fact + QUESTIONS_PER_DOMAIN per domain,
        capped; recommend more when below target.
      - kpis: KPIS_PER_FACT per fact, plus any KPIs currently missing an
        implementing measure (from kpi_coverage), capped.

    Returns {"questions": GenSufficiency, "kpis": GenSufficiency}. Pure; the API
    layer supplies the current counts + already-fetched rows.
    """
    kpi_coverage = kpi_coverage or {}
    rec = recommend_erd(
        tables=tables, fk_rows=fk_rows, ontology_rows=ontology_rows,
        profiling_by_table=profiling_by_table, existing_defs=existing_defs,
        kpi_coverage=kpi_coverage,
    )
    facts = [n.table for n in rec.nodes if n.role == "fact"] \
        or [n.table for n in rec.nodes if n.role in ("source", "bridge")] or tables
    n_facts = len(facts)
    domains = _distinct_domains(ontology_rows or [], tables)
    n_domains = len(domains)

    # --- Questions ---
    q_target = min(
        n_facts * QUESTIONS_PER_FACT + n_domains * QUESTIONS_PER_DOMAIN,
        MAX_RECOMMENDED_QUESTIONS,
    )
    q_target = max(q_target, QUESTIONS_PER_FACT)  # always at least a few
    q_reasons = [f"{n_facts} fact table(s) x {QUESTIONS_PER_FACT} questions"]
    if n_domains:
        q_reasons.append(f"+{n_domains} business domain(s)")
    q = GenSufficiency(
        kind="questions",
        current=current_questions,
        recommended=q_target,
        gap=max(q_target - current_questions, 0),
        should_generate_more=current_questions < q_target,
        reasons=q_reasons,
    )

    # --- KPIs ---
    missing_kpis = list(kpi_coverage.get("missing") or [])
    k_target = min(n_facts * KPIS_PER_FACT, MAX_RECOMMENDED_KPIS)
    k_target = max(k_target, current_kpis + len(missing_kpis))  # never below what's already needed
    k_target = min(k_target, MAX_RECOMMENDED_KPIS)
    k_reasons = [f"{n_facts} fact table(s) x {KPIS_PER_FACT} KPIs"]
    if missing_kpis:
        k_reasons.append(f"{len(missing_kpis)} defined KPI(s) not yet implemented by a measure")
    k = GenSufficiency(
        kind="kpis",
        current=current_kpis,
        recommended=k_target,
        gap=max(k_target - current_kpis, 0),
        should_generate_more=(current_kpis < k_target) or bool(missing_kpis),
        reasons=k_reasons,
    )

    return {"questions": q, "kpis": k}

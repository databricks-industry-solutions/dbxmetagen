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
import math
import re
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

# PQ-5: role-inference signal weights, lifted to constants so naming is a tie-BREAKER,
# not a decider. Customers don't universally use fct_/dim_ prefixes, so naming is now
# weighted BELOW the data/structure signals (FK topology, profiling shape, ontology).
ROLE_NAMING_WEIGHT = 0.20         # was 0.35 -- naming prefix hint (fact/dim)
ROLE_MART_NAMING_WEIGHT = 0.20    # was 0.30 -- mart/summary naming hint
ROLE_FK_TOPOLOGY_WEIGHT = 0.35    # outbound/inbound FK degree (data-driven)
ROLE_ONTOLOGY_WEIGHT = 0.25       # ontology entity_role (steward/AI attribution)

# Sufficiency: how many metric views a schema "wants". One comprehensive view per
# fact/source GRAIN anchor; dimensions attach as joins (never their own view).
# Standalone measurable tables earn a diminishing bump; see _recommend_view_count.
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
    cardinality: str = "many_to_one" # many_to_one (safe star join) | one_to_many (fan-out risk)
    fanout_risk: bool = False        # join target isn't a clean unique key -> may multiply rows


@dataclass
class Sufficiency:
    metric_views_current: int = 0
    metric_views_recommended: int = 0
    metric_views_gap: int = 0
    reasons: list[str] = field(default_factory=list)
    uncovered_tables: list[str] = field(default_factory=list)
    missing_kpis: list[str] = field(default_factory=list)
    fanout_warnings: list[str] = field(default_factory=list)


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


# Key-vs-measure discrimination is DATA-DRIVEN, never name-based -- column-name
# conventions don't generalize across schemas, so DATA decides everything it
# can, in priority order:
#   1. FK PARTICIPATION (`key_hints`): a column that joins is a key, whatever it
#      is called. Primary, fully schema-agnostic signal.
#   2. TYPE + UNIQUENESS, for the ambiguous case -- a unique numeric with NO FK
#      evidence, where profiling alone can't tell a surrogate key from a
#      continuous measure (both are `is_unique_candidate`). A CONTINUOUS numeric
#      (float/double/decimal) is a measure even when unique; a unique INTEGER is
#      an identifier.
#   3. NAME, as a MILD tiebreaker ONLY where the data is genuinely silent -- a
#      unique numeric whose `data_type` is unknown (thin/federated profiling).
#      The lexical prior is a weak nudge, never a gate: it cannot override FK
#      participation or a clear continuous/integer type, and defaults to
#      "identifier" when the name is uninformative.
# `data_type` is profiling's readable type string, e.g. "long", "integer",
# "double", "decimal(18,2)", "string" (Spark type name, lower-cased).
_CONTINUOUS_NUMERIC_PREFIXES = ("double", "float", "decimal", "numeric", "real")
_INTEGER_NUMERIC_PREFIXES = (
    "int", "integer", "long", "bigint", "short", "smallint", "byte", "tinyint",
)

# Weak lexical priors (token-based, not substring, so "account" never matches
# "count"). Used only as a tiebreaker per rule 3 above and to lightly order grain
# candidates -- deliberately small; missing a term just means "no hint", not a
# wrong answer.
_KEY_NAME_TOKENS = frozenset((
    "id", "key", "code", "guid", "uuid", "sk", "pk", "num", "no", "nbr", "number",
))
_MEASURE_NAME_TOKENS = frozenset((
    "amount", "amt", "total", "qty", "quantity", "price", "cost", "revenue",
    "sales", "balance", "count", "sum", "avg", "rate", "value", "volume", "spend",
))


def _is_continuous_numeric(data_type: Optional[str]) -> bool:
    """True for fractional numeric types (a continuous MEASURE shape)."""
    if not data_type:
        return False
    return data_type.strip().lower().startswith(_CONTINUOUS_NUMERIC_PREFIXES)


def _is_integer_numeric(data_type: Optional[str]) -> bool:
    """True for integer numeric types (an identifier/surrogate-key shape)."""
    if not data_type:
        return False
    return data_type.strip().lower().startswith(_INTEGER_NUMERIC_PREFIXES)


def _name_hint(name: Optional[str]) -> Optional[str]:
    """A MILD lexical prior: 'key' | 'measure' | None from name tokens. A weak
    tiebreaker only -- callers must let FK/type signals win first. Token-based
    (split on non-alphanumerics, trailing plural 's' stripped) so it never fires
    on a substring (e.g. "account" is not "count")."""
    if not name:
        return None
    key_hit = measure_hit = False
    for tok in re.split(r"[^a-z0-9]+", name.lower()):
        if not tok:
            continue
        base = tok[:-1] if tok.endswith("s") and len(tok) > 1 else tok
        if tok in _KEY_NAME_TOKENS or base in _KEY_NAME_TOKENS:
            key_hit = True
        if tok in _MEASURE_NAME_TOKENS or base in _MEASURE_NAME_TOKENS:
            measure_hit = True
    if key_hit == measure_hit:      # neither, or ambiguous both -> no signal
        return None
    return "key" if key_hit else "measure"


def _as_bool(v: Any) -> bool:
    """Coerce a profiling/FK flag to a real bool. The app feeds these dicts from
    ``execute_sql``, whose SQL-statement-API ``data_array`` returns EVERY value as a
    STRING -- so a boolean column arrives as the string ``"true"``/``"false"``, and
    ``bool("false")`` is True. Relying on truthiness therefore marks every column
    ``is_unique_candidate`` (and every FK ``is_fk``), which made integer measure
    columns look like unique keys -> dropped from measures -> integer-grain fact tables
    (e.g. order_lines/inventory_snapshots with LONG quantities) were mislabeled
    bridge/dimension. Parse the value instead of trusting truthiness."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("true", "1", "t", "yes", "y")


def _is_key_like(col: dict, key_hints: Optional[set] = None) -> bool:
    """Data-driven: is this column an identifier / join key (not a measure)?

    Priority: FK participation (`key_hints`) wins outright; otherwise a column
    must be near-unique to be a key at all. Among unique columns, TYPE decides
    where it can -- a continuous numeric (e.g. a high-precision amount) is a
    measure even if unique, a unique integer/string/date is an identifier -- and
    only when the type is UNKNOWN does the mild name prior (`_name_hint`) break
    the tie, defaulting to identifier."""
    name = (col.get("column_name") or "").lower()
    if key_hints and name in key_hints:
        return True
    is_unique = (
        _as_bool(col.get("is_unique_candidate"))
        or float(col.get("cardinality_ratio") or 0.0) >= PK_UNIQUENESS_MIN
    )
    if not is_unique:
        return False
    if _as_bool(col.get("has_numeric_stats")):
        dt = col.get("data_type")
        if _is_continuous_numeric(dt):
            return False            # continuous numeric = measure (type wins)
        if _is_integer_numeric(dt):
            return True             # integer + unique = identifier (type wins)
        # Type is silent -> mild name prior breaks the tie; default to identifier.
        return _name_hint(name) != "measure"
    return True                     # unique non-numeric (string/date) = key


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
        is_fk = _as_bool(fk.get("is_fk"))
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
        pk_uniq = fk.get("pk_uniqueness")
        if pk_uniq is not None:
            reasons.append(f"pk_uniqueness={float(pk_uniq):.2f}")
        # Fan-out risk: the referenced (dst) side isn't a clean unique key, so the
        # join is not a safe many-to-one star join -- aggregating the source across
        # it can multiply rows and inflate measures. pk_uniqueness is GREATEST of
        # both columns' cardinality ratios, so < PK_UNIQUENESS_MIN means neither
        # side is near-unique.
        fanout = pk_uniq is not None and float(pk_uniq) < PK_UNIQUENESS_MIN
        edges.append(ErdEdge(
            src=src_t,
            dst=dst_t,
            on=f"src.{src_c} = {_short(dst_t)}.{dst_c}",
            confidence=1.0 if is_fk else round(conf, 3),
            source="confirmed" if confirmed else "predicted",
            reasons=reasons,
            cardinality="one_to_many" if fanout else "many_to_one",
            fanout_risk=fanout,
        ))
    return edges


def _measurable_columns(profiling_rows: list[dict], key_hints: Optional[set] = None) -> list[str]:
    """Numeric, low-null columns that make good measures. A column is dropped only
    when it is KEY-LIKE (`_is_key_like`): it participates in an FK, or it is a
    unique non-continuous numeric (a surrogate/natural key). A unique CONTINUOUS
    numeric (e.g. a high-precision amount) stays a measure. This is symmetric with
    `_grain_column`, so the two never disagree, and it is name-independent."""
    out = []
    for c in profiling_rows or []:
        if not _as_bool(c.get("has_numeric_stats")):
            continue
        null_rate = float(c.get("null_rate") or 0.0)
        if null_rate > 0.5:
            continue
        name = c.get("column_name")
        if not name:
            continue
        # A key/identifier is not a measure. Continuous numerics are never keys
        # (see _is_key_like), so a unique amount/price is correctly kept here.
        if _is_key_like(c, key_hints):
            continue
        out.append(name)
    return out


def _grain_column(profiling_rows: list[dict], key_hints: Optional[set] = None) -> Optional[str]:
    """Best identifier/grain candidate, chosen from DATA not names: unique + low
    null and NOT a continuous-numeric measure. A high-cardinality continuous
    measure is unique + low-null too, so uniqueness alone must not win -- type
    tells them apart (`_is_key_like`), otherwise a measure gets mislabeled the
    grain (which also poisons role inference via the 'has key, few measures ->
    dimension' nudge). Ranking prefers FK-participating columns, then non-numeric
    keys (natural keys/codes), then integer identifiers, with a MILD name nudge
    among equals; ties break on cardinality."""
    # rank: 2 = FK column, 1 = non-numeric key candidate, 0 = integer identifier.
    # +0.5 name nudge lets an id-named candidate edge out an unnamed peer of the
    # same structural rank -- ordering only, it excludes nothing.
    best = None
    best_key: tuple = (-1.0, -1.0)  # (rank, cardinality_ratio)
    for c in profiling_rows or []:
        name = c.get("column_name")
        if not name:
            continue
        ratio = float(c.get("cardinality_ratio") or 0.0)
        low_null = float(c.get("null_rate") or 0.0) <= 0.05
        is_unique = _as_bool(c.get("is_unique_candidate")) or ratio >= PK_UNIQUENESS_MIN
        if not (is_unique and low_null):
            continue
        # A continuous numeric is a measure, never the grain.
        if not _is_key_like(c, key_hints):
            continue
        if key_hints and name.lower() in key_hints:
            rank = 2.0
        elif not _as_bool(c.get("has_numeric_stats")):
            rank = 1.0
        else:
            rank = 0.0
        if _name_hint(name) == "key":
            rank += 0.5
        cand = (rank, ratio)
        if cand > best_key:
            best_key = cand
            best = name
    return best


def _infer_role(
    table: str,
    profiling_rows: list[dict],
    outbound_fk_count: int,
    inbound_fk_count: int,
    ontology_role: Optional[str],
    is_bridge: bool,
    key_hints: Optional[set] = None,
) -> tuple[str, float, list[str]]:
    """Combine naming, FK topology, profiling and ontology into a role + why."""
    short = _short(table)
    reasons: list[str] = []
    score = {"fact": 0.0, "dimension": 0.0, "source": 0.0, "bridge": 0.0}

    # 1. Naming conventions. A tie-BREAKER hint WHEN PRESENT (PQ-5). Many real schemas
    #    (marts, denormalized/OBT tables) aren't named fct_/dim_, so naming is weighted
    #    BELOW the data/structure signals below -- see ROLE_*_WEIGHT constants.
    if _looks_like_fact(short):
        score["fact"] += ROLE_NAMING_WEIGHT
        reasons.append("fact naming prefix")
    if _looks_like_dim(short):
        score["dimension"] += ROLE_NAMING_WEIGHT
        reasons.append("dimension naming prefix")
    if _looks_like_mart(short):
        score["source"] += ROLE_MART_NAMING_WEIGHT
        reasons.append("mart/summary naming")

    # 2. FK topology (data-driven). Facts reference many dims (outbound); dims are
    #    referenced by many (inbound); a table that is both src and dst is a bridge.
    if is_bridge:
        score["bridge"] += 0.5
        reasons.append("bridges FK chains (both src and dst)")
    if outbound_fk_count >= FACT_MIN_OUTBOUND_FKS:
        score["fact"] += ROLE_FK_TOPOLOGY_WEIGHT
        reasons.append(f"{outbound_fk_count} outbound FKs")
    if inbound_fk_count >= 1 and outbound_fk_count == 0:
        score["dimension"] += ROLE_FK_TOPOLOGY_WEIGHT
        reasons.append(f"referenced by {inbound_fk_count} table(s), no outbound FKs")

    # 3. Profiling shape (naming-independent -- this is what lets an unlabeled
    #    mart / wide table be classified sensibly).
    grain = _grain_column(profiling_rows, key_hints)
    measures = _measurable_columns(profiling_rows, key_hints)
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
            score["fact"] += ROLE_ONTOLOGY_WEIGHT
            reasons.append(f"ontology role '{ontology_role}'")
        elif r in ("referenced", "dimension", "contextual"):
            score["dimension"] += ROLE_ONTOLOGY_WEIGHT
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
    anchors: list[str],
    orphans: list[str],
    missing_kpis: list[str],
    current_views: int,
    no_clear_anchor: bool = False,
) -> tuple[int, list[str]]:
    """Grain-anchor target: one comprehensive metric view per fact/source GRAIN.
    Returns (recommended_total, reasons).

    Dimensions attach as joins, never their own view. Standalone (disconnected)
    measurable tables earn a small, strongly-diminishing bump -- floor(sqrt(n)) --
    so they aren't left uncovered, but the total never approaches the raw table
    count. Missing KPIs are a coverage NOTE (add measures to the relevant grain
    view), NOT extra views. Floored at current_views, capped at MAX_RECOMMENDED_VIEWS.
    """
    reasons: list[str] = []
    base = max(len(anchors), 1)
    if no_clear_anchor:
        reasons.append(
            f"no clear fact/grain — review table classification; anchoring {base} view(s)"
        )
    else:
        reasons.append(
            f"{len(anchors)} grain anchor(s) — one comprehensive metric view per grain"
        )

    # Diminishing: 1 orphan -> +1, 4 -> +2, 10 -> +3. Never one-view-per-table.
    bonus = int(math.floor(math.sqrt(len(orphans)))) if orphans else 0
    if orphans:
        reasons.append(f"+{bonus} for {len(orphans)} standalone table(s) not joined to a fact")

    if missing_kpis:
        reasons.append(
            f"{len(missing_kpis)} KPI(s) need a measure — add to the relevant grain view "
            f"(not a new view)"
        )

    # The recommendation is the GRAIN target (anchors + diminishing orphan bump), NOT
    # floored at current_views. Flooring at existing views made the number volatile
    # (it changed as views were applied) so the displayed "recommended: N" diverged from
    # the generation cap actually enforced. `current_views` still drives metric_views_gap
    # in the caller, but no longer inflates the target itself.
    recommended = min(base + bonus, MAX_RECOMMENDED_VIEWS)
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
    # Per-table set of columns that participate in a FK (either side). These are
    # the real join keys, used as grain hints so the natural-key detector prefers
    # them over a high-cardinality measure.
    fk_cols_by_table: dict[str, set] = {t.lower(): set() for t in tables}
    src_set, dst_set = set(), set()
    for fk in fk_rows:
        conf = float(fk.get("final_confidence") or 0.0)
        if not _as_bool(fk.get("is_fk")) and conf < FK_CONFIDENCE_MIN:
            continue
        st = (fk.get("src_table") or "").lower()
        dt = (fk.get("dst_table") or "").lower()
        sc = (fk.get("src_column") or "").split(".")[-1].lower()
        dc = (fk.get("dst_column") or "").split(".")[-1].lower()
        if st in outbound:
            outbound[st] += 1
            src_set.add(st)
            if sc:
                fk_cols_by_table[st].add(sc)
        if dt in inbound:
            inbound[dt] += 1
            dst_set.add(dt)
            if dc:
                fk_cols_by_table[dt].add(dc)
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
        hints = fk_cols_by_table.get(key) or set()
        role, conf, reasons = _infer_role(
            table=t,
            profiling_rows=prof,
            outbound_fk_count=outbound.get(key, 0),
            inbound_fk_count=inbound.get(key, 0),
            ontology_role=onto_role.get(key),
            is_bridge=key in bridges,
            key_hints=hints,
        )
        nodes.append(ErdNode(
            table=t,
            role=role,
            confidence=conf,
            reasons=reasons,
            measurable_columns=_measurable_columns(prof, hints),
            grain=_grain_column(prof, hints),
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

    # --- Sufficiency: grain anchors, not table count --------------------------
    # A metric view is built per fact/source GRAIN; dimensions attach as joins,
    # never as their own view. Anchors are fact/source/bridge nodes that actually
    # have something to aggregate (measurable columns).
    # A fact IS a grain regardless of numeric measures -- COUNT(*) is always a valid
    # measure at a fact's grain, so a measure-less fact (e.g. an event log whose columns
    # are all keys/ids, like fact_clinical_event) still anchors its own metric view. Only
    # source/bridge nodes need a detectable measure to earn their own view.
    anchor_nodes = [n for n in nodes
                    if n.role == "fact"
                    or (n.role in ("source", "bridge") and n.measurable_columns)]
    if not anchor_nodes:
        # No fact and no measurable source/bridge: relax to any source/bridge (never
        # dimensions), so a fact-less but measurable schema still gets an anchor.
        anchor_nodes = [n for n in nodes if n.role in ("source", "bridge")]
    no_clear_anchor = False
    if not anchor_nodes:
        # No structural fact/source/bridge signal at all. Do NOT invent anchors:
        # neither from every table (the old bug that reported N fact tables / N
        # views for an all-dimension schema) nor from every measurable table
        # (that just re-degenerates to one-view-per-table). Leave anchors empty --
        # the count collapses to a single starter view (base = max(0, 1)) and any
        # disconnected measurable table still earns the diminishing orphan bump
        # below, so we never approach the raw table count.
        no_clear_anchor = True
        anchor_nodes = []
    anchors = [n.table for n in anchor_nodes]
    anchor_set = {t.lower() for t in anchors}

    # A table connected to an anchor via any edge joins INTO that anchor's view --
    # it doesn't need its own. A standalone (disconnected) table that still has
    # measures would otherwise get zero coverage, so it earns a small, strongly-
    # diminishing bump (see _recommend_view_count), never one-view-per-table.
    connected: set = set()
    for e in edges:
        s, d = e.src.lower(), e.dst.lower()
        if s in anchor_set:
            connected.add(d)
        if d in anchor_set:
            connected.add(s)
    orphans = [n.table for n in nodes
               if n.table.lower() not in anchor_set
               and n.role in ("dimension", "source", "bridge")
               and n.measurable_columns
               and n.table.lower() not in connected]

    covered = {
        (d.get("source_table") or "").lower()
        for d in existing_defs
        if (d.get("status") or "") in ("validated", "applied")
    }
    # The real gap is anchors (grain views) not yet realized.
    uncovered_tables = [t for t in anchors if t.lower() not in covered]
    missing_kpis = list(kpi_coverage.get("missing") or [])
    # Count only realized views (validated/applied) so current_views is
    # consistent with `covered` -- 'created'/'failed' drafts must not inflate it
    # (which would spuriously shrink the recommended gap to zero).
    current_views = sum(
        1 for d in existing_defs if (d.get("status") or "") in ("validated", "applied")
    )
    # Fan-out warnings: one line per risky join (detect-and-warn; no generation change).
    fanout_warnings = [
        f"{_short(e.src)} → {_short(e.dst)} join may fan out ({'; '.join(e.reasons) or 'low pk uniqueness'})"
        f" — use COUNT(DISTINCT) and don't SUM/AVG dimension attributes at this grain"
        for e in edges if e.fanout_risk
    ]

    recommended, reasons = _recommend_view_count(
        anchors, orphans, missing_kpis, current_views, no_clear_anchor
    )
    sufficiency = Sufficiency(
        metric_views_current=current_views,
        metric_views_recommended=recommended,
        metric_views_gap=max(recommended - current_views, 0),
        reasons=reasons,
        uncovered_tables=uncovered_tables,
        missing_kpis=missing_kpis,
        fanout_warnings=fanout_warnings,
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

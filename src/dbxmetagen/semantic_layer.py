"""
Semantic Layer Generator module.

Uses AI_QUERY to auto-generate Unity Catalog metric view definitions from
user business questions and existing catalog metadata (knowledge bases,
ontology entities, FK predictions). Outputs JSON that is converted to YAML
for CREATE METRIC VIEW statements.
"""

import json
import logging
import os
import re
import uuid
import yaml
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

# Shared, substrate-agnostic metric-view helpers (single source of truth; also
# imported by the app backend). Re-exported here for backward compatibility so
# existing `from dbxmetagen.semantic_layer import ...` call sites keep working.
from dbxmetagen.metric_view_core import (
    _infer_display_name,
    _infer_synonyms,
    _backfill_agent_metadata,
    _drop_broken_measures,
    _drop_placeholder_dimensions,
    _normalize_window_specs,
    _strip_kpi_references,
    _infer_format_specs,
    _fix_percentage_scaling,
    _KPI_REF_RE,
    _SELF_DIV_RE,
    _ALIAS_DOT_RE,
    _CURRENCY_PATTERNS,
    _PERCENTAGE_PATTERNS,
    _PERCENTAGE_NAME_PATTERNS,
    _autofix_expr as _autofix_expr_core,
    _restructure_chained_to_nested,
    _qualify_nested_refs,
)
from dbxmetagen import metric_view_core as _mvc

try:
    import mlflow
except ImportError:
    mlflow = None


@contextmanager
def _trace_span(name: str):
    """Context manager: mlflow.start_span when mlflow available, else no-op. Yields span or None."""
    if mlflow is None:
        yield None
        return
    with mlflow.start_span(name=name) as span:
        yield span

logger = logging.getLogger(__name__)

_DIM_PREFIXES = {"dim_", "dimension_", "d_"}
_FACT_PREFIXES = {"fct_", "fact_", "f_"}
_MART_KEYWORDS = {"summary", "agg", "mart", "report", "rollup", "cube", "snapshot"}


def profile_schema(table_names: list[str], fk_rows: list[dict],
                    col_by_table: dict[str, list] | None = None) -> dict:
    """Classify schema type and emit a SCHEMA PROFILE block for LLM context.

    Returns dict with keys: table_count, fk_count, fact_tables, dim_tables,
    mart_tables, schema_type, profile_text.
    """
    shorts = {t.split(".")[-1].lower() for t in table_names}
    fact_tables = [s for s in shorts if any(s.startswith(p) for p in _FACT_PREFIXES)]
    dim_tables = [s for s in shorts if any(s.startswith(p) for p in _DIM_PREFIXES)]
    mart_tables = [s for s in shorts if any(kw in s for kw in _MART_KEYWORDS)]

    # Detect multi-hop FK chains (A->B->C where B is both src and dst)
    if fk_rows:
        dst_set = {fk.get("dst_table", "").split(".")[-1].lower() for fk in fk_rows}
        src_set = {fk.get("src_table", "").split(".")[-1].lower() for fk in fk_rows}
        bridge_tables = dst_set & src_set
    else:
        bridge_tables = set()

    fk_count = len(fk_rows)
    n_tables = len(table_names)

    if mart_tables and not fact_tables and fk_count == 0:
        schema_type = "DATA_MART"
        guidance = "pre-aggregated/summary tables -- create simple single-table metric views with direct aggregations, no joins needed"
    elif fk_count == 0 or (fk_count <= 1 and n_tables <= 3):
        schema_type = "SIMPLE"
        guidance = "few or no FK relationships -- create single-table metric views with direct aggregations; do not fabricate joins"
    elif bridge_tables:
        schema_type = "SNOWFLAKE"
        guidance = "multi-hop FK chains detected -- use nested joins for dimension hierarchies and maximize dimension reach"
    else:
        schema_type = "STAR"
        guidance = "FK relationships available -- maximize cross-table joins and multi-dimension breakdowns"

    lines = [
        "\nSCHEMA PROFILE:",
        f"  Tables: {n_tables}, FK relationships: {fk_count}",
    ]
    if fact_tables:
        lines.append(f"  Fact tables (by naming): {', '.join(sorted(fact_tables))}")
    if dim_tables:
        lines.append(f"  Dimension tables (by naming): {', '.join(sorted(dim_tables))}")
    if mart_tables:
        lines.append(f"  Data mart / summary tables: {', '.join(sorted(mart_tables))}")
    lines.append(f"  Schema type: {schema_type} ({guidance})")

    return {
        "table_count": n_tables,
        "fk_count": fk_count,
        "fact_tables": fact_tables,
        "dim_tables": dim_tables,
        "mart_tables": mart_tables,
        "schema_type": schema_type,
        "profile_text": "\n".join(lines),
    }


def check_dim_source_pattern(defn: dict, fk_rows: list[dict]) -> Optional[dict]:
    """Detect dim-source + fact-join anti-pattern. Returns warning dict or None."""
    source = defn.get("source", "")
    joins = defn.get("joins", [])
    if not source or not joins:
        return None

    src_short = source.split(".")[-1].lower()
    join_tables = []
    def _collect_join_sources(jlist):
        for j in jlist:
            j_src = j.get("source", "")
            if j_src:
                join_tables.append(j_src)
            if j.get("joins"):
                _collect_join_sources(j["joins"])
    _collect_join_sources(joins)
    if not join_tables:
        return None

    join_shorts = {t.split(".")[-1].lower() for t in join_tables}
    signals = []

    # Signal 1: FK direction -- source is PK parent (dst_table) of a FK where child (src_table) is in joins
    for fk in fk_rows:
        fk_dst = fk.get("dst_table", "")
        fk_src = fk.get("src_table", "")
        if fk_dst.split(".")[-1].lower() == src_short and fk_src.split(".")[-1].lower() in join_shorts:
            signals.append(("fk_direction", f"source {src_short} is PK parent of {fk_src.split('.')[-1]}"))
            break

    # Signal 2: Table name prefixes
    src_is_dim = any(src_short.startswith(p) for p in _DIM_PREFIXES)
    fact_in_joins = [j for j in join_shorts if any(j.startswith(p) for p in _FACT_PREFIXES)]
    if src_is_dim and fact_in_joins:
        signals.append(("name_prefix", f"source {src_short} is dim-prefixed, joins include fact-prefixed {fact_in_joins}"))

    # Signal 3: FK fan-out count -- source appears as dst_table (PK target) far more than as src_table
    if fk_rows:
        as_dst = sum(1 for fk in fk_rows if fk.get("dst_table", "").split(".")[-1].lower() == src_short)
        as_src = sum(1 for fk in fk_rows if fk.get("src_table", "").split(".")[-1].lower() == src_short)
        if as_dst >= 2 and as_src == 0:
            signals.append(("fk_fanout", f"source {src_short} appears as FK target {as_dst}x, never as FK source"))

    if not signals:
        return None

    # Counter-signal: if source is NOT dim-prefixed and no join is fact-prefixed
    # but joins ARE dim-prefixed, FK signals alone are unreliable -- suppress.
    dim_in_joins = any(any(j.startswith(p) for p in _DIM_PREFIXES) for j in join_shorts)
    if not src_is_dim and not fact_in_joins and dim_in_joins:
        return None

    suspected_fact = fact_in_joins[0] if fact_in_joins else next(iter(join_shorts), "unknown")
    return {
        "source": source,
        "signals": signals,
        "suspected_fact": suspected_fact,
        "suspected_dim": src_short,
        "message": f"Dim-source pattern detected: {src_short} (dim) joined to {suspected_fact} (fact). "
                   f"Signals: {', '.join(s[0] for s in signals)}. This may produce inflated aggregates.",
    }


def _swap_source_and_join(defn: dict, fact_table: str) -> dict:
    """Create a copy with source/join swapped: fact becomes source, old source becomes join."""
    import copy
    swapped = copy.deepcopy(defn)
    old_source = swapped["source"]
    old_source_short = old_source.split(".")[-1]

    # Find the fact join to promote
    new_joins = []
    fact_join = None
    for j in swapped.get("joins", []):
        if j.get("source", "").split(".")[-1].lower() == fact_table.lower():
            fact_join = j
        else:
            new_joins.append(j)

    if not fact_join:
        return swapped

    swapped["source"] = fact_join["source"]
    # Flip the on clause direction
    on_clause = fact_join.get("on", "")
    if on_clause:
        parts = on_clause.split("=", 1)
        if len(parts) == 2:
            on_clause = f"{parts[1].strip()} = {parts[0].strip()}"

    new_joins.append({
        "name": old_source_short,
        "source": old_source,
        "on": on_clause,
    })
    swapped["joins"] = new_joins
    return swapped


@dataclass
class SemanticLayerConfig:
    """Configuration for the metric view semantic layer pipeline.

    Controls catalog/schema targeting, model selection, FK enrichment threshold,
    validation behavior, and two-phase generation mode. Use ``fq()`` to produce
    fully-qualified ``catalog.schema.table`` references.
    """

    catalog_name: str
    schema_name: str
    questions_table: str = "semantic_layer_questions"
    definitions_table: str = "metric_view_definitions"
    model_endpoint: str = "databricks-gpt-oss-120b"
    fk_confidence_threshold: float = 0.7
    validate_expressions: bool = True
    use_two_phase: bool = True
    validate_before_store: bool = True
    max_context_cols_per_table: int = 100
    max_join_hops: int = 2
    materialize_metric_views: bool = False
    materialization_schedule: str = "every 6 hours"

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"


@lru_cache(maxsize=4)
def _load_reference(name: str) -> dict:
    """Load a JSON reference file from configurations/agent_references/.

    Searches multiple candidate directories so it works whether the repo is
    git-cloned into the workspace or pip-installed as a package.
    Returns {} if the file is not found.
    """
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "..", "configurations", "agent_references", name),
        os.path.join("configurations", "agent_references", name),
        os.path.join("/Workspace", "configurations", "agent_references", name),
    ]
    for p in candidates:
        p = os.path.normpath(p)
        if os.path.isfile(p):
            with open(p) as f:
                return json.load(f)
    logger.debug("Reference file %s not found in any candidate path", name)
    return {}


def _format_reference_section(ref: dict, sections: list[str] | None = None) -> str:
    """Format selected sections of a reference dict into a prompt-friendly string."""
    if not ref:
        return ""
    parts = []
    keys = sections or list(ref.keys())
    for k in keys:
        val = ref.get(k)
        if val is None:
            continue
        if isinstance(val, list):
            parts.append(f"\n### {k}\n" + "\n".join(f"- {item}" if isinstance(item, str) else f"- {json.dumps(item)}" for item in val))
        elif isinstance(val, dict) and "description" in val:
            parts.append(f"\n### {k}\n{val['description']}")
            for sk, sv in val.items():
                if sk == "description":
                    continue
                if isinstance(sv, list):
                    for ex in sv:
                        if isinstance(ex, dict):
                            parts.append(f"  {ex.get('name', ex.get('alias', sk))}: {ex.get('expr', ex.get('template', json.dumps(ex)))}")
                elif isinstance(sv, dict) and "template" in sv:
                    parts.append(f"  Pattern: {sv['template']}")
                    for ex in sv.get("examples", []):
                        parts.append(f"    {ex.get('name', '')}: {ex.get('expr', '')}")
        elif isinstance(val, dict):
            parts.append(f"\n### {k}")
            for sk, sv in val.items():
                parts.append(f"  {sk}: {json.dumps(sv) if not isinstance(sv, str) else sv}")
        elif isinstance(val, str):
            parts.append(f"\n### {k}\n{val}")
    return "\n".join(parts)


# -- Few-shot examples for the AI prompt (domain-aware) -------------------

_FEW_SHOT_BY_DOMAIN = {
    "sales": """\
INPUT tables:
  sales.orders (Comment: "Customer orders") columns: [order_id BIGINT, customer_id BIGINT, order_date DATE, total_amount DECIMAL(10,2), region STRING, status STRING, is_returned BOOLEAN]
  sales.customers (Comment: "Customer master") columns: [id BIGINT, name STRING, segment STRING, signup_date DATE]
  FK: orders.customer_id -> customers.id (confidence 0.95)
INPUT questions:
  1. What is total revenue by region?  2. How many orders per month?  3. What is the fulfillment rate by segment?
OUTPUT:
[
  {"name": "order_performance_metrics", "source": "sales.orders",
   "comment": "Order performance including revenue, fulfillment rates, and return analysis",
   "filter": "status IS NOT NULL",
   "dimensions": [
     {"name": "Order Month", "expr": "DATE_TRUNC('MONTH', order_date)", "comment": "Month of order placement"},
     {"name": "Region", "expr": "region", "comment": "Sales region"},
     {"name": "Segment", "expr": "customers.segment", "comment": "Customer segment from joined customers table"}],
   "measures": [
     {"name": "Total Revenue", "expr": "SUM(total_amount)", "comment": "Sum of all order values", "display_name": "Total Revenue", "synonyms": ["revenue", "total sales", "gross revenue"], "format": {"type": "currency"}},
     {"name": "Avg Order Value", "expr": "AVG(total_amount)", "comment": "Average order amount", "display_name": "Avg Order Value", "synonyms": ["AOV", "average order size"], "format": {"type": "currency"}},
     {"name": "Revenue per Customer", "expr": "SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_id), 0)", "comment": "Average revenue per unique customer", "display_name": "Revenue per Customer", "synonyms": ["ARPC", "per-customer revenue"], "format": {"type": "currency"}},
     {"name": "Fulfillment Rate", "expr": "SUM(CASE WHEN status = 'fulfilled' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of orders fulfilled", "display_name": "Fulfillment Rate", "synonyms": ["fill rate", "completion rate"], "format": {"type": "percentage"}},
     {"name": "Fulfilled Revenue", "expr": "SUM(total_amount) FILTER (WHERE status = 'fulfilled')", "comment": "Revenue from fulfilled orders only", "display_name": "Fulfilled Revenue", "synonyms": ["completed revenue"], "format": {"type": "currency"}},
     {"name": "30-Day Rolling Avg Revenue", "expr": "AVG(SUM(total_amount))", "window": [{"order": "order_date", "range": "trailing 30 day", "semiadditive": "last"}], "comment": "Rolling 30-day average of daily revenue", "format": {"type": "currency", "currency_code": "USD"}}],
   "joins": [{"name": "customers", "source": "sales.customers", "on": "source.customer_id = customers.id"}]}
]""",
    "healthcare": """\
INPUT tables:
  clinical.encounters (Comment: "Patient encounters") columns: [encounter_id BIGINT, patient_id BIGINT, provider_id BIGINT, admit_date DATE, discharge_date DATE, encounter_type STRING, department STRING, total_charges DECIMAL(12,2), status STRING]
  clinical.patients (Comment: "Patient demographics") columns: [patient_id BIGINT, birth_date DATE, gender STRING, zip_code STRING, insurance_type STRING]
  FK: encounters.patient_id -> patients.patient_id (confidence 0.92)
INPUT questions:
  1. What is the average length of stay by department?  2. What is the readmission rate within 30 days?  3. How does patient volume trend by month?
OUTPUT:
[
  {"name": "encounter_throughput_metrics", "source": "clinical.encounters",
   "comment": "Encounter volume, throughput, and clinical outcome metrics",
   "filter": "status != 'cancelled'",
   "dimensions": [
     {"name": "Admit Month", "expr": "DATE_TRUNC('MONTH', admit_date)", "comment": "Month of admission"},
     {"name": "Department", "expr": "department", "comment": "Clinical department"},
     {"name": "Insurance Type", "expr": "insurance_type", "comment": "Patient insurance from joined patients table"}],
   "measures": [
     {"name": "Encounter Count", "expr": "COUNT(*)", "comment": "Total encounters", "display_name": "Encounter Count", "synonyms": ["visits", "admissions"], "format": {"type": "number"}},
     {"name": "Unique Patients", "expr": "COUNT(DISTINCT patient_id)", "comment": "Distinct patient count", "display_name": "Unique Patients", "synonyms": ["patient count", "distinct patients"], "format": {"type": "number"}},
     {"name": "Avg Length of Stay", "expr": "AVG(DATEDIFF(discharge_date, admit_date))", "comment": "Average days from admit to discharge", "display_name": "Avg Length of Stay", "synonyms": ["ALOS", "average LOS"], "format": {"type": "number"}},
     {"name": "Encounters per Patient", "expr": "COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT patient_id), 0)", "comment": "Average visits per patient", "display_name": "Encounters per Patient", "synonyms": ["visits per patient"], "format": {"type": "number"}},
     {"name": "Charge per Encounter", "expr": "SUM(total_charges) / NULLIF(COUNT(*), 0)", "comment": "Average charge per encounter", "display_name": "Charge per Encounter", "synonyms": ["cost per visit", "avg charge"], "format": {"type": "currency"}}],
   "joins": [{"name": "patients", "source": "clinical.patients", "on": "source.patient_id = patients.patient_id"}]}
]""",
    "finance": """\
INPUT tables:
  finance.transactions (Comment: "Financial transactions") columns: [txn_id BIGINT, account_id BIGINT, txn_date DATE, amount DECIMAL(12,2), txn_type STRING, category STRING, is_fraud BOOLEAN]
  finance.accounts (Comment: "Customer accounts") columns: [account_id BIGINT, customer_name STRING, account_type STRING, opened_date DATE, region STRING]
  FK: transactions.account_id -> accounts.account_id (confidence 0.94)
INPUT questions:
  1. What is the total transaction volume by category?  2. What is the fraud rate by account type?  3. How has monthly deposit growth trended?
OUTPUT:
[
  {"name": "transaction_risk_metrics", "source": "finance.transactions",
   "comment": "Transaction volume, fraud rates, and financial flow analysis",
   "dimensions": [
     {"name": "Transaction Month", "expr": "DATE_TRUNC('MONTH', txn_date)", "comment": "Month of transaction"},
     {"name": "Category", "expr": "category", "comment": "Transaction category"},
     {"name": "Account Type", "expr": "account_type", "comment": "Account classification from joined accounts"}],
   "measures": [
     {"name": "Transaction Count", "expr": "COUNT(*)", "comment": "Total transactions", "display_name": "Transaction Count", "synonyms": ["txn count", "number of transactions"], "format": {"type": "number"}},
     {"name": "Total Amount", "expr": "SUM(amount)", "comment": "Sum of transaction amounts", "display_name": "Total Transaction Amount", "synonyms": ["total value", "transaction volume"], "format": {"type": "currency"}},
     {"name": "Fraud Rate", "expr": "SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of flagged transactions", "display_name": "Fraud Rate", "synonyms": ["fraud percentage", "suspicious rate"], "format": {"type": "percentage"}},
     {"name": "Deposit Volume", "expr": "SUM(amount) FILTER (WHERE txn_type = 'deposit')", "comment": "Total deposit inflows", "display_name": "Deposit Volume", "synonyms": ["deposit total", "inflows"], "format": {"type": "currency"}}],
   "joins": [{"name": "accounts", "source": "finance.accounts", "on": "source.account_id = accounts.account_id"}]}
]""",
}


def _select_few_shot(context: str) -> str:
    """Pick the best few-shot example based on domain keywords in the context."""
    ctx_lower = context.lower()
    domain_keywords = {
        "healthcare": ["patient", "encounter", "provider", "clinical", "diagnosis", "admit", "discharge", "readmission"],
        "finance": ["transaction", "account", "ledger", "balance", "deposit", "withdrawal", "fraud", "loan"],
        "sales": ["order", "customer", "revenue", "product", "invoice", "shipment", "discount"],
    }
    scores = {d: sum(1 for kw in kws if kw in ctx_lower) for d, kws in domain_keywords.items()}
    best = max(scores, key=scores.get) if max(scores.values()) > 0 else "sales"
    return _FEW_SHOT_BY_DOMAIN[best]


def build_materialization(defn: dict, schedule: str = "every 6 hours") -> dict:
    """Build a default ``materialization`` block for a metric view definition.

    Produces a single ``unaggregated`` materialized view named ``<mv_name>_baseline``.
    Unaggregated needs no dimension/measure references, so it is always valid and
    accelerates all queries against the view. ``mode`` is always ``relaxed`` (the only
    supported value). ``schedule`` is omitted when blank (manual refresh only).
    """
    mv_name = defn.get("name") or "metric_view"
    block: dict = {"mode": "relaxed"}
    if schedule and schedule.strip():
        block["schedule"] = schedule.strip()
    block["materialized_views"] = [{"name": f"{mv_name}_baseline", "type": "unaggregated"}]
    return block


def validate_materialization(defn: dict) -> list[str]:
    """Structurally validate a ``materialization`` block against the metric view spec.

    Returns a list of error strings (empty if valid or absent). Checks: ``mode`` must be
    ``relaxed``; ``materialized_views`` must be a non-empty list; each entry needs a unique
    non-empty ``name`` and a ``type`` of ``aggregated``/``unaggregated``; ``aggregated``
    entries must reference only defined dimensions/measures; ``schedule`` (if present) must
    be a string without ``TRIGGER ON UPDATE``.
    """
    mat = defn.get("materialization")
    if mat is None:
        return []
    errors: list[str] = []
    if not isinstance(mat, dict):
        return ["materialization must be a mapping"]
    if mat.get("mode") != "relaxed":
        errors.append("materialization.mode must be 'relaxed'")
    schedule = mat.get("schedule")
    if schedule is not None:
        if not isinstance(schedule, str):
            errors.append("materialization.schedule must be a string")
        elif "TRIGGER ON UPDATE" in schedule.upper():
            errors.append("materialization.schedule does not support TRIGGER ON UPDATE")
    mvs = mat.get("materialized_views")
    if not isinstance(mvs, list) or not mvs:
        errors.append("materialization.materialized_views must be a non-empty list")
        return errors
    dim_names = {d.get("name") for d in defn.get("dimensions", []) if d.get("name")}
    measure_names = {m.get("name") for m in defn.get("measures", []) if m.get("name")}
    seen: set[str] = set()
    for entry in mvs:
        if not isinstance(entry, dict):
            errors.append("each materialized_views entry must be a mapping")
            continue
        name = entry.get("name")
        if not name:
            errors.append("materialized_views entry missing 'name'")
        elif name in seen:
            errors.append(f"duplicate materialized_views name '{name}'")
        else:
            seen.add(name)
        mtype = entry.get("type")
        if mtype not in ("aggregated", "unaggregated"):
            errors.append(f"materialized_views '{name}': type must be aggregated or unaggregated")
        if mtype == "aggregated":
            dims = entry.get("dimensions") or []
            meas = entry.get("measures") or []
            if not dims and not meas:
                errors.append(f"materialized_views '{name}': aggregated requires dimensions and/or measures")
            for d in dims:
                if d not in dim_names:
                    errors.append(f"materialized_views '{name}': unknown dimension '{d}'")
            for m in meas:
                if m not in measure_names:
                    errors.append(f"materialized_views '{name}': unknown measure '{m}'")
    return errors


class SemanticLayerGenerator:
    """Orchestrates metric view generation, validation, and deployment to Unity Catalog.

    Pipeline stages:
      1. ``create_tables`` -- provision control tables for questions and MV definitions
      2. ``ingest_questions`` -- store business questions as pending rows
      3. ``generate_metric_views`` -- read pending questions, build catalog context
         (KB, FK, ontology), call AI_QUERY (single- or two-phase), parse/validate
         JSON definitions, enrich joins from FK predictions, and persist to the
         definitions table
      4. ``apply_metric_views`` -- read validated definitions and execute
         ``CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML`` in UC
      5. ``create_genie_space`` -- assemble applied MVs + context into a Genie
         ``serialized_space`` and deploy via the Databricks REST API

    Two-phase generation (``use_two_phase=True``): phase 1 plans view names,
    sources, joins, and dimension/measure outlines; phase 2 fills in SQL
    expressions per view. This improves quality on large question sets.
    """

    def __init__(self, spark: SparkSession, config: SemanticLayerConfig):
        self.spark = spark
        self.config = config

    @staticmethod
    def _prioritize_columns(cols: list, col_props: dict) -> list:
        """Sort columns by usefulness for metric view generation."""
        def _sort_key(c):
            cp = col_props.get(c["column_name"], {})
            has_link = 1 if cp.get("linked") else 0
            has_fk_role = 1 if cp.get("role") in ("fk", "pk", "id") else 0
            is_id_pattern = 1 if re.search(r'(_id|_key|_code)$', c["column_name"], re.I) else 0
            has_comment = 1 if c.get("comment") else 0
            return (-has_link, -has_fk_role, -is_id_pattern, -has_comment, c["column_name"])
        return sorted(cols, key=_sort_key)

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------

    def create_tables(self) -> None:
        """Create ``semantic_layer_questions`` and ``metric_view_definitions`` tables if they don't exist."""
        fq = self.config.fq
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq(self.config.questions_table)} (
                question_id STRING NOT NULL,
                question_text STRING,
                status STRING,
                created_at TIMESTAMP,
                processed_at TIMESTAMP
            ) COMMENT 'Business questions for semantic layer generation'
        """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq(self.config.definitions_table)} (
                definition_id STRING NOT NULL,
                metric_view_name STRING,
                source_table STRING,
                json_definition STRING,
                source_questions STRING,
                status STRING,
                validation_errors STRING,
                genie_space_id STRING,
                created_at TIMESTAMP,
                applied_at TIMESTAMP,
                deployed_catalog STRING,
                deployed_schema STRING
            ) COMMENT 'Generated metric view definitions'
        """
        )
        logger.info("Semantic layer tables ready")

    # ------------------------------------------------------------------
    # Question ingestion
    # ------------------------------------------------------------------

    def ingest_questions(self, questions: List[str]) -> int:
        """Insert business questions as ``pending`` rows. Returns number ingested."""
        fq_table = self.config.fq(self.config.questions_table)
        now = datetime.utcnow().isoformat()
        rows = []
        for q in questions:
            q = q.strip()
            if not q:
                continue
            rows.append(
                f"('{uuid.uuid4()}', '{q.replace(chr(39), chr(39)*2)}', 'pending', '{now}', NULL)"
            )
        if not rows:
            return 0
        values = ", ".join(rows)
        # INSERT: Bulk-append pending semantic-layer questions into the configured questions Delta table with freshly minted
        # question_id (UUID) PKs; fills question_text (escaped single-quote duplication),
        # status='pending', created_at (UTC ISO); processed_at left NULL until generation completes.
        # WHY: Seeds the semantic-layer pipeline queue so downstream LLM generation can pick only unanswered rows.
        # TRADEOFFS: Bulk INSERT VALUES is fast/low JDBC chatter versus iterative executesSQL—but escapes rely on
        # deterministic sanitisation alone (risk vs parameterized ROW constructors/MERGE); no UPSERT so callers must dedupe upstream question texts manually if uniqueness matters.
        self.spark.sql(f"INSERT INTO {fq_table} VALUES {values}")
        logger.info("Ingested %d questions", len(rows))
        return len(rows)

    # ------------------------------------------------------------------
    # Context building
    # ------------------------------------------------------------------

    def build_context(self) -> str:
        """Assemble metadata context from knowledge bases and optional upstream tables."""
        fq = self.config.fq
        parts: list[str] = []

        # Table knowledge base (required)
        tables = self._safe_collect(
            f"SELECT table_name, comment, domain, subdomain FROM {fq('table_knowledge_base')}"
        )
        if not tables:
            raise RuntimeError(
                "table_knowledge_base is empty or missing -- run metadata generation first"
            )

        # Column knowledge base (required) -- paginated by table batch to avoid OOM
        _CKB_TABLE_BATCH = 500
        table_names_list = [t["table_name"] for t in tables]
        col_by_table: dict[str, list] = {}
        for i in range(0, len(table_names_list), _CKB_TABLE_BATCH):
            batch_names = table_names_list[i : i + _CKB_TABLE_BATCH]
            placeholders = ", ".join(f"'{n}'" for n in batch_names)
            batch_cols = self._safe_collect(
                f"SELECT table_name, column_name, data_type, comment, classification "
                f"FROM {fq('column_knowledge_base')} WHERE table_name IN ({placeholders})"
            )
            for c in batch_cols:
                col_by_table.setdefault(c["table_name"], []).append(c)

        # FK predictions (optional; stash on self for source validation reuse)
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} "
            f"WHERE final_confidence >= {self.config.fk_confidence_threshold}"
            f" AND (is_fk IS NULL OR is_fk = TRUE)"
        )
        self._fk_rows = fk_rows

        # Ontology: primary entities per table
        ont_rows = self._safe_collect(
            f"SELECT entity_type, source_tables FROM {fq('ontology_entities')} "
            f"WHERE COALESCE(entity_role, 'primary') = 'primary' AND confidence >= 0.4"
        )
        entity_map: dict[str, str] = {}
        for o in ont_rows:
            for t in o.get("source_tables") or []:
                entity_map[t] = o["entity_type"]

        # Column properties for role-based hints
        cp_rows = self._safe_collect(
            f"SELECT table_name, column_name, property_role, linked_entity_type "
            f"FROM {fq('ontology_column_properties')}"
        )
        col_props_by_table: dict[str, dict[str, dict]] = {}
        for cp in cp_rows:
            col_props_by_table.setdefault(cp["table_name"], {})[cp["column_name"]] = {
                "role": cp.get("property_role", "attribute"),
                "linked": cp.get("linked_entity_type"),
            }

        # Named relationships for join hints
        rel_rows = self._safe_collect(
            f"SELECT src_entity_type, relationship_name, dst_entity_type, cardinality "
            f"FROM {fq('ontology_relationships')} WHERE confidence >= 0.4"
        )
        rel_by_entity: dict[str, list[str]] = {}
        for r in rel_rows:
            src, dst, rn = r.get("src_entity_type", ""), r.get("dst_entity_type", ""), r.get("relationship_name", "")
            if src and dst and rn:
                card = r.get("cardinality", "")
                rel_by_entity.setdefault(src, []).append(f"{rn} -> {dst} ({card})" if card else f"{rn} -> {dst}")

        # Entity-specific measure suggestions (dynamic based on column metadata)
        _TEMPORAL_KW = {"date", "timestamp", "created", "updated", "modified"}
        _TEMPORAL_SUFFIXES = {"_at", "_date", "_time", "_ts", "_dt", "_time_ms"}

        def _is_temporal_col(col_name: str) -> bool:
            lc = col_name.lower()
            return lc in _TEMPORAL_KW or any(lc.endswith(s) for s in _TEMPORAL_SUFFIXES)

        def _entity_suggestion(ent_type: str, ent_tables: list[str]) -> str:
            has_temporal = any(
                any(
                    _is_temporal_col(c["column_name"]) or
                    (c.get("data_type") or "").upper() in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ")
                    for c in col_by_table.get(t, [])
                )
                for t in ent_tables
            )
            has_status = any(
                any("status" in c["column_name"].lower() or "state" in c["column_name"].lower() for c in col_by_table.get(t, []))
                for t in ent_tables
            )
            has_amount = any(
                any(kw in c["column_name"].lower() for kw in ("amount", "price", "cost", "revenue", "total") for c in col_by_table.get(t, []))
                for t in ent_tables
            )
            parts_s = ["Consider: counts, key ratios, and segmentation dimensions"]
            if has_temporal:
                parts_s.append("time-based rates and period-over-period comparisons")
            if has_status:
                parts_s.append("status-based rates (e.g. fulfillment, completion, conversion)")
            if has_amount:
                parts_s.append("revenue/cost aggregates, per-entity averages, value distribution")
            parts_s.append("add joins when breakdowns by related tables are needed")
            return "; ".join(parts_s) + "."

        unique_entities = sorted(set(entity_map.values()))
        entity_tables: dict[str, list[str]] = {}
        for tname, etype in entity_map.items():
            entity_tables.setdefault(etype, []).append(tname)
        if unique_entities:
            parts.append("\nENTITY MEASURE SUGGESTIONS (use to steer measures per entity type):")
            for ent in unique_entities:
                suggestion = _entity_suggestion(ent, entity_tables.get(ent, []))
                rels_str = ", ".join(rel_by_entity.get(ent, []))
                extra = f" Relationships: {rels_str}." if rels_str else ""
                parts.append(f"  {ent}: {suggestion}{extra}")

        # Assemble per-table context (with column cap + priority sort)
        cap = self.config.max_context_cols_per_table
        for t in tables:
            tname = t["table_name"]
            ent = entity_map.get(tname, "")
            ent_str = f" Entity: {ent}" if ent else ""
            line = f"Table: {tname} (Comment: \"{t.get('comment', '')}\" Domain: {t.get('domain', '')} / {t.get('subdomain', '')}){ent_str}"
            cols = col_by_table.get(tname, [])
            props = col_props_by_table.get(tname, {})
            if len(cols) > cap:
                cols = self._prioritize_columns(cols, props)[:cap]
                omitted = len(col_by_table.get(tname, [])) - cap
            else:
                omitted = 0
            col_strs = []
            for c in cols:
                cname = c["column_name"]
                cp = props.get(cname)
                role_str = ""
                if cp:
                    role_str = f" [{cp['role']}]"
                    if cp.get("linked"):
                        role_str = f" [link -> {cp['linked']}]"
                display = f"`{cname}`" if " " in cname else cname
                col_strs.append(
                    f"  - {display} {c.get('data_type', '')}{role_str} : {c.get('comment', '')}"
                )
            if omitted > 0:
                col_strs.append(f"  ... ({omitted} additional columns not shown)")
            parts.append(
                line + "\n  Columns:\n" + "\n".join(col_strs) if col_strs else line
            )

        # FK relationships and recommended joins (highly visible for model)
        if fk_rows:
            parts.append("\nFOREIGN KEY RELATIONSHIPS:")
            for fk in fk_rows:
                sc = fk['src_column']
                dc = fk['dst_column']
                sc_d = f"`{sc}`" if " " in sc else sc
                dc_d = f"`{dc}`" if " " in dc else dc
                parts.append(
                    f"  {fk['src_table']}.{sc_d} -> {fk['dst_table']}.{dc_d} (confidence {fk['final_confidence']:.2f})"
                )
            parts.append(
                "\nRECOMMENDED JOINS (use these when building metric views; fact tables like encounters/orders should join to dimension tables for breakdowns):"
            )
            for fk in fk_rows:
                sc_d = f"`{fk['src_column']}`" if " " in fk['src_column'] else fk['src_column']
                dc_d = f"`{fk['dst_column']}`" if " " in fk['dst_column'] else fk['dst_column']
                parts.append(
                    f"  {fk['src_table']} + {fk['dst_table']}: src_column={sc_d}, dst_column={dc_d}"
                )

        # Graph-traversed join paths (multi-hop, nested snowflake joins)
        if fk_rows and self.config.max_join_hops > 0:
            path_sections: list[str] = []
            for tname in table_names_list:
                paths = self._discover_join_paths(tname)
                if paths:
                    def _render(joins: list[dict], indent: int = 4) -> list[str]:
                        lines: list[str] = []
                        for j in joins:
                            lines.append(" " * indent + f"JOIN {j['source']} AS {j['name']} ON {j['on']}")
                            if j.get("joins"):
                                lines.extend(_render(j["joins"], indent + 4))
                        return lines
                    path_sections.append(f"  FROM {tname}:")
                    path_sections.extend(_render(paths))
            if path_sections:
                parts.append(
                    "\nJOIN PATHS (graph-traversed, use nested joins for multi-hop):"
                )
                parts.extend(path_sections)

        # Name-matched column hints (safety net for FK gaps)
        _GENERIC_COLS = {
            "id", "name", "type", "status", "code", "description",
            "created_at", "updated_at", "created_by", "updated_by",
            "modified_at", "modified_by", "is_active", "is_deleted", "version",
        }
        fk_pairs = {(fk["src_table"], fk["dst_table"]) for fk in fk_rows} | {
            (fk["dst_table"], fk["src_table"]) for fk in fk_rows
        } if fk_rows else set()
        id_cols: dict[str, list[tuple[str, str]]] = {}  # col_short -> [(table, col_fqn)]
        for tname, cols in col_by_table.items():
            for c in cols:
                cn = c["column_name"].lower()
                if cn in _GENERIC_COLS:
                    continue
                if cn.endswith(("_id", "_key", "_code")):
                    id_cols.setdefault(cn, []).append((tname, cn))
        import re as _re
        possible_hints: list[str] = []
        table_shorts = {t.split(".")[-1].lower(): t for t in table_names_list}
        for col_short, locations in id_cols.items():
            stem = _re.sub(r"(_id|_key|_code)$", "", col_short)
            if not stem or stem in _GENERIC_COLS:
                continue
            target_names = [stem, stem + "s", stem + "es"]
            for tgt_short in target_names:
                if tgt_short in table_shorts:
                    target_fq = table_shorts[tgt_short]
                    for src_tbl, _ in locations:
                        if src_tbl == target_fq:
                            continue
                        if (src_tbl, target_fq) in fk_pairs:
                            continue
                        possible_hints.append(
                            f"  {src_tbl}.{col_short} -> {target_fq}.{col_short} (name match)"
                        )
        if possible_hints:
            parts.append(
                "\nPOSSIBLE JOINS (name-matched, not confirmed by FK prediction -- use when questions need these tables):"
            )
            parts.extend(possible_hints)

        # Existing metric views -- avoid duplicates
        existing_mvs = self._safe_collect(
            f"SELECT metric_view_name, source_table, json_definition "
            f"FROM {fq(self.config.definitions_table)} "
            f"WHERE status IN ('validated', 'applied') AND metric_view_name IS NOT NULL"
        )
        if existing_mvs:
            parts.append("\nEXISTING METRIC VIEWS (do NOT duplicate -- create complementary views):")
            for emv in existing_mvs:
                comment = ""
                try:
                    edefn = json.loads(emv["json_definition"]) if isinstance(emv["json_definition"], str) else emv["json_definition"]
                    comment = edefn.get("comment", "")
                except Exception:
                    pass
                parts.append(f"  - {emv['metric_view_name']} (source: {emv.get('source_table', '?')}){': ' + comment if comment else ''}")

        # Inject metric view best-practices reference (loaded from JSON)
        ref = _load_reference("metric_view_reference.json")
        if ref:
            ref_text = _format_reference_section(ref, ["yaml_syntax_rules", "measure_patterns", "join_templates", "anti_patterns", "validation_checklist"])
            if ref_text:
                parts.append("\nREFERENCE: METRIC VIEW BEST PRACTICES (follow these rules strictly)")
                parts.append(ref_text)

        # Schema profile: adaptive signal so LLM calibrates output complexity
        sp = profile_schema(table_names_list, fk_rows)
        parts.append(sp["profile_text"])

        return "\n".join(parts)

    def _safe_collect(self, sql: str) -> list[dict]:
        """Run SQL and return list of dicts; returns [] if table/column doesn't exist."""
        try:
            return [row.asDict() for row in self.spark.sql(sql).collect()]
        except Exception as e:
            err = str(e)
            if any(k in err for k in ("TABLE_OR_VIEW_NOT_FOUND", "UNRESOLVED_COLUMN")):
                logger.info("Skipping unavailable table/column: %s", e)
                return []
            if type(e).__name__ == "AnalysisException":
                logger.info("Skipping SQL analysis error: %s", e)
                return []
            raise

    # ------------------------------------------------------------------
    # AI generation
    # ------------------------------------------------------------------

    def generate_metric_views(self) -> Dict[str, Any]:
        """Generate metric view YAML definitions from pending business questions.

        Reads pending questions, gathers catalog context (KB comments, FK
        predictions, ontology entities, profiling stats), calls AI_QUERY in
        single- or two-phase mode, parses JSON output, enriches with FK-derived
        join columns, optionally dry-runs ``CREATE VIEW`` for validation, and
        persists rows to ``metric_view_definitions``. Returns summary dict with
        counts of generated, validated, and failed views.
        """
        fq = self.config.fq

        # Read pending questions
        q_rows = self.spark.sql(
            f"SELECT question_id, question_text FROM {fq(self.config.questions_table)} WHERE status = 'pending'"
        ).collect()
        if not q_rows:
            logger.warning("No pending questions found")
            return {"generated": 0, "validated": 0, "failed": 0}

        questions = [r["question_text"] for r in q_rows]
        q_ids = [r["question_id"] for r in q_rows]
        logger.info("Processing %d pending questions", len(questions))

        with _trace_span("semantic_layer.generate_metric_views") as root_span:
            with _trace_span("build_context") as ctx_span:
                context = self.build_context()
                if ctx_span is not None:
                    ctx_span.set_outputs({"context_length": len(context)})

            definitions = []
            if self.config.use_two_phase:
                plan_prompt = self._build_plan_prompt(questions, context)
                plan_response = ""
                try:
                    plan_response = self.spark.sql(
                        "SELECT AI_QUERY(:model, :prompt) as response",
                        args={"model": self.config.model_endpoint, "prompt": plan_prompt},
                    ).collect()[0]["response"]
                except Exception as e:
                    logger.warning("Plan phase failed, falling back to single-phase: %s", e)
                plan_views = self._parse_plan_response(plan_response) if plan_response else []
                if plan_views:
                    for idx, plan_view in enumerate(plan_views):
                        gen_prompt = self._build_generate_prompt_for_plan(plan_view, questions, context)
                        try:
                            gen_response = self.spark.sql(
                                "SELECT AI_QUERY(:model, :prompt) as response",
                                args={"model": self.config.model_endpoint, "prompt": gen_prompt},
                            ).collect()[0]["response"]
                            one = self._parse_single_definition(gen_response)
                            if one and one.get("dimensions") and one.get("measures"):
                                # Ensure joins from plan if missing
                                if plan_view.get("joins") and not one.get("joins"):
                                    one["joins"] = plan_view["joins"]
                                definitions.append(one)
                            else:
                                logger.warning("Phase 2 for view %s returned invalid definition", plan_view.get("name"))
                        except Exception as e:
                            logger.warning("Phase 2 for view %s failed: %s", plan_view.get("name"), e)
                else:
                    logger.info("Plan returned no views, falling back to single-phase")
            if not definitions:
                prompt = self._build_prompt(questions, context)
                last_response = ""
                max_retries = 3
                with _trace_span("ai_generate_definitions") as ai_span:
                    if ai_span is not None:
                        ai_span.set_inputs({
                            "prompt_truncated": prompt[:2000] + "..." if len(prompt) > 2000 else prompt,
                            "model": self.config.model_endpoint,
                            "num_questions": len(questions),
                        })
                    for attempt in range(max_retries):
                        try:
                            last_response = self.spark.sql(
                                "SELECT AI_QUERY(:model, :prompt) as response",
                                args={"model": self.config.model_endpoint, "prompt": prompt},
                            ).collect()[0]["response"]
                            definitions = self._parse_ai_response(last_response)
                            if definitions:
                                break
                            logger.warning(
                                "Attempt %d: AI returned 0 definitions, retrying", attempt + 1
                            )
                        except Exception as e:
                            logger.warning("Attempt %d failed: %s", attempt + 1, e)
                            if attempt == max_retries - 1:
                                logger.error("All %d AI_QUERY attempts failed", max_retries)
                    if ai_span is not None:
                        ai_span.set_outputs({
                            "response_truncated": (last_response[:1500] + "...") if len(last_response) > 1500 else last_response or "(empty)",
                            "num_definitions": len(definitions),
                        })

            logger.info("AI returned %d metric view definitions", len(definitions))

            # Validate and store each independently
            now = datetime.utcnow().isoformat()
            q_id_str = ",".join(q_ids)
            stats = {"generated": 0, "validated": 0, "failed": 0}

            for defn in definitions:
                # Auto-enrich joins from FK predictions
                self._enrich_joins_from_fk(defn)
                _infer_format_specs(defn)
                _fix_percentage_scaling(defn)
                _backfill_agent_metadata(defn)
                _strip_kpi_references(defn)
                _drop_broken_measures(defn)
                _drop_placeholder_dimensions(defn)
                defn = self._restructure_chained_to_nested(defn)
                defn = self._qualify_nested_refs(defn)

                defn_id = str(uuid.uuid4())
                mv_name = defn.get("name", f"metric_view_{defn_id[:8]}")
                source = defn.get("source", "")

                with _trace_span("validate_definition") as val_span:
                    errors = self._validate_definition(defn)
                    if self.config.validate_expressions and not errors:
                        errors = self._validate_expressions(defn)
                    if val_span is not None:
                        val_span.set_inputs({"metric_view_name": mv_name, "source": source})
                        val_span.set_outputs({"outcome": "validated" if not errors else "failed", "validation_errors": errors or None})

                # Source validation: warn on dim+fact pattern, recover if failed
                fk_data = getattr(self, "_fk_rows", []) or []
                src_warning = check_dim_source_pattern(defn, fk_data)
                if src_warning:
                    logger.warning("Source pattern warning for '%s': %s", mv_name, src_warning["message"])
                    if errors:
                        suspected_fact = src_warning["suspected_fact"]
                        # Tier 1: try swapping source and join
                        swapped = _swap_source_and_join(defn, suspected_fact)
                        swap_errors = self._validate_definition(swapped)
                        if not swap_errors and self.config.validate_expressions:
                            swap_errors = self._validate_expressions(swapped)
                        if not swap_errors:
                            logger.info("Tier 1 recovery (swap) succeeded for '%s'", mv_name)
                            defn = swapped
                            source = defn.get("source", source)
                            errors = []
                        else:
                            # Tier 2: strip fact joins, keep dim-only
                            import copy
                            dim_only = copy.deepcopy(defn)
                            dim_only["joins"] = [
                                j for j in dim_only.get("joins", [])
                                if j.get("source", "").split(".")[-1].lower() != suspected_fact.lower()
                            ]
                            dim_errors = self._validate_definition(dim_only)
                            if not dim_errors and self.config.validate_expressions:
                                dim_errors = self._validate_expressions(dim_only)
                            if not dim_errors:
                                logger.info("Tier 2 recovery (dim-only) succeeded for '%s'", mv_name)
                                defn = dim_only
                                errors = []

                if self.config.materialize_metric_views:
                    defn["materialization"] = build_materialization(defn, self.config.materialization_schedule)
                    errors = (errors or []) + validate_materialization(defn)

                if not errors and self.config.validate_before_store:
                    dry_run_name = f"{mv_name}_dry_run"
                    fq_dry = f"{self.config.catalog_name}.{self.config.schema_name}.{dry_run_name}"
                    try:
                        yaml_body = self._definition_to_yaml(
                            defn, include_materialization=self.config.materialize_metric_views
                        )
                        self.spark.sql(
                            f"CREATE OR REPLACE VIEW {fq_dry}\nWITH METRICS LANGUAGE YAML AS $$\n{yaml_body}$$"
                        )
                        self.spark.sql(f"DROP VIEW IF EXISTS {fq_dry}")
                    except Exception as e:
                        errors = (errors or []) + [f"dry-run CREATE failed: {e}"]

                status = "validated" if not errors else "failed"
                error_str = "; ".join(errors).replace("'", "''") if errors else ""
                json_str = json.dumps(defn).replace("'", "''")

                self.spark.sql(
                    f"""
                    INSERT INTO {fq(self.config.definitions_table)} VALUES (
                        '{defn_id}', '{mv_name}', '{source}', '{json_str}',
                        '{q_id_str}', '{status}', '{error_str}', NULL,
                        '{now}', NULL, NULL, NULL
                    )
                """
                )
                stats[status] += 1
                stats["generated"] += 1

            # Flag duplicate-source views (same grain generated more than once)
            source_seen: dict[str, list[str]] = {}
            for d in definitions:
                src = d.get("source", "")
                name = d.get("name", "")
                if src:
                    source_seen.setdefault(src, []).append(name)
            for src, names in source_seen.items():
                if len(names) > 1:
                    logger.warning("Duplicate source grain '%s' across views: %s", src, names)

            if stats["generated"] > 0 and stats["validated"] == 0:
                logger.warning("All %d generated metric views failed validation", stats["generated"])

            # Mark questions: processed if at least one definition stored, failed if all definitions failed validation
            id_list = ", ".join(f"'{qid}'" for qid in q_ids)
            if stats["generated"] > 0:
                status_val = "processed" if stats["validated"] > 0 else "failed"
                # UPDATE: Rows in the configured questions table keyed by question_id IN the current batch IDs; sets status
                # to 'processed' when any definition validated, else 'failed', and stamps processed_at to now.
                # WHY: Advances the question queue so retries skip already-handled items while recording failure
                # when nothing passed validation.
                # TRADEOFFS: Predicate is IN-list (efficient for modest batch sizes) versus temp-table join for
                # huge batches; overwrites prior status without versioning (simpler lifecycle but no history).
                self.spark.sql(
                    f"""
                    UPDATE {fq(self.config.questions_table)}
                    SET status = '{status_val}', processed_at = current_timestamp()
                    WHERE question_id IN ({id_list})
                """
                )
                if stats["validated"] == 0:
                    logger.warning("No definitions passed validation; questions marked failed")
            else:
                logger.warning(
                    "No definitions generated; questions remain pending for retry"
                )

            if root_span is not None:
                root_span.set_outputs(stats)

        logger.info("Generation complete: %s", stats)
        return stats

    def _build_prompt(self, questions: List[str], context: str) -> str:
        q_block = "\n".join(f"  {i+1}. {q}" for i, q in enumerate(questions))
        few_shot = _select_few_shot(context)
        return f"""You are a data modeler building a semantic layer for Databricks Unity Catalog.

TASK: Generate metric view definitions (as a JSON array) that enable answering the business questions below.

ORGANIZING PRINCIPLE -- one comprehensive view per fact-table grain:
- Each metric view declares its grain via its source table (one row = one prescription, one order line, one encounter, etc.)
- All measures must be valid at that grain. NEVER mix measures implying different grains
- Prefer ONE broad view per grain with all relevant measures and dimensions, rather than splitting by analytical theme. The consumer (Genie, agent, SQL) selects which dimensions/measures to use per query
- Split into multiple views from the same source ONLY when the persistent filter, join path, or grain changes -- NOT because of different "themes"
- Use nested joins to maximize dimension reach through FK chains (e.g. line_items -> orders -> customers -> regions)
- When no FK relationships or join paths are available, create simple single-table metric views with direct aggregations. Do NOT fabricate joins. Dimension-only views (sourced from a dimension table with no joins) are valid for entity-level analytics (e.g. customer counts by region)

RULES:
1. Create measures that directly support answering the business questions. Do NOT add a generic "row count" or "Record Count" measure unless a question explicitly asks for "how many records" or "count of X". Prefer ratios (e.g. rate, per capita), conditional aggregates (FILTER), and entity-specific KPIs (e.g. readmission rate, avg length of stay) over raw COUNT(*) when the question implies a more specific metric.
2. Prefer one comprehensive view per fact-table grain with all relevant measures. Multiple views from the same source table are acceptable ONLY when filters, join paths, or grains differ
3. Only reference columns that exist in the metadata below (Columns in the metadata). Do not invent column names
4. Use standard SQL aggregate functions: SUM, COUNT, AVG, MIN, MAX, COUNT(DISTINCT ...). NEVER use SQL window functions (OVER, PARTITION BY, ROW_NUMBER, LAG, LEAD) in measure expressions -- they are not supported in metric views. For rolling/trailing calculations, use the "window" property on the measure instead
5. Date/time function rules (Databricks/Spark SQL):
   a. DATE_TRUNC: quote the interval -- DATE_TRUNC('MONTH', col). NEVER use bare DATE_TRUNC(MONTH, col)
   b. EXTRACT: use EXTRACT(HOUR FROM col) for extracting date parts. Do NOT use DATE_PART(HOUR, col)
   c. DATEDIFF only returns days with 2 args: DATEDIFF(end, start). For other units use TIMESTAMPDIFF(MINUTE, start, end) with a bare unquoted keyword unit
   d. TIMESTAMPADD: bare unquoted singular unit -- TIMESTAMPADD(MONTH, 1, col), NOT TIMESTAMPADD('MONTHS', 1, col)
   e. For year-month dimensions, use DATE_FORMAT(col, 'yyyy-MM'). NEVER use SUBSTR(col, 1, 7) on date columns
6. For NULL checks, always use IS NULL / IS NOT NULL. NEVER use = NULL, <> NULL, or != NULL (these evaluate to NULL in SQL, not boolean)
7. ALWAYS single-quote ALL string literal values everywhere in expressions:
   - Comparisons: status = 'fulfilled', NOT status = fulfilled
   - THEN/ELSE results: CASE WHEN x = 'A' THEN 'Category A' ELSE 'Other' END, NOT THEN Category A
   - IN lists: department IN ('Surgery', 'Pediatrics'), NOT IN (Surgery, Pediatrics)
   - CONCAT separators: CONCAT(hospital, ' - ', department), NOT CONCAT(hospital, - , department)
   - CASE results with parens/hyphens: THEN '0-15 min (Excellent)', NOT THEN 0-15 min (Excellent)
   The ONLY unquoted tokens should be column names, SQL keywords, and numbers
   WRONG: status = fulfilled, region IN (North, South). CORRECT: status = 'fulfilled', region IN ('North', 'South')
   IDENTIFIERS WITH SPACES: If a column name contains spaces, wrap it in backticks: source.`assay name`, NOT source."assay name". Double quotes are NOT valid for identifiers in Databricks SQL
8. Join format (Unity Catalog):
   STAR SCHEMA (default): name: <alias>, source: catalog.schema.table, on: source.<fk> = <alias>.<pk>. The root table is always "source".
   NESTED / SNOWFLAKE JOINS: for dimension hierarchies (e.g. customer -> nation -> region), nest child joins inside the parent's "joins" array. Child "on" references the PARENT alias, not "source":
   {{"name": "customer", "source": "...", "on": "source.customer_id = customer.id", "joins": [{{"name": "nation", "source": "...", "on": "customer.nation_id = nation.id"}}]}}
   Limit nesting to 2 levels. Use nested joins when JOIN PATHS in the metadata show multi-hop FK chains.
   NESTED ALIAS REACHABILITY: all nested join aliases are fully reachable in expressions. If you nest physician -> account -> territory, use "account.bed_count" and "territory.region" directly. NEVER substitute a placeholder column from an unrelated table/alias. If a column is unreachable through joins, DROP the dimension entirely rather than faking it.
9. Include joins when RECOMMENDED JOINS exist; when questions ask for breakdowns by attributes in another table (e.g. by customer segment, department), you MUST add a join. Prefer at least one metric view with joins when FKs exist
10. Every metric view MUST have at least one measure and one dimension
11. Add a top-level "comment" (1-2 sentences) describing what the metric view measures, its analytical purpose, and which source tables it draws from. Do NOT reference question numbers, KPI numbers, or list which questions are/aren't answerable. Focus on content and lineage (e.g. "Analyzes order revenue by product family and sales representative, joining line items to the product catalog and parent order for discount tracking.")
12. Every dimension and measure MUST have: "comment" (what it represents), "display_name" (human-readable label, max 255 chars), and "synonyms" (array of 2-5 alternative names for Genie discoverability, e.g. ["revenue", "total sales"] for Total Revenue). Every measure MUST have a "format" object: {{"type": "currency"}} for monetary values, {{"type": "percentage"}} for rates/ratios that return a 0-to-1 FRACTION (e.g. 0.167 for 16.7%), or {{"type": "number"}} for counts/averages/scores. CRITICAL: percentage-format expressions must NOT multiply by 100 -- the rendering layer does that automatically. Write `SUM(won)/NULLIF(COUNT(*),0)` (returns 0.167), NOT `100.0 * SUM(won)/NULLIF(COUNT(*),0)` (returns 16.7). Also do NOT wrap percentage expressions in ROUND(); the format handles decimal precision.
13. Use "filter" (optional) for persistent WHERE clauses (e.g. excluding null/test rows)
14. Use measure-level FILTER for conditional aggregation: SUM(col) FILTER (WHERE condition)
15. If some questions are not answerable with metrics (e.g. document search, free-text lookups, SOP retrieval), generate metric views for the ones that ARE quantitative/analytical and silently ignore the rest. Do NOT mention skipped or unanswerable questions in the comment field
16. Each metric view "name" must be unique and descriptive, reflecting the grain (e.g. prescription_metrics, order_line_metrics, encounter_metrics)
16. Output ONLY a valid JSON array, no explanation
17. Use domain and subdomain from table metadata to choose which dimensions (e.g. department, region, product category) are relevant to the questions.
18. When Entity types are annotated on tables, use the ENTITY MEASURE SUGGESTIONS in the metadata and generate entity-specific analytical metrics:
    - People/patients/users: include counts, return/readmission rates, segmentation dimensions, and per-entity averages
    - Transactions/events/encounters: include volume counts, value sums, time-based rates, and categorical breakdowns
    - Resources/staff/inventory: include utilization rates (active/total), efficiency ratios, and capacity measures
    Always include at least one RATIO measure (x / NULLIF(y, 0)) and one RATE measure (conditional_count * 1.0 / NULLIF(total, 0)) per metric view
19. When RECOMMENDED JOINS / FOREIGN KEY RELATIONSHIPS exist, generate cross-table metrics that join fact tables to dimension tables. Use dimension table columns as grouping dimensions and fact table columns as measures
20. Dimension and measure names should be colloquial and business-friendly:
    - For simple column references, use the column's natural name (e.g. "Industry" not "Account Industry", "Status" not "Order Status")
    - Only add a qualifier when two dimensions would otherwise be ambiguous (e.g. "Billing State" vs "Shipping State")
    - For date truncations, use "{{Column}} Month" or "{{Column}} Quarter" style (e.g. "Order Month")
    - For measures, use what a business user would say: "Total Sales", "Avg Order Value", "Fulfillment Rate"
    - NEVER prefix dimension names with the join alias (use "Industry" not "account.Industry")
    - LIKE patterns MUST be quoted: product_code LIKE 'HW%', NOT product_code LIKE HW%
21. GRAIN INTEGRITY with joins: When joining a fact table to a dimension table, only use dimension columns as GROUP BY dimensions or in FILTER clauses. NEVER aggregate a dimension-table numeric attribute (e.g. SUM(dim.bed_count), AVG(dim.capacity)) from a fact-grain view -- the value fans out by the number of fact rows per dimension row, producing inflated results. If you need to analyze dimension attributes directly, create a SEPARATE dimension-only metric view with NO fact-table joins
    STAR SCHEMA SOURCE RULE: When joins are present, the source MUST be the fact table (the table at the grain of the analysis, typically the one with the most rows and multiple foreign keys to dimension tables). The join relationship from source to join should be many-to-one. If you need metrics about a dimension entity itself (e.g. count of customers by region) with no fact-table aggregation, source from the dimension with NO fact-table joins -- this is valid. NEVER source from a dimension table and join to a fact table -- this fans out rows and produces incorrect aggregates
    FACT-TO-FACT JOIN PROHIBITION: Do NOT join from a fact source to another fact table (tables prefixed with fact_, fct_, f_ or those with high row counts and their own aggregatable measures). Fact-to-fact joins create one-to-many fan-out that inflates ALL aggregates. If you need columns from another fact table, create a SEPARATE metric view sourced from that table instead.
    JOIN USAGE REQUIREMENT: Every join you include MUST have at least one dimension or measure expression that references a column from it (via its alias). Do NOT include joins "for completeness" or "in case they are needed." Unused joins waste query resources and risk fan-out inflation.
22. NEVER create share-of-total or percent-of-total measures (e.g. SUM(x)/SUM(total_x), COUNT(*)/COUNT(*)). These require window functions (OVER()) for the denominator, which are not supported. The denominator collapses to the same group as the numerator, always producing 1.0. Instead use: conditional ratios with FILTER, within-grain rates, or describe the share concept in the view comment for downstream Genie SQL
23. NEVER nest aggregate functions inside other aggregate functions (e.g. SUM(COUNT(*)), AVG(SUM(x))). Databricks SQL does not allow nested aggregates. If you need a two-stage aggregation, use a conditional aggregate with CASE/WHEN or create a separate metric view for the inner aggregation
24. If EXISTING METRIC VIEWS are listed in the metadata, do NOT recreate views that serve the same analytical purpose (as described in their comment) or use the same source table with overlapping measures. Instead create complementary views that cover genuinely different grains, join paths, or business questions not already addressed by existing views
25. Do NOT create duplicate measures with identical expressions but different names. Each measure must have a semantically distinct expr. "Revenue per Physician" as SUM(cost) is just a duplicate of "Total Revenue" -- the grouping is a query-time choice, not a measure definition property

EXAMPLE:
{few_shot}

CATALOG METADATA:
{context}

BUSINESS QUESTIONS:
{q_block}

OUTPUT (JSON array only):"""

    def _build_plan_prompt(self, questions: List[str], context: str) -> str:
        """Build prompt for phase 1: plan views (no SQL expressions)."""
        q_block = "\n".join(f"  {i+1}. {q}" for i, q in enumerate(questions))
        return f"""You are a data modeler planning a semantic layer for Databricks Unity Catalog.

TASK: Output a PLAN only (no SQL). Reply with a single JSON object: {{ "views": [ ... ] }}.

ORGANIZING PRINCIPLE -- one comprehensive view per fact-table grain:
- Each metric view declares its grain via its source table (one row = one prescription, one order, etc.)
- Prefer ONE broad view per grain with all relevant measures/dimensions, rather than splitting by theme
- Split into multiple views from the same source ONLY when the filter, join path, or grain differs
- Use nested joins to maximize dimension reach through FK chains
- When no FK relationships or join paths are available, create simple single-table metric views with direct aggregations. Do NOT fabricate joins. Dimension-only views (sourced from a dimension table with no joins) are valid for entity-level analytics (e.g. customer counts by region)

For each metric view in "views", include:
- "name": unique snake_case name reflecting the grain (e.g. prescription_metrics, order_line_metrics)
- "source": fully qualified source table (catalog.schema.table)
- "comment": one sentence describing what the view measures and its source lineage. Do NOT reference question numbers or KPI numbers
- "joins": array of {{ "name": "<alias>", "source": "catalog.schema.table", "on": "source.<fk_col> = <alias>.<pk_col>" }}
  Include joins supported by high-confidence FK relationships to maximize dimension reach. Use nested joins for dimension hierarchies (e.g. orders -> customers -> regions):
  {{ "name": "customer", ..., "joins": [{{ "name": "nation", ..., "on": "customer.nation_id = nation.id" }}] }}
  Do NOT join the same physical table via multiple paths unless each join serves a genuinely different FK role (e.g. ship_to_address vs bill_to_address). If the source table already has a direct FK to a dimension, do NOT also reach that dimension through a nested join chain.
- "dimensions": array of {{ "name": "Display Name", "comment": "what it is" }} (no expr)
- "measures": array of {{ "name": "Display Name", "comment": "what it measures" }} (no expr)
- "question_indices": array of 0-based question indices this view answers

STAR SCHEMA SOURCE RULE: When joins are present, the source MUST be the fact table (the table at the grain of the analysis, typically the one with the most rows and multiple foreign keys to dimension tables). The join relationship from source to join should be many-to-one. If you need metrics about a dimension entity itself with no fact-table aggregation, source from the dimension with NO fact-table joins. NEVER source from a dimension table and join to a fact table -- this fans out rows and produces incorrect aggregates.
FACT-TO-FACT JOIN PROHIBITION: Do NOT join from a fact source to another fact table (tables prefixed with fact_, fct_, f_ or those with high row counts). Fact-to-fact joins create one-to-many fan-out that inflates ALL aggregates. If you need columns from another fact table, create a SEPARATE metric view sourced from that table.
JOIN USAGE REQUIREMENT: Only include joins whose columns you intend to use in dimensions or measures. Do NOT include joins "for completeness."

Create measures that match the business questions (ratios, rates, KPIs); avoid generic row count unless a question explicitly asks for it. Each view must have at least one dimension and one measure. Cross-table breakdowns using joined dimension tables are strongly preferred.

CATALOG METADATA:
{context}

BUSINESS QUESTIONS:
{q_block}

OUTPUT (single JSON object with "views" key only, no explanation):"""

    def _parse_plan_response(self, response: str) -> List[dict]:
        """Extract plan views from phase 1 response."""
        text = response.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end <= start:
            return []
        try:
            data = json.loads(text[start:end])
            return data.get("views") or []
        except json.JSONDecodeError:
            return []

    def _build_generate_prompt_for_plan(self, plan_view: dict, questions: List[str], context: str) -> str:
        """Build prompt for phase 2: one metric view with full SQL expressions."""
        q_refs = plan_view.get("question_indices", [])
        q_block = "\n".join(f"  {i+1}. {questions[i]}" for i in q_refs if 0 <= i < len(questions))
        plan_str = json.dumps(plan_view, indent=2)
        return f"""You are a data modeler. Output exactly ONE JSON object for a single metric view (not an array).

PLANNED VIEW (names only; you must add "expr" for each dimension and measure):
{plan_str}

RULES:
- Output a single object with keys: name, source, comment, filter (optional), dimensions, measures, joins.
- comment: 1-2 sentences on what the view measures and its source lineage. Do NOT reference question numbers, KPI numbers, or list which questions are/aren't answerable.
- dimensions: array of {{ "name", "expr", "comment", "display_name", "synonyms" }}. "display_name" and "synonyms" (array of 2-5 alternative names) are REQUIRED. expr must be valid Databricks/Spark SQL using ONLY columns from the metadata below.
- measures: array of {{ "name", "expr", "comment", "display_name", "synonyms", "format" }}. "display_name", "synonyms", and "format" are REQUIRED. format is {{"type": "currency"}}, {{"type": "percentage"}} (expr must return a 0-to-1 fraction, NOT multiplied by 100), or {{"type": "number"}}. Use SUM, COUNT, AVG, FILTER, etc.
- QUOTING (critical): ALL string literals in expressions MUST be wrapped in single quotes. This includes LIKE patterns, CASE WHEN/THEN/ELSE values, IN lists, and FILTER conditions. The ONLY unquoted tokens are column references, SQL keywords, and numeric literals.
  CORRECT: CASE WHEN col LIKE '%Binding%' THEN 'Binding' WHEN col LIKE '%Biological Activity%' THEN 'Functional' ELSE 'Other' END
  WRONG:   CASE WHEN col LIKE %Binding% THEN Binding
  CORRECT: COUNT(*) FILTER (WHERE status LIKE '%Active%')
  WRONG:   COUNT(*) FILTER (WHERE status LIKE %Active%)
  CORRECT: DATE_TRUNC('MONTH', order_date)
  WRONG:   DATE_TRUNC(MONTH, order_date)
  Multi-word patterns MUST be one quoted string: '%Biological Activity%' -- NEVER split as '%Biological' Activity%.
  IDENTIFIERS WITH SPACES: If a column name contains spaces, wrap it in backticks: source.`assay name`, NOT source."assay name". Double quotes are NOT valid for identifiers in Databricks SQL.
- joins: use exactly: on: source.<fk_column> = <join_name>.<pk_column>. Keep the same join names and sources as in the plan.
- JOIN FAN-OUT PROTECTION: When the metric view has joins, use COUNT(DISTINCT source.pk_col) instead of COUNT(source.pk_col) for count measures on the source table. Joins can multiply rows (one-to-many fan-out), making plain COUNT overcount. Apply the same logic to denominators in rate/average calculations.
- STAR SCHEMA SOURCE RULE: The source MUST be the fact table (the table at the grain of the analysis). Joins should be many-to-one to dimension tables. NEVER source from a dimension and join to a fact table.
- FACT-TO-FACT JOIN PROHIBITION: Do NOT join from a fact source to another fact table. Fact-to-fact joins create fan-out that inflates ALL aggregates.
- JOIN USAGE REQUIREMENT: Every join MUST have at least one dimension or measure that references it. Remove any plan joins that you cannot map to a concrete expression.
- Only use column names that appear in the metadata.

CATALOG METADATA:
{context}

QUESTIONS this view answers:
{q_block}

OUTPUT (one JSON object only, no array, no explanation):"""

    def _parse_single_definition(self, response: str) -> Optional[dict]:
        """Extract a single JSON object from AI response (phase 2)."""
        text = response.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end <= start:
            return None
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            return None

    def _parse_ai_response(self, response: str) -> list[dict]:
        """Extract JSON array from AI response, tolerating markdown fences."""
        text = response.strip()
        # Strip markdown code fences
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        # Find the JSON array
        start = text.find("[")
        end = text.rfind("]")
        if start == -1 or end == -1:
            logger.error("No JSON array found in AI response")
            return []
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError as e:
            logger.error("Failed to parse AI JSON: %s", e)
            # Try individual objects
            return self._parse_individual_objects(text[start : end + 1])

    def _parse_individual_objects(self, text: str) -> list[dict]:
        """Fallback: extract individual JSON objects from a malformed array."""
        results = []
        depth = 0
        start = None
        for i, ch in enumerate(text):
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and start is not None:
                    try:
                        results.append(json.loads(text[start : i + 1]))
                    except json.JSONDecodeError:
                        pass
                    start = None
        return results

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _discover_join_paths(self, source_table: str, max_hops: int | None = None) -> list[dict]:
        """Walk FK edges from *source_table* up to *max_hops*, returning nested join specs.

        Returns a list of join dicts compatible with the UC metric view ``joins``
        schema.  Each dict has ``name``, ``source``, ``on``, and optionally a
        nested ``joins`` list for multi-hop snowflake patterns.
        """
        if max_hops is None:
            max_hops = self.config.max_join_hops
        fq = self.config.fq
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} "
            f"WHERE final_confidence >= {self.config.fk_confidence_threshold}"
            f" AND (is_fk IS NULL OR is_fk = TRUE)"
        )
        if not fk_rows:
            return []

        # Build undirected adjacency: table -> [(neighbor, fk_col, pk_col)]
        adj: dict[str, list[tuple[str, str, str]]] = {}
        for fk in fk_rows:
            src_t, dst_t = fk["src_table"], fk["dst_table"]
            src_c = fk["src_column"].split(".")[-1]
            dst_c = fk["dst_column"].split(".")[-1]
            adj.setdefault(src_t, []).append((dst_t, src_c, dst_c))
            adj.setdefault(dst_t, []).append((src_t, dst_c, src_c))

        def _walk(table: str, depth: int, visited: set) -> list[dict]:
            if depth >= max_hops:
                return []
            joins: list[dict] = []
            for neighbor, fk_col, pk_col in adj.get(table, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                alias = neighbor.split(".")[-1]
                parent_alias = "source" if depth == 0 else table.split(".")[-1]
                child_joins = _walk(neighbor, depth + 1, visited)
                entry: dict = {
                    "name": alias,
                    "source": neighbor,
                    "on": f"{parent_alias}.{fk_col} = {alias}.{pk_col}",
                }
                if child_joins:
                    entry["joins"] = child_joins
                joins.append(entry)
            return joins

        return _walk(source_table, 0, {source_table})

    def _enrich_joins_from_fk(self, defn: dict) -> None:
        """Auto-add joins block from FK predictions when the definition has none.

        Uses graph-traversed join discovery to produce nested (snowflake) joins
        up to ``max_join_hops`` deep.
        """
        if defn.get("joins"):
            return
        source = defn.get("source", "")
        if not source:
            return
        joins = self._discover_join_paths(source)
        if joins:
            defn["joins"] = joins
            logger.info("Auto-added %d joins to %s from FK predictions (graph-traversed)", len(joins), defn.get("name", ""))

    def _register_join_aliases(self, joins: list[dict], alias_cols: dict[str, set[str]], existing_cols: set[str]) -> None:
        """Recursively register column sets for each join alias (including nested joins)."""
        for j in joins:
            j_source = j.get("source", "")
            j_name = j.get("name", j_source.split(".")[-1] if j_source else "")
            j_parts = j_source.split(".")
            try:
                if len(j_parts) == 3:
                    j_rows = self.spark.sql(
                        f"SELECT column_name FROM system.information_schema.columns "
                        f"WHERE table_catalog = '{j_parts[0]}' AND table_schema = '{j_parts[1]}' "
                        f"AND table_name = '{j_parts[2]}'"
                    ).collect()
                    jcols = {r["column_name"].lower() for r in j_rows}
                    alias_cols[j_name.lower()] = jcols
                    existing_cols.update(jcols)
            except Exception:
                pass
            if j.get("joins"):
                self._register_join_aliases(j["joins"], alias_cols, existing_cols)

    def _validate_definition(self, defn: dict) -> list[str]:
        """Tier 1: structural validation against information_schema."""
        errors = []
        source = defn.get("source", "")
        if not source:
            errors.append("Missing source table")
            return errors

        parts = source.split(".")
        if len(parts) == 3:
            cat, sch, tbl = parts
        elif len(parts) == 2:
            cat, sch, tbl = self.config.catalog_name, parts[0], parts[1]
        else:
            cat, sch, tbl = self.config.catalog_name, self.config.schema_name, source

        existing_cols = set()
        try:
            rows = self.spark.sql(
                f"""
                SELECT column_name FROM system.information_schema.columns
                WHERE table_catalog = '{cat}' AND table_schema = '{sch}' AND table_name = '{tbl}'
            """
            ).collect()
            existing_cols = {r["column_name"].lower() for r in rows}
        except Exception:
            errors.append(f"Could not verify table {source}")
            return errors

        if not existing_cols:
            errors.append(f"Table {source} not found in information_schema")
            return errors

        # Build per-alias column sets so dotted refs (alias.col) are validated correctly
        alias_cols: dict[str, set[str]] = {"source": set(existing_cols)}
        self._register_join_aliases(defn.get("joins", []), alias_cols, existing_cols)

        for item_type in ("dimensions", "measures"):
            for item in defn.get(item_type, []):
                expr = item.get("expr", "")
                col_refs = self._extract_column_refs_with_prefix(expr)
                for prefix, col in col_refs:
                    if prefix and prefix.lower() in alias_cols:
                        if col.lower() not in alias_cols[prefix.lower()]:
                            errors.append(
                                f"{item_type} {item.get('name', '')}: column {col} not found in {prefix}"
                            )
                    elif col.lower() not in existing_cols:
                        errors.append(
                            f"{item_type} {item.get('name', '')}: column {col} not found in {source}"
                        )

        return errors

    _COL_REF_KEYWORDS = {
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DATE_TRUNC", "DISTINCT",
        "MONTH", "QUARTER", "YEAR", "WEEK", "DAY", "HOUR", "CAST", "AS",
        "STRING", "INT", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "DATE",
        "TIMESTAMP", "BOOLEAN", "COALESCE", "IF", "CASE", "WHEN", "THEN",
        "ELSE", "END", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
        "CONCAT", "UPPER", "LOWER", "TRIM", "EXTRACT", "FROM", "FILTER",
        "OVER", "PARTITION", "BY", "ORDER", "ASC", "DESC", "NULLIF",
        "TIMESTAMPDIFF", "ROUND", "ABS", "LENGTH", "SUBSTRING", "REPLACE",
        "LIKE", "BETWEEN", "IN", "IS", "SELECT", "WHERE", "GROUP", "HAVING",
    }

    def _extract_column_refs(self, expr: str) -> list[str]:
        """Extract likely column references from a SQL expression.

        For dotted identifiers (table.col or catalog.schema.table.col),
        only the last segment is treated as a column reference.
        """
        return [col for _, col in self._extract_column_refs_with_prefix(expr)]

    def _extract_column_refs_with_prefix(self, expr: str) -> list[tuple[str, str]]:
        """Extract (prefix, column) tuples from a SQL expression.

        For ``alias.col`` returns ``("alias", "col")``.
        For bare ``col`` returns ``("", "col")``.
        Handles backtick-quoted identifiers: ``source.`col name``` -> ``("source", "col name")``.
        """
        cleaned = re.sub(r"'[^']*'", "", expr)
        cleaned = re.sub(r'"[^"]*"', "", cleaned)
        refs: list[tuple[str, str]] = []
        # Pre-extract backtick-quoted qualified refs
        for m in re.finditer(r"\b([a-zA-Z_]\w*)\.`([^`]+)`", cleaned):
            refs.append((m.group(1), m.group(2)))
        cleaned = re.sub(r"\b[a-zA-Z_]\w*\.`[^`]+`", "", cleaned)
        # Pre-extract bare backtick refs
        for m in re.finditer(r"`([^`]+)`", cleaned):
            refs.append(("", m.group(1)))
        cleaned = re.sub(r"`[^`]+`", "", cleaned)
        # Standard word-character refs
        tokens = re.findall(r"\b([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)\b", cleaned)
        for t in tokens:
            parts = t.rsplit(".", 1)
            if len(parts) == 2:
                prefix, col = parts
            else:
                prefix, col = "", parts[0]
            if col.upper() not in self._COL_REF_KEYWORDS:
                refs.append((prefix, col))
        return refs

    # ------------------------------------------------------------------
    # Expression auto-fix helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Expression auto-fix helpers -- now sourced from dbxmetagen.metric_view_core
    # (shared with the app backend). These classmethods are thin delegators kept
    # for backward compatibility with existing call sites and tests that invoke
    # SemanticLayerGenerator._fix_*/._autofix_expr directly. Constants are also
    # re-exported so any external reference keeps resolving.
    # ------------------------------------------------------------------

    _DATE_TRUNC_INTERVALS = _mvc._DATE_TRUNC_INTERVALS
    _SQL_RESERVED = _mvc._SQL_RESERVED
    _DATEDIFF_UNITS = _mvc._DATEDIFF_UNITS
    _PLURAL_UNITS = _mvc._PLURAL_UNITS

    _fix_unquoted_literals = staticmethod(_mvc._fix_unquoted_literals)
    _fix_case_quoting = staticmethod(_mvc._fix_case_quoting)
    _fix_then_else_literals = staticmethod(_mvc._fix_then_else_literals)
    _fix_in_clause_literals = staticmethod(_mvc._fix_in_clause_literals)
    _fix_concat_separators = staticmethod(_mvc._fix_concat_separators)
    _fix_date_part = staticmethod(_mvc._fix_date_part)
    _fix_datediff = staticmethod(_mvc._fix_datediff)
    _fix_like_patterns = staticmethod(_mvc._fix_like_patterns)
    _fix_dquote_identifier = staticmethod(_mvc._fix_dquote_identifier)
    _fix_instr_bare_arg = staticmethod(_mvc._fix_instr_bare_arg)
    _fix_position_bare_char = staticmethod(_mvc._fix_position_bare_char)
    _fix_double_commas = staticmethod(_mvc._fix_double_commas)
    _fix_bare_whitespace_separator = staticmethod(_mvc._fix_bare_whitespace_separator)
    _fix_quoted_computation = staticmethod(_mvc._fix_quoted_computation)
    _fix_bare_comparison = staticmethod(_mvc._fix_bare_comparison)
    _fix_null_comparison = staticmethod(_mvc._fix_null_comparison)
    _fix_none_literal = staticmethod(_mvc._fix_none_literal)
    _fix_concat_bare_first_arg = staticmethod(_mvc._fix_concat_bare_first_arg)
    _fix_percentile_cont = staticmethod(_mvc._fix_percentile_cont)
    _autofix_expr = staticmethod(_autofix_expr_core)


    def _validate_expressions(self, defn: dict) -> list[str]:
        """Tier 2: test each expression with LIMIT 0 query, auto-fixing when possible."""
        errors = []
        source = defn.get("source", "")
        if not source:
            return errors

        from_clause = self._build_validation_from(defn)

        for item_type in ("dimensions", "measures"):
            for item in defn.get(item_type, []):
                expr = item.get("expr", "")
                name = item.get("name", "")
                fixed = self._autofix_expr(expr)
                if fixed != expr:
                    item["expr"] = fixed
                    expr = fixed
                try:
                    self.spark.sql(f"SELECT {expr} FROM {from_clause} LIMIT 0")
                except Exception as e:
                    errors.append(f"{item_type} '{name}': expression error: {e}")
        return errors

    @staticmethod
    def _build_validation_from(defn: dict) -> str:
        """Build a FROM clause with proper aliases for expression validation."""
        source = defn.get("source", "")
        joins = defn.get("joins", [])
        if not joins:
            return source

        def _render_joins(jlist: list[dict]) -> list[str]:
            parts: list[str] = []
            for j in jlist:
                j_source = j.get("source", "")
                if not j_source:
                    continue
                alias = j.get("name", j_source.split(".")[-1])
                join_type = j.get("type", "LEFT").upper()
                on_clause = j.get("on", "")
                if on_clause:
                    parts.append(f"{join_type} JOIN {j_source} AS {alias} ON {on_clause}")
                else:
                    using = j.get("using", [])
                    if using:
                        cols = ", ".join(using)
                        parts.append(f"{join_type} JOIN {j_source} AS {alias} USING ({cols})")
                    else:
                        parts.append(f"CROSS JOIN {j_source} AS {alias}")
                if j.get("joins"):
                    parts.extend(_render_joins(j["joins"]))
            return parts

        return " ".join([f"{source} AS source"] + _render_joins(joins))

    # ------------------------------------------------------------------
    # Apply metric views
    # ------------------------------------------------------------------

    def apply_metric_views(self) -> Dict[str, Any]:
        """Deploy validated metric view definitions as UC views via ``CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML``.

        Reads rows with status ``validated`` from the definitions table, executes
        the DDL, and updates each row's status to ``applied`` (recording
        ``applied_at`` and ``deployed_catalog``/``deployed_schema``) or ``failed``
        on error. Returns a summary dict of counts.
        """
        fq = self.config.fq
        rows = [
            r.asDict()
            for r in self.spark.sql(
                f"SELECT definition_id, metric_view_name, source_table, json_definition "
                f"FROM {fq(self.config.definitions_table)} WHERE status = 'validated'"
            ).collect()
        ]

        applied = 0
        failed = 0
        for row in rows:
            defn = json.loads(row["json_definition"])
            mv_name = row["metric_view_name"]
            source = defn.get("source", row.get("source_table", ""))
            src_parts = source.split(".") if source else []
            deploy_cat = src_parts[0] if len(src_parts) >= 3 else self.config.catalog_name
            deploy_sch = src_parts[1] if len(src_parts) >= 3 else self.config.schema_name
            fq_mv = f"{deploy_cat}.{deploy_sch}.{mv_name}"
            with _trace_span("apply_metric_view") as apply_span:
                if apply_span is not None:
                    apply_span.set_inputs({"metric_view_name": mv_name, "fq_mv": fq_mv})
                try:
                    yaml_body = self._definition_to_yaml(defn, include_materialization=True)
                    sql = f"CREATE OR REPLACE VIEW {fq_mv}\nWITH METRICS LANGUAGE YAML AS $$\n{yaml_body}$$"
                    self.spark.sql(sql)
                    cat_esc = deploy_cat.replace("'", "''")
                    sch_esc = deploy_sch.replace("'", "''")
                    # UPDATE: Single definitions-table row by definition_id after successful CREATE OR REPLACE VIEW;
                    # transitions status→'applied', sets applied_at, records deployed_catalog/schema where the UC view landed.
                    # WHY: Gives operators a persisted source of truth for which YAML definitions succeeded and where
                    # they physically live for governance and Genie onboarding.
                    # TRADEOFFS: Row-wise UPDATE mirrors sequential apply (clear errors) versus batched MERGE (faster
                    # writes but coarse failure attribution); catalogs/schemas derived from source triple may diverge from config if mis-specified upstream.
                    self.spark.sql(
                        f"""
                        UPDATE {fq(self.config.definitions_table)}
                        SET status = 'applied', applied_at = current_timestamp(),
                            deployed_catalog = '{cat_esc}', deployed_schema = '{sch_esc}'
                        WHERE definition_id = '{row['definition_id']}'
                    """
                    )
                    applied += 1
                    logger.info("Applied metric view %s", fq_mv)
                    if apply_span is not None:
                        apply_span.set_outputs({"success": True})
                except Exception as e:
                    logger.error("Failed to apply metric view %s: %s", fq_mv, e)
                    err = str(e).replace("'", "''")
                    # UPDATE: Same definitions-table row keyed by definition_id after DDL failure; flags status='failed'
                    # and overwrites validation_errors with the DDL exception text (truncation only by exception length).
                    # WHY: Keeps persisted definitions aligned with runtime—operators can distinguish pre-store validation failures
                    # vs deploy-time breakage without scraping logs only.
                    # TRADEOFFS: Mutates validation_errors (loses earlier static validation messaging) versus append-only audit
                    # columns; quoting depends on sanitising apostrophes in error strings instead of parameterized SQL paths.
                    self.spark.sql(
                        f"""
                        UPDATE {fq(self.config.definitions_table)}
                        SET status = 'failed', validation_errors = '{err}'
                        WHERE definition_id = '{row['definition_id']}'
                    """
                    )
                    failed += 1
                    if apply_span is not None:
                        apply_span.set_outputs({"success": False, "error": str(e)[:500]})

        if applied > 0:
            try:
                from dbxmetagen.semantic_graph import build_semantic_graph
                build_semantic_graph(self.spark, self.config)
            except Exception as e:
                logger.warning("Semantic graph sync after apply failed: %s", e)

        return {"applied": applied, "failed": failed}

    # ------------------------------------------------------------------
    # Join normalization + YAML serialization -- sourced from metric_view_core
    # (shared with the app backend). _definition_to_yaml runs the join pipeline
    # here (library contract) then delegates to the shared pure serializer.
    # ------------------------------------------------------------------

    _normalize_joins = staticmethod(_mvc._normalize_joins)
    _restructure_chained_to_nested = staticmethod(_mvc._restructure_chained_to_nested)
    _qualify_nested_refs = staticmethod(_mvc._qualify_nested_refs)
    _IndentYamlDumper = _mvc._IndentYamlDumper

    def _definition_to_yaml(self, defn: dict, include_materialization: bool = False) -> str:
        """Normalize joins, then serialize to YAML for CREATE VIEW WITH METRICS.

        ``include_materialization`` is False by default so dry-run validation never
        emits the materialization block (it would provision a Lakeflow pipeline).
        Real create/apply paths pass True.
        """
        _mvc._normalize_joins(defn)
        defn = _mvc._restructure_chained_to_nested(defn)
        defn = _mvc._qualify_nested_refs(defn)
        return _mvc._definition_to_yaml(defn, include_materialization=include_materialization)


    # ------------------------------------------------------------------
    # Genie space creation
    # ------------------------------------------------------------------

    def _build_genie_instructions(self, applied: list, fk_rows: list, tables_meta: list, entity_map: dict) -> dict:
        """Build rich Genie space instructions from available metadata."""
        # text_instructions: schema context
        text_instructions = []
        domains = set()
        entities = set()
        table_names = []
        for t in tables_meta:
            table_names.append(t.get("table_name", "").split(".")[-1])
            if t.get("domain"):
                domains.add(t["domain"])
        for ent in entity_map.values():
            entities.add(ent)
        if domains:
            text_instructions.append(f"This space contains {', '.join(sorted(domains))} data.")
        if entities:
            text_instructions.append(f"Key entities: {', '.join(sorted(entities))}.")
        text_instructions.append("IDs are typically integer primary keys. Date columns use DATE or TIMESTAMP types.")
        if table_names:
            text_instructions.append(f"Key tables: {', '.join(table_names[:10])}.")

        # join_specs from FK predictions
        join_specs = []
        for fk in fk_rows:
            src_col = fk.get("src_column", "").split(".")[-1]
            dst_col = fk.get("dst_column", "").split(".")[-1]
            join_specs.append({
                "left_table": fk["src_table"],
                "right_table": fk["dst_table"],
                "on_clause": f"{fk['src_table']}.{src_col} = {fk['dst_table']}.{dst_col}",
            })

        # sql_snippets from metric view measures
        measures_snippets = []
        for r in applied:
            try:
                defn = json.loads(r["json_definition"]) if isinstance(r.get("json_definition"), str) else {}
                for m in defn.get("measures", [])[:3]:
                    measures_snippets.append({
                        "title": m.get("name", ""),
                        "description": m.get("comment", ""),
                        "body": m.get("expr", ""),
                    })
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "text_instructions": text_instructions,
            "example_question_sqls": [],
            "join_specs": join_specs[:20],
            "sql_snippets": measures_snippets[:15] if measures_snippets else None,
        }

    def _generate_example_sqls(self, sample_qs: list, context: str) -> list:
        """Generate validated example SQL queries for Genie from business questions."""
        if not sample_qs:
            return []
        examples = []
        q_block = "\n".join(f"  {i+1}. {q}" for i, q in enumerate(sample_qs[:5]))
        prompt = (
            f"Given these business questions and catalog metadata, generate a SQL query for each.\n\n"
            f"CATALOG:\n{context[:3000]}\n\nQUESTIONS:\n{q_block}\n\n"
            f"Output JSON array of objects: [{{\"question\": \"...\", \"sql\": \"SELECT ...\"}}]. Only output JSON."
        )
        escaped = prompt.replace("'", "''")
        try:
            resp = self.spark.sql(
                f"SELECT AI_QUERY('{self.config.model_endpoint}', '{escaped}') as response"
            ).collect()[0]["response"]
            text = resp.strip()
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            start = text.find("[")
            end = text.rfind("]")
            if start >= 0 and end > start:
                items = json.loads(text[start:end + 1])
                for item in items:
                    sql = item.get("sql", "")
                    if not sql:
                        continue
                    # Validate by running with LIMIT 1
                    try:
                        self.spark.sql(f"{sql.rstrip(';')} LIMIT 1")
                        examples.append({"question": item.get("question", ""), "sql": sql})
                    except Exception:
                        logger.debug("Example SQL failed validation: %s", sql[:100])
        except Exception as e:
            logger.warning("Example SQL generation failed: %s", e)
        return examples

    def create_genie_space(self, display_name: str, warehouse_id: str) -> Optional[str]:
        """Build a Genie space from applied metric views and deploy via the REST API.

        This is the **notebook/job path** -- a simplified builder that reads
        applied MVs, KB, FK predictions, and ontology entities directly via
        Spark SQL, assembles a Genie payload dict, and POSTs it to the Genie
        spaces endpoint.  It does *not* use ``GenieContextAssembler``,
        ``run_genie_agent``, or ``build_serialized_space`` from ``genie/``.

        For the full-featured Genie builder with 3-phase LLM agent, Pydantic
        schema normalization, join merging, and deploy retries, use the in-app
        ``/api/genie/create`` endpoint (backed by ``src/dbxmetagen/genie/*``).

        Returns the ``genie_space_id`` or None on failure.
        """
        fq = self.config.fq

        applied = [
            r.asDict()
            for r in self.spark.sql(
                f"SELECT metric_view_name, source_table, json_definition, deployed_catalog, deployed_schema "
                f"FROM {fq(self.config.definitions_table)} WHERE status = 'applied'"
            ).collect()
        ]
        if not applied:
            logger.warning("No applied metric views -- skipping Genie space creation")
            return None

        questions = [
            r.asDict()
            for r in self.spark.sql(
                f"SELECT question_text FROM {fq(self.config.questions_table)} WHERE status = 'processed'"
            ).collect()
        ]
        sample_qs = [r["question_text"] for r in questions if r.get("question_text")][:10]

        # Gather metadata for instructions
        tables_meta = self._safe_collect(
            f"SELECT table_name, comment, domain, subdomain FROM {fq('table_knowledge_base')}"
        )
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} WHERE final_confidence >= {self.config.fk_confidence_threshold}"
            f" AND (is_fk IS NULL OR is_fk = TRUE)"
        )
        ont_rows = self._safe_collect(
            f"SELECT entity_type, source_tables FROM {fq('ontology_entities')} WHERE confidence >= 0.4"
        )
        entity_map: dict = {}
        for o in ont_rows:
            for t in o.get("source_tables") or []:
                entity_map[t] = o["entity_type"]

        # Relationship edges for richer description
        edge_rows = self._safe_collect(
            f"""SELECT e_src.entity_type AS src_type, e_dst.entity_type AS dst_type, ge.relationship
                FROM {fq('graph_edges')} ge
                JOIN {fq('ontology_entities')} e_src ON ge.src = e_src.entity_id
                JOIN {fq('ontology_entities')} e_dst ON ge.dst = e_dst.entity_id
                WHERE ge.relationship != 'is_a'"""
        )
        rel_pairs: list[str] = []
        for r in edge_rows:
            src, dst, rel = r.get("src_type", ""), r.get("dst_type", ""), r.get("relationship", "")
            if src and dst and rel:
                rel_pairs.append(f"{src} {rel} {dst}")

        # Build metric view data sources
        metric_views_ds = []
        for r in applied:
            mv_name = r.get("metric_view_name")
            if not mv_name:
                continue
            mv_cat = r.get("deployed_catalog") or self.config.catalog_name
            mv_sch = r.get("deployed_schema") or self.config.schema_name
            identifier = f"{mv_cat}.{mv_sch}.{mv_name}"
            desc_lines = []
            try:
                defn = json.loads(r["json_definition"]) if isinstance(r.get("json_definition"), str) else {}
                if defn.get("comment"):
                    desc_lines.append(defn["comment"])
            except (json.JSONDecodeError, TypeError):
                pass
            if not desc_lines:
                desc_lines = [f"Metric view on {r.get('source_table', '')}"]
            metric_views_ds.append({"identifier": identifier, "description": desc_lines})

        # Build instructions
        instructions = self._build_genie_instructions(applied, fk_rows, tables_meta, entity_map)

        # Generate and validate example SQL queries
        try:
            context = self.build_context()
            example_sqls = self._generate_example_sqls(sample_qs, context)
            if example_sqls:
                instructions["example_question_sqls"] = example_sqls
        except Exception as e:
            logger.warning("Could not generate example SQLs: %s", e)

        # Description
        domain_str = ", ".join(sorted({t.get("domain", "") for t in tables_meta if t.get("domain")}))
        entity_str = ", ".join(sorted(set(entity_map.values())))
        description = (
            f"Semantic layer for {self.config.catalog_name}.{self.config.schema_name} "
            f"with {len(metric_views_ds)} metric views"
        )
        if domain_str:
            description += f" covering {domain_str} domains"
        if entity_str:
            description += f". Key entities: {entity_str}"
        if rel_pairs:
            description += f". Relationships: {'; '.join(rel_pairs[:10])}"

        space = {
            "version": 2,
            "data_sources": {"tables": [], "metric_views": metric_views_ds},
            "instructions": instructions,
        }
        if sample_qs:
            space["config"] = {
                "sample_questions": [{"id": str(uuid.uuid4()).replace("-", "")[:24], "question": [q]} for q in sample_qs]
            }

        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            payload = {
                "title": display_name,
                "warehouse_id": warehouse_id,
                "description": description,
                "serialized_space": json.dumps(space),
            }
            resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
            space_id = resp.get("space_id", resp.get("id", ""))
            logger.info("Created Genie space %s (%s)", display_name, space_id)

            # UPDATE: All definitions-table rows with status='applied' receive the same genie_space_id returned
            # by the Genie REST API (no per-definition WHERE clause).
            # WHY: After creating one composite Genie space for the batch, every successfully deployed metric view row
            # must point consumers to that space id for navigation and API follow-up.
            # TRADEOFFS: Simple broadcast update vs per-definition mapping (cannot represent multiple spaces in one table
            # without extra columns); any stale 'applied' rows from prior runs get relabeled to the newest space_id.
            self.spark.sql(
                f"""
                UPDATE {fq(self.config.definitions_table)}
                SET genie_space_id = '{space_id}'
                WHERE status = 'applied'
            """
            )
            return space_id
        except Exception as e:
            logger.error(
                "Genie space creation failed (API may not be available): %s", e
            )
            return None


def generate_semantic_layer(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    questions: Optional[List[str]] = None,
    model_endpoint: str = "databricks-gpt-oss-120b",
    fk_confidence_threshold: float = 0.7,
    validate_expressions: bool = True,
    materialize_metric_views: bool = False,
    materialization_schedule: str = "every 6 hours",
) -> Dict[str, Any]:
    """Convenience function to generate semantic layer metric views.

    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        questions: Optional list of new questions to ingest before generating
        model_endpoint: AI model endpoint for generation
        fk_confidence_threshold: Min FK confidence for join generation
        validate_expressions: If True, test expressions with LIMIT 0 queries
        materialize_metric_views: If True, attach an unaggregated materialization to each view
        materialization_schedule: Refresh schedule for materializations (MV schedule clause syntax)

    Returns:
        Dict with generation statistics
    """
    config = SemanticLayerConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        model_endpoint=model_endpoint,
        fk_confidence_threshold=fk_confidence_threshold,
        validate_expressions=validate_expressions,
        materialize_metric_views=materialize_metric_views,
        materialization_schedule=materialization_schedule,
    )
    gen = SemanticLayerGenerator(spark, config)
    gen.create_tables()
    if questions:
        gen.ingest_questions(questions)
    return gen.generate_metric_views()

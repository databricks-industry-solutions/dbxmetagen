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


@dataclass
class SemanticLayerConfig:
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
     {"name": "Total Revenue", "expr": "SUM(total_amount)", "comment": "Sum of all order values"},
     {"name": "Avg Order Value", "expr": "AVG(total_amount)", "comment": "Average order amount"},
     {"name": "Revenue per Customer", "expr": "SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_id), 0)", "comment": "Average revenue per unique customer"},
     {"name": "Fulfillment Rate", "expr": "SUM(CASE WHEN status = 'fulfilled' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of orders fulfilled"},
     {"name": "Fulfilled Revenue", "expr": "SUM(total_amount) FILTER (WHERE status = 'fulfilled')", "comment": "Revenue from fulfilled orders only"},
     {"name": "30-Day Rolling Avg Revenue", "expr": "AVG(SUM(total_amount))", "window": {"order_by": "order_date", "range": "INTERVAL 30 DAYS PRECEDING"}, "comment": "Rolling 30-day average of daily revenue"}],
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
     {"name": "Encounter Count", "expr": "COUNT(*)", "comment": "Total encounters"},
     {"name": "Unique Patients", "expr": "COUNT(DISTINCT patient_id)", "comment": "Distinct patient count"},
     {"name": "Avg Length of Stay", "expr": "AVG(DATEDIFF(discharge_date, admit_date))", "comment": "Average days from admit to discharge"},
     {"name": "Encounters per Patient", "expr": "COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT patient_id), 0)", "comment": "Average visits per patient"},
     {"name": "Charge per Encounter", "expr": "SUM(total_charges) / NULLIF(COUNT(*), 0)", "comment": "Average charge per encounter"}],
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
     {"name": "Transaction Count", "expr": "COUNT(*)", "comment": "Total transactions"},
     {"name": "Total Amount", "expr": "SUM(amount)", "comment": "Sum of transaction amounts"},
     {"name": "Fraud Rate", "expr": "SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of flagged transactions"},
     {"name": "Deposit Volume", "expr": "SUM(amount) FILTER (WHERE txn_type = 'deposit')", "comment": "Total deposit inflows"}],
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


class SemanticLayerGenerator:

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
                applied_at TIMESTAMP
            ) COMMENT 'Generated metric view definitions'
        """
        )
        logger.info("Semantic layer tables ready")

    # ------------------------------------------------------------------
    # Question ingestion
    # ------------------------------------------------------------------

    def ingest_questions(self, questions: List[str]) -> int:
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

        # Column knowledge base (required)
        columns = self._safe_collect(
            f"SELECT table_name, column_name, data_type, comment, classification FROM {fq('column_knowledge_base')}"
        )
        col_by_table: dict[str, list] = {}
        for c in columns:
            col_by_table.setdefault(c["table_name"], []).append(c)

        # FK predictions (optional; filter by confidence only - fk_predictions has no is_fk column)
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} "
            f"WHERE final_confidence >= {self.config.fk_confidence_threshold}"
        )

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
        def _entity_suggestion(ent_type: str, ent_tables: list[str]) -> str:
            has_temporal = any(
                any(col_props_by_table.get(t, {}).get(c, {}).get("is_temporal") for c in [cc["column_name"] for cc in col_by_table.get(t, [])])
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
                col_strs.append(
                    f"  - {cname} {c.get('data_type', '')}{role_str} : {c.get('comment', '')}"
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
                parts.append(
                    f"  {fk['src_table']}.{fk['src_column']} -> {fk['dst_table']}.{fk['dst_column']} (confidence {fk['final_confidence']:.2f})"
                )
            parts.append(
                "\nRECOMMENDED JOINS (use these when building metric views; fact tables like encounters/orders should join to dimension tables for breakdowns):"
            )
            for fk in fk_rows:
                parts.append(
                    f"  {fk['src_table']} + {fk['dst_table']}: src_column={fk['src_column']}, dst_column={fk['dst_column']}"
                )

        # Inject metric view best-practices reference (loaded from JSON)
        ref = _load_reference("metric_view_reference.json")
        if ref:
            ref_text = _format_reference_section(ref, ["yaml_syntax_rules", "measure_patterns", "join_templates", "anti_patterns", "validation_checklist"])
            if ref_text:
                parts.append("\nREFERENCE: METRIC VIEW BEST PRACTICES (follow these rules strictly)")
                parts.append(ref_text)

        return "\n".join(parts)

    def _safe_collect(self, sql: str) -> list[dict]:
        """Run SQL and return list of dicts; returns [] if table doesn't exist."""
        try:
            return [row.asDict() for row in self.spark.sql(sql).collect()]
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(e):
                logger.info("Skipping unavailable table: %s", e)
                return []
            raise

    # ------------------------------------------------------------------
    # AI generation
    # ------------------------------------------------------------------

    def generate_metric_views(self) -> Dict[str, Any]:
        """Read pending questions, call AI_QUERY, validate, store definitions."""
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
                plan_escaped = plan_prompt.replace("'", "''")
                plan_response = ""
                try:
                    plan_response = self.spark.sql(
                        f"SELECT AI_QUERY('{self.config.model_endpoint}', '{plan_escaped}') as response"
                    ).collect()[0]["response"]
                except Exception as e:
                    logger.warning("Plan phase failed, falling back to single-phase: %s", e)
                plan_views = self._parse_plan_response(plan_response) if plan_response else []
                if plan_views:
                    for idx, plan_view in enumerate(plan_views):
                        gen_prompt = self._build_generate_prompt_for_plan(plan_view, questions, context)
                        gen_escaped = gen_prompt.replace("'", "''")
                        try:
                            gen_response = self.spark.sql(
                                f"SELECT AI_QUERY('{self.config.model_endpoint}', '{gen_escaped}') as response"
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
                escaped = prompt.replace("'", "''")
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
                                f"SELECT AI_QUERY('{self.config.model_endpoint}', '{escaped}') as response"
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

                defn_id = str(uuid.uuid4())
                mv_name = defn.get("name", f"metric_view_{defn_id[:8]}")
                source = defn.get("source", "")
                json_str = json.dumps(defn).replace("'", "''")

                with _trace_span("validate_definition") as val_span:
                    errors = self._validate_definition(defn)
                    if self.config.validate_expressions and not errors:
                        errors = self._validate_expressions(defn)
                    if not errors and self.config.validate_before_store:
                        dry_run_name = f"{mv_name}_dry_run"
                        fq_dry = f"{self.config.catalog_name}.{self.config.schema_name}.{dry_run_name}"
                        try:
                            yaml_body = self._definition_to_yaml(defn)
                            self.spark.sql(
                                f"CREATE OR REPLACE VIEW {fq_dry}\nWITH METRICS LANGUAGE YAML AS $$\n{yaml_body}$$"
                            )
                            self.spark.sql(f"DROP VIEW IF EXISTS {fq_dry}")
                        except Exception as e:
                            errors = [f"dry-run CREATE failed: {e}"]
                    if val_span is not None:
                        val_span.set_inputs({"metric_view_name": mv_name, "source": source})
                        val_span.set_outputs({"outcome": "validated" if not errors else "failed", "validation_errors": errors or None})

                status = "validated" if not errors else "failed"
                error_str = "; ".join(errors).replace("'", "''") if errors else ""

                self.spark.sql(
                    f"""
                    INSERT INTO {fq(self.config.definitions_table)} VALUES (
                        '{defn_id}', '{mv_name}', '{source}', '{json_str}',
                        '{q_id_str}', '{status}', '{error_str}', NULL,
                        '{now}', NULL
                    )
                """
                )
                stats[status] += 1
                stats["generated"] += 1

            # Mark questions: processed if at least one definition stored, failed if all definitions failed validation
            id_list = ", ".join(f"'{qid}'" for qid in q_ids)
            if stats["generated"] > 0:
                status_val = "processed" if stats["validated"] > 0 else "failed"
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

RULES:
1. Create measures that directly support answering the business questions. Do NOT add a generic "row count" or "Record Count" measure unless a question explicitly asks for "how many records" or "count of X". Prefer ratios (e.g. rate, per capita), conditional aggregates (FILTER), and entity-specific KPIs (e.g. readmission rate, avg length of stay) over raw COUNT(*) when the question implies a more specific metric.
2. Create metric views organized around analytical themes from the questions, not just one-to-one with tables. Multiple metric views from the same table are fine if they address different analytical angles
4. Only reference columns that exist in the metadata below (Columns in the metadata). Do not invent column names
5. Use standard SQL aggregate functions: SUM, COUNT, AVG, MIN, MAX, COUNT(DISTINCT ...)
6. Date/time function rules (Databricks/Spark SQL):
   a. DATE_TRUNC: quote the interval -- DATE_TRUNC('MONTH', col). NEVER use bare DATE_TRUNC(MONTH, col)
   b. EXTRACT: use EXTRACT(HOUR FROM col) for extracting date parts. Do NOT use DATE_PART(HOUR, col)
   c. DATEDIFF only returns days with 2 args: DATEDIFF(end, start). For other units use TIMESTAMPDIFF(MINUTE, start, end) with a bare unquoted keyword unit
   d. TIMESTAMPADD: bare unquoted singular unit -- TIMESTAMPADD(MONTH, 1, col), NOT TIMESTAMPADD('MONTHS', 1, col)
7. ALWAYS single-quote ALL string literal values everywhere in expressions:
   - Comparisons: status = 'fulfilled', NOT status = fulfilled
   - THEN/ELSE results: CASE WHEN x = 'A' THEN 'Category A' ELSE 'Other' END, NOT THEN Category A
   - IN lists: department IN ('Surgery', 'Pediatrics'), NOT IN (Surgery, Pediatrics)
   - CONCAT separators: CONCAT(hospital, ' - ', department), NOT CONCAT(hospital, - , department)
   - CASE results with parens/hyphens: THEN '0-15 min (Excellent)', NOT THEN 0-15 min (Excellent)
   The ONLY unquoted tokens should be column names, SQL keywords, and numbers
   WRONG: status = fulfilled, region IN (North, South). CORRECT: status = 'fulfilled', region IN ('North', 'South')
8. Join format (Unity Catalog): For each join use: name: <short_alias>, source: catalog.schema.table, on: source.<fk_column> = <join_name>.<pk_column>. The root table is always "source"; the joined table is referenced by its "name" (short alias). Example: on: source.customer_id = customers.id
9. Include joins when RECOMMENDED JOINS exist; when questions ask for breakdowns by attributes in another table (e.g. by customer segment, department), you MUST add a join. Prefer at least one metric view with joins when FKs exist
10. Every metric view MUST have at least one measure and one dimension
11. Add a top-level "comment" describing the metric view's purpose
12. Add a "comment" to each dimension and measure explaining what it represents. Optionally add "display_name" (human-readable label, max 255 chars) and "synonyms" (array of 2-5 alternative names for Genie discoverability, e.g. ["revenue", "total sales"] for Total Revenue).
13. Use "filter" (optional) for persistent WHERE clauses (e.g. excluding null/test rows)
14. Use measure-level FILTER for conditional aggregation: SUM(col) FILTER (WHERE condition)
15. If some questions are not answerable with metrics (e.g. document search, free-text lookups, SOP retrieval), generate metric views for the ones that ARE quantitative/analytical and silently ignore the rest
16. Each metric view "name" must be unique and descriptive (e.g. staffing_efficiency_metrics, ed_throughput_analysis). Vary names based on the analytical theme, not just the table name
17. Output ONLY a valid JSON array, no explanation
18. Use domain and subdomain from table metadata to choose which dimensions (e.g. department, region, product category) are relevant to the questions.
19. When Entity types are annotated on tables, use the ENTITY MEASURE SUGGESTIONS in the metadata and generate entity-specific analytical metrics:
    - People/patients/users: include counts, return/readmission rates, segmentation dimensions, and per-entity averages
    - Transactions/events/encounters: include volume counts, value sums, time-based rates, and categorical breakdowns
    - Resources/staff/inventory: include utilization rates (active/total), efficiency ratios, and capacity measures
    Always include at least one RATIO measure (x / NULLIF(y, 0)) and one RATE measure (conditional_count * 1.0 / NULLIF(total, 0)) per metric view
20. When RECOMMENDED JOINS / FOREIGN KEY RELATIONSHIPS exist, generate cross-table metrics that join fact tables to dimension tables. Use dimension table columns as grouping dimensions and fact table columns as measures
21. Dimension and measure names should be colloquial and business-friendly:
    - For simple column references, use the column's natural name (e.g. "Industry" not "Account Industry", "Status" not "Order Status")
    - Only add a qualifier when two dimensions would otherwise be ambiguous (e.g. "Billing State" vs "Shipping State")
    - For date truncations, use "{{Column}} Month" or "{{Column}} Quarter" style (e.g. "Order Month")
    - For measures, use what a business user would say: "Total Sales", "Avg Order Value", "Fulfillment Rate"
    - NEVER prefix dimension names with the join alias (use "Industry" not "account.Industry")
    - LIKE patterns MUST be quoted: product_code LIKE 'HW%', NOT product_code LIKE HW%

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

For each metric view in "views", include:
- "name": unique snake_case name (e.g. order_performance_metrics)
- "source": fully qualified source table (catalog.schema.table)
- "comment": one sentence purpose
- "joins": array of {{ "name": "<alias>", "source": "catalog.schema.table", "on": "source.<fk_col> = <alias>.<pk_col>" }} when RECOMMENDED JOINS exist and questions need breakdowns by another table
- "dimensions": array of {{ "name": "Display Name", "comment": "what it is" }} (no expr)
- "measures": array of {{ "name": "Display Name", "comment": "what it measures" }} (no expr)
- "question_indices": array of 0-based question indices this view answers

Create measures that match the business questions (ratios, rates, KPIs); avoid generic row count unless a question explicitly asks for it. Use RECOMMENDED JOINS when questions ask for breakdown by attributes in another table. Each view must have at least one dimension and one measure.

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
- dimensions: array of {{ "name", "expr", "comment" }}; optionally "display_name" and "synonyms" (array of strings) for semantic metadata. expr must be valid Databricks/Spark SQL using ONLY columns from the metadata below. Use DATE_TRUNC('MONTH', col) etc.; single-quote all string literals.
- measures: array of {{ "name", "expr", "comment" }}; optionally "display_name" and "synonyms". Use SUM, COUNT, AVG, FILTER, etc. String literals single-quoted (e.g. status = 'fulfilled').
- joins: use exactly: on: source.<fk_column> = <join_name>.<pk_column>. Keep the same join names and sources as in the plan.
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

    def _enrich_joins_from_fk(self, defn: dict) -> None:
        """Auto-add joins block from FK predictions when the definition has none."""
        if defn.get("joins"):
            return
        source = defn.get("source", "")
        if not source:
            return
        fq = self.config.fq
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} "
            f"WHERE final_confidence >= {self.config.fk_confidence_threshold} "
            f"AND (src_table = '{source}' OR dst_table = '{source}')"
        )
        if not fk_rows:
            return
        joins = []
        seen_tables = set()
        for fk in fk_rows:
            if fk["src_table"] == source:
                join_table = fk["dst_table"]
                fk_col = fk["src_column"].split(".")[-1]
                pk_col = fk["dst_column"].split(".")[-1]
            else:
                join_table = fk["src_table"]
                fk_col = fk["dst_column"].split(".")[-1]
                pk_col = fk["src_column"].split(".")[-1]
            if join_table in seen_tables:
                continue
            seen_tables.add(join_table)
            alias = join_table.split(".")[-1]
            joins.append({
                "name": alias,
                "source": join_table,
                "on": f"source.{fk_col} = {alias}.{pk_col}",
            })
        if joins:
            defn["joins"] = joins
            logger.info("Auto-added %d joins to %s from FK predictions", len(joins), defn.get("name", ""))

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
                SELECT column_name FROM {cat}.information_schema.columns
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
        for j in defn.get("joins", []):
            j_source = j.get("source", "")
            j_name = j.get("name", j_source.split(".")[-1] if j_source else "")
            j_parts = j_source.split(".")
            try:
                if len(j_parts) == 3:
                    j_rows = self.spark.sql(
                        f"""
                        SELECT column_name FROM {j_parts[0]}.information_schema.columns
                        WHERE table_catalog = '{j_parts[0]}' AND table_schema = '{j_parts[1]}' AND table_name = '{j_parts[2]}'
                    """
                    ).collect()
                    jcols = {r["column_name"].lower() for r in j_rows}
                    alias_cols[j_name.lower()] = jcols
                    existing_cols.update(jcols)
            except Exception:
                pass

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
        """
        cleaned = re.sub(r"'[^']*'", "", expr)
        cleaned = re.sub(r'"[^"]*"', "", cleaned)
        tokens = re.findall(r"\b([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)\b", cleaned)
        refs: list[tuple[str, str]] = []
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

    _DATE_TRUNC_INTERVALS = {
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
    }
    _SQL_RESERVED = {
        "THEN",
        "ELSE",
        "END",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "CASE",
        "WHEN",
        "IN",
        "IS",
        "LIKE",
        "BETWEEN",
        "SELECT",
        "FROM",
        "WHERE",
        "FILTER",
        "DISTINCT",
        "SUM",
        "AVG",
        "COUNT",
        "MIN",
        "MAX",
        "DATE_TRUNC",
        "IF",
        "COALESCE",
        "NULLIF",
        "OVER",
        "PARTITION",
        "BY",
        "ORDER",
        "ASC",
        "DESC",
    }

    @classmethod
    def _fix_unquoted_literals(cls, expr: str) -> str:
        """Quote bare words used as string literals in comparisons."""

        def _replacer(m):
            op, word, trail = m.group(1), m.group(2), m.group(3)
            if (
                word.upper() in cls._SQL_RESERVED
                or word.upper() in cls._DATE_TRUNC_INTERVALS
            ):
                return m.group(0)
            return f"{op}'{word}'{trail}"

        return re.sub(
            r"([=!<>]+\s*)([A-Za-z_]\w*)(\s*(?:THEN|ELSE|END|AND|OR|WHEN|,|\))|$)",
            _replacer,
            expr,
            flags=re.IGNORECASE,
        )

    @classmethod
    def _fix_case_quoting(cls, expr: str) -> str:
        """Fix ELSE/END keywords trapped inside single-quoted string literals.

        Rewrites  'Medium Cost ELSE Standard Cost END'
        to        'Medium Cost' ELSE 'Standard Cost' END
        """
        _KW = re.compile(r"\b(ELSE|END|WHEN)\b", re.IGNORECASE)

        def _split_lit(m):
            full = m.group(0)
            inner = m.group(1)
            if not _KW.search(inner):
                return full
            parts = _KW.split(inner)
            result = ""
            for i, part in enumerate(parts):
                text = part.strip()
                if _KW.fullmatch(text):
                    result += f" {text.upper()} "
                elif text:
                    result += f"'{text}'"
            return result.strip()

        return re.sub(r"'([^']*\b(?:ELSE|END|WHEN)\b[^']*)'", _split_lit, expr, flags=re.IGNORECASE)

    @classmethod
    def _fix_then_else_literals(cls, expr: str) -> str:
        """Quote bare text after THEN/ELSE that isn't already quoted or a number/column/keyword."""
        _BOUNDARY_KW = re.compile(r"\b(WHEN|ELSE|END|AND|OR|THEN)\b", re.IGNORECASE)

        def _replacer(m):
            kw = m.group(1)
            body = m.group(2).strip()
            if not body:
                return m.group(0)
            if body.startswith("'") or body.startswith('"'):
                return m.group(0)
            if re.match(r"^-?\d+(\.\d+)?$", body):
                return m.group(0)
            tokens = body.split()
            if len(tokens) == 1 and tokens[0].upper() in cls._SQL_RESERVED:
                return m.group(0)
            if len(tokens) == 1 and re.match(r"^[A-Za-z_]\w*$", tokens[0]):
                if tokens[0].upper() not in cls._SQL_RESERVED:
                    return f"{kw} '{body}'"
                return m.group(0)
            return f"{kw} '{body}'"

        return re.sub(
            r"\b(THEN|ELSE)\s+(.*?)(?=\s+(?:WHEN|ELSE|END)\b)",
            _replacer,
            expr,
            flags=re.IGNORECASE,
        )

    @classmethod
    def _fix_in_clause_literals(cls, expr: str) -> str:
        """Quote bare words inside IN (...) clauses."""

        def _fix_in_body(m):
            prefix, body = m.group(1), m.group(2)
            tokens = [t.strip() for t in body.split(",")]
            fixed = []
            for tok in tokens:
                if not tok or tok.startswith("'") or tok.startswith('"'):
                    fixed.append(tok)
                elif re.match(r"^-?\d+(\.\d+)?$", tok):
                    fixed.append(tok)
                elif tok.upper() in cls._SQL_RESERVED:
                    fixed.append(tok)
                else:
                    fixed.append(f"'{tok}'")
            return f"{prefix}{', '.join(fixed)})"

        return re.sub(r"(\bIN\s*\()([^)]+)\)", _fix_in_body, expr, flags=re.IGNORECASE)

    @classmethod
    def _fix_concat_separators(cls, expr: str) -> str:
        """Quote bare non-alphanumeric tokens between commas in function calls."""
        return re.sub(
            r",\s*([^\w\s'\"`(][^\w'\"`(]*?)\s*,",
            lambda m: f", '{m.group(1).strip()}',",
            expr,
        )

    @classmethod
    def _fix_date_part(cls, expr: str) -> str:
        """Rewrite DATE_PART(UNIT, col) to EXTRACT(UNIT FROM col)."""

        def _repl(m):
            unit = m.group(1).upper()
            col = m.group(2).strip()
            if unit in cls._DATE_TRUNC_INTERVALS:
                return f"EXTRACT({unit} FROM {col})"
            return m.group(0)

        return re.sub(
            r"DATE_PART\(\s*(['\"]?)(\w+)\1\s*,\s*(.+?)\)",
            lambda m: (
                f"EXTRACT({m.group(2).upper()} FROM {m.group(3).strip()})"
                if m.group(2).upper() in cls._DATE_TRUNC_INTERVALS
                else m.group(0)
            ),
            expr,
            flags=re.IGNORECASE,
        )

    _DATEDIFF_UNITS = {
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MILLISECOND",
        "MICROSECOND",
    }
    _PLURAL_UNITS = {
        "YEARS": "YEAR",
        "QUARTERS": "QUARTER",
        "MONTHS": "MONTH",
        "WEEKS": "WEEK",
        "DAYS": "DAY",
        "HOURS": "HOUR",
        "MINUTES": "MINUTE",
        "SECONDS": "SECOND",
    }

    @classmethod
    def _fix_datediff(cls, expr: str) -> str:
        """Rewrite DATEDIFF(UNIT, start, end) to TIMESTAMPDIFF(UNIT, start, end)."""

        def _repl(m):
            unit_raw = m.group(1).upper()
            unit = cls._PLURAL_UNITS.get(unit_raw, unit_raw)
            rest = m.group(2)
            if unit in cls._DATEDIFF_UNITS:
                return f"TIMESTAMPDIFF({unit}{rest}"
            return m.group(0)

        return re.sub(
            r"DATEDIFF\(\s*(['\"]?)(\w+)\1\s*(,\s*\w+.*?,\s*\w+.*?\))",
            lambda m: (
                f"TIMESTAMPDIFF({cls._PLURAL_UNITS.get(m.group(2).upper(), m.group(2).upper())}{m.group(3)}"
                if cls._PLURAL_UNITS.get(m.group(2).upper(), m.group(2).upper())
                in cls._DATEDIFF_UNITS
                else m.group(0)
            ),
            expr,
            flags=re.IGNORECASE,
        )

    @classmethod
    def _fix_like_patterns(cls, expr: str) -> str:
        """Quote bare LIKE/NOT LIKE patterns: ``col LIKE HW%`` -> ``col LIKE 'HW%'``."""

        def _repl(m):
            prefix = m.group(1)
            pat = m.group(2).strip()
            if pat.startswith("'") or pat.startswith('"'):
                return m.group(0)
            return f"{prefix}'{pat}'"

        return re.sub(
            r"(LIKE\s+)([^'\"\s(]+)",
            _repl,
            expr,
            flags=re.IGNORECASE,
        )

    @classmethod
    def _fix_position_bare_char(cls, expr: str) -> str:
        """Quote bare non-alnum char in POSITION(X IN ...)."""
        def _repl(m):
            ch = m.group(1).strip()
            if ch.startswith("'") or ch.startswith('"'):
                return m.group(0)
            return f"POSITION('{ch}' IN{m.group(2)}"
        return re.sub(r"POSITION\(\s*([^\w\s'\"]+)\s+(IN\b)", _repl, expr, flags=re.IGNORECASE)

    @classmethod
    def _fix_double_commas(cls, expr: str) -> str:
        """Collapse empty arguments: CONCAT(a, , b) -> CONCAT(a, b)."""
        while ", ," in expr:
            expr = expr.replace(", ,", ",")
        while ",," in expr:
            expr = expr.replace(",,", ",")
        return expr

    @classmethod
    def _fix_none_literal(cls, expr: str) -> str:
        """Replace Python None leaked into SQL with NULL."""
        return re.sub(r"\bNone\b", "NULL", expr)

    @classmethod
    def _fix_concat_bare_first_arg(cls, expr: str) -> str:
        """Quote bare short non-column first arg in CONCAT."""
        def _repl(m):
            fn = m.group(1)
            arg = m.group(2).strip()
            if arg.startswith("'") or arg.startswith('"'):
                return m.group(0)
            if "." in arg or arg.upper() in cls._SQL_RESERVED or re.match(r"^-?\d", arg):
                return m.group(0)
            if len(arg) <= 3 and arg.isalpha():
                return f"{fn}'{arg}',"
            return m.group(0)
        return re.sub(r"(CONCAT\(\s*)([^',\s]+)\s*,", _repl, expr, flags=re.IGNORECASE)

    @classmethod
    def _autofix_expr(cls, expr: str) -> str:
        """Fix common AI expression mistakes."""

        def _fix_date_trunc(m):
            interval = m.group(1)
            rest = m.group(2)
            if interval.upper() in cls._DATE_TRUNC_INTERVALS:
                return f"DATE_TRUNC('{interval}'{rest}"
            return m.group(0)

        expr = re.sub(
            r"DATE_TRUNC\(\s*([A-Za-z]+)(,)", _fix_date_trunc, expr, flags=re.IGNORECASE
        )
        expr = cls._fix_date_part(expr)
        expr = cls._fix_datediff(expr)
        expr = cls._fix_none_literal(expr)
        expr = cls._fix_double_commas(expr)
        expr = cls._fix_position_bare_char(expr)
        expr = cls._fix_concat_bare_first_arg(expr)
        expr = cls._fix_case_quoting(expr)
        expr = cls._fix_unquoted_literals(expr)
        expr = cls._fix_then_else_literals(expr)
        expr = cls._fix_in_clause_literals(expr)
        expr = cls._fix_concat_separators(expr)
        expr = cls._fix_like_patterns(expr)
        return expr

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
        parts = [f"{source} AS source"]
        for j in joins:
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
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Apply metric views
    # ------------------------------------------------------------------

    def apply_metric_views(self) -> Dict[str, Any]:
        """Read validated definitions and create metric views in UC."""
        fq = self.config.fq
        rows = self.spark.sql(
            f"SELECT definition_id, metric_view_name, source_table, json_definition "
            f"FROM {fq(self.config.definitions_table)} WHERE status = 'validated'"
        ).collect()

        applied = 0
        failed = 0
        for row in rows:
            defn = json.loads(row["json_definition"])
            mv_name = row["metric_view_name"]
            fq_mv = f"{self.config.catalog_name}.{self.config.schema_name}.{mv_name}"
            with _trace_span("apply_metric_view") as apply_span:
                if apply_span is not None:
                    apply_span.set_inputs({"metric_view_name": mv_name, "fq_mv": fq_mv})
                try:
                    yaml_body = self._definition_to_yaml(defn)
                    sql = f"CREATE OR REPLACE VIEW {fq_mv}\nWITH METRICS LANGUAGE YAML AS $$\n{yaml_body}$$"
                    self.spark.sql(sql)
                    self.spark.sql(
                        f"""
                        UPDATE {fq(self.config.definitions_table)}
                        SET status = 'applied', applied_at = current_timestamp()
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

        return {"applied": applied, "failed": failed}

    def _normalize_joins(self, defn: dict) -> None:
        """Normalize join 'on' to UC format: source.<fk_col> = <join_name>.<pk_col>. Mutates defn."""
        source = defn.get("source", "")
        if not source or not defn.get("joins"):
            return
        source_short = source.split(".")[-1]
        for j in defn["joins"]:
            on = j.get("on", "")
            if on and f"{source_short}." in on:
                j["on"] = on.replace(f"{source_short}.", "source.")

    def _definition_to_yaml(self, defn: dict) -> str:
        """Convert a JSON definition to the YAML body for CREATE VIEW WITH METRICS."""
        self._normalize_joins(defn)
        mv: dict = {"version": "1.1", "source": defn["source"]}
        if defn.get("comment"):
            mv["comment"] = defn["comment"]
        if defn.get("filter"):
            mv["filter"] = defn["filter"]
        mv["dimensions"] = [
            {
                k: v
                for k, v in {
                    "name": d["name"],
                    "expr": d["expr"],
                    "comment": d.get("comment"),
                    "display_name": d.get("display_name"),
                    "synonyms": d.get("synonyms"),
                }.items()
                if v
            }
            for d in defn.get("dimensions", [])
        ]
        measures_out = []
        for m in defn.get("measures", []):
            entry = {
                k: v
                for k, v in {
                    "name": m["name"],
                    "expr": m["expr"],
                    "comment": m.get("comment"),
                    "display_name": m.get("display_name"),
                    "synonyms": m.get("synonyms"),
                }.items()
                if v
            }
            if m.get("window"):
                entry["window"] = m["window"]
            measures_out.append(entry)
        mv["measures"] = measures_out
        if defn.get("joins"):
            mv["joins"] = defn["joins"]
        return yaml.dump(mv, default_flow_style=False, sort_keys=False)

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
        """Create a Genie space from applied metric views with rich instructions. Returns space_id or None."""
        fq = self.config.fq

        applied = self.spark.sql(
            f"SELECT metric_view_name, source_table, json_definition FROM {fq(self.config.definitions_table)} WHERE status = 'applied'"
        ).collect()
        if not applied:
            logger.warning("No applied metric views -- skipping Genie space creation")
            return None

        questions = self.spark.sql(
            f"SELECT question_text FROM {fq(self.config.questions_table)} WHERE status = 'processed'"
        ).collect()
        sample_qs = [r["question_text"] for r in questions if r.get("question_text")][:10]

        # Gather metadata for instructions
        tables_meta = self._safe_collect(
            f"SELECT table_name, comment, domain, subdomain FROM {fq('table_knowledge_base')}"
        )
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} WHERE final_confidence >= {self.config.fk_confidence_threshold}"
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
            identifier = f"{self.config.catalog_name}.{self.config.schema_name}.{mv_name}"
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

    Returns:
        Dict with generation statistics
    """
    config = SemanticLayerConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        model_endpoint=model_endpoint,
        fk_confidence_threshold=fk_confidence_threshold,
        validate_expressions=validate_expressions,
    )
    gen = SemanticLayerGenerator(spark, config)
    gen.create_tables()
    if questions:
        gen.ingest_questions(questions)
    return gen.generate_metric_views()

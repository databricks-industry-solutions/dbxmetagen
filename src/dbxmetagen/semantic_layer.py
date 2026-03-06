"""
Semantic Layer Generator module.

Uses AI_QUERY to auto-generate Unity Catalog metric view definitions from
user business questions and existing catalog metadata (knowledge bases,
ontology entities, FK predictions). Outputs JSON that is converted to YAML
for CREATE METRIC VIEW statements.
"""

import json
import logging
import re
import uuid
import yaml
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

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

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"


# -- Few-shot example for the AI prompt ----------------------------------

_FEW_SHOT_EXAMPLE = """\
INPUT tables:
  sales.orders (Comment: "Customer orders") columns: [order_id BIGINT, customer_id BIGINT, order_date DATE, total_amount DECIMAL(10,2), region STRING, status STRING, is_returned BOOLEAN]
  sales.customers (Comment: "Customer master") columns: [id BIGINT, name STRING, segment STRING, signup_date DATE]
  FK: orders.customer_id -> customers.id (confidence 0.95)

INPUT questions:
  1. What is total revenue by region?
  2. How many orders per month?
  3. What is the fulfillment rate by segment?

OUTPUT:
[
  {
    "name": "order_performance_metrics",
    "source": "sales.orders",
    "comment": "Order performance including revenue, fulfillment rates, and return analysis",
    "filter": "status IS NOT NULL",
    "dimensions": [
      {"name": "Order Month", "expr": "DATE_TRUNC('MONTH', order_date)", "comment": "Month of order placement"},
      {"name": "Order Quarter", "expr": "DATE_TRUNC('QUARTER', order_date)", "comment": "Quarter of order placement"},
      {"name": "Region", "expr": "region", "comment": "Sales region"},
      {"name": "Status", "expr": "status", "comment": "Order fulfillment status"},
      {"name": "Customer Segment", "expr": "segment", "comment": "Customer segment from joined customers table"},
      {"name": "Customer Tier", "expr": "CASE WHEN segment IN ('Enterprise', 'Strategic') THEN 'Top Tier' WHEN segment = 'Mid-Market' THEN 'Growth' ELSE 'Standard' END", "comment": "Customer tier grouping"}
    ],
    "measures": [
      {"name": "Total Revenue", "expr": "SUM(total_amount)", "comment": "Sum of all order values"},
      {"name": "Order Count", "expr": "COUNT(*)", "comment": "Number of orders"},
      {"name": "Avg Order Value", "expr": "AVG(total_amount)", "comment": "Average order amount"},
      {"name": "Unique Customers", "expr": "COUNT(DISTINCT customer_id)", "comment": "Distinct customer count"},
      {"name": "Revenue per Customer", "expr": "SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_id), 0)", "comment": "Average revenue per unique customer"},
      {"name": "Fulfillment Rate", "expr": "SUM(CASE WHEN status = 'fulfilled' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of orders fulfilled (0 to 1)"},
      {"name": "Return Rate", "expr": "SUM(CASE WHEN is_returned = TRUE THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)", "comment": "Fraction of orders returned (0 to 1)"},
      {"name": "Fulfilled Revenue", "expr": "SUM(total_amount) FILTER (WHERE status = 'fulfilled')", "comment": "Revenue from fulfilled orders only"},
      {"name": "High Value Order Count", "expr": "COUNT(*) FILTER (WHERE total_amount > 1000)", "comment": "Orders above 1000 threshold"}
    ],
    "joins": [
      {"name": "customers", "source": "sales.customers", "on": "customers.id = orders.customer_id"}
    ]
  }
]"""


class SemanticLayerGenerator:

    def __init__(self, spark: SparkSession, config: SemanticLayerConfig):
        self.spark = spark
        self.config = config

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

        # FK predictions (optional)
        fk_rows = self._safe_collect(
            f"SELECT src_table, dst_table, src_column, dst_column, final_confidence "
            f"FROM {fq('fk_predictions')} "
            f"WHERE is_fk = true AND final_confidence >= {self.config.fk_confidence_threshold}"
        )

        # Ontology entities (optional)
        ont_rows = self._safe_collect(
            f"SELECT entity_type, source_tables FROM {fq('ontology_entities')} WHERE confidence >= 0.4"
        )
        entity_map: dict[str, str] = {}
        for o in ont_rows:
            for t in o.get("source_tables") or []:
                entity_map[t] = o["entity_type"]

        # Assemble per-table context
        for t in tables:
            tname = t["table_name"]
            ent = entity_map.get(tname, "")
            ent_str = f" Entity: {ent}" if ent else ""
            line = f"Table: {tname} (Comment: \"{t.get('comment', '')}\" Domain: {t.get('domain', '')} / {t.get('subdomain', '')}){ent_str}"
            cols = col_by_table.get(tname, [])
            col_strs = [
                f"  - {c['column_name']} {c.get('data_type', '')} : {c.get('comment', '')}"
                for c in cols
            ]
            parts.append(
                line + "\n  Columns:\n" + "\n".join(col_strs) if col_strs else line
            )

        # FK relationships
        if fk_rows:
            parts.append("\nFOREIGN KEY RELATIONSHIPS:")
            for fk in fk_rows:
                parts.append(
                    f"  {fk['src_table']}.{fk['src_column']} -> {fk['dst_table']}.{fk['dst_column']} (confidence {fk['final_confidence']:.2f})"
                )

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

        context = self.build_context()
        prompt = self._build_prompt(questions, context)

        # Call AI_QUERY
        escaped = prompt.replace("'", "''")
        result = self.spark.sql(
            f"""
            SELECT AI_QUERY('{self.config.model_endpoint}', '{escaped}') as response
        """
        ).collect()[0]["response"]

        # Parse JSON response (extract JSON array from response)
        definitions = self._parse_ai_response(result)
        logger.info("AI returned %d metric view definitions", len(definitions))

        # Validate and store each independently
        now = datetime.utcnow().isoformat()
        q_id_str = ",".join(q_ids)
        stats = {"generated": 0, "validated": 0, "failed": 0}

        for defn in definitions:
            defn_id = str(uuid.uuid4())
            mv_name = defn.get("name", f"metric_view_{defn_id[:8]}")
            source = defn.get("source", "")
            json_str = json.dumps(defn).replace("'", "''")
            errors = self._validate_definition(defn)

            if self.config.validate_expressions and not errors:
                errors = self._validate_expressions(defn)

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

        # Update question status
        id_list = ", ".join(f"'{qid}'" for qid in q_ids)
        self.spark.sql(
            f"""
            UPDATE {fq(self.config.questions_table)}
            SET status = 'processed', processed_at = current_timestamp()
            WHERE question_id IN ({id_list})
        """
        )

        logger.info("Generation complete: %s", stats)
        return stats

    def _build_prompt(self, questions: List[str], context: str) -> str:
        q_block = "\n".join(f"  {i+1}. {q}" for i, q in enumerate(questions))
        return f"""You are a data modeler building a semantic layer for Databricks Unity Catalog.

TASK: Generate metric view definitions (as a JSON array) that enable answering the business questions below.

RULES:
1. ONE metric view per primary/fact table, not one per question
2. Only reference columns that exist in the metadata below
3. Use standard SQL aggregate functions: SUM, COUNT, AVG, MIN, MAX, COUNT(DISTINCT ...)
4. Use DATE_TRUNC for date dimensions with QUOTED intervals: DATE_TRUNC('MONTH', col), DATE_TRUNC('QUARTER', col)
5. ALWAYS single-quote ALL string literal values everywhere in expressions:
   - Comparisons: status = 'fulfilled', NOT status = fulfilled
   - THEN/ELSE results: CASE WHEN x = 'A' THEN 'Category A' ELSE 'Other' END, NOT THEN Category A
   - IN lists: department IN ('Surgery', 'Pediatrics'), NOT IN (Surgery, Pediatrics)
   - CONCAT separators: CONCAT(hospital, ' - ', department), NOT CONCAT(hospital, - , department)
   The ONLY unquoted tokens should be column names, SQL keywords, and numbers
6. Include joins ONLY when FK relationships exist in the metadata
7. Every metric view MUST have at least one measure and one dimension
8. Add a top-level "comment" describing the metric view's purpose
9. Add a "comment" to each dimension and measure explaining what it represents
10. Use "filter" (optional) for persistent WHERE clauses (e.g. excluding null/test rows)
11. Use measure-level FILTER for conditional aggregation: SUM(col) FILTER (WHERE condition)
12. If some questions are not answerable with metrics (e.g. document search, free-text lookups, SOP retrieval), generate metric views for the ones that ARE quantitative/analytical and silently ignore the rest
13. Output ONLY a valid JSON array, no explanation
14. When ONTOLOGY METRIC SUGGESTIONS are provided, use them as the primary source for measure definitions. Translate each suggestion's aggregation_type and source_field into a measure expr. If a suggestion has a filter_condition, use it as a FILTER clause
15. When Entity types are annotated on tables, generate entity-specific analytical metrics:
    - People/patients/users: include counts, return/readmission rates, segmentation dimensions, and per-entity averages
    - Transactions/events/encounters: include volume counts, value sums, time-based rates, and categorical breakdowns
    - Resources/staff/inventory: include utilization rates (active/total), efficiency ratios, and capacity measures
    Always include at least one RATIO measure (x / NULLIF(y, 0)) and one RATE measure (conditional_count * 1.0 / NULLIF(total, 0)) per metric view
16. When FOREIGN KEY RELATIONSHIPS exist between selected tables, generate cross-table metrics that join fact tables to dimension tables. Use dimension table columns as grouping dimensions and fact table columns as measures

EXAMPLE:
{_FEW_SHOT_EXAMPLE}

CATALOG METADATA:
{context}

BUSINESS QUESTIONS:
{q_block}

OUTPUT (JSON array only):"""

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

    def _validate_definition(self, defn: dict) -> list[str]:
        """Tier 1: structural validation against information_schema."""
        errors = []
        source = defn.get("source", "")
        if not source:
            errors.append("Missing source table")
            return errors

        # Check table exists
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

        # Collect join table columns too
        for j in defn.get("joins", []):
            j_source = j.get("source", "")
            j_parts = j_source.split(".")
            try:
                if len(j_parts) == 3:
                    j_rows = self.spark.sql(
                        f"""
                        SELECT column_name FROM {j_parts[0]}.information_schema.columns
                        WHERE table_catalog = '{j_parts[0]}' AND table_schema = '{j_parts[1]}' AND table_name = '{j_parts[2]}'
                    """
                    ).collect()
                    existing_cols.update(r["column_name"].lower() for r in j_rows)
            except Exception:
                pass

        # Check column references in dimensions and measures
        for item_type in ("dimensions", "measures"):
            for item in defn.get(item_type, []):
                expr = item.get("expr", "")
                col_refs = self._extract_column_refs(expr)
                for col in col_refs:
                    if col.lower() not in existing_cols:
                        errors.append(
                            f"{item_type} '{item.get('name', '')}': column '{col}' not found in {source}"
                        )

        return errors

    def _extract_column_refs(self, expr: str) -> list[str]:
        """Extract likely column references from a SQL expression."""
        # Remove string literals, function calls, and keywords
        cleaned = re.sub(r"'[^']*'", "", expr)
        cleaned = re.sub(r"\"[^\"]*\"", "", cleaned)
        tokens = re.findall(r"\b([a-zA-Z_]\w*)\b", cleaned)
        sql_keywords = {
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "DATE_TRUNC",
            "DISTINCT",
            "MONTH",
            "QUARTER",
            "YEAR",
            "WEEK",
            "DAY",
            "HOUR",
            "CAST",
            "AS",
            "STRING",
            "INT",
            "BIGINT",
            "DOUBLE",
            "FLOAT",
            "DECIMAL",
            "DATE",
            "TIMESTAMP",
            "BOOLEAN",
            "COALESCE",
            "IF",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "AND",
            "OR",
            "NOT",
            "NULL",
            "TRUE",
            "FALSE",
            "CONCAT",
            "UPPER",
            "LOWER",
            "TRIM",
        }
        return [t for t in tokens if t.upper() not in sql_keywords]

    # ------------------------------------------------------------------
    # Expression auto-fix helpers
    # ------------------------------------------------------------------

    _DATE_TRUNC_INTERVALS = {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"}
    _SQL_RESERVED = {
        "THEN", "ELSE", "END", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
        "CASE", "WHEN", "IN", "IS", "LIKE", "BETWEEN", "SELECT", "FROM",
        "WHERE", "FILTER", "DISTINCT", "SUM", "AVG", "COUNT", "MIN", "MAX",
        "DATE_TRUNC", "IF", "COALESCE", "NULLIF", "OVER", "PARTITION", "BY",
        "ORDER", "ASC", "DESC",
    }

    @classmethod
    def _fix_unquoted_literals(cls, expr: str) -> str:
        """Quote bare words used as string literals in comparisons."""
        def _replacer(m):
            op, word, trail = m.group(1), m.group(2), m.group(3)
            if word.upper() in cls._SQL_RESERVED or word.upper() in cls._DATE_TRUNC_INTERVALS:
                return m.group(0)
            return f"{op}'{word}'{trail}"

        return re.sub(
            r"([=!<>]+\s*)([A-Za-z_]\w*)(\s*(?:THEN|ELSE|END|AND|OR|WHEN|,|\))|$)",
            _replacer,
            expr,
            flags=re.IGNORECASE,
        )

    @classmethod
    def _fix_then_else_literals(cls, expr: str) -> str:
        """Quote multi-word bare text after THEN/ELSE keywords."""
        _BOUNDARY = {"WHEN", "THEN", "ELSE", "END", "AND", "OR", "CASE"}

        def _replacer(m):
            kw = m.group(1)
            body = m.group(2).strip()
            words = body.split()
            if len(words) < 2:
                if words and words[0].upper() not in cls._SQL_RESERVED and words[0].upper() not in _BOUNDARY:
                    return f"{kw} '{body}'"
                return m.group(0)
            return f"{kw} '{body}'"

        return re.sub(
            r"\b(THEN|ELSE)\s+((?:[A-Za-z_]\w*\s+)*[A-Za-z_]\w*)(?=\s+(?:WHEN|ELSE|END|AND|OR)\b|\s*$)",
            _replacer, expr, flags=re.IGNORECASE,
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
        expr = cls._fix_unquoted_literals(expr)
        expr = cls._fix_then_else_literals(expr)
        expr = cls._fix_in_clause_literals(expr)
        expr = cls._fix_concat_separators(expr)
        return expr

    @staticmethod
    def score_definition_complexity(defn: dict) -> dict:
        """Score a metric view definition's analytical richness.

        Returns {"complexity_score": int, "complexity_level": str}.
        """
        score = 0
        _COMPUTED = re.compile(r"CASE\b|DATE_TRUNC\b|CONCAT\b", re.IGNORECASE)
        for dim in defn.get("dimensions", []):
            if _COMPUTED.search(dim.get("expr", "")):
                score += 1
        for meas in defn.get("measures", []):
            expr = meas.get("expr", "")
            if re.search(r"/\s*NULLIF\b", expr, re.IGNORECASE):
                score += 2
            elif re.search(r"FILTER\b|DISTINCT\b|CASE\b", expr, re.IGNORECASE):
                score += 1
        if defn.get("joins"):
            score += 2
        if defn.get("filter"):
            score += 1
        level = "trivial" if score < 3 else ("moderate" if score <= 5 else "rich")
        return {"complexity_score": score, "complexity_level": level}

    def _validate_expressions(self, defn: dict) -> list[str]:
        """Tier 2: test each expression with LIMIT 0 query, auto-fixing when possible."""
        errors = []
        source = defn.get("source", "")
        if not source:
            return errors

        for item_type in ("dimensions", "measures"):
            for item in defn.get(item_type, []):
                expr = item.get("expr", "")
                name = item.get("name", "")
                fixed = self._autofix_expr(expr)
                if fixed != expr:
                    item["expr"] = fixed
                    expr = fixed
                try:
                    self.spark.sql(f"SELECT {expr} FROM {source} LIMIT 0")
                except Exception as e:
                    errors.append(f"{item_type} '{name}': expression error: {e}")
        return errors

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

        return {"applied": applied, "failed": failed}

    def _definition_to_yaml(self, defn: dict) -> str:
        """Convert a JSON definition to the YAML body for CREATE VIEW WITH METRICS."""
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
                }.items()
                if v
            }
            for d in defn.get("dimensions", [])
        ]
        mv["measures"] = [
            {
                k: v
                for k, v in {
                    "name": m["name"],
                    "expr": m["expr"],
                    "comment": m.get("comment"),
                }.items()
                if v
            }
            for m in defn.get("measures", [])
        ]
        if defn.get("joins"):
            mv["joins"] = defn["joins"]
        return yaml.dump(mv, default_flow_style=False, sort_keys=False)

    # ------------------------------------------------------------------
    # Genie space creation
    # ------------------------------------------------------------------

    def create_genie_space(self, display_name: str, warehouse_id: str) -> Optional[str]:
        """Create a Genie space from applied metric views. Returns space_id or None."""
        fq = self.config.fq

        # Gather source tables and questions
        applied = self.spark.sql(
            f"SELECT DISTINCT source_table FROM {fq(self.config.definitions_table)} WHERE status = 'applied'"
        ).collect()
        table_ids = [r["source_table"] for r in applied if r["source_table"]]

        questions = self.spark.sql(
            f"SELECT question_text FROM {fq(self.config.questions_table)} WHERE status = 'processed'"
        ).collect()
        sample_qs = [r["question_text"] for r in questions][:20]

        if not table_ids:
            logger.warning("No applied metric views -- skipping Genie space creation")
            return None

        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            # Use REST API for Genie space creation (more portable across SDK versions)
            payload = {
                "title": display_name,
                "warehouse_id": warehouse_id,
                "table_identifiers": table_ids,
                "description": f"Auto-generated by dbxmetagen from {len(table_ids)} tables",
            }
            if sample_qs:
                payload["sample_questions"] = sample_qs[:10]
            resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
            space_id = resp.get("space_id", resp.get("id", ""))
            logger.info("Created Genie space %s (%s)", display_name, space_id)

            # Update definitions with genie_space_id
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

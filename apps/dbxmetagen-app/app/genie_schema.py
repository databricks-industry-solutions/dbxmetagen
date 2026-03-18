"""Pydantic models for the Databricks Genie API serialized_space proto schema.

Provides whitelist-only construction: only known-good fields reach the API.
"""

import re
import uuid
from typing import Optional

from pydantic import BaseModel, Field


def _hex_id() -> str:
    return uuid.uuid4().hex


def _simplify_join_sql(sql: str) -> str:
    """Reduce catalog.schema.table.column references to table.column in join SQL."""
    return re.sub(
        r"\b(\w+)\.(\w+)\.(\w+)\.(\w+)\b",
        lambda m: f"{m.group(3)}.{m.group(4)}",
        sql,
    )


_DATE_UNITS = {
    "YEARS": "YEAR",
    "QUARTERS": "QUARTER",
    "MONTHS": "MONTH",
    "WEEKS": "WEEK",
    "DAYS": "DAY",
    "HOURS": "HOUR",
    "MINUTES": "MINUTE",
    "SECONDS": "SECOND",
    "MILLISECONDS": "MILLISECOND",
    "MICROSECONDS": "MICROSECOND",
}
_VALID_UNITS = {
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "DAYOFYEAR",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MILLISECOND",
    "MICROSECOND",
}


def _fix_date_func_units(sql: str) -> str:
    """Fix TIMESTAMPADD/TIMESTAMPDIFF unit arguments: strip quotes, de-pluralize."""

    def _fix_unit(m):
        func = m.group(1)
        quote = m.group(2) or ""
        unit = m.group(3).upper()
        rest = m.group(4)
        unit = _DATE_UNITS.get(unit, unit)
        return f"{func}({unit}{rest}"

    return re.sub(
        r"(TIMESTAMPADD|TIMESTAMPDIFF)\(\s*(['\"]?)(\w+)\2\s*(,)",
        _fix_unit,
        sql,
        flags=re.IGNORECASE,
    )


# ---------------------------------------------------------------------------
# Pydantic models matching the Genie API proto
# ---------------------------------------------------------------------------


class DataSourceRef(BaseModel):
    identifier: str
    description: Optional[list[str]] = None


class DataSources(BaseModel):
    tables: list[DataSourceRef] = []
    metric_views: list[DataSourceRef] = []


class TextInstruction(BaseModel):
    id: str = Field(default_factory=_hex_id)
    content: list[str]


class ExampleSQL(BaseModel):
    id: str = Field(default_factory=_hex_id)
    question: list[str]
    sql: list[str]


class JoinSide(BaseModel):
    identifier: str


class JoinSpec(BaseModel):
    id: str = Field(default_factory=_hex_id)
    left: JoinSide
    right: JoinSide
    sql: list[str]


class SnippetMeasure(BaseModel):
    id: str = Field(default_factory=_hex_id)
    alias: str
    sql: list[str]
    display_name: Optional[str] = None
    synonyms: Optional[list[str]] = None
    description: Optional[str] = Field(default=None, exclude=True)


class SnippetFilter(BaseModel):
    id: str = Field(default_factory=_hex_id)
    display_name: str
    sql: list[str]


class SnippetExpression(BaseModel):
    id: str = Field(default_factory=_hex_id)
    alias: str
    sql: list[str]
    display_name: Optional[str] = None
    synonyms: Optional[list[str]] = None
    description: Optional[str] = Field(default=None, exclude=True)


class SqlSnippets(BaseModel):
    measures: list[SnippetMeasure] = []
    filters: list[SnippetFilter] = []
    expressions: list[SnippetExpression] = []


class Instructions(BaseModel):
    text_instructions: list[TextInstruction] = []
    example_question_sqls: list[ExampleSQL] = []
    join_specs: list[JoinSpec] = []
    sql_snippets: Optional[SqlSnippets] = None


class SampleQuestion(BaseModel):
    id: str = Field(default_factory=_hex_id)
    question: list[str]


class GenieConfig(BaseModel):
    sample_questions: list[SampleQuestion] = []


class SerializedSpace(BaseModel):
    version: int = 2
    data_sources: DataSources
    instructions: Instructions = Instructions()
    config: Optional[GenieConfig] = None


# ---------------------------------------------------------------------------
# Whitelist-only construction from raw agent/builder output
# ---------------------------------------------------------------------------


def _to_str_list(val) -> Optional[list[str]]:
    """Normalize a value to list[str] or None."""
    if isinstance(val, list):
        return [str(v) for v in val if v]
    if isinstance(val, str) and val:
        return [val]
    return None


def _ensure_list(val) -> list[str]:
    if isinstance(val, list):
        return [str(v) for v in val if v]
    if isinstance(val, str) and val:
        return [val]
    return []


def _name_from_sql(sql_list: list[str]) -> str:
    """Derive a display name from a SQL expression when the alias is empty."""
    if not sql_list:
        return ""
    raw = sql_list[0].strip()
    raw = re.sub(r"[`'\"]", "", raw)
    if len(raw) > 60:
        raw = raw[:57] + "..."
    return raw


def _split_text_into_instructions(text: str) -> list[TextInstruction]:
    """Split a markdown string into separate TextInstruction blocks by ## headers."""
    if not text or not isinstance(text, str):
        return []
    sections: list[tuple[str, str]] = []
    current_header = ""
    current_lines: list[str] = []
    for line in text.split("\n"):
        if line.startswith("## "):
            if current_lines:
                body = "\n".join(current_lines).strip()
                if body:
                    sections.append((current_header, body))
            current_header = line.lstrip("# ").strip()
            current_lines = []
        else:
            current_lines.append(line)
    if current_lines:
        body = "\n".join(current_lines).strip()
        if body:
            sections.append((current_header, body))
    if not sections:
        return [TextInstruction(content=[text])]
    if len(sections) == 1 and not sections[0][0]:
        return [TextInstruction(content=[sections[0][1]])]
    result = []
    for header, body in sections:
        content = f"## {header}\n{body}" if header else body
        result.append(TextInstruction(content=[content]))
    return result


def _extract_table_refs_from_sql(sql: str) -> set[str]:
    """Extract fully-qualified or short table references from a SQL string."""
    refs: set[str] = set()
    for m in re.finditer(r"\bFROM\s+(`?[\w.]+`?)", sql, re.IGNORECASE):
        refs.add(m.group(1).replace("`", ""))
    for m in re.finditer(r"\bJOIN\s+(`?[\w.]+`?)", sql, re.IGNORECASE):
        refs.add(m.group(1).replace("`", ""))
    return refs


def _dedup_join_specs(joins: list[JoinSpec]) -> list[JoinSpec]:
    """Remove duplicate join specs (same left+right pair)."""
    seen: set[tuple[str, str]] = set()
    deduped: list[JoinSpec] = []
    for j in joins:
        key = (j.left.identifier, j.right.identifier)
        rev_key = (j.right.identifier, j.left.identifier)
        if key not in seen and rev_key not in seen:
            seen.add(key)
            deduped.append(j)
    return deduped


def build_serialized_space(raw: dict) -> dict:
    """Construct a clean serialized_space dict from raw agent output.

    Only known-good fields survive. Everything else is silently dropped.
    """
    # -- data_sources --
    ds_raw = raw.get("data_sources", {})
    tables = [
        DataSourceRef(
            identifier=t["identifier"], description=_to_str_list(t.get("description"))
        )
        for t in ds_raw.get("tables", [])
        if t.get("identifier")
    ]
    mvs = [
        DataSourceRef(
            identifier=m["identifier"], description=_to_str_list(m.get("description"))
        )
        for m in ds_raw.get("metric_views", [])
        if m.get("identifier")
    ]

    # -- instructions --
    inst_raw = raw.get("instructions", {})

    # text_instructions
    text = inst_raw.get("text") or inst_raw.get("text_instructions") or ""
    text_insts = []
    if isinstance(text, str) and text:
        text_insts = [TextInstruction(content=[text])]
    elif isinstance(text, list):
        all_parts = []
        for t in text:
            if isinstance(t, dict) and t.get("content"):
                all_parts.extend(_ensure_list(t["content"]))
            elif isinstance(t, str) and t:
                all_parts.append(t)
        if all_parts:
            text_insts = [TextInstruction(content=["\n\n".join(all_parts)])]

    # example_sql / example_question_sqls
    examples_raw = (
        inst_raw.get("example_sql") or inst_raw.get("example_question_sqls") or []
    )
    examples = []
    for ex in examples_raw:
        q = _ensure_list(ex.get("question"))
        s = [_fix_date_func_units(x) for x in _ensure_list(ex.get("sql"))]
        if q and s:
            examples.append(ExampleSQL(question=q, sql=s))

    # join_specs -- split compound AND, simplify FQ names
    joins_raw = inst_raw.get("join_specs") or []
    joins = []
    for j in joins_raw:
        if isinstance(j.get("left"), dict):
            left_id = j["left"].get("identifier", "")
        else:
            left_id = j.get("left_table", "")
        if isinstance(j.get("right"), dict):
            right_id = j["right"].get("identifier", "")
        else:
            right_id = j.get("right_table", "")

        sql_list = j.get("sql", [])
        if isinstance(j.get("join_condition"), str) and j["join_condition"]:
            sql_list = [j["join_condition"]]
        sql_list = _ensure_list(sql_list)

        split: list[str] = []
        for s in sql_list:
            s = _simplify_join_sql(s)
            split.extend(
                p.strip()
                for p in re.split(r"\s+AND\s+", s, flags=re.IGNORECASE)
                if p.strip()
            )

        if left_id and right_id and split:
            joins.append(
                JoinSpec(
                    left=JoinSide(identifier=left_id),
                    right=JoinSide(identifier=right_id),
                    sql=split,
                )
            )
    joins = _dedup_join_specs(joins)

    # sql_snippets -- apply date-function autofix on all SQL
    snip_raw = inst_raw.get("sql_snippets") or raw.get("sql_snippets") or {}

    def _fix_sql_list(sl: list[str]) -> list[str]:
        return [_fix_date_func_units(s) for s in sl]

    snip_measures = []
    for m in snip_raw.get("measures", []):
        sql = _fix_sql_list(_ensure_list(m.get("sql")))
        if not sql:
            continue
        alias = m.get("alias") or m.get("name") or _name_from_sql(sql)
        if not alias:
            continue
        snip_measures.append(SnippetMeasure(
            alias=alias, sql=sql,
            display_name=m.get("display_name") or alias,
            synonyms=m.get("synonyms"),
            description=m.get("description"),
        ))

    snip_filters = []
    for m in snip_raw.get("filters", []):
        sql = _fix_sql_list(_ensure_list(m.get("sql")))
        if not sql:
            continue
        dn = (
            m.get("display_name")
            or m.get("name")
            or m.get("description")
            or _name_from_sql(sql)
        )
        if not dn:
            continue
        snip_filters.append(SnippetFilter(display_name=dn, sql=sql))

    snip_exprs = []
    for m in snip_raw.get("expressions", []):
        sql = _fix_sql_list(_ensure_list(m.get("sql")))
        if not sql:
            continue
        alias = m.get("alias") or m.get("name") or _name_from_sql(sql)
        if not alias:
            continue
        snip_exprs.append(SnippetExpression(
            alias=alias, sql=sql,
            display_name=m.get("display_name") or alias,
            synonyms=m.get("synonyms"),
            description=m.get("description"),
        ))
    snippets = None
    if snip_measures or snip_filters or snip_exprs:
        snippets = SqlSnippets(
            measures=sorted(snip_measures, key=lambda x: x.id),
            filters=sorted(snip_filters, key=lambda x: x.id),
            expressions=sorted(snip_exprs, key=lambda x: x.id),
        )

    # -- sample_questions --
    sq_raw = raw.get("sample_questions") or []
    sample_qs = []
    for q in sq_raw:
        if isinstance(q, str) and q:
            sample_qs.append(SampleQuestion(question=[q]))
        elif isinstance(q, dict):
            qval = _ensure_list(q.get("question"))
            if qval:
                sample_qs.append(SampleQuestion(question=qval))

    # -- assemble and sort --
    space = SerializedSpace(
        data_sources=DataSources(
            tables=sorted(tables, key=lambda x: x.identifier),
            metric_views=sorted(mvs, key=lambda x: x.identifier),
        ),
        instructions=Instructions(
            text_instructions=sorted(text_insts, key=lambda x: x.id),
            example_question_sqls=sorted(examples, key=lambda x: x.id),
            join_specs=sorted(joins, key=lambda x: x.id),
            sql_snippets=snippets,
        ),
        config=(
            GenieConfig(sample_questions=sorted(sample_qs, key=lambda x: x.id))
            if sample_qs
            else None
        ),
    )
    return space.model_dump(exclude_none=True)

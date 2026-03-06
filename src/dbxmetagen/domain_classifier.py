"""
Domain classifier for dbxmetagen.

Supports two modes:
- Legacy single-shot: one LLM call picks domain + subdomain simultaneously.
- Two-stage (default): keyword pre-filter -> domain LLM call -> subdomain LLM call.
  This produces smaller prompts and uses the full subdomain config (descriptions + keywords).
"""

import os
import re
import json
import yaml
import logging
from typing import Dict, Any, Optional, List
from databricks_langchain import ChatDatabricks
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TableClassification(BaseModel):
    """Legacy single-shot response schema (kept for backward compat)."""

    catalog: str = Field(description="Catalog name")
    schema_name: str = Field(description="Schema name", alias="schema")
    table: str = Field(description="Table name")
    domain: str = Field(description="Primary business domain classification")
    subdomain: str = Field(description="Subdomain classification")
    confidence: float = Field(
        description="Confidence score between 0.0 and 1.0", ge=0.0, le=1.0
    )
    recommended_domain: Optional[str] = Field(
        default=None,
        description="Recommended domain to use if the table does not fit into any of the domains",
    )
    recommended_subdomain: Optional[str] = Field(
        default=None,
        description="Recommended subdomain to use if the table does not fit into any of the subdomains",
    )
    reasoning: str = Field(description="Detailed reasoning for the classification")
    metadata_summary: Optional[str] = Field(
        default=None, description="Summary of key table metadata"
    )

    class Config:
        populate_by_name = True


class DomainResult(BaseModel):
    """Stage-1 response: domain only."""

    domain: str = Field(description="Primary business domain key from the provided list")
    confidence: float = Field(
        description="Confidence score between 0.0 and 1.0", ge=0.0, le=1.0
    )
    reasoning: str = Field(description="Brief reasoning for the domain choice")


class SubdomainResult(BaseModel):
    """Stage-2 response: subdomain within a known domain."""

    subdomain: str = Field(description="Subdomain key from the provided list")
    confidence: float = Field(
        description="Confidence score between 0.0 and 1.0", ge=0.0, le=1.0
    )
    recommended_subdomain: Optional[str] = Field(
        default=None,
        description="Suggested subdomain if none of the provided options fit",
    )
    reasoning: str = Field(description="Detailed reasoning for the subdomain choice")
    metadata_summary: Optional[str] = Field(
        default=None, description="Summary of key metadata factors"
    )


def load_domain_config(config_path: str = None, bundle_path: str = None) -> Dict[str, Any]:
    """Load domain configuration from YAML file or ontology bundle.

    When *bundle_path* is provided, the ``domains`` section is extracted
    from the bundle YAML.  Otherwise falls back to *config_path* or the
    default ``configurations/domain_config.yaml``.
    """
    if bundle_path:
        resolved = _resolve_path(bundle_path)
        if resolved:
            try:
                with open(resolved, "r") as f:
                    bundle = yaml.safe_load(f)
                if bundle and "domains" in bundle:
                    logger.info("Loaded domain config from bundle %s", resolved)
                    return {"version": bundle.get("metadata", {}).get("version", "1.0"),
                            "domains": bundle["domains"]}
            except Exception as e:
                logger.warning("Could not extract domains from bundle %s: %s", resolved, e)

    if config_path:
        resolved = _resolve_path(config_path)
        if resolved:
            try:
                with open(resolved, "r") as f:
                    config = yaml.safe_load(f)
                logger.info("Loaded domain config from %s", resolved)
                return config
            except Exception as e:
                logger.warning("Could not load domain config from %s: %s", resolved, e)

    for path in [
        "configurations/domain_config.yaml",
        "../configurations/domain_config.yaml",
        "../../configurations/domain_config.yaml",
    ]:
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    config = yaml.safe_load(f)
                logger.info("Loaded domain config from %s", path)
                return config
        except Exception as e:
            logger.warning("Could not load domain config from %s: %s", path, e)

    logger.warning("Could not load domain config from any path, using fallback")
    return {
        "domains": {"unknown": {"name": "Unknown", "keywords": [], "subdomains": {}}}
    }


def _resolve_path(path: str) -> Optional[str]:
    """Return the first existing path variant, or None."""
    candidates = [
        path,
        os.path.join("..", path),
        os.path.join(os.path.dirname(__file__), "..", "..", path),
    ]
    try:
        candidates.append(os.path.join(os.getcwd(), path))
    except Exception:
        pass
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


# ---------------------------------------------------------------------------
# Keyword pre-filter (no LLM)
# ---------------------------------------------------------------------------

_SPLIT_RE = re.compile(r"[^a-z0-9]+")


def _tokenize(text: str) -> set:
    """Lowercase tokenize a string into a set of words."""
    return set(_SPLIT_RE.split(text.lower())) - {""}


def keyword_prefilter(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    top_n: int = 5,
) -> List[str]:
    """Score each domain by keyword hits and return the top N domain keys.

    Tokenises the table name, column names, comments, and tags into a bag of
    words, then counts how many of each domain's keywords appear.  Returns all
    domains if no keywords match at all.
    """
    domains = domain_config.get("domains", {})
    if not domains:
        return []

    bag = _tokenize(table_name)
    col_contents = table_metadata.get("column_contents", {})
    if isinstance(col_contents, dict):
        for key in col_contents:
            bag |= _tokenize(str(key))
    for field in ("table_tags", "table_comments", "table_constraints"):
        val = table_metadata.get(field, "")
        if val:
            bag |= _tokenize(str(val))

    scores: Dict[str, int] = {}
    for domain_key, domain_info in domains.items():
        kws = set(k.lower() for k in domain_info.get("keywords", []))
        # also count subdomain keywords
        for sd_info in domain_info.get("subdomains", {}).values():
            kws |= set(k.lower() for k in sd_info.get("keywords", []))
        scores[domain_key] = len(bag & kws)

    ranked = sorted(scores, key=lambda k: scores[k], reverse=True)
    if top_n <= 0 or top_n >= len(ranked):
        return ranked
    return ranked[:top_n]


# ---------------------------------------------------------------------------
# Prompt generators
# ---------------------------------------------------------------------------

def generate_domain_prompt_section(domain_config: Dict[str, Any]) -> str:
    """Generate the domain section for the legacy single-shot system prompt."""
    domains = domain_config.get("domains", {})
    if not domains:
        return "Available domains include: unknown"

    domain_names = list(domains.keys())
    domain_list = ", ".join(domain_names)

    domain_descriptions = []
    for domain_key, domain_info in domains.items():
        name = domain_info.get("name", domain_key.title())
        description = domain_info.get("description", "")
        keywords = domain_info.get("keywords", [])
        subdomains = domain_info.get("subdomains", {})

        keyword_text = f" Keywords: {', '.join(keywords[:10])}" if keywords else ""
        subdomain_names = list(subdomains.keys()) if subdomains else []
        subdomain_text = (
            f" (subdomains: {', '.join(subdomain_names)})" if subdomain_names else ""
        )
        domain_descriptions.append(
            f"- **{domain_key}** ({name}): {description}{subdomain_text}{keyword_text}"
        )

    domain_details = "\n".join(domain_descriptions)
    return f"""Available domains include: {domain_list}

Domain Details:
{domain_details}

For subdomains, use the specific subdomain keys from the configuration above, or create descriptive terms that may include spaces if not found in the predefined list."""


def generate_domain_only_prompt(
    domain_config: Dict[str, Any],
    candidate_domains: List[str],
) -> str:
    """Stage-1 system prompt: domain-level only, with full keywords, for candidates only."""
    domains = domain_config.get("domains", {})
    entries = []
    for dk in candidate_domains:
        info = domains.get(dk, {})
        name = info.get("name", dk.title())
        desc = info.get("description", "")
        kws = info.get("keywords", [])
        kw_text = f"  Keywords: {', '.join(kws)}" if kws else ""
        entries.append(f"- **{dk}** ({name}): {desc}\n{kw_text}")

    domain_block = "\n".join(entries)
    domain_keys = ", ".join(candidate_domains)

    return f"""You are a Domain Classification Agent. Given table metadata, pick the single best business domain.

Choose ONLY from these domain keys: {domain_keys}

{domain_block}

Respond with the domain key, a confidence score (0.0-1.0), and brief reasoning.
Lower your confidence if the table doesn't clearly fit any listed domain."""


def generate_subdomain_prompt(
    domain_config: Dict[str, Any],
    domain_key: str,
    include_secondary: Optional[str] = None,
) -> str:
    """Stage-2 system prompt: subdomain-level with full descriptions and keywords."""
    domains = domain_config.get("domains", {})
    domain_info = domains.get(domain_key, {})
    subdomains = domain_info.get("subdomains", {})

    entries = []
    for sdk, sd_info in subdomains.items():
        sd_name = sd_info.get("name", sdk.title())
        sd_desc = sd_info.get("description", "")
        sd_kws = sd_info.get("keywords", [])
        kw_text = f"  Keywords: {', '.join(sd_kws)}" if sd_kws else ""
        entries.append(f"- **{sdk}** ({sd_name}): {sd_desc}\n{kw_text}")

    if include_secondary:
        sec_info = domains.get(include_secondary, {})
        sec_subs = sec_info.get("subdomains", {})
        for sdk, sd_info in sec_subs.items():
            sd_name = sd_info.get("name", sdk.title())
            sd_desc = sd_info.get("description", "")
            sd_kws = sd_info.get("keywords", [])
            kw_text = f"  Keywords: {', '.join(sd_kws)}" if sd_kws else ""
            entries.append(f"- **{sdk}** ({sd_name}) [from {include_secondary}]: {sd_desc}\n{kw_text}")

    subdomain_block = "\n".join(entries) if entries else "(no predefined subdomains)"
    all_keys = list(subdomains.keys())
    if include_secondary:
        all_keys += list(domains.get(include_secondary, {}).get("subdomains", {}).keys())

    return f"""You are a Subdomain Classification Agent. The table has been classified into domain **{domain_key}**.
Now pick the most specific subdomain.

Choose from these subdomain keys: {', '.join(all_keys) if all_keys else 'none defined'}

{subdomain_block}

If none of the subdomains fit, set subdomain to the best match and suggest a better one in recommended_subdomain.
Provide a confidence score (0.0-1.0), detailed reasoning, and a brief metadata summary."""


# ---------------------------------------------------------------------------
# Legacy single-shot system prompt
# ---------------------------------------------------------------------------

def create_system_prompt(domain_config: Dict[str, Any]) -> str:
    """Create the legacy single-shot system prompt with all domains + subdomains."""
    domain_section = generate_domain_prompt_section(domain_config)

    return f"""You are a Table Classification Agent that helps classify database tables into business domains.

Your task is to analyze table metadata and classify tables into appropriate business domains. 

You will be given a domain configuration that you will use to classify the table. 
Only provide the domain and subdomain keys from the configuration. Do not make up any domains or subdomains.

Reduce your confidence score if you are not sure about the classification or you think that the table does not fit into any of the domains.

The metadata provided includes:
- Table name (catalog.schema.table format)
- Column names and data types
- Sample data from the table
- Extended metadata (statistics, constraints, tags, etc.)

{domain_section}

Based on the metadata provided, classify the table into the most appropriate domain.

Always provide structured responses with:
- Domain classification (use the domain keys from the configuration above)
- Confidence score (0.0 to 1.0)
- Subdomain (use subdomain keys from configuration)
- Detailed reasoning explaining your classification
- Metadata summary highlighting key factors in your decision

Consider:
1. Table and column names for domain hints
2. Data types and patterns in the data and metadata
3. Keywords from the domain configuration
4. Overall purpose and business context of the table

You must return a structured response matching the TableClassification schema exactly.

Be thorough, accurate, and provide detailed explanations for your classifications.
"""


# ---------------------------------------------------------------------------
# Shared helper: build the user message from table metadata
# ---------------------------------------------------------------------------

def _enforce_value(
    predicted: str, allowed: List[str], fallback: str = "unknown",
) -> tuple:
    """Snap a predicted value to the nearest allowed value.

    Returns ``(value, was_exact_match)``.
    """
    low = predicted.lower().strip()
    allowed_map = {a.lower(): a for a in allowed}
    if low in allowed_map:
        return allowed_map[low], True
    for a_low, a_orig in allowed_map.items():
        if a_low in low or low in a_low:
            return a_orig, False
    return fallback, False


def _build_user_message(table_name: str, table_metadata: Dict[str, Any]) -> str:
    msg = f"""Please classify the following table:

Table: {table_name}

Column Information:
{json.dumps(table_metadata.get('column_contents', {}), indent=2)}
"""
    if table_metadata.get("table_tags"):
        msg += f"\nTable Tags:\n{table_metadata['table_tags']}\n"
    if table_metadata.get("table_constraints"):
        msg += f"\nTable Constraints:\n{table_metadata['table_constraints']}\n"
    if table_metadata.get("table_comments"):
        msg += f"\nExisting Table Comment:\n{table_metadata['table_comments']}\n"
    if table_metadata.get("column_metadata"):
        msg += f"\nColumn Metadata:\n{json.dumps(table_metadata['column_metadata'], indent=2)}\n"
    return msg


def _error_result(table_name: str, error: Exception) -> Dict[str, Any]:
    catalog, schema, table = table_name.split(".")
    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "domain": "unknown",
        "subdomain": None,
        "confidence": 0.0,
        "reasoning": f"Classification failed: {str(error)}",
        "metadata_summary": "Error during classification",
    }


# ---------------------------------------------------------------------------
# Stage-1: Domain classification
# ---------------------------------------------------------------------------

def classify_domain_stage1(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    candidate_domains: List[str],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 2048,
) -> Dict[str, Any]:
    """Call the LLM to pick a domain from the candidate list."""
    llm = ChatDatabricks(
        endpoint=model_endpoint, temperature=temperature, max_tokens=max_tokens
    )
    structured_llm = llm.with_structured_output(DomainResult)
    system_prompt = generate_domain_only_prompt(domain_config, candidate_domains)
    user_message = _build_user_message(table_name, table_metadata)

    response = structured_llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ])
    return response.dict()


# ---------------------------------------------------------------------------
# Stage-2: Subdomain classification
# ---------------------------------------------------------------------------

def classify_subdomain_stage2(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    domain_key: str,
    domain_confidence: float,
    confidence_threshold: float = 0.5,
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 4096,
) -> Dict[str, Any]:
    """Call the LLM to pick a subdomain within the chosen domain."""
    include_secondary = None
    if domain_confidence < confidence_threshold:
        domains = domain_config.get("domains", {})
        others = [k for k in domains if k != domain_key]
        if others:
            include_secondary = others[0]
            logger.info(
                "Low domain confidence (%.2f); expanding stage-2 to include %s",
                domain_confidence, include_secondary,
            )

    llm = ChatDatabricks(
        endpoint=model_endpoint, temperature=temperature, max_tokens=max_tokens
    )
    structured_llm = llm.with_structured_output(SubdomainResult)
    system_prompt = generate_subdomain_prompt(
        domain_config, domain_key, include_secondary=include_secondary,
    )
    user_message = _build_user_message(table_name, table_metadata)

    response = structured_llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ])
    return response.dict()


# ---------------------------------------------------------------------------
# Legacy single-shot classification
# ---------------------------------------------------------------------------

def _classify_single_shot(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 8192,
) -> Dict[str, Any]:
    """Original single-call classification (domain + subdomain in one shot)."""
    llm = ChatDatabricks(
        endpoint=model_endpoint, temperature=temperature, max_tokens=max_tokens
    )
    structured_llm = llm.with_structured_output(TableClassification)
    system_prompt = create_system_prompt(domain_config)
    catalog, schema, table = table_name.split(".")
    user_message = _build_user_message(table_name, table_metadata)

    response = structured_llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ])
    result = response.dict()
    result["catalog"] = catalog
    result["schema"] = schema
    result["table"] = table

    # Enforce domain value
    domains = domain_config.get("domains", {})
    enforced_domain, exact = _enforce_value(result["domain"], list(domains.keys()))
    if not exact:
        logger.warning("Snapped domain '%s' -> '%s' for %s", result["domain"], enforced_domain, table_name)
        result["confidence"] = max(0.0, result["confidence"] - 0.1)
    result["domain"] = enforced_domain

    # Enforce subdomain value
    sd_keys = list(domains.get(enforced_domain, {}).get("subdomains", {}).keys())
    if sd_keys and result.get("subdomain"):
        enforced_sd, sd_exact = _enforce_value(result["subdomain"], sd_keys)
        if not sd_exact:
            logger.warning("Snapped subdomain '%s' -> '%s' for %s", result["subdomain"], enforced_sd, table_name)
            result["confidence"] = max(0.0, result["confidence"] - 0.1)
        result["subdomain"] = enforced_sd

    return result


# ---------------------------------------------------------------------------
# Two-stage classification (prefilter -> domain -> subdomain)
# ---------------------------------------------------------------------------

def _classify_two_stage(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 8192,
    prefilter_top_n: int = 5,
    confidence_threshold: float = 0.5,
) -> Dict[str, Any]:
    """Two-stage pipeline: keyword pre-filter -> domain LLM -> subdomain LLM."""
    catalog, schema, table = table_name.split(".")

    # Pre-filter
    candidates = keyword_prefilter(
        table_name, table_metadata, domain_config, top_n=prefilter_top_n,
    )
    if not candidates:
        candidates = list(domain_config.get("domains", {}).keys())
    logger.info("Stage-1 candidates for %s: %s", table_name, candidates)

    # Stage 1
    domains = domain_config.get("domains", {})
    s1 = classify_domain_stage1(
        table_name, table_metadata, domain_config, candidates,
        model_endpoint=model_endpoint, temperature=temperature,
        max_tokens=min(max_tokens, 2048),
    )

    # Enforce domain value against config
    enforced_domain, exact = _enforce_value(s1["domain"], list(domains.keys()))
    if not exact:
        logger.warning("Snapped stage-1 domain '%s' -> '%s' for %s", s1["domain"], enforced_domain, table_name)
        s1["confidence"] = max(0.0, s1["confidence"] - 0.1)
    s1["domain"] = enforced_domain

    logger.info(
        "Stage-1 result for %s: domain=%s confidence=%.2f",
        table_name, s1["domain"], s1["confidence"],
    )

    # Stage 2
    s2 = classify_subdomain_stage2(
        table_name, table_metadata, domain_config,
        domain_key=s1["domain"],
        domain_confidence=s1["confidence"],
        confidence_threshold=confidence_threshold,
        model_endpoint=model_endpoint, temperature=temperature,
        max_tokens=max_tokens,
    )

    # Enforce subdomain value against config
    sd_keys = list(domains.get(s1["domain"], {}).get("subdomains", {}).keys())
    if sd_keys and s2.get("subdomain"):
        enforced_sd, sd_exact = _enforce_value(s2["subdomain"], sd_keys)
        if not sd_exact:
            logger.warning("Snapped stage-2 subdomain '%s' -> '%s' for %s", s2["subdomain"], enforced_sd, table_name)
            s2["confidence"] = max(0.0, s2["confidence"] - 0.1)
        s2["subdomain"] = enforced_sd

    # Merge into the canonical result shape
    combined_confidence = round(min(s1["confidence"], s2["confidence"]), 4)
    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "domain": s1["domain"],
        "subdomain": s2["subdomain"],
        "confidence": combined_confidence,
        "recommended_domain": None,
        "recommended_subdomain": s2.get("recommended_subdomain"),
        "reasoning": f"[Domain] {s1['reasoning']} [Subdomain] {s2['reasoning']}",
        "metadata_summary": s2.get("metadata_summary"),
    }


# ---------------------------------------------------------------------------
# Public entry point (unchanged signature)
# ---------------------------------------------------------------------------

def classify_table_domain(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 8192,
    two_stage: bool = True,
    prefilter_top_n: int = 0,
    confidence_threshold: float = 0.5,
) -> Dict[str, Any]:
    """Classify a table into a business domain.

    When ``two_stage=True`` (default), uses the keyword pre-filter -> domain
    LLM call -> subdomain LLM call pipeline.  Set ``two_stage=False`` to use
    the legacy single-shot path.

    The return dict shape is identical in both modes so callers (processing.py,
    append_domain_table_row) need no changes.
    """
    try:
        if two_stage:
            return _classify_two_stage(
                table_name, table_metadata, domain_config,
                model_endpoint=model_endpoint, temperature=temperature,
                max_tokens=max_tokens, prefilter_top_n=prefilter_top_n,
                confidence_threshold=confidence_threshold,
            )
        else:
            return _classify_single_shot(
                table_name, table_metadata, domain_config,
                model_endpoint=model_endpoint, temperature=temperature,
                max_tokens=max_tokens,
            )
    except Exception as e:
        logger.error("Error classifying table %s: %s", table_name, e)
        return _error_result(table_name, e)


async def classify_table_domain_async(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 4096,
    two_stage: bool = True,
    prefilter_top_n: int = 5,
    confidence_threshold: float = 0.5,
) -> Dict[str, Any]:
    """Async wrapper around classify_table_domain."""
    import asyncio

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        lambda: classify_table_domain(
            table_name, table_metadata, domain_config,
            model_endpoint=model_endpoint, temperature=temperature,
            max_tokens=max_tokens, two_stage=two_stage,
            prefilter_top_n=prefilter_top_n,
            confidence_threshold=confidence_threshold,
        ),
    )

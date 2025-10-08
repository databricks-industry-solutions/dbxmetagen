"""
Simplified domain classifier for dbxmetagen that works without UC tools.
This module provides table-level domain classification using metadata passed directly.
"""

import os
import json
import yaml
import logging
from typing import Dict, Any, Optional
from databricks_langchain import ChatDatabricks
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TableClassification(BaseModel):
    """Pydantic model for table classification response"""

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


def load_domain_config(config_path: str = None) -> Dict[str, Any]:
    """Load domain configuration from YAML file"""

    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)
            logger.info("Loaded domain config from %s", config_path)
            return config
        except Exception as e:
            logger.warning("Could not load domain config from %s: %s", config_path, e)

    # Try default paths
    # TODO: This should be refactored to use the ConfigManager class and evaluate which path is correct for the current environment
    config_paths = [
        "configurations/domain_config.yaml",
        "../configurations/domain_config.yaml",
        "../../configurations/domain_config.yaml",
    ]

    for path in config_paths:
        try:
            if os.path.exists(path):
                with open(path, "r") as file:
                    config = yaml.safe_load(file)
                logger.info("Loaded domain config from %s", path)
                return config
        except Exception as e:
            logger.warning("Could not load domain config from %s: %s", path, e)

    # Fallback configuration
    logger.warning("Could not load domain config from any path, using fallback")
    return {
        "domains": {"unknown": {"name": "Unknown", "keywords": [], "subdomains": {}}}
    }


def generate_domain_prompt_section(domain_config: Dict[str, Any]) -> str:
    """Generate the domain section for the system prompt based on configuration"""
    domains = domain_config.get("domains", {})

    if not domains:
        return "Available domains include: unknown"

    # Extract domain names
    domain_names = list(domains.keys())
    domain_list = ", ".join(domain_names)

    # Generate detailed domain descriptions
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


def create_system_prompt(domain_config: Dict[str, Any]) -> str:
    """Create the system prompt with dynamic domain information"""
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


def classify_table_domain(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 8192,
) -> Dict[str, Any]:
    """
    Classify a table into a business domain using metadata.

    Args:
        table_name: Full table name (catalog.schema.table)
        table_metadata: Dictionary containing table metadata including:
            - column_contents: Dict with columns, data, and metadata
            - table_tags: Table-level tags
            - table_constraints: Table constraints
            - table_comments: Existing table comments
        domain_config: Domain configuration dictionary
        model_endpoint: LLM endpoint to use
        temperature: Model temperature
        max_tokens: Maximum tokens for completion

    Returns:
        Dictionary with classification results
    """
    try:
        llm = ChatDatabricks(
            endpoint=model_endpoint, temperature=temperature, max_tokens=max_tokens
        )
        structured_llm = llm.with_structured_output(TableClassification)
        system_prompt = create_system_prompt(domain_config)
        catalog, schema, table = table_name.split(".")
        user_message = f"""Please classify the following table:

Table: {table_name}

Column Information:
{json.dumps(table_metadata.get('column_contents', {}), indent=2)}

"""
        if table_metadata.get("table_tags"):
            user_message += f"\nTable Tags:\n{table_metadata['table_tags']}\n"

        if table_metadata.get("table_constraints"):
            user_message += (
                f"\nTable Constraints:\n{table_metadata['table_constraints']}\n"
            )

        if table_metadata.get("table_comments"):
            user_message += (
                f"\nExisting Table Comment:\n{table_metadata['table_comments']}\n"
            )

        if table_metadata.get("column_metadata"):
            user_message += f"\nColumn Metadata:\n{json.dumps(table_metadata['column_metadata'], indent=2)}\n"

        # Get classification
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        response = structured_llm.invoke(messages)

        result = response.dict()
        result["catalog"] = catalog
        result["schema"] = schema
        result["table"] = table

        logger.info(
            "Classified %s as %s/%s (confidence: %.2f)",
            table_name,
            result["domain"],
            result.get("subdomain", "N/A"),
            result["confidence"],
        )

        return result

    except Exception as e:
        logger.error("Error classifying table %s: %s", table_name, e)
        catalog, schema, table = table_name.split(".")
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "domain": "unknown",
            "subdomain": None,
            "confidence": 0.0,
            "reasoning": f"Classification failed: {str(e)}",
            "metadata_summary": "Error during classification",
        }


async def classify_table_domain_async(
    table_name: str,
    table_metadata: Dict[str, Any],
    domain_config: Dict[str, Any],
    model_endpoint: str = "databricks-claude-3-7-sonnet",
    temperature: float = 0.1,
    max_tokens: int = 4096,
) -> Dict[str, Any]:
    """
    Async version of classify_table_domain for batch processing.

    Args:
        table_name: Full table name (catalog.schema.table)
        table_metadata: Dictionary containing table metadata
        domain_config: Domain configuration dictionary
        model_endpoint: LLM endpoint to use
        temperature: Model temperature
        max_tokens: Maximum tokens for completion

    Returns:
        Dictionary with classification results
    """
    # For now, wrap the sync version
    # TODO: Implement true async with aio support if needed
    import asyncio

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        classify_table_domain,
        table_name,
        table_metadata,
        domain_config,
        model_endpoint,
        temperature,
        max_tokens,
    )

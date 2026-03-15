# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,helper functions
"""
Table Metadata Enrichment Module

Adapted from etl/02_enrich_table_metadata.py for direct table FQN processing.
This module enriches table metadata with:
- Column samples and value dictionaries
- LLM-enhanced column descriptions
- LLM-synthesized table descriptions
- Multi-level chunks for vector search (table + column levels)

Designed to run as a Databricks job with Spark SQL.
TODO: 1. llm output only selected fields (enhanced_comment and classification) in json, not all fields.
      2. optimize ai_query by running on a table over many rows at once.
      Example:
"""
# df_out = df.selectExpr("""
#   ai_query(
#     'databricks-meta-llama-3-3-70b-instruct',
#     CONCAT('Please summarize: ', text),
#     modelParameters => named_struct('max_tokens', 100, 'temperature', 0.7)
#   ) AS summary
# """)
# df_out.write.mode("overwrite").saveAsTable("output_table")

import json
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from typing import Dict, List, Any


# ============================================================================
# Helper Functions
# ============================================================================

def json_serializer(obj):
    """
    Convert non-serializable objects to strings.
    
    Helper function to safely serialize objects for JSON output.
    """
    # Handle PySpark Column objects
    if hasattr(obj, '__class__') and 'pyspark' in str(type(obj)):
        return str(obj)
    # Handle datetime/date objects
    if hasattr(obj, 'isoformat') and callable(getattr(obj, 'isoformat', None)):
        return obj.isoformat()
    return str(obj)


def get_table_metadata(table_identifier: str) -> pd.DataFrame:
    """
    Get table metadata including columns, data types, and comments.
    
    Args:
        table_identifier: Fully qualified table name (catalog.schema.table)
    
    Returns:
        Pandas DataFrame with column metadata
    """
    try:
        df_description = spark.sql(f"DESCRIBE {table_identifier}")
        
        # Filter out metadata rows, keep only actual columns
        df_clean = df_description.filter(
            (col('data_type').isNotNull()) & 
            (~col('col_name').startswith('#')) & 
            (col('col_name') != '') & 
            (col('data_type') != '') &
            (~col('col_name').isin(
                '# Delta Statistics Columns', 'Column Names', 'Column Selection Method', 
                'Created Time', 'Last Access', 'Created By', 'Statistics', 'Type', 
                'Location', 'Provider', 'Owner', 'Is_managed_location', 
                'Predictive Optimization', 'Table Properties', 'Catalog', 'Database', 
                'Table', '# Detailed Table Information', 'Name', 'Comment', 
                '# Partitioning', 'Part 0', 'Partition Provider',
                'Legacy UC Partitioned DELTASHARING Table'
            ))
        )
        
        return df_clean.toPandas()
    except Exception as e:
        print(f"Error getting metadata for {table_identifier}: {str(e)}")
        return pd.DataFrame(columns=['col_name', 'data_type', 'comment'])


def sample_column_values(table_identifier: str, column_name: str, sample_size: int = 100) -> List[Any]:
    """
    Sample distinct values from a column.
    
    Args:
        table_identifier: Fully qualified table name
        column_name: Column to sample from
        sample_size: Number of samples to retrieve
    
    Returns:
        List of sampled values (converted to JSON-serializable types)
    """
    try:
        query = f"""
        SELECT DISTINCT `{column_name}` 
        FROM {table_identifier} 
        WHERE `{column_name}` IS NOT NULL 
        LIMIT {sample_size}
        """
        result = spark.sql(query).collect()
        # Convert values to JSON-serializable types
        sampled_values = []
        for row in result:
            val = row[0]
            # Convert date/datetime objects to strings
            if hasattr(val, 'isoformat'):
                sampled_values.append(val.isoformat())
            else:
                sampled_values.append(val)
        return sampled_values
    except Exception as e:
        print(f"Error sampling {column_name} from {table_identifier}: {str(e)}")
        return []


def build_value_dictionary(table_identifier: str, column_name: str, max_values: int = 50) -> Dict[str, int]:
    """
    Build a value dictionary (value frequency) for a column.
    
    Args:
        table_identifier: Fully qualified table name
        column_name: Column to build dictionary for
        max_values: Maximum number of unique values to include
    
    Returns:
        Dictionary mapping values to their frequencies
    """
    try:
        query = f"""
        SELECT `{column_name}`, COUNT(*) as frequency 
        FROM {table_identifier} 
        WHERE `{column_name}` IS NOT NULL 
        GROUP BY `{column_name}` 
        ORDER BY frequency DESC 
        LIMIT {max_values}
        """
        result = spark.sql(query).collect()
        return {str(row[0]): int(row[1]) for row in result}
    except Exception as e:
        print(f"Error building value dictionary for {column_name} from {table_identifier}: {str(e)}")
        return {}


def enrich_column_metadata(columns_metadata: pd.DataFrame, table_identifier: str, 
                          sample_size: int, max_unique_values: int,
                          column_configs: List[Dict] = None) -> List[Dict]:
    """
    Enrich column metadata with sampled values and value dictionaries.
    
    Args:
        columns_metadata: DataFrame with column metadata
        table_identifier: Fully qualified table name
        sample_size: Number of samples per column
        max_unique_values: Maximum unique values for value dictionary
        column_configs: Original column configurations (optional)
    
    Returns:
        List of enriched column metadata dictionaries
    """
    enriched_columns = []
    
    # Create a lookup for column configs
    config_lookup = {}
    if column_configs:
        for config in column_configs:
            config_lookup[config.get('column_name')] = config
    
    for _, row in columns_metadata.iterrows():
        col_name = row['col_name']
        col_type = row['data_type']
        col_comment = row.get('comment', '')
        
        # Get original config if exists
        original_config = config_lookup.get(col_name, {})
        
        enriched_col = {
            'column_name': col_name,
            'data_type': col_type,
            'comment': col_comment,
            'original_config': original_config
        }
        
        # Only sample and build dictionaries if not excluded
        if not original_config.get('exclude', False):
            # Sample values if requested or by default
            if original_config.get('enable_format_assistance', True):
                sampled_values = sample_column_values(table_identifier, col_name, sample_size)
                enriched_col['sample_values'] = sampled_values
            
            # Build value dictionary if requested
            if original_config.get('enable_entity_matching', False):
                value_dict = build_value_dictionary(table_identifier, col_name, max_unique_values)
                enriched_col['value_dictionary'] = value_dict
        
        enriched_columns.append(enriched_col)
    
    return enriched_columns


def enhance_comments_with_llm(columns: List[Dict], llm_endpoint: str) -> List[Dict]:
    """
    Use LLM to enhance column comments with better descriptions.
    Processes columns in batches to avoid token limits and JSON parsing issues.
    
    Args:
        columns: List of column dictionaries
        llm_endpoint: LLM endpoint name
    
    Returns:
        List of columns with enhanced comments
    """
    BATCH_SIZE = 50  # Process 50 columns at a time
    
    # Helper to serialize dates and other objects
    def safe_json_default(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    
    print(f"  → Processing {len(columns)} columns in batches of {BATCH_SIZE}")
    
    total_batches = (len(columns) + BATCH_SIZE - 1) // BATCH_SIZE
    successful_batches = 0
    
    for batch_idx in range(0, len(columns), BATCH_SIZE):
        batch = columns[batch_idx:batch_idx+BATCH_SIZE]
        batch_num = (batch_idx // BATCH_SIZE) + 1
        
        print(f"    → Batch {batch_num}/{total_batches}: Processing columns {batch_idx+1}-{min(batch_idx+BATCH_SIZE, len(columns))}")
        
        # Prepare simplified version for this batch
        simplified_cols = []
        for col in batch:
            simplified_cols.append({
                'col_name': col['column_name'],
                'data_type': col['data_type'],
                'comment': col.get('comment', ''),
                'sample_values': col.get('sample_values', [])[:5] if 'sample_values' in col else []
            })
        
        prompt = (
            f"You will receive {len(batch)} database columns in JSON format. "
            "Your task is to improve the 'comment' field for each column by:\n"
            "1. Making it more descriptive and informative\n"
            "2. Using the data_type and sample_values as context\n"
            "3. Explaining what the column represents in plain English\n"
            "4. Classify the column into one of the following categories: categorical, measure, temporal, identifier, boolean, free_text, semi_structured, geospatial, foreign_key, or other\n\n"
            f"{json.dumps(simplified_cols, indent=2, default=safe_json_default)}\n\n"
            "Return a JSON array with the same structure, but add an 'enhanced_comment' field and a 'classification' field "
            "with your improved description and classification. Only return valid JSON, no explanations."
        )
        
        try:
            llm_result = spark.sql(
                f"SELECT ai_query('{llm_endpoint}', ?) as result", 
                [prompt]
            ).collect()[0]['result']
            
            # Clean up response - remove markdown code blocks
            llm_result_str = llm_result.replace('```json', '').replace('```', '').strip()
            
            # Additional cleaning: find JSON array boundaries
            start_idx = llm_result_str.find('[')
            end_idx = llm_result_str.rfind(']')
            
            if start_idx >= 0 and end_idx > start_idx:
                llm_result_str = llm_result_str[start_idx:end_idx+1]
            
            # Parse JSON
            enhanced_cols = json.loads(llm_result_str)
            
            # Merge enhanced comments back into original batch
            for orig_col, enhanced_col in zip(batch, enhanced_cols):
                if isinstance(enhanced_col, dict):
                    if 'enhanced_comment' in enhanced_col:
                        orig_col['enhanced_comment'] = enhanced_col['enhanced_comment']
                    else:
                        orig_col['enhanced_comment'] = orig_col.get('comment', '')
                    
                    if 'classification' in enhanced_col:
                        orig_col['classification'] = enhanced_col['classification']
                else:
                    # Fallback to original comment
                    orig_col['enhanced_comment'] = orig_col.get('comment', '')
            
            successful_batches += 1
            print(f"    ✓ Batch {batch_num}/{total_batches} completed")
            
        except json.JSONDecodeError as je:
            print(f"    ✗ Batch {batch_num}/{total_batches} JSON parse error at line {je.lineno}, column {je.colno}")
            print(f"      Error context: {je.msg}")
            # Keep original comments for this batch
            for col in batch:
                col['enhanced_comment'] = col.get('comment', '')
        except Exception as e:
            print(f"    ✗ Batch {batch_num}/{total_batches} failed: {str(e)}")
            # Keep original comments for this batch
            for col in batch:
                col['enhanced_comment'] = col.get('comment', '')
    
    print(f"  ✓ Column enhancement complete: {successful_batches}/{total_batches} batches successful")
    return columns


def synthesize_table_description(table_identifier: str, enriched_columns: List[Dict], llm_endpoint: str) -> str:
    """
    Synthesize a comprehensive table description using column metadata.
    
    Args:
        table_identifier: Fully qualified table name
        enriched_columns: List of enriched column dictionaries
        llm_endpoint: LLM endpoint name
    
    Returns:
        Synthesized table description
    """
    # Helper to serialize dates and other objects
    def safe_json_default(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    
    # Prepare column summary for LLM
    col_summaries = []
    for col in enriched_columns:
        col_summary = {
            'column_name': col['column_name'],
            'data_type': col['data_type'],
            'enhanced_comment': col.get('enhanced_comment', col.get('comment', '')),
            'has_value_dictionary': 'value_dictionary' in col,
            'sample_values': col.get('sample_values', [])[:3] if 'sample_values' in col else []
        }
        col_summaries.append(col_summary)
    
    prompt = (
        f"You are analyzing a database table: {table_identifier}\n\n"
        f"Here are the columns with their descriptions:\n"
        f"{json.dumps(col_summaries, indent=2, default=safe_json_default)}\n\n"
        "Based on this information, write a concise 2-3 sentence description of what this table contains "
        "and its purpose. Focus on the data domain, key entities, and typical use cases. "
        "Return only the description text, no JSON or formatting."
    )
    
    try:
        llm_result = spark.sql(
            f"SELECT ai_query('{llm_endpoint}', ?) as result", 
            [prompt]
        ).collect()[0]['result']
        
        # Clean up response
        table_description = llm_result.strip()
        return table_description
    except Exception as e:
        print(f"Error synthesizing table description: {str(e)}")
        # Fallback: create a simple description
        return f"Table {table_identifier} with {len(enriched_columns)} columns."


# ============================================================================
# Main Enrichment Functions
# ============================================================================

def enrich_table(table_fqn: str, 
                 sample_size: int = 20, 
                 max_unique_values: int = 50, 
                 llm_endpoint: str = "databricks-claude-sonnet-4-5") -> Dict[str, Any]:
    """
    Enrich a single table with comprehensive metadata.
    
    Args:
        table_fqn: Fully qualified table name (catalog.schema.table)
        sample_size: Number of sample values per column
        max_unique_values: Maximum unique values for value dictionary
        llm_endpoint: LLM endpoint for description enhancement
    
    Returns:
        Enriched table metadata dictionary
    """
    print(f"\n{'='*80}")
    print(f"Enriching table: {table_fqn}")
    print(f"{'='*80}")
    
    # Parse FQN
    parts = table_fqn.split('.')
    if len(parts) != 3:
        return {
            'table_fqn': table_fqn,
            'error': 'Invalid FQN format (expected catalog.schema.table)',
            'enriched': False
        }
    
    catalog, schema, table = parts
    
    try:
        # Get base table metadata
        table_metadata = get_table_metadata(table_fqn)
        
        if table_metadata.empty:
            print(f"  ✗ Could not get metadata for {table_fqn}")
            return {
                'table_fqn': table_fqn,
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'error': 'No metadata available',
                'enriched': False
            }
        
        # Enrich with samples and value dictionaries
        print(f"  → Enriching {len(table_metadata)} columns...")
        enriched_columns = enrich_column_metadata(
            table_metadata, 
            table_fqn, 
            sample_size, 
            max_unique_values,
            column_configs=None  # No pre-existing configs for direct table enrichment
        )
        
        # Enhance comments with LLM
        print(f"  → Enhancing column comments with LLM...")
        enriched_columns = enhance_comments_with_llm(enriched_columns, llm_endpoint)
        
        # Synthesize table description using enriched column metadata
        print(f"  → Synthesizing table description...")
        table_description = synthesize_table_description(table_fqn, enriched_columns, llm_endpoint)
        
        enriched_table = {
            'table_fqn': table_fqn,
            'catalog': catalog,
            'schema': schema,
            'table': table,
            'table_description': table_description,
            'enriched_columns': enriched_columns,
            'total_columns': len(enriched_columns),
            'enriched': True,
            'enrichment_timestamp': datetime.now().isoformat()
        }
        
        print(f"  ✓ Enriched {len(enriched_columns)} columns")
        print(f"  ✓ Table description: {table_description[:100]}...")
        
        return enriched_table
        
    except Exception as e:
        print(f"  ✗ Error enriching {table_fqn}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'table_fqn': table_fqn,
            'catalog': catalog,
            'schema': schema,
            'table': table,
            'error': str(e),
            'enriched': False
        }


def enrich_tables(table_fqns: List[str], 
                  sample_size: int = 20, 
                  max_unique_values: int = 50, 
                  llm_endpoint: str = "databricks-claude-sonnet-4-5") -> List[Dict[str, Any]]:
    """
    Enrich multiple tables with comprehensive metadata.
    
    Args:
        table_fqns: List of fully qualified table names
        sample_size: Number of sample values per column
        max_unique_values: Maximum unique values for value dictionary
        llm_endpoint: LLM endpoint for description enhancement
    
    Returns:
        List of enriched table metadata dictionaries
    """
    print(f"\n{'='*80}")
    print(f"Starting enrichment of {len(table_fqns)} tables")
    print(f"{'='*80}")
    
    enriched_tables = []
    
    for fqn in table_fqns:
        enriched = enrich_table(fqn, sample_size, max_unique_values, llm_endpoint)
        enriched_tables.append(enriched)
    
    successful = sum(1 for t in enriched_tables if t.get('enriched', False))
    print(f"\n{'='*80}")
    print(f"Enrichment complete: {successful}/{len(table_fqns)} tables successful")
    print(f"{'='*80}")
    
    return enriched_tables


# ============================================================================
# Chunk Generation for Vector Search
# ============================================================================

def create_table_chunks(enriched_tables: List[Dict]) -> List[Dict]:
    """
    Create multi-level chunks for vector search (table + column levels only).
    
    This follows the Hybrid Multi-Level Chunking strategy from the parent implementation,
    but excludes space-level chunks.
    
    Args:
        enriched_tables: List of enriched table dictionaries
    
    Returns:
        List of chunk dictionaries with metadata for vector search
    """
    all_chunks = []
    chunk_id = 0
    
    for table_doc in enriched_tables:
        if not table_doc.get('enriched', False):
            continue  # Skip failed enrichments
        
        table_fqn = table_doc.get('table_fqn', '')
        table_name = table_doc.get('table', '')
        table_desc = table_doc.get('table_description', '')
        enriched_columns = table_doc.get('enriched_columns', [])
        
        # ===================================================================
        # Level 1: Table Overview Chunk
        # ===================================================================
        # Build column list with brief descriptions
        column_lines = []
        categorical_fields = []
        
        for col in enriched_columns:
            col_name = col.get('column_name')
            col_type = col.get('data_type')
            enhanced_comment = col.get('enhanced_comment') or col.get('comment') or ''
            classification = col.get('classification', '')
            print(col)
            
            # Truncate long descriptions for overview
            if len(enhanced_comment) > 300:
                enhanced_comment = enhanced_comment[:300-3] + "..."
            
            column_lines.append(f"• {col_name} ({col_type}): {enhanced_comment} ({classification})")
            
            # Track categorical fields with top values
            if 'value_dictionary' in col and col['value_dictionary']:
                top_values = list(col['value_dictionary'].keys())[:5]
                categorical_fields.append(f"• {col_name}: {', '.join(top_values)}")
        
        table_overview_text = f"""Table: {table_name}
Full Path: {table_fqn}

Table Description: {table_desc}

Columns ({len(enriched_columns)} total):
{chr(10).join(column_lines)}
"""
        
        if categorical_fields:
            table_overview_text += f"""
Key Categorical Fields:
{chr(10).join(categorical_fields)}
"""
        
        all_chunks.append({
            'chunk_id': chunk_id,
            'chunk_type': 'table_overview',
            'table_fqn': table_fqn,
            'table_name': table_name,
            'column_name': None,
            'searchable_content': table_overview_text,
            'is_categorical': any('categorical' in c.get('classification', '').lower() if c.get('classification') else 'value_dictionary' in c for c in enriched_columns),
            'is_temporal': any('temporal' in c.get('classification', '').lower() if c.get('classification') else any(t in c.get('data_type', '').lower() for t in ['date', 'time', 'timestamp']) for c in enriched_columns),
            'is_identifier': any('identifier' in c.get('classification', '').lower() if c.get('classification') else any(k in c['column_name'].lower() for k in ['_id', 'id_']) for c in enriched_columns),
            'has_value_dictionary': any('value_dictionary' in c for c in enriched_columns),
            'metadata_json': json.dumps({
                'table_fqn': table_fqn,
                'total_columns': len(enriched_columns)
            }, default=json_serializer)
        })
        chunk_id += 1
        
        # ===================================================================
        # Level 2: Column Detail Chunks (one per column)
        # ===================================================================
        for col in enriched_columns:
            col_name = col.get('column_name')
            col_type = col.get('data_type')
            enhanced_comment = col.get('enhanced_comment', col.get('comment', ''))
            sample_values = col.get('sample_values', [])
            value_dict = col.get('value_dictionary', {})
            
            # Determine column characteristics
            classification = col.get('classification', '').lower()
            
            is_categorical = 'categorical' in classification if classification else len(value_dict) > 0
            is_temporal = 'temporal' in classification if classification else any(t in col_type.lower() for t in ['date', 'time', 'timestamp'])
            is_identifier = 'identifier' in classification if classification else any(k in col_name.lower() for k in ['_id', 'id_'])
            has_value_dictionary = len(value_dict) > 0
            
            # Build column detail text
            column_detail_text = f"""Column: {col_name}
Table: {table_name}
Full Table Path: {table_fqn}
Data Type: {col_type}

Description: {enhanced_comment}
"""
            
            # Add sample values (limited to 5 for readability)
            if sample_values:
                limited_samples = sample_values[:5]
                column_detail_text += f"""
Sample Values: {', '.join([str(v) for v in limited_samples])}
"""
            
            # Add top values from value dictionary
            if value_dict:
                top_entries = sorted(value_dict.items(), key=lambda x: x[1], reverse=True)[:10]
                value_lines = [f"  - {val}: {count:,} records" for val, count in top_entries]
                column_detail_text += f"""
Top Values:
{chr(10).join(value_lines)}
"""
            
            # Add classification info
            characteristics = []
            if classification:
                characteristics.append(classification)
            else:
                if is_categorical:
                    characteristics.append("categorical")
                if is_temporal:
                    characteristics.append("temporal")
                if is_identifier:
                    characteristics.append("identifier")
            
            if characteristics:
                column_detail_text += f"""
Classification: {', '.join(characteristics)}
"""
            
            all_chunks.append({
                'chunk_id': chunk_id,
                'chunk_type': 'column_detail',
                'table_fqn': table_fqn,
                'table_name': table_name,
                'column_name': col_name,
                'searchable_content': column_detail_text,
                'is_categorical': is_categorical,
                'is_temporal': is_temporal,
                'is_identifier': is_identifier,
                'has_value_dictionary': has_value_dictionary,
                'metadata_json': json.dumps({
                    'data_type': col_type,
                    'sample_values': sample_values[:5],
                    'value_dictionary_size': len(value_dict)
                }, default=json_serializer)
            })
            chunk_id += 1
    
    return all_chunks


# ============================================================================
# Save to Unity Catalog
# ============================================================================

def save_to_unity_catalog(enriched_tables: List[Dict], 
                          chunks: List[Dict],
                          catalog_name: str = "yyang",
                          schema_name: str = "multi_agent_genie",
                          enriched_docs_table: str = "enriched_tables_direct",
                          chunks_table: str = "enriched_tables_chunks",
                          write_mode: str = "overwrite") -> None:
    """
    Save enriched tables and chunks to Unity Catalog delta tables.
    
    Args:
        enriched_tables: List of enriched table dictionaries
        chunks: List of chunk dictionaries
        catalog_name: Target catalog name
        schema_name: Target schema name
        enriched_docs_table: Name for enriched docs table
        chunks_table: Name for chunks table
        write_mode: Write mode - "overwrite", "append", or "error"
    """
    # Ensure catalog and schema exist
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
    except Exception:
        pass
    spark.sql(f"USE CATALOG `{catalog_name}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")
    spark.sql(f"USE SCHEMA `{schema_name}`")
    
    print(f"\n{'='*80}")
    print(f"Saving to Unity Catalog: {catalog_name}.{schema_name}")
    print(f"{'='*80}")
    
    # Save enriched docs
    if enriched_tables:
        enriched_docs_json = [json.dumps(doc, default=json_serializer) for doc in enriched_tables]
        df_enriched = spark.createDataFrame(
            [(i, doc_json, doc['table_fqn'], doc.get('enriched', False)) 
             for i, (doc_json, doc) in enumerate(zip(enriched_docs_json, enriched_tables))],
            schema="id INT, enriched_doc STRING, table_fqn STRING, enriched BOOLEAN"
        )
        
        full_table_name = f"{catalog_name}.{schema_name}.{enriched_docs_table}"
        df_enriched.write.mode(write_mode).option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"✓ Saved {df_enriched.count()} enriched docs to: {full_table_name} (mode: {write_mode})")
    
    # Save chunks
    if chunks:
        df_chunks = spark.createDataFrame(chunks)
        full_chunks_table = f"{catalog_name}.{schema_name}.{chunks_table}"
        df_chunks.write.mode(write_mode).option("overwriteSchema", "true").saveAsTable(full_chunks_table)
        print(f"✓ Saved {df_chunks.count()} chunks to: {full_chunks_table} (mode: {write_mode})")
        
        # Show chunk distribution
        chunk_type_counts = {}
        for chunk in chunks:
            chunk_type = chunk['chunk_type']
            chunk_type_counts[chunk_type] = chunk_type_counts.get(chunk_type, 0) + 1
        
        print("\nChunk distribution:")
        for chunk_type, count in chunk_type_counts.items():
            print(f"  - {chunk_type}: {count} chunks")


# ============================================================================
# Entry Point for Databricks Job
# ============================================================================

# COMMAND ----------

# DBTITLE 1,Create notebook widgets with default values
# Create notebook widgets with default values
dbutils.widgets.text("table_list_table", "serverless_dbx_unifiedchat_catalog.gold.temp_enrichment_tables_4cb71ab1", "Table List Table")
dbutils.widgets.text("tables", "", "Tables (comma-separated FQNs)")
dbutils.widgets.text("sample_size", "10", "Sample Size")
dbutils.widgets.text("max_unique_values", "10", "Max Unique Values")
dbutils.widgets.text("llm_endpoint", "databricks-gemini-3-1-flash-lite", "LLM Endpoint") # databricks-gemini-3-flash, databricks-claude-haiku-4-5, databricks-gpt-5-mini, databricks-gemini-3-1-flash-lite, databricks-gpt-oss-120b
dbutils.widgets.text("metadata_table", "serverless_dbx_unifiedchat_catalog.gold.enriched_table_metadata", "Metadata Table")
dbutils.widgets.text("chunks_table", "serverless_dbx_unifiedchat_catalog.gold.enriched_table_chunks", "Chunks Table")
dbutils.widgets.text("write_mode", "overwrite", "Write Mode")

# COMMAND ----------

# DBTITLE 1,Parse and validate input parameters from notebook widge ...
# Get parameters from notebook widgets
table_list_table = dbutils.widgets.get("table_list_table").strip() or None
table_fqns_str = dbutils.widgets.get("tables").strip() or None
sample_size = int(dbutils.widgets.get("sample_size"))
max_unique_values = int(dbutils.widgets.get("max_unique_values"))
llm_endpoint = dbutils.widgets.get("llm_endpoint")
metadata_table_full = dbutils.widgets.get("metadata_table")
chunks_table_full = dbutils.widgets.get("chunks_table")
write_mode = dbutils.widgets.get("write_mode")

# Get table FQNs either from temp table or comma-separated string
if table_list_table:
    print(f"Reading table list from temp table: {table_list_table}")
    try:
        df_tables = spark.sql(f"SELECT table_fqn FROM {table_list_table}")
        table_fqns = [row.table_fqn for row in df_tables.collect()]
        print(f"✓ Loaded {len(table_fqns)} tables from temp table")
    except Exception as e:
        print(f"Error reading temp table {table_list_table}: {str(e)}")
        raise ValueError(f"Failed to read table list from {table_list_table}")
elif table_fqns_str:
    # Parse comma-separated table FQNs (legacy method)
    print(f"Reading table list from comma-separated parameter")
    table_fqns = [fqn.strip() for fqn in table_fqns_str.split(',') if fqn.strip()]
else:
    table_fqns = []

if not table_fqns:
    print("Error: No table FQNs provided")
    print("Usage: Provide 'table_list_table' parameter with temp table name or 'tables' parameter with comma-separated FQNs")
    raise ValueError("No table FQNs provided")

# Parse destination table names (format: catalog.schema.table)
metadata_parts = metadata_table_full.split('.')
chunks_parts = chunks_table_full.split('.')

if len(metadata_parts) != 3 or len(chunks_parts) != 3:
    print(f"Error: Invalid table format. Expected 'catalog.schema.table'")
    print(f"  Metadata table: {metadata_table_full}")
    print(f"  Chunks table: {chunks_table_full}")
    raise ValueError("Invalid table format")

catalog_name = metadata_parts[0]
schema_name = metadata_parts[1]
metadata_table_name = metadata_parts[2]
chunks_table_name = chunks_parts[2]

print(f"Parameters:")
print(f"  Tables: {len(table_fqns)}")
print(f"  Sample size: {sample_size}")
print(f"  Max unique values: {max_unique_values}")
print(f"  LLM endpoint: {llm_endpoint}")
print(f"  Metadata table: {metadata_table_full}")
print(f"  Chunks table: {chunks_table_full}")
print(f"  Write mode: {write_mode}")

# COMMAND ----------

# DBTITLE 1,Index and display all fully qualified table names
for i, value in enumerate(table_fqns, 0):
    print(i, value)

# COMMAND ----------

# DBTITLE 1,Enrich Selected Tables with LLM Endpoint and Parameters
# Run enrichment
enriched = enrich_tables(table_fqns, sample_size, max_unique_values, llm_endpoint)

# COMMAND ----------

# DBTITLE 1,Generate Table Data Chunks from Enriched Dataset
# Create chunks
chunks = create_table_chunks(enriched)

# COMMAND ----------

# DBTITLE 1,Save Enriched Data and Chunks to Unity Catalog
# Save to Unity Catalog
save_to_unity_catalog(
    enriched, 
    chunks,
    catalog_name=catalog_name,
    schema_name=schema_name,
    enriched_docs_table=metadata_table_name,
    chunks_table=chunks_table_name,
    write_mode=write_mode
)

print("\n" + "="*80)
print("Enrichment job complete!")
print("="*80)

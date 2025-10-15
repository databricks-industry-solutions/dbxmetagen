"""
Enhanced Metadata Generator
===========================

Advanced metadata generation with intelligent sampling, chunking, and context optimization.
Provides enterprise-grade generation capabilities inspired by dbxmetagen.
"""

import json
import logging
import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import statistics
from collections import Counter
from pii_detector import PIIDetector

logger = logging.getLogger(__name__)

class EnhancedMetadataGenerator:
    """
    Enterprise-grade metadata generator with advanced sampling and analysis capabilities.
    Combines the power of LLM generation with intelligent data analysis.
    """
    
    def __init__(self, llm_service, unity_service, settings_manager=None):
        self.llm_service = llm_service
        self.unity_service = unity_service
        self.settings_manager = settings_manager
        self.pii_detector = PIIDetector(settings_manager, llm_service)
        self.progress_callback = None
        
        # Configuration - mirrors dbxmetagen variables.yml
        self.config = {
            'sample_rows': 50,
            'max_chunk_size': 10,
            'confidence_threshold': 0.7,
            'max_tokens': 512,
            'temperature': 0.3,
            'enable_pii_detection': True,
            'enable_data_profiling': True,
            'context_window': 3  # Number of related objects to include for context
        }
    
    def update_config(self, **kwargs):
        """Update generator configuration"""
        self.config.update(kwargs)
        logger.info(f"Updated config: {kwargs}")
    
    def set_progress_callback(self, callback):
        """Set progress callback function"""
        self.progress_callback = callback
    
    def _update_progress(self, **kwargs):
        """Update progress if callback is set (optimized for minimal overhead)"""
        if self.progress_callback:
            try:
                # Only update if we have meaningful changes to avoid excessive calls
                if kwargs:
                    self.progress_callback(**kwargs)
            except Exception as e:
                # Use debug level to avoid log spam during generation
                logger.debug(f"Progress callback error: {e}")
    
    async def generate_enhanced_metadata(self, catalog_name: str, model: str, style: str = 'enterprise', selected_objects: Dict = None, run_id: str = None) -> Dict:
        """
        Generate enhanced metadata for entire catalog with PII detection and data profiling
        """
        logger.info(f"Starting enhanced metadata generation for {catalog_name}")
        
        start_time = datetime.now()
        if run_id is None:
            run_id = f"enhanced_{start_time.strftime('%Y%m%d_%H%M%S')}_{catalog_name}"
        
        results = {
            'run_id': run_id,
            'catalog_name': catalog_name,
            'model': model,
            'style': style,
            'started_at': start_time.isoformat(),
            'config': self.config.copy(),
            'summary': {
                'total_objects': 0,
                'processed_objects': 0,
                'pii_objects_detected': 0,
                'high_confidence_results': 0,
                'errors': 0
            },
            'generated_metadata': []
        }
        
        try:
            # Optimized: Query only selected objects instead of full catalog scan
            if selected_objects:
                selected_schema_names = selected_objects.get('schemas', [])
                selected_table_names = selected_objects.get('tables', [])
                selected_column_names = selected_objects.get('columns', [])
                
                logger.info(f"Exact selection scope - Schemas: {selected_schema_names}, Tables: {selected_table_names}, Columns: {selected_column_names}")
                
                # Query only the selected objects (much more efficient)
                missing_schemas = self._get_selected_schemas_metadata(catalog_name, selected_schema_names) if selected_schema_names else []
                missing_tables = self._get_selected_tables_metadata(catalog_name, selected_table_names) if selected_table_names else []
                missing_columns = self._get_selected_columns_metadata(catalog_name, selected_column_names) if selected_column_names else []
            else:
                # Fallback: Get all objects (only when no selection provided)
                logger.info("No selection provided, scanning entire catalog")
                missing_schemas = self.unity_service.get_schemas_with_missing_metadata(catalog_name)
                missing_tables = self.unity_service.get_tables_with_missing_metadata(catalog_name)
                missing_columns = self.unity_service.get_columns_with_missing_metadata(catalog_name)
            
            logger.info(f"Exact filtering result: {len(missing_schemas)} schemas, {len(missing_tables)} tables, {len(missing_columns)} columns")
            
            results['summary']['total_objects'] = len(missing_schemas) + len(missing_tables) + len(missing_columns)
            
            # Update progress: Setup complete
            self._update_progress(
                current_phase="Schema Analysis",
                phase_progress=0,
                processed_objects=0,
                current_object="Starting schema analysis..."
            )
            
            # Process schemas with enhanced context
            for schema in missing_schemas:
                try:
                    metadata = await self._generate_schema_metadata_enhanced(
                        schema, catalog_name, model, style, run_id
                    )
                    results['generated_metadata'].append(metadata)
                    results['summary']['processed_objects'] += 1
                    
                    # Update progress after each schema
                    self._update_progress(
                        processed_objects=results['summary']['processed_objects'],
                        current_object=f"Schema: {schema['name']}"
                    )
                    
                    if metadata.get('pii_detected'):
                        results['summary']['pii_objects_detected'] += 1
                    if metadata.get('confidence_score', 0) >= self.config['confidence_threshold']:
                        results['summary']['high_confidence_results'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing schema {schema['name']}: {e}")
                    results['summary']['errors'] += 1
                    # Update progress even on error
                    self._update_progress(
                        processed_objects=results['summary']['processed_objects'],
                        current_object=f"Schema: {schema['name']} (error)",
                        errors=[str(e)]
                    )
            
            # Update progress: Moving to table analysis
            self._update_progress(
                current_phase="Table Analysis",
                phase_progress=0,
                current_object="Starting table analysis..."
            )
            
            # Process tables with enhanced analysis
            for table in missing_tables:
                try:
                    metadata = await self._generate_table_metadata_enhanced(
                        table, catalog_name, model, style, run_id
                    )
                    results['generated_metadata'].append(metadata)
                    results['summary']['processed_objects'] += 1
                    
                    # Update progress after each table
                    self._update_progress(
                        processed_objects=results['summary']['processed_objects'],
                        current_object=f"Table: {table['schema_name']}.{table['name']}"
                    )
                    
                    if metadata.get('pii_detected'):
                        results['summary']['pii_objects_detected'] += 1
                    if metadata.get('confidence_score', 0) >= self.config['confidence_threshold']:
                        results['summary']['high_confidence_results'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing table {table['name']}: {e}")
                    results['summary']['errors'] += 1
                    # Update progress even on error
                    self._update_progress(
                        processed_objects=results['summary']['processed_objects'],
                        current_object=f"Table: {table['schema_name']}.{table['name']} (error)"
                    )
            
            # Update progress: Moving to column analysis
            self._update_progress(
                current_phase="Column Analysis",
                phase_progress=0,
                current_object="Starting column analysis..."
            )
            
            # Process columns in optimized chunks
            column_results = await self._generate_columns_metadata_chunked(
                missing_columns, catalog_name, model, style, run_id
            )
            
            results['generated_metadata'].extend(column_results['metadata'])
            results['summary']['processed_objects'] += column_results['processed']
            results['summary']['pii_objects_detected'] += column_results['pii_detected']
            results['summary']['high_confidence_results'] += column_results['high_confidence']
            results['summary']['errors'] += column_results['errors']
            
            # Update progress: Finalization
            self._update_progress(
                current_phase="Finalization",
                phase_progress=100,
                processed_objects=results['summary']['processed_objects'],
                current_object="Completing generation..."
            )
            
        except Exception as e:
            logger.error(f"Error in enhanced metadata generation: {e}")
            results['error'] = str(e)
            # Update progress on error
            self._update_progress(
                current_phase="Error",
                current_object=f"Generation failed: {str(e)}"
            )
        
        results['completed_at'] = datetime.now().isoformat()
        results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        # Final progress update
        self._update_progress(
            current_phase="Completed",
            phase_progress=100,
            processed_objects=results['summary']['processed_objects'],
            current_object="Generation completed successfully"
        )
        
        logger.info(f"Enhanced generation complete: {results['summary']}")
        return results
    
    async def _generate_schema_metadata_enhanced(self, schema_info: Dict, catalog_name: str, model: str, style: str, run_id: str = None) -> Dict:
        """Generate enhanced schema metadata with context and analysis"""
        schema_name = schema_info['name']
        
        # Get context: tables in this schema
        try:
            tables_response = await self._get_tables_in_schema(catalog_name, schema_name)
            table_names = [t['name'] for t in tables_response[:10]]  # Limit for context
            logger.info(f"🔍 Schema {schema_name}: Found {len(table_names)} tables: {table_names}")
        except Exception as e:
            logger.warning(f"Could not get tables for schema {schema_name}: {e}")
            table_names = []
        
        # Build enhanced context for LLM
        context_info = {
            'schema_name': schema_name,
            'catalog_name': catalog_name,
            'table_count': len(table_names),
            'table_names': table_names,
            'owner': schema_info.get('owner', ''),
            'created_at': schema_info.get('created_at', '')
        }
        
        # Generate description with context
        prompt = self._build_schema_prompt(context_info, style)
        
        try:
            description = self.llm_service._call_databricks_llm(
                prompt=prompt,
                max_tokens=self.config['max_tokens'],
                model=model,
                temperature=self.config['temperature'],
                style=style
            )
            
            # Debug: Log what we got from LLM service
            logger.info(f"Schema description from LLM service: {description[:200]}...")
            
            confidence_score = self._calculate_confidence_score(description, context_info, 'schema')
            
        except Exception as e:
            logger.error(f"LLM generation failed for schema {schema_name}: {e}")
            description = f"Schema containing {schema_name.replace('_', ' ')} related data and supporting tables"
            confidence_score = 0.3
        
        return {
            'run_id': run_id,
            'full_name': f"{catalog_name}.{schema_name}",
            'object_type': 'schema',
            'proposed_comment': description,
            'confidence_score': confidence_score,
            'pii_tags': [],
            'policy_tags': [],
            'data_classification': 'INTERNAL',
            'source_model': model,
            'generation_style': style,
            'context_used': context_info,
            'generated_at': datetime.now().isoformat(),
            'pii_detected': False
        }
    
    async def _generate_table_metadata_enhanced(self, table_info: Dict, catalog_name: str, model: str, style: str, run_id: str = None) -> Dict:
        """Generate enhanced table metadata with column analysis and PII detection"""
        table_name = table_info['name']
        schema_name = table_info['schema_name']
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        
        # Get column information and sample data
        try:
            columns_info = await self._get_table_columns_with_samples(
                catalog_name, schema_name, table_name
            )
        except Exception as e:
            logger.warning(f"Could not get columns for table {full_name}: {e}")
            columns_info = []
        
        # Perform PII analysis on the table
        pii_analysis = None
        if self.config['enable_pii_detection'] and columns_info:
            try:
                pii_analysis = self.pii_detector.analyze_table({
                    'name': table_name,
                    'columns': columns_info
                })
            except Exception as e:
                logger.warning(f"PII analysis failed for table {full_name}: {e}")
        
        # Build enhanced context
        context_info = {
            'table_name': table_name,
            'schema_name': schema_name,
            'catalog_name': catalog_name,
            'table_type': table_info.get('table_type', ''),
            'column_count': len(columns_info),
            'column_names': [col['name'] for col in columns_info[:15]],  # Limit for context
            'data_types': [col['data_type'] for col in columns_info[:15]],
            'owner': table_info.get('owner', ''),
            'pii_analysis': pii_analysis
        }
        
        # Generate description with enhanced context
        prompt = self._build_table_prompt(context_info, style)
        
        try:
            description = self.llm_service._call_databricks_llm(
                prompt=prompt,
                max_tokens=self.config['max_tokens'],
                model=model,
                temperature=self.config['temperature'],
                style=style
            )
            
            confidence_score = self._calculate_confidence_score(description, context_info, 'table')
            
        except Exception as e:
            logger.error(f"LLM generation failed for table {full_name}: {e}")
            description = f"Table containing {table_name.replace('_', ' ')} data with related attributes"
            confidence_score = 0.3
        
        # Extract PII information
        pii_detected = False
        pii_tags = []
        policy_tags = []
        classification = 'INTERNAL'
        
        if pii_analysis:
            pii_detected = pii_analysis['pii_columns'] > 0
            policy_tags = pii_analysis.get('recommended_tags', [])
            classification = pii_analysis.get('highest_classification', 'INTERNAL')
            
            # Extract PII types for tags
            for col_analysis in pii_analysis.get('column_analysis', []):
                if col_analysis['pii_types']:
                    pii_tags.extend(col_analysis['pii_types'])
        
        return {
            'run_id': run_id,
            'full_name': full_name,
            'object_type': 'table',
            'proposed_comment': description,
            'confidence_score': confidence_score,
            'pii_tags': json.dumps(list(set(pii_tags))),
            'policy_tags': json.dumps(policy_tags),
            'data_classification': classification,
            'source_model': model,
            'generation_style': style,
            'context_used': context_info,
            'pii_analysis': pii_analysis,
            'generated_at': datetime.now().isoformat(),
            'pii_detected': pii_detected
        }
    
    async def _generate_columns_metadata_chunked(self, columns: List[Dict], catalog_name: str, model: str, style: str, run_id: str = None) -> Dict:
        """Generate column metadata using intelligent chunking for efficiency"""
        results = {
            'metadata': [],
            'processed': 0,
            'pii_detected': 0,
            'high_confidence': 0,
            'errors': 0
        }
        
        if not columns:
            return results
        
        # Group columns by table for context efficiency
        table_groups = {}
        for column in columns:
            table_key = f"{column['catalog_name']}.{column['schema_name']}.{column['table_name']}"
            if table_key not in table_groups:
                table_groups[table_key] = []
            table_groups[table_key].append(column)
        
        # Process each table's columns together
        for table_key, table_columns in table_groups.items():
            try:
                # Get sample data for all columns in this table
                enriched_columns = await self._enrich_columns_with_samples(table_columns)
                
                # Process in chunks
                chunks = self._create_optimal_chunks(enriched_columns)
                
                for chunk in chunks:
                    try:
                        chunk_results = await self._generate_column_chunk_metadata(
                            chunk, model, style, run_id
                        )
                        
                        results['metadata'].extend(chunk_results['metadata'])
                        results['processed'] += chunk_results['processed']
                        results['pii_detected'] += chunk_results['pii_detected']
                        results['high_confidence'] += chunk_results['high_confidence']
                        
                        # Batch progress updates: Only update every 5 chunks or for large chunks to reduce overhead
                        if len(chunk) >= 5 or results['processed'] % 50 == 0:
                            self._update_progress(
                                processed_objects=results['processed'],
                                current_object=f"Columns in {table_key} (chunk {len(chunk)} columns)"
                            )
                        
                    except Exception as e:
                        logger.error(f"Error processing column chunk in {table_key}: {e}")
                        results['errors'] += len(chunk)
                        # Update progress on error
                        self._update_progress(
                            processed_objects=results['processed'],
                            current_object=f"Columns in {table_key} (error)"
                        )
                        
            except Exception as e:
                logger.error(f"Error processing columns for table {table_key}: {e}")
                results['errors'] += len(table_columns)
        
        return results
    
    async def _generate_column_chunk_metadata(self, columns: List[Dict], model: str, style: str, run_id: str = None) -> Dict:
        """Generate metadata for a chunk of columns with shared context"""
        if not columns:
            return {'metadata': [], 'processed': 0, 'pii_detected': 0, 'high_confidence': 0}
        
        # All columns in chunk are from same table
        first_col = columns[0]
        table_context = {
            'catalog_name': first_col['catalog_name'],
            'schema_name': first_col['schema_name'],
            'table_name': first_col['table_name'],
            'columns': columns
        }
        
        results = {
            'metadata': [],
            'processed': 0,
            'pii_detected': 0,
            'high_confidence': 0
        }
        
        # Build batch prompt for efficiency
        prompt = self._build_column_batch_prompt(table_context, style)
        
        try:
            # Get batch response from LLM
            batch_response = self.llm_service._call_databricks_llm(
                prompt=prompt,
                max_tokens=self.config['max_tokens'] * len(columns),
                model=model,
                temperature=self.config['temperature'],
                style=style
            )
            
            # Parse batch response into individual descriptions
            descriptions = self._parse_batch_response(batch_response, columns)
            
        except Exception as e:
            logger.error(f"Batch LLM generation failed: {e}")
            # Fallback to simple descriptions
            descriptions = [
                f"Contains {col['column_name'].replace('_', ' ')} information"
                for col in columns
            ]
        
        # Process each column
        for i, column in enumerate(columns):
            try:
                description = descriptions[i] if i < len(descriptions) else descriptions[0]
                
                # PII analysis
                pii_analysis = None
                pii_enabled = self.config['enable_pii_detection']
                
                # Check settings manager for PII detection override
                if self.settings_manager and pii_enabled:
                    try:
                        pii_config = self.settings_manager.get_pii_config()
                        pii_enabled = pii_config.get('enabled', True)
                    except Exception as e:
                        logger.warning(f"Failed to get PII settings, using config default: {e}")
                
                if pii_enabled:
                    pii_analysis = self.pii_detector.analyze_column(
                        column_name=column['column_name'],
                        data_type=column['data_type'],
                        sample_values=column.get('sample_values', [])
                    )
                
                confidence_score = self._calculate_confidence_score(
                    description, column, 'column', pii_analysis
                )
                
                # Build result
                full_name = f"{column['catalog_name']}.{column['schema_name']}.{column['table_name']}.{column['column_name']}"
                
                pii_detected = bool(pii_analysis and pii_analysis['pii_types'])
                pii_tags = pii_analysis['pii_types'] if pii_analysis else []
                # Use proposed tags instead of automatic tags
                proposed_policy_tags = pii_analysis['proposed_policy_tags'] if pii_analysis else []
                policy_tags = []  # No automatic tags - only proposed ones
                classification = pii_analysis['classification'] if pii_analysis else 'INTERNAL'
                
                metadata = {
                    'run_id': run_id,
                    'full_name': full_name,
                    'object_type': 'column',
                    'proposed_comment': description,
                    'confidence_score': confidence_score,
                    'pii_tags': json.dumps(pii_tags),
                    'policy_tags': json.dumps(policy_tags),  # Empty - no automatic tags
                    'proposed_policy_tags': json.dumps(proposed_policy_tags),  # New - for manual review
                    'data_classification': classification,
                    'source_model': model,
                    'generation_style': style,
                    'context_used': table_context,
                    'pii_analysis': pii_analysis,
                    'generated_at': datetime.now().isoformat(),
                    'pii_detected': pii_detected
                }
                
                results['metadata'].append(metadata)
                results['processed'] += 1
                
                if pii_detected:
                    results['pii_detected'] += 1
                if confidence_score >= self.config['confidence_threshold']:
                    results['high_confidence'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing column {column['column_name']}: {e}")
                continue
        
        return results
    
    def _build_schema_prompt(self, context: Dict, style: str) -> str:
        """Build concise prompt for schema description"""
        schema_name = context['schema_name']
        table_names = context.get('table_names', [])
        table_count = context.get('table_count', 0)
        
        # Build table context for business purpose inference
        if table_names:
            table_context = f"- Contains {table_count} tables: {', '.join(table_names[:5])}"
            if len(table_names) > 5:
                table_context += f" (and {len(table_names) - 5} more)"
        else:
            table_context = f"- Contains {table_count} tables (names not available)"
        
        base_prompt = f"""
Generate a CONCISE description for the database schema '{schema_name}' in exactly 2-4 sentences.

Context:
- Schema: {schema_name}
{table_context}

Requirements:
- Maximum 4 sentences
- INFER the business domain and purpose from the schema name and table names
- DO NOT mention table names, counts, or list tables in the description
- Use the table names as intelligence to understand the business domain
- Explain what business processes and data types this schema supports
- Professional {style} style
- NO tables, bullet points, or formatting
- Plain text only

Business Domain Inference Examples:
- Tables like "patient", "diagnosis", "treatment" → Focus on healthcare data management and clinical operations
- Tables like "claims", "policies", "coverage" → Focus on insurance operations and risk management
- Tables like "orders", "customers", "products" → Focus on e-commerce and retail operations
- Tables like "transactions", "accounts", "balances" → Focus on financial services and banking

Generate a business-focused description that explains the domain purpose without mentioning specific tables.
"""
        return base_prompt.strip()
    
    def _build_table_prompt(self, context: Dict, style: str) -> str:
        """Build enhanced prompt for table description with PII awareness"""
        table_name = context['table_name']
        column_names = context.get('column_names', [])
        pii_analysis = context.get('pii_analysis')
        
        # Build column context for business purpose inference
        if column_names:
            column_context = f"- Contains {len(column_names)} columns: {', '.join(column_names[:15])}"
            if len(column_names) > 15:
                column_context += f" (and {len(column_names) - 15} more)"
        else:
            column_context = f"- Contains {len(column_names)} columns (names not available)"
        
        base_prompt = f"""
Generate a CONCISE description for the table '{table_name}' in exactly 2-3 sentences.

Context:
- Table: {table_name}
{column_context}
- Data types: {', '.join(set(context.get('data_types', [])))}
"""
        
        if pii_analysis and pii_analysis['pii_columns'] > 0:
            base_prompt += f"""
- PII Assessment: {pii_analysis['pii_columns']} columns contain PII
- Risk Level: {pii_analysis['risk_assessment']}
- Classification: {pii_analysis['highest_classification']}
"""
        
        base_prompt += f"""
Requirements:
- Maximum 3 sentences
- INFER the business purpose from the table name and column names
- DO NOT mention column names, counts, or list columns in the description
- Use the column names as intelligence to understand the data purpose
- Explain what business entity or process this table represents
- Professional {style} style
- NO formatting, tables, or bullet points
- Plain text only

Business Purpose Inference Examples:
- Columns like "patient_id", "diagnosis", "treatment_date" → Focus on medical records and patient care
- Columns like "claim_id", "policy_number", "amount" → Focus on insurance claims processing
- Columns like "order_id", "customer_id", "product_name" → Focus on e-commerce transactions
- Columns like "account_number", "balance", "transaction_date" → Focus on financial account management

Generate a business-focused description that explains the data purpose without mentioning specific columns.
"""
        return base_prompt.strip()
    
    def _build_column_batch_prompt(self, context: Dict, style: str) -> str:
        """Build batch prompt for multiple columns"""
        table_name = context['table_name']
        columns = context['columns']
        
        prompt = f"""
Generate professional descriptions for the following columns in table '{table_name}':

"""
        
        for i, col in enumerate(columns, 1):
            prompt += f"""
{i}. Column: {col['column_name']}
   Type: {col['data_type']}
   Sample values: {', '.join(str(v) for v in col.get('sample_values', [])[:3])}

"""
        
        prompt += f"""
For each column, provide a single professional sentence describing:
- What data it stores
- Its purpose in the table
- Any business significance

Style: {style}
Format: Return exactly {len(columns)} descriptions, one per line, numbered.
"""
        
        return prompt.strip()
    
    def _parse_batch_response(self, response: str, columns: List[Dict]) -> List[str]:
        """Parse LLM batch response into individual descriptions"""
        if not response or not columns:
            return []
        
        # Try to split by numbered lines
        lines = response.strip().split('\n')
        descriptions = []
        
        for line in lines:
            line = line.strip()
            if line and (line[0].isdigit() or line.startswith('-') or line.startswith('*')):
                # Remove numbering/bullets
                clean_line = re.sub(r'^\d+\.?\s*', '', line)
                clean_line = re.sub(r'^[-*]\s*', '', clean_line)
                if clean_line:
                    descriptions.append(clean_line.strip())
        
        # If parsing failed, split by sentences
        if len(descriptions) < len(columns):
            sentences = response.replace('\n', ' ').split('.')
            descriptions = [s.strip() + '.' for s in sentences if s.strip()]
        
        # Ensure we have enough descriptions
        while len(descriptions) < len(columns):
            descriptions.append(f"Contains {columns[len(descriptions)]['column_name'].replace('_', ' ')} information.")
        
        return descriptions[:len(columns)]
    
    def _calculate_confidence_score(self, description: str, context: Dict, object_type: str, pii_analysis: Dict = None) -> float:
        """Calculate confidence score for generated description"""
        if not description or description.strip() == "":
            return 0.0
        
        score = 0.5  # Base score
        
        # Length and detail bonus
        if len(description) > 50:
            score += 0.1
        if len(description) > 100:
            score += 0.1
        
        # Context usage bonus
        if object_type == 'schema' and context.get('table_names'):
            score += 0.1
        elif object_type == 'table' and context.get('column_names'):
            score += 0.1
        elif object_type == 'column' and context.get('sample_values'):
            score += 0.1
        
        # PII analysis bonus
        if pii_analysis and pii_analysis.get('pii_types'):
            score += 0.1
        
        # Avoid generic descriptions penalty
        generic_phrases = ['contains', 'stores', 'information', 'data']
        if sum(phrase in description.lower() for phrase in generic_phrases) > 2:
            score -= 0.1
        
        return min(1.0, max(0.0, score))
    
    def _create_optimal_chunks(self, columns: List[Dict]) -> List[List[Dict]]:
        """Create optimal chunks for column processing"""
        if not columns:
            return []
        
        chunk_size = self.config['max_chunk_size']
        chunks = []
        
        for i in range(0, len(columns), chunk_size):
            chunk = columns[i:i + chunk_size]
            chunks.append(chunk)
        
        return chunks
    
    async def _get_tables_in_schema(self, catalog_name: str, schema_name: str) -> List[Dict]:
        """Get tables in a schema for context"""
        try:
            # Use the Unity service to get tables in the schema
            tables = self.unity_service.get_tables_with_missing_metadata(catalog_name)
            # Filter to only tables in this specific schema
            schema_tables = [t for t in tables if t.get('schema_name') == schema_name]
            
            # Also get tables that already have metadata (not just missing ones)
            try:
                # Call Unity Catalog API directly for all tables in schema
                token = self.unity_service._get_oauth_token()
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                url = f"https://{self.unity_service.workspace_host}/api/2.1/unity-catalog/tables"
                params = {'catalog_name': catalog_name, 'schema_name': schema_name}
                
                import requests
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                all_tables = response.json().get('tables', [])
                
                logger.info(f"Found {len(all_tables)} tables in schema {catalog_name}.{schema_name}")
                return all_tables
                
            except Exception as api_error:
                logger.warning(f"Could not get all tables via API for {catalog_name}.{schema_name}: {api_error}")
                # Fallback to just the missing metadata tables
                return schema_tables
                
        except Exception as e:
            logger.error(f"Error getting tables for schema {catalog_name}.{schema_name}: {e}")
            return []
    
    async def _get_table_columns_with_samples(self, catalog_name: str, schema_name: str, table_name: str) -> List[Dict]:
        """Get table columns with sample data"""
        # This would call Unity Catalog API and sample data - placeholder for now
        return []
    
    async def _enrich_columns_with_samples(self, columns: List[Dict]) -> List[Dict]:
        """Enrich column information with sample data"""
        # This would sample actual data - placeholder for now
        return columns
    
    def _get_selected_schemas_metadata(self, catalog_name: str, selected_schema_names: List[str]) -> List[Dict]:
        """Get metadata for only the selected schemas (optimized)"""
        if not selected_schema_names:
            return []
        
        try:
            # Build SQL query for only the selected schemas
            schema_list = "', '".join(selected_schema_names)
            sql_query = f"""
                SELECT schema_name, schema_owner, created, last_altered
                FROM system.information_schema.schemata
                WHERE catalog_name = '{catalog_name}' 
                AND schema_name IN ('{schema_list}')
                AND (comment IS NULL OR comment = '' OR LENGTH(TRIM(comment)) = 0)
            """
            
            data = self.unity_service._execute_sql_warehouse(sql_query)
            missing_metadata = []
            
            for row in data:
                schema_name = row[0] if len(row) > 0 else ''
                schema_owner = row[1] if len(row) > 1 else ''
                created = row[2] if len(row) > 2 else None
                updated = row[3] if len(row) > 3 else None
                
                missing_metadata.append({
                    'name': schema_name,
                    'full_name': f"{catalog_name}.{schema_name}",
                    'catalog_name': catalog_name,
                    'comment': '',
                    'owner': schema_owner,
                    'created_at': created,
                    'updated_at': updated
                })
            
            logger.info(f"🚀 Found {len(missing_metadata)} selected schemas with missing descriptions using optimized SQL")
            return missing_metadata
            
        except Exception as e:
            logger.warning(f"Optimized schema query failed, using fallback: {e}")
            # Fallback to full scan + filter
            all_schemas = self.unity_service.get_schemas_with_missing_metadata(catalog_name)
            return [s for s in all_schemas if s['name'] in selected_schema_names]
    
    def _get_selected_tables_metadata(self, catalog_name: str, selected_table_names: List[str]) -> List[Dict]:
        """Get metadata for only the selected tables (optimized)"""
        if not selected_table_names:
            return []
        
        try:
            # Build SQL query for only the selected tables
            table_conditions = []
            for full_name in selected_table_names:
                parts = full_name.split('.')
                if len(parts) >= 3:
                    catalog, schema, table = parts[0], parts[1], parts[2]
                    table_conditions.append(f"(table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}')")
            
            if not table_conditions:
                return []
            
            conditions_sql = " OR ".join(table_conditions)
            sql_query = f"""
                SELECT table_catalog, table_schema, table_name, table_type, table_owner, created, last_altered
                FROM system.information_schema.tables
                WHERE ({conditions_sql})
                AND (comment IS NULL OR comment = '' OR LENGTH(TRIM(comment)) = 0)
            """
            
            data = self.unity_service._execute_sql_warehouse(sql_query)
            missing_metadata = []
            
            for row in data:
                catalog = row[0] if len(row) > 0 else ''
                schema_name = row[1] if len(row) > 1 else ''
                table_name = row[2] if len(row) > 2 else ''
                table_type = row[3] if len(row) > 3 else ''
                owner = row[4] if len(row) > 4 else ''
                created = row[5] if len(row) > 5 else None
                updated = row[6] if len(row) > 6 else None
                
                missing_metadata.append({
                    'name': table_name,
                    'full_name': f"{catalog}.{schema_name}.{table_name}",
                    'catalog_name': catalog,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'table_type': table_type,
                    'comment': '',
                    'owner': owner,
                    'created_at': created,
                    'updated_at': updated
                })
            
            logger.info(f"🚀 Found {len(missing_metadata)} selected tables with missing descriptions using optimized SQL")
            return missing_metadata
            
        except Exception as e:
            logger.warning(f"Optimized table query failed, using fallback: {e}")
            # Fallback to full scan + filter
            all_tables = self.unity_service.get_tables_with_missing_metadata(catalog_name)
            return [t for t in all_tables if t.get('full_name', '') in selected_table_names]
    
    def _get_selected_columns_metadata(self, catalog_name: str, selected_column_names: List[str]) -> List[Dict]:
        """Get metadata for only the selected columns (optimized)"""
        if not selected_column_names:
            return []
        
        try:
            # Build SQL query for only the selected columns
            column_conditions = []
            for full_name in selected_column_names:
                parts = full_name.split('.')
                if len(parts) >= 4:
                    catalog, schema, table, column = parts[0], parts[1], parts[2], parts[3]
                    column_conditions.append(f"(table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}' AND column_name = '{column}')")
            
            if not column_conditions:
                return []
            
            conditions_sql = " OR ".join(column_conditions)
            sql_query = f"""
                SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable, column_default
                FROM system.information_schema.columns
                WHERE ({conditions_sql})
                AND (comment IS NULL OR comment = '' OR LENGTH(TRIM(comment)) = 0)
            """
            
            data = self.unity_service._execute_sql_warehouse(sql_query)
            missing_metadata = []
            
            for row in data:
                catalog = row[0] if len(row) > 0 else ''
                schema_name = row[1] if len(row) > 1 else ''
                table_name = row[2] if len(row) > 2 else ''
                column_name = row[3] if len(row) > 3 else ''
                data_type = row[4] if len(row) > 4 else ''
                is_nullable = row[5] if len(row) > 5 else ''
                column_default = row[6] if len(row) > 6 else ''
                
                missing_metadata.append({
                    'name': column_name,
                    'full_name': f"{catalog}.{schema_name}.{table_name}.{column_name}",
                    'catalog_name': catalog,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'column_name': column_name,
                    'data_type': data_type,
                    'is_nullable': is_nullable,
                    'column_default': column_default,
                    'comment': ''
                })
            
            logger.info(f"🚀 Found {len(missing_metadata)} selected columns with missing comments using optimized SQL")
            return missing_metadata
            
        except Exception as e:
            logger.warning(f"Optimized column query failed, using fallback: {e}")
            # Fallback to full scan + filter
            all_columns = self.unity_service.get_columns_with_missing_metadata(catalog_name)
            return [c for c in all_columns if c.get('full_name', '') in selected_column_names]

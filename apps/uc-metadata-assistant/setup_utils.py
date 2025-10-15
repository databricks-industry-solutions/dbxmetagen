"""
Automatic Setup Utilities
=========================

Self-contained utilities for automatic setup of schemas, tables, and permissions.
Eliminates the need for manual configuration by users.
"""

import logging
import json
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class AutoSetupManager:
    """
    Manages automatic setup of all required infrastructure for enhanced metadata generation.
    Creates schemas, tables, grants permissions, and validates configuration.
    """
    
    def __init__(self, unity_service):
        self.unity_service = unity_service
        self.default_config = {
            'output_catalog': 'uc_metadata_assistant',
            'output_schema': 'generated_metadata',
            'quality_schema': 'quality_metrics',
            'cache_schema': 'cache',
            'results_table': 'metadata_results',
            'audit_table': 'generation_audit'
        }
        # Session-level cache for setup validation (5 minute TTL)
        self._setup_cache = None
        self._setup_cache_timestamp = None
        self._setup_cache_ttl = 300  # 5 minutes in seconds
    
    async def ensure_setup_complete(self, config: Optional[Dict] = None, force_refresh: bool = False) -> Dict:
        """
        Ensure all required setup is complete. Creates infrastructure if needed.
        Uses session-level caching to avoid redundant checks within 5 minutes.
        
        Args:
            config: Optional configuration override
            force_refresh: If True, bypass cache and revalidate infrastructure
            
        Returns:
            Dict with setup status and details
        """
        # Check cache first (unless force_refresh is True)
        if not force_refresh and self._setup_cache is not None and self._setup_cache_timestamp is not None:
            cache_age = (datetime.now() - self._setup_cache_timestamp).total_seconds()
            if cache_age < self._setup_cache_ttl:
                logger.info(f"✅ Using cached setup status (age: {int(cache_age)}s)")
                return self._setup_cache
        
        setup_config = {**self.default_config, **(config or {})}
        
        setup_status = {
            'setup_complete': False,
            'catalog_ready': False,
            'schema_ready': False,
            'tables_ready': False,
            'permissions_ready': False,
            'errors': [],
            'created_objects': [],
            'config': setup_config,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("🔧 Running full infrastructure validation...")
        
        try:
            # Step 1: Ensure output catalog exists
            catalog_status = await self._ensure_catalog_exists(setup_config['output_catalog'])
            setup_status['catalog_ready'] = catalog_status['exists']
            if catalog_status.get('created'):
                setup_status['created_objects'].append(f"Catalog: {setup_config['output_catalog']}")
            
            # Step 2: Ensure all required schemas exist
            schemas_to_create = [
                ('output_schema', 'generated_metadata'),
                ('quality_schema', 'quality_metrics'),
                ('cache_schema', 'cache')
            ]
            
            all_schemas_ready = True
            for config_key, default_name in schemas_to_create:
                schema_name = setup_config.get(config_key, default_name)
                schema_status = await self._ensure_schema_exists(
                    setup_config['output_catalog'], 
                    schema_name
                )
                if not schema_status['exists']:
                    all_schemas_ready = False
                    setup_status['errors'].append(f"Failed to create schema: {setup_config['output_catalog']}.{schema_name}")
                elif schema_status.get('created'):
                    setup_status['created_objects'].append(f"Schema: {setup_config['output_catalog']}.{schema_name}")
            
            setup_status['schema_ready'] = all_schemas_ready
            
            # Step 3: Ensure tables exist with proper schema
            tables_status = await self._ensure_tables_exist(setup_config)
            setup_status['tables_ready'] = tables_status['all_exist']
            setup_status['created_objects'].extend(tables_status.get('created_objects', []))
            
            # Step 4: Validate permissions (attempt basic operations)
            permissions_status = await self._validate_permissions(setup_config)
            setup_status['permissions_ready'] = permissions_status['valid']
            if permissions_status.get('errors'):
                setup_status['errors'].extend(permissions_status['errors'])
            
            # Overall status
            setup_status['setup_complete'] = all([
                setup_status['catalog_ready'],
                setup_status['schema_ready'], 
                setup_status['tables_ready'],
                setup_status['permissions_ready']
            ])
            
        except Exception as e:
            logger.error(f"Error during setup validation: {e}")
            setup_status['errors'].append(f"Setup validation failed: {str(e)}")
        
        logger.info(f"Setup status: {setup_status['setup_complete']}, Created: {len(setup_status['created_objects'])}")
        
        # Cache the result for future calls (only if successful)
        if setup_status['setup_complete']:
            self._setup_cache = setup_status
            self._setup_cache_timestamp = datetime.now()
            logger.info(f"✅ Cached setup status for {self._setup_cache_ttl}s")
        
        return setup_status
    
    async def _ensure_catalog_exists(self, catalog_name: str) -> Dict:
        """Ensure catalog exists, create if needed"""
        try:
            # Check if catalog exists
            catalogs = self.unity_service.get_catalogs()
            existing_catalog = next((c for c in catalogs if c['name'] == catalog_name), None)
            
            if existing_catalog:
                logger.info(f"Catalog {catalog_name} already exists")
                return {'exists': True, 'created': False}
            
            # Create catalog - try different storage approaches
            approaches = [
                # Approach 1: Try REST API creation (bypasses SQL storage issues)
                {
                    'method': 'rest_api',
                    'description': 'REST API with default storage'
                },
                
                # Approach 2: Try with managed storage default (most common)
                {
                    'sql': f"CREATE CATALOG IF NOT EXISTS {catalog_name} MANAGED LOCATION 'default' COMMENT 'Auto-created catalog for UC Metadata Assistant'",
                    'description': 'managed storage default'
                },
                
                # Approach 3: Try without storage location (uses workspace default storage)
                {
                    'sql': f"CREATE CATALOG IF NOT EXISTS {catalog_name} COMMENT 'Auto-created catalog for UC Metadata Assistant'",
                    'description': 'workspace default storage'
                },
                
                # Approach 4: Try without IF NOT EXISTS (in case that's causing issues)
                {
                    'sql': f"CREATE CATALOG {catalog_name} COMMENT 'Auto-created catalog for UC Metadata Assistant'",
                    'description': 'direct creation'
                }
            ]
            
            for i, approach in enumerate(approaches, 1):
                description = approach['description']
                
                logger.info(f"🔧 Attempting catalog creation approach {i} ({description})")
                
                # Handle REST API approach
                if approach.get('method') == 'rest_api':
                    try:
                        result = await self._create_catalog_via_rest_api(catalog_name)
                        if result['success']:
                            logger.info(f"✅ Successfully created catalog: {catalog_name} using {description}")
                            return {'exists': True, 'created': True}
                        else:
                            error_msg = result.get('error', 'Unknown error')
                            logger.warning(f"❌ Approach {i} ({description}) failed: {error_msg}")
                            
                            # If it's a "catalog already exists" error, that's actually success
                            if 'already exists' in error_msg.lower() or 'catalog_already_exists' in error_msg.lower():
                                logger.info(f"✅ Catalog {catalog_name} already exists (detected from error message)")
                                return {'exists': True, 'created': False}
                    except Exception as e:
                        logger.warning(f"❌ REST API approach failed: {e}")
                else:
                    # Handle SQL approach
                    sql_statement = approach['sql']
                    logger.debug(f"SQL: {sql_statement}")
                    
                    result = await self._execute_sql(sql_statement)
                    
                    if result['success']:
                        logger.info(f"✅ Successfully created catalog: {catalog_name} using {description}")
                        return {'exists': True, 'created': True}
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        logger.warning(f"❌ Approach {i} ({description}) failed: {error_msg}")
                        
                        # If it's a "catalog already exists" error, that's actually success
                        if 'already exists' in error_msg.lower() or 'catalog_already_exists' in error_msg.lower():
                            logger.info(f"✅ Catalog {catalog_name} already exists (detected from error message)")
                            return {'exists': True, 'created': False}
                
                if i < len(approaches):
                    logger.info(f"🔄 Trying approach {i+1}...")
            
            # If all approaches failed, return detailed error with user guidance
            logger.error(f"❌ All {len(approaches)} catalog creation approaches failed for {catalog_name}")
            error_msg = (
                f"❌ CATALOG CREATION FAILED: Unable to automatically create catalog '{catalog_name}'. "
                f"This workspace requires manual catalog creation. "
                f"Please create the catalog manually in the Databricks UI:\n"
                f"1. Go to Catalog → Create Catalog\n"
                f"2. Name: {catalog_name}\n"
                f"3. Use Default Storage\n"
                f"4. Click Create\n"
                f"Then retry the metadata generation."
            )
            return {'exists': False, 'created': False, 'error': error_msg}
                
        except Exception as e:
            logger.error(f"Error ensuring catalog {catalog_name}: {e}")
            return {'exists': False, 'created': False, 'error': str(e)}
    
    async def _create_catalog_via_rest_api(self, catalog_name: str) -> Dict:
        """Create catalog using REST API (bypasses SQL storage issues)"""
        try:
            import requests
            
            token = self.unity_service._get_oauth_token()
            workspace_host = self.unity_service.workspace_host
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Use Unity Catalog REST API to create catalog
            url = f"https://{workspace_host}/api/2.1/unity-catalog/catalogs"
            
            # Try different payload structures for different workspace configurations
            payloads = [
                # Payload 1: Minimal payload (let Databricks handle storage)
                {
                    'name': catalog_name,
                    'comment': 'Auto-created catalog for UC Metadata Assistant'
                },
                
                # Payload 2: Explicit default storage request
                {
                    'name': catalog_name,
                    'comment': 'Auto-created catalog for UC Metadata Assistant',
                    'storage_root': '',  # Empty string for default
                    'properties': {}
                },
                
                # Payload 3: Force managed location
                {
                    'name': catalog_name,
                    'comment': 'Auto-created catalog for UC Metadata Assistant',
                    'properties': {
                        'managed_location': 'default'
                    }
                }
            ]
            
            for j, payload in enumerate(payloads, 1):
                logger.info(f"🌐 Creating catalog via REST API (payload {j}): {url}")
                logger.debug(f"Payload: {payload}")
                
                response = requests.post(url, headers=headers, json=payload)
                
                if response.status_code == 200 or response.status_code == 201:
                    logger.info(f"✅ REST API catalog creation successful with payload {j}")
                    return {'success': True}
                elif response.status_code == 409:
                    # Catalog already exists
                    logger.info(f"✅ Catalog already exists (409 response)")
                    return {'success': True}
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    logger.warning(f"❌ REST API payload {j} failed: {error_msg}")
                    if j < len(payloads):
                        logger.info(f"🔄 Trying REST API payload {j+1}...")
                        continue
            
            # If all REST API payloads failed
            return {'success': False, 'error': 'All REST API payload approaches failed'}
                
        except Exception as e:
            logger.error(f"❌ Exception in REST API catalog creation: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _ensure_schema_exists(self, catalog_name: str, schema_name: str) -> Dict:
        """Ensure schema exists, create if needed"""
        try:
            # Check if schema exists
            try:
                schemas = self.unity_service.get_schemas_with_missing_metadata(catalog_name)
                # If this call succeeds, we can access the catalog, now check for our specific schema
                
                sql_check = f"SHOW SCHEMAS IN {catalog_name}"
                check_result = await self._execute_sql(sql_check)
                
                if check_result['success']:
                    existing_schemas = [row[0] for row in check_result.get('data', [])]
                    if schema_name in existing_schemas:
                        logger.info(f"Schema {catalog_name}.{schema_name} already exists")
                        return {'exists': True, 'created': False}
                
            except Exception as e:
                logger.warning(f"Could not check existing schemas: {e}")
            
            # Create schema
            sql_statement = f"""
                CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
                COMMENT 'Auto-created schema for UC Metadata Assistant enhanced generation and audit data'
            """
            
            result = await self._execute_sql(sql_statement)
            
            if result['success']:
                logger.info(f"Created schema: {catalog_name}.{schema_name}")
                return {'exists': True, 'created': True}
            else:
                logger.error(f"Failed to create schema: {result.get('error')}")
                return {'exists': False, 'created': False, 'error': result.get('error')}
                
        except Exception as e:
            logger.error(f"Error ensuring schema {catalog_name}.{schema_name}: {e}")
            return {'exists': False, 'created': False, 'error': str(e)}
    
    async def _ensure_tables_exist(self, config: Dict) -> Dict:
        """Ensure all required tables exist with proper schema"""
        catalog_name = config['output_catalog']
        schema_name = config['output_schema']
        
        results = {
            'all_exist': True,
            'created_objects': [],
            'errors': []
        }
        
        # Define table schemas
        table_definitions = {
            config['results_table']: self._get_results_table_ddl(catalog_name, schema_name, config['results_table']),
            config['audit_table']: self._get_audit_table_ddl(catalog_name, schema_name, config['audit_table'])
        }
        
        for table_name, ddl in table_definitions.items():
            try:
                full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
                
                # Check if table already exists first
                table_exists = await self._table_exists(catalog_name, schema_name, table_name)
                
                if table_exists:
                    logger.info(f"✅ Table up to date: {full_table_name}")
                    
                    # For existing results table, ensure proposed_policy_tags column exists
                    if table_name == config['results_table']:
                        await self._ensure_results_table_schema(full_table_name, results)
                else:
                    # Table doesn't exist, create it
                    result = await self._execute_sql(ddl)
                    
                    if result['success']:
                        results['created_objects'].append(f"Table: {full_table_name}")
                        logger.info(f"Created table: {full_table_name}")
                    else:
                        error_msg = f"Failed to create table {full_table_name}: {result.get('message', 'Unknown error')}"
                        results['errors'].append(error_msg)
                        logger.error(f"❌ {error_msg}")
                    
            except Exception as e:
                results['all_exist'] = False
                error_msg = f"Error creating table {table_name}: {str(e)}"
                results['errors'].append(error_msg)
                logger.error(error_msg)
        
        return results
    
    async def _ensure_results_table_schema(self, full_table_name: str, results: Dict) -> None:
        """Ensure results table has all required columns (schema migration)"""
        try:
            # Check if proposed_policy_tags column exists
            describe_sql = f"DESCRIBE TABLE {full_table_name}"
            describe_result = await self._execute_sql(describe_sql)
            
            if describe_result['success']:
                columns = [row[0].lower() for row in describe_result.get('data', [])]
                
                if 'proposed_policy_tags' not in columns:
                    # Add missing column
                    alter_sql = f"""
                        ALTER TABLE {full_table_name} 
                        ADD COLUMN proposed_policy_tags STRING 
                        COMMENT 'JSON array of proposed policy tags for manual review'
                    """
                    
                    alter_result = await self._execute_sql(alter_sql)
                    
                    if alter_result['success']:
                        # Handle different results dict structures
                        if 'created_objects' in results:
                            results['created_objects'].append(f"Column: {full_table_name}.proposed_policy_tags")
                        logger.info(f"✅ Added proposed_policy_tags column to {full_table_name}")
                    else:
                        error_msg = f"Failed to add proposed_policy_tags column: {alter_result.get('error')}"
                        if 'errors' in results:
                            results['errors'].append(error_msg)
                        logger.error(f"❌ {error_msg}")
                else:
                    logger.info(f"✅ Schema up to date: {full_table_name}")
            else:
                error_msg = f"Failed to describe table {full_table_name}: {describe_result.get('error')}"
                if 'errors' in results:
                    results['errors'].append(error_msg)
                logger.error(f"❌ {error_msg}")
                
        except Exception as e:
            error_msg = f"Error checking table schema for {full_table_name}: {str(e)}"
            if 'errors' in results:
                results['errors'].append(error_msg)
            logger.error(f"❌ {error_msg}")
    
    async def _table_exists(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            check_sql = f"""
                SELECT COUNT(*) as table_count 
                FROM system.information_schema.tables 
                WHERE table_catalog = '{catalog_name}' 
                AND table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            """
            result = await self._execute_sql(check_sql)
            
            if result['success'] and result.get('data'):
                count = int(result['data'][0][0]) if result['data'] and len(result['data']) > 0 else 0
                return count > 0
            return False
        except Exception as e:
            logger.warning(f"Could not check if table exists {catalog_name}.{schema_name}.{table_name}: {e}")
            return False
    
    def _get_results_table_ddl(self, catalog_name: str, schema_name: str, table_name: str) -> str:
        """Get DDL for metadata results table"""
        return f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
                run_id STRING COMMENT 'Unique identifier for the generation run',
                full_name STRING COMMENT 'Full object name (catalog.schema.table or catalog.schema.table.column)',
                object_type STRING COMMENT 'Type of object: schema, table, or column',
                proposed_comment STRING COMMENT 'AI-generated description/comment',
                confidence_score DOUBLE COMMENT 'Confidence score for the generated content (0.0 to 1.0)',
                pii_tags STRING COMMENT 'JSON array of detected PII types',
                policy_tags STRING COMMENT 'JSON array of automatically applied policy tags (empty for manual review)',
                proposed_policy_tags STRING COMMENT 'JSON array of proposed policy tags for manual review',
                data_classification STRING COMMENT 'Data classification level (PUBLIC, PII, PHI, etc.)',
                source_model STRING COMMENT 'LLM model used for generation',
                generation_style STRING COMMENT 'Generation style used (technical, business, etc.)',
                pii_detected BOOLEAN COMMENT 'Whether PII was detected in this object',
                context_used STRING COMMENT 'JSON of context information used during generation',
                pii_analysis STRING COMMENT 'JSON of detailed PII analysis results',
                generated_at TIMESTAMP COMMENT 'When the metadata was generated',
                submitted_at TIMESTAMP COMMENT 'When the metadata was submitted to Unity Catalog',
                status STRING COMMENT 'Status: generated, reviewed, submitted, error',
                error_message STRING COMMENT 'Error message if submission failed',
                created_by STRING COMMENT 'User or service that created this entry'
            )
            USING DELTA
            PARTITIONED BY (object_type)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            COMMENT 'Enhanced metadata generation results with PII detection and analysis'
        """
    
    def _get_audit_table_ddl(self, catalog_name: str, schema_name: str, table_name: str) -> str:
        """Get DDL for audit/history table"""
        return f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
                audit_id STRING COMMENT 'Unique identifier for this audit entry',
                run_id STRING COMMENT 'Associated generation run ID',
                event_type STRING COMMENT 'Type of event: generation_started, generation_completed, submission_started, etc.',
                catalog_name STRING COMMENT 'Target catalog name',
                object_count INT COMMENT 'Number of objects processed',
                success_count INT COMMENT 'Number of successful operations',
                error_count INT COMMENT 'Number of failed operations',
                pii_detected_count INT COMMENT 'Number of objects with PII detected',
                high_confidence_count INT COMMENT 'Number of high-confidence results',
                model_used STRING COMMENT 'LLM model used',
                style_used STRING COMMENT 'Generation style used',
                config_used STRING COMMENT 'JSON of configuration parameters',
                duration_seconds DOUBLE COMMENT 'Duration of the operation in seconds',
                event_timestamp TIMESTAMP COMMENT 'When this event occurred',
                created_by STRING COMMENT 'User or service that triggered this event',
                additional_info STRING COMMENT 'JSON of additional event-specific information'
            )
            USING DELTA
            PARTITIONED BY (event_type)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            COMMENT 'Audit trail for all enhanced metadata generation activities'
        """
    
    async def _validate_permissions(self, config: Dict) -> Dict:
        """Validate that we have necessary permissions"""
        catalog_name = config['output_catalog']
        schema_name = config['output_schema']
        
        validation_results = {
            'valid': True,
            'errors': [],
            'permissions_checked': []
        }
        
        # Test basic operations
        tests = [
            {
                'name': 'catalog_access',
                'sql': f"SHOW SCHEMAS IN {catalog_name}",
                'description': 'Access to catalog'
            },
            {
                'name': 'schema_access', 
                'sql': f"SHOW TABLES IN {catalog_name}.{schema_name}",
                'description': 'Access to schema'
            },
            {
                'name': 'table_select',
                'sql': f"SELECT COUNT(*) FROM {catalog_name}.{schema_name}.{config['results_table']} LIMIT 1",
                'description': 'SELECT permission on results table'
            }
        ]
        
        for test in tests:
            try:
                result = await self._execute_sql(test['sql'])
                
                if result['success']:
                    validation_results['permissions_checked'].append(f"[OK] {test['description']}")
                    logger.debug(f"Permission test passed: {test['name']}")
                else:
                    validation_results['valid'] = False
                    error_msg = f"Permission test failed - {test['description']}: {result.get('error')}"
                    validation_results['errors'].append(error_msg)
                    logger.warning(error_msg)
                    
            except Exception as e:
                validation_results['valid'] = False
                error_msg = f"Permission test error - {test['description']}: {str(e)}"
                validation_results['errors'].append(error_msg)
                logger.warning(error_msg)
        
        return validation_results
    
    async def _execute_sql(self, sql_statement: str) -> Dict:
        """Execute SQL statement and return standardized result"""
        try:
            token = self.unity_service._get_oauth_token()
            workspace_host = self.unity_service.workspace_host
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Get warehouse ID from environment variables (portable across workspaces)
            warehouse_id = self._get_warehouse_id()
            
            payload = {
                'statement': sql_statement,
                'warehouse_id': warehouse_id
            }
            
            url = f"https://{workspace_host}/api/2.0/sql/statements"
            
            import requests
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            result = response.json()
            
            # Check execution status
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                return {
                    'success': True,
                    'data': result.get('result', {}).get('data_array', []),
                    'message': 'SQL executed successfully'
                }
            else:
                error_info = result.get('status', {}).get('error', {})
                return {
                    'success': False,
                    'error': error_info.get('message', 'Unknown SQL error'),
                    'details': error_info
                }
                
        except Exception as e:
            logger.error(f"SQL execution error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _get_warehouse_id(self) -> str:
        """Get warehouse ID from environment variables (portable across workspaces)"""
        import os
        
        # Try multiple environment variable sources
        warehouse_id = None
        
        # Method 1: Direct warehouse ID environment variable
        warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID')
        if warehouse_id:
            logger.debug(f"Found warehouse ID from DATABRICKS_WAREHOUSE_ID: {warehouse_id}")
            return warehouse_id
        
        # Method 2: Extract from HTTP_PATH environment variable
        http_path = os.environ.get('DATABRICKS_HTTP_PATH')
        if http_path and '/warehouses/' in http_path:
            warehouse_id = http_path.split('/warehouses/')[-1]
            logger.debug(f"Extracted warehouse ID from DATABRICKS_HTTP_PATH: {warehouse_id}")
            return warehouse_id
        
        # Method 3: Get from unity service if available
        if hasattr(self, 'unity_service') and hasattr(self.unity_service, 'warehouse_id'):
            warehouse_id = self.unity_service.warehouse_id
            if warehouse_id:
                logger.debug(f"Found warehouse ID from unity service: {warehouse_id}")
                return warehouse_id
        
        # Method 4: Try to extract from any environment variable containing warehouse info
        for key, value in os.environ.items():
            if 'warehouse' in key.lower() and value:
                if '/warehouses/' in value:
                    warehouse_id = value.split('/warehouses/')[-1]
                    logger.debug(f"Extracted warehouse ID from {key}: {warehouse_id}")
                    return warehouse_id
                elif len(value) == 16 and value.replace('-', '').replace('_', '').isalnum():
                    # Looks like a warehouse ID format
                    logger.debug(f"Found potential warehouse ID from {key}: {warehouse_id}")
                    return value
        
        # Fallback: Log warning and return None (will cause error, which is better than silent failure)
        logger.error("Could not determine warehouse ID from environment variables. Please set DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH.")
        logger.error("Available environment variables: " + ", ".join([k for k in os.environ.keys() if 'databricks' in k.lower() or 'warehouse' in k.lower()]))
        raise ValueError("Warehouse ID not found in environment variables. Cannot execute SQL queries.")

    def get_current_user(self) -> str:
        """Get the current user from Databricks context"""
        try:
            # Try to get user from SQL Warehouse context
            result = self._execute_sql_sync("SELECT current_user() as user")
            if result.get('success') and result.get('data'):
                user = result['data'][0][0] if result['data'] else None
                if user:
                    # Clean up the user string (remove domain if present)
                    if '@' in user:
                        user = user.split('@')[0]
                    return user
        except Exception as e:
            logger.debug(f"Could not get current user from SQL: {e}")
        
        # Fallback: try to get from OAuth token context
        try:
            token = self.unity_service._get_oauth_token()
            # This is a basic fallback - in a real implementation you might decode the JWT token
            return "User"
        except Exception as e:
            logger.debug(f"Could not get user from token: {e}")
            return "User"
    
    def _execute_sql_sync(self, sql_statement: str) -> Dict:
        """Synchronous version of _execute_sql for simple queries"""
        try:
            token = self.unity_service._get_oauth_token()
            workspace_host = self.unity_service.workspace_host
            
            import requests
            import time
            
            # Submit SQL statement
            submit_url = f"https://{workspace_host}/api/2.0/sql/statements/"
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'statement': sql_statement,
                'warehouse_id': self._get_warehouse_id()
            }
            
            response = requests.post(submit_url, headers=headers, json=payload)
            response.raise_for_status()
            statement_id = response.json()['statement_id']
            
            # Poll for completion (simple version)
            for _ in range(30):  # Max 30 seconds
                status_url = f"https://{workspace_host}/api/2.0/sql/statements/{statement_id}"
                result_response = requests.get(status_url, headers=headers)
                result_response.raise_for_status()
                result = result_response.json()
                
                state = result.get('status', {}).get('state')
                if state == 'SUCCEEDED':
                    return {
                        'success': True,
                        'data': result.get('result', {}).get('data_array', []),
                        'message': 'SQL executed successfully'
                    }
                elif state in ['FAILED', 'CANCELED']:
                    return {
                        'success': False,
                        'error': result.get('status', {}).get('error', {}).get('message', 'Query failed')
                    }
                
                time.sleep(1)
            
            return {
                'success': False,
                'error': 'Query timeout'
            }
            
        except Exception as e:
            logger.error(f"Sync SQL execution error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_username_from_id(self, user_id: str) -> str:
        """Try to resolve a numeric user ID to an actual username"""
        try:
            # Try using the Databricks Users API to get user info
            token = self.unity_service._get_oauth_token()
            workspace_host = self.unity_service.workspace_host
            
            import requests
            
            # Try the SCIM Users API
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Try to get user by ID
            user_url = f"https://{workspace_host}/api/2.0/preview/scim/v2/Users/{user_id}"
            response = requests.get(user_url, headers=headers)
            
            if response.status_code == 200:
                user_data = response.json()
                # Try to get username from various fields
                username = (
                    user_data.get('userName') or 
                    user_data.get('displayName') or
                    user_data.get('name', {}).get('givenName', '') + '.' + user_data.get('name', {}).get('familyName', '')
                ).strip('.')
                
                if username and '@' in username:
                    username = username.split('@')[0]
                    
                logger.info(f"Successfully resolved user ID {user_id} to {username}")
                return username
            else:
                logger.debug(f"SCIM API returned {response.status_code} for user ID {user_id}")
                
        except Exception as e:
            logger.debug(f"Failed to resolve user ID {user_id} via SCIM API: {e}")
        
        # If SCIM fails, return None to trigger fallback
        return None
    
    async def log_generation_event(self, event_data: Dict, config: Optional[Dict] = None) -> bool:
        """Log a generation event to the audit table"""
        setup_config = {**self.default_config, **(config or {})}
        audit_table = f"{setup_config['output_catalog']}.{setup_config['output_schema']}.{setup_config['audit_table']}"
        
        try:
            # Prepare audit data
            audit_entry = {
                'audit_id': f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{event_data.get('event_type', 'unknown')}",
                'run_id': event_data.get('run_id', ''),
                'event_type': event_data.get('event_type', 'unknown'),
                'catalog_name': event_data.get('catalog_name', ''),
                'object_count': event_data.get('object_count', 0),
                'success_count': event_data.get('success_count', 0),
                'error_count': event_data.get('error_count', 0),
                'pii_detected_count': event_data.get('pii_detected_count', 0),
                'high_confidence_count': event_data.get('high_confidence_count', 0),
                'model_used': event_data.get('model_used', ''),
                'style_used': event_data.get('style_used', ''),
                'config_used': json.dumps(event_data.get('config_used', {})),
                'duration_seconds': event_data.get('duration_seconds', 0.0),
                'event_timestamp': datetime.now().isoformat(),
                'created_by': event_data.get('created_by', 'uc_metadata_assistant'),
                'additional_info': json.dumps(event_data.get('additional_info', {}))
            }
            
            # Build INSERT statement
            columns = ', '.join(audit_entry.keys())
            values = ', '.join([f"'{str(v)}'" if v is not None else 'NULL' for v in audit_entry.values()])
            
            sql_statement = f"""
                INSERT INTO {audit_table} ({columns})
                VALUES ({values})
            """
            
            result = await self._execute_sql(sql_statement)
            
            if result['success']:
                logger.info(f"Logged audit event: {audit_entry['event_type']} for run {audit_entry['run_id']}")
                return True
            else:
                logger.error(f"Failed to log audit event: {result.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"Error logging audit event: {e}")
            return False
    
    async def save_generation_results(self, results: List[Dict], config: Optional[Dict] = None) -> Dict:
        """Save generation results to the results table"""
        setup_config = {**self.default_config, **(config or {})}
        results_table = f"{setup_config['output_catalog']}.{setup_config['output_schema']}.{setup_config['results_table']}"
        
        save_status = {
            'success': False,
            'saved_count': 0,
            'error_count': 0,
            'errors': []
        }
        
        if not results:
            return save_status
        
        # Debug: Log what we're trying to save
        logger.info(f"💾 Saving {len(results)} results to {results_table}")
        
        try:
            # Ensure complete infrastructure exists before saving (uses cache if available)
            setup_status = await self.ensure_setup_complete()
            if not setup_status.get('setup_complete', False):
                logger.warning(f"⚠️ Setup not complete: {setup_status}")
                # Try to continue anyway, but log the issue
            
            # Ensure table schema is up to date before saving
            await self._ensure_results_table_schema(results_table, save_status)
            
            # Process results in batches for efficiency
            batch_size = 100
            
            for i in range(0, len(results), batch_size):
                batch = results[i:i + batch_size]
                batch_num = i // batch_size + 1
                logger.info(f"📦 Processing batch {batch_num} ({len(batch)} results)")
                
                try:
                    # Build batch INSERT
                    sql_parts = []
                    
                    for idx, result in enumerate(batch):
                        # Escape and format values with enhanced escaping
                        raw_comment = result.get('proposed_comment', '')
                        if raw_comment:
                            # Convert to string and apply comprehensive escaping
                            comment_text = str(raw_comment)
                            # Escape single quotes for SQL
                            comment_text = comment_text.replace("'", "''")
                            # Also escape backslashes that might cause issues
                            comment_text = comment_text.replace("\\", "\\\\")
                        else:
                            comment_text = None
                        
                        # Only log first result in batch for verification
                        if idx == 0:
                            logger.info(f"   First result: {result.get('full_name', '')} ({result.get('object_type', '')})")
                        
                        # Escape JSON fields that might contain single quotes
                        context_json = json.dumps(result.get('context_used', {})) if result.get('context_used') else None
                        if context_json:
                            context_json = context_json.replace("'", "''")
                        
                        pii_analysis_json = json.dumps(result.get('pii_analysis', {})) if result.get('pii_analysis') else None
                        if pii_analysis_json:
                            pii_analysis_json = pii_analysis_json.replace("'", "''")
                        
                        # Escape string fields that might contain quotes
                        def escape_sql_string(value):
                            if value:
                                return str(value).replace("'", "''")
                            return value
                        
                        values = [
                            f"'{result.get('run_id', '')}'" if result.get('run_id') else 'NULL',
                            f"'{result.get('full_name', '')}'" if result.get('full_name') else 'NULL',
                            f"'{result.get('object_type', '')}'" if result.get('object_type') else 'NULL',
                            f"'{comment_text}'" if comment_text else 'NULL',
                            str(result.get('confidence_score', 0.0)),
                            f"'{escape_sql_string(result.get('pii_tags', ''))}'" if result.get('pii_tags') else 'NULL',
                            f"'{escape_sql_string(result.get('policy_tags', ''))}'" if result.get('policy_tags') else 'NULL',
                            f"'{escape_sql_string(result.get('proposed_policy_tags', ''))}'" if result.get('proposed_policy_tags') else 'NULL',
                            f"'{escape_sql_string(result.get('data_classification', 'INTERNAL'))}'" if result.get('data_classification') else "'INTERNAL'",
                            f"'{escape_sql_string(result.get('source_model', ''))}'" if result.get('source_model') else 'NULL',
                            f"'{escape_sql_string(result.get('generation_style', ''))}'" if result.get('generation_style') else 'NULL',
                            str(result.get('pii_detected', False)).lower(),
                            f"'{context_json}'" if context_json else 'NULL',
                            f"'{pii_analysis_json}'" if pii_analysis_json else 'NULL',
                            f"'{result.get('generated_at', datetime.now().isoformat())}'" if result.get('generated_at') else f"'{datetime.now().isoformat()}'",
                            'NULL',  # submitted_at
                            "'generated'",  # status
                            'NULL',  # error_message
                            "'uc_metadata_assistant'"  # created_by
                        ]
                        
                        sql_parts.append(f"({', '.join(values)})")
                    
                    sql_statement = f"""
                        INSERT INTO {results_table} (
                            run_id, full_name, object_type, proposed_comment, confidence_score,
                            pii_tags, policy_tags, proposed_policy_tags, data_classification, source_model, generation_style,
                            pii_detected, context_used, pii_analysis, generated_at, submitted_at,
                            status, error_message, created_by
                        )
                        VALUES {', '.join(sql_parts)}
                    """
                    
                    result = await self._execute_sql(sql_statement)
                    
                    if result['success']:
                        save_status['saved_count'] += len(batch)
                        logger.info(f"   ✅ Saved {len(batch)} results successfully")
                    else:
                        save_status['error_count'] += len(batch)
                        error_msg = f"Batch {batch_num}: {result.get('error')}"
                        save_status['errors'].append(error_msg)
                        logger.error(f"❌ SQL execution failed: {error_msg}")
                        logger.error(f"Failed SQL statement: {sql_statement[:500]}...")  # Log first 500 chars for debugging
                        
                except Exception as e:
                    save_status['error_count'] += len(batch)
                    error_details = f"Batch {i//batch_size + 1}: {str(e)}"
                    save_status['errors'].append(error_details)
                    logger.error(f"Error saving batch: {error_details}")
                    logger.error(f"Batch data sample: {batch[0] if batch else 'Empty batch'}")
            
            save_status['success'] = save_status['saved_count'] > 0
            logger.info(f"Saved {save_status['saved_count']} results, {save_status['error_count']} errors")
            
        except Exception as e:
            logger.error(f"Error saving generation results: {e}")
            save_status['errors'].append(str(e))
        
        return save_status

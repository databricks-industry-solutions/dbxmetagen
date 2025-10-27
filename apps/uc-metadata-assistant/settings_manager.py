"""
Settings Manager
================

Centralized settings management for the UC Metadata Assistant.
Handles workspace-wide configuration for models, PII detection, and tags policy.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Global initialization flag to prevent repeated initialization across requests
_SETTINGS_INITIALIZED = False

class SettingsManager:
    """
    Centralized settings manager for workspace-wide configuration.
    Handles models, PII detection, and tags policy settings.
    """
    
    def __init__(self, unity_service):
        self.unity_service = unity_service
        self.settings_schema = "uc_metadata_assistant.settings"
        self.settings_table = f"{self.settings_schema}.workspace_settings"
        self._initialized = False
        
        # Default settings
        self.default_settings = {
            "models": {
                "enabled_models": [
                    "databricks-gpt-oss-120b",
                    "databricks-claude-sonnet-4", 
                    "databricks-meta-llama-3-3-70b-instruct",
                    "databricks-gemma-3-12b"
                ],
                "custom_models": [],
                "default_model": "databricks-gpt-oss-120b"
            },
            "pii_detection": {
                "enabled": True,
                "custom_patterns": [],
                "llm_assessment_enabled": True,
                "llm_detection_enabled": True,
                "llm_model": "databricks-gemma-3-12b",
                "severity_weights": {
                    "high": 10,
                    "medium": 5, 
                    "low": 1
                }
            },
            "tags_policy": {
                "tags_enabled": True,
                "governed_tags_only": False
            },
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "version": "1.0"
            }
        }
    
    def initialize_settings(self):
        """Initialize settings storage if it doesn't exist"""
        try:
            # Ensure settings schema exists
            self._ensure_settings_schema()
            
            # Create settings table if it doesn't exist
            self._create_settings_table()
            
            # Load or create default settings (use direct query to avoid recursion)
            settings = self._get_settings_direct()
            if not settings:
                self.save_settings(self.default_settings)
                logger.info("📋 Initialized default workspace settings")
            # Settings already exist, no need to log
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize settings: {e}")
            raise
    
    def _ensure_settings_schema(self):
        """Ensure the settings schema exists"""
        try:
            # Check if schema exists first to avoid unnecessary logging
            check_schema_sql = f"""
            SELECT schema_name 
            FROM system.information_schema.schemata 
            WHERE schema_name = '{self.settings_schema.split('.')[-1]}' 
            AND catalog_name = '{self.settings_schema.split('.')[0]}'
            """
            
            result = self.unity_service._execute_sql_warehouse(check_schema_sql)
            
            if not result or len(result) == 0:
                # Schema doesn't exist, create it
                create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.settings_schema}"
                self.unity_service._execute_sql_warehouse(create_schema_sql)
                logger.info(f"✅ Created settings schema: {self.settings_schema}")
            # If schema exists, don't log anything to reduce noise
            
        except Exception as e:
            logger.error(f"❌ Failed to ensure settings schema: {e}")
            raise
    
    def _create_settings_table(self):
        """Create the settings table if it doesn't exist"""
        try:
            # Check if table exists first to avoid unnecessary logging
            check_table_sql = f"""
            SELECT table_name 
            FROM system.information_schema.tables 
            WHERE table_name = '{self.settings_table.split('.')[-1]}' 
            AND table_schema = '{self.settings_table.split('.')[-2]}'
            AND table_catalog = '{self.settings_table.split('.')[0]}'
            """
            
            result = self.unity_service._execute_sql_warehouse(check_table_sql)
            
            if not result or len(result) == 0:
                # Table doesn't exist, create it
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.settings_table} (
                    setting_key STRING,
                    setting_value STRING,
                    updated_at TIMESTAMP,
                    updated_by STRING
                ) USING DELTA
                """
                
                self.unity_service._execute_sql_warehouse(create_table_sql)
                logger.info(f"✅ Created settings table: {self.settings_table}")
            # If table exists, don't log anything to reduce noise
            
        except Exception as e:
            logger.error(f"❌ Failed to create settings table: {e}")
            raise
    
    def _ensure_initialized(self):
        """Ensure settings are initialized (lazy initialization)"""
        global _SETTINGS_INITIALIZED
        
        if not _SETTINGS_INITIALIZED and not self._initialized:
            try:
                self.initialize_settings()
                self._initialized = True
                _SETTINGS_INITIALIZED = True
            except Exception as e:
                logger.warning(f"Settings initialization failed, using defaults: {e}")
                # Don't mark as initialized so it can retry later
    
    def _get_settings_direct(self) -> Optional[Dict]:
        """Get settings directly without initialization check (for internal use)"""
        try:
            query = f"""
            SELECT setting_key, setting_value 
            FROM {self.settings_table}
            WHERE setting_key = 'workspace_config'
            """
            
            result = self.unity_service._execute_sql_warehouse(query)
            
            if result and len(result) > 0:
                # result is array of arrays: [setting_key, setting_value]
                settings_json = result[0][1]  # setting_value is at index 1
                return json.loads(settings_json)
            
            return None
            
        except Exception as e:
            # Don't log errors here as this is used during initialization
            return None
    
    def get_settings(self) -> Optional[Dict]:
        """Get current workspace settings"""
        try:
            # Ensure initialization before accessing settings
            self._ensure_initialized()
            
            query = f"""
            SELECT setting_key, setting_value 
            FROM {self.settings_table}
            WHERE setting_key = 'workspace_config'
            """
            
            result = self.unity_service._execute_sql_warehouse(query)
            
            if result and len(result) > 0:
                # result is array of arrays: [setting_key, setting_value]
                settings_json = result[0][1]  # setting_value is at index 1
                return json.loads(settings_json)
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get settings: {e}")
            return None
    
    def save_settings(self, settings: Dict, max_retries: int = 3):
        """Save workspace settings with retry logic for concurrency conflicts"""
        import time
        import random
        
        # Update timestamp
        settings['metadata']['updated_at'] = datetime.now().isoformat()
        
        # Convert to JSON and escape quotes
        settings_json = json.dumps(settings, indent=2).replace("'", "''")
        
        # Upsert settings with retry logic
        merge_sql = f"""
        MERGE INTO {self.settings_table} AS target
        USING (
            SELECT 'workspace_config' as setting_key,
                   '{settings_json}' as setting_value,
                   CURRENT_TIMESTAMP() as updated_at,
                   CURRENT_USER() as updated_by
        ) AS source
        ON target.setting_key = source.setting_key
        WHEN MATCHED THEN UPDATE SET
            setting_value = source.setting_value,
            updated_at = source.updated_at,
            updated_by = source.updated_by
        WHEN NOT MATCHED THEN INSERT (setting_key, setting_value, updated_at, updated_by)
        VALUES (source.setting_key, source.setting_value, source.updated_at, source.updated_by)
        """
        
        for attempt in range(max_retries):
            try:
                self.unity_service._execute_sql_warehouse(merge_sql)
                logger.info("✅ Settings saved successfully")
                return
                
            except Exception as e:
                error_message = str(e).lower()
                
                # Check if this is a concurrency conflict
                if 'concurrent' in error_message or 'delta_concurrent_append' in error_message:
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter: 0.5-1.5s, 1-3s, 2-6s
                        base_delay = 0.5 * (2 ** attempt)
                        jitter = random.uniform(0.5, 1.5)
                        delay = base_delay * jitter
                        
                        logger.warning(f"⚠️ Concurrency conflict on attempt {attempt + 1}, retrying in {delay:.1f}s...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"❌ Failed to save settings after {max_retries} attempts due to concurrency conflicts")
                        raise Exception(f"Settings save failed after {max_retries} retries due to concurrent updates. Please try again.")
                else:
                    # Non-concurrency error, don't retry
                    logger.error(f"❌ Failed to save settings: {e}")
                    raise
    
    def get_models_config(self) -> Dict:
        """Get models configuration"""
        settings = self.get_settings()
        if settings:
            return settings.get('models', self.default_settings['models'])
        return self.default_settings['models']
    
    def update_models_config(self, models_config: Dict):
        """Update models configuration"""
        settings = self.get_settings() or self.default_settings.copy()
        settings['models'].update(models_config)
        self.save_settings(settings)
    
    def get_pii_config(self) -> Dict:
        """Get PII detection configuration"""
        settings = self.get_settings()
        if settings:
            return settings.get('pii_detection', self.default_settings['pii_detection'])
        return self.default_settings['pii_detection']
    
    def update_pii_config(self, pii_config: Dict):
        """Update PII detection configuration"""
        settings = self.get_settings() or self.default_settings.copy()
        settings['pii_detection'].update(pii_config)
        self.save_settings(settings)
    
    def get_tags_policy(self) -> Dict:
        """Get tags policy configuration"""
        settings = self.get_settings()
        if settings:
            return settings.get('tags_policy', self.default_settings['tags_policy'])
        return self.default_settings['tags_policy']
    
    def update_tags_policy(self, tags_policy: Dict):
        """Update tags policy configuration"""
        settings = self.get_settings() or self.default_settings.copy()
        settings['tags_policy'].update(tags_policy)
        self.save_settings(settings)
    
    def get_default_settings(self) -> Dict:
        """Get default settings structure"""
        return self.default_settings.copy()

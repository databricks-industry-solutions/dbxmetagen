"""
Unity Catalog Governance Dashboard - React Frontend with Flask Backend
This version serves the React frontend and proxies API calls to the existing backend.
"""

import os
import logging
from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS

# Import all existing services and endpoints from the original app
from app import (
    UnityMetadataService,
    get_unity_service,
    get_settings_manager_safe,
    get_models_config_manager,
    get_llm_service,
    PIIDetector,
    EnhancedMetadataGenerator
)

# --------------------------- App & Logging -----------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("uc-metadata-assistant-react")

# Create Flask app - don't use Flask's automatic static serving
react_build_path = os.path.join(os.path.dirname(__file__), 'client', 'build')
flask_app = Flask(__name__)
CORS(flask_app)

logger.info(f"React build path: {react_build_path}")
logger.info(f"React build exists: {os.path.exists(react_build_path)}")

# --------------------------- Serve React App -----------------------------------
@flask_app.route('/')
def serve_react_root():
    """Serve React app root"""
    return send_from_directory(react_build_path, 'index.html')

@flask_app.route('/<path:path>')
def serve_react_app(path):
    """Serve React app for all non-API routes"""
    logger.info(f"Catch-all route hit for path: {path}")
    
    # If the path starts with 'assets/', serve it as a static file
    if path.startswith('assets/'):
        logger.info(f"Serving static asset: {path}")
        return send_from_directory(react_build_path, path)
    
    # For everything else (React Router paths), serve index.html
    logger.info(f"Serving index.html for React Router path: {path}")
    return send_from_directory(react_build_path, 'index.html')

# --------------------------- API Routes (Import from existing app) -----------------------------------

def resolve_user_id_to_username(user_id: str) -> str:
    """Resolve a numeric user ID to an actual username using Databricks SCIM API"""
    try:
        unity = get_unity_service()
        token = unity._get_oauth_token()
        workspace_host = unity.workspace_host
        
        # Try the SCIM Users API
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        user_url = f"https://{workspace_host}/api/2.0/preview/scim/v2/Users/{user_id}"
        logger.info(f"Attempting SCIM API call: {user_url}")
        response = requests.get(user_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            user_data = response.json()
            username = (
                user_data.get('userName') or 
                user_data.get('displayName') or
                (user_data.get('name', {}).get('givenName', '') + '.' + user_data.get('name', {}).get('familyName', '')).strip('.')
            )
            
            if username and '@' in username:
                username = username.split('@')[0]
                
            logger.info(f"✅ Resolved user ID {user_id} to {username}")
            return username
        else:
            logger.info(f"⚠️ SCIM API returned {response.status_code} for user ID {user_id}")
            return None
    except Exception as e:
        logger.warning(f"❌ Failed to resolve user ID {user_id}: {e}")
        return None


def get_initials(username: str) -> str:
    """Extract initials from username (mobeen.vaid -> MV)"""
    if not username or username == "User":
        return "U"
    
    if '.' in username:
        parts = username.split('.')
        return ''.join([p[0].upper() for p in parts[:2] if p])
    elif ' ' in username:
        parts = username.split(' ')
        return ''.join([p[0].upper() for p in parts[:2] if p])
    else:
        return username[:2].upper()


@flask_app.route('/api/current-user')
def api_get_current_user():
    """Get current user from Databricks context (mirrors app.py logic)"""
    try:
        from flask import request
        current_user = None
        
        # Log headers for debugging (only keys, not values for security)
        logger.info(f"🔍 Available request headers: {list(request.headers.keys())}")
        
        # Check for Databricks App user headers (prioritize email headers)
        if 'X-Forwarded-Email' in request.headers:
            current_user = request.headers.get('X-Forwarded-Email')
            logger.info(f"👤 User from X-Forwarded-Email: {current_user}")
        elif 'X-Forwarded-Preferred-Username' in request.headers:
            current_user = request.headers.get('X-Forwarded-Preferred-Username')
            logger.info(f"👤 User from X-Forwarded-Preferred-Username: {current_user}")
        elif 'X-Databricks-User' in request.headers:
            current_user = request.headers.get('X-Databricks-User')
            logger.info(f"👤 User from X-Databricks-User: {current_user}")
        elif 'X-Forwarded-User' in request.headers:
            current_user = request.headers.get('X-Forwarded-User')
            logger.info(f"👤 User from X-Forwarded-User: {current_user}")
        elif 'X-User' in request.headers:
            current_user = request.headers.get('X-User')
            logger.info(f"👤 User from X-User: {current_user}")
        elif 'User' in request.headers:
            current_user = request.headers.get('User')
            logger.info(f"👤 User from User header: {current_user}")
        
        # If no user from headers, try SQL approach
        if not current_user:
            logger.info("🔍 No user headers found, trying SQL current_user()...")
            setup_manager = get_setup_manager()
            current_user = setup_manager.get_current_user()
            logger.info(f"👤 User from SQL current_user(): {current_user}")
            
            # If it's a service principal UUID, use fallback
            if current_user and len(current_user) == 36 and current_user.count('-') == 4:
                logger.info(f"🤖 Detected service principal UUID: {current_user}, using fallback")
                current_user = "User"
        
        # Clean up the user string
        if current_user:
            # If it's an email, extract username part
            if '@' in current_user:
                current_user = current_user.split('@')[0]
            
            # If it's a numeric ID, try to resolve to username
            if current_user.isdigit():
                logger.info(f"🔢 Detected numeric user ID: {current_user}, attempting resolution...")
                real_username = resolve_user_id_to_username(current_user)
                if real_username and real_username != current_user:
                    logger.info(f"✅ Resolved to username: {real_username}")
                    current_user = real_username
                else:
                    logger.info(f"⚠️ Could not resolve user ID, using fallback")
                    current_user = "User"
        
        if not current_user:
            current_user = "User"
        
        logger.info(f"✅ Final current user: {current_user} (initials: {get_initials(current_user)})")
        
        return jsonify({
            'username': current_user,
            'initials': get_initials(current_user)
        })
    except Exception as e:
        logger.error(f"❌ Error getting current user: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'username': 'User', 'initials': 'U'}), 200


@flask_app.route('/api/catalogs')
def api_catalogs():
    """Get all catalogs"""
    try:
        unity = get_unity_service()
        catalogs = unity.get_catalogs()
        return jsonify(catalogs)
    except Exception as e:
        logger.error(f"Error getting catalogs: {e}")
        return jsonify({"error": str(e)}), 500

@flask_app.route('/api/fast-counts/<catalog_name>')
def api_fast_counts(catalog_name):
    """Get fast counts for catalog"""
    try:
        from flask import request
        unity = get_unity_service()
        
        # Get filter parameters
        filter_object_type = request.args.get('filterObjectType', '').strip()
        filter_data_object = request.args.get('filterDataObject', '').strip()
        filter_owner = request.args.get('filterOwner', '').strip()
        
        # Get counts using the actual method name from UnityMetadataService
        counts = unity._fast_counts_via_sql(
            catalog_name,
            filter_object_type,
            filter_data_object,
            filter_owner
        )
        
        # Transform nested structure to flat structure for React frontend
        transformed = {
            'schemas_missing_description': counts.get('schemas', {}).get('missing', 0),
            'tables_missing_description': counts.get('tables', {}).get('missing', 0),
            'columns_missing_comment': counts.get('columns', {}).get('missing', 0),
            'missing_tags': counts.get('tags', {}).get('missing', 0),
            # Also include totals for potential future use
            'schemas_total': counts.get('schemas', {}).get('total', 0),
            'tables_total': counts.get('tables', {}).get('total', 0),
            'columns_total': counts.get('columns', {}).get('total', 0),
        }
        
        logger.info(f"Transformed counts for {catalog_name}: {transformed}")
        return jsonify(transformed)
    except Exception as e:
        logger.error(f"Error getting fast counts: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/top-gaps/<catalog_name>')
def api_get_top_gaps(catalog_name):
    """Get top metadata gaps - schemas with most tables and tables with most columns"""
    try:
        unity = get_unity_service()
        
        # Get schemas with most tables (and their missing description count)
        schema_query = f"""
            SELECT 
                table_schema as schema_name,
                COUNT(*) as table_count,
                SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) as missing_descriptions
            FROM {catalog_name}.information_schema.tables
            WHERE table_catalog = '{catalog_name}'
              AND table_schema NOT IN ('information_schema', 'system')
            GROUP BY table_schema
            ORDER BY table_count DESC
            LIMIT 5
        """
        
        logger.info(f"🔍 Fetching top schemas by table count for {catalog_name}")
        schema_data = unity._execute_sql_warehouse(schema_query)
        
        # Get tables with most columns (and their missing comment count)
        table_query = f"""
            SELECT 
                table_schema,
                table_name,
                COUNT(*) as column_count,
                SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) as missing_comments
            FROM {catalog_name}.information_schema.columns
            WHERE table_catalog = '{catalog_name}'
              AND table_schema NOT IN ('information_schema', 'system')
            GROUP BY table_schema, table_name
            ORDER BY column_count DESC
            LIMIT 5
        """
        
        logger.info(f"🔍 Fetching top tables by column count for {catalog_name}")
        table_data = unity._execute_sql_warehouse(table_query)
        
        # Format the results
        top_schemas = []
        for row in schema_data:
            if len(row) >= 3:
                top_schemas.append({
                    'name': row[0],
                    'table_count': row[1],
                    'missing_descriptions': row[2],
                    'type': 'schema'
                })
        
        top_tables = []
        for row in table_data:
            if len(row) >= 4:
                top_tables.append({
                    'schema': row[0],
                    'name': row[1],
                    'full_name': f"{catalog_name}.{row[0]}.{row[1]}",
                    'column_count': row[2],
                    'missing_comments': row[3],
                    'type': 'table'
                })
        
        logger.info(f"✅ Found {len(top_schemas)} top schemas and {len(top_tables)} top tables")
        
        return jsonify({
            'top_schemas': top_schemas,
            'top_tables': top_tables
        })
    except Exception as e:
        logger.error(f"❌ Error getting top gaps for {catalog_name}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/settings/models')
def api_get_model_settings():
    """Get model settings with proper model structure (same as original app)"""
    try:
        # Use models config manager to get ALL models (builtin + custom)
        models_config = get_models_config_manager()
        logger.info(f"🔧 Models config manager initialized: {models_config}")
        
        # Get the raw models config data to debug
        settings_manager = get_settings_manager_safe()
        if settings_manager:
            raw_config = settings_manager.get_models_config()
            logger.info(f"📦 Raw models config - custom_models count: {len(raw_config.get('custom_models', []))}")
            logger.info(f"📦 Raw models config - enabled_models: {raw_config.get('enabled_models', [])}")
            for cm in raw_config.get('custom_models', []):
                logger.info(f"   📌 Custom model: {cm.get('model_id')} - {cm.get('name')}")
        
        models = models_config.get_available_models()
        
        logger.info(f"📋 Retrieved {len(models)} models from models_config.get_available_models()")
        logger.info(f"📋 Model IDs: {list(models.keys())}")
        
        # Log each model's details
        for model_id, model_info in models.items():
            is_custom = not model_info.get('builtin', True)
            logger.info(f"   {'🔧' if is_custom else '📦'} {model_id}: enabled={model_info.get('enabled')}, builtin={model_info.get('builtin')}")
        
        # Transform to match frontend expectations
        transformed_models = {}
        for model_id, model_info in models.items():
            transformed_models[model_id] = {
                "display_name": model_info.get('name', model_id),
                "description": model_info.get('description', ''),
                "endpoint": model_id,
                "max_tokens": model_info.get('max_tokens', 2048),
                "enabled": model_info.get('enabled', False),
                "builtin": model_info.get('builtin', False)
            }
        
        return jsonify({
            "status": "success",
            "models": transformed_models
        })
    except Exception as e:
        logger.error(f"❌ Failed to get model settings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/models/toggle', methods=['POST'])
def api_toggle_model():
    """Toggle model enabled/disabled (same as original app)"""
    try:
        from flask import request
        data = request.get_json()
        model_id = data.get('model_name') or data.get('model_id')  # Accept both for compatibility
        
        if not model_id:
            return jsonify({"status": "error", "error": "model_id or model_name is required"}), 400
        
        # Get current models to check enabled state
        models_config = get_models_config_manager()
        models = models_config.get_available_models()
        current_enabled = models.get(model_id, {}).get('enabled', False)
        
        # Toggle the state
        if current_enabled:
            success = models_config.disable_model(model_id)
        else:
            success = models_config.enable_model(model_id)
        
        if success:
            logger.info(f"✅ Model {model_id} {'disabled' if current_enabled else 'enabled'} successfully")
            return jsonify({"status": "success", "enabled": not current_enabled})
        else:
            return jsonify({"status": "error", "error": "Failed to update model status"}), 500
    except Exception as e:
        logger.error(f"❌ Failed to toggle model: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@flask_app.route('/api/settings/models/debug')
def api_debug_models():
    """Debug endpoint to see raw models configuration"""
    try:
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        models_config_data = settings_manager.get_models_config()
        
        # Also try to get from models config manager
        try:
            models_config = get_models_config_manager()
            available_models = models_config.get_available_models()
            available_models_list = list(available_models.keys())
        except Exception as e:
            available_models_list = f"Error: {str(e)}"
        
        return jsonify({
            "raw_settings_models": settings.get('models', {}) if settings else {},
            "models_config_data": models_config_data,
            "enabled_models": models_config_data.get('enabled_models', []),
            "custom_models": models_config_data.get('custom_models', []),
            "available_models_from_manager": available_models_list
        })
    except Exception as e:
        logger.error(f"Failed to debug models: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@flask_app.route('/api/settings/models/test/<model_id>')
def api_test_model_endpoint(model_id):
    """Test if a model endpoint is callable"""
    try:
        # Get the LLM service (same as original app)
        llm_service = get_llm_service()
        if not llm_service:
            return jsonify({"success": False, "message": "LLM service not available"}), 500
        
        # Try a simple test call
        try:
            test_response = llm_service._call_databricks_llm(
                prompt="Say 'test successful' if you can read this.",
                max_tokens=10,
                model=model_id,
                temperature=0.1
            )
            
            if test_response and len(test_response.strip()) > 0:
                logger.info(f"✅ Model test successful: {model_id}")
                return jsonify({
                    "success": True, 
                    "message": f"Model {model_id} is working correctly",
                    "response": test_response[:100]  # First 100 chars
                })
            else:
                return jsonify({
                    "success": False, 
                    "message": "Model returned empty response"
                })
        
        except Exception as llm_error:
            error_msg = str(llm_error)
            logger.error(f"❌ Model test failed for {model_id}: {error_msg}")
            return jsonify({
                "success": False, 
                "message": f"Model invocation failed: {error_msg}"
            })
    
    except Exception as e:
        logger.error(f"❌ Error testing model: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "message": str(e)}), 500

@flask_app.route('/api/settings/models/update/<model_id>', methods=['POST'])
def api_update_custom_model(model_id):
    """Update a custom model's max_tokens"""
    try:
        from flask import request
        data = request.get_json()
        new_max_tokens = data.get('max_tokens')
        
        if not new_max_tokens:
            return jsonify({"success": False, "message": "max_tokens is required"}), 400
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"success": False, "message": "Settings manager unavailable"}), 500
        
        models_config_data = settings_manager.get_models_config()
        custom_models = models_config_data.get('custom_models', [])
        
        # Find and update the model
        updated = False
        for model in custom_models:
            if model['model_id'] == model_id:
                model['max_tokens'] = int(new_max_tokens)
                updated = True
                break
        
        if not updated:
            return jsonify({"success": False, "message": f"Model {model_id} not found"}), 404
        
        # Save updated config
        settings_manager.update_models_config({
            'custom_models': custom_models
        })
        
        logger.info(f"✅ Updated {model_id} max_tokens to {new_max_tokens}")
        return jsonify({"success": True, "message": f"Updated {model_id} to {new_max_tokens} max_tokens"})
        
    except Exception as e:
        logger.error(f"❌ Error updating custom model: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@flask_app.route('/api/settings/models/remove/<model_id>', methods=['DELETE'])
def api_remove_custom_model_direct(model_id):
    """Remove a custom model by ID (for debugging)"""
    try:
        models_config = get_models_config_manager()
        success = models_config.remove_custom_model(model_id)
        
        if success:
            logger.info(f"✅ Removed custom model: {model_id}")
            return jsonify({"success": True, "message": f"Model {model_id} removed successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to remove model"}), 400
    except Exception as e:
        logger.error(f"❌ Error removing custom model: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@flask_app.route('/api/settings/models/migrate', methods=['GET', 'POST'])
def api_migrate_model_settings():
    """Migrate old model IDs to new model IDs"""
    try:
        # Mapping of old model IDs to new model IDs
        model_migrations = {
            'databricks-gpt-oss-120b': 'databricks-gpt-oss-20b',
            'databricks-meta-llama-3-3-70b-instruct': 'databricks-meta-llama-3-1-8b-instruct'
        }
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"success": False, "message": "Settings manager unavailable"}), 500
        
        models_config_data = settings_manager.get_models_config()
        enabled_models = models_config_data.get('enabled_models', [])
        default_model = models_config_data.get('default_model', '')
        
        # Migrate enabled_models list
        migrated_enabled = []
        changes = []
        for model_id in enabled_models:
            if model_id in model_migrations:
                new_id = model_migrations[model_id]
                migrated_enabled.append(new_id)
                changes.append(f"{model_id} → {new_id}")
                logger.info(f"🔄 Migrating enabled model: {model_id} → {new_id}")
            else:
                migrated_enabled.append(model_id)
        
        # Migrate default_model
        if default_model in model_migrations:
            new_default = model_migrations[default_model]
            changes.append(f"default: {default_model} → {new_default}")
            logger.info(f"🔄 Migrating default model: {default_model} → {new_default}")
            default_model = new_default
        
        # Save migrated settings
        if changes:
            settings_manager.update_models_config({
                'enabled_models': migrated_enabled,
                'default_model': default_model
            })
            
            logger.info(f"✅ Migration complete: {len(changes)} changes")
            return jsonify({
                "success": True,
                "message": f"Migrated {len(changes)} model references",
                "changes": changes
            })
        else:
            return jsonify({
                "success": True,
                "message": "No migration needed - all models are up to date",
                "changes": []
            })
    
    except Exception as e:
        logger.error(f"❌ Error migrating model settings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "message": str(e)}), 500

@flask_app.route('/api/settings/models/add', methods=['POST'])
def api_add_custom_model():
    """Add a custom model (validates first, then adds)"""
    try:
        from flask import request
        data = request.get_json()
        
        model_name = data.get('model_name', '').strip()
        display_name = data.get('display_name', '').strip()
        description = data.get('description', '').strip()
        max_tokens = data.get('max_tokens')
        
        if not model_name or not display_name:
            return jsonify({"success": False, "message": "Model name and display name are required"}), 400
        
        # Validate max_tokens if provided
        if max_tokens:
            try:
                max_tokens = int(max_tokens)
                if max_tokens < 512 or max_tokens > 16000:
                    return jsonify({"success": False, "message": "Max tokens must be between 512 and 16000"}), 400
            except (ValueError, TypeError):
                return jsonify({"success": False, "message": "Max tokens must be a valid number"}), 400
        
        # Get models config manager (same as original app)
        models_config = get_models_config_manager()
        
        # Add the custom model (add_custom_model validates automatically)
        success, message = models_config.add_custom_model(
            model_name=model_name,
            display_name=display_name,
            description=description or f"Custom model: {display_name}",
            max_tokens=max_tokens  # None triggers smart defaults
        )
        
        if success:
            logger.info(f"✅ Added and enabled custom model: {model_name}")
            return jsonify({"success": True, "message": "Model validated and added successfully"})
        else:
            logger.warning(f"❌ Failed to add custom model {model_name}: {message}")
            return jsonify({"success": False, "message": message}), 400
    
    except Exception as e:
        logger.error(f"❌ Error adding custom model: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    except Exception as e:
        logger.error(f"Failed to toggle model: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/pii')
def api_get_pii_settings():
    """Get PII detection settings with actual patterns"""
    try:
        settings_manager = get_settings_manager_safe()
        
        # Default PII detection patterns organized by category
        default_pii_patterns = {
            'Personal': ['ssn', 'social_security', 'phone', 'email', 'name', 'address', 'zip', 'postal'],
            'Financial': ['credit_card', 'account_number', 'routing_number', 'aba', 'iban', 'swift'],
            'Medical': ['mrn', 'medical_record', 'npi', 'icd', 'patient_id', 'diagnosis'],
            'Government': ['passport', 'driver_license', 'tax_id', 'ein', 'visa'],
            'Employment': ['employee_id', 'payroll', 'salary', 'compensation'],
            'Education': ['student_id', 'transcript', 'grade'],
            'Biometric': ['fingerprint', 'facial', 'iris', 'biometric'],
            'Custom': []
        }
        
        # Try to get configuration from settings
        main_enabled = False
        enable_llm_assessment = False
        enable_llm_pii_detection = False
        
        if settings_manager:
            settings = settings_manager.get_settings()
            if settings:
                pii_detection = settings.get('pii_detection', {})
                main_enabled = pii_detection.get('enabled', False)  # Main toggle
                enable_llm_assessment = pii_detection.get('enable_llm_assessment', False)
                enable_llm_pii_detection = pii_detection.get('enable_llm_pii_detection', False)
                
                # Merge custom patterns if available
                custom_patterns = pii_detection.get('custom_patterns', [])
                if custom_patterns:
                    default_pii_patterns['Custom'] = custom_patterns
        
        # Return PII configuration
        pii_config = {
            'main_enabled': main_enabled,  # ✅ Include main toggle
            'enable_llm_assessment': enable_llm_assessment,
            'enable_llm_pii_detection': enable_llm_pii_detection,
            'pii_patterns': default_pii_patterns
        }
        
        logger.info(f"📋 PII Settings: main={main_enabled}, llm_assessment={enable_llm_assessment}, llm_detection={enable_llm_pii_detection}")
        
        return jsonify(pii_config)
    except Exception as e:
        logger.error(f"Failed to get PII settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/pii/toggle-assessment', methods=['POST'])
def api_toggle_pii_assessment():
    """Toggle LLM assessment for PII"""
    try:
        from flask import request
        data = request.get_json()
        enabled = data.get('enable_llm_assessment', False)
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        settings['enable_llm_assessment'] = enabled
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ LLM assessment: {enabled}")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to toggle LLM assessment: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/pii/toggle-detection', methods=['POST'])
def api_toggle_pii_detection():
    """Toggle LLM PII detection"""
    try:
        from flask import request
        data = request.get_json()
        enabled = data.get('enable_llm_pii_detection', False)
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        settings['enable_llm_pii_detection'] = enabled
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ LLM PII detection: {enabled}")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to toggle LLM PII detection: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/tags')
def api_get_tags_settings():
    """Get tags policy settings"""
    try:
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        # Get tags policy configuration
        tags_policy = settings.get('tags_policy', {
            'tags_enabled': True,
            'governed_tags_only': False
        })
        
        logger.info(f"📋 Returning tags policy: {tags_policy}")
        
        # Return in the expected format
        return jsonify({
            "status": "success",
            "policy": tags_policy
        })
    except Exception as e:
        logger.error(f"Failed to get tags settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/tags/update', methods=['POST'])
def api_update_tags_settings():
    """Update tags policy settings"""
    try:
        from flask import request
        data = request.get_json()
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        # Initialize tags_policy if it doesn't exist
        if 'tags_policy' not in settings:
            settings['tags_policy'] = {
                'tags_enabled': True,
                'governed_tags_only': False
            }
        
        # Update the tags policy with provided settings
        for key, value in data.items():
            if key in ['tags_enabled', 'governed_tags_only']:
                settings['tags_policy'][key] = value
                logger.info(f"✅ Updated tags policy: {key} = {value}")
        
        # Save updated settings
        settings_manager.save_settings(settings)
        
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to update tags policy: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

# --------------------------- Sensitive Data Endpoints -----------------------------------
@flask_app.route('/api/settings/sensitive-data')
def api_get_sensitive_data_settings():
    """Get sensitive data detection settings"""
    try:
        settings_manager = get_settings_manager_safe()
        
        # Patterns organized by compliance framework (matching original app)
        patterns_by_framework = {
            'PII (GDPR/CCPA)': ['ssn', 'social_security', 'phone', 'email', 'name', 'address', 'zip', 'postal', 'date_of_birth'],
            'PHI (HIPAA)': ['mrn', 'medical_record', 'patient_id', 'diagnosis', 'prescription', 'health_insurance'],
            'PCI (Payment Card)': ['credit_card', 'cvv', 'card_number', 'expiry_date', 'pan'],
            'Financial (SOX/GLBA)': ['account_number', 'routing_number', 'aba', 'iban', 'swift', 'salary', 'compensation'],
            'Biometric': ['fingerprint', 'facial', 'iris', 'retina', 'voiceprint'],
            'Custom': []
        }
        
        # Get configuration from settings
        main_enabled = True
        enable_llm_assessment = False
        enable_llm_pii_detection = False
        
        if settings_manager:
            settings = settings_manager.get_settings()
            if settings:
                pii_detection = settings.get('pii_detection', {})
                main_enabled = pii_detection.get('enabled', True)
                enable_llm_assessment = pii_detection.get('enable_llm_assessment', False)  # ✅ Fixed field name
                enable_llm_pii_detection = pii_detection.get('enable_llm_pii_detection', False)  # ✅ Fixed field name
                
                # Merge custom patterns
                custom_patterns = pii_detection.get('custom_patterns', [])
                if custom_patterns:
                    patterns_by_framework['Custom'] = custom_patterns
        
        return jsonify({
            'main_enabled': main_enabled,
            'enable_llm_assessment': enable_llm_assessment,
            'enable_llm_pii_detection': enable_llm_pii_detection,
            'patterns': patterns_by_framework
        })
    except Exception as e:
        logger.error(f"Failed to get sensitive data settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sensitive-data/toggle-main', methods=['POST'])
def api_toggle_sensitive_data_main():
    """Toggle main sensitive data detection"""
    try:
        from flask import request
        data = request.get_json()
        enabled = data.get('enabled', True)
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'pii_detection' not in settings:
            settings['pii_detection'] = {}
        settings['pii_detection']['enabled'] = enabled
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ Sensitive data detection: {enabled}")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to toggle sensitive data detection: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sensitive-data/toggle-llm-assessment', methods=['POST'])
def api_toggle_sensitive_data_llm_assessment():
    """Toggle LLM context assessment"""
    try:
        from flask import request
        data = request.get_json()
        enabled = data.get('enabled', False)
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'pii_detection' not in settings:
            settings['pii_detection'] = {}
        settings['pii_detection']['enable_llm_assessment'] = enabled  # ✅ Fixed field name
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ LLM context assessment: {enabled}")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to toggle LLM assessment: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sensitive-data/toggle-llm-detection', methods=['POST'])
def api_toggle_sensitive_data_llm_detection():
    """Toggle LLM data scanning"""
    try:
        from flask import request
        data = request.get_json()
        enabled = data.get('enabled', False)
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'pii_detection' not in settings:
            settings['pii_detection'] = {}
        settings['pii_detection']['enable_llm_pii_detection'] = enabled  # ✅ Fixed field name
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ LLM data scanning: {enabled}")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to toggle LLM detection: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sensitive-data/update-patterns', methods=['POST'])
def api_update_sensitive_data_patterns():
    """Update sensitive data detection patterns"""
    try:
        from flask import request
        data = request.get_json()
        patterns = data.get('patterns', {})
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'pii_detection' not in settings:
            settings['pii_detection'] = {}
        settings['pii_detection']['patterns'] = patterns
        
        # Clear persistent cache when patterns are updated (force re-validation on next use)
        if 'validated_tag_mappings_cache' in settings['pii_detection']:
            del settings['pii_detection']['validated_tag_mappings_cache']
            logger.debug("🗑️ Cleared persistent tag mappings cache (patterns changed)")
        
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ Updated sensitive data patterns")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to update patterns: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

# Helper functions for tag mapping validation
def _validate_tag_mappings_against_governed(mappings: dict, governed_tags: dict) -> dict:
    """
    Validate tag mappings against Unity Catalog's governed tags.
    For invalid mappings (governed key with invalid value), keep the pattern but clear the tag info.
    This allows users to see all patterns and manually assign valid tags.
    
    Returns: Cleaned mappings dict with invalid tag info cleared but patterns preserved
    """
    cleaned_mappings = mappings.copy()
    
    # Validate pii_type_tags
    if 'pii_type_tags' in cleaned_mappings:
        cleaned_pii_tags = {}
        for pattern, tag_info in cleaned_mappings['pii_type_tags'].items():
            tag_key = tag_info.get('key')
            tag_value = tag_info.get('value')
            
            # Check if this tag is governed
            if tag_key in governed_tags:
                allowed_values = governed_tags[tag_key].get('allowed_values', [])
                # If governed and value not in allowed list, keep pattern but clear tag
                if allowed_values and tag_value not in allowed_values:
                    logger.debug(f"Clearing invalid mapping for pattern '{pattern}': {tag_key}.{tag_value} not allowed")
                    # Keep the pattern but with empty tag info
                    cleaned_pii_tags[pattern] = {
                        'key': '',
                        'value': '',
                        'reason': tag_info.get('reason', ''),
                        'framework': tag_info.get('framework', ''),
                        'invalid_default': True  # Flag to show this had an invalid default
                    }
                    continue
            
            # Valid mapping, keep it as-is
            cleaned_pii_tags[pattern] = tag_info
        
        cleaned_mappings['pii_type_tags'] = cleaned_pii_tags
    
    # Validate classification_tags
    if 'classification_tags' in cleaned_mappings:
        cleaned_class_tags = {}
        for classification, tag_info in cleaned_mappings['classification_tags'].items():
            tag_key = tag_info.get('key')
            tag_value = tag_info.get('value')
            
            # Check if this tag is governed
            if tag_key in governed_tags:
                allowed_values = governed_tags[tag_key].get('allowed_values', [])
                # If governed and value not in allowed list, keep classification but clear tag
                if allowed_values and tag_value not in allowed_values:
                    logger.debug(f"Clearing invalid classification mapping for '{classification}': {tag_key}.{tag_value} not allowed")
                    # Keep the classification but with empty tag info
                    cleaned_class_tags[classification] = {
                        'key': '',
                        'value': '',
                        'tag': '',
                        'invalid_default': True
                    }
                    continue
            
            # Valid mapping, keep it as-is
            cleaned_class_tags[classification] = tag_info
        
        cleaned_mappings['classification_tags'] = cleaned_class_tags
    
    return cleaned_mappings

def _count_tag_mapping_differences(original: dict, filtered: dict) -> int:
    """Count how many tag mappings were removed during filtering"""
    original_count = len(original.get('pii_type_tags', {})) + len(original.get('classification_tags', {}))
    filtered_count = len(filtered.get('pii_type_tags', {})) + len(filtered.get('classification_tags', {}))
    return original_count - filtered_count

@flask_app.route('/api/settings/sensitive-data/tag-mappings', methods=['GET'])
def api_get_tag_mappings():
    """Get configurable policy tag mappings - dynamically generated from PIIDetector (validated against governed tags)"""
    try:
        # Get PIIDetector instance to access comprehensive tag mappings
        from pii_detector import PIIDetector
        
        settings_manager = get_settings_manager_safe()
        llm_service = get_llm_service()
        unity_service = get_unity_service()
        
        # Create PIIDetector instance to get comprehensive tag mappings
        pii_detector = PIIDetector(settings_manager, llm_service, unity_service)
        
        # Get comprehensive mappings (includes ALL patterns, with invalid ones marked with empty key/value)
        # NOTE: pii_detector._get_tag_mappings() already validates against governed tags
        # and marks invalid patterns with key='', value='', invalid_default=True
        comprehensive_mappings = pii_detector._get_tag_mappings()
        
        total_patterns = len(comprehensive_mappings.get('pii_type_tags', {}))
        patterns_needing_config = sum(
            1 for p in comprehensive_mappings.get('pii_type_tags', {}).values()
            if not p.get('key') or not p.get('value')
        )
        
        if patterns_needing_config > 0:
            logger.info(f"📋 Returning {total_patterns} PII type tag mappings ({patterns_needing_config} need configuration)")
        else:
            logger.info(f"✅ Returning {total_patterns} PII type tag mappings (all configured)")
        
        return jsonify(comprehensive_mappings)
    except Exception as e:
        logger.error(f"Failed to get tag mappings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sensitive-data/tag-mappings', methods=['POST'])
def api_update_tag_mappings():
    """Update configurable policy tag mappings"""
    try:
        from flask import request
        data = request.get_json()
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'pii_detection' not in settings:
            settings['pii_detection'] = {}
        
        # Store custom tag mappings
        settings['pii_detection']['tag_mappings'] = {
            'classification_tags': data.get('classification_tags', {}),
            'pii_type_tags': data.get('pii_type_tags', {})
        }
        
        # Clear persistent cache when mappings are updated (force re-validation on next use)
        if 'validated_tag_mappings_cache' in settings['pii_detection']:
            del settings['pii_detection']['validated_tag_mappings_cache']
            logger.info("🗑️ Cleared persistent tag mappings cache (will re-validate on next generation)")
        
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ Updated policy tag mappings")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to update tag mappings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sensitive-data/clear-tag-cache', methods=['POST'])
def api_clear_tag_mappings_cache():
    """Clear persistent tag mappings cache (forces re-validation)"""
    try:
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            return jsonify({"status": "success", "message": "No cache to clear"})
        
        if 'pii_detection' in settings and 'validated_tag_mappings_cache' in settings['pii_detection']:
            del settings['pii_detection']['validated_tag_mappings_cache']
            settings_manager.save_settings(settings)
            logger.info("✅ Cleared persistent tag mappings cache - will regenerate with new validation logic")
            return jsonify({"status": "success", "message": "Cache cleared successfully"})
        else:
            return jsonify({"status": "success", "message": "No cache found"})
    except Exception as e:
        logger.error(f"Failed to clear tag mappings cache: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

# --------------------------- Sampling Endpoints -----------------------------------
@flask_app.route('/api/settings/sampling')
def api_get_sampling_settings():
    """Get sampling configuration"""
    try:
        settings_manager = get_settings_manager_safe()
        
        # Default sampling config (matching settings_manager.py)
        sampling_config = {
            'enable_sampling': True,
            'sample_rows': 10,
            'min_sample_rows': 5,
            'max_sample_rows': 100,
            'redact_pii_in_samples': False,
            'max_prompt_tokens': 4000,
            'max_batch_schemas': 15,
            'max_batch_tables': 10,
            'max_batch_columns': 20,
            'estimated_tokens_per_schema': 150,
            'estimated_tokens_per_table': 300,
            'estimated_tokens_per_column': 100
        }
        
        if settings_manager:
            settings = settings_manager.get_settings()
            if settings and 'sampling' in settings:
                sampling_config.update(settings['sampling'])
        
        return jsonify(sampling_config)
    except Exception as e:
        logger.error(f"Failed to get sampling settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/sampling/update', methods=['POST'])
def api_update_sampling_settings():
    """Update sampling configuration"""
    try:
        from flask import request
        data = request.get_json()
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'sampling' not in settings:
            settings['sampling'] = {}
        
        # Update only provided keys
        for key in ['enable_sampling', 'sample_rows', 'redact_pii_in_samples', 
                    'max_prompt_tokens', 'max_batch_schemas', 'max_batch_tables', 'max_batch_columns',
                    'estimated_tokens_per_schema', 'estimated_tokens_per_table', 'estimated_tokens_per_column']:
            if key in data:
                settings['sampling'][key] = data[key]
        
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ Updated sampling config: {data}")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to update sampling settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

# --------------------------- Prompts Configuration API Routes ----------------------

@flask_app.route('/api/settings/prompts')
def api_get_prompts_settings():
    """Get prompt configuration settings"""
    try:
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        prompt_config = settings.get('prompt_config', settings_manager.default_settings['prompt_config'])
        
        logger.info(f"📋 Returning prompts config: {len(prompt_config.get('custom_terminology', {}))} terms, length={prompt_config.get('description_length')}")
        return jsonify(prompt_config)
    except Exception as e:
        logger.error(f"Failed to get prompts settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/prompts/update', methods=['POST'])
def api_update_prompts_settings():
    """Update prompt configuration"""
    try:
        from flask import request
        data = request.get_json()
        
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        if not settings:
            settings = settings_manager.default_settings.copy()
        
        if 'prompt_config' not in settings:
            settings['prompt_config'] = {}
        
        # Update only provided keys
        for key in ['custom_terminology', 'additional_instructions', 'description_length',
                    'include_technical_details', 'include_business_context', 'custom_examples']:
            if key in data:
                settings['prompt_config'][key] = data[key]
        
        settings_manager.save_settings(settings)
        
        logger.info(f"✅ Updated prompts config")
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Failed to update prompts settings: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@flask_app.route('/api/settings/prompts/preview')
def api_preview_prompt():
    """Generate a preview of the prompt with current settings"""
    try:
        settings_manager = get_settings_manager_safe()
        if not settings_manager:
            return jsonify({"status": "error", "error": "Settings manager unavailable"}), 500
        
        settings = settings_manager.get_settings()
        prompt_config = settings.get('prompt_config', settings_manager.default_settings['prompt_config'])
        
        # Build a sample prompt showing how customizations are applied
        sample_prompt = _build_sample_prompt_preview(prompt_config)
        
        return jsonify({
            "preview": sample_prompt,
            "config": prompt_config
        })
    except Exception as e:
        logger.error(f"Failed to generate prompt preview: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

def _build_sample_prompt_preview(prompt_config: dict) -> str:
    """Build a sample prompt showing how customizations are applied"""
    # Base prompt structure
    terminology = prompt_config.get('custom_terminology', {})
    additional_instructions = prompt_config.get('additional_instructions', '')
    
    # Handle description_length as either dict (per object type) or string (legacy)
    length_config = prompt_config.get('description_length', {})
    if isinstance(length_config, str):
        # Legacy format - apply same length to all
        column_length = length_config
    else:
        # New format - per object type
        column_length = length_config.get('column', 'standard')
    
    include_technical = prompt_config.get('include_technical_details', True)
    include_business = prompt_config.get('include_business_context', True)
    
    # Length guidance
    length_guidance = {
        'concise': "Keep descriptions brief and to the point (1 sentence, ~20-30 words).",
        'standard': "Provide clear, professional descriptions (1-2 sentences, ~30-50 words).",
        'detailed': "Generate comprehensive descriptions with full context (2-3 sentences, ~50-80 words)."
    }
    
    prompt = f"""Generate professional descriptions for the following columns in table 'sample_table':

1. Column: customer_id
   Type: STRING
   Sample values: 'CUST001', 'CUST002', 'CUST003'

2. Column: order_amount
   Type: DECIMAL(10,2)
   Sample values: 1299.99, 89.50, 2450.00

For each column, provide a description that includes:"""
    
    if include_technical:
        prompt += "\n- What data it stores (technical details: data type, format, constraints)"
    else:
        prompt += "\n- What data it stores"
    
    if include_business:
        prompt += "\n- Its business purpose and significance"
    else:
        prompt += "\n- Its purpose in the table"
    
    prompt += f"\n\nStyle: {length_guidance.get(column_length, length_guidance['standard'])}"
    
    # Add custom terminology
    if terminology:
        prompt += "\n\nIMPORTANT TERMINOLOGY:"
        for term, meaning in terminology.items():
            prompt += f"\n- '{term}' means '{meaning}'"
    
    # Add additional instructions
    if additional_instructions and additional_instructions.strip():
        prompt += f"\n\nADDITIONAL REQUIREMENTS:\n{additional_instructions.strip()}"
    
    prompt += "\n\nFormat: Return exactly 2 descriptions, one per line, numbered."
    
    return prompt

# --------------------------- Generate API Routes -----------------------------------

@flask_app.route("/api/generation-options/<catalog_name>/<object_type>")
def api_generation_options(catalog_name, object_type):
    """Get objects with missing metadata for generation selection"""
    try:
        logger.info(f"Getting generation options for {catalog_name}/{object_type}")
        
        # Get unity service instance
        unity = get_unity_service()
        
        # Map object_type to the correct method
        if object_type == 'schemas':
            objects = unity.get_schemas_with_missing_metadata(catalog_name)
        elif object_type == 'tables':
            objects = unity.get_tables_with_missing_metadata(catalog_name)
        elif object_type == 'columns':
            objects = unity.get_columns_with_missing_metadata(catalog_name)
        else:
            return jsonify({"error": f"Invalid object type: {object_type}"}), 400
        
        return jsonify({
            "success": True,
            "catalog": catalog_name,
            "object_type": object_type,
            "objects": objects,
            "count": len(objects)
        })
    except Exception as e:
        logger.error(f"Error getting generation options: {e}")
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/styles")
def api_styles():
    """Get available generation styles"""
    return jsonify({
        "styles": [
            {"value": "enterprise", "label": "Enterprise", "description": "Formal, professional tone for business users"},
            {"value": "technical", "label": "Technical", "description": "Detailed technical specifications"},
            {"value": "business", "label": "Business", "description": "Business-focused, non-technical language"},
            {"value": "concise", "label": "Concise", "description": "Brief, to-the-point descriptions"}
        ]
    })

# Import generation functionality from main app
from app import run_async_in_thread, get_enhanced_generator, get_setup_manager
from datetime import datetime as dt

def update_generation_progress(run_id: str, **kwargs):
    """Update progress for a running generation task in app_react context"""
    if not hasattr(flask_app, 'generation_progress'):
        flask_app.generation_progress = {}
    if not hasattr(flask_app, 'progress_last_update'):
        flask_app.progress_last_update = {}
    
    if run_id in flask_app.generation_progress:
        progress_info = flask_app.generation_progress[run_id]
        
        # Update provided fields
        for key, value in kwargs.items():
            if key in progress_info:
                progress_info[key] = value
                logger.info(f"📊 Progress update for {run_id}: {key}={value}")
        
        # Calculate overall progress if processed_objects is updated
        if 'processed_objects' in kwargs and progress_info.get('total_objects', 0) > 0:
            progress_info['progress'] = min(100, int((progress_info['processed_objects'] / progress_info['total_objects']) * 100))
            logger.info(f"📊 Progress: {progress_info['progress']}% ({progress_info['processed_objects']}/{progress_info['total_objects']})")

@flask_app.route("/api/enhanced/run", methods=["POST"])
def api_enhanced_run():
    """Start enhanced metadata generation"""
    try:
        from flask import request
        body = request.get_json() or {}
        catalog = body.get("catalog")
        model = body.get("model", "databricks-gpt-oss-20b")
        pii_model = body.get("piiModel", model)  # Default to metadata model if not specified
        style = body.get("style", "enterprise")
        selected_objects = body.get("selected_objects", {})
        
        if not catalog:
            return jsonify({"error": "Catalog is required", "success": False}), 400
        
        if not selected_objects or selected_objects.get("totalCount", 0) == 0:
            return jsonify({
                "error": "Please select at least one schema, table, or column for generation",
                "success": False
            }), 400
        
        logger.info(f"Starting enhanced generation for catalog {catalog} (metadata model: {model}, PII model: {pii_model})")
        
        # Ensure setup is complete
        setup_manager = get_setup_manager()
        setup_future = run_async_in_thread(setup_manager.ensure_setup_complete())
        setup_status = setup_future.result(timeout=30)
        
        if not setup_status['setup_complete']:
            logger.warning("Setup not complete, but proceeding with generation")
        
        # Start enhanced generation
        enhanced_generator = get_enhanced_generator()
        
        # Generate run ID
        run_id = f"enhanced_{dt.now().strftime('%Y%m%d_%H%M%S')}_{catalog}"
        
        # Set up progress callback
        def progress_callback(**kwargs):
            update_generation_progress(run_id, **kwargs)
        
        enhanced_generator.set_progress_callback(progress_callback)
        
        # Start generation in background with PII model
        generation_future = run_async_in_thread(
            enhanced_generator.generate_enhanced_metadata(catalog, model, style, selected_objects, run_id, pii_model)
        )
        
        # Store future for status checking
        if not hasattr(flask_app, 'generation_tasks'):
            flask_app.generation_tasks = {}
        if not hasattr(flask_app, 'generation_progress'):
            flask_app.generation_progress = {}
        
        flask_app.generation_tasks[run_id] = generation_future
        
        # Initialize progress tracking
        total_objects = selected_objects.get("totalCount", 0)
        flask_app.generation_progress[run_id] = {
            "status": "initializing",
            "progress": 0,
            "total_objects": total_objects,
            "processed_objects": 0,
            "current_phase": "Setup",
            "current_object": "",
            "phases": ["Setup", "Schema Analysis", "Table Analysis", "Column Analysis", "Finalization"],
            "phase_progress": 0,
            "start_time": dt.now().isoformat(),
            "errors": []
        }
        
        return jsonify({
            "success": True,
            "run_id": run_id,
            "catalog": catalog,
            "model": model,
            "pii_model": pii_model,
            "style": style,
            "status": "Enhanced generation started",
            "timestamp": dt.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error starting enhanced generation: {e}")
        return jsonify({"error": str(e), "success": False}), 500

@flask_app.route("/api/enhanced/status/<run_id>")
def api_enhanced_status(run_id):
    """Check status of enhanced generation run"""
    try:
        if not hasattr(flask_app, 'generation_tasks'):
            return jsonify({"error": "No generation tasks found"}), 404
        
        future = flask_app.generation_tasks.get(run_id)
        if not future:
            return jsonify({"error": "Run ID not found"}), 404
        
        # Get progress information
        progress_info = flask_app.generation_progress.get(run_id, {})
        
        if future.done():
            # Generation completed
            try:
                result = future.result()
                
                if 'error' in result:
                    return jsonify({
                        "success": False,
                        "run_id": run_id,
                        "status": "FAILED",
                        "error": result['error'],
                        "timestamp": dt.now().isoformat()
                    })
                elif result.get('status') == 'CANCELLED':
                    # Generation was cancelled by user
                    logger.info(f"🛑 Generation {run_id} was cancelled")
                    
                    # Save any results that were generated before cancellation
                    if result.get('generated_metadata'):
                        try:
                            setup_manager = get_setup_manager()
                            save_future = run_async_in_thread(
                                setup_manager.save_generation_results(result.get('generated_metadata', []))
                            )
                            save_status = save_future.result(timeout=30)
                            logger.info(f"💾 Saved {save_status.get('saved_count', 0)} results from cancelled generation")
                        except Exception as e:
                            logger.error(f"Failed to save cancelled generation results: {e}")
                    
                    return jsonify({
                        "success": True,
                        "run_id": run_id,
                        "status": "CANCELLED",
                        "summary": result.get('summary', {}),
                        "processed_objects": result.get('summary', {}).get('processed_objects', 0),
                        "cancelled_at": result.get('cancelled_at'),
                        "timestamp": dt.now().isoformat()
                    })
                else:
                    # Check if we've already saved results for this run_id (prevent duplicates)
                    if not hasattr(flask_app, 'completed_runs'):
                        flask_app.completed_runs = {}
                    
                    if run_id not in flask_app.completed_runs:
                        # Save results ONCE
                        logger.info(f"💾 Saving results for {run_id} (first time)")
                        setup_manager = get_setup_manager()
                        save_future = run_async_in_thread(
                            setup_manager.save_generation_results(result.get('generated_metadata', []))
                        )
                        save_status = save_future.result(timeout=30)
                        
                        # Cache the completed status to prevent re-saving
                        flask_app.completed_runs[run_id] = {
                            "success": True,
                            "run_id": run_id,
                            "status": "COMPLETED",
                            "summary": result.get('summary', {}),
                            "duration_seconds": result.get('duration_seconds', 0),
                            "results_saved": save_status['saved_count'],
                            "timestamp": dt.now().isoformat()
                        }
                    else:
                        logger.info(f"✅ Results for {run_id} already saved, returning cached status")
                    
                    return jsonify(flask_app.completed_runs[run_id])
                    
            except Exception as e:
                return jsonify({
                    "success": False,
                    "run_id": run_id,
                    "status": "ERROR",
                    "error": str(e),
                    "timestamp": dt.now().isoformat()
                })
        else:
            # Still running
            base_response = {
                "success": True,
                "run_id": run_id,
                "status": "RUNNING",
                "message": "Enhanced generation in progress...",
                "timestamp": dt.now().isoformat()
            }
            
            # Add progress information
            if progress_info:
                base_response.update({
                    "progress": progress_info.get("progress", 0),
                    "total_objects": progress_info.get("total_objects", 0),
                    "processed_objects": progress_info.get("processed_objects", 0),
                    "current_phase": progress_info.get("current_phase", "Processing"),
                    "current_object": progress_info.get("current_object", ""),
                    "phase_progress": progress_info.get("phase_progress", 0),
                    "phases": progress_info.get("phases", []),
                    "start_time": progress_info.get("start_time"),
                    "errors": progress_info.get("errors", [])
                })
            
            return jsonify(base_response)
            
    except Exception as e:
        logger.error(f"Error checking generation status: {e}")
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/enhanced/cancel/<run_id>", methods=['POST'])
def api_enhanced_cancel(run_id):
    """Cancel a running generation task"""
    try:
        if not hasattr(flask_app, 'generation_tasks'):
            return jsonify({"error": "No generation tasks found"}), 404
        
        future = flask_app.generation_tasks.get(run_id)
        if not future:
            return jsonify({"error": "Run ID not found"}), 404
        
        # Check if already completed
        if future.done():
            return jsonify({
                "success": False,
                "message": "Generation already completed",
                "run_id": run_id
            }), 400
        
        # Set cancellation flag
        if not hasattr(flask_app, 'generation_cancellations'):
            flask_app.generation_cancellations = set()
        
        flask_app.generation_cancellations.add(run_id)
        
        logger.info(f"🛑 Cancellation requested for generation run: {run_id}")
        
        # Update progress to show cancellation in progress
        if hasattr(flask_app, 'generation_progress') and run_id in flask_app.generation_progress:
            flask_app.generation_progress[run_id]['current_phase'] = 'Cancelling'
            flask_app.generation_progress[run_id]['current_object'] = 'Cancellation requested...'
        
        return jsonify({
            "success": True,
            "message": "Cancellation requested. Generation will stop after current object completes.",
            "run_id": run_id,
            "timestamp": dt.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error cancelling generation: {e}")
        return jsonify({"error": str(e)}), 500

# --------------------------- Review & Commit API Routes -----------------------------------

@flask_app.route("/api/enhanced/results")
def api_enhanced_results():
    """Get pending generated metadata for review"""
    try:
        from flask import request
        
        # Get query parameters
        run_id = request.args.get("run_id")
        catalog = request.args.get("catalog")
        limit = int(request.args.get("limit", "500"))
        
        # Get setup manager for table configuration
        setup_manager = get_setup_manager()
        config = setup_manager.default_config
        
        # Build SQL query for results table
        out_cat = config['output_catalog']
        out_sch = config['output_schema']
        out_tbl = config['results_table']
        
        # Build WHERE clause
        where_conditions = ["status = 'generated'"]
        
        if run_id:
            # If run_id is provided, only filter by run_id (most specific)
            where_conditions.append(f"run_id = '{run_id}'")
        elif catalog:
            # If no run_id but catalog is provided, filter by catalog and recent results
            where_conditions.append(f"full_name LIKE '{catalog}.%'")
            where_conditions.append("generated_at >= current_timestamp() - INTERVAL '48' HOUR")
        else:
            # No filters provided, just show recent results
            where_conditions.append("generated_at >= current_timestamp() - INTERVAL '48' HOUR")
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        sql_statement = f"""
            SELECT
                full_name,
                object_type AS type,
                proposed_comment AS description,
                confidence_score AS confidence,
                pii_tags,
                policy_tags,
                proposed_policy_tags,
                data_classification,
                source_model,
                generation_style,
                pii_detected,
                run_id,
                generated_at,
                context_used,
                pii_analysis
            FROM {out_cat}.{out_sch}.{out_tbl}
            {where_clause}
            ORDER BY generated_at DESC, confidence_score DESC
            LIMIT {limit}
        """
        
        # Execute query
        future = run_async_in_thread(setup_manager._execute_sql(sql_statement))
        result = future.result(timeout=30)
        
        if result['success']:
            # Convert data array to formatted results
            formatted_results = []
            for row in result.get('data', []):
                # Parse pii_tags to check if actually contains PII
                pii_tags_str = row[4] if len(row) > 4 else '[]'
                try:
                    import json
                    pii_tags = json.loads(pii_tags_str) if isinstance(pii_tags_str, str) else pii_tags_str
                    has_pii = len(pii_tags) > 0 if isinstance(pii_tags, list) else False
                except:
                    has_pii = False
                
                object_type_value = row[1] if len(row) > 1 else ''
                formatted_results.append({
                    'full_name': row[0] if len(row) > 0 else '',
                    'type': object_type_value,
                    'object_type': object_type_value,  # Add object_type for filter compatibility
                    'description': row[2] if len(row) > 2 else '',
                    'confidence': row[3] if len(row) > 3 else 0.0,
                    'pii_tags': pii_tags_str,
                    'policy_tags': row[5] if len(row) > 5 else '[]',
                    'proposed_policy_tags': row[6] if len(row) > 6 else '[]',
                    'data_classification': row[7] if len(row) > 7 else 'INTERNAL',
                    'source_model': row[8] if len(row) > 8 else '',
                    'generation_style': row[9] if len(row) > 9 else '',
                    'pii_detected': has_pii,  # Derived from pii_tags instead of db field
                    'run_id': row[11] if len(row) > 11 else '',
                    'generated_at': row[12] if len(row) > 12 else '',
                    'context_used': row[13] if len(row) > 13 else '{}',
                    'pii_analysis': row[14] if len(row) > 14 else '{}'
                })
            
            logger.info(f"Retrieved {len(formatted_results)} results for review")
            
            return jsonify({
                "success": True,
                "results": formatted_results,
                "count": len(formatted_results),
                "run_id": run_id,
                "source": "enhanced_generation",
                "timestamp": dt.now().isoformat()
            })
        else:
            return jsonify({
                "error": f"Query failed: {result.get('error')}",
                "success": False
            }), 500
            
    except Exception as e:
        logger.error(f"Error reading results: {e}")
        return jsonify({"error": str(e), "success": False}), 500

# --------------------------- Governed Tags API Route -----------------------------------

@flask_app.route("/api/governed-tags")
def api_governed_tags():
    """Get governed tags from Unity Catalog"""
    try:
        unity = get_unity_service()
        governed_tags = unity.get_governed_tags()
        
        return jsonify({
            'success': True,
            'governed_tags': governed_tags,
            'count': len(governed_tags) if isinstance(governed_tags, dict) else 0
        })
        
    except Exception as e:
        logger.error(f"Error fetching governed tags: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'governed_tags': {}
        }), 500

# --------------------------- Quality Metrics API Route -----------------------------------

@flask_app.route("/api/quality/metrics/<catalog_name>")
def api_quality_metrics(catalog_name):
    """Get quality metrics for a catalog (LEGACY - kept for backward compatibility)"""
    try:
        from flask import request
        unity = get_unity_service()
        
        # Get filter params
        filter_object_type = request.args.get('object_type', '')
        filter_data_object = request.args.get('data_object', '')
        filter_owner = request.args.get('owner', '')
        
        logger.info(f"🏆 Getting quality metrics for {catalog_name}")
        
        # Calculate quality metrics
        metrics = unity.calculate_quality_metrics(
            catalog_name,
            filter_object_type=filter_object_type,
            filter_data_object=filter_data_object,
            filter_owner=filter_owner
        )
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Error getting quality metrics: {e}")
        return jsonify({"error": str(e)}), 500


# ==================== SPLIT QUALITY ENDPOINTS (Option A) ====================

@flask_app.route("/api/quality/donut-charts/<catalog_name>")
def api_quality_donut_charts(catalog_name):
    """Get top 3 quality metrics (completeness, accuracy, tag coverage)"""
    try:
        unity = get_unity_service()
        logger.info(f"📊 Getting donut chart metrics for {catalog_name}")
        
        # Get fast counts
        counts = unity._fast_counts_via_sql(catalog_name, '', '', '')
        
        # Calculate metrics
        completeness = unity._calculate_completeness_percentage(counts)
        accuracy = unity._calculate_accuracy_score(catalog_name)
        tag_coverage = unity._calculate_tag_coverage_percentage(catalog_name, '', '')
        
        return jsonify({
            'completeness': completeness,
            'accuracy': accuracy,
            'tagCoverage': tag_coverage
        })
    except Exception as e:
        logger.error(f"Error getting donut chart metrics: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/quality/numeric-tiles/<catalog_name>")
def api_quality_numeric_tiles(catalog_name):
    """Get numeric tile metrics (PII exposure, review backlog, time to document) with graceful fallbacks"""
    try:
        unity = get_unity_service()
        logger.info(f"📊 Getting numeric tiles for {catalog_name}")
        
        # Try each metric independently with fallbacks
        pii_exposure = 0
        review_backlog = 0
        time_to_document = 0
        
        try:
            pii_exposure = unity._calculate_pii_exposure(catalog_name, '', '')
        except Exception as e:
            logger.warning(f"PII exposure calculation failed, using 0: {e}")
            pii_exposure = 0
        
        try:
            review_backlog = unity._calculate_review_backlog(catalog_name)
        except Exception as e:
            logger.warning(f"Review backlog calculation failed, using 0: {e}")
            review_backlog = 0
        
        try:
            time_to_document = unity._calculate_time_to_document(catalog_name)
        except Exception as e:
            logger.warning(f"Time to document calculation failed, using 0: {e}")
            time_to_document = 0
        
        return jsonify({
            'piiExposure': pii_exposure,
            'reviewBacklog': review_backlog,
            'timeToDocument': time_to_document
        })
    except Exception as e:
        logger.error(f"Error getting numeric tiles: {e}", exc_info=True)
        # Return zeros instead of error
        return jsonify({
            'piiExposure': 0,
            'reviewBacklog': 0,
            'timeToDocument': 0
        })


@flask_app.route("/api/quality/trend/<catalog_name>")
def api_quality_trend(catalog_name):
    """Get completeness trend data"""
    try:
        unity = get_unity_service()
        logger.info(f"📈 Getting completeness trend for {catalog_name}")
        
        trend = unity._calculate_completeness_trend(catalog_name)
        
        return jsonify({'trend': trend})
    except Exception as e:
        logger.error(f"Error getting trend: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/quality/leaderboard/<catalog_name>")
def api_quality_leaderboard(catalog_name):
    """Get owner leaderboard with graceful fallback"""
    try:
        unity = get_unity_service()
        logger.info(f"👥 Getting owner leaderboard for {catalog_name}")
        
        leaderboard = unity._calculate_owner_leaderboard(catalog_name)
        
        # Check if leaderboard is empty
        if not leaderboard or len(leaderboard) == 0:
            logger.warning(f"No owner data available for {catalog_name}")
            return jsonify({'leaderboard': []})
        
        return jsonify({'leaderboard': leaderboard})
    except Exception as e:
        logger.warning(f"Owner leaderboard failed for {catalog_name}: {e}")
        # Return empty array instead of error
        return jsonify({'leaderboard': []})


@flask_app.route("/api/quality/schema-coverage/<catalog_name>")
def api_quality_schema_coverage(catalog_name):
    """Get schema coverage heatmap"""
    try:
        unity = get_unity_service()
        logger.info(f"🗺️ Getting schema coverage for {catalog_name}")
        
        coverage = unity._calculate_schema_coverage_heatmap(catalog_name)
        
        return jsonify({'coverage': coverage})
    except Exception as e:
        logger.error(f"Error getting schema coverage: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/quality/pii-risk/<catalog_name>")
def api_quality_pii_risk(catalog_name):
    """Get PII risk matrix"""
    try:
        unity = get_unity_service()
        logger.info(f"🔒 Getting PII risk matrix for {catalog_name}")
        
        risk_matrix = unity._calculate_pii_risk_matrix(catalog_name)
        
        return jsonify({'riskMatrix': risk_matrix})
    except Exception as e:
        logger.error(f"Error getting PII risk: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/quality/confidence/<catalog_name>")
def api_quality_confidence(catalog_name):
    """Get confidence distribution with graceful fallback"""
    try:
        unity = get_unity_service()
        logger.info(f"📊 Getting confidence distribution for {catalog_name}")
        
        distribution = unity._calculate_confidence_distribution(catalog_name)
        
        # Check if distribution is empty or None
        if not distribution or len(distribution) == 0:
            logger.warning(f"No confidence data available for {catalog_name}, returning empty")
            return jsonify({'distribution': []})
        
        return jsonify({'distribution': distribution})
    except Exception as e:
        logger.warning(f"Confidence distribution failed for {catalog_name}: {e}")
        # Return empty array instead of error
        return jsonify({'distribution': []})


@flask_app.route("/api/metadata-history/<catalog_name>")
def api_metadata_history(catalog_name):
    """Get metadata update history for a catalog"""
    try:
        from flask import request
        
        logger.info(f"📜 Starting metadata history fetch for {catalog_name}")
        
        unity = get_unity_service()
        setup_manager = get_setup_manager()
        
        # Get filter parameters
        days = int(request.args.get('days', 7))  # Default to 7 days
        filter_object_type = request.args.get('object_type', '')
        filter_data_object = request.args.get('data_object', '')
        
        logger.info(f"📜 Fetching metadata history for {catalog_name} (last {days} days)")
        
        # Query the metadata_results table for history
        config = setup_manager.default_config
        results_table = f"{config['output_catalog']}.{config['output_schema']}.{config['results_table']}"
        
        # Build WHERE conditions
        where_conditions = []
        
        # Time filter
        if days > 0:
            where_conditions.append(f"generated_at >= current_timestamp() - INTERVAL {days} DAY")
        
        # Catalog filter
        where_conditions.append(f"full_name LIKE '{catalog_name}.%'")
        
        # Object type filter
        if filter_object_type:
            where_conditions.append(f"object_type = '{filter_object_type}'")
        
        # Data object filter
        if filter_data_object:
            where_conditions.append(f"full_name LIKE '%{filter_data_object}%'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query for history records (matching original app.py structure)
        # NOTE: No LIMIT here - let frontend handle pagination of full dataset
        # If performance becomes an issue with large datasets, implement server-side pagination
        query = f"""
            SELECT 
                generated_at as date,
                full_name,
                object_type,
                source_model,
                status,
                'AI Generated' as source_type
            FROM {results_table}
            {where_clause}
            ORDER BY generated_at DESC
        """
        
        logger.info(f"📜 Executing history query on {results_table}")
        
        try:
            # Execute query using setup_manager (same as review endpoint)
            future = run_async_in_thread(setup_manager._execute_sql(query))
            result = future.result(timeout=30)
        except Exception as query_error:
            logger.error(f"❌ SQL query failed: {query_error}")
            # Return empty history if table doesn't exist or query fails
            return jsonify({
                'history': [],
                'total': 0,
                'message': 'No history available yet. Generate some metadata first!'
            })
        
        history_records = []
        if result.get('success') and result.get('data'):
            total_rows = len(result.get('data', []))
            logger.info(f"📊 Retrieved {total_rows} history records from database")
            
            for row in result.get('data', []):
                if len(row) >= 6:
                    # Query columns: generated_at(0), full_name(1), object_type(2), source_model(3), status(4), source_type(5)
                    generated_at, full_name, object_type, source_model, status, source_type = row[:6]
                    
                    # Handle null object_type
                    object_type_str = object_type.lower() if object_type else 'object'
                    object_type_display = object_type.title() if object_type else 'Unknown'
                    
                    # Determine action and changes based on status (matching original app.py logic)
                    if status == 'committed':
                        action = 'Committed'
                        changes = f"Committed {object_type_str} metadata to Unity Catalog"
                    elif status == 'generated':
                        action = 'Generated'
                        changes = f"Generated {object_type_str} description"
                    elif status == 'cancelled':
                        action = 'Cancelled'
                        changes = f"Cancelled {object_type_str} metadata generation"
                    elif status == 'error':
                        action = 'Failed'
                        changes = f"Failed to generate {object_type_str} metadata"
                    else:
                        action = status.capitalize() if status else 'Unknown'
                        changes = f"{action} {object_type_str} metadata"
                    
                    history_records.append({
                        'date': generated_at,
                        'object': full_name,
                        'type': object_type_display,
                        'action': action,
                        'changes': changes,
                        'source': source_type,
                        'model': source_model
                    })
        
        logger.info(f"📊 Found {len(history_records)} history records")
        
        return jsonify({
            'history': history_records,
            'total': len(history_records)
        })
        
    except Exception as e:
        logger.error(f"❌ Error getting metadata history: {e}", exc_info=True)
        # Return empty history instead of 500 error
        return jsonify({
            'history': [],
            'total': 0,
            'message': 'Unable to load history. The metadata tracking table may not be set up yet.'
        })


def update_commit_progress(run_id: str, **kwargs):
    """Update progress for a running commit task in app_react context"""
    if not hasattr(flask_app, 'commit_progress'):
        flask_app.commit_progress = {}
    if not hasattr(flask_app, 'commit_progress_last_update'):
        flask_app.commit_progress_last_update = {}
    
    if run_id in flask_app.commit_progress:
        progress_info = flask_app.commit_progress[run_id]
        
        # Update provided fields
        for key, value in kwargs.items():
            if key in progress_info:
                progress_info[key] = value
                logger.info(f"📊 Commit progress update for {run_id}: {key}={value}")
        
        # Calculate overall progress if processed_objects is updated
        if 'processed_objects' in kwargs and progress_info.get('total_objects', 0) > 0:
            progress_info['progress'] = min(100, int((progress_info['processed_objects'] / progress_info['total_objects']) * 100))
            logger.info(f"📊 Commit Progress: {progress_info['progress']}% ({progress_info['processed_objects']}/{progress_info['total_objects']})")

@flask_app.route("/api/submit-metadata", methods=["POST"])
def api_submit_metadata():
    """Submit approved metadata to Unity Catalog"""
    try:
        from flask import request
        unity = get_unity_service()
        data = request.get_json()
        items = data.get('items', [])
        
        if not items:
            return jsonify({
                'success': False,
                'error': 'No items provided',
                'total': 0
            }), 400
        
        # Validate permissions for all catalogs
        catalogs_to_check = set()
        for item in items:
            full_name = item.get('full_name', '')
            if '.' in full_name:
                catalog_name = full_name.split('.')[0]
                catalogs_to_check.add(catalog_name)
        
        permission_errors = []
        for catalog_name in catalogs_to_check:
            perm_result = unity.validate_catalog_permissions(catalog_name)
            if not perm_result.get('has_access', False):
                permission_errors.append(f"Catalog '{catalog_name}': {perm_result.get('error', 'Access denied')}")
        
        if permission_errors:
            return jsonify({
                'success': False,
                'submitted': 0,
                'errors': permission_errors,
                'total': len(items),
                'message': 'Permission validation failed. Cannot proceed.',
                'has_permission_errors': True
            }), 403
        
        # Generate run ID for tracking
        run_id = f"commit_{dt.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Initialize commit progress tracking
        if not hasattr(flask_app, 'commit_tasks'):
            flask_app.commit_tasks = {}
        if not hasattr(flask_app, 'commit_progress'):
            flask_app.commit_progress = {}
        
        flask_app.commit_progress[run_id] = {
            "status": "initializing",
            "progress": 0,
            "total_objects": len(items),
            "processed_objects": 0,
            "current_phase": "Committing to Unity Catalog",
            "current_object": "Starting commit...",
            "start_time": dt.now().isoformat(),
            "errors": []
        }
        
        # Transform items to match expected format for commit function
        transformed_items = []
        for item in items:
            # Debug: log what we're receiving
            logger.info(f"📦 Raw item keys: {list(item.keys())}")
            logger.info(f"📦 Raw item object_type: {item.get('object_type')}")
            
            # Get object type - handle both lowercase and uppercase field names
            obj_type = item.get('object_type') or item.get('type') or ''
            if obj_type:
                obj_type = obj_type.upper()  # Normalize to uppercase first
                # Map to lowercase for commit function
                type_map = {'SCHEMA': 'schema', 'TABLE': 'table', 'COLUMN': 'column'}
                obj_type = type_map.get(obj_type, obj_type.lower())
            
            transformed = {
                'full_name': item.get('full_name'),
                'generated_comment': item.get('description') or item.get('proposed_comment'),
                'type': obj_type,
                'proposed_policy_tags': item.get('proposed_policy_tags', '[]'),
                'apply_tags': True  # Default to applying tags
            }
            logger.info(f"🔄 Transformed item for commit: {transformed['full_name']} (type='{transformed['type']}', comment={transformed['generated_comment'][:50] if transformed['generated_comment'] else 'None'}...)")
            
            if not transformed['type']:
                logger.error(f"❌ WARNING: Empty type for {transformed['full_name']}! Item will fail to commit!")
            
            transformed_items.append(transformed)
        
        # Store items for later status update
        flask_app.commit_progress[run_id]['items'] = transformed_items
        
        # Make update_commit_progress available in app module context
        import app
        app.update_commit_progress = update_commit_progress
        
        # Import commit function from main app
        from app import commit_metadata_with_progress
        
        logger.info(f"🚀 Starting commit for {len(transformed_items)} items with run_id: {run_id}")
        
        # Start commit in background thread
        commit_future = run_async_in_thread(
            commit_metadata_with_progress(unity, transformed_items, run_id)
        )
        
        flask_app.commit_tasks[run_id] = commit_future
        
        return jsonify({
            'success': True,
            'run_id': run_id,
            'status': 'Commit started',
            'total': len(items),
            'timestamp': dt.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in submit-metadata: {e}")
        return jsonify({"error": str(e), "success": False}), 500

@flask_app.route("/api/delete-metadata", methods=["POST"])
def api_delete_metadata():
    """Delete (cancel) generated metadata items without committing them - async background processing"""
    try:
        from flask import request
        data = request.get_json()
        items = data.get('items', [])
        
        if not items:
            return jsonify({
                'success': False,
                'error': 'No items provided',
                'deleted': 0
            }), 400
        
        # Get setup manager for table configuration
        setup_manager = get_setup_manager()
        config = setup_manager.default_config
        out_cat = config['output_catalog']
        out_sch = config['output_schema']
        out_tbl = config['results_table']
        
        # Build list of full_names
        full_names = [item['full_name'] for item in items]
        
        # Escape single quotes in full_names and build SQL-safe list
        escaped_names = [name.replace("'", "''") for name in full_names]
        full_names_str = "', '".join(escaped_names)
        
        # Update status to 'cancelled' instead of deleting
        delete_sql = f"""
            UPDATE `{out_cat}`.`{out_sch}`.`{out_tbl}`
            SET status = 'cancelled'
            WHERE full_name IN ('{full_names_str}')
            AND status = 'generated'
        """
        
        logger.info(f"🗑️ Queueing deletion for {len(full_names)} items (background processing)")
        
        # Execute in background thread - don't wait for result
        def delete_in_background():
            try:
                future = run_async_in_thread(setup_manager._execute_sql(delete_sql))
                result = future.result(timeout=30)
                
                if result.get('success', False):
                    logger.info(f"✅ Background deletion completed for {len(full_names)} items")
                else:
                    logger.error(f"❌ Background deletion failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"❌ Background deletion exception: {e}")
        
        # Start background thread
        import threading
        background_thread = threading.Thread(target=delete_in_background)
        background_thread.daemon = True
        background_thread.start()
        
        # Return immediately
        logger.info(f"🚀 Deletion request accepted for {len(full_names)} items, processing in background")
        
        return jsonify({
            'success': True,
            'deleted': len(full_names),
            'message': f'Successfully queued removal of {len(full_names)} item(s)',
            'async': True
        })
        
    except Exception as e:
        logger.error(f"Error initiating metadata deletion: {e}")
        return jsonify({"error": str(e), "success": False}), 500

@flask_app.route("/api/commit/status/<run_id>")
def api_commit_status(run_id):
    """Check status of commit operation"""
    try:
        if not hasattr(flask_app, 'commit_tasks'):
            return jsonify({"error": "No commit tasks found"}), 404
        
        future = flask_app.commit_tasks.get(run_id)
        if not future:
            return jsonify({"error": "Run ID not found"}), 404
        
        # Get progress information
        progress_info = flask_app.commit_progress.get(run_id, {})
        
        if future.done():
            # Commit completed
            try:
                result = future.result()
                
                if not result.get('success', False):
                    return jsonify({
                        "success": False,
                        "run_id": run_id,
                        "status": "FAILED",
                        "error": result.get('error', 'Unknown error'),
                        "errors": result.get('errors', []),
                        "timestamp": dt.now().isoformat()
                    })
                else:
                    # Mark successfully committed items as 'committed' in database
                    try:
                        items = flask_app.commit_progress[run_id].get('items', [])
                        submitted_count = result.get('submitted', 0)
                        
                        if items and submitted_count > 0:
                            setup_manager = get_setup_manager()
                            config = setup_manager.default_config
                            out_cat = config['output_catalog']
                            out_sch = config['output_schema']
                            out_tbl = config['results_table']
                            
                            # Build UPDATE statement to mark items as committed
                            # Use the first N items (where N = submitted_count)
                            submitted_items = items[:submitted_count]
                            full_names = [item['full_name'] for item in submitted_items]
                            
                            if full_names:
                                # Escape single quotes in full_names
                                escaped_names = [name.replace("'", "''") for name in full_names]
                                full_names_str = "', '".join(escaped_names)
                                
                                update_sql = f"""
                                    UPDATE `{out_cat}`.`{out_sch}`.`{out_tbl}`
                                    SET status = 'committed'
                                    WHERE full_name IN ('{full_names_str}')
                                    AND status = 'generated'
                                """
                                
                                # Execute and wait for result
                                update_future = run_async_in_thread(setup_manager._execute_sql(update_sql))
                                update_result = update_future.result(timeout=30)
                                
                                if update_result.get('success', False):
                                    logger.info(f"✅ Marked {len(full_names)} items as committed in database")
                                else:
                                    logger.error(f"❌ Failed to mark items as committed: {update_result.get('error', 'Unknown error')}")
                    except Exception as e:
                        logger.error(f"Failed to update commit status in database: {e}")
                        # Don't fail the whole operation, just log the error
                    
                    return jsonify({
                        "success": True,
                        "run_id": run_id,
                        "status": "COMPLETED",
                        "submitted": result.get('submitted', 0),
                        "failed": result.get('failed', 0),
                        "total": result.get('total', 0),
                        "errors": result.get('errors', []),
                        "timestamp": dt.now().isoformat()
                    })
                    
            except Exception as e:
                return jsonify({
                    "success": False,
                    "run_id": run_id,
                    "status": "ERROR",
                    "error": str(e),
                    "timestamp": dt.now().isoformat()
                })
        else:
            # Still running
            base_response = {
                "success": True,
                "run_id": run_id,
                "status": "RUNNING",
                "message": "Committing to Unity Catalog...",
                "timestamp": dt.now().isoformat()
            }
            
            # Add progress information
            if progress_info:
                base_response.update({
                    "progress": progress_info.get("progress", 0),
                    "total_objects": progress_info.get("total_objects", 0),
                    "processed_objects": progress_info.get("processed_objects", 0),
                    "current_phase": progress_info.get("current_phase", "Processing"),
                    "current_object": progress_info.get("current_object", ""),
                    "start_time": progress_info.get("start_time"),
                    "errors": progress_info.get("errors", [])
                })
            
            return jsonify(base_response)
            
    except Exception as e:
        logger.error(f"Error checking commit status: {e}")
        return jsonify({"error": str(e)}), 500

# --------------------------- Health Check -----------------------------------
@flask_app.route('/api/health')
def api_health():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "message": "React + Flask backend is running",
        "react_build_exists": os.path.exists(react_build_path)
    })

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    if not os.path.exists(react_build_path):
        logger.warning(f"⚠️ React build folder not found at {react_build_path}")
        logger.warning("Run 'cd client && npm install && npm run build' to build the React app")
    else:
        logger.info("✅ React build folder found, ready to serve!")
    
    flask_app.run(host="0.0.0.0", port=5000, debug=True)


"""
Models Configuration Manager
============================

Handles model management including enable/disable, adding custom models,
and model validation for Databricks native models.
"""

import logging
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class ModelsConfigManager:
    """
    Manages model configuration for the UC Metadata Assistant.
    Handles built-in and custom Databricks native models.
    """
    
    def __init__(self, llm_service, settings_manager):
        self.llm_service = llm_service
        self.settings_manager = settings_manager
        
        # Built-in models (from LLMMetadataGenerator)
        self.builtin_models = {
            "databricks-gpt-oss-20b": {
                "name": "GPT-OSS-20B",
                "description": "GPT-OSS 20B is a state-of-the-art, lightweight reasoning model built and trained by OpenAI. This model also has a 128K token context window and excels at real-time copilots and batch inference tasks.",
                "max_tokens": 4096,
                "builtin": True
            },
            "databricks-claude-sonnet-4": {
                "name": "Claude Sonnet 4", 
                "description": "Anthropic Claude Sonnet 4 - Advanced reasoning",
                "max_tokens": 4096,
                "builtin": True
            },
            "databricks-meta-llama-3-1-8b-instruct": {
                "name": "Llama 3.1 8B",
                "description": "Llama 3.1 is a state-of-the-art 8B parameter dense language model trained and released by Meta. The model supports a context length of 128K tokens.", 
                "max_tokens": 4096,
                "builtin": True
            },
            "databricks-gemma-3-12b": {
                "name": "Gemma 3 12B",
                "description": "Google Gemma 3 12B - Efficient performance",
                "max_tokens": 1024,
                "builtin": True
            }
        }
    
    def get_available_models(self) -> Dict[str, Dict]:
        """Get all available models (builtin + custom) with their enabled status"""
        try:
            models_config = self.settings_manager.get_models_config()
            enabled_models = models_config.get('enabled_models', [])
            custom_models = models_config.get('custom_models', [])
            
            # Combine builtin and custom models
            all_models = {}
            
            # Add builtin models
            for model_id, model_info in self.builtin_models.items():
                all_models[model_id] = {
                    **model_info,
                    "enabled": model_id in enabled_models,
                    "status": "available" if model_id in enabled_models else "disabled"
                }
            
            # Add custom models
            for custom_model in custom_models:
                model_id = custom_model['model_id']
                all_models[model_id] = {
                    **custom_model,
                    "enabled": model_id in enabled_models,
                    "builtin": False
                }
            
            return all_models
            
        except Exception as e:
            logger.error(f"❌ Failed to get available models: {e}")
            return self.builtin_models
    
    def get_enabled_models(self) -> List[str]:
        """Get list of enabled model IDs"""
        try:
            models_config = self.settings_manager.get_models_config()
            return models_config.get('enabled_models', list(self.builtin_models.keys()))
        except Exception as e:
            logger.error(f"❌ Failed to get enabled models: {e}")
            return list(self.builtin_models.keys())
    
    def enable_model(self, model_id: str) -> bool:
        """Enable a specific model"""
        try:
            models_config = self.settings_manager.get_models_config()
            enabled_models = models_config.get('enabled_models', [])
            
            if model_id not in enabled_models:
                enabled_models.append(model_id)
                self.settings_manager.update_models_config({
                    'enabled_models': enabled_models
                })
                logger.info(f"✅ Enabled model: {model_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to enable model {model_id}: {e}")
            return False
    
    def disable_model(self, model_id: str) -> bool:
        """Disable a specific model"""
        try:
            models_config = self.settings_manager.get_models_config()
            enabled_models = models_config.get('enabled_models', [])
            
            if model_id in enabled_models:
                enabled_models.remove(model_id)
                self.settings_manager.update_models_config({
                    'enabled_models': enabled_models
                })
                logger.info(f"✅ Disabled model: {model_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to disable model {model_id}: {e}")
            return False
    
    def add_custom_model(self, model_name: str, display_name: str, 
                              description: str, max_tokens: int = None) -> Tuple[bool, str]:
        """
        Add a custom Databricks native model
        
        Args:
            model_name: The model name (as served in Databricks - no prefix needed)
            display_name: Human-readable name
            description: Model description
            max_tokens: Maximum tokens for the model
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Use the model name as provided - custom models don't need databricks- prefix
            model_id = model_name
            
            # Apply smart defaults for max_tokens
            if max_tokens is None:
                max_tokens = self._get_smart_token_default(model_name)
            
            # Validate model doesn't already exist
            all_models = self.get_available_models()
            if model_id in all_models:
                return False, f"Model {model_id} already exists"
            
            # Validate the model by testing connectivity
            is_valid, validation_message = self.validate_model(model_id)
            if not is_valid:
                return False, f"Model validation failed: {validation_message}"
            
            # Add to custom models
            models_config = self.settings_manager.get_models_config()
            custom_models = models_config.get('custom_models', [])
            
            new_model = {
                "model_id": model_id,
                "name": display_name,
                "description": description,
                "max_tokens": max_tokens,
                "builtin": False,
                "status": "available",
                "added_at": datetime.now().isoformat()
            }
            
            custom_models.append(new_model)
            
            # Enable the model by default
            enabled_models = models_config.get('enabled_models', [])
            enabled_models.append(model_id)
            
            self.settings_manager.update_models_config({
                'custom_models': custom_models,
                'enabled_models': enabled_models
            })
            
            logger.info(f"✅ Added custom model: {model_id}")
            return True, f"Successfully added model {model_id}"
            
        except Exception as e:
            logger.error(f"❌ Failed to add custom model: {e}")
            return False, f"Failed to add model: {str(e)}"
    
    def remove_custom_model(self, model_id: str) -> bool:
        """Remove a custom model"""
        try:
            models_config = self.settings_manager.get_models_config()
            custom_models = models_config.get('custom_models', [])
            enabled_models = models_config.get('enabled_models', [])
            
            # Remove from custom models
            custom_models = [m for m in custom_models if m['model_id'] != model_id]
            
            # Remove from enabled models
            if model_id in enabled_models:
                enabled_models.remove(model_id)
            
            self.settings_manager.update_models_config({
                'custom_models': custom_models,
                'enabled_models': enabled_models
            })
            
            logger.info(f"✅ Removed custom model: {model_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to remove custom model {model_id}: {e}")
            return False
    
    def validate_model(self, model_id: str) -> Tuple[bool, str]:
        """
        Validate that a model can be accessed via Databricks serving endpoints
        
        Args:
            model_id: The model ID to validate
            
        Returns:
            Tuple of (is_valid: bool, message: str)
        """
        try:
            # Test with a simple prompt
            test_prompt = "Hello, this is a test."
            
            # Use the LLM service to test the model
            response = self.llm_service._call_databricks_llm(
                prompt=test_prompt,
                max_tokens=10,
                model=model_id,
                temperature=0.1
            )
            
            if response and len(response.strip()) > 0:
                return True, "Model validation successful"
            else:
                return False, "Model returned empty response"
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False, f"Model endpoint not found: {model_id}"
            elif e.response.status_code == 403:
                return False, f"Access denied to model: {model_id}"
            else:
                return False, f"HTTP error {e.response.status_code}: {e}"
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def get_model_info(self, model_id: str) -> Optional[Dict]:
        """Get detailed information about a specific model"""
        try:
            all_models = self.get_available_models()
            return all_models.get(model_id)
        except Exception as e:
            logger.error(f"❌ Failed to get model info for {model_id}: {e}")
            return None
    
    def get_builtin_models(self) -> Dict[str, Dict]:
        """Get the built-in models dictionary"""
        return self.builtin_models.copy()
    
    def _get_smart_token_default(self, model_name: str) -> int:
        """
        Get smart default for max tokens based on model name patterns
        
        Args:
            model_name: The model name to analyze
            
        Returns:
            Recommended max tokens value
        """
        model_lower = model_name.lower()
        
        # Pattern-based defaults
        if any(pattern in model_lower for pattern in ['gpt-4', 'claude', 'sonnet']):
            return 4096  # Large context models
        elif any(pattern in model_lower for pattern in ['7b', 'small', 'mini']):
            return 2048  # Smaller models
        elif any(pattern in model_lower for pattern in ['13b', '20b', '30b']):
            return 3072  # Medium models
        elif any(pattern in model_lower for pattern in ['70b', '120b', 'large', 'xl']):
            return 4096  # Large models
        elif 'instruct' in model_lower:
            return 2048  # Instruction-tuned models (often optimized for shorter responses)
        else:
            return 2048  # Safe default for metadata generation
    
    def _estimate_tokens_for_metadata(self) -> int:
        """
        Estimate token requirements for typical metadata generation tasks
        
        Returns:
            Estimated token count needed for metadata generation
        """
        # Typical metadata generation breakdown:
        # - Prompt: ~200-400 tokens
        # - Context (table/schema info): ~300-800 tokens  
        # - Response (description): ~200-600 tokens
        # - Buffer: ~500 tokens
        # Total: ~1200-2300 tokens
        
        return 2048  # Safe default that covers most metadata use cases

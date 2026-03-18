"""
Self-contained PII Detection Module
==================================

Lightweight PII/PHI detection without external dependencies.
Provides enterprise-grade data classification and policy tagging.

Based on common PII patterns and data analysis techniques used in dbxmetagen.

Version: 2.0 (Batch Processing)
"""

import re
import json
from typing import List, Dict, Optional, Tuple, Set, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PIIDetector:
    """
    Self-contained PII detection with pattern matching and data analysis.
    Provides similar functionality to Presidio but embedded and lightweight.
    """
    
    def __init__(self, settings_manager=None, llm_service=None, unity_service=None):
        self.settings_manager = settings_manager
        self.llm_service = llm_service
        self.unity_service = unity_service
        self.patterns = self._initialize_patterns()
        self._governed_tags_cache = None  # Cache governed tags to avoid repeated API calls
        self._tag_mappings_cache = None  # Cache validated tag mappings to avoid repeated validation
        self.keywords = self._initialize_keywords()
        self.data_classifications = self._initialize_classifications()
        
        # PII detection settings
        self.enabled = True  # PII detection enabled by default
        self.llm_assessment = True if llm_service else False  # LLM-based PII detection if LLM service available
        
        # NOTE: LLM model is now passed explicitly to analyze_columns_batch() instead of storing here
        # The old self.llm_model = self._get_pii_llm_model() is no longer needed
        
        # Load custom patterns if settings manager is available
        if self.settings_manager:
            self._load_custom_patterns()
    
    def _initialize_patterns(self) -> Dict[str, re.Pattern]:
        """Initialize PII detection patterns"""
        return {
            # Personal Identifiers
            'ssn': re.compile(r'\b\d{3}-\d{2}-\d{4}\b|\b\d{9}\b'),
            'phone': re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'),
            'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            'credit_card': re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b'),
            'date_of_birth': re.compile(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b'),
            
            # Medical
            'mrn': re.compile(r'\b[A-Z0-9]{6,12}\b'),  # Medical Record Numbers
            'npi': re.compile(r'\b\d{10}\b'),  # National Provider Identifier
            'icd': re.compile(r'\b[A-Z]\d{2}(\.\d+)?\b'),  # ICD codes
            
            # Financial
            'account_number': re.compile(r'\b\d{8,17}\b'),
            'routing_number': re.compile(r'\b\d{9}\b'),
            'aba': re.compile(r'\b\d{9}\b'),
            
            # Government
            'passport': re.compile(r'\b[A-Z0-9]{6,9}\b'),
            'license': re.compile(r'\b[A-Z0-9]{5,12}\b'),
            'tax_id': re.compile(r'\b\d{2}-\d{7}\b'),
        }
    
    def _initialize_keywords(self) -> Dict[str, List[str]]:
        """
        Initialize PII keyword detection - CONSERVATIVE APPROACH
        
        Only flags truly sensitive PERSONAL identifiers that require regulatory protection.
        Generic business terms (department, position, hospital) are NOT PII.
        Ambiguous words (degree, patient, provider) are NOT included to avoid false positives.
        """
        return {
            'personal_info': [
                'first_name', 'last_name', 'full_name', 'fname', 'lname',
                'home_address', 'street_address', 'residential_address', 'mailing_address',
                'phone_number', 'mobile_number', 'cell_phone', 'telephone_number',
                'email_address',
                'ssn', 'social_security_number', 'sin',
                'date_of_birth', 'dob', 'birth_date', 'birthdate',
                'drivers_license', 'driver_license', 'passport_number', 'national_id'
            ],
            'financial_pci': [
                # Payment Card Industry (PCI-DSS)
                'credit_card', 'credit_card_number', 'card_number', 'cc_number', 'cvv', 'cvv2',
                'bank_account', 'bank_account_number', 'routing_number', 'aba_number', 
                'swift_code', 'iban', 'account_number'
            ],
            'financial_regulated': [
                # SOX, GLBA, banking regulations
                'account_balance', 'transaction_amount', 'trade_amount', 'investment_balance',
                'portfolio_value', 'asset_value', 'revenue', 'profit', 'loss',
                'personal_salary', 'individual_income', 'employee_salary', 'compensation',
                'wire_transfer', 'ach_transfer', 'payment_detail'
            ],
            'medical_phi': [
                # Only truly sensitive medical identifiers (PHI under HIPAA)
                'patient_id', 'patient_number', 'mrn', 'medical_record_number',
                'diagnosis_code', 'icd_code', 'procedure_code', 'prescription',
                'health_insurance', 'insurance_id', 'policy_number',
                'lab_result', 'test_result', 'blood_test', 'genetic_test'
            ],
            'education': [
                # Only specific educational PII (student_id, transcript)
                # NOT generic terms like "school" or "degree" (handled by context-aware logic)
                'student_id', 'transcript', 'gpa', 'grade_level'
            ],
            'sensitive_ids': [
                'employee_id', 'emp_id', 'staff_id',
                'customer_id', 'user_id', 'account_id'
            ],
            'biometric': [
                'fingerprint', 'retina_scan', 'iris_scan', 'facial_recognition',
                'biometric_id', 'dna_sequence', 'genetic_profile'
            ]
        }
    
    def _initialize_classifications(self) -> Dict[str, Dict]:
        """Initialize data classification levels"""
        return {
            'PUBLIC': {
                'risk_level': 0,
                'description': 'Public information with no privacy concerns',
                'tags': ['public']
            },
            'INTERNAL': {
                'risk_level': 1,
                'description': 'Internal business information',
                'tags': ['internal', 'business']
            },
            'CONFIDENTIAL': {
                'risk_level': 2,
                'description': 'Confidential information requiring protection',
                'tags': ['confidential', 'protected']
            },
            'RESTRICTED': {
                'risk_level': 3,
                'description': 'Restricted information with legal/regulatory requirements',
                'tags': ['restricted', 'compliance']
            },
            'PII': {
                'risk_level': 4,
                'description': 'Personally Identifiable Information',
                'tags': ['pii', 'personal', 'privacy']
            },
            'PHI': {
                'risk_level': 5,
                'description': 'Protected Health Information (HIPAA)',
                'tags': ['phi', 'health', 'hipaa', 'medical']
            },
            'PCI': {
                'risk_level': 4,
                'description': 'Payment Card Industry data',
                'tags': ['pci', 'financial', 'payment']
            }
        }
    
    def _get_pii_llm_model(self):
        """
        Get fallback PII model (for backwards compatibility).
        In the new architecture, PII model should be explicitly passed from the frontend.
        This method returns a default only if no model is provided.
        """
        default_model = 'databricks-gemma-3-12b'
        
        # Validate that the default model is enabled
        try:
            from models_config import ModelsConfigManager
            models_mgr = ModelsConfigManager(self.llm_service, self.settings_manager)
            models = models_mgr.get_available_models()
            
            # Check if default model exists and is enabled
            if default_model in models and models[default_model].get('enabled', False):
                logger.debug(f"🤖 Fallback PII model available for initialization: {default_model}")
                return default_model
            else:
                logger.warning(f"⚠️ Default PII model '{default_model}' is disabled. Skipping LLM detection.")
                return None
                
        except Exception as e:
            logger.warning(f"Could not validate model status: {e}, using default: {default_model}")
            return default_model
            
        except Exception as e:
            logger.warning(f"Failed to get PII model config: {e}, using default: {default_model}")
            return default_model
    
    def _load_custom_patterns(self):
        """Load custom PII patterns from Settings Manager"""
        try:
            if not self.settings_manager:
                return
                
            # Get PII configuration from settings
            pii_config = self.settings_manager.get_pii_config()
            custom_patterns = pii_config.get('custom_patterns', [])
            
            logger.info(f"🔍 Loading {len(custom_patterns)} custom PII patterns")
            
            # Add custom patterns to keywords
            for pattern in custom_patterns:
                if not pattern.get('enabled', True):
                    continue
                    
                pattern_name = pattern.get('name', '').lower().replace(' ', '_')
                keywords_raw = pattern.get('keywords', '')
                
                # Parse keywords - handle both string and list formats
                if isinstance(keywords_raw, str):
                    # Split comma-separated string and clean up
                    keywords = [k.strip().lower() for k in keywords_raw.split(',') if k.strip()]
                elif isinstance(keywords_raw, list):
                    keywords = [str(k).strip().lower() for k in keywords_raw if str(k).strip()]
                else:
                    keywords = []
                
                if pattern_name and keywords:
                    # Add to custom category
                    if 'custom' not in self.keywords:
                        self.keywords['custom'] = []
                    
                    # Add all keywords for this custom pattern
                    self.keywords['custom'].extend(keywords)
                    
                    logger.info(f"🔍 Added custom PII pattern '{pattern_name}' with keywords: {keywords}")
                    
        except Exception as e:
            logger.error(f"❌ Failed to load custom PII patterns: {e}")
    
    def analyze_columns_batch(self, columns: List[Dict], max_batch_size: int = 20, llm_model: str = None) -> List[Dict]:
        """
        Analyze multiple columns for PII with intelligent batch splitting.
        
        Args:
            columns: List of dicts with 'column_name', 'data_type', and 'sample_values'
            max_batch_size: Maximum columns per LLM batch call (default: 20)
            llm_model: Optional LLM model to use for PII detection (overrides settings)
        
        Returns:
            List of PII analysis results (same format as analyze_column)
        """
        if not columns:
            return []
        
        logger.info(f"🔍 PII Detection Settings: enabled={self.enabled}, llm_assessment={self.llm_assessment}, model={llm_model or 'default'}")
        
        # If we have too many columns, split into multiple batches
        if len(columns) > max_batch_size:
            logger.info(f"🔍 Splitting {len(columns)} columns into batches of {max_batch_size}")
            all_results = []
            for i in range(0, len(columns), max_batch_size):
                batch = columns[i:i+max_batch_size]
                batch_results = self._analyze_columns_batch_internal(batch, llm_model=llm_model)
                all_results.extend(batch_results)
            return all_results
        else:
            return self._analyze_columns_batch_internal(columns, llm_model=llm_model)
    
    def _analyze_columns_batch_internal(self, columns: List[Dict], llm_model: str = None) -> List[Dict]:
        """Internal method for single batch PII analysis (no splitting)"""
        if not columns:
            return []
        
        logger.info(f"🔍 Batch PII Detection - Analyzing {len(columns)} columns")
        
        if not self.enabled:
            logger.info("ℹ️ PII detection disabled in settings")
            return [self._empty_result() for _ in columns]
        
        # First pass: Pattern-based detection for all columns (fast)
        results = []
        llm_candidates = []  # Columns that need LLM analysis
        
        for col in columns:
            column_name = col.get('column_name', '') or col.get('name', '')
            data_type = col.get('data_type', '')
            sample_values = col.get('sample_values', [])
            
            # Run pattern and keyword detection using existing methods WITH CONTEXT
            name_analysis = self._analyze_column_name(column_name.lower(), data_type, sample_values)
            data_analysis = self._analyze_sample_data(sample_values) if sample_values else {'pii_types': [], 'confidence': 0.0}
            
            combined_types = list(set(name_analysis['pii_types'] + data_analysis['pii_types']))
            confidence = max(name_analysis['confidence'], data_analysis['confidence'])
            
            if combined_types:
                # High confidence from patterns - skip LLM
                # Data patterns (sample analysis) have higher confidence than name analysis
                has_pattern_match = len(data_analysis['pii_types']) > 0
                classification = self._determine_classification(combined_types)
                result = {
                    'pii_types': combined_types,
                    'confidence': confidence,
                    'classification': classification,
                    'method': 'PATTERN' if has_pattern_match else 'KEYWORD',
                    'column_name': column_name,
                    'data_type': data_type,
                    'proposed_policy_tags': self._generate_proposed_policy_tags(combined_types, classification, self._get_governed_tags()),
                    'policy_tags': [],  # No automatic tagging
                    'risk_factors': self._assess_risk_factors(combined_types, data_type),
                    'recommendations': []
                }
                results.append(result)
            else:
                # No pattern match - candidate for LLM batch analysis
                llm_candidates.append({
                    'column_name': column_name,
                    'data_type': data_type,
                    'sample_values': sample_values,
                    'index': len(results)  # Track position in results
                })
                # Placeholder result (will be replaced by LLM result)
                results.append(None)
        
        # Second pass: Batch LLM analysis for candidates
        if llm_candidates and self.llm_assessment:
            logger.info(f"🤖 Batch LLM PII Detection for {len(llm_candidates)} columns (single API call)")
            llm_results = self._analyze_batch_with_llm(llm_candidates, llm_model=llm_model)
            
            # Insert LLM results back into results list
            for i, llm_result in enumerate(llm_results):
                if i < len(llm_candidates):
                    result_index = llm_candidates[i]['index']
                    results[result_index] = llm_result
        
        # Fill any remaining None placeholders with empty results
        for i in range(len(results)):
            if results[i] is None:
                results[i] = self._empty_result()
        
        logger.info(f"✅ Batch PII Detection complete: {len(results)} columns analyzed")
        return results
    
    def analyze_column(self, column_name: str, data_type: str, sample_values: List[str] = None) -> Dict:
        """
        Analyze a column for PII content and return classification results
        
        Args:
            column_name: Name of the column
            data_type: SQL data type (STRING, INT, etc.)
            sample_values: Optional sample values for analysis
            
        Returns:
            Dict with PII analysis results
        """
        results = {
            'column_name': column_name,
            'data_type': data_type,
            'pii_types': [],
            'confidence_score': 0.0,
            'classification': 'PUBLIC',
            'policy_tags': [],  # Empty - no automatic tagging
            'proposed_policy_tags': [],  # New - proposed tags for manual review
            'risk_factors': [],
            'recommendations': []
        }
        
        # Check if PII detection is enabled in settings
        pii_enabled = True
        llm_assessment_enabled = True
        
        if self.settings_manager:
            try:
                pii_config = self.settings_manager.get_pii_config()
                pii_enabled = pii_config.get('enabled', True)
                llm_assessment_enabled = pii_config.get('llm_assessment_enabled', True)
                
                logger.info(f"🔍 PII Detection Settings: enabled={pii_enabled}, llm_assessment={llm_assessment_enabled}")
                
            except Exception as e:
                logger.error(f"❌ Failed to get PII settings, using defaults: {e}")
        
        # Skip PII detection if disabled in settings
        if not pii_enabled:
            logger.info(f"🔒 PII Detection disabled in settings for column '{column_name}'")
            results['recommendations'].append('PII detection disabled in workspace settings')
            return results
        
        # Analyze column name for PII indicators
        name_analysis = self._analyze_column_name(column_name.lower())
        results['pii_types'].extend(name_analysis['pii_types'])
        results['confidence_score'] = max(results['confidence_score'], name_analysis['confidence'])
        
        # Analyze sample data if provided
        if sample_values:
            data_analysis = self._analyze_sample_data(sample_values)
            results['pii_types'].extend(data_analysis['pii_types'])
            results['confidence_score'] = max(results['confidence_score'], data_analysis['confidence'])
        
        # LLM-based analysis (if enabled and available)
        logger.info(f"🤖 Attempting LLM PII detection for column '{column_name}'")
        llm_analysis = self._analyze_with_llm(
            column_name=column_name,
            data_type=data_type,
            sample_values=sample_values
        )
        
        logger.info(f"🤖 LLM analysis method for '{column_name}': {llm_analysis['method']}")
        
        if llm_analysis['method'] in ['LLM'] and llm_analysis['pii_types']:
            # Merge LLM results with pattern-based results
            results['pii_types'].extend(llm_analysis['pii_types'])
            results['confidence_score'] = max(results['confidence_score'], llm_analysis['confidence'])
            
            # Use LLM classification if it's higher risk than pattern-based
            llm_classification = llm_analysis.get('classification', 'PUBLIC')
            if self._classification_rank(llm_classification) > self._classification_rank(results['classification']):
                results['classification'] = llm_classification
            
            # Add LLM method to recommendations
            results['recommendations'].append(f"LLM detected: {', '.join(llm_analysis['pii_types'])}")
            
            logger.info(f"✅ LLM enhanced PII detection for '{column_name}': {llm_analysis['pii_types']}")
        elif llm_analysis['method'] in ['DISABLED', 'UNAVAILABLE']:
            logger.info(f"ℹ️ LLM PII detection not used for '{column_name}': {llm_analysis['method']}")
        
        # Determine classification and generate PROPOSED tags
        results['classification'] = self._determine_classification(results['pii_types'])
        results['proposed_policy_tags'] = self._generate_proposed_policy_tags(results['pii_types'], results['classification'], self._get_governed_tags())
        results['risk_factors'] = self._assess_risk_factors(results['pii_types'], data_type)
        results['recommendations'] = self._generate_recommendations(results)
        
        # Keep old policy_tags field empty (no automatic tagging)
        results['policy_tags'] = []
        
        # Remove duplicates
        results['pii_types'] = list(set(results['pii_types']))
        results['policy_tags'] = list(set(results['policy_tags']))
        
        return results
    
    def _analyze_column_name(self, column_name: str, data_type: str = '', sample_values: List = None) -> Dict:
        """
        Analyze column name for PII indicators with CONTEXT AWARENESS.
        
        Uses data type and sample values to disambiguate ambiguous keywords:
        - "degree" + numeric type + numeric samples = temperature (NOT PII)
        - "degree" + string type = education (PII)
        - "name" with "store_"/"product_" prefix = business name (NOT PII)
        - "name" with "first_"/"last_" prefix = personal name (PII)
        """
        pii_types = []
        confidence = 0.0
        
        # Context-aware ambiguous keyword detection
        # These words can be PII OR non-PII depending on context
        ambiguous_keywords = {
            'degree': {
                'pii_if': lambda dt, sv: dt.upper() in ['STRING', 'VARCHAR', 'TEXT'],  # Education degree
                'not_pii_if': lambda dt, sv: dt.upper() in ['INT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC', 'BIGINT']  # Temperature
            },
            'name': {
                'pii_if': lambda dt, sv: any(prefix in column_name for prefix in ['first_', 'last_', 'full_', 'middle_', 'given_', 'surname']),
                'not_pii_if': lambda dt, sv: any(prefix in column_name for prefix in ['store_', 'product_', 'location_', 'factory_', 'warehouse_', 'business_', 'company_', 'vendor_'])
            },
            'id': {
                'pii_if': lambda dt, sv: any(prefix in column_name for prefix in ['patient_', 'employee_', 'customer_', 'user_', 'person_']),
                'not_pii_if': lambda dt, sv: any(prefix in column_name for prefix in ['product_', 'order_', 'transaction_', 'invoice_', 'shipment_'])
            }
        }
        
        # Check ambiguous keywords with context
        for keyword, rules in ambiguous_keywords.items():
            if keyword in column_name:
                # Check if context indicates NOT PII
                if rules['not_pii_if'](data_type, sample_values):
                    # Skip this keyword - it's not PII in this context
                    continue
                # Check if context indicates PII
                elif rules['pii_if'](data_type, sample_values):
                    pii_types.append(f"personal_info_{keyword}")
                    confidence = max(confidence, 0.8)
        
        # Direct keyword matching for unambiguous terms
        for category, keywords in self.keywords.items():
            for keyword in keywords:
                # Skip already-handled ambiguous keywords
                if keyword in ambiguous_keywords:
                    continue
                    
                if keyword in column_name:
                    pii_types.append(f"{category}_{keyword}")
                    confidence = max(confidence, 0.8)
        
        # Pattern-based detection for truly sensitive formats
        sensitive_patterns = {
            'email': ['email', '@'],
            'phone': ['phone_number', 'mobile_number', 'cell_phone'],  # Must be specific
            'ssn': ['ssn', 'social_security'],
            'personal_address': ['home_address', 'residential_address', 'street_address', 'mailing_address']
        }
        
        for pattern_type, indicators in sensitive_patterns.items():
            if any(indicator in column_name for indicator in indicators):
                pii_types.append(pattern_type)
                confidence = max(confidence, 0.7)
        
        return {
            'pii_types': pii_types,
            'confidence': confidence
        }
    
    def _analyze_sample_data(self, sample_values: List[str]) -> Dict:
        """Analyze sample data for PII patterns"""
        pii_types = []
        confidence = 0.0
        
        if not sample_values:
            return {'pii_types': [], 'confidence': 0.0}
        
        # Convert to strings and clean
        clean_samples = []
        for value in sample_values[:10]:  # Analyze first 10 samples
            if value is not None:
                clean_samples.append(str(value).strip())
        
        if not clean_samples:
            return {'pii_types': [], 'confidence': 0.0}
        
        # Pattern matching on sample data
        for pattern_name, pattern in self.patterns.items():
            matches = 0
            for sample in clean_samples:
                if pattern.search(sample):
                    matches += 1
            
            if matches > 0:
                match_ratio = matches / len(clean_samples)
                pii_types.append(pattern_name)
                confidence = max(confidence, match_ratio * 0.9)
        
        return {
            'pii_types': pii_types,
            'confidence': confidence
        }
    
    def _determine_classification(self, pii_types: List[str]) -> str:
        """
        Determine data classification based on detected PII types.
        
        Uses conservative logic to avoid over-classification.
        Only truly sensitive data gets PHI/PCI/PII classification.
        """
        if not pii_types:
            return 'PUBLIC'
        
        pii_str = ''.join(pii_types).lower()
        
        # PHI: Protected Health Information (HIPAA-regulated)
        phi_indicators = ['medical_phi', 'patient_id', 'mrn', 'diagnosis', 'prescription', 'health_insurance']
        if any(indicator in pii_str for indicator in phi_indicators):
            return 'PHI'
        
        # PCI: Payment Card Industry (credit card data) - PCI-DSS compliance
        pci_indicators = ['financial_pci', 'credit_card', 'cvv', 'cvv2', 'card_number', 'bank_account', 'routing']
        if any(indicator in pii_str for indicator in pci_indicators):
            return 'PCI'
        
        # FINANCIAL: Regulated financial data (SOX, GLBA) - not PCI but still sensitive
        financial_indicators = ['financial_regulated', 'account_balance', 'transaction_amount', 
                               'trade_amount', 'portfolio', 'investment', 'wire_transfer', 'ach_transfer']
        if any(indicator in pii_str for indicator in financial_indicators):
            return 'CONFIDENTIAL'  # Financial data is confidential but not PCI
        
        # PII: Personally Identifiable Information (SSN, address, biometric, education records)
        pii_indicators = ['personal_info', 'ssn', 'social_security', 'home_address', 
                         'passport', 'drivers_license', 'biometric', 'dna', 'education', 
                         'student_id', 'transcript', 'gpa']
        if any(indicator in pii_str for indicator in pii_indicators):
            return 'PII'
        
        # Sensitive IDs: Internal identifiers (employee_id, customer_id)
        # These are INTERNAL, not PII - they don't expose personal information by themselves
        id_indicators = ['sensitive_ids', 'employee_id', 'customer_id', 'user_id']
        if any(indicator in pii_str for indicator in id_indicators):
            return 'INTERNAL'
        
        # Default to INTERNAL for any other detected types
        return 'INTERNAL' if pii_types else 'PUBLIC'
    
    def _generate_proposed_policy_tags(self, pii_types: List[str], classification: str, governed_tags: Dict = None) -> List[Dict]:
        """Generate PROPOSED policy tags based on PII analysis for manual review (KEY.VALUE format)
        
        Args:
            pii_types: List of detected PII types
            classification: Overall classification (PII, PHI, etc.)
            governed_tags: Dict of governed tags {key: {allowed_values: [...]}} - if None, will not validate
            
        Returns:
            List of proposed tags (empty if conflicts with governed tags)
        """
        proposed_tags = []
        tagging_warnings = []
        
        # Get custom tag mappings from settings
        tag_mapping = self._get_tag_mappings()
        
        # Add classification tag proposal
        if classification != 'PUBLIC':
            # Check if there's a custom mapping for this classification
            classification_mapping = tag_mapping.get('classification_tags', {}).get(classification, {})
            
            # Skip if marked as invalid or not configured
            if isinstance(classification_mapping, dict):
                if classification_mapping.get('invalid_default'):
                    logger.debug(f"⏭️ Skipping classification tag for {classification} - marked as needing configuration")
                    # Don't return early, continue to process PII type tags
                    classification_mapping = {}
            
            # Handle both dict format (new) and string format (old/backward compatibility)
            if isinstance(classification_mapping, dict) and classification_mapping:
                classification_tag = classification_mapping.get('tag', f'classification.{classification}')
                tag_key = classification_mapping.get('key', 'classification')
                tag_value = classification_mapping.get('value', classification)
                
                # Skip if key or value is empty (not configured)
                if not tag_key or not tag_value:
                    logger.debug(f"⏭️ Skipping classification tag for {classification} - not configured")
                    classification_mapping = {}
            elif isinstance(classification_mapping, str):
                # Old string format
                classification_tag = classification_mapping
                tag_key = 'classification'
                tag_value = classification
            else:
                # No mapping available, skip
                classification_mapping = {}
            
            # Only validate and add if we have a valid mapping
            if classification_mapping:
                # Validate against governed tags if provided
                is_valid = True
                if governed_tags and tag_key in governed_tags:
                    allowed_values = governed_tags[tag_key].get('allowed_values', [])
                    if allowed_values and tag_value not in allowed_values:
                        is_valid = False
                        logger.info(f"⚠️ Tag proposal blocked: '{tag_key}.{tag_value}' - '{tag_value}' not in allowed values {allowed_values} for governed tag '{tag_key}'")
                        tagging_warnings.append(
                            f"Cannot propose '{tag_key}.{tag_value}': '{tag_value}' not in allowed values for governed tag '{tag_key}'. "
                            f"Please configure tag mapping in Settings or update governed tag."
                        )
                
                if is_valid:
                    proposed_tags.append({
                        'tag': classification_tag,  # Full tag for backward compatibility
                        'key': tag_key,  # Separate key for governed tag compatibility
                        'value': tag_value,  # Separate value for governed tag compatibility
                        'reason': f'Data classified as {classification} based on content analysis',
                        'confidence': 'high',
                        'auto_apply': False  # Always requires manual approval
                    })
                    logger.debug(f"✅ Proposed classification tag: {tag_key}.{tag_value}")
        
        # Add specific PII tag proposals using custom mappings (KEY.VALUE format)
        for pii_type in pii_types:
            for pattern_key, tag_info in tag_mapping.get('pii_type_tags', {}).items():
                if pattern_key in pii_type.lower():
                    # Skip patterns marked as invalid (need user configuration in Settings)
                    if tag_info.get('invalid_default'):
                        logger.debug(f"⏭️ Skipping tag proposal for {pattern_key} - marked as needing configuration")
                        continue
                    
                    # Use key/value structure if available, fallback to parsing 'tag' field
                    tag_key = tag_info.get('key', 'PII')
                    tag_value = tag_info.get('value', pattern_key.upper())
                    
                    # Skip if key or value is empty (not configured)
                    if not tag_key or not tag_value:
                        logger.debug(f"⏭️ Skipping tag proposal for {pattern_key} - not configured")
                        continue
                    
                    # Validate against governed tags if provided
                    is_valid = True
                    if governed_tags and tag_key in governed_tags:
                        allowed_values = governed_tags[tag_key].get('allowed_values', [])
                        if allowed_values and tag_value not in allowed_values:
                            is_valid = False
                            logger.info(f"⚠️ Tag proposal blocked: '{tag_key}.{tag_value}' for {pattern_key} - '{tag_value}' not in allowed values {allowed_values} for governed tag '{tag_key}'")
                            tagging_warnings.append(
                                f"Cannot propose '{tag_key}.{tag_value}' for {pattern_key}: '{tag_value}' not in allowed values for governed tag '{tag_key}'. "
                                f"Please configure tag mapping in Settings or update governed tag."
                            )
                    
                    if is_valid:
                        # Build full tag for backward compatibility
                        full_tag = f'{tag_key}.{tag_value}'
                        
                        proposed_tags.append({
                            'tag': full_tag,  # Full tag for backward compatibility (e.g., "PII.SSN")
                            'key': tag_key,   # Separate key for governed tag compatibility
                            'value': tag_value,  # Separate value for governed tag compatibility
                            'reason': tag_info.get('reason', f'{pattern_key} patterns detected'),
                            'confidence': 'high' if pattern_key in ['ssn', 'credit_card', 'email'] else 'medium',
                            'auto_apply': False,  # Always requires manual approval
                            'pii_type': pii_type
                        })
                        logger.debug(f"✅ Proposed PII tag: {tag_key}.{tag_value} for {pattern_key}")
        
        # If we have warnings but no tags, return them as metadata
        if tagging_warnings and not proposed_tags:
            logger.info(f"⚠️ Tag proposal blocked due to governed tag conflicts: {'; '.join(tagging_warnings)}")
        
        return proposed_tags
    
    def _get_governed_tags(self) -> Dict:
        """Fetch governed tags from Unity Catalog (with caching)"""
        if self._governed_tags_cache is not None:
            logger.debug(f"🔄 Using cached governed tags ({len(self._governed_tags_cache)} tags)")
            return self._governed_tags_cache
        
        governed_tags = {}
        
        # Use unity_service.get_governed_tags() if available
        if self.unity_service and hasattr(self.unity_service, 'get_governed_tags'):
            try:
                governed_tags = self.unity_service.get_governed_tags()
                logger.info(f"✅ Fetched {len(governed_tags)} governed tags from Unity Catalog for PII validation")
                if governed_tags:
                    logger.debug(f"   Governed tags: {', '.join(governed_tags.keys())}")
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch governed tags: {e}")
        else:
            logger.warning("⚠️ Unity service not available for governed tags fetch - tag validation disabled")
        
        # Cache the result (even if empty)
        self._governed_tags_cache = governed_tags
        return governed_tags
    
    def _get_tag_mappings(self) -> Dict:
        """
        Get tag mappings from settings or generate comprehensive defaults from all detection patterns.
        CRITICAL: Validates all mappings against Unity Catalog's governed tags to filter out invalid ones.
        This ensures invalid mappings are never used during generation.
        
        PERFORMANCE: 
        - In-memory cache for current instance (fast)
        - Persistent cache in settings for cross-session reuse (avoids repeated validation)
        """
        
        # Return in-memory cache if available (fastest path)
        if self._tag_mappings_cache is not None:
            return self._tag_mappings_cache
        
        # Try to load from persistent cache in settings (faster than re-validation)
        if self.settings_manager:
            try:
                pii_config = self.settings_manager.get_pii_config()
                cached_validated_mappings = pii_config.get('validated_tag_mappings_cache')
                if cached_validated_mappings:
                    logger.info("✅ Using persistent tag mappings cache (from previous session)")
                    self._tag_mappings_cache = cached_validated_mappings
                    return cached_validated_mappings
            except Exception as e:
                logger.debug(f"No persistent tag mappings cache available: {e}")
        
        logger.info("🔄 Loading and validating tag mappings (first time - will be cached)...")
        
        # Classification tags (static) - KEY/VALUE format for UI compatibility
        default_classification_tags = {
            'PII': {'key': 'classification', 'value': 'PII', 'tag': 'classification.PII'},
            'PHI': {'key': 'classification', 'value': 'PHI', 'tag': 'classification.PHI'},
            'PCI': {'key': 'classification', 'value': 'PCI', 'tag': 'classification.PCI'},
            'CONFIDENTIAL': {'key': 'classification', 'value': 'Confidential', 'tag': 'classification.Confidential'},
            'SENSITIVE': {'key': 'classification', 'value': 'Sensitive', 'tag': 'classification.Sensitive'}
        }
        
        # DYNAMIC: Generate PII type tags from ALL keywords and patterns
        default_pii_type_tags = self._generate_comprehensive_tag_mappings()
        
        default_mappings = {
            'classification_tags': default_classification_tags,
            'pii_type_tags': default_pii_type_tags
        }
        
        # Try to get custom mappings from settings
        if self.settings_manager:
            try:
                pii_config = self.settings_manager.get_pii_config()
                custom_mappings = pii_config.get('tag_mappings', {})
                if custom_mappings:
                    # Merge custom with defaults (custom overrides defaults)
                    default_mappings['classification_tags'].update(custom_mappings.get('classification_tags', {}))
                    default_mappings['pii_type_tags'].update(custom_mappings.get('pii_type_tags', {}))
            except Exception as e:
                logger.debug(f"Could not load custom tag mappings: {e}")
        
        # CRITICAL VALIDATION: Mark invalid tag mappings against governed tags
        # Invalid mappings get empty key/value so users can configure them in UI
        governed_tags = self._get_governed_tags()
        if governed_tags:
            validated_mappings = self._validate_mappings_against_governed(default_mappings, governed_tags)
            invalid_count = sum(
                1 for tag_info in validated_mappings.get('pii_type_tags', {}).values()
                if isinstance(tag_info, dict) and tag_info.get('invalid_default')
            )
            if invalid_count > 0:
                logger.info(f"⚠️ Marked {invalid_count} tag mappings as needing configuration (governed tag conflicts)")
            
            # Cache in memory for current instance
            self._tag_mappings_cache = validated_mappings
            
            # Persist to settings for cross-session reuse (background, non-blocking)
            if self.settings_manager:
                try:
                    import threading
                    def _save_cache():
                        try:
                            self.settings_manager.update_pii_config({
                                'validated_tag_mappings_cache': validated_mappings
                            })
                            logger.debug(f"💾 Persisted validated tag mappings to settings cache")
                        except Exception as e:
                            logger.debug(f"Could not persist tag mappings cache: {e}")
                    
                    # Save in background thread to not block generation
                    threading.Thread(target=_save_cache, daemon=True).start()
                except Exception as e:
                    logger.debug(f"Could not start cache persistence thread: {e}")
            
            logger.info(f"✅ Cached {len(validated_mappings.get('pii_type_tags', {}))} validated PII tag mappings (in-memory + persistent)")
            return validated_mappings
        
        # No governed tags to validate against - cache the defaults
        self._tag_mappings_cache = default_mappings
        logger.debug(f"✅ Cached {len(default_mappings.get('pii_type_tags', {}))} tag mappings (no validation)")
        return default_mappings
    
    def _validate_mappings_against_governed(self, mappings: dict, governed_tags: dict) -> dict:
        """
        Validate tag mappings against Unity Catalog's governed tags.
        Mark mappings where key is governed but value is not in allowed_values with empty key/value.
        This allows UI to show ALL patterns and let users configure valid tags.
        
        Returns: Mappings dict with invalid entries marked with empty key/value (not removed)
        """
        validated_mappings = {
            'classification_tags': {},
            'pii_type_tags': {}
        }
        
        # Validate classification tags
        for classification, tag_info in mappings.get('classification_tags', {}).items():
            if not tag_info or not isinstance(tag_info, dict):
                continue
            
            tag_key = tag_info.get('key')
            tag_value = tag_info.get('value')
            
            # Check if this tag is governed
            if tag_key in governed_tags:
                allowed_values = governed_tags[tag_key].get('allowed_values', [])
                # If governed and value not in allowed list, mark as needing configuration
                if allowed_values and tag_value not in allowed_values:
                    logger.debug(f"Marking invalid classification mapping for user config: {classification} -> {tag_key}.{tag_value}")
                    validated_mappings['classification_tags'][classification] = {
                        **tag_info,
                        'key': '',
                        'value': '',
                        'tag': '',
                        'invalid_default': True,
                        'reason': f"❌ Default tag '{tag_key}.{tag_value}' conflicts with governed tag. Please configure a valid tag."
                    }
                    continue
            
            # Valid mapping, keep it as-is
            validated_mappings['classification_tags'][classification] = tag_info
        
        # Validate PII type tags
        for pattern, tag_info in mappings.get('pii_type_tags', {}).items():
            if not tag_info or not isinstance(tag_info, dict):
                continue
            
            tag_key = tag_info.get('key')
            tag_value = tag_info.get('value')
            
            # Check if this tag is governed
            if tag_key in governed_tags:
                allowed_values = governed_tags[tag_key].get('allowed_values', [])
                # If governed and value not in allowed list, mark as needing configuration
                if allowed_values and tag_value not in allowed_values:
                    logger.debug(f"Marking invalid PII mapping for user config: {pattern} -> {tag_key}.{tag_value}")
                    validated_mappings['pii_type_tags'][pattern] = {
                        **tag_info,
                        'key': '',
                        'value': '',
                        'tag': '',
                        'invalid_default': True,
                        'reason': f"❌ Default tag '{tag_key}.{tag_value}' conflicts with governed tag. Please configure a valid tag."
                    }
                    continue
            
            # Valid mapping, keep it as-is
            validated_mappings['pii_type_tags'][pattern] = tag_info
        
        return validated_mappings
    
    def _count_removed_mappings(self, original: dict, validated: dict) -> int:
        """Count how many mappings were removed during validation"""
        original_count = len(original.get('pii_type_tags', {})) + len(original.get('classification_tags', {}))
        validated_count = len(validated.get('pii_type_tags', {})) + len(validated.get('classification_tags', {}))
        return original_count - validated_count
    
    def _generate_comprehensive_tag_mappings(self) -> Dict:
        """Generate tag mappings for ALL detection patterns dynamically"""
        mappings = {}
        
        # Map from keyword categories to their framework and tag prefix
        category_framework_map = {
            'personal_info': ('PII', 'GDPR/CCPA'),
            'financial_pci': ('PCI', 'PCI-DSS'),
            'financial_regulated': ('Financial', 'SOX/GLBA'),
            'medical_phi': ('PHI', 'HIPAA'),
            'education': ('PII', 'FERPA'),
            'sensitive_ids': ('PII', 'Internal Identifiers'),
            'biometric': ('Biometric', 'Biometric Data'),
            'custom': ('Custom', 'Custom Patterns')
        }
        
        # Specific tag mappings for common patterns (with KEY/VALUE split for governed tag compatibility)
        specific_mappings = {
            # PII (GDPR/CCPA)
            'ssn': {'key': 'PII', 'value': 'SSN', 'reason': 'Social Security Number patterns detected', 'framework': 'GDPR/CCPA'},
            'social_security': {'key': 'PII', 'value': 'SSN', 'reason': 'Social Security Number patterns detected', 'framework': 'GDPR/CCPA'},
            'email': {'key': 'PII', 'value': 'Email', 'reason': 'Email address patterns detected', 'framework': 'GDPR/CCPA'},
            'phone': {'key': 'PII', 'value': 'Phone', 'reason': 'Phone number patterns detected', 'framework': 'GDPR/CCPA'},
            'name': {'key': 'PII', 'value': 'Name', 'reason': 'Name patterns detected', 'framework': 'GDPR/CCPA'},
            'surname': {'key': 'PII', 'value': 'Name', 'reason': 'Surname/last name patterns detected', 'framework': 'GDPR/CCPA'},
            'address': {'key': 'PII', 'value': 'Address', 'reason': 'Address patterns detected', 'framework': 'GDPR/CCPA'},
            'date_of_birth': {'key': 'PII', 'value': 'DateOfBirth', 'reason': 'Date of birth patterns detected', 'framework': 'GDPR/CCPA'},
            'postal': {'key': 'PII', 'value': 'Postal', 'reason': 'Postal/ZIP code patterns detected', 'framework': 'GDPR/CCPA'},
            'zip': {'key': 'PII', 'value': 'Postal', 'reason': 'ZIP code patterns detected', 'framework': 'GDPR/CCPA'},
            
            # PCI (Payment Card)
            'credit_card': {'key': 'PCI', 'value': 'CreditCard', 'reason': 'Credit card number patterns detected', 'framework': 'PCI-DSS'},
            'cvv': {'key': 'PCI', 'value': 'CVV', 'reason': 'Card security code patterns detected', 'framework': 'PCI-DSS'},
            'card_number': {'key': 'PCI', 'value': 'CardNumber', 'reason': 'Card number patterns detected', 'framework': 'PCI-DSS'},
            'pan': {'key': 'PCI', 'value': 'PAN', 'reason': 'Primary Account Number patterns detected', 'framework': 'PCI-DSS'},
            'expiry_date': {'key': 'PCI', 'value': 'ExpiryDate', 'reason': 'Card expiry date patterns detected', 'framework': 'PCI-DSS'},
            
            # PHI (HIPAA)
            'medical': {'key': 'PHI', 'value': 'Medical', 'reason': 'Medical record patterns detected', 'framework': 'HIPAA'},
            'patient': {'key': 'PHI', 'value': 'Patient', 'reason': 'Patient information patterns detected', 'framework': 'HIPAA'},
            'mrn': {'key': 'PHI', 'value': 'MRN', 'reason': 'Medical Record Number patterns detected', 'framework': 'HIPAA'},
            'patient_id': {'key': 'PHI', 'value': 'PatientID', 'reason': 'Patient ID patterns detected', 'framework': 'HIPAA'},
            'diagnosis': {'key': 'PHI', 'value': 'Diagnosis', 'reason': 'Diagnosis patterns detected', 'framework': 'HIPAA'},
            'prescription': {'key': 'PHI', 'value': 'Prescription', 'reason': 'Prescription patterns detected', 'framework': 'HIPAA'},
            'health_insurance': {'key': 'PHI', 'value': 'Insurance', 'reason': 'Health insurance patterns detected', 'framework': 'HIPAA'},
            
            # Financial (SOX/GLBA)
            'account_number': {'key': 'Financial', 'value': 'AccountNumber', 'reason': 'Account number patterns detected', 'framework': 'SOX/GLBA'},
            'routing_number': {'key': 'Financial', 'value': 'RoutingNumber', 'reason': 'Routing number patterns detected', 'framework': 'SOX/GLBA'},
            'aba': {'key': 'Financial', 'value': 'ABA', 'reason': 'ABA number patterns detected', 'framework': 'SOX/GLBA'},
            'iban': {'key': 'Financial', 'value': 'IBAN', 'reason': 'IBAN patterns detected', 'framework': 'SOX/GLBA'},
            'swift': {'key': 'Financial', 'value': 'SWIFT', 'reason': 'SWIFT code patterns detected', 'framework': 'SOX/GLBA'},
            'salary': {'key': 'Financial', 'value': 'Salary', 'reason': 'Salary information detected', 'framework': 'SOX/GLBA'},
            'compensation': {'key': 'Financial', 'value': 'Compensation', 'reason': 'Compensation information detected', 'framework': 'SOX/GLBA'},
            
            # Biometric
            'fingerprint': {'key': 'Biometric', 'value': 'Fingerprint', 'reason': 'Fingerprint data detected', 'framework': 'Biometric'},
            'facial': {'key': 'Biometric', 'value': 'Facial', 'reason': 'Facial recognition data detected', 'framework': 'Biometric'},
            'iris': {'key': 'Biometric', 'value': 'Iris', 'reason': 'Iris scan data detected', 'framework': 'Biometric'},
            'retina': {'key': 'Biometric', 'value': 'Retina', 'reason': 'Retina scan data detected', 'framework': 'Biometric'},
            'voiceprint': {'key': 'Biometric', 'value': 'Voiceprint', 'reason': 'Voiceprint data detected', 'framework': 'Biometric'}
        }
        
        # Start with specific mappings
        mappings.update(specific_mappings)
        
        # Add any remaining keywords from self.keywords that aren't already mapped
        for category, keywords in self.keywords.items():
            prefix, framework = category_framework_map.get(category, ('PII', 'General'))
            
            for keyword in keywords:
                # Normalize keyword for matching
                normalized_key = keyword.lower().replace(' ', '_')
                
                # Skip if already specifically mapped
                if normalized_key in mappings:
                    continue
                
                # Generate a tag for this keyword (KEY/VALUE split)
                # Convert snake_case to TitleCase for value
                tag_value = ''.join(word.capitalize() for word in normalized_key.split('_'))
                
                mappings[normalized_key] = {
                    'key': prefix,
                    'value': tag_value,
                    'reason': f'{keyword.replace("_", " ").title()} patterns detected',
                    'framework': framework
                }
        
        return mappings
    
    def _assess_risk_factors(self, pii_types: List[str], data_type: str) -> List[str]:
        """Assess risk factors for the detected PII"""
        risks = []
        
        if pii_types:
            risks.append('Contains personally identifiable information')
        
        if 'medical' in ''.join(pii_types).lower():
            risks.append('Subject to HIPAA regulations')
        
        if 'financial' in ''.join(pii_types).lower():
            risks.append('Subject to PCI DSS requirements')
        
        if data_type == 'STRING' and any('id' in pii for pii in pii_types):
            risks.append('Potential unique identifier')
        
        return risks
    
    def _generate_recommendations(self, analysis: Dict) -> List[str]:
        """Generate recommendations based on PII analysis"""
        recommendations = []
        
        classification = analysis.get('classification', 'PUBLIC')
        pii_types = analysis.get('pii_types', [])
        
        if classification in ['PII', 'PHI', 'PCI']:
            recommendations.append('Consider data masking or tokenization')
            recommendations.append('Implement access controls and audit logging')
            recommendations.append('Ensure compliance with relevant regulations')
        
        if 'credit_card' in ''.join(pii_types).lower():
            recommendations.append('Apply PCI DSS security controls')
        
        if 'medical' in ''.join(pii_types).lower():
            recommendations.append('Implement HIPAA safeguards')
        
        if analysis.get('confidence_score', 0) < 0.5:
            recommendations.append('Manual review recommended - low confidence detection')
        
        return recommendations

    def analyze_table(self, table_info: Dict) -> Dict:
        """
        Analyze entire table for PII content
        
        Args:
            table_info: Dictionary with table metadata and sample data
            
        Returns:
            Dict with table-level PII analysis
        """
        table_name = table_info.get('name', '')
        columns = table_info.get('columns', [])
        
        results = {
            'table_name': table_name,
            'total_columns': len(columns),
            'pii_columns': 0,
            'highest_classification': 'PUBLIC',
            'recommended_tags': [],
            'risk_assessment': 'LOW',
            'column_analysis': []
        }
        
        # Use BATCH analysis for all columns at once (100x faster than one-by-one!)
        logger.info(f"🚀 Analyzing table '{table_name}' with {len(columns)} columns using BATCH PII detection")
        column_analyses = self.analyze_columns_batch(columns, max_batch_size=20)
        
        # Aggregate results
        for column_analysis in column_analyses:
            results['column_analysis'].append(column_analysis)
            
            # Track PII columns
            if column_analysis['classification'] != 'PUBLIC':
                results['pii_columns'] += 1
            
            # Update highest classification
            if self._classification_rank(column_analysis['classification']) > self._classification_rank(results['highest_classification']):
                results['highest_classification'] = column_analysis['classification']
            
            # Collect unique tags
            results['recommended_tags'].extend(column_analysis.get('policy_tags', []))
        
        # Remove duplicate tags
        results['recommended_tags'] = list(set(results['recommended_tags']))
        
        # Determine risk assessment
        pii_ratio = results['pii_columns'] / results['total_columns'] if results['total_columns'] > 0 else 0
        if pii_ratio > 0.5 or results['highest_classification'] in ['PHI', 'PCI']:
            results['risk_assessment'] = 'HIGH'
        elif pii_ratio > 0.2 or results['highest_classification'] == 'PII':
            results['risk_assessment'] = 'MEDIUM'
        else:
            results['risk_assessment'] = 'LOW'
        
        return results
    
    def _analyze_with_llm(self, column_name: str, data_type: str, sample_values: List[str] = None, table_context: Dict = None) -> Dict:
        """
        Use LLM to analyze column for REGULATED/SENSITIVE DATA across ALL frameworks.
        
        Detects:
        - PII: Personal information (GDPR, CCPA)
        - PHI: Protected Health Information (HIPAA)
        - PCI: Payment Card data (PCI-DSS)
        - Financial: Banking/trading data (SOX, GLBA)
        - Other regulated data requiring compliance controls
        """
        try:
            if not self.llm_service:
                logger.warning("🤖 LLM service not available for regulatory data detection")
                return {'pii_types': [], 'confidence': 0.0, 'method': 'UNAVAILABLE'}
            
            # Check if LLM detection is enabled
            llm_enabled = True
            if self.settings_manager:
                try:
                    pii_config = self.settings_manager.get_pii_config()
                    llm_enabled = pii_config.get('llm_detection_enabled', True)
                except Exception as e:
                    logger.warning(f"Failed to check LLM settings: {e}")
            
            if not llm_enabled:
                logger.info(f"🤖 LLM regulatory data detection disabled in settings for column '{column_name}'")
                return {'pii_types': [], 'confidence': 0.0, 'method': 'DISABLED'}
            
            # Build context for LLM
            table_info = ""
            if table_context:
                table_info = f"Table: {table_context.get('schema_name', '')}.{table_context.get('table_name', '')}\n"
            
            sample_info = ""
            if sample_values and len(sample_values) > 0:
                # Show first few sample values (anonymized)
                samples = sample_values[:3]
                sample_info = f"Sample values: {', '.join(str(s)[:20] + '...' if len(str(s)) > 20 else str(s) for s in samples)}\n"
            
            prompt = f"""Analyze this database column for REGULATED/SENSITIVE DATA requiring compliance controls:

Column Name: {column_name}
Data Type: {data_type}
{table_info}{sample_info}

REGULATORY FRAMEWORKS TO CONSIDER:
1. PII (GDPR/CCPA): Personal identifiers (name, email, SSN, address, phone, DOB, passport, license)
2. PHI (HIPAA): Health information (patient records, diagnoses, prescriptions, medical IDs, insurance)
3. PCI (PCI-DSS): Payment data (credit cards, bank accounts, CVV, routing numbers, transaction details)
4. FINANCIAL (SOX/GLBA): Banking/trading data (account numbers, balances, trades, investment records)
5. BIOMETRIC: Fingerprints, facial recognition, DNA, retina scans
6. CONFIDENTIAL: Trade secrets, proprietary algorithms, competitive intelligence

Consider:
- Column name patterns and business context
- Data type appropriateness for sensitive data
- Sample values and data patterns
- Regulatory exposure if this data were breached

Respond with ONLY this format:
PII_TYPES: [comma-separated list like "email", "ssn", "credit_card", "patient_id", "diagnosis", "account_balance" or "none"]
CONFIDENCE: [number 0.0-1.0]
CLASSIFICATION: [PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED, PII, PHI, PCI]"""

            logger.info(f"🔒 LLM Regulatory Data Detection - Analyzing column '{column_name}' (type: {data_type})")
            logger.info(f"🔒 LLM Prompt (first 200 chars):\n{prompt[:200]}...")  # Log first 200 chars of prompt
            
            # NOTE: This is legacy code - modern code uses analyze_columns_batch() with explicit model parameter
            # Get fallback model for backwards compatibility
            fallback_model = self._get_pii_llm_model()
            
            # Skip LLM detection if model is disabled
            if not fallback_model:
                logger.info(f"⏭️  Skipping LLM PII detection for {column_name} - model is disabled")
                return {'pii_types': [], 'confidence': 0.0, 'method': 'DISABLED'}
            
            response = self.llm_service._call_databricks_llm(
                prompt=prompt,
                max_tokens=100,
                model=fallback_model,
                temperature=0.1,
                style="concise"
            )
            
            if not response:
                logger.warning(f"🤖 Empty LLM response for column '{column_name}'")
                return {'pii_types': [], 'confidence': 0.0, 'method': 'FAILED'}
            
            # Parse LLM response
            pii_types = []
            confidence = 0.0
            classification = 'PUBLIC'
            
            import re
            
            # Extract PII types
            pii_match = re.search(r'PII_TYPES:\s*\[(.*?)\]', response, re.IGNORECASE)
            if pii_match:
                pii_text = pii_match.group(1).strip()
                if pii_text.lower() != 'none' and pii_text:
                    pii_types = [t.strip().strip('"\'') for t in pii_text.split(',') if t.strip()]
            
            # Extract confidence
            conf_match = re.search(r'CONFIDENCE:\s*([0-9.]+)', response, re.IGNORECASE)
            if conf_match:
                confidence = min(1.0, max(0.0, float(conf_match.group(1))))
            
            # Extract classification
            class_match = re.search(r'CLASSIFICATION:\s*(\w+)', response, re.IGNORECASE)
            if class_match:
                classification = class_match.group(1).upper()
            
            logger.info(f"🤖 LLM PII Detection Result for '{column_name}':")
            logger.info(f"   - PII Types: {pii_types if pii_types else 'None'}")
            logger.info(f"   - Confidence: {confidence:.2f}")
            logger.info(f"   - Classification: {classification}")
            logger.info(f"   - Raw Response: {response[:150]}...")  # Log first 150 chars
            
            return {
                'pii_types': pii_types,
                'confidence': confidence,
                'classification': classification,
                'method': 'LLM',
                'raw_response': response
            }
            
        except Exception as e:
            logger.error(f"🤖 LLM PII detection failed for column '{column_name}': {e}")
            return {'pii_types': [], 'confidence': 0.0, 'method': 'ERROR'}
    
    def _analyze_batch_with_llm(self, columns: List[Dict], llm_model: str = None) -> List[Dict]:
        """
        Analyze multiple columns with LLM for REGULATED/SENSITIVE DATA in a single batch call.
        
        Detects ALL regulatory frameworks: PII, PHI, PCI, Financial, Biometric, Confidential.
        Much more efficient than one-by-one analysis.
        
        Args:
            columns: List of column dicts with name, type, and sample values
            llm_model: Optional LLM model to use (overrides settings/default)
        """
        if not columns:
            return []
        
        try:
            if not self.llm_service:
                logger.warning("🤖 LLM service not available for batch regulatory data detection")
                return [{'pii_types': [], 'confidence': 0.0, 'method': 'UNAVAILABLE', 'proposed_policy_tags': []} for _ in columns]
            
            # Check if LLM detection is enabled
            llm_enabled = True
            if self.settings_manager:
                try:
                    pii_config = self.settings_manager.get_pii_config()
                    llm_enabled = pii_config.get('llm_detection_enabled', True)
                except Exception as e:
                    logger.warning(f"Failed to check LLM settings: {e}")
            
            if not llm_enabled:
                logger.info(f"🤖 LLM regulatory data detection disabled in settings")
                return [{'pii_types': [], 'confidence': 0.0, 'method': 'DISABLED', 'proposed_policy_tags': []} for _ in columns]
            
            # Build batch prompt
            prompt = f"""Analyze these {len(columns)} database columns for REGULATED/SENSITIVE DATA requiring compliance controls:

"""
            
            for i, col in enumerate(columns, 1):
                column_name = col['column_name']
                data_type = col['data_type']
                sample_values = col.get('sample_values', [])
                
                sample_info = ""
                if sample_values:
                    samples = sample_values[:3]
                    sample_str = ', '.join(str(s)[:20] + '...' if len(str(s)) > 20 else str(s) for s in samples)
                    sample_info = f" | Sample values: {sample_str}"
                
                prompt += f"{i}. {column_name} ({data_type}){sample_info}\n"
            
            prompt += f"""
REGULATORY FRAMEWORKS:
- PII (GDPR/CCPA): Personal identifiers (name, email, SSN, phone, address, DOB)
- PHI (HIPAA): Health data (diagnoses, prescriptions, patient IDs, medical records)
- PCI (PCI-DSS): Payment data (credit cards, bank accounts, CVV, transactions)
- FINANCIAL (SOX/GLBA): Banking/trading data (balances, account numbers, trades)
- BIOMETRIC: Fingerprints, facial recognition, DNA
- CONFIDENTIAL: Trade secrets, proprietary data

For EACH column, classify based on regulatory exposure if breached. Consider:
- Column name patterns and business context
- Sample values and data patterns
- Data type appropriateness for sensitive data

Respond with EXACTLY {len(columns)} lines in this format (one per column):
PII_TYPES: [types like "email", "ssn", "credit_card", "diagnosis", "account_balance" or "none"] | CONFIDENCE: [0.0-1.0] | CLASSIFICATION: [PUBLIC/INTERNAL/PII/PHI/PCI]
"""
            
            logger.info(f"🔒 Batch LLM Regulatory Detection Prompt (first 300 chars):\n{prompt[:300]}...")
            logger.info(f"Calling LLM with prompt length: {len(prompt)}")
            
            # Use provided model or get from settings
            model_to_use = llm_model if llm_model else self._get_pii_llm_model()
            
            # Skip LLM detection if model is disabled
            if not model_to_use:
                logger.info(f"⏭️  Skipping batch LLM PII detection for {len(columns)} columns - model is disabled")
                return [{'column_name': col.get('column_name', ''), 'pii_types': [], 'confidence': 0.0, 'method': 'SKIPPED'} for col in columns]
            
            logger.info(f"🤖 Using PII model: {model_to_use}")
            
            response = self.llm_service._call_databricks_llm(
                prompt=prompt,
                max_tokens=1000,  # More tokens for batch response
                model=model_to_use,
                temperature=0.1,
                style="concise"
            )
            
            if not response:
                logger.warning("❌ Empty response from LLM for batch PII detection")
                return [self._empty_result() for _ in columns]
            
            logger.info(f"🤖 Batch LLM Response (first 300 chars):\n{str(response)[:300]}...")
            
            # Parse batch response
            results = self._parse_batch_llm_response(response, columns)
            
            logger.info(f"✅ Batch LLM PII Detection complete: {len(results)} columns analyzed")
            return results
            
        except Exception as e:
            logger.error(f"❌ Batch LLM PII detection error: {e}")
            return [self._empty_result() for _ in columns]
    
    def _parse_batch_llm_response(self, response: Any, columns: List[Dict]) -> List[Dict]:
        """Parse LLM batch response into individual column results"""
        try:
            # Extract text content from response
            response_text = ""
            if isinstance(response, dict):
                if 'choices' in response and len(response['choices']) > 0:
                    response_text = response['choices'][0].get('message', {}).get('content', '')
                elif 'content' in response:
                    response_text = response['content']
                else:
                    response_text = str(response)
            else:
                response_text = str(response)
            
            if not response_text:
                return [self._empty_result() for _ in columns]
            
            # Extract only lines that contain PII_TYPES (the actual analysis results)
            lines = [line.strip() for line in response_text.strip().split('\n') if line.strip()]
            pii_lines = [line for line in lines if 'PII_TYPES:' in line]
            
            logger.debug(f"📊 Extracted {len(pii_lines)} PII analysis lines from {len(lines)} total lines")
            
            results = []
            for i, col in enumerate(columns):
                if i < len(pii_lines):
                    line = pii_lines[i]
                    result = self._parse_single_pii_response(line)
                    result['column_name'] = col['column_name']
                    result['data_type'] = col.get('data_type', '')
                    result['method'] = 'LLM'
                    result['proposed_policy_tags'] = self._generate_proposed_policy_tags(result['pii_types'], result['classification'], self._get_governed_tags())
                    result['policy_tags'] = []  # No automatic tagging
                    result['risk_factors'] = self._assess_risk_factors(result['pii_types'], result['data_type'])
                    result['recommendations'] = []
                    
                    logger.info(f"✅ Parsed PII result for {col['column_name']}: classification={result['classification']}, pii_types={result['pii_types']}, proposed_tags={len(result['proposed_policy_tags'])}")
                    results.append(result)
                else:
                    # Not enough PII lines in response
                    logger.warning(f"⚠️ No PII line found for column {col['column_name']} (index {i})")
                    results.append(self._empty_result())
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to parse batch LLM response: {e}")
            return [self._empty_result() for _ in columns]
    
    def _parse_single_pii_response(self, response_line: str) -> Dict:
        """Parse a single line of PII response"""
        try:
            import re
            pii_types = []
            confidence = 0.0
            classification = 'PUBLIC'
            
            # Extract PII_TYPES
            if 'PII_TYPES:' in response_line:
                types_part = response_line.split('PII_TYPES:')[1].split('|')[0].strip()
                types_part = types_part.replace('[', '').replace(']', '').replace("'", "").replace('"', '')
                if 'none' not in types_part.lower():
                    pii_types = [t.strip() for t in types_part.split(',') if t.strip()]
            
            # Extract CONFIDENCE
            if 'CONFIDENCE:' in response_line:
                conf_part = response_line.split('CONFIDENCE:')[1].split('|')[0].strip()
                conf_part = conf_part.replace('[', '').replace(']', '')
                try:
                    confidence = float(conf_part)
                except:
                    confidence = 0.8 if pii_types else 0.0
            
            # Extract CLASSIFICATION
            if 'CLASSIFICATION:' in response_line:
                class_part = response_line.split('CLASSIFICATION:')[1].strip()
                class_part = class_part.replace('[', '').replace(']', '').replace("'", "").replace('"', '').strip()
                if class_part in ['PUBLIC', 'PII', 'PHI', 'PCI']:
                    classification = class_part
            
            return {
                'pii_types': pii_types,
                'confidence': confidence,
                'classification': classification
            }
            
        except Exception as e:
            logger.warning(f"Failed to parse PII response line: {e}")
            return {'pii_types': [], 'confidence': 0.0, 'classification': 'PUBLIC'}
    
    def _empty_result(self) -> Dict:
        """Return empty PII analysis result"""
        return {
            'pii_types': [],
            'confidence': 0.0,
            'classification': 'PUBLIC',
            'method': 'NONE',
            'proposed_policy_tags': []
        }
    
    def _classification_rank(self, classification: str) -> int:
        """Get numeric rank for classification level"""
        rankings = {
            'PUBLIC': 0,
            'INTERNAL': 1,
            'CONFIDENTIAL': 2,
            'RESTRICTED': 3,
            'PII': 4,
            'PCI': 4,
            'PHI': 5
        }
        return rankings.get(classification, 0)

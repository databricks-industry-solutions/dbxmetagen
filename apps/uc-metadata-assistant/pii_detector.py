"""
Self-contained PII Detection Module
==================================

Lightweight PII/PHI detection without external dependencies.
Provides enterprise-grade data classification and policy tagging.

Based on common PII patterns and data analysis techniques used in dbxmetagen.
"""

import re
import json
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PIIDetector:
    """
    Self-contained PII detection with pattern matching and data analysis.
    Provides similar functionality to Presidio but embedded and lightweight.
    """
    
    def __init__(self, settings_manager=None, llm_service=None):
        self.settings_manager = settings_manager
        self.llm_service = llm_service
        self.patterns = self._initialize_patterns()
        self.keywords = self._initialize_keywords()
        self.data_classifications = self._initialize_classifications()
        
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
        """Initialize PII keyword detection"""
        return {
            'personal_info': [
                'first_name', 'last_name', 'full_name', 'name', 'fname', 'lname',
                'address', 'street', 'city', 'state', 'zip', 'postal', 'country',
                'phone', 'mobile', 'telephone', 'email', 'mail', 'contact',
                'ssn', 'social_security', 'social_security_number', 'sin',
                'date_of_birth', 'dob', 'birth_date', 'birthdate', 'age',
                'gender', 'sex', 'race', 'ethnicity', 'nationality'
            ],
            'financial': [
                'account', 'account_number', 'account_num', 'acct_num',
                'credit_card', 'card_number', 'cc_number', 'payment',
                'bank', 'routing', 'aba', 'swift', 'iban',
                'salary', 'income', 'wage', 'earning', 'revenue',
                'amount', 'balance', 'transaction', 'payment'
            ],
            'medical': [
                'patient', 'patient_id', 'patient_number', 'mrn', 'medical_record',
                'diagnosis', 'condition', 'treatment', 'medication', 'drug',
                'procedure', 'icd', 'cpt', 'npi', 'provider', 'physician',
                'hospital', 'clinic', 'healthcare', 'health', 'medical',
                'symptoms', 'allergies', 'insurance', 'claim'
            ],
            'employment': [
                'employee', 'employee_id', 'emp_id', 'staff_id', 'worker',
                'department', 'position', 'title', 'role', 'manager',
                'hire_date', 'start_date', 'termination', 'salary', 'wage'
            ],
            'education': [
                'student', 'student_id', 'grade', 'gpa', 'transcript',
                'school', 'university', 'college', 'education', 'degree'
            ],
            'biometric': [
                'fingerprint', 'retina', 'iris', 'facial', 'biometric',
                'dna', 'genetic', 'blood_type'
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
        llm_analysis = self._analyze_with_llm(
            column_name=column_name,
            data_type=data_type,
            sample_values=sample_values
        )
        
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
            
            logger.info(f"🤖 LLM enhanced PII detection for '{column_name}': {llm_analysis['pii_types']}")
        
        # Determine classification and generate PROPOSED tags
        results['classification'] = self._determine_classification(results['pii_types'])
        results['proposed_policy_tags'] = self._generate_proposed_policy_tags(results['pii_types'], results['classification'])
        results['risk_factors'] = self._assess_risk_factors(results['pii_types'], data_type)
        results['recommendations'] = self._generate_recommendations(results)
        
        # Keep old policy_tags field empty (no automatic tagging)
        results['policy_tags'] = []
        
        # Remove duplicates
        results['pii_types'] = list(set(results['pii_types']))
        results['policy_tags'] = list(set(results['policy_tags']))
        
        return results
    
    def _analyze_column_name(self, column_name: str) -> Dict:
        """Analyze column name for PII indicators"""
        pii_types = []
        confidence = 0.0
        
        # Direct keyword matching
        for category, keywords in self.keywords.items():
            for keyword in keywords:
                if keyword in column_name:
                    pii_types.append(f"{category}_{keyword}")
                    confidence = max(confidence, 0.8)
        
        # Pattern-based detection for common formats
        common_patterns = {
            'email': ['email', 'mail', '@'],
            'phone': ['phone', 'mobile', 'tel'],
            'ssn': ['ssn', 'social_security', 'social'],
            'address': ['address', 'addr', 'street', 'city', 'zip'],
            'name': ['name', 'fname', 'lname', 'first', 'last'],
            'id': ['_id', 'id_', 'identifier', 'number', 'num']
        }
        
        for pattern_type, indicators in common_patterns.items():
            if any(indicator in column_name for indicator in indicators):
                pii_types.append(pattern_type)
                confidence = max(confidence, 0.6)
        
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
        """Determine data classification based on detected PII types"""
        if not pii_types:
            return 'PUBLIC'
        
        # Medical data gets highest classification
        medical_indicators = ['medical', 'patient', 'mrn', 'npi', 'icd', 'phi']
        if any(indicator in ''.join(pii_types).lower() for indicator in medical_indicators):
            return 'PHI'
        
        # Financial data
        financial_indicators = ['credit_card', 'account', 'financial', 'payment', 'bank']
        if any(indicator in ''.join(pii_types).lower() for indicator in financial_indicators):
            return 'PCI'
        
        # Personal information
        personal_indicators = ['ssn', 'social_security', 'personal_info', 'email', 'phone', 'name', 'address']
        if any(indicator in ''.join(pii_types).lower() for indicator in personal_indicators):
            return 'PII'
        
        # Default to restricted if any PII detected
        return 'RESTRICTED' if pii_types else 'PUBLIC'
    
    def _generate_proposed_policy_tags(self, pii_types: List[str], classification: str) -> List[Dict]:
        """Generate PROPOSED policy tags based on PII analysis for manual review"""
        proposed_tags = []
        
        # Add classification tag proposal
        if classification != 'PUBLIC':
            proposed_tags.append({
                'tag': f'classification.{classification}',
                'reason': f'Data classified as {classification} based on content analysis',
                'confidence': 'high',
                'auto_apply': False  # Always requires manual approval
            })
        
        # Add specific PII tag proposals
        tag_mapping = {
            'ssn': {'tag': 'PII.SSN', 'reason': 'Social Security Number patterns detected'},
            'email': {'tag': 'PII.Email', 'reason': 'Email address patterns detected'},
            'phone': {'tag': 'PII.Phone', 'reason': 'Phone number patterns detected'},
            'credit_card': {'tag': 'PCI.CreditCard', 'reason': 'Credit card number patterns detected'},
            'medical': {'tag': 'PHI.Medical', 'reason': 'Medical record patterns detected'},
            'patient': {'tag': 'PHI.Patient', 'reason': 'Patient information patterns detected'},
            'financial': {'tag': 'PCI.Financial', 'reason': 'Financial data patterns detected'},
            'personal_info': {'tag': 'PII.Personal', 'reason': 'Personal information patterns detected'}
        }
        
        for pii_type in pii_types:
            for key, tag_info in tag_mapping.items():
                if key in pii_type.lower():
                    proposed_tags.append({
                        'tag': tag_info['tag'],
                        'reason': tag_info['reason'],
                        'confidence': 'high' if key in ['ssn', 'credit_card', 'email'] else 'medium',
                        'auto_apply': False,  # Always requires manual approval
                        'pii_type': pii_type
                    })
        
        return proposed_tags
    
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
        
        # Analyze each column
        for column in columns:
            column_analysis = self.analyze_column(
                column_name=column.get('name', ''),
                data_type=column.get('data_type', ''),
                sample_values=column.get('sample_values', [])
            )
            
            results['column_analysis'].append(column_analysis)
            
            # Track PII columns
            if column_analysis['classification'] != 'PUBLIC':
                results['pii_columns'] += 1
            
            # Update highest classification
            if self._classification_rank(column_analysis['classification']) > self._classification_rank(results['highest_classification']):
                results['highest_classification'] = column_analysis['classification']
            
            # Collect unique tags
            results['recommended_tags'].extend(column_analysis['policy_tags'])
        
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
        """Use LLM to analyze column for PII content and sensitivity"""
        try:
            if not self.llm_service:
                logger.warning("🤖 LLM service not available for PII detection")
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
                logger.info(f"🤖 LLM PII detection disabled in settings for column '{column_name}'")
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
            
            prompt = f"""Analyze this database column for PII (Personally Identifiable Information):

Column Name: {column_name}
Data Type: {data_type}
{table_info}{sample_info}

Identify if this column contains PII and classify it. Consider:
- Column name patterns (ssn, email, phone, address, etc.)
- Data type appropriateness for PII
- Sample values (if provided)

Respond with ONLY this format:
PII_TYPES: [comma-separated list of detected PII types like "email", "phone", "ssn", "address", "name" or "none"]
CONFIDENCE: [number 0.0-1.0]
CLASSIFICATION: [PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED, PII, PHI, PCI]"""

            logger.info(f"🤖 Calling LLM for PII detection on column '{column_name}'")
            
            response = self.llm_service._call_databricks_llm(
                prompt=prompt,
                max_tokens=100,
                model="databricks-gemma-3-12b",
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
            
            logger.info(f"🤖 LLM detected PII types: {pii_types}, confidence: {confidence}, classification: {classification}")
            
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

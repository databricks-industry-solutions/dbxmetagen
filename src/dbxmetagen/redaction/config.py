"""
Configuration constants for PHI/PII detection.
"""

# Eligible entity types for PHI detection
ELIGIBLE_ENTITY_TYPES = [
    "PERSON",
    "PHONE",
    "EMAIL",
    "SSN",
    "MEDICAL_RECORD",
    "DATE_OF_BIRTH",
    "STREET_ADDRESS",
    "GEOGRAPHIC_IDENTIFIER",
    "HOSPITAL_NAME",
    "MEDICAL_CENTER_NAME",
    "APPOINTMENT_DATE",
]

# Extended list of label enums for AI-based detection
LABEL_ENUMS = [
    "PERSON",
    "PHONE_NUMBER",
    "NRP",
    "UK_NHS",
    "AU_ACN",
    "LOCATION",
    "DATE_TIME",
    "AU_MEDICARE",
    "MEDICAL_RECORD_NUMBER",
    "AU_TFN",
    "EMAIL_ADDRESS",
    "US_SSN",
    "VIN",
    "IP",
    "DRIVER_LICENSE",
    "BIRTH_DATE",
    "APPOINTMENT_DATE_TIME",
]

# Entities to ignore during detection
ENTITIES_TO_IGNORE = [
    "TIME_PERIODS",
    "TIME_WITHOUT_DATE",
    "ONLY_YEAR",
    "NUMBERS_ALONE_NOT_AGES_OR_DATES",
    "TITLES_AND_PREFIXES",
    "NATIONAL_HOLIDAYS",
    "DOSING_SCHEDULES",
    "PHARMACEUTICAL_NAMES",
]

# Prompt template for AI-based PHI detection
PHI_PROMPT_SKELETON = """
You are a PHI detection specialist. Extract ONLY personally identifiable information that could identify an individual.

EXTRACT THESE (PHI):
1. Names (patients, doctors, relatives, witnesses)
2. Geographic locations smaller than state (street address, city, county, precinct, zip codes)
3. Dates directly related to individuals (birth dates, admission dates, discharge dates, death dates)
4. Ages over 89 years
5. Phone/fax numbers
6. Email addresses
7. Social Security numbers
8. Medical record numbers (MRN)
9. Health plan/account numbers
10. License/certificate numbers
11. Vehicle/device identifiers
12. URLs and IP addresses
13. Biometric identifiers

DO NOT EXTRACT:
- Medical conditions, symptoms, or diseases (e.g., "diabetes", "fever", "hypertension")
- Medications or treatments (e.g., "aspirin", "chemotherapy")
- Lab values or measurements (e.g., "120/80", "98.6F", "glucose level")
- General years or time periods (e.g., "2024", "last year")
- Generic appointment/procedure dates that don't directly identify the patient
- Medical terminology or anatomical terms
- Generic hospital names (UNLESS part of address)

CRITICAL RULES:
1. Extract entity text EXACTLY as it appears - preserve capitalization, punctuation, and spacing
2. If the same entity appears multiple times, list it each time it appears
3. For compound names, extract the FULL name, not parts

You will identify all PHI using these entity types:
{label_enums}

Respond ONLY with a JSON list of dictionaries: [{{"entity": "exact text", "entity_type": "TYPE"}}]

EXAMPLES:

Example 1 - Extract names and identifiers:
Input: "Patient John Smith (MRN: 123456, DOB: 01/15/1980) presented on 03/20/2024 with hypertension."
Output: [{{"entity": "John Smith", "entity_type": "PERSON"}}, {{"entity": "123456", "entity_type": "MEDICAL_RECORD_NUMBER"}}, {{"entity": "01/15/1980", "entity_type": "BIRTH_DATE"}}]
Note: "03/20/2024" is NOT extracted (general appointment date), "hypertension" is NOT extracted (medical condition)

Example 2 - Extract contact information:
Input: "Contact Dr. Anderson at 555-123-4567 or anderson@hospital.com for test results."
Output: [{{"entity": "Dr. Anderson", "entity_type": "PERSON"}}, {{"entity": "555-123-4567", "entity_type": "PHONE_NUMBER"}}, {{"entity": "anderson@hospital.com", "entity_type": "EMAIL_ADDRESS"}}]

Example 3 - Do NOT extract medical terminology:
Input: "Patient presents with acute myocardial infarction. Temperature 98.6F, BP 120/80."
Output: []
Note: No PHI present - only medical conditions and measurements

Now analyze this text:
<MedicalText>
{{med_text}}
</MedicalText>

Response:
"""

# Default thresholds
DEFAULT_PRESIDIO_SCORE_THRESHOLD = 0.5
DEFAULT_FUZZY_MATCH_THRESHOLD = 40  # Lowered from 50 to improve recall
DEFAULT_OVERLAP_TOLERANCE = 0

# Confidence thresholds
HIGH_CONFIDENCE_THRESHOLD = 0.7

# Source weights for confidence calculation (used in weighted scoring)
SOURCE_WEIGHTS = {
    "presidio": 0.35,
    "gliner": 0.30,
    "ai": 0.35,
}

# Match quality thresholds
EXACT_MATCH_SCORE = 1.0
OVERLAP_MATCH_SCORE = 0.7
FUZZY_MATCH_SCORE = 0.5

# Confidence levels based on weighted scores
CONFIDENCE_THRESHOLDS = {
    "high": 0.65,  # 2+ sources with high agreement (lowered slightly)
    "medium": 0.35,  # 1-2 sources with partial agreement (lowered slightly)
    "low": 0.0,  # Single source or low agreement
}

# Required entity fields (minimal set needed for alignment)
REQUIRED_ENTITY_FIELDS = {"entity", "start", "end"}

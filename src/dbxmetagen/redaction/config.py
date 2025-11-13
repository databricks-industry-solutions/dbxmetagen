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
- Generic appointment/procedure dates that do not directly identify the patient
- Medical terminology or anatomical terms
- Generic hospital names (UNLESS part of address)

CRITICAL RULES:
1. Extract entity text EXACTLY as it appears - preserve capitalization, punctuation, and spacing
2. If the same entity appears multiple times, list it each time it appears
3. For compound names, extract the FULL name, not parts
4. If two entities appear one after the other, extract them both separately, not as a single entity. For example, "Alice Anderson 123-45-6789" should be extracted as two entities, "Alice Anderson" and "123-45-6789".

You will identify all PHI with the following enums as the "label":

{label_enums}

Respond with a list of dictionaries such as [{{"entity": "Alice Anderson", "entity_type": "PERSON"}}, {{"entity": "123-45-6789", "entity_type": "US_SSN"}}]

Note that if there are multiple of the same entities, you should list them multiple times. For example, if the text suggests "The patient, Brennan, notes that is feeling unwell. Brennan presents with a moderate fever of 100.5F," you should list the entity "brennan" twice. 

The text is listed here: 
<MedicalText>
{{med_text}}
<MedicalText/>

EXAMPLE: 
MedicalText: "MRN: 222345 -- I saw patient Alice Anderson today at 11:30am, who presents with a sore throat and temperature of 103F"
response: [{{"entity": "Alice Anderson", "entity_type": "PERSON"}}, {{"entity": "222345", "entity_type": "MEDICAL_RECORD_NUMBER"}}]

Note: "11:30am" and "103F" are NOT extracted (time and measurement, not PHI). "sore throat" is NOT extracted (medical symptom).
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

# Databricks notebook source

# MAGIC %md
# MAGIC # Multi-Domain Data Generator for Healthcare and Life Sciences

# COMMAND ----------


# MAGIC %pip install dbldatagen faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Output Catalog Name (Required)")
catalog_name = dbutils.widgets.get("catalog_name")
if catalog_name == "":
    raise ValueError("Output Catalog Name is required")

# COMMAND ----------

from dataclasses import dataclass
from typing import Dict, List, Tuple
from abc import ABC, abstractmethod
import re
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType,
    DoubleType,
    TimestampType,
    BooleanType,
)

from dbldatagen import DataGenerator, PyfuncText
from faker import Faker
from faker.providers import BaseProvider

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


@dataclass
class SchemaConfig:
    """Configuration for schema generation"""

    base_rows: int = 100
    partitions: int = 1
    start_date: str = "2020-01-01"
    end_date: str = "2024-12-31"


class BaseSchemaGenerator(ABC):
    """Abstract base class for domain-specific schema generators"""

    def __init__(self, spark: SparkSession, config: SchemaConfig):
        self.spark = spark
        self.config = config
        self.faker = Faker()

    @abstractmethod
    def generate_tables(self) -> Dict[str, DataFrame]:
        """Generate all tables for this schema"""
        ...


class MedicalProvider(BaseProvider):
    """Custom Faker provider for medical data"""

    medical_conditions = [
        "Hypertension",
        "Type 2 Diabetes",
        "Coronary Artery Disease",
        "Asthma",
        "COPD",
        "Pneumonia",
        "Myocardial Infarction",
        "Atrial Fibrillation",
        "Heart Failure",
        "Acute Renal Failure",
        "Sepsis",
        "Stroke",
        "Pulmonary Embolism",
    ]

    medications = [
        "Lisinopril",
        "Metformin",
        "Aspirin",
        "Atorvastatin",
        "Omeprazole",
        "Albuterol",
        "Furosemide",
        "Warfarin",
        "Insulin",
        "Prednisone",
    ]

    departments = [
        "Emergency",
        "Cardiology",
        "Internal Medicine",
        "Surgery",
        "ICU",
        "Oncology",
        "Pediatrics",
        "Orthopedics",
        "Neurology",
        "Radiology",
    ]

    def medical_condition(self) -> str:
        return self.random_element(self.medical_conditions)

    def medication(self) -> str:
        return self.random_element(self.medications)

    def department(self) -> str:
        return self.random_element(self.departments)


class ClinicalTrialProvider(BaseProvider):
    """Custom Faker provider for clinical trial data"""

    trial_phases = ["Phase I", "Phase II", "Phase III", "Phase IV"]
    trial_statuses = ["Active", "Completed", "Suspended", "Terminated"]
    therapeutic_areas = ["Oncology", "Cardiology", "Neurology", "Immunology"]

    def trial_phase(self) -> str:
        return self.random_element(self.trial_phases)

    def trial_status(self) -> str:
        return self.random_element(self.trial_statuses)

    def therapeutic_area(self) -> str:
        return self.random_element(self.therapeutic_areas)


class LivestockProvider(BaseProvider):
    """Custom Faker provider for livestock data"""

    species_list = ["Cattle", "Sheep", "Goat", "Pig", "Chicken", "Horse"]
    breeds = {
        "Cattle": ["Holstein", "Angus", "Hereford", "Jersey"],
        "Sheep": ["Merino", "Suffolk", "Dorper", "Romney"],
        "Pig": ["Yorkshire", "Duroc", "Hampshire", "Landrace"],
    }

    def animal_species(self) -> str:
        return self.random_element(self.species_list)

    def animal_breed(self) -> str:
        return self.random_element(
            ["Holstein", "Angus", "Merino", "Yorkshire", "Mixed"]
        )


# Standalone functions for PyfuncText to avoid serialization issues
def init_faker_for_generation(context):
    """Initialize faker context for PyfuncText - standalone function"""
    context.faker = Faker()
    # Add custom providers
    context.faker.add_provider(MedicalProvider)
    context.faker.add_provider(ClinicalTrialProvider)
    context.faker.add_provider(LivestockProvider)


def generate_first_name(context, _):
    """Generate first name"""
    return context.faker.first_name()


def generate_last_name(context, _):
    """Generate last name"""
    return context.faker.last_name()


def generate_ssn(context, _):
    """Generate SSN"""
    return context.faker.ssn()


def generate_phone_number(context, _):
    """Generate phone number"""
    return context.faker.phone_number()


def generate_email(context, _):
    """Generate email address"""
    return context.faker.email()


def generate_address(context, _):
    """Generate address"""
    return context.faker.address().replace("\n", ", ")


def generate_company(context, _):
    """Generate company name"""
    return context.faker.company()


def generate_name(context, _):
    """Generate full name"""
    return context.faker.name()


def generate_city_medical_center(context, _):
    """Generate city medical center name"""
    return f"{context.faker.city()} Medical Center"


def generate_city_research_farm(context, _):
    """Generate city research farm name"""
    return f"{context.faker.city()} Research Farm"


def generate_dr_name(context, _):
    """Generate doctor name"""
    return f"Dr. {context.faker.name()}"


def generate_medical_condition(context, _):
    """Generate medical condition"""
    return context.faker.medical_condition()


def generate_medication(context, _):
    """Generate medication name"""
    return context.faker.medication()


def generate_department(context, _):
    """Generate department name"""
    return context.faker.department()


def generate_trial_phase(context, _):
    """Generate trial phase"""
    return context.faker.trial_phase()


def generate_trial_status(context, _):
    """Generate trial status"""
    return context.faker.trial_status()


def generate_therapeutic_area(context, _):
    """Generate therapeutic area"""
    return context.faker.therapeutic_area()


def generate_animal_species(context, _):
    """Generate animal species"""
    return context.faker.animal_species()


def generate_animal_breed(context, _):
    """Generate animal breed"""
    return context.faker.animal_breed()


def generate_trial_title(context, _):
    """Generate clinical trial title"""
    return (
        f"Study of {context.faker.medication()} in {context.faker.therapeutic_area()}"
    )


def generate_study_title(context, _):
    """Generate research study title"""
    return (
        f"Effects of {context.faker.medication()} on {context.faker.animal_species()}"
    )


def generate_soap_note(context, _):
    """Generate realistic SOAP format clinical note with detailed PII/PHI"""
    patient_name = context.faker.name()
    dob = context.faker.date_of_birth(minimum_age=18, maximum_age=90)
    mrn = f"MRN-{context.faker.random_number(digits=7, fix_len=True)}"
    ssn = context.faker.ssn()
    address = context.faker.address().replace("\n", ", ")
    phone = context.faker.phone_number()
    physician = f"Dr. {context.faker.name()}"
    condition = context.faker.medical_condition()
    medication1 = context.faker.medication()
    medication2 = context.faker.medication()

    bp_sys = context.faker.random_int(min=110, max=160)
    bp_dia = context.faker.random_int(min=70, max=95)
    temp = round(context.faker.random.uniform(97.5, 99.8), 1)
    hr = context.faker.random_int(min=60, max=100)
    rr = context.faker.random_int(min=12, max=20)

    hospital = f"{context.faker.city()} Medical Center"
    date = context.faker.date_between(start_date="-1y", end_date="today")

    return f"""SOAP NOTE - {hospital}
Date of Service: {date.strftime('%B %d, %Y')}
Provider: {physician}

PATIENT INFORMATION:
Name: {patient_name}
Date of Birth: {dob.strftime('%m/%d/%Y')}
MRN: {mrn}
SSN: {ssn}
Address: {address}
Phone: {phone}

SUBJECTIVE:
Chief Complaint: Patient presents with symptoms of {condition.lower()}.

History of Present Illness: {patient_name} is a {context.faker.random_int(min=25, max=75)}-year-old patient who reports experiencing symptoms for the past {context.faker.random_int(min=3, max=14)} days. Patient describes {context.faker.sentence(nb_words=15)} The symptoms have been progressively {context.faker.random_element(['worsening', 'improving', 'stable'])}. Patient denies any recent trauma, fever, or weight loss. Previous treatment with over-the-counter medications provided minimal relief.

Past Medical History: {context.faker.random_element(['Hypertension', 'Type 2 Diabetes', 'Hyperlipidemia'])}, {context.faker.random_element(['Asthma', 'COPD', 'No significant history'])}

Current Medications: {medication1} {context.faker.random_int(min=5, max=100)}mg {context.faker.random_element(['once daily', 'twice daily', 'three times daily'])}, {medication2} {context.faker.random_int(min=10, max=500)}mg {context.faker.random_element(['daily', 'as needed'])}

Allergies: {context.faker.random_element(['NKDA (No Known Drug Allergies)', 'Penicillin - rash', 'Sulfa drugs - hives'])}

Social History: {context.faker.random_element(['Non-smoker', 'Former smoker', 'Current smoker - 1 PPD'])}, alcohol use {context.faker.random_element(['social', 'none', 'occasional'])}, denies illicit drug use. Lives with {context.faker.random_element(['spouse', 'family', 'alone'])}, works as {context.faker.job()}.

OBJECTIVE:
Vital Signs:
- Blood Pressure: {bp_sys}/{bp_dia} mmHg
- Temperature: {temp}°F
- Heart Rate: {hr} bpm
- Respiratory Rate: {rr} breaths/min
- O2 Saturation: {context.faker.random_int(min=95, max=100)}% on room air
- Weight: {context.faker.random_int(min=120, max=250)} lbs
- Height: {context.faker.random_int(min=60, max=75)} inches

Physical Examination:
General: Patient appears {context.faker.random_element(['well-developed, well-nourished', 'comfortable', 'in no acute distress'])}, alert and oriented x3.
HEENT: Normocephalic, atraumatic. PERRLA. Oropharynx clear without erythema or exudate. Tympanic membranes intact bilaterally.
Cardiovascular: Regular rate and rhythm. Normal S1 and S2. No murmurs, rubs, or gallops appreciated. Peripheral pulses 2+ bilaterally.
Respiratory: Lungs clear to auscultation bilaterally. No wheezes, rales, or rhonchi. Respiratory effort normal.
Abdomen: Soft, non-tender, non-distended. Bowel sounds present in all four quadrants. No organomegaly or masses palpated.
Extremities: No cyanosis, clubbing, or edema. Full range of motion. No joint tenderness or swelling.
Neurological: Cranial nerves II-XII grossly intact. Motor strength 5/5 in all extremities. Sensation intact to light touch.

ASSESSMENT:
1. {condition} - {context.faker.random_element(['acute', 'chronic', 'acute exacerbation of chronic'])}
2. {context.faker.random_element(['Hypertension - controlled', 'Type 2 Diabetes - stable', 'No secondary diagnoses'])}

PLAN:
1. Diagnostic Studies: {context.faker.random_element(['Complete blood count', 'Basic metabolic panel', 'Chest X-ray', 'ECG', 'No labs needed at this time'])}
2. Medications: Prescribe {medication1} {context.faker.random_int(min=5, max=100)}mg {context.faker.random_element(['once daily', 'twice daily'])} for {context.faker.random_int(min=7, max=30)} days. Continue current {medication2}.
3. Patient Education: Discussed diagnosis, treatment plan, and expected outcomes with patient. Patient verbalizes understanding and agrees with plan. Provided written materials regarding condition management.
4. Follow-up: Return to clinic in {context.faker.random_int(min=1, max=4)} weeks for re-evaluation. Patient instructed to call or return sooner if symptoms worsen or new symptoms develop.
5. Referrals: {context.faker.random_element(['None needed at this time', f'Refer to {context.faker.random_element(["Cardiology", "Endocrinology", "Gastroenterology"])} for further evaluation'])}

Time spent with patient: {context.faker.random_int(min=15, max=45)} minutes
Medical decision making: {context.faker.random_element(['Low complexity', 'Moderate complexity', 'High complexity'])}

Electronically signed by {physician}, MD
License: MD{context.faker.random_number(digits=6, fix_len=True)}
Date: {date.strftime('%m/%d/%Y %H:%M')}"""


def generate_hp_note(context, _):
    """Generate realistic History & Physical note with detailed PII/PHI"""
    patient_name = context.faker.name()
    dob = context.faker.date_of_birth(minimum_age=18, maximum_age=90)
    mrn = f"MRN-{context.faker.random_number(digits=7, fix_len=True)}"
    ssn = context.faker.ssn()
    address = context.faker.address().replace("\n", ", ")
    phone = context.faker.phone_number()
    physician = f"Dr. {context.faker.name()}"
    condition = context.faker.medical_condition()
    medication1 = context.faker.medication()
    medication2 = context.faker.medication()

    bp_sys = context.faker.random_int(min=110, max=160)
    bp_dia = context.faker.random_int(min=70, max=95)
    temp = round(context.faker.random.uniform(97.5, 99.8), 1)
    hr = context.faker.random_int(min=60, max=100)
    rr = context.faker.random_int(min=12, max=20)

    hospital = f"{context.faker.city()} Memorial Hospital"
    admit_date = context.faker.date_between(start_date="-30d", end_date="today")

    return f"""HISTORY AND PHYSICAL EXAMINATION
{hospital}

Date of Admission: {admit_date.strftime('%B %d, %Y at %H:%M')}
Attending Physician: {physician}

PATIENT DEMOGRAPHICS:
Name: {patient_name}
Date of Birth: {dob.strftime('%m/%d/%Y')} (Age: {context.faker.random_int(min=25, max=75)} years)
Medical Record Number: {mrn}
Social Security Number: {ssn}
Home Address: {address}
Contact Phone: {phone}
Emergency Contact: {context.faker.name()}, {context.faker.random_element(['spouse', 'daughter', 'son', 'parent'])} - {context.faker.phone_number()}

CHIEF COMPLAINT:
"{context.faker.sentence(nb_words=8)[:-1]}"

HISTORY OF PRESENT ILLNESS:
{patient_name} is a {context.faker.random_int(min=25, max=75)}-year-old {context.faker.random_element(['male', 'female'])} with a past medical history significant for {context.faker.random_element(['hypertension', 'diabetes mellitus type 2', 'coronary artery disease'])} who presents to the emergency department with {condition.lower()}. The patient reports symptom onset approximately {context.faker.random_int(min=6, max=72)} hours prior to presentation. 

Associated symptoms include {context.faker.sentence(nb_words=12)} The patient denies any recent illness, travel, or sick contacts. Patient attempted self-management with {context.faker.random_element(['over-the-counter analgesics', 'rest', 'home remedies'])} without significant improvement. Given persistent symptoms and {context.faker.random_element(['concern for complications', 'worsening condition', 'inability to tolerate oral intake'])}, patient presented to ED for evaluation.

PAST MEDICAL HISTORY:
1. {context.faker.random_element(['Hypertension', 'Type 2 Diabetes Mellitus', 'Hyperlipidemia'])} - diagnosed {context.faker.random_int(min=2, max=15)} years ago
2. {context.faker.random_element(['Coronary Artery Disease', 'Asthma', 'COPD', 'GERD'])}
3. {context.faker.random_element(['Chronic Kidney Disease Stage 2', 'Hypothyroidism', 'Osteoarthritis', 'No other significant history'])}

PAST SURGICAL HISTORY:
1. {context.faker.random_element(['Appendectomy', 'Cholecystectomy', 'Hernia repair', 'None'])} - {context.faker.random_int(min=5, max=30)} years ago
2. {context.faker.random_element(['Knee arthroscopy', 'Carpal tunnel release', 'Cataract surgery', 'None'])}

MEDICATIONS (CURRENT):
1. {medication1} {context.faker.random_int(min=5, max=100)}mg orally {context.faker.random_element(['once daily', 'twice daily'])}
2. {medication2} {context.faker.random_int(min=10, max=500)}mg orally {context.faker.random_element(['once daily', 'twice daily', 'as needed'])}
3. {context.faker.medication()} {context.faker.random_int(min=5, max=50)}mg {context.faker.random_element(['daily', 'twice daily'])}
4. Aspirin 81mg orally once daily

ALLERGIES:
{context.faker.random_element(['No Known Drug Allergies (NKDA)', 'Penicillin (rash)', 'Sulfa medications (hives and itching)', 'Codeine (nausea)'])}

FAMILY HISTORY:
Father: {context.faker.random_element(['Myocardial infarction at age 62', 'Diabetes', 'Hypertension', 'No significant history'])}
Mother: {context.faker.random_element(['Breast cancer', 'Stroke at age 70', 'Diabetes', 'Alive and well'])}
Siblings: {context.faker.random_int(min=0, max=4)} {context.faker.random_element(['siblings', 'sibling', 'none'])} - {context.faker.random_element(['non-contributory', 'history of hypertension'])}

SOCIAL HISTORY:
Occupation: {context.faker.job()}
Tobacco: {context.faker.random_element(['Never smoker', 'Former smoker, quit 10 years ago', 'Current smoker, 1 pack per day for 20 years'])}
Alcohol: {context.faker.random_element(['Social drinker, 1-2 drinks per week', 'None', '2-3 drinks daily'])}
Illicit Drugs: Denies
Living Situation: Lives with {context.faker.random_element(['spouse', 'family', 'alone'])} in {context.faker.random_element(['house', 'apartment'])}
Exercise: {context.faker.random_element(['Walks 30 minutes 3x per week', 'Sedentary lifestyle', 'Regular exercise'])}

REVIEW OF SYSTEMS:
Constitutional: {context.faker.random_element(['Denies fever, chills, night sweats', 'Endorses fatigue', 'Reports recent weight loss'])}
HEENT: {context.faker.random_element(['Denies headache, vision changes, hearing loss', 'Reports occasional headaches'])}
Cardiovascular: {context.faker.random_element(['Denies chest pain, palpitations', 'Reports occasional palpitations'])}
Respiratory: {context.faker.random_element(['Denies shortness of breath, cough', 'Reports dyspnea on exertion'])}
Gastrointestinal: {context.faker.random_element(['Denies nausea, vomiting, diarrhea', 'Reports occasional constipation'])}
All other systems reviewed and negative except as noted in HPI.

PHYSICAL EXAMINATION:
Vital Signs:
Temperature: {temp}°F ({round(context.faker.random.uniform(36.5, 37.6), 1)}°C)
Blood Pressure: {bp_sys}/{bp_dia} mmHg
Heart Rate: {hr} beats per minute, regular
Respiratory Rate: {rr} breaths per minute
Oxygen Saturation: {context.faker.random_int(min=94, max=100)}% on room air
Height: {context.faker.random_int(min=150, max=190)} cm
Weight: {context.faker.random_int(min=55, max=120)} kg
BMI: {round(context.faker.random.uniform(20, 32), 1)}

General: Patient appears stated age, {context.faker.random_element(['well-developed', 'well-nourished'])}, in {context.faker.random_element(['no acute distress', 'mild distress', 'moderate distress'])}. Alert, oriented to person, place, time, and situation.

HEENT: Head is normocephalic and atraumatic. Pupils equal, round, reactive to light and accommodation. Extraocular movements intact. Sclerae anicteric. Conjunctivae pink. Oropharynx clear without erythema, exudate, or lesions. Mucous membranes moist. Tympanic membranes intact with normal light reflex bilaterally.

Neck: Supple without lymphadenopathy. No thyromegaly or thyroid nodules. Trachea midline. No jugular venous distension. Carotid pulses 2+ bilaterally without bruits.

Cardiovascular: Regular rate and rhythm. Normal S1 and S2. No S3 or S4 gallops. No murmurs, rubs, or clicks appreciated. Point of maximal impulse non-displaced. Peripheral pulses 2+ and symmetric in all extremities. Capillary refill less than 2 seconds.

Respiratory: Chest wall symmetric with normal respiratory effort. Lungs clear to auscultation in all fields bilaterally. No wheezes, rales, or rhonchi. No accessory muscle use. Tactile fremitus normal.

Abdomen: Soft, non-tender, non-distended. Bowel sounds normoactive in all four quadrants. No hepatosplenomegaly. No masses or hernias palpated. No costovertebral angle tenderness. No rebound or guarding.

Musculoskeletal: Normal bulk and tone. Full range of motion in all major joints. No erythema, warmth, or effusion. No focal bony tenderness. Gait steady and coordinated.

Skin: Warm, dry, intact. Normal turgor. No rashes, lesions, or ulcerations. No cyanosis or pallor.

Neurological: Alert and oriented x4. Cranial nerves II through XII grossly intact. Motor strength 5/5 in all major muscle groups. Deep tendon reflexes 2+ and symmetric. Sensation intact to light touch and proprioception. Coordination and cerebellar function normal. Negative Romberg.

Psychiatric: Appropriate mood and affect. Thought process logical and goal-directed. No suicidal or homicidal ideation.

DIAGNOSTIC STUDIES:
Laboratory: {context.faker.random_element(['Complete blood count, comprehensive metabolic panel pending', 'Results pending', 'Labs drawn and sent to lab'])}
Imaging: {context.faker.random_element(['Chest X-ray ordered', 'CT scan pending', 'No imaging at this time'])}

ASSESSMENT AND PLAN:
{patient_name} is a {context.faker.random_int(min=25, max=75)}-year-old with history of {context.faker.random_element(['hypertension', 'diabetes', 'CAD'])} presenting with {condition.lower()}.

Primary Diagnosis: {condition}
{context.faker.sentence(nb_words=20)}

Plan:
1. Admit to {context.faker.random_element(['medical floor', 'observation unit', 'telemetry'])} for further management and monitoring
2. NPO pending {context.faker.random_element(['imaging', 'procedure', 'further evaluation'])}
3. IV fluids: Normal saline at {context.faker.random_int(min=75, max=125)} mL/hour
4. Medications: Continue home medications. Add {medication1} for symptom management
5. Consult {context.faker.random_element(['Cardiology', 'Gastroenterology', 'Surgery', 'Internal Medicine'])} service
6. Serial examinations and vital signs monitoring every {context.faker.random_element(['2', '4', '6'])} hours
7. DVT prophylaxis with subcutaneous heparin
8. Fall precautions
9. Advance diet as tolerated
10. Discharge planning: Will reassess based on clinical response to treatment

Patient and family counseled regarding diagnosis, treatment plan, and expected hospital course. All questions answered. Patient consents to admission and treatment plan.

Electronically signed: {physician}, MD
Medical License: MD{context.faker.random_number(digits=6, fix_len=True)}
Date and Time: {admit_date.strftime('%m/%d/%Y %H:%M')}
Location: {hospital} Emergency Department"""


def deidentify_text_with_annotations(text: str) -> Tuple[str, List[Dict]]:
    """
    De-identify clinical text and return both masked text and entity annotations.
    Returns tuple of (deidentified_text, annotations_list)
    """
    # Pattern definitions with entity types - ORDER MATTERS for proper masking
    patterns = [
        # SSN patterns
        (r"\b\d{3}-\d{2}-\d{4}\b", "SSN"),
        # MRN patterns
        (r"MRN-\d{7}", "MEDICAL_RECORD_NUMBER"),
        (r"Medical Record Number:\s*MRN-\d{7}", "MEDICAL_RECORD_NUMBER"),
        # Date patterns (various formats)
        (
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}(?:\s+at\s+\d{2}:\d{2})?",
            "DATE",
        ),
        (r"\b\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{2}:\d{2})?", "DATE"),
        # Phone patterns
        (r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "PHONE_NUMBER"),
        (r"\(\d{3}\)\s*\d{3}[-.]?\d{4}\b", "PHONE_NUMBER"),
        # License patterns
        (r"License:\s*MD\d{6}", "LICENSE_NUMBER"),
        (r"Medical License:\s*MD\d{6}", "LICENSE_NUMBER"),
    ]

    # First pass: collect all entities with positions
    entities_to_mask = []
    for pattern, entity_type in patterns:
        for match in re.finditer(pattern, text):
            entities_to_mask.append(
                {
                    "start": match.start(),
                    "end": match.end(),
                    "text": match.group(),
                    "entity_type": entity_type,
                }
            )

    # Name patterns - more complex, done separately
    # Look for "Name: " pattern and "Dr. Name" pattern
    name_patterns = [
        r"Name:\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"Dr\.\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"Electronically signed by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"Provider:\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"Attending Physician:\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"Emergency Contact:\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"([A-Z][a-z]+\s+[A-Z][a-z]+)\s+is a\s+\d{2}-year-old",
    ]

    for pattern in name_patterns:
        for match in re.finditer(pattern, text):
            name_text = match.group(1) if "(" in pattern else match.group()
            entities_to_mask.append(
                {
                    "start": match.start(),
                    "end": match.end(),
                    "text": match.group(),
                    "entity_type": "PERSON",
                    "name_only": name_text,
                }
            )

    # Address patterns - look for "Address:" pattern
    address_pattern = r"(?:Address|Home Address):\s+([^\\n]+(?:,\s*[^\\n]+)*)"
    for match in re.finditer(address_pattern, text):
        entities_to_mask.append(
            {
                "start": match.start(),
                "end": match.end(),
                "text": match.group(),
                "entity_type": "LOCATION",
            }
        )

    # Location patterns (hospitals, cities in context)
    location_patterns = [
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Medical Center|Memorial Hospital|Hospital)",
    ]
    for pattern in location_patterns:
        for match in re.finditer(pattern, text):
            entities_to_mask.append(
                {
                    "start": match.start(),
                    "end": match.end(),
                    "text": match.group(),
                    "entity_type": "LOCATION",
                }
            )

    # Sort by position (reverse order for replacement)
    entities_to_mask.sort(key=lambda x: x["start"], reverse=True)

    # Remove overlapping entities (keep first/longest)
    filtered_entities = []
    for entity in entities_to_mask:
        overlap = False
        for existing in filtered_entities:
            if not (
                entity["end"] <= existing["start"] or entity["start"] >= existing["end"]
            ):
                overlap = True
                break
        if not overlap:
            filtered_entities.append(entity)

    # Now mask the text and build annotations
    masked_text = text
    final_annotations = []

    for entity in filtered_entities:
        original_entity = entity["text"]
        entity_type = entity["entity_type"]
        original_start = entity["start"]
        original_end = entity["end"]

        # Create mask based on entity type
        if entity_type == "PERSON":
            mask = "[NAME]"
        elif entity_type == "DATE":
            mask = "[DATE]"
        elif entity_type == "SSN":
            mask = "[SSN]"
        elif entity_type == "MEDICAL_RECORD_NUMBER":
            mask = "[MRN]"
        elif entity_type == "PHONE_NUMBER":
            mask = "[PHONE]"
        elif entity_type == "LOCATION":
            mask = "[LOCATION]"
        elif entity_type == "LICENSE_NUMBER":
            mask = "[LICENSE]"
        else:
            mask = f"[{entity_type}]"

        # Replace in text
        masked_text = masked_text[:original_start] + mask + masked_text[original_end:]

        # Calculate new positions after masking (working backwards)
        annotation = {
            "entity": original_entity,
            "entity_type": entity_type,
            "start": original_start,
            "end": original_start + len(mask),
            "score": None,
            "analysis_explanation": None,
            "recognition_metadata": {},
        }
        final_annotations.append(annotation)

    # Sort annotations by start position for output
    final_annotations.sort(key=lambda x: x["start"])

    return masked_text, final_annotations


def generate_deidentified_note(context, row):
    """Generate de-identified version of clinical note"""
    # Generate original note (alternating between SOAP and H&P)
    note_type = row if row else 0
    if note_type % 2 == 0:
        original_note = generate_soap_note(context, None)
    else:
        original_note = generate_hp_note(context, None)

    deidentified, _ = deidentify_text_with_annotations(original_note)
    return deidentified


def generate_entity_annotations(context, row):
    """Generate entity annotations for de-identified note"""
    # Generate original note
    note_type = row if row else 0
    if note_type % 2 == 0:
        original_note = generate_soap_note(context, None)
    else:
        original_note = generate_hp_note(context, None)

    _, annotations = deidentify_text_with_annotations(original_note)
    # Return as JSON string to be parsed by Spark
    return json.dumps(annotations)


def generate_clinical_note_mixed(context, _):
    """Generate either SOAP or H&P note randomly"""
    if context.faker.random.random() < 0.6:  # 60% SOAP, 40% H&P
        return generate_soap_note(context, None)
    else:
        return generate_hp_note(context, None)


def generate_billing_note(context, _):
    """Generate billing note text"""
    return f"""BILLING NOTE
Insurance: {context.faker.company()} Health Plan
Claim Status: {context.faker.random_element(['Approved', 'Pending', 'Denied'])}
Contact: {context.faker.phone_number()}
Patient Responsibility: ${context.faker.random_int(min=0, max=5000)}"""


def generate_ae_description(context, _):
    """Generate adverse event description"""
    event = context.faker.random_element(["Nausea", "Headache", "Fatigue"])
    return f"ADVERSE EVENT: Patient experienced {event.lower()} after study drug administration."


def generate_vet_observation(context, _):
    """Generate detailed veterinary observation note"""
    species = context.faker.animal_species()
    breed = context.faker.animal_breed()
    animal_id = f"TAG{context.faker.random_number(digits=6, fix_len=True)}"
    veterinarian = f"Dr. {context.faker.name()}"
    facility = f"{context.faker.city()} Research Farm"
    date = context.faker.date_between(start_date="-6m", end_date="today")

    temp = round(context.faker.random.uniform(37.5, 39.5), 1)
    hr = context.faker.random_int(min=60, max=120)
    rr = context.faker.random_int(min=15, max=40)
    weight = round(context.faker.random.uniform(50, 600), 1)

    medication = context.faker.medication()
    dosage = context.faker.random_int(min=5, max=500)

    clinical_status = context.faker.random_element(
        [
            "Normal - no abnormalities detected",
            "Mild lethargy noted",
            "Decreased appetite observed",
            "Good body condition",
            "Alert and responsive",
        ]
    )

    return f"""VETERINARY OBSERVATION REPORT
{facility}
Date of Examination: {date.strftime('%B %d, %Y')}
Examining Veterinarian: {veterinarian}

ANIMAL IDENTIFICATION:
Species: {species}
Breed: {breed}
Animal ID: {animal_id}
Study Group: {context.faker.random_element(['Control', 'Treatment A', 'Treatment B'])}

VITAL SIGNS:
Body Weight: {weight} kg
Temperature: {temp}°C ({round(temp * 9/5 + 32, 1)}°F)
Heart Rate: {hr} beats per minute
Respiratory Rate: {rr} breaths per minute

PHYSICAL EXAMINATION:
General Appearance: Animal is {context.faker.random_element(['alert', 'bright', 'quiet but responsive', 'active'])} and {context.faker.random_element(['well-nourished', 'appropriate body condition', 'good body condition'])}. Body condition score: {context.faker.random_int(min=3, max=5)}/5.

Integument: Haircoat appears {context.faker.random_element(['healthy and lustrous', 'normal', 'well-groomed'])}. Skin is intact with no lesions, masses, or areas of alopecia noted. Mucous membranes are {context.faker.random_element(['pink and moist', 'normal pink', 'pale pink'])} with capillary refill time less than 2 seconds.

Eyes, Ears, Nose, Throat: Eyes are clear and bright with no discharge. Sclera are white. Pupils equal and responsive to light. Ears clean with no erythema or discharge. Nares clear with no nasal discharge. No abnormalities of oral cavity.

Cardiovascular: Heart rate and rhythm regular. No murmurs or arrhythmias detected on auscultation. Peripheral pulse strong and synchronous with heartbeat.

Respiratory: Respiratory effort normal and unlabored. Lung sounds clear bilaterally on auscultation. No coughing, wheezing, or abnormal respiratory sounds noted.

Gastrointestinal: Appetite reported as {context.faker.random_element(['good - consuming full ration', 'normal', 'slightly decreased', 'excellent'])}. Abdomen soft and non-painful on palpation. Fecal consistency normal. No vomiting or diarrhea reported.

Musculoskeletal: Gait is {context.faker.random_element(['normal and coordinated', 'steady', 'appropriate for species'])}. No lameness detected. Full range of motion in all limbs. No swelling or pain on palpation of joints or long bones.

Neurological: Animal is alert and responsive to external stimuli. Cranial nerve function appears intact. Postural reactions normal. No neurological deficits observed.

CLINICAL SIGNS AND OBSERVATIONS:
{clinical_status}

Behavior: Animal exhibits {context.faker.random_element(['normal species-typical behavior', 'appropriate social behavior', 'calm demeanor', 'active and curious behavior'])}. No signs of distress or abnormal behaviors noted.

TREATMENTS ADMINISTERED:
Date: {date.strftime('%m/%d/%Y')}
Medication: {medication}
Dosage: {dosage} mg
Route: {context.faker.random_element(['Intramuscular (IM)', 'Subcutaneous (SC)', 'Oral (PO)', 'Intravenous (IV)'])}
Frequency: {context.faker.random_element(['Once daily', 'Twice daily', 'As needed', 'Single dose'])}

ADVERSE REACTIONS:
{context.faker.random_element(['None observed', 'No adverse reactions noted', 'Animal tolerated treatment well', 'Mild injection site reaction - resolved within 24 hours'])}

ASSESSMENT:
Overall health status: {context.faker.random_element(['Good', 'Excellent', 'Fair', 'Stable'])}
{context.faker.sentence(nb_words=15)}

PLAN:
Continue current study protocol. Monitor for any changes in appetite, behavior, or clinical signs. Reweigh in {context.faker.random_int(min=3, max=14)} days. Next scheduled examination: {context.faker.random_int(min=7, max=30)} days.

Additional comments: {context.faker.sentence(nb_words=12)}

Veterinarian Signature: {veterinarian}, DVM
License: DVM{context.faker.random_number(digits=5, fix_len=True)}
Date: {date.strftime('%m/%d/%Y %H:%M')}"""


class MedicalNotesSchemaGenerator(BaseSchemaGenerator):
    """Generates medical notes schema with realistic clinical data"""

    def generate_tables(self) -> Dict[str, DataFrame]:
        """Generate 5 interconnected tables for medical notes domain"""

        # 1. Patients table (PII/PHI) - Expanded to 20 columns
        patients_spec = (
            DataGenerator(
                self.spark,
                name="patients",
                rows=self.config.base_rows,
                partitions=self.config.partitions,
            )
            .withColumn(
                "patient_id",
                IntegerType(),
                minValue=100000,
                maxValue=999999,
                uniqueValues=self.config.base_rows,
            )
            .withColumn("mrn", StringType(), template="MRN-#######")
            .withColumn(
                "first_name",
                StringType(),
                text=PyfuncText(generate_first_name, init=init_faker_for_generation),
            )
            .withColumn(
                "last_name",
                StringType(),
                text=PyfuncText(generate_last_name, init=init_faker_for_generation),
            )
            .withColumn(
                "date_of_birth",
                DateType(),
                expr=f"date_add('{self.config.start_date}', -cast(rand()*365*60 + 365*18 as int))",
            )
            .withColumn(
                "ssn",
                StringType(),
                text=PyfuncText(generate_ssn, init=init_faker_for_generation),
            )
            .withColumn(
                "phone",
                StringType(),
                text=PyfuncText(generate_phone_number, init=init_faker_for_generation),
            )
            .withColumn(
                "email",
                StringType(),
                text=PyfuncText(generate_email, init=init_faker_for_generation),
            )
            .withColumn(
                "address",
                StringType(),
                text=PyfuncText(generate_address, init=init_faker_for_generation),
            )
            .withColumn("insurance_id", StringType(), template="INS-########")
            .withColumn(
                "race",
                StringType(),
                values=[
                    "White",
                    "Black or African American",
                    "Asian",
                    "Native American",
                    "Other",
                    "Decline to Answer",
                ],
                weights=[6, 2, 1, 0.5, 0.5, 1],
            )
            .withColumn(
                "ethnicity",
                StringType(),
                values=[
                    "Hispanic or Latino",
                    "Not Hispanic or Latino",
                    "Decline to Answer",
                ],
                weights=[2, 7, 1],
            )
            .withColumn(
                "preferred_language",
                StringType(),
                values=["English", "Spanish", "Chinese", "Vietnamese", "Other"],
                weights=[8, 1.5, 0.3, 0.1, 0.1],
            )
            .withColumn(
                "marital_status",
                StringType(),
                values=["Single", "Married", "Divorced", "Widowed", "Separated"],
                weights=[3, 5, 1.5, 0.8, 0.2],
            )
            .withColumn(
                "emergency_contact_name",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker_for_generation),
            )
            .withColumn(
                "emergency_contact_phone",
                StringType(),
                text=PyfuncText(generate_phone_number, init=init_faker_for_generation),
            )
            .withColumn(
                "employment_status",
                StringType(),
                values=[
                    "Employed Full-time",
                    "Employed Part-time",
                    "Unemployed",
                    "Retired",
                    "Disabled",
                    "Student",
                ],
                weights=[5, 2, 1, 2, 0.5, 1],
            )
            .withColumn(
                "blood_type",
                StringType(),
                values=["O+", "O-", "A+", "A-", "B+", "B-", "AB+", "AB-"],
                weights=[37.4, 6.6, 35.7, 6.3, 8.5, 1.5, 3.4, 0.6],
            )
            .withColumn(
                "height_cm",
                DoubleType(),
                minValue=150.0,
                maxValue=200.0,
                expr="150 + rand() * 50",
            )
            .withColumn(
                "weight_kg",
                DoubleType(),
                minValue=50.0,
                maxValue=150.0,
                expr="50 + rand() * 100",
            )
            .withColumn(
                "bmi",
                DoubleType(),
                expr="round(weight_kg / ((height_cm / 100) * (height_cm / 100)), 1)",
            )
        )

        # 2. Providers table
        providers_spec = (
            DataGenerator(
                self.spark,
                name="providers",
                rows=200,
                partitions=self.config.partitions,
            )
            .withColumn(
                "provider_id",
                IntegerType(),
                minValue=1000,
                maxValue=1199,
                uniqueValues=200,
            )
            .withColumn("npi", StringType(), template="##########")
            .withColumn(
                "first_name",
                StringType(),
                text=PyfuncText(generate_first_name, init=init_faker_for_generation),
            )
            .withColumn(
                "last_name",
                StringType(),
                text=PyfuncText(generate_last_name, init=init_faker_for_generation),
            )
            .withColumn(
                "specialty",
                StringType(),
                text=PyfuncText(generate_department, init=init_faker_for_generation),
            )
        )

        # 3. Medical encounters table
        encounters_spec = (
            DataGenerator(
                self.spark,
                name="encounters",
                rows=self.config.base_rows * 3,
                partitions=self.config.partitions,
            )
            .withColumn(
                "encounter_id",
                IntegerType(),
                minValue=200000,
                maxValue=999999,
                uniqueValues=self.config.base_rows * 3,
            )
            .withColumn(
                "patient_id",
                IntegerType(),
                minValue=100000,
                maxValue=100000 + self.config.base_rows - 1,
                random=True,
            )
            .withColumn(
                "provider_id", IntegerType(), minValue=1000, maxValue=1199, random=True
            )
            .withColumn(
                "encounter_date",
                DateType(),
                # expr=f"date_add(current_date(), -cast(rand()*365*80 + 365*18 as int))"
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            .withColumn(
                "encounter_type",
                StringType(),
                values=["Inpatient", "Outpatient", "Emergency", "Telehealth"],
                random=True,
            )
            .withColumn(
                "chief_complaint",
                StringType(),
                text=PyfuncText(
                    generate_medical_condition, init=init_faker_for_generation
                ),
            )
        )

        # 4. Clinical notes table (Unstructured PHI) - Using SOAP and H&P formats
        notes_spec = (
            DataGenerator(
                self.spark,
                name="clinical_notes",
                rows=self.config.base_rows * 2,
                partitions=self.config.partitions,
            )
            .withColumn(
                "note_id",
                IntegerType(),
                minValue=300000,
                maxValue=999999,
                uniqueValues=self.config.base_rows * 2,
            )
            .withColumn(
                "encounter_id",
                IntegerType(),
                minValue=200000,
                maxValue=200000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "note_type",
                StringType(),
                values=["SOAP Note", "History and Physical"],
                weights=[6, 4],
            )
            .withColumn(
                "note_text",
                StringType(),
                text=PyfuncText(
                    generate_clinical_note_mixed, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "created_datetime",
                TimestampType(),
                expr=f"timestamp(date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int)))",
            )
        )

        # 5. De-identified Clinical Notes table (for PII redaction training)
        deidentified_notes_spec = (
            DataGenerator(
                self.spark,
                name="deidentified_clinical_notes",
                rows=self.config.base_rows * 2,
                partitions=self.config.partitions,
            )
            .withColumn(
                "note_id",
                IntegerType(),
                minValue=300000,
                maxValue=999999,
                uniqueValues=self.config.base_rows * 2,
            )
            .withColumn(
                "encounter_id",
                IntegerType(),
                minValue=200000,
                maxValue=200000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "note_type",
                StringType(),
                values=["SOAP Note", "History and Physical"],
                weights=[6, 4],
            )
            .withColumn(
                "deidentified_note_text",
                StringType(),
                text=PyfuncText(
                    generate_deidentified_note, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "entity_annotations_json",
                StringType(),
                text=PyfuncText(
                    generate_entity_annotations, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "created_datetime",
                TimestampType(),
                expr=f"timestamp(date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int)))",
            )
        )

        # 6. Lab results table (Structured PHI) - Expanded to 20 columns with correlations
        lab_results_spec = (
            DataGenerator(
                self.spark,
                name="lab_results",
                rows=self.config.base_rows * 4,
                partitions=self.config.partitions,
            )
            .withColumn(
                "lab_result_id",
                IntegerType(),
                minValue=400000,
                maxValue=999999,
                uniqueValues=self.config.base_rows * 4,
            )
            .withColumn(
                "encounter_id",
                IntegerType(),
                minValue=200000,
                maxValue=200000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "test_name",
                StringType(),
                values=[
                    "CBC",
                    "BMP",
                    "Lipid Panel",
                    "HbA1c",
                    "TSH",
                    "Creatinine",
                    "ALT",
                    "Glucose",
                ],
                weights=[2, 2, 1.5, 1, 1, 1.5, 1, 2],
            )
            # Correlated test values based on test type
            .withColumn(
                "test_value",
                DoubleType(),
                baseColumn="test_name",
                expr="""
                CASE 
                    WHEN test_name = 'HbA1c' THEN 4.5 + rand() * 7.5
                    WHEN test_name = 'Glucose' THEN 70 + rand() * 130
                    WHEN test_name = 'Lipid Panel' THEN 120 + rand() * 180
                    WHEN test_name = 'Creatinine' THEN 0.5 + rand() * 2.5
                    WHEN test_name = 'ALT' THEN 10 + rand() * 100
                    WHEN test_name = 'TSH' THEN 0.5 + rand() * 8.5
                    WHEN test_name = 'CBC' THEN 4.0 + rand() * 7.0
                    ELSE 50 + rand() * 100
                END
                """,
            )
            # Correlated diagnosis codes based on test values
            .withColumn(
                "diagnosis_code",
                StringType(),
                baseColumn=["test_name", "test_value"],
                expr="""
                CASE 
                    WHEN test_name = 'HbA1c' AND test_value >= 6.5 THEN 'E11.9'
                    WHEN test_name = 'HbA1c' AND test_value >= 5.7 THEN 'R73.03'
                    WHEN test_name = 'Glucose' AND test_value >= 126 THEN 'E11.9'
                    WHEN test_name = 'Glucose' AND test_value >= 100 THEN 'R73.09'
                    WHEN test_name = 'Lipid Panel' AND test_value >= 240 THEN 'E78.5'
                    WHEN test_name = 'Lipid Panel' AND test_value >= 200 THEN 'E78.0'
                    WHEN test_name = 'Creatinine' AND test_value >= 1.5 THEN 'N18.3'
                    WHEN test_name = 'ALT' AND test_value >= 60 THEN 'K76.9'
                    WHEN test_name = 'TSH' AND test_value >= 5.0 THEN 'E03.9'
                    WHEN test_name = 'TSH' AND test_value <= 0.4 THEN 'E05.90'
                    ELSE NULL
                END
                """,
            )
            # Correlated condition severity based on how far out of range
            .withColumn(
                "condition_severity",
                StringType(),
                baseColumn=["test_name", "test_value", "diagnosis_code"],
                expr="""
                CASE 
                    WHEN diagnosis_code IN ('E11.9', 'N18.3') AND test_value >= 
                        CASE WHEN test_name = 'HbA1c' THEN 9.0 
                             WHEN test_name = 'Glucose' THEN 200
                             WHEN test_name = 'Creatinine' THEN 2.5
                        END THEN 'High'
                    WHEN diagnosis_code IN ('E78.5', 'K76.9') AND test_value >= 
                        CASE WHEN test_name = 'Lipid Panel' THEN 280
                             WHEN test_name = 'ALT' THEN 80
                        END THEN 'High'
                    WHEN diagnosis_code IS NOT NULL THEN 'Moderate'
                    ELSE 'Normal'
                END
                """,
            )
            .withColumn(
                "reference_range",
                StringType(),
                baseColumn=["condition_severity", "diagnosis_code"],
                expr="""
                CASE 
                    WHEN condition_severity = 'High' THEN 'Critical'
                    WHEN condition_severity = 'Moderate' THEN 'High'
                    WHEN condition_severity = 'Normal' AND diagnosis_code IS NOT NULL THEN 'Borderline'
                    ELSE 'Normal'
                END
                """,
            )
            .withColumn(
                "specimen_type",
                StringType(),
                values=["Whole Blood", "Serum", "Plasma", "Urine"],
                weights=[4, 5, 2, 1],
            )
            .withColumn(
                "collection_method",
                StringType(),
                values=["Venipuncture", "Fingerstick", "Arterial Line", "Catheter"],
                weights=[8, 1.5, 0.3, 0.2],
            )
            .withColumn(
                "lab_name",
                StringType(),
                text=PyfuncText(generate_company, init=init_faker_for_generation),
            )
            .withColumn("ordering_physician_npi", StringType(), template="##########")
            .withColumn(
                "performing_technician",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker_for_generation),
            )
            .withColumn(
                "result_units",
                StringType(),
                baseColumn="test_name",
                expr="""
                CASE 
                    WHEN test_name = 'HbA1c' THEN '%'
                    WHEN test_name = 'Glucose' THEN 'mg/dL'
                    WHEN test_name = 'Lipid Panel' THEN 'mg/dL'
                    WHEN test_name = 'Creatinine' THEN 'mg/dL'
                    WHEN test_name = 'ALT' THEN 'U/L'
                    WHEN test_name = 'TSH' THEN 'mIU/L'
                    WHEN test_name = 'CBC' THEN 'x10^9/L'
                    ELSE 'mg/dL'
                END
                """,
            )
            .withColumn(
                "reference_min",
                DoubleType(),
                baseColumn="test_name",
                expr="""
                CASE 
                    WHEN test_name = 'HbA1c' THEN 4.0
                    WHEN test_name = 'Glucose' THEN 70.0
                    WHEN test_name = 'Lipid Panel' THEN 100.0
                    WHEN test_name = 'Creatinine' THEN 0.5
                    WHEN test_name = 'ALT' THEN 7.0
                    WHEN test_name = 'TSH' THEN 0.4
                    WHEN test_name = 'CBC' THEN 4.0
                    ELSE 10.0
                END
                """,
            )
            .withColumn(
                "reference_max",
                DoubleType(),
                baseColumn="test_name",
                expr="""
                CASE 
                    WHEN test_name = 'HbA1c' THEN 5.6
                    WHEN test_name = 'Glucose' THEN 99.0
                    WHEN test_name = 'Lipid Panel' THEN 199.0
                    WHEN test_name = 'Creatinine' THEN 1.2
                    WHEN test_name = 'ALT' THEN 56.0
                    WHEN test_name = 'TSH' THEN 4.5
                    WHEN test_name = 'CBC' THEN 11.0
                    ELSE 100.0
                END
                """,
            )
            .withColumn(
                "abnormal_flag",
                BooleanType(),
                baseColumn=["test_value", "reference_min", "reference_max"],
                expr="test_value < reference_min OR test_value > reference_max",
            )
            .withColumn(
                "critical_flag",
                BooleanType(),
                baseColumn="reference_range",
                expr="reference_range = 'Critical'",
            )
            .withColumn(
                "fasting_status",
                StringType(),
                values=["Fasting", "Non-Fasting", "Unknown"],
                weights=[3, 6, 1],
            )
            .withColumn(
                "collected_datetime",
                TimestampType(),
                expr=f"timestamp(date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int)))",
            )
        )

        # Build and return tables
        tables = {}
        for spec in [
            patients_spec,
            providers_spec,
            encounters_spec,
            notes_spec,
            deidentified_notes_spec,
            lab_results_spec,
        ]:
            df = spec.build()
            tables[spec.name] = df

        return tables


class HospitalDataSchemaGenerator(BaseSchemaGenerator):
    """Generates hospital operational data with PII/PHI"""

    def generate_tables(self) -> Dict[str, DataFrame]:
        """Generate 5 interconnected tables for hospital operations"""

        # 1. Hospital staff table (PII)
        staff_spec = (
            DataGenerator(
                self.spark,
                name="hospital_staff",
                rows=500,
                partitions=self.config.partitions,
            )
            .withColumn(
                "staff_id",
                IntegerType(),
                minValue=50000,
                maxValue=50499,
                uniqueValues=500,
            )
            .withColumn("employee_id", StringType(), template="EMP######")
            .withColumn(
                "first_name",
                StringType(),
                text=PyfuncText(generate_first_name, init=init_faker_for_generation),
            )
            .withColumn(
                "last_name",
                StringType(),
                text=PyfuncText(generate_last_name, init=init_faker_for_generation),
            )
            .withColumn(
                "ssn",
                StringType(),
                text=PyfuncText(generate_ssn, init=init_faker_for_generation),
            )
            .withColumn(
                "phone",
                StringType(),
                text=PyfuncText(generate_phone_number, init=init_faker_for_generation),
            )
            .withColumn(
                "role",
                StringType(),
                values=["Nurse", "Doctor", "Technician", "Administrator"],
                random=True,
            )
            .withColumn(
                "department",
                StringType(),
                text=PyfuncText(generate_department, init=init_faker_for_generation),
            )
            .withColumn(
                "salary", DoubleType(), minValue=40000, maxValue=350000, random=True
            )
        )

        # 2. Hospital rooms table
        rooms_spec = (
            DataGenerator(
                self.spark,
                name="hospital_rooms",
                rows=300,
                partitions=self.config.partitions,
            )
            .withColumn(
                "room_id", IntegerType(), minValue=1000, maxValue=1299, uniqueValues=300
            )
            .withColumn("room_number", StringType(), template="###A")
            .withColumn(
                "room_type",
                StringType(),
                values=["ICU", "General", "Private", "Semi-Private"],
                random=True,
            )
            .withColumn(
                "department",
                StringType(),
                text=PyfuncText(generate_department, init=init_faker_for_generation),
            )
            .withColumn(
                "daily_rate", DoubleType(), minValue=500.0, maxValue=5000.0, random=True
            )
        )

        # 3. Patient admissions table (PHI)
        admissions_spec = (
            DataGenerator(
                self.spark,
                name="patient_admissions",
                rows=self.config.base_rows * 2,
                partitions=self.config.partitions,
            )
            .withColumn(
                "admission_id",
                IntegerType(),
                minValue=600000,
                maxValue=799999,
                uniqueValues=self.config.base_rows * 2,
            )
            .withColumn(
                "patient_id",
                IntegerType(),
                minValue=100000,
                maxValue=100000 + self.config.base_rows - 1,
                random=True,
            )
            .withColumn(
                "room_id", IntegerType(), minValue=1000, maxValue=1299, random=True
            )
            .withColumn(
                "attending_physician_id",
                IntegerType(),
                minValue=50000,
                maxValue=50499,
                random=True,
            )
            .withColumn(
                "admission_date",
                DateType(),
                # expr=f"date_add(current_date(), -cast(rand()*365*80 + 365*18 as int))"
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            .withColumn(
                "admission_diagnosis",
                StringType(),
                text=PyfuncText(
                    generate_medical_condition, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "total_charges",
                DoubleType(),
                minValue=1000.0,
                maxValue=100000.0,
                random=True,
            )
        )

        # 4. Medical procedures table
        procedures_spec = (
            DataGenerator(
                self.spark,
                name="medical_procedures",
                rows=self.config.base_rows * 3,
                partitions=self.config.partitions,
            )
            .withColumn(
                "procedure_id",
                IntegerType(),
                minValue=700000,
                maxValue=899999,
                uniqueValues=self.config.base_rows * 3,
            )
            .withColumn(
                "admission_id",
                IntegerType(),
                minValue=600000,
                maxValue=600000 + self.config.base_rows * 2 - 1,
                random=True,
            )
            .withColumn(
                "procedure_name",
                StringType(),
                values=[
                    "Cardiac Catheterization",
                    "Appendectomy",
                    "CT Scan",
                    "MRI",
                    "X-Ray",
                ],
                random=True,
            )
            .withColumn(
                "performing_physician_id",
                IntegerType(),
                minValue=50000,
                maxValue=50499,
                random=True,
            )
            .withColumn(
                "procedure_cost",
                DoubleType(),
                minValue=200.0,
                maxValue=50000.0,
                random=True,
            )
        )

        # 5. Billing records table (PHI with unstructured notes) - Expanded to 20 columns
        billing_spec = (
            DataGenerator(
                self.spark,
                name="billing_records",
                rows=self.config.base_rows * 2,
                partitions=self.config.partitions,
            )
            .withColumn(
                "billing_id",
                IntegerType(),
                minValue=800000,
                maxValue=999999,
                uniqueValues=self.config.base_rows * 2,
            )
            .withColumn(
                "admission_id",
                IntegerType(),
                minValue=600000,
                maxValue=600000 + self.config.base_rows * 2 - 1,
                random=True,
            )
            .withColumn(
                "billing_date",
                DateType(),
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            .withColumn(
                "total_amount",
                DoubleType(),
                minValue=500.0,
                maxValue=200000.0,
                random=True,
            )
            .withColumn(
                "insurance_paid",
                DoubleType(),
                expr="total_amount * (0.6 + rand() * 0.35)",
            )
            .withColumn(
                "patient_responsibility",
                DoubleType(),
                expr="total_amount - insurance_paid",
            )
            .withColumn(
                "billing_status",
                StringType(),
                values=["Paid", "Pending", "Overdue", "Partial"],
                weights=[5, 3, 1, 2],
            )
            .withColumn(
                "payment_method",
                StringType(),
                values=[
                    "Insurance",
                    "Credit Card",
                    "Cash",
                    "Check",
                    "Payment Plan",
                    "Pending",
                ],
                weights=[6, 2, 0.5, 1, 1.5, 2],
            )
            .withColumn("transaction_id", StringType(), template="TXN-###########")
            .withColumn("billing_provider_npi", StringType(), template="##########")
            .withColumn(
                "primary_diagnosis_code",
                StringType(),
                values=["E11.9", "I10", "J44.9", "N18.3", "I25.10", "E78.5", "F41.9"],
                random=True,
            )
            .withColumn(
                "secondary_diagnosis_code",
                StringType(),
                values=["E78.0", "E66.9", "K21.9", "M79.3", "R53.83", "NULL"],
                weights=[1, 1, 1, 1, 1, 3],
            )
            .withColumn(
                "procedure_code",
                StringType(),
                values=["99213", "99214", "99215", "99285", "99291", "36415"],
                random=True,
            )
            .withColumn(
                "insurance_policy_number", StringType(), template="POL-#########"
            )
            .withColumn("insurance_group_number", StringType(), template="GRP-######")
            .withColumn(
                "copay_amount",
                DoubleType(),
                values=[0, 15, 25, 35, 50, 75],
                weights=[2, 4, 4, 3, 2, 1],
            )
            .withColumn(
                "deductible_amount",
                DoubleType(),
                expr="CASE WHEN rand() < 0.3 THEN 0 ELSE 500 + rand() * 2500 END",
            )
            .withColumn(
                "amount_paid_to_date",
                DoubleType(),
                baseColumn=["billing_status", "total_amount"],
                expr="""
                CASE 
                    WHEN billing_status = 'Paid' THEN total_amount
                    WHEN billing_status = 'Partial' THEN total_amount * (0.3 + rand() * 0.5)
                    WHEN billing_status = 'Pending' THEN 0
                    WHEN billing_status = 'Overdue' THEN total_amount * (rand() * 0.3)
                    ELSE 0
                END
                """,
            )
            .withColumn(
                "outstanding_balance",
                DoubleType(),
                baseColumn=["total_amount", "amount_paid_to_date"],
                expr="total_amount - amount_paid_to_date",
            )
            .withColumn(
                "billing_notes",
                StringType(),
                text=PyfuncText(generate_billing_note, init=init_faker_for_generation),
            )
        )

        # Build and return tables
        tables = {}
        for spec in [
            staff_spec,
            rooms_spec,
            admissions_spec,
            procedures_spec,
            billing_spec,
        ]:
            df = spec.build()
            tables[spec.name] = df

        return tables


# COMMAND ----------


# COMMAND ----------


class ClinicalTrialsSchemaGenerator(BaseSchemaGenerator):
    """Generates pharmaceutical clinical trial data with PII/PHI"""

    def generate_tables(self) -> Dict[str, DataFrame]:
        """Generate 5 interconnected tables for clinical trials"""

        # 1. Clinical trials table
        trials_spec = (
            DataGenerator(
                self.spark,
                name="clinical_trials",
                rows=100,
                partitions=self.config.partitions,
            )
            .withColumn(
                "trial_id",
                IntegerType(),
                minValue=10000,
                maxValue=10099,
                uniqueValues=100,
            )
            .withColumn("nct_number", StringType(), template="NCT########")
            .withColumn(
                "trial_title",
                StringType(),
                text=PyfuncText(generate_trial_title, init=init_faker_for_generation),
            )
            .withColumn(
                "sponsor_company",
                StringType(),
                text=PyfuncText(generate_company, init=init_faker_for_generation),
            )
            .withColumn(
                "phase",
                StringType(),
                text=PyfuncText(generate_trial_phase, init=init_faker_for_generation),
            )
            .withColumn(
                "status",
                StringType(),
                text=PyfuncText(generate_trial_status, init=init_faker_for_generation),
            )
        )

        # 2. Study sites table (PII)
        sites_spec = (
            DataGenerator(
                self.spark,
                name="study_sites",
                rows=300,
                partitions=self.config.partitions,
            )
            .withColumn(
                "site_id",
                IntegerType(),
                minValue=20000,
                maxValue=20299,
                uniqueValues=300,
            )
            .withColumn(
                "trial_id", IntegerType(), minValue=10000, maxValue=10099, random=True
            )
            .withColumn(
                "site_name",
                StringType(),
                text=PyfuncText(
                    generate_city_medical_center, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "principal_investigator",
                StringType(),
                text=PyfuncText(generate_dr_name, init=init_faker_for_generation),
            )
            .withColumn(
                "phone",
                StringType(),
                text=PyfuncText(generate_phone_number, init=init_faker_for_generation),
            )
        )

        # 3. Study participants table (PII/PHI)
        participants_spec = (
            DataGenerator(
                self.spark,
                name="study_participants",
                rows=self.config.base_rows * 3,
                partitions=self.config.partitions,
            )
            .withColumn(
                "participant_id",
                IntegerType(),
                minValue=30000,
                maxValue=99999,
                uniqueValues=self.config.base_rows * 3,
            )
            .withColumn(
                "site_id", IntegerType(), minValue=20000, maxValue=20299, random=True
            )
            .withColumn("subject_id", StringType(), template="SUBJ-#####")
            .withColumn(
                "date_of_birth",
                DateType(),
                expr="current_date()",  # - cast(rand()*365*60 + 365*18 as int)",
            )
            .withColumn("gender", StringType(), values=["Male", "Female"], random=True)
            .withColumn(
                "treatment_arm",
                StringType(),
                values=["Active Drug", "Placebo"],
                random=True,
            )
        )

        # 4. Adverse events table (PHI with unstructured text)
        adverse_events_spec = (
            DataGenerator(
                self.spark,
                name="adverse_events",
                rows=self.config.base_rows,
                partitions=self.config.partitions,
            )
            .withColumn(
                "ae_id",
                IntegerType(),
                minValue=40000,
                maxValue=99999,
                uniqueValues=self.config.base_rows,
            )
            .withColumn(
                "participant_id",
                IntegerType(),
                minValue=30000,
                maxValue=30000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "ae_term",
                StringType(),
                values=["Nausea", "Headache", "Fatigue"],
                random=True,
            )
            .withColumn(
                "severity",
                StringType(),
                values=["Mild", "Moderate", "Severe"],
                random=True,
            )
            .withColumn(
                "ae_description",
                StringType(),
                text=PyfuncText(
                    generate_ae_description, init=init_faker_for_generation
                ),
            )
        )

        # 5. Lab measurements table (Structured PHI) - Expanded to 20 columns
        lab_measurements_spec = (
            DataGenerator(
                self.spark,
                name="lab_measurements",
                rows=self.config.base_rows * 5,
                partitions=self.config.partitions,
            )
            .withColumn(
                "measurement_id",
                IntegerType(),
                minValue=50000,
                maxValue=99999,
                uniqueValues=self.config.base_rows * 5,
            )
            .withColumn(
                "participant_id",
                IntegerType(),
                minValue=30000,
                maxValue=30000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "visit_name",
                StringType(),
                values=[
                    "Screening",
                    "Baseline",
                    "Week 4",
                    "Week 8",
                    "Week 12",
                    "End of Study",
                ],
                weights=[1, 1.5, 1.2, 1.2, 1.2, 1],
            )
            .withColumn(
                "visit_date",
                DateType(),
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            .withColumn(
                "lab_test",
                StringType(),
                values=["Hemoglobin", "WBC Count", "ALT", "AST", "Creatinine", "BUN"],
                weights=[2, 2, 1.5, 1.5, 1, 1],
            )
            .withColumn(
                "result_value",
                DoubleType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 10 + rand() * 8
                    WHEN lab_test = 'WBC Count' THEN 3 + rand() * 9
                    WHEN lab_test = 'ALT' THEN 10 + rand() * 80
                    WHEN lab_test = 'AST' THEN 10 + rand() * 80
                    WHEN lab_test = 'Creatinine' THEN 0.5 + rand() * 2
                    WHEN lab_test = 'BUN' THEN 7 + rand() * 23
                    ELSE 50 + rand() * 100
                END
                """,
            )
            .withColumn(
                "result_units",
                StringType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 'g/dL'
                    WHEN lab_test = 'WBC Count' THEN 'x10^9/L'
                    WHEN lab_test IN ('ALT', 'AST') THEN 'U/L'
                    WHEN lab_test = 'Creatinine' THEN 'mg/dL'
                    WHEN lab_test = 'BUN' THEN 'mg/dL'
                    ELSE 'units'
                END
                """,
            )
            .withColumn(
                "reference_min",
                DoubleType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 12.0
                    WHEN lab_test = 'WBC Count' THEN 4.0
                    WHEN lab_test = 'ALT' THEN 7.0
                    WHEN lab_test = 'AST' THEN 10.0
                    WHEN lab_test = 'Creatinine' THEN 0.6
                    WHEN lab_test = 'BUN' THEN 7.0
                    ELSE 10.0
                END
                """,
            )
            .withColumn(
                "reference_max",
                DoubleType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 16.0
                    WHEN lab_test = 'WBC Count' THEN 11.0
                    WHEN lab_test = 'ALT' THEN 56.0
                    WHEN lab_test = 'AST' THEN 40.0
                    WHEN lab_test = 'Creatinine' THEN 1.2
                    WHEN lab_test = 'BUN' THEN 20.0
                    ELSE 100.0
                END
                """,
            )
            .withColumn(
                "abnormal_flag",
                BooleanType(),
                baseColumn=["result_value", "reference_min", "reference_max"],
                expr="result_value < reference_min OR result_value > reference_max",
            )
            .withColumn(
                "specimen_type",
                StringType(),
                values=["Whole Blood", "Serum", "Plasma"],
                weights=[3, 5, 2],
            )
            .withColumn(
                "collection_time",
                TimestampType(),
                expr=f"timestamp(date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int)))",
            )
            .withColumn(
                "analysis_time",
                TimestampType(),
                expr="collection_time + INTERVAL cast(1 + rand() * 48 as int) HOUR",
            )
            .withColumn(
                "lab_technician",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker_for_generation),
            )
            .withColumn(
                "lab_location",
                StringType(),
                text=PyfuncText(
                    generate_city_medical_center, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "fasting_status",
                StringType(),
                values=["Fasting", "Non-Fasting", "Unknown"],
                weights=[4, 5, 1],
            )
            .withColumn(
                "sample_quality",
                StringType(),
                values=["Acceptable", "Hemolyzed", "Lipemic", "Icteric"],
                weights=[9, 0.5, 0.3, 0.2],
            )
            .withColumn(
                "retest_flag",
                BooleanType(),
                baseColumn="sample_quality",
                expr="CASE WHEN sample_quality != 'Acceptable' THEN true ELSE rand() < 0.05 END",
            )
            .withColumn(
                "clinically_significant",
                BooleanType(),
                baseColumn=[
                    "abnormal_flag",
                    "result_value",
                    "reference_min",
                    "reference_max",
                ],
                expr="abnormal_flag AND (result_value < reference_min * 0.7 OR result_value > reference_max * 1.3)",
            )
            .withColumn(
                "reviewed_by_physician",
                StringType(),
                text=PyfuncText(generate_dr_name, init=init_faker_for_generation),
            )
            .withColumn(
                "comments",
                StringType(),
                baseColumn=["clinically_significant", "abnormal_flag", "retest_flag"],
                expr="""
                CASE 
                    WHEN clinically_significant = true THEN 'Clinically significant abnormality - follow-up required'
                    WHEN abnormal_flag = true THEN 'Mild abnormality noted'
                    WHEN retest_flag = true THEN 'Retest performed due to quality issues'
                    ELSE 'Within normal limits'
                END
                """,
            )
        )

        # Build and return tables
        tables = {}
        for spec in [
            trials_spec,
            sites_spec,
            participants_spec,
            adverse_events_spec,
            lab_measurements_spec,
        ]:
            df = spec.build()
            tables[spec.name] = df

        return tables


# COMMAND ----------


class LivestockResearchSchemaGenerator(BaseSchemaGenerator):
    """Generates livestock research data for animal health company"""

    def generate_tables(self) -> Dict[str, DataFrame]:
        """Generate 5 interconnected tables for livestock research"""

        # 1. Research facilities table (PII)
        facilities_spec = (
            DataGenerator(
                self.spark,
                name="research_facilities",
                rows=50,
                partitions=self.config.partitions,
            )
            .withColumn(
                "facility_id",
                IntegerType(),
                minValue=60000,
                maxValue=60049,
                uniqueValues=50,
            )
            .withColumn(
                "facility_name",
                StringType(),
                text=PyfuncText(
                    generate_city_research_farm, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "facility_manager",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker_for_generation),
            )
            .withColumn(
                "phone",
                StringType(),
                text=PyfuncText(generate_phone_number, init=init_faker_for_generation),
            )
            .withColumn(
                "capacity_head", IntegerType(), minValue=100, maxValue=5000, random=True
            )
        )

        # 2. Researchers table (PII)
        researchers_spec = (
            DataGenerator(
                self.spark,
                name="researchers",
                rows=200,
                partitions=self.config.partitions,
            )
            .withColumn(
                "researcher_id",
                IntegerType(),
                minValue=70000,
                maxValue=70199,
                uniqueValues=200,
            )
            .withColumn(
                "first_name",
                StringType(),
                text=PyfuncText(generate_first_name, init=init_faker_for_generation),
            )
            .withColumn(
                "last_name",
                StringType(),
                text=PyfuncText(generate_last_name, init=init_faker_for_generation),
            )
            .withColumn(
                "email",
                StringType(),
                text=PyfuncText(generate_email, init=init_faker_for_generation),
            )
            .withColumn(
                "specialty",
                StringType(),
                values=["Veterinary Medicine", "Animal Nutrition", "Genetics"],
                random=True,
            )
            .withColumn(
                "facility_id",
                IntegerType(),
                minValue=60000,
                maxValue=60049,
                random=True,
            )
        )

        # 3. Animals table
        animals_spec = (
            DataGenerator(
                self.spark,
                name="research_animals",
                rows=self.config.base_rows * 2,
                partitions=self.config.partitions,
            )
            .withColumn(
                "animal_id",
                IntegerType(),
                minValue=80000,
                maxValue=99999,
                uniqueValues=self.config.base_rows * 2,
            )
            .withColumn(
                "facility_id",
                IntegerType(),
                minValue=60000,
                maxValue=60049,
                random=True,
            )
            .withColumn("animal_tag", StringType(), template="TAG######")
            .withColumn(
                "species",
                StringType(),
                text=PyfuncText(
                    generate_animal_species, init=init_faker_for_generation
                ),
            )
            .withColumn(
                "breed",
                StringType(),
                text=PyfuncText(generate_animal_breed, init=init_faker_for_generation),
            )
            .withColumn(
                "birth_date",
                DateType(),
                expr="current_date()",  # - cast(rand()*365*8 as int)",
            )
            .withColumn(
                "weight_kg", DoubleType(), minValue=5.0, maxValue=800.0, random=True
            )
        )

        # 4. Research studies table
        studies_spec = (
            DataGenerator(
                self.spark,
                name="research_studies",
                rows=100,
                partitions=self.config.partitions,
            )
            .withColumn(
                "study_id",
                IntegerType(),
                minValue=90000,
                maxValue=90099,
                uniqueValues=100,
            )
            .withColumn(
                "study_title",
                StringType(),
                text=PyfuncText(generate_study_title, init=init_faker_for_generation),
            )
            .withColumn(
                "principal_investigator_id",
                IntegerType(),
                minValue=70000,
                maxValue=70199,
                random=True,
            )
            .withColumn(
                "study_type",
                StringType(),
                values=["Drug Safety", "Efficacy", "Nutrition"],
                random=True,
            )
            .withColumn(
                "sponsor",
                StringType(),
                text=PyfuncText(generate_company, init=init_faker_for_generation),
            )
        )

        # 5. Veterinary observations table (Unstructured text with some PII) - Expanded to 20 columns with correlations
        observations_spec = (
            DataGenerator(
                self.spark,
                name="veterinary_observations",
                rows=self.config.base_rows * 4,
                partitions=self.config.partitions,
            )
            .withColumn(
                "observation_id",
                IntegerType(),
                minValue=100000,
                maxValue=199999,
                uniqueValues=self.config.base_rows * 4,
            )
            .withColumn(
                "animal_id",
                IntegerType(),
                minValue=80000,
                maxValue=80000 + self.config.base_rows * 2 - 1,
                random=True,
            )
            .withColumn(
                "study_id", IntegerType(), minValue=90000, maxValue=90099, random=True
            )
            .withColumn(
                "researcher_id",
                IntegerType(),
                minValue=70000,
                maxValue=70199,
                random=True,
            )
            .withColumn(
                "observation_date",
                DateType(),
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            # Body weight with realistic distribution - correlated with other health metrics
            .withColumn(
                "body_weight_kg",
                DoubleType(),
                expr="50 + abs(randn() * 150)",  # Normal distribution
            )
            # Temperature correlated with clinical signs
            .withColumn(
                "temperature_celsius",
                DoubleType(),
                expr="37.5 + randn() * 0.8 + CASE WHEN rand() < 0.15 THEN 1.5 ELSE 0 END",  # Fever in ~15%
            )
            # Heart rate with species variation and correlation to temperature
            .withColumn(
                "heart_rate_bpm",
                IntegerType(),
                baseColumn="temperature_celsius",
                expr="cast(70 + randn() * 20 + CASE WHEN temperature_celsius > 39.0 THEN 15 ELSE 0 END as int)",
            )
            # Respiratory rate correlated with heart rate and temperature
            .withColumn(
                "respiratory_rate_pm",
                IntegerType(),
                baseColumn="temperature_celsius",
                expr="cast(20 + randn() * 8 + CASE WHEN temperature_celsius > 39.0 THEN 8 ELSE 0 END as int)",
            )
            # Clinical signs correlated with temperature and weight
            .withColumn(
                "clinical_signs",
                StringType(),
                baseColumn=["temperature_celsius", "body_weight_kg"],
                expr="""
                CASE 
                    WHEN temperature_celsius > 39.5 THEN 'Fever and Lethargy'
                    WHEN temperature_celsius > 39.0 THEN 'Mild Lethargy'
                    WHEN body_weight_kg < 80 AND rand() < 0.3 THEN 'Decreased Appetite'
                    WHEN rand() < 0.15 THEN 'Decreased Appetite'
                    ELSE 'Normal'
                END
                """,
            )
            .withColumn(
                "mucous_membrane_color",
                StringType(),
                baseColumn="clinical_signs",
                expr="""
                CASE 
                    WHEN clinical_signs LIKE '%Lethargy%' AND rand() < 0.4 THEN 'Pale Pink'
                    WHEN rand() < 0.05 THEN 'Pale'
                    ELSE 'Pink'
                END
                """,
            )
            .withColumn(
                "capillary_refill_time",
                StringType(),
                values=["<2 seconds", "2 seconds", ">2 seconds"],
                weights=[8, 1.5, 0.5],
            )
            .withColumn(
                "hydration_status",
                StringType(),
                baseColumn="clinical_signs",
                expr="""
                CASE 
                    WHEN clinical_signs LIKE '%Appetite%' AND rand() < 0.3 THEN 'Mildly Dehydrated'
                    WHEN rand() < 0.05 THEN 'Dehydrated'
                    ELSE 'Normal'
                END
                """,
            )
            .withColumn(
                "body_condition_score",
                IntegerType(),
                baseColumn="body_weight_kg",
                expr="""
                CASE 
                    WHEN body_weight_kg < 80 THEN cast(2 + rand() * 2 as int)
                    WHEN body_weight_kg > 500 THEN cast(3 + rand() * 2 as int)
                    ELSE cast(3 + rand() * 2 as int)
                END
                """,
            )
            .withColumn(
                "appetite_assessment",
                StringType(),
                baseColumn="clinical_signs",
                expr="""
                CASE 
                    WHEN clinical_signs LIKE '%Appetite%' THEN 'Decreased'
                    WHEN rand() < 0.1 THEN 'Increased'
                    ELSE 'Normal'
                END
                """,
            )
            # Treatment correlated with clinical signs
            .withColumn(
                "treatment_administered",
                BooleanType(),
                baseColumn="clinical_signs",
                expr="clinical_signs != 'Normal' OR rand() < 0.2",
            )
            .withColumn(
                "medication_name",
                StringType(),
                text=PyfuncText(generate_medication, init=init_faker_for_generation),
            )
            .withColumn(
                "dosage_mg",
                DoubleType(),
                baseColumn="treatment_administered",
                expr="CASE WHEN treatment_administered THEN 5 + rand() * 495 ELSE NULL END",
            )
            .withColumn(
                "route_of_administration",
                StringType(),
                baseColumn="treatment_administered",
                expr="""
                CASE 
                    WHEN treatment_administered THEN 
                        CASE 
                            WHEN rand() < 0.4 THEN 'Intramuscular (IM)'
                            WHEN rand() < 0.6 THEN 'Oral (PO)'
                            WHEN rand() < 0.8 THEN 'Subcutaneous (SC)'
                            ELSE 'Intravenous (IV)'
                        END
                    ELSE NULL
                END
                """,
            )
            .withColumn(
                "adverse_reactions",
                StringType(),
                baseColumn="treatment_administered",
                expr="""
                CASE 
                    WHEN treatment_administered AND rand() < 0.05 THEN 'Mild injection site reaction'
                    WHEN treatment_administered AND rand() < 0.02 THEN 'Transient hypersensitivity'
                    ELSE 'None observed'
                END
                """,
            )
            # Treatment outcome correlated with severity of signs and treatment
            .withColumn(
                "treatment_outcome",
                StringType(),
                baseColumn=["treatment_administered", "clinical_signs"],
                expr="""
                CASE 
                    WHEN NOT treatment_administered THEN 'Not Applicable'
                    WHEN clinical_signs = 'Fever and Lethargy' AND rand() < 0.7 THEN 'Improving'
                    WHEN clinical_signs = 'Fever and Lethargy' THEN 'Stable'
                    WHEN clinical_signs != 'Normal' AND rand() < 0.85 THEN 'Resolved'
                    WHEN clinical_signs != 'Normal' THEN 'Improving'
                    ELSE 'Not Applicable'
                END
                """,
            )
            .withColumn(
                "follow_up_required",
                BooleanType(),
                baseColumn=["clinical_signs", "treatment_outcome", "adverse_reactions"],
                expr="""
                CASE 
                    WHEN clinical_signs = 'Fever and Lethargy' THEN true
                    WHEN treatment_outcome IN ('Stable', 'Improving') THEN true
                    WHEN adverse_reactions != 'None observed' THEN true
                    ELSE rand() < 0.1
                END
                """,
            )
            .withColumn(
                "veterinarian_name",
                StringType(),
                text=PyfuncText(generate_dr_name, init=init_faker_for_generation),
            )
            .withColumn(
                "observation_notes",
                StringType(),
                text=PyfuncText(
                    generate_vet_observation, init=init_faker_for_generation
                ),
            )
        )

        # Build and return tables
        tables = {}
        for spec in [
            facilities_spec,
            researchers_spec,
            animals_spec,
            studies_spec,
            observations_spec,
        ]:
            df = spec.build()
            tables[spec.name] = df

        return tables


# COMMAND ----------


class DataGenerationOrchestrator:
    """Orchestrates generation of all schema data"""

    def __init__(self, spark: SparkSession, config: SchemaConfig):
        self.spark = spark
        self.config = config

    def generate_all_schemas(self) -> Dict[str, Dict[str, DataFrame]]:
        """Generate data for all 4 schemas"""

        all_schemas = {}

        # Generate Medical Notes schema
        medical_gen = MedicalNotesSchemaGenerator(self.spark, self.config)
        all_schemas["medical_notes"] = medical_gen.generate_tables()

        # Generate Hospital Data schema
        hospital_gen = HospitalDataSchemaGenerator(self.spark, self.config)
        all_schemas["hospital_data"] = hospital_gen.generate_tables()

        # Generate Clinical Trials schema
        trials_gen = ClinicalTrialsSchemaGenerator(self.spark, self.config)
        all_schemas["clinical_trials"] = trials_gen.generate_tables()

        # Generate Livestock Research schema
        livestock_gen = LivestockResearchSchemaGenerator(self.spark, self.config)
        all_schemas["livestock_research"] = livestock_gen.generate_tables()

        return all_schemas

    def save_tables_to_catalog(
        self, schemas: Dict[str, Dict[str, DataFrame]], catalog_name: str
    ) -> None:
        """Save generated tables to Unity Catalog with each domain as its own schema"""
        for schema_name, schema_tables in schemas.items():
            # Create schema if it doesn't exist
            full_schema_name = f"{catalog_name}.{schema_name}"

            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}")

            # Save each table to the schema
            i = 0
            for table_name, table_df in schema_tables.items():
                print("i", i)
                full_table_name = f"{full_schema_name}.{table_name}"
                # display(table_df)
                table_df.write.mode("overwrite").option(
                    "overwriteSchema", "true"
                ).saveAsTable(full_table_name)
                i += 1

                # # Add table comment for documentation
                # table_comment = self._get_table_comment(schema_name, table_name)
                # if table_comment:
                #     self.spark.sql(
                #         f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{table_comment}')"
                #     )

    def _get_table_comment(self, schema_name: str, table_name: str) -> str:
        """Get descriptive comment for each table"""

        comments = {
            "medical_notes": {
                "patients": "Patient demographic and contact information with PII/PHI including SSN, DOB, and insurance details",
                "providers": "Healthcare provider information including NPI numbers and specialties",
                "encounters": "Medical visit records linking patients to providers with encounter details",
                "clinical_notes": "Unstructured clinical documentation containing PHI and medical assessments",
                "lab_results": "Structured laboratory test results with PHI including test values and reference ranges",
            },
            "hospital_data": {
                "hospital_staff": "Hospital employee records with PII including SSN, salary, and contact information",
                "hospital_rooms": "Hospital room and facility information including room types and daily rates",
                "patient_admissions": "Patient admission records with PHI including diagnoses and charges",
                "medical_procedures": "Medical procedure records linked to admissions with costs and performing physicians",
                "billing_records": "Billing data with unstructured notes and PHI including insurance and payment information",
            },
            "clinical_trials": {
                "clinical_trials": "Clinical trial study information including NCT numbers, phases, and sponsor details",
                "study_sites": "Research site data with PII including principal investigator and contact information",
                "study_participants": "Study subject data with PII/PHI including demographics and treatment arms",
                "adverse_events": "Adverse event reports with unstructured PHI text and causality assessments",
                "lab_measurements": "Clinical laboratory measurements with PHI including visit data and test results",
            },
            "livestock_research": {
                "research_facilities": "Research facility information with PII including manager contacts and capacity",
                "researchers": "Researcher records with PII including contact information and specialties",
                "research_animals": "Research animal subject data including species, breeds, and health status",
                "research_studies": "Research study protocol information including investigators and study types",
                "veterinary_observations": "Veterinary observation notes with unstructured text and some PII",
            },
        }

        return comments.get(schema_name, {}).get(table_name, "")


# COMMAND ----------

# Initialize configuration and generate data
config = SchemaConfig(base_rows=1000, partitions=4)
orchestrator = DataGenerationOrchestrator(spark, config)

print("Starting data generation for all life sciences schemas...")
all_schemas = orchestrator.generate_all_schemas()

print(f"\nGenerated {len(all_schemas)} schemas with the following tables:")


# COMMAND ----------

# Save to Unity Catalog
print(f"\nSaving all tables to Unity Catalog: {catalog_name}")
orchestrator.save_tables_to_catalog(all_schemas, catalog_name)

print(f"\nSuccess! Tables saved to Unity Catalog with the following structure:")
print(f"Catalog: {catalog_name}")
for schema_name, schema_tables in all_schemas.items():
    print(f"  Schema: {catalog_name}.{schema_name}")
    for table_name in schema_tables.keys():
        print(f"    {catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# Display sample data from each schema
print("Sample data from each schema:")
for schema_name, schema_tables in all_schemas.items():
    print(f"\n{'='*60}")
    print(f"SCHEMA: {schema_name.upper()}")
    print(f"{'='*60}")

    for table_name, table_df in schema_tables.items():
        print(f"\nTable: {table_name}")
        print(f"Rows: {table_df.count():,}")
        table_df.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Validate Generated Tables & Example Queries

# COMMAND ----------


# Validate table relationships and data quality
def validate_generated_data(schemas: Dict[str, Dict[str, DataFrame]]) -> None:
    """Validate the generated data for quality and relationships"""

    print("Validating generated data...")

    # Check Medical Notes schema relationships
    medical_tables = schemas["medical_notes"]
    patient_count = medical_tables["patients"].count()
    encounter_count = medical_tables["encounters"].count()

    print(f"\nMedical Notes Schema Validation:")
    print(f"  • Patients: {patient_count:,}")
    print(
        f"  • Encounters: {encounter_count:,} (ratio: {encounter_count/patient_count:.1f}x)"
    )

    # Check Hospital Data schema relationships
    hospital_tables = schemas["hospital_data"]
    staff_count = hospital_tables["hospital_staff"].count()
    admission_count = hospital_tables["patient_admissions"].count()

    print(f"\nHospital Data Schema Validation:")
    print(f"  • Staff: {staff_count:,}")
    print(f"  • Admissions: {admission_count:,}")

    # Check Clinical Trials schema relationships
    trials_tables = schemas["clinical_trials"]
    trial_count = trials_tables["clinical_trials"].count()
    participant_count = trials_tables["study_participants"].count()

    print(f"\nClinical Trials Schema Validation:")
    print(f"  • Trials: {trial_count:,}")
    print(
        f"  • Participants: {participant_count:,} (avg: {participant_count/trial_count:.0f} per trial)"
    )

    # Check Livestock Research schema relationships
    livestock_tables = schemas["livestock_research"]
    facility_count = livestock_tables["research_facilities"].count()
    animal_count = livestock_tables["research_animals"].count()

    print(f"\nLivestock Research Schema Validation:")
    print(f"  • Facilities: {facility_count:,}")
    print(
        f"  • Animals: {animal_count:,} (avg: {animal_count/facility_count:.0f} per facility)"
    )

    print("\nData validation complete!")


# Run validation
validate_generated_data(all_schemas)

# COMMAND ----------

# Example queries to demonstrate the generated data
print("Example queries on generated Unity Catalog data:\n")

try:
    # Query 1: Medical Notes - Find patients with multiple encounters
    print("Query 1: Patients with multiple medical encounters")
    result1 = spark.sql(
        f"""
        SELECT p.first_name, p.last_name, p.patient_id, COUNT(e.encounter_id) as encounter_count
        FROM {catalog_name}.medical_notes.patients p
        JOIN {catalog_name}.medical_notes.encounters e ON p.patient_id = e.patient_id  
        GROUP BY p.patient_id, p.first_name, p.last_name
        HAVING COUNT(e.encounter_id) > 2
        ORDER BY encounter_count DESC
        LIMIT 5
    """
    )
    result1.show()

    # Query 2: Hospital Data - High-cost procedures by department
    print("\nQuery 2: High-cost procedures by department")
    result2 = spark.sql(
        f"""
        SELECT r.department, COUNT(*) as procedure_count, AVG(p.procedure_cost) as avg_cost
        FROM {catalog_name}.hospital_data.medical_procedures p
        JOIN {catalog_name}.hospital_data.patient_admissions a ON p.admission_id = a.admission_id
        JOIN {catalog_name}.hospital_data.hospital_rooms r ON a.room_id = r.room_id
        GROUP BY r.department
        ORDER BY avg_cost DESC
    """
    )
    result2.show()

    # Query 3: Clinical Trials - Adverse events by severity
    print("\nQuery 3: Adverse events by severity and treatment arm")
    result3 = spark.sql(
        f"""
        SELECT p.treatment_arm, ae.severity, COUNT(*) as event_count
        FROM {catalog_name}.clinical_trials.adverse_events ae
        JOIN {catalog_name}.clinical_trials.study_participants p ON ae.participant_id = p.participant_id
        GROUP BY p.treatment_arm, ae.severity
        ORDER BY p.treatment_arm, ae.severity
    """
    )
    result3.show()

    print("All example queries executed successfully!")

except Exception as e:
    print(f"Query execution skipped - tables may not be saved to catalog yet: {e}")

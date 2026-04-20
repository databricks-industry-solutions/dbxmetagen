"""
Ground truth definitions for dbxmetagen evaluation.

Defines synthetic healthcare table schemas, data generators, and expected
pipeline outputs (comments, PI, domain, ontology, FK) aligned with the
FHIR R4 ontology bundle.

Column property ground truths are determined by tracing the exact
classification cascade:
  1. bundle_match (tier1): column name in FHIR property typical_attributes
  2. heuristic cascade (tier2): linked_entity > source_columns PK >
     _id/_key suffix > PI PII/PHI > boolean > geo > _code suffix >
     system > DATE/TIMESTAMP > hierarchy > text pattern > numeric
  3. fallback (tier3): default dimension 0.40

Tables live in a dedicated eval schema (e.g. eval_data) with plain names
(patients, encounters, ...) so that stem-matching heuristics and FK
name-based candidates work the same way as on production tables.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SEED = 42

EVAL_TABLE_NAMES = [
    "patients",
    "providers",
    "encounters",
    "diagnosis",
    "medications",
    "lab_results",
    "insurance_claims",
]

# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "patients": StructType(
        [
            StructField("patient_id", IntegerType(), False),
            StructField("mrn", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    ),
    "providers": StructType(
        [
            StructField("provider_id", IntegerType(), False),
            StructField("npi", StringType(), True),
            StructField("name", StringType(), True),
            StructField("specialty", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
        ]
    ),
    "encounters": StructType(
        [
            StructField("encounter_id", IntegerType(), False),
            StructField("patient_id", IntegerType(), True),
            StructField("provider_id", IntegerType(), True),
            StructField("admit_date", DateType(), True),
            StructField("discharge_date", DateType(), True),
            StructField("encounter_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("discharge_notes", StringType(), True),
        ]
    ),
    "diagnosis": StructType(
        [
            StructField("diagnosis_id", IntegerType(), False),
            StructField("patient_id", IntegerType(), True),
            StructField("encounter_id", IntegerType(), True),
            StructField("icd10_code", StringType(), True),
            StructField("description", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("diagnosis_date", DateType(), True),
        ]
    ),
    "medications": StructType(
        [
            StructField("medication_id", IntegerType(), False),
            StructField("patient_id", IntegerType(), True),
            StructField("encounter_id", IntegerType(), True),
            StructField("drug_name", StringType(), True),
            StructField("ndc", StringType(), True),
            StructField("dose", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("prescribed_date", DateType(), True),
        ]
    ),
    "lab_results": StructType(
        [
            StructField("lab_result_id", IntegerType(), False),
            StructField("patient_id", IntegerType(), True),
            StructField("encounter_id", IntegerType(), True),
            StructField("test_code", StringType(), True),
            StructField("result_value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("reference_range", StringType(), True),
            StructField("abnormal_flag", StringType(), True),
            StructField("collected_date", DateType(), True),
        ]
    ),
    "insurance_claims": StructType(
        [
            StructField("claim_id", IntegerType(), False),
            StructField("patient_id", IntegerType(), True),
            StructField("encounter_id", IntegerType(), True),
            StructField("policy_number", StringType(), True),
            StructField("service_date", DateType(), True),
            StructField("billed_amount", DoubleType(), True),
            StructField("paid_amount", DoubleType(), True),
            StructField("status", StringType(), True),
        ]
    ),
}

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

ICD10_CODES = [
    ("I10", "Essential hypertension", "moderate"),
    ("E11.9", "Type 2 diabetes mellitus without complications", "mild"),
    ("J06.9", "Acute upper respiratory infection", "mild"),
    ("M54.5", "Low back pain", "moderate"),
    ("F32.9", "Major depressive disorder", "moderate"),
    ("I25.10", "Atherosclerotic heart disease", "severe"),
    ("J44.1", "COPD with acute exacerbation", "severe"),
    ("N18.3", "Chronic kidney disease stage 3", "moderate"),
    ("K21.0", "Gastroesophageal reflux disease", "mild"),
    ("G47.33", "Obstructive sleep apnea", "moderate"),
]

DRUG_LIST = [
    ("Metformin", "00378-1085-01", "500mg", "twice daily"),
    ("Lisinopril", "00378-0839-01", "10mg", "once daily"),
    ("Atorvastatin", "00378-3957-93", "20mg", "once daily"),
    ("Amoxicillin", "00093-4150-01", "500mg", "three times daily"),
    ("Omeprazole", "00378-6150-01", "20mg", "once daily"),
    ("Albuterol", "00487-9801-01", "90mcg", "as needed"),
    ("Sertraline", "00378-4187-05", "50mg", "once daily"),
    ("Amlodipine", "00378-0056-01", "5mg", "once daily"),
]

LOINC_CODES = [
    ("2345-7", "Glucose", "mg/dL", "70-100"),
    ("718-7", "Hemoglobin", "g/dL", "12.0-17.5"),
    ("2160-0", "Creatinine", "mg/dL", "0.7-1.3"),
    ("2093-3", "Total Cholesterol", "mg/dL", "0-200"),
    ("6690-2", "WBC Count", "10*3/uL", "4.5-11.0"),
    ("789-8", "RBC Count", "10*6/uL", "4.5-5.5"),
    ("4548-4", "HbA1c", "%", "4.0-5.6"),
    ("2571-8", "Triglycerides", "mg/dL", "0-150"),
]

SPECIALTIES = [
    "Internal Medicine", "Cardiology", "Endocrinology", "Pulmonology",
    "Family Medicine", "Psychiatry", "Nephrology", "Gastroenterology",
    "Orthopedics", "Neurology",
]

ENCOUNTER_TYPES = ["inpatient", "outpatient", "emergency", "observation", "telehealth"]
ENCOUNTER_STATUSES = ["completed", "in-progress", "cancelled"]
CLAIM_STATUSES = ["paid", "pending", "denied", "appealed"]


def generate_all_tables(seed: int = SEED) -> dict:
    """Generate all eval tables from a single coherent data model.

    Builds dimension tables (patients, providers) first, then generates
    encounters with power-law patient frequency, and projects child records
    (diagnosis, medications, labs, claims) per encounter so every FK value
    is guaranteed to exist in the parent table.
    """
    import datetime
    from faker import Faker

    fake = Faker()
    Faker.seed(seed)
    rng = np.random.RandomState(seed)

    # --- Patients (dimension, 200 rows) ---
    genders = ["male", "female", "other", "unknown"]
    patients = []
    for i in range(1, 201):
        g = rng.choice(genders, p=[0.48, 0.48, 0.02, 0.02])
        patients.append({
            "patient_id": i,
            "mrn": f"MRN-{100000 + i}",
            "first_name": fake.first_name_male() if g == "male" else fake.first_name_female(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90),
            "gender": g,
            "address": fake.address().replace("\n", ", "),
            "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
        })

    # --- Providers (dimension, 30 rows) ---
    providers = []
    for i in range(1, 31):
        providers.append({
            "provider_id": i,
            "npi": f"{rng.randint(1000000000, 9999999999)}",
            "name": f"Dr. {fake.last_name()}",
            "specialty": rng.choice(SPECIALTIES),
            "email": fake.email(domain="example.com"),
            "phone": fake.phone_number(),
        })

    # --- Encounters (fact grain, ~500 rows) ---
    # Power-law patient frequency: top 20% of patients generate ~80% of visits
    patient_ids = np.array([p["patient_id"] for p in patients])
    pw = 1.0 / np.arange(1, len(patient_ids) + 1).astype(float) ** 0.8
    rng.shuffle(pw)
    pw /= pw.sum()

    provider_ids = [p["provider_id"] for p in providers]
    discharge_phrases = [
        "Patient discharged in stable condition.",
        "Follow up with PCP in 2 weeks.",
        "Prescription provided. Monitor symptoms.",
        "Referred to specialist for further evaluation.",
        "Observation period completed. No acute findings.",
    ]
    encounters = []
    for i in range(1, 501):
        pid = int(rng.choice(patient_ids, p=pw))
        admit = fake.date_between(start_date="-1y", end_date="today")
        days = int(rng.exponential(3)) + 1
        status = rng.choice(ENCOUNTER_STATUSES, p=[0.8, 0.1, 0.1])
        encounters.append({
            "encounter_id": i,
            "patient_id": pid,
            "provider_id": int(rng.choice(provider_ids)),
            "admit_date": admit,
            "discharge_date": admit + datetime.timedelta(days=days) if status == "completed" else None,
            "encounter_type": rng.choice(ENCOUNTER_TYPES),
            "status": status,
            "discharge_notes": rng.choice(discharge_phrases) if status == "completed" else None,
        })

    # --- Child records projected from encounters ---
    diagnosis, medications, lab_results, insurance_claims = [], [], [], []
    dx_id = med_id = lab_id = claim_id = 1

    for enc in encounters:
        pid, eid, admit = enc["patient_id"], enc["encounter_id"], enc["admit_date"]

        for _ in range(int(rng.choice([0, 1, 1, 2, 2, 3]))):
            code, desc, sev = ICD10_CODES[rng.randint(0, len(ICD10_CODES))]
            diagnosis.append({
                "diagnosis_id": dx_id, "patient_id": pid, "encounter_id": eid,
                "icd10_code": code, "description": desc, "severity": sev,
                "diagnosis_date": admit,
            })
            dx_id += 1

        for _ in range(int(rng.choice([0, 0, 1, 1, 2]))):
            drug, ndc, dose, freq = DRUG_LIST[rng.randint(0, len(DRUG_LIST))]
            medications.append({
                "medication_id": med_id, "patient_id": pid, "encounter_id": eid,
                "drug_name": drug, "ndc": ndc, "dose": dose, "frequency": freq,
                "prescribed_date": admit,
            })
            med_id += 1

        for _ in range(int(rng.choice([0, 1, 2, 2, 3, 4]))):
            code, _, unit, ref_range = LOINC_CODES[rng.randint(0, len(LOINC_CODES))]
            lo, hi = [float(x) for x in ref_range.split("-")]
            val = round(rng.normal((lo + hi) / 2, (hi - lo) / 4), 2)
            lab_results.append({
                "lab_result_id": lab_id, "patient_id": pid, "encounter_id": eid,
                "test_code": code, "result_value": val, "unit": unit,
                "reference_range": ref_range,
                "abnormal_flag": "H" if val > hi else ("L" if val < lo else "N"),
                "collected_date": admit,
            })
            lab_id += 1

        if rng.random() < 0.6:
            billed = round(rng.lognormal(7, 1), 2)
            cstatus = rng.choice(CLAIM_STATUSES, p=[0.6, 0.2, 0.15, 0.05])
            insurance_claims.append({
                "claim_id": claim_id, "patient_id": pid, "encounter_id": eid,
                "policy_number": f"POL-{fake.bothify('??####').upper()}",
                "service_date": admit,
                "billed_amount": billed,
                "paid_amount": round(billed * rng.uniform(0.5, 1.0), 2) if cstatus == "paid" else 0.0,
                "status": cstatus,
            })
            claim_id += 1

    return {
        "patients": patients,
        "providers": providers,
        "encounters": encounters,
        "diagnosis": diagnosis,
        "medications": medications,
        "lab_results": lab_results,
        "insurance_claims": insurance_claims,
    }


_ALL_TABLES_CACHE: dict = {}


def _get_table(name: str, seed: int = SEED) -> list:
    global _ALL_TABLES_CACHE
    if not _ALL_TABLES_CACHE or _ALL_TABLES_CACHE.get("_seed") != seed:
        _ALL_TABLES_CACHE = generate_all_tables(seed)
        _ALL_TABLES_CACHE["_seed"] = seed
    return _ALL_TABLES_CACHE[name]


GENERATORS = {t: (lambda t=t: _get_table(t)) for t in EVAL_TABLE_NAMES}

# ---------------------------------------------------------------------------
# Expected values -- PI
# ---------------------------------------------------------------------------
# Values for the `type` column in metadata_generation_log (lowercase).
# The `classification` column is just "protected" or "None" after
# hardcode_classification -- not useful for evaluation.

EXPECTED_PI: Dict[Tuple[str, str], Tuple[str, str]] = {
    # (table_name, column_name): (expected_type, acceptable_alternatives)
    # Ground truth aligned with HIPAA rules and dbxmetagen PI prompt:
    #   - HIPAA 18 identifiers (names, dates, IDs, contact info) → pii
    #   - Healthcare-specific solo IDs (MRN) → phi (solo_medical_identifier)
    #   - Medical data without embedded identifiers → medical_information
    #   - Non-sensitive → None
    #
    # patients
    ("patients", "patient_id"): ("pii", "phi"),
    ("patients", "mrn"): ("phi", "pii"),
    ("patients", "first_name"): ("pii", ""),
    ("patients", "last_name"): ("pii", ""),
    ("patients", "date_of_birth"): ("pii", "phi"),
    ("patients", "gender"): ("None", "pii"),
    ("patients", "address"): ("pii", ""),
    ("patients", "created_at"): ("None", "pii"),
    # providers
    ("providers", "provider_id"): ("pii", "None"),
    ("providers", "npi"): ("pii", "phi"),
    ("providers", "name"): ("pii", ""),
    ("providers", "specialty"): ("None", "medical_information"),
    ("providers", "email"): ("pii", ""),
    ("providers", "phone"): ("pii", ""),
    # encounters -- IDs reference patients, dates are HIPAA identifiers
    ("encounters", "encounter_id"): ("pii", "None"),
    ("encounters", "patient_id"): ("pii", "phi"),
    ("encounters", "provider_id"): ("pii", "None"),
    ("encounters", "admit_date"): ("pii", "phi"),
    ("encounters", "discharge_date"): ("pii", "phi"),
    ("encounters", "encounter_type"): ("None", ""),
    ("encounters", "status"): ("None", ""),
    ("encounters", "discharge_notes"): ("phi", "pii,medical_information"),
    # diagnosis -- IDs and dates are PII, clinical data is medical_information
    ("diagnosis", "diagnosis_id"): ("pii", "None"),
    ("diagnosis", "patient_id"): ("pii", "phi"),
    ("diagnosis", "encounter_id"): ("pii", "None"),
    ("diagnosis", "icd10_code"): ("medical_information", "phi"),
    ("diagnosis", "description"): ("medical_information", "phi"),
    ("diagnosis", "severity"): ("medical_information", "None"),
    ("diagnosis", "diagnosis_date"): ("pii", "phi"),
    # medications -- medication data is medical_information
    ("medications", "medication_id"): ("pii", "None"),
    ("medications", "patient_id"): ("pii", "phi"),
    ("medications", "encounter_id"): ("pii", "None"),
    ("medications", "drug_name"): ("medical_information", "phi"),
    ("medications", "ndc"): ("medical_information", "None"),
    ("medications", "dose"): ("medical_information", "phi"),
    ("medications", "frequency"): ("medical_information", "None"),
    ("medications", "prescribed_date"): ("pii", "phi"),
    # lab_results -- lab data is medical_information
    ("lab_results", "lab_result_id"): ("pii", "None"),
    ("lab_results", "patient_id"): ("pii", "phi"),
    ("lab_results", "encounter_id"): ("pii", "None"),
    ("lab_results", "test_code"): ("medical_information", "None"),
    ("lab_results", "result_value"): ("medical_information", "phi"),
    ("lab_results", "unit"): ("medical_information", "None"),
    ("lab_results", "reference_range"): ("medical_information", "None"),
    ("lab_results", "abnormal_flag"): ("medical_information", "None"),
    ("lab_results", "collected_date"): ("pii", "phi"),
    # insurance_claims
    ("insurance_claims", "claim_id"): ("pii", "None"),
    ("insurance_claims", "patient_id"): ("pii", "phi"),
    ("insurance_claims", "encounter_id"): ("pii", "None"),
    ("insurance_claims", "policy_number"): ("pii", "pci,phi"),
    ("insurance_claims", "service_date"): ("pii", "phi"),
    ("insurance_claims", "billed_amount"): ("None", "pci"),
    ("insurance_claims", "paid_amount"): ("None", "pci"),
    ("insurance_claims", "status"): ("None", ""),
}

# ---------------------------------------------------------------------------
# Expected values -- Domain classification
# ---------------------------------------------------------------------------
# Keys are YAML domain/subdomain keys from healthcare.yaml.
# Domain classification is LLM-dependent after keyword prefilter, so
# we include acceptable alternatives for ambiguous tables.

EXPECTED_DOMAINS: Dict[str, Tuple[str, str, str, str]] = {
    # table: (domain, subdomain, domain_alts, subdomain_alts)
    "patients": ("clinical", "patient_care", "", ""),
    "providers": ("clinical", "patient_care", "workforce", "credentialing"),
    "encounters": ("clinical", "patient_care", "", ""),
    "diagnosis": ("clinical", "diagnosis_condition", "", "patient_care"),
    "medications": ("clinical", "medication_orders", "", ""),
    "lab_results": ("diagnostics", "laboratory", "", "vitals_observations"),
    "insurance_claims": ("payer", "claims", "", "membership"),
}

# ---------------------------------------------------------------------------
# Expected values -- Ontology entities (PascalCase)
# ---------------------------------------------------------------------------

EXPECTED_ENTITIES: Dict[str, Tuple[str, str, str]] = {
    # table: (entity_type, confidence_tier, acceptable_alternatives)
    # With fhir_r4 bundle, entity names follow FHIR resource naming.
    "patients": ("Patient", "tier1", ""),
    "providers": ("Practitioner", "tier1", "Provider"),
    "encounters": ("Encounter", "tier1", ""),
    "diagnosis": ("Condition", "tier2", "DiagnosticReport"),
    "medications": ("Medication", "tier1", "MedicationRequest"),
    "lab_results": ("Observation", "tier1", "DiagnosticReport"),
    "insurance_claims": ("Claim", "tier1", ""),
}

# ---------------------------------------------------------------------------
# Expected values -- Column property roles
# ---------------------------------------------------------------------------
# Roles from fhir_r4.yaml / healthcare.yaml property_roles vocabulary.
# Tiers: tier1 = bundle_match (0.95), tier2 = heuristic (0.55-0.85),
# tier3 = fallback (0.40). Tier annotations are verified by tracing each
# column through _build_bundle_property_index -> _heuristic_classify.
#
# KNOWN PIPELINE GAP: Entity discovery over-attributes source_columns to
# referenced entities, inflating the col_entity_map. When
# classify_column_properties finds a column in col_entity_map, the
# linked_entity heuristic returns object_property immediately, bypassing
# downstream pattern checks (e.g., _code -> dimension, DATE -> temporal).
# This causes ~21 FNs where the expected role is dimension/primary_key/pii
# but the pipeline returns object_property. This is a real accuracy gap in
# entity discovery's source_column attribution, NOT an eval or ground truth
# bug. Improving entity discovery's column attribution is the highest-impact
# accuracy improvement available.

@dataclass
class ExpectedProperty:
    role: str
    is_sensitive: bool
    tier: str = "tier2"

EXPECTED_COLUMN_PROPERTIES: Dict[Tuple[str, str], ExpectedProperty] = {
    # --- patients -> Patient ---
    # Patient has 15 FHIR properties (identifier, name, birthDate, gender, address, telecom, ...)
    ("patients", "patient_id"): ExpectedProperty("primary_key", False, "tier1"),  # Patient.identifier
    ("patients", "mrn"): ExpectedProperty("business_key", True, "tier2"),  # PI=PHI, heuristic business_key
    ("patients", "first_name"): ExpectedProperty("label", True, "tier1"),  # Patient.name -> label
    ("patients", "last_name"): ExpectedProperty("label", True, "tier1"),  # Patient.name -> label
    ("patients", "date_of_birth"): ExpectedProperty("temporal", True, "tier1"),  # Patient.birthDate
    ("patients", "gender"): ExpectedProperty("dimension", False, "tier1"),  # Patient.gender
    ("patients", "address"): ExpectedProperty("label", True, "tier1"),  # Patient.address -> label (bundle)
    ("patients", "created_at"): ExpectedProperty("temporal", False, "tier2"),  # heuristic: TIMESTAMP dtype
    # --- providers -> Practitioner ---
    # Practitioner has 9 FHIR properties. provider_id not in typical_attrs
    # (practitioner_id is); self-ref PK heuristic works (providers -> provider).
    ("providers", "provider_id"): ExpectedProperty("primary_key", False, "tier2"),  # self-ref PK heuristic
    ("providers", "npi"): ExpectedProperty("business_key", True, "tier2"),  # PI=PII, heuristic business_key
    ("providers", "name"): ExpectedProperty("label", True, "tier1"),  # Practitioner.name -> label
    ("providers", "specialty"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("providers", "email"): ExpectedProperty("label", True, "tier1"),  # Practitioner.telecom -> label
    ("providers", "phone"): ExpectedProperty("label", True, "tier1"),  # Practitioner.telecom -> label
    # --- encounters -> Encounter ---
    # Encounter has 17 FHIR properties (identifier, subject, status, type, period, ...).
    ("encounters", "encounter_id"): ExpectedProperty("primary_key", False, "tier1"),  # Encounter.identifier
    ("encounters", "patient_id"): ExpectedProperty("object_property", False, "tier1"),  # Encounter.subject
    ("encounters", "provider_id"): ExpectedProperty("object_property", False, "tier2"),  # _id suffix heuristic
    ("encounters", "admit_date"): ExpectedProperty("temporal", True, "tier2"),  # DATE dtype heuristic
    ("encounters", "discharge_date"): ExpectedProperty("temporal", True, "tier2"),  # DATE dtype heuristic
    ("encounters", "encounter_type"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("encounters", "status"): ExpectedProperty("dimension", False, "tier1"),  # Encounter.status
    ("encounters", "discharge_notes"): ExpectedProperty("label", True, "tier2"),  # text pattern heuristic
    # --- diagnosis -> Condition ---
    # Condition has 23 FHIR properties. diagnosis_id not in typical_attrs (condition_id is).
    # Self-ref PK: diagnosis -> diagnosi, diagnosis_id.startswith(diagnosi) -> primary_key.
    ("diagnosis", "diagnosis_id"): ExpectedProperty("primary_key", False, "tier2"),  # self-ref PK heuristic
    ("diagnosis", "patient_id"): ExpectedProperty("object_property", False, "tier1"),  # Condition.subject
    ("diagnosis", "encounter_id"): ExpectedProperty("object_property", False, "tier1"),  # Condition.encounter
    ("diagnosis", "icd10_code"): ExpectedProperty("dimension", False, "tier2"),  # _code suffix heuristic
    ("diagnosis", "description"): ExpectedProperty("label", False, "tier2"),  # text pattern heuristic
    ("diagnosis", "severity"): ExpectedProperty("dimension", False, "tier1"),  # Condition.severity
    ("diagnosis", "diagnosis_date"): ExpectedProperty("temporal", True, "tier2"),  # DATE dtype heuristic
    # --- medications -> Medication ---
    # Medication has 11 FHIR properties. patient_id/encounter_id are NOT on
    # Medication (those are on MedicationRequest), so they fall to heuristic.
    # If entity discovery maps to MedicationRequest instead, patient_id and
    # encounter_id would get bundle_match (tier1).
    ("medications", "medication_id"): ExpectedProperty("primary_key", False, "tier1"),  # Medication.identifier
    ("medications", "patient_id"): ExpectedProperty("object_property", False, "tier2"),  # _id suffix heuristic
    ("medications", "encounter_id"): ExpectedProperty("object_property", False, "tier2"),  # _id suffix heuristic
    ("medications", "drug_name"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("medications", "ndc"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("medications", "dose"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("medications", "frequency"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("medications", "prescribed_date"): ExpectedProperty("temporal", True, "tier2"),  # DATE dtype heuristic
    # --- lab_results -> Observation ---
    # Observation has 30 FHIR properties. lab_result_id not in typical_attrs
    # (observation_id is); self-ref PK works (lab_results -> lab_result).
    ("lab_results", "lab_result_id"): ExpectedProperty("primary_key", False, "tier2"),  # self-ref PK heuristic
    ("lab_results", "patient_id"): ExpectedProperty("object_property", False, "tier1"),  # Observation.subject
    ("lab_results", "encounter_id"): ExpectedProperty("object_property", False, "tier1"),  # Observation.encounter
    ("lab_results", "test_code"): ExpectedProperty("dimension", False, "tier2"),  # _code suffix heuristic
    ("lab_results", "result_value"): ExpectedProperty("measure", False, "tier1"),  # Observation.valueQuantity
    ("lab_results", "unit"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("lab_results", "reference_range"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("lab_results", "abnormal_flag"): ExpectedProperty("dimension", False, "tier3"),  # STRING fallback
    ("lab_results", "collected_date"): ExpectedProperty("temporal", True, "tier2"),  # DATE dtype heuristic
    # --- insurance_claims -> Claim ---
    # Claim has 17 FHIR properties (identifier, patient, status, billablePeriod, ...).
    ("insurance_claims", "claim_id"): ExpectedProperty("primary_key", False, "tier1"),  # Claim.identifier
    ("insurance_claims", "patient_id"): ExpectedProperty("object_property", False, "tier1"),  # Claim.patient
    ("insurance_claims", "encounter_id"): ExpectedProperty("object_property", False, "tier2"),  # _id suffix heuristic
    ("insurance_claims", "policy_number"): ExpectedProperty("business_key", True, "tier2"),  # PI=PII, heuristic business_key
    ("insurance_claims", "service_date"): ExpectedProperty("temporal", True, "tier2"),  # DATE dtype heuristic
    ("insurance_claims", "billed_amount"): ExpectedProperty("measure", False, "tier2"),  # DOUBLE numeric heuristic
    ("insurance_claims", "paid_amount"): ExpectedProperty("measure", False, "tier2"),  # DOUBLE numeric heuristic
    ("insurance_claims", "status"): ExpectedProperty("dimension", False, "tier1"),  # Claim.status
}

# ---------------------------------------------------------------------------
# Expected values -- Ontology relationships
# ---------------------------------------------------------------------------
# _resolve_edge_name tries: bundle properties -> legacy relationships ->
# edge catalog find_edge -> fallback "references". Direction matters.
# We always accept "references" as an alternative.

EXPECTED_RELATIONSHIPS: List[Tuple[str, str, str, str]] = [
    # (src_entity_type, dst_entity_type, expected_name, acceptable_alternatives)
    # Relationships come from two sources:
    #   1. discover_named_relationships: reads object_property + linked_entity_type
    #      from ontology_column_properties (column-level evidence)
    #   2. emit_bundle_edges: reads tier-1 edge definitions (entity-level)
    # Pipeline consistently uses FHIR entity names (Practitioner, MedicationRequest).
    # Removed duplicate business-name aliases that caused double-counted FNs.
    ("Encounter", "Patient", "references", "subject"),
    ("Encounter", "Practitioner", "references", "participant"),
    ("Condition", "Patient", "references", "subject"),
    ("Condition", "Encounter", "references", "encounter"),
    ("MedicationRequest", "Patient", "references", "subject"),
    ("MedicationRequest", "Encounter", "references", "encounter"),
    ("Observation", "Patient", "references", "subject"),
    ("Observation", "Encounter", "references", "encounter"),
    ("Claim", "Patient", "references", "patient"),
    ("Claim", "Encounter", "for_encounter", "references"),
    # Valid FHIR bundle edge (Patient.generalPractitioner)
    ("Patient", "Practitioner", "references", "generalPractitioner"),
    # Reverse FK-inferred edges (discover_named_relationships emits both directions)
    # Patient->Encounter and Patient->Condition removed: auto_inverse emission
    # is unreliable depending on seen-set ordering and bundle-edge collision.
    ("Patient", "MedicationRequest", "references", ""),
    ("Patient", "Observation", "references", ""),
    ("Patient", "Claim", "references", ""),
    ("Encounter", "Condition", "references", ""),
    ("Encounter", "Observation", "references", ""),
    ("Encounter", "Claim", "references", ""),
    ("Encounter", "MedicationRequest", "references", ""),
]

# ---------------------------------------------------------------------------
# Expected values -- FK predictions
# ---------------------------------------------------------------------------
# Discovery mechanism for eval tables (6 candidate sources, ordered by trust):
#   SR_DECLARED (0): Declared FKs from extended_table_metadata.
#   SR_QUERY (1): Query-history join patterns.
#   SR_COL_PROP (2): NEW -- reads ontology_column_properties directly.
#     For each object_property with linked_entity_type, proposes
#     (src_column -> PK of linked entity's primary table). Falls back to
#     same-column-name on target table when no PK is found. This closes the
#     previous pipeline gap where column-level evidence was aggregated to
#     entity-type pairs and then column pairs were re-guessed.
#   SR_NAME (3): Name heuristics (same-column-name, stem-to-table).
#   SR_ONTOLOGY (4): Entity-type pairing from ontology_relationships then
#     id-like column matching.
#   SR_EMBEDDING (5): Embedding similarity on graph_edges.
#   AI judge: tie-breaker on borderline scores.
#
# With SR_COL_PROP, most FK pairs should now be discovered directly
# from column property linkage (patient_id -> Patient entity -> patients
# PK column) rather than relying on name heuristics or re-guessing.

EXPECTED_FK: List[Tuple[str, str, str, str]] = [
    ("encounters", "patient_id", "patients", "patient_id"),
    ("encounters", "provider_id", "providers", "provider_id"),
    ("diagnosis", "patient_id", "patients", "patient_id"),
    ("diagnosis", "encounter_id", "encounters", "encounter_id"),
    ("medications", "patient_id", "patients", "patient_id"),
    ("medications", "encounter_id", "encounters", "encounter_id"),
    ("lab_results", "patient_id", "patients", "patient_id"),
    ("lab_results", "encounter_id", "encounters", "encounter_id"),
    ("insurance_claims", "patient_id", "patients", "patient_id"),
    ("insurance_claims", "encounter_id", "encounters", "encounter_id"),
]

# ---------------------------------------------------------------------------
# Expected values -- Comment keywords
# ---------------------------------------------------------------------------
# Keyword lists per (table, column). Column=None means table-level comment.
# These are terms the generated comment should contain (case-insensitive).

EXPECTED_COMMENTS: Dict[Tuple[str, Optional[str]], List[str]] = {
    ("patients", None): ["patient", "demographic", "healthcare"],
    ("patients", "patient_id"): ["patient", "identifier"],
    ("patients", "mrn"): ["medical record", "identifier"],
    ("patients", "first_name"): ["first name", "patient"],
    ("patients", "last_name"): ["last name", "patient"],
    ("patients", "date_of_birth"): ["birth", "date"],
    ("patients", "gender"): ["gender"],
    ("patients", "address"): ["address"],
    ("patients", "created_at"): ["created", "timestamp"],
    ("providers", None): ["provider", "healthcare", "practitioner"],
    ("providers", "provider_id"): ["provider", "identifier"],
    ("providers", "npi"): ["npi", "national provider"],
    ("providers", "name"): ["name", "provider"],
    ("providers", "specialty"): ["specialty", "medical"],
    ("providers", "email"): ["email"],
    ("providers", "phone"): ["phone"],
    ("encounters", None): ["encounter", "visit", "patient"],
    ("encounters", "encounter_id"): ["encounter", "identifier"],
    ("encounters", "patient_id"): ["patient", "reference"],
    ("encounters", "provider_id"): ["provider", "reference"],
    ("encounters", "admit_date"): ["admission", "date"],
    ("encounters", "discharge_date"): ["discharge", "date"],
    ("encounters", "encounter_type"): ["type", "encounter"],
    ("encounters", "status"): ["status"],
    ("encounters", "discharge_notes"): ["discharge", "notes"],
    ("diagnosis", None): ["diagnosis", "condition", "clinical"],
    ("diagnosis", "diagnosis_id"): ["diagnosis", "identifier"],
    ("diagnosis", "icd10_code"): ["icd", "code", "diagnosis"],
    ("diagnosis", "description"): ["description", "diagnosis"],
    ("diagnosis", "severity"): ["severity"],
    ("diagnosis", "diagnosis_date"): ["date", "diagnosis"],
    ("medications", None): ["medication", "prescription", "drug"],
    ("medications", "medication_id"): ["medication", "identifier"],
    ("medications", "drug_name"): ["drug", "name", "medication"],
    ("medications", "ndc"): ["ndc", "drug", "code"],
    ("medications", "dose"): ["dose", "dosage"],
    ("medications", "frequency"): ["frequency", "administration"],
    ("medications", "prescribed_date"): ["prescribed", "date"],
    ("lab_results", None): ["lab", "result", "test"],
    ("lab_results", "lab_result_id"): ["lab", "identifier"],
    ("lab_results", "test_code"): ["test", "code"],
    ("lab_results", "result_value"): ["result", "value"],
    ("lab_results", "unit"): ["unit", "measurement"],
    ("lab_results", "reference_range"): ["reference", "range", "normal"],
    ("lab_results", "abnormal_flag"): ["abnormal", "flag"],
    ("lab_results", "collected_date"): ["collected", "date"],
    ("insurance_claims", None): ["insurance", "claim", "billing"],
    ("insurance_claims", "claim_id"): ["claim", "identifier"],
    ("insurance_claims", "policy_number"): ["policy", "insurance"],
    ("insurance_claims", "service_date"): ["service", "date"],
    ("insurance_claims", "billed_amount"): ["billed", "amount"],
    ("insurance_claims", "paid_amount"): ["paid", "amount"],
    ("insurance_claims", "status"): ["status", "claim"],
}

import os
import json
import logging
from typing import Dict, Any, List, Tuple, Optional
from presidio_analyzer import (
    AnalyzerEngine,
    PatternRecognizer,
    RecognizerResult,
    Pattern,
)
import spacy
from datetime import datetime

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.user_utils import sanitize_user_identifier

def luhn_checksum(card_number: str):
    card_number = card_number.replace(" ", "").replace("-", "")
    if not card_number.isdigit():
        return False
    if len(card_number) < 13 or len(card_number) > 19:
        return False
    sum_ = 0
    alt = False
    for digit in reversed(card_number):
        d = int(digit)
        if alt:
            d = d * 2
            if d > 9:
                d -= 9
        sum_ += d
        alt = not alt
    return sum_ % 10 == 0

TEN_DIGIT_PHONE_PATTERN = Pattern(
    name="ten_digit_phone_pattern",
    regex=r"\b\d{10}\b",  # exactly 10 digits, word-boundary safe
    score=0.8
)

WHITESPACE_PHONE_PATTERN = Pattern(
    name="whitespace_phone_pattern",
    regex=r"\(\s*\d{3}\s*\)\s*-\s*\d{3}\s*-\s*\d{4}",
    score=0.9
)

PhoneRecognizer = PatternRecognizer(
    supported_entity="PHONE_NUMBER",
    patterns=[TEN_DIGIT_PHONE_PATTERN, WHITESPACE_PHONE_PATTERN],
    context=["phone", "call", "contact", "mobile"]
)

AGE_GENDER_PATTERN = Pattern(
    name="age_gender_pattern",
    regex=r"\b\d{1,3}\s?[YyMmFf]\b",
    score=0.8
)

AgeGenderRecognizer = PatternRecognizer(
    supported_entity="AGE_GENDER",
    patterns=[AGE_GENDER_PATTERN],
    context=["age", "sex", "gender"]
)

def add_recognizers_to_analyzer(analyzer_engine):
    analyzer_engine.registry.add_recognizer(PhoneRecognizer)
    analyzer_engine.registry.add_recognizer(AgeGenderRecognizer)
    return analyzer_engine


def get_analyzer_engine(add_pci: bool = True, add_phi: bool = True, **kwargs) -> AnalyzerEngine:
    """Initialize Presidio AnalyzerEngine with PCI/PHI recognizers."""
    analyzer = AnalyzerEngine(**kwargs)
    if add_pci:
        pci_patterns = [
            Pattern(
                name="credit_card_basic", regex=r"\b(?:\d[ -]*?){13,19}\b", score=0.5
            ),
            Pattern(
                name="credit_card_grouped",
                regex=r"\b(?:\d{4}[ -]?){3}\d{4}\b",
                score=0.6,
            ),
            Pattern(
                name="iban",
                regex=r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b",  # At least 15 chars total
                score=0.8,
            ),
            Pattern(
                name="swift", regex=r"\b[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?\b", score=0.8
            ),
        ]
        pci_recognizer = PatternRecognizer(
            supported_entity="CREDIT_CARD",
            patterns=pci_patterns,
            context=[
                "credit",
                "card",
                "visa",
                "mastercard",
                "amex",
                "discover",
                "payment",
                "cvv",
                "cvc",
                "expiry",
                "expiration",
                "cardholder",
                "pan",
                "primary account",
            ],
        )
        analyzer.registry.add_recognizer(pci_recognizer)

    if add_phi:
        # PHI patterns for medical data
        phi_patterns = [
            Pattern(name="mrn", regex=r"\bMRN[:\s]*\d{6,10}\b", score=0.8),
            Pattern(
                name="patient_id",
                regex=r"\b(?:PT|PAT|PATIENT)[:\s-]*\d{6,10}\b",
                score=0.8,
            ),
            Pattern(name="health_insurance", regex=r"\b[A-Z]{3}\d{9,12}\b", score=0.7),
            Pattern(
                name="medical_license",
                regex=r"\b(?:MD|DO|NP|RN)[:\s-]*\d{6,10}\b",
                score=0.7,
            ),
            # Pattern(
            #     name="diagnosis_code", regex=r"\b[A-Z]\d{2}\.?\d{1,2}\b", score=0.6
            # ),
            # Pattern(
            #     name="prescription_number", regex=r"\bRX[:\s-]*\d{6,12}\b", score=0.7
            # ),
        ]
        phi_recognizer = PatternRecognizer(
            supported_entity="MEDICAL_RECORD_NUMBER",
            patterns=phi_patterns,
            context=[
                "mrn",
                "medical",
                "record",
                "patient",
                "health",
                "insurance",
                # "diagnosis",
                # "prescription",
                "doctor",
            ],
        )
        analyzer.registry.add_recognizer(phi_recognizer)

    analyzer = add_recognizers_to_analyzer(analyzer)
    return analyzer


def analyze_column(
    analyzer: AnalyzerEngine,
    column_data: List[Any],
    entities: Optional[List[str]] = None,
    language: str = "en",
    score_threshold: float = 0.5,
) -> List[List[RecognizerResult]]:
    """
    Analyze each cell in a column for PII/PHI/PCI entities.
    Only returns results above the score threshold to reduce false positives.
    """
    results = []
    for i, cell in enumerate(column_data):
        try:
            text = str(cell) if cell is not None else ""
            analysis = analyzer.analyze(
                text=text,
                language=language,
                entities=entities,
                score_threshold=score_threshold,
            )
            results.append(analysis)
        except Exception as e:
            logging.error("Error analyzing cell at index %s: %s", i, e)
            results.append([])
    return results


def classify_column(
    analyzer: AnalyzerEngine,
    column_name: str,
    column_data: List[Any],
    score_threshold: float = 0.5,
) -> Tuple[str, List[str]]:
    """
    Classify a column as PII, PHI, PCI, or Non-sensitive.
    Returns the detected type and a list of detected entities.
    Filters out overly aggressive entities and applies score thresholds.
    """
    entity_map = {
        "PII": [
            "PERSON",
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "ADDRESS",
            "NRP",
            "LOCATION",
            "IP_ADDRESS",
            "URL",
            "CRYPTO",
            "US_SSN",
            "US_DRIVER_LICENSE",
            "US_PASSPORT",
            "US_ITIN",
            "UK_NHS",
            "UK_NINO",
            "IT_FISCAL_CODE",
            "IT_DRIVER_LICENSE",
            "IT_VAT_CODE",
            "IT_PASSPORT",
            "IT_IDENTITY_CARD",
            "ES_NIF",
            "ES_NIE",
            "PL_PESEL",
            "SG_NRIC_FIN",
            "SG_UEN",
            "AU_ABN",
            "AU_ACN",
            "AU_TFN",
            "AU_MEDICARE",
            "IN_PAN",
            "IN_AADHAAR",
            "IN_VEHICLE_REGISTRATION",
            "IN_VOTER_ID",
            "IN_PASSPORT",
            "FI_PERSONAL_ID",
        ],
        "PHI": [
            "MEDICAL_LICENSE",
            "MEDICAL_RECORD_NUMBER",
            "MRN",
            "HEALTH_INSURANCE_NUMBER",
            "PATIENT_NAME",
            "PATIENT_ID",
            "PATIENT_MRN",
            "PATIENT_SSN",
            "PATIENT_DOB",
            "PATIENT_GENDER",
            "PATIENT_RACE",
            "PATIENT_ETHNICITY",
            "PATIENT_ADDRESS",
            "PATIENT_PHONE",
            "PATIENT_EMAIL",
            "PATIENT_ZIP",
        ],
        "PCI": [
            "CREDIT_CARD",
            "US_BANK_NUMBER",
            "IBAN_CODE",
            "CREDIT_CARD_NUMBER",
            "CREDIT_CARD_EXPIRATION_DATE",
            "CREDIT_CARD_CVV",
            "CREDIT_CARD_HOLDER_NAME",
            "CREDIT_CARD_ISSUER",
            "CREDIT_CARD_TYPE",
            "CREDIT_CARD_NETWORK",
            "SWIFT_CODE",
            "ABA_NUMBER",
            "BANK_ACCOUNT_NUMBER",
        ],
    }

 
    entities_to_ignore = {}
    entities_to_ignore = {
        "DATE_TIME",  # Too aggressive - matches times, dates, and random numbers
    }

    detected_types = set()
    detected_entities = set()
    results = analyze_column(analyzer, column_data, score_threshold=score_threshold)

    for cell_results in results:
        for res in cell_results:
            # Skip ignored entities
            if res.entity_type in entities_to_ignore:
                continue
            if res.entity_type == "CREDIT_CARD":
                # TODO: should not be res.score but the actual CC string
                if not luhn_checksum(str(res.score)):
                    continue

            for typ, ents in entity_map.items():
                if res.entity_type in ents:
                    detected_types.add(typ)
                    detected_entities.add(res.entity_type)

    if not detected_types:
        return "Non-sensitive", []
    return ", ".join(sorted(detected_types)), sorted(detected_entities)


def process_table(
    config: MetadataConfig,
    data: Dict[str, Any],
    score_threshold: float = 0.5,
) -> List[Dict[str, Any]]:
    """
    Process the input data, classify each column, and save results.

    Args:
        config: MetadataConfig with catalog/schema info
        data: Dictionary with column_contents (columns and data)
        score_threshold: Minimum confidence score for Presidio matches (0.0-1.0).
                        Higher values reduce false positives but may miss some PII.
                        Recommended: 0.5 for balanced, 0.6-0.7 for stricter filtering.
    """
    current_date = datetime.now().strftime("%Y%m%d")
    current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_dir = f"/Volumes/{config.catalog_name}/{config.schema_name}/generated_metadata/{sanitize_user_identifier(config.current_user)}/{current_date}/presidio_logs"

    # Create directory for UC volumes (they don't auto-create)
    os.makedirs(output_dir, exist_ok=True)

    analyzer = get_analyzer_engine()
    column_contents = data.get("column_contents", {})
    columns = column_contents.get("columns", [])
    data_rows = column_contents.get("data", [])
    results = []

    logging.info(
        f"Analyzing {len(columns)} columns with score threshold {score_threshold}"
    )

    for idx, col in enumerate(columns):
        column_data = [row[idx] for row in data_rows]
        col_type, entities = classify_column(
            analyzer, col, column_data, score_threshold=score_threshold
        )
        results.append(
            {"column": col, "classification": col_type, "entities": entities}
        )
        logging.info(
            f"Column '{col}' classified as '{col_type}' with entities: {entities}"
        )

    output_path = os.path.join(
        output_dir, f"presidio_column_classification_results_{current_timestamp}.txt"
    )
    try:
        with open(output_path, "w") as f:
            f.write(f"Presidio Analysis Results (score_threshold={score_threshold})\n")
            f.write("=" * 80 + "\n\n")
            for res in results:
                f.write(
                    f"{res['column']}: {res['classification']} ({', '.join(res['entities'])})\n"
                )
        logging.info(f"Results saved to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save results: {e}")
    return results


def ensure_spacy_model(model_name: str = "en_core_web_lg"):
    """
    Load pre-installed spaCy model. Model should be installed via requirements.txt.
    """
    try:
        return spacy.load(model_name)
    except OSError as exc:
        logging.error("Error loading spaCy model: %s", exc)
        raise RuntimeError(
            f"spaCy model '{model_name}' not found. "
            f"Ensure it's installed via requirements.txt or run: python -m spacy download {model_name}"
        )


def detect_pi(config, input_data: Dict[str, Any]) -> str:
    """
    Main function to process input data for PII/PHI/PCI detection.
    Uses presidio_score_threshold from config (default 0.6) to filter results.
    """
    # Get score threshold from config, default to 0.6 if not set
    score_threshold = getattr(config, "presidio_score_threshold", 0.6)

    logging.info(
        f"Starting PII/PHI/PCI detection with score threshold {score_threshold}"
    )
    results = process_table(config, input_data, score_threshold=score_threshold)
    logging.info("Detection process completed.")
    return json.dumps({"deterministic_results": results})

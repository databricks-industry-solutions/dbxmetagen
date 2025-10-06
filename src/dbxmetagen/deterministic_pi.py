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


def get_analyzer_engine(add_pci: bool = True, add_phi: bool = True) -> AnalyzerEngine:
    """Initialize Presidio AnalyzerEngine with PCI/PHI recognizers."""
    analyzer = AnalyzerEngine()

    if add_pci:
        # PCI patterns for financial data
        pci_patterns = [
            Pattern(name="credit_card", regex=r"\b(?:\d[ -]*?){13,16}\b", score=0.8),
            Pattern(name="cvv", regex=r"\b\d{3,4}\b", score=0.6),
            Pattern(
                name="expiry_date",
                regex=r"\b(0[1-9]|1[0-2])[\/\-](\d{2}|\d{4})\b",
                score=0.7,
            ),
            Pattern(name="iban", regex=r"\b[A-Z]{2}\d{2}[A-Z0-9]{1,30}\b", score=0.8),
            Pattern(
                name="swift", regex=r"\b[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?\b", score=0.8
            ),
            Pattern(name="bank_account", regex=r"\b\d{8,17}\b", score=0.6),
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
                "payment",
                "cvv",
                "expiry",
                "bank",
                "account",
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
            Pattern(
                name="diagnosis_code", regex=r"\b[A-Z]\d{2}\.?\d{1,2}\b", score=0.6
            ),
            Pattern(
                name="prescription_number", regex=r"\bRX[:\s-]*\d{6,12}\b", score=0.7
            ),
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
                "diagnosis",
                "prescription",
                "doctor",
            ],
        )
        analyzer.registry.add_recognizer(phi_recognizer)

    return analyzer


def analyze_column(
    analyzer: AnalyzerEngine,
    column_data: List[Any],
    entities: Optional[List[str]] = None,
    language: str = "en",
) -> List[List[RecognizerResult]]:
    """
    Analyze each cell in a column for PII/PHI/PCI entities.
    """
    results = []
    for cell in column_data:
        try:
            text = str(cell) if cell is not None else ""
            analysis = analyzer.analyze(text=text, language=language, entities=entities)
            results.append(analysis)
        except Exception as e:
            logging.error(f"Error analyzing cell '{cell}': {e}")
            results.append([])
    return results


def classify_column(
    analyzer: AnalyzerEngine, column_name: str, column_data: List[Any]
) -> Tuple[str, List[str]]:
    """
    Classify a column as PII, PHI, PCI, or Non-sensitive.
    Returns the detected type and a list of detected entities.
    """
    entity_map = {
        "PII": [
            "PERSON",
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "ADDRESS",
            "DATE_TIME",
            "NRP",
            "LOCATION",
            "IP_ADDRESS",
            "URL",
            "CREDIT_CARD",
            "IBAN_CODE",
            "CRYPTO",
            "US_SSN",
            "US_BANK_NUMBER",
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
    detected_types = set()
    detected_entities = set()
    results = analyze_column(analyzer, column_data)
    for cell_results in results:
        for res in cell_results:
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
) -> List[Dict[str, Any]]:
    """
    Process the input data, classify each column, and save results.
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
    for idx, col in enumerate(columns):
        column_data = [row[idx] for row in data_rows]
        col_type, entities = classify_column(analyzer, col, column_data)
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
    except OSError:
        raise RuntimeError(
            f"spaCy model '{model_name}' not found. "
            f"Ensure it's installed via requirements.txt or run: python -m spacy download {model_name}"
        )


def detect_pi(config, input_data: Dict[str, Any]) -> str:
    """
    Main function to process input data for PII/PHI/PCI detection.
    """
    # setup_logging()
    logging.info("Starting PII/PHI/PCI detection process.")
    results = process_table(config, input_data)
    return json.dumps({"deterministic_results": results})
    logging.info("Detection process completed.")

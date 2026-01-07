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
from typing import Dict
from abc import ABC, abstractmethod
import logging

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
        pass


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


def generate_clinical_note(context, _):
    """Generate clinical note text"""
    condition = context.faker.medical_condition()
    medication = context.faker.medication()
    return f"""CLINICAL NOTE
Patient presents with {condition.lower()}. 
Physical examination reveals {context.faker.sentence(nb_words=8)}.
Current medications include {medication}.
Assessment: {condition} - stable condition.
Plan: Continue current treatment regimen. {context.faker.sentence(nb_words=12)}.
Follow-up in {context.faker.random_int(min=1, max=12)} weeks."""


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
    """Generate veterinary observation note"""
    observation = context.faker.random_element(
        ["normal behavior", "mild lethargy", "good appetite"]
    )
    return f"VETERINARY NOTE: Animal appears {observation}. Examiner: Dr. {context.faker.last_name()}"


class MedicalNotesSchemaGenerator(BaseSchemaGenerator):
    """Generates medical notes schema with realistic clinical data"""

    def generate_tables(self) -> Dict[str, DataFrame]:
        """Generate 5 interconnected tables for medical notes domain"""

        # 1. Patients table (PII/PHI)
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
                expr="current_date()",  # dateadd(current_date(), - cast(rand()*365*80 + 365*18 as int)),
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

        # 4. Clinical notes table (Unstructured PHI)
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
                values=["Progress Note", "Discharge Summary", "Consultation"],
                random=True,
            )
            .withColumn(
                "note_text",
                StringType(),
                text=PyfuncText(generate_clinical_note, init=init_faker_for_generation),
            )
            .withColumn(
                "created_datetime",
                TimestampType(),
                expr="current_timestamp()",  # - interval cast(rand()*365*2 as int) day",
            )
        )

        # 5. Lab results table (Structured PHI)
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
                values=["CBC", "BMP", "Lipid Panel", "HbA1c", "TSH"],
                random=True,
            )
            .withColumn(
                "test_value", DoubleType(), minValue=1.0, maxValue=500.0, random=True
            )
            .withColumn(
                "reference_range",
                StringType(),
                values=["Normal", "High", "Low", "Critical"],
                random=True,
            )
            .withColumn(
                "collected_datetime",
                TimestampType(),
                expr="current_timestamp()",  # - interval cast(rand()*365*2 as int) day",
            )
        )

        # Build and return tables
        tables = {}
        for spec in [
            patients_spec,
            providers_spec,
            encounters_spec,
            notes_spec,
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

        # 5. Billing records table (PHI with unstructured notes)
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
                "total_amount",
                DoubleType(),
                minValue=500.0,
                maxValue=200000.0,
                random=True,
            )
            .withColumn(
                "insurance_paid",
                DoubleType(),
                minValue=0.0,
                maxValue=180000.0,
                random=True,
            )
            .withColumn(
                "billing_status",
                StringType(),
                values=["Paid", "Pending", "Overdue"],
                random=True,
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

        # 5. Lab measurements table (Structured PHI)
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
                values=["Screening", "Baseline", "Week 4", "End of Study"],
                random=True,
            )
            .withColumn(
                "lab_test",
                StringType(),
                values=["Hemoglobin", "WBC Count", "ALT"],
                random=True,
            )
            .withColumn(
                "result_value", DoubleType(), minValue=1.0, maxValue=300.0, random=True
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

        # 5. Veterinary observations table (Unstructured text with some PII)
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
                "body_weight_kg",
                DoubleType(),
                minValue=5.0,
                maxValue=800.0,
                random=True,
            )
            .withColumn(
                "clinical_signs",
                StringType(),
                values=["Normal", "Lethargy", "Decreased Appetite"],
                random=True,
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
    print(f"  - Patients: {patient_count:,}")
    print(
        f"  - Encounters: {encounter_count:,} (ratio: {encounter_count/patient_count:.1f}x)"
    )

    # Check Hospital Data schema relationships
    hospital_tables = schemas["hospital_data"]
    staff_count = hospital_tables["hospital_staff"].count()
    admission_count = hospital_tables["patient_admissions"].count()

    print(f"\nHospital Data Schema Validation:")
    print(f"  - Staff: {staff_count:,}")
    print(f"  - Admissions: {admission_count:,}")

    # Check Clinical Trials schema relationships
    trials_tables = schemas["clinical_trials"]
    trial_count = trials_tables["clinical_trials"].count()
    participant_count = trials_tables["study_participants"].count()

    print(f"\nClinical Trials Schema Validation:")
    print(f"  - Trials: {trial_count:,}")
    print(
        f"  - Participants: {participant_count:,} (avg: {participant_count/trial_count:.0f} per trial)"
    )

    # Check Livestock Research schema relationships
    livestock_tables = schemas["livestock_research"]
    facility_count = livestock_tables["research_facilities"].count()
    animal_count = livestock_tables["research_animals"].count()

    print(f"\nLivestock Research Schema Validation:")
    print(f"  - Facilities: {facility_count:,}")
    print(
        f"  - Animals: {animal_count:,} (avg: {animal_count/facility_count:.0f} per facility)"
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

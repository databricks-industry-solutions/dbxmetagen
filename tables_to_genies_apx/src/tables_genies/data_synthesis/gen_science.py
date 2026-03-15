"""Generate synthetic science research data: NASA, Drug Discovery, Semiconductors."""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURATION
# =============================================================================
SEED = 42
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)

CATALOG = "serverless_dbx_unifiedchat_catalog"
SCHEMAS = ["nasa", "drug_discovery", "semiconductors"]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def create_infrastructure(spark, catalog, schemas):
    """Create schemas if they don't exist (catalog must already exist)."""
    print(f"Using catalog: {catalog}")
    
    for schema in schemas:
        print(f"Creating schema: {catalog}.{schema}")
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            print(f"  ✓ Created schema {schema}")
        except Exception as e:
            print(f"  Note: {e}")

def save_table(spark, df_pandas, catalog, schema, table_name):
    """Convert pandas DataFrame to Spark DataFrame and save as table."""
    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    print(f"  Saving {full_table_name} ({len(df_pandas):,} rows)")
    
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
    print(f"  ✓ Saved {full_table_name}")

# =============================================================================
# SETUP
# =============================================================================
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# CREATE INFRASTRUCTURE
# =============================================================================
create_infrastructure(spark, CATALOG, SCHEMAS)

# =============================================================================
# NASA SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating NASA data...")
print(f"{'='*80}")

# 1. Missions
missions_nasa = pd.DataFrame({
    "mission_id": [f"NASA-{i+1:03d}" for i in range(120)],
    "mission_name": [fake.company() + "-" + str(i) for i in range(120)],
    "mission_type": np.random.choice(['Satellite', 'Rover', 'Probe', 'Crewed', 'ISS'], 120, p=[0.4, 0.2, 0.2, 0.1, 0.1]),
    "launch_date": [fake.date_between(start_date=START_DATE, end_date=END_DATE) for _ in range(120)],
    "status": np.random.choice(['Active', 'Completed', 'Planned', 'Failed'], 120, p=[0.3, 0.5, 0.15, 0.05]),
    "budget_millions": np.random.lognormal(5, 1, 120).round(2),
})
save_table(spark, missions_nasa, CATALOG, "nasa", "missions")

# 2. Astronauts
astronauts_nasa = pd.DataFrame({
    "astronaut_id": [f"ASTR-{i+1:03d}" for i in range(150)],
    "name": [fake.name() for _ in range(150)],
    "nationality": np.random.choice(['USA', 'Russia', 'China', 'ESA', 'Japan', 'Canada'], 150, p=[0.5, 0.2, 0.1, 0.1, 0.05, 0.05]),
    "missions_completed": np.random.randint(0, 7, 150),
    "spacewalks": np.random.randint(0, 12, 150),
    "total_space_hours": np.random.lognormal(6, 1.5, 150).round(1),
})
save_table(spark, astronauts_nasa, CATALOG, "nasa", "astronauts")

# 3. Spacecraft
spacecraft_nasa = pd.DataFrame({
    "spacecraft_id": [f"SC-{i+1:03d}" for i in range(100)],
    "name": [fake.company() + " Spacecraft" for _ in range(100)],
    "type": np.random.choice(['Satellite', 'Capsule', 'Lander', 'Orbiter', 'Rover'], 100, p=[0.4, 0.2, 0.15, 0.15, 0.1]),
    "launch_year": np.random.randint(1990, 2026, 100),
    "operational": np.random.choice([True, False], 100, p=[0.6, 0.4]),
    "mass_kg": np.random.lognormal(7, 1, 100).round(0),
})
save_table(spark, spacecraft_nasa, CATALOG, "nasa", "spacecraft")

# 4. Launches
launches_nasa_data = []
mission_ids_nasa = missions_nasa['mission_id'].tolist()
spacecraft_ids_nasa = spacecraft_nasa['spacecraft_id'].tolist()

for i in range(150):
    launches_nasa_data.append({
        "launch_id": f"LNCH-{i+1:04d}",
        "mission_id": np.random.choice(mission_ids_nasa),
        "spacecraft_id": np.random.choice(spacecraft_ids_nasa),
        "launch_site": np.random.choice(['Kennedy Space Center', 'Cape Canaveral', 'Vandenberg', 'Wallops', 'Baikonur']),
        "launch_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "success": np.random.choice([True, False], p=[0.92, 0.08]),
    })
launches_nasa = pd.DataFrame(launches_nasa_data)
save_table(spark, launches_nasa, CATALOG, "nasa", "launches")

# 5. Discoveries
discoveries_nasa = pd.DataFrame({
    "discovery_id": [f"DISC-{i+1:03d}" for i in range(130)],
    "mission_id": np.random.choice(mission_ids_nasa, 130),
    "discovery_type": np.random.choice(['Exoplanet', 'Mineral', 'Water', 'Organic Compound', 'Geological Feature'], 130),
    "discovery_date": [fake.date_between(start_date=START_DATE, end_date=END_DATE) for _ in range(130)],
    "significance": np.random.choice(['Low', 'Medium', 'High', 'Critical'], 130, p=[0.3, 0.4, 0.25, 0.05]),
    "description": [fake.sentence() for _ in range(130)],
})
save_table(spark, discoveries_nasa, CATALOG, "nasa", "discoveries")

# =============================================================================
# DRUG DISCOVERY SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Drug Discovery data...")
print(f"{'='*80}")

# 1. Compounds
compounds_dd = pd.DataFrame({
    "compound_id": [f"CMP-{i+1:04d}" for i in range(200)],
    "compound_name": [fake.bothify("??-####") for _ in range(200)],
    "molecular_weight": np.random.uniform(150, 800, 200).round(2),
    "discovery_date": [fake.date_between(start_date='-5y', end_date=END_DATE) for _ in range(200)],
    "compound_class": np.random.choice(['Small Molecule', 'Antibody', 'Peptide', 'Nucleotide'], 200, p=[0.6, 0.2, 0.15, 0.05]),
    "solubility": np.random.choice(['High', 'Medium', 'Low', 'Poor'], 200, p=[0.2, 0.4, 0.3, 0.1]),
})
save_table(spark, compounds_dd, CATALOG, "drug_discovery", "compounds")

# 2. Targets (biological targets)
targets_dd = pd.DataFrame({
    "target_id": [f"TGT-{i+1:03d}" for i in range(120)],
    "target_name": [fake.bothify("???-?###") for _ in range(120)],
    "target_type": np.random.choice(['Protein', 'Enzyme', 'Receptor', 'Ion Channel', 'DNA'], 120, p=[0.4, 0.3, 0.2, 0.05, 0.05]),
    "disease_area": np.random.choice(['Oncology', 'Neurology', 'Cardiology', 'Immunology', 'Infectious Disease'], 120),
    "validation_status": np.random.choice(['Validated', 'In Validation', 'Hypothesis'], 120, p=[0.3, 0.5, 0.2]),
})
save_table(spark, targets_dd, CATALOG, "drug_discovery", "targets")

# 3. Trials (reference compounds and targets)
trials_dd_data = []
compound_ids_dd = compounds_dd['compound_id'].tolist()
target_ids_dd = targets_dd['target_id'].tolist()

for i in range(180):
    trials_dd_data.append({
        "trial_id": f"TRIAL-{i+1:04d}",
        "compound_id": np.random.choice(compound_ids_dd),
        "target_id": np.random.choice(target_ids_dd),
        "phase": np.random.choice(['Preclinical', 'Phase I', 'Phase II', 'Phase III', 'Phase IV'], p=[0.4, 0.25, 0.2, 0.1, 0.05]),
        "start_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "patients_enrolled": np.random.randint(10, 500),
        "status": np.random.choice(['Ongoing', 'Completed', 'Terminated'], p=[0.5, 0.4, 0.1]),
    })
trials_dd = pd.DataFrame(trials_dd_data)
save_table(spark, trials_dd, CATALOG, "drug_discovery", "trials")

# 4. Researchers
researchers_dd = pd.DataFrame({
    "researcher_id": [f"RES-{i+1:04d}" for i in range(150)],
    "name": [fake.name() for _ in range(150)],
    "institution": [fake.company() + " University" for _ in range(150)],
    "specialization": np.random.choice(['Medicinal Chemistry', 'Pharmacology', 'Toxicology', 'Structural Biology'], 150),
    "publications": np.random.randint(5, 200, 150),
    "h_index": np.random.randint(10, 100, 150),
})
save_table(spark, researchers_dd, CATALOG, "drug_discovery", "researchers")

# 5. Publications
publications_dd = pd.DataFrame({
    "publication_id": [f"PUB-{i+1:04d}" for i in range(220)],
    "researcher_id": np.random.choice(researchers_dd['researcher_id'].tolist(), 220),
    "compound_id": np.random.choice(compound_ids_dd + [None]*50, 220),  # Some without compound
    "title": [fake.sentence() for _ in range(220)],
    "journal": np.random.choice(['Nature', 'Science', 'Cell', 'JAMA', 'Lancet', 'NEJM'], 220),
    "publication_date": [fake.date_between(start_date=START_DATE, end_date=END_DATE) for _ in range(220)],
    "citations": np.random.randint(0, 500, 220),
})
save_table(spark, publications_dd, CATALOG, "drug_discovery", "publications")

# =============================================================================
# SEMICONDUCTORS SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Semiconductors data...")
print(f"{'='*80}")

# 1. Chips
chips_semi = pd.DataFrame({
    "chip_id": [f"CHIP-{i+1:04d}" for i in range(150)],
    "chip_model": [fake.bothify("??-####-?") for _ in range(150)],
    "process_node_nm": np.random.choice([3, 5, 7, 10, 14, 22, 28], 150, p=[0.05, 0.1, 0.15, 0.2, 0.25, 0.15, 0.1]),
    "transistor_count_billions": np.random.lognormal(2, 0.8, 150).round(2),
    "die_area_mm2": np.random.uniform(50, 800, 150).round(2),
    "design_date": [fake.date_between(start_date='-3y', end_date=END_DATE) for _ in range(150)],
    "application": np.random.choice(['CPU', 'GPU', 'Mobile', 'Automotive', 'IoT', 'AI'], 150, p=[0.2, 0.2, 0.25, 0.1, 0.15, 0.1]),
})
save_table(spark, chips_semi, CATALOG, "semiconductors", "chips")

# 2. Fabrication Plants (fabs)
fabs_semi = pd.DataFrame({
    "fab_id": [f"FAB-{i+1:02d}" for i in range(20)],
    "fab_name": [fake.company() + " Fab" for _ in range(20)],
    "location": np.random.choice(['Arizona', 'Taiwan', 'South Korea', 'Germany', 'Japan', 'Texas', 'Oregon'], 20),
    "process_nodes_nm": [','.join(map(str, np.random.choice([3, 5, 7, 10, 14], 3, replace=False))) for _ in range(20)],
    "capacity_wafers_per_month": np.random.randint(10000, 150000, 20),
    "operational": np.random.choice([True, False], 20, p=[0.85, 0.15]),
})
save_table(spark, fabs_semi, CATALOG, "semiconductors", "fabrication_plants")

# 3. Wafer Lots (production runs)
wafer_lots_semi_data = []
chip_ids_semi = chips_semi['chip_id'].tolist()
fab_ids_semi = fabs_semi['fab_id'].tolist()

for i in range(200):
    wafer_lots_semi_data.append({
        "lot_id": f"LOT-{i+1:05d}",
        "chip_id": np.random.choice(chip_ids_semi),
        "fab_id": np.random.choice(fab_ids_semi),
        "production_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "wafer_count": np.random.randint(10, 50),
        "yield_percentage": np.random.uniform(70, 98, 1)[0].round(2),
    })
wafer_lots_semi = pd.DataFrame(wafer_lots_semi_data)
save_table(spark, wafer_lots_semi, CATALOG, "semiconductors", "wafer_lots")

# 4. Defects
defects_semi_data = []
lot_ids_semi = wafer_lots_semi['lot_id'].tolist()

for i in range(250):
    defects_semi_data.append({
        "defect_id": f"DEF-{i+1:05d}",
        "lot_id": np.random.choice(lot_ids_semi),
        "wafer_number": np.random.randint(1, 50),
        "defect_type": np.random.choice(['Particle', 'Pattern', 'Scratch', 'Hotspot', 'Edge'], p=[0.4, 0.25, 0.15, 0.1, 0.1]),
        "detection_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "severity": np.random.choice(['Critical', 'Major', 'Minor'], p=[0.1, 0.3, 0.6]),
    })
defects_semi = pd.DataFrame(defects_semi_data)
save_table(spark, defects_semi, CATALOG, "semiconductors", "defects")

# 5. Test Results
test_results_semi_data = []
for i in range(300):
    test_results_semi_data.append({
        "test_id": f"TEST-{i+1:05d}",
        "lot_id": np.random.choice(lot_ids_semi),
        "chip_id": np.random.choice(chip_ids_semi),
        "test_type": np.random.choice(['Functional', 'Performance', 'Burn-in', 'Reliability'], p=[0.4, 0.3, 0.2, 0.1]),
        "test_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "pass_fail": np.random.choice(['Pass', 'Fail'], p=[0.92, 0.08]),
        "frequency_ghz": np.random.uniform(1.5, 5.5, 1)[0].round(2) if np.random.random() > 0.3 else None,
        "power_watts": np.random.uniform(5, 350, 1)[0].round(1) if np.random.random() > 0.3 else None,
    })
test_results_semi = pd.DataFrame(test_results_semi_data)
save_table(spark, test_results_semi, CATALOG, "semiconductors", "test_results")

print(f"\n{'='*80}")
print(f"✓ Science Research data generation complete!")
print(f"  - NASA: 5 tables")
print(f"  - Drug Discovery: 5 tables")
print(f"  - Semiconductors: 5 tables")
print(f"{'='*80}")

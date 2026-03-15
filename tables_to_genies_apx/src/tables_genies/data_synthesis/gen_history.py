"""Generate synthetic history data: World War II, Roman History."""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURATION
# =============================================================================
SEED = 42

CATALOG = "serverless_dbx_unifiedchat_catalog"
SCHEMAS = ["world_war_2", "roman_history"]

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
# WORLD WAR II SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating World War II data...")
print(f"{'='*80}")

# Famous WW2 battles
WW2_BATTLES = [
    "Battle of Britain", "Pearl Harbor", "Stalingrad", "Midway", "El Alamein",
    "D-Day", "Battle of the Bulge", "Iwo Jima", "Okinawa", "Berlin"
]

# 1. Battles
battles_ww2 = pd.DataFrame({
    "battle_id": [f"WW2B-{i+1:03d}" for i in range(len(WW2_BATTLES) + 90)],
    "battle_name": WW2_BATTLES + [fake.city() + " " + np.random.choice(['Offensive', 'Campaign', 'Operation']) for _ in range(90)],
    "start_date": [fake.date_between(start_date=datetime(1939, 9, 1), end_date=datetime(1945, 8, 15)) for _ in range(len(WW2_BATTLES) + 90)],
    "end_date": [fake.date_between(start_date=datetime(1939, 9, 1), end_date=datetime(1945, 8, 15)) for _ in range(len(WW2_BATTLES) + 90)],
    "location": [fake.country() for _ in range(len(WW2_BATTLES) + 90)],
    "theater": np.random.choice(['European', 'Pacific', 'African', 'Mediterranean'], len(WW2_BATTLES) + 90, p=[0.4, 0.35, 0.15, 0.1]),
    "victor": np.random.choice(['Allied', 'Axis', 'Indecisive'], len(WW2_BATTLES) + 90, p=[0.55, 0.35, 0.1]),
})
save_table(spark, battles_ww2, CATALOG, "world_war_2", "battles")

# 2. Leaders
leaders_ww2 = pd.DataFrame({
    "leader_id": [f"LDR-{i+1:03d}" for i in range(120)],
    "name": [fake.name() for _ in range(120)],
    "country": np.random.choice(['USA', 'UK', 'USSR', 'Germany', 'Japan', 'Italy', 'France', 'China'], 120),
    "role": np.random.choice(['General', 'Admiral', 'Prime Minister', 'President', 'Field Marshal'], 120, p=[0.4, 0.15, 0.15, 0.15, 0.15]),
    "allegiance": np.random.choice(['Allied', 'Axis'], 120, p=[0.6, 0.4]),
    "birth_year": np.random.randint(1880, 1920, 120),
})
save_table(spark, leaders_ww2, CATALOG, "world_war_2", "leaders")

# 3. Campaigns
campaigns_ww2_data = []
battle_ids_ww2 = battles_ww2['battle_id'].tolist()
leader_ids_ww2 = leaders_ww2['leader_id'].tolist()

for i in range(150):
    campaigns_ww2_data.append({
        "campaign_id": f"CAMP-{i+1:04d}",
        "campaign_name": fake.catch_phrase() + " Campaign",
        "commander_id": np.random.choice(leader_ids_ww2),
        "start_date": fake.date_between(start_date=datetime(1939, 9, 1), end_date=datetime(1945, 5, 8)),
        "end_date": fake.date_between(start_date=datetime(1939, 9, 1), end_date=datetime(1945, 8, 15)),
        "objective": np.random.choice(['Territorial Gain', 'Strategic Position', 'Supply Line', 'Liberation'], p=[0.35, 0.25, 0.2, 0.2]),
        "outcome": np.random.choice(['Success', 'Failure', 'Partial'], p=[0.5, 0.3, 0.2]),
    })
campaigns_ww2 = pd.DataFrame(campaigns_ww2_data)
save_table(spark, campaigns_ww2, CATALOG, "world_war_2", "campaigns")

# 4. Casualties
casualties_ww2_data = []
for i in range(200):
    casualties_ww2_data.append({
        "casualty_id": f"CAS-{i+1:05d}",
        "battle_id": np.random.choice(battle_ids_ww2),
        "country": np.random.choice(['USA', 'UK', 'USSR', 'Germany', 'Japan', 'Italy', 'France', 'China']),
        "killed": np.random.randint(100, 50000),
        "wounded": np.random.randint(500, 100000),
        "missing": np.random.randint(0, 20000),
        "captured": np.random.randint(0, 30000),
    })
casualties_ww2 = pd.DataFrame(casualties_ww2_data)
save_table(spark, casualties_ww2, CATALOG, "world_war_2", "casualties")

# 5. Treaties
treaties_ww2 = pd.DataFrame({
    "treaty_id": [f"TREATY-{i+1:03d}" for i in range(80)],
    "treaty_name": [fake.catch_phrase() + " " + np.random.choice(['Agreement', 'Pact', 'Treaty', 'Accord']) for _ in range(80)],
    "signing_date": [fake.date_between(start_date=datetime(1939, 9, 1), end_date=datetime(1950, 12, 31)) for _ in range(80)],
    "signatories": [', '.join(np.random.choice(['USA', 'UK', 'USSR', 'France', 'Germany', 'Japan', 'Italy'], np.random.randint(2, 5), replace=False)) for _ in range(80)],
    "treaty_type": np.random.choice(['Peace', 'Alliance', 'Armistice', 'Non-Aggression', 'Surrender'], 80, p=[0.3, 0.25, 0.2, 0.15, 0.1]),
    "status": np.random.choice(['Ratified', 'Superseded', 'Violated'], 80, p=[0.6, 0.3, 0.1]),
})
save_table(spark, treaties_ww2, CATALOG, "world_war_2", "treaties")

# =============================================================================
# ROMAN HISTORY SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Roman History data...")
print(f"{'='*80}")

# Famous Roman emperors
EMPERORS = [
    "Augustus", "Tiberius", "Caligula", "Claudius", "Nero", "Vespasian", "Titus", "Domitian",
    "Trajan", "Hadrian", "Marcus Aurelius", "Commodus", "Septimius Severus", "Caracalla",
    "Constantine", "Julian", "Theodosius"
]

# 1. Emperors
emperors_roman = pd.DataFrame({
    "emperor_id": [f"EMP-{i+1:02d}" for i in range(len(EMPERORS))],
    "name": EMPERORS,
    "reign_start_year": [27 + i*10 for i in range(len(EMPERORS))],
    "reign_end_year": [37 + i*10 for i in range(len(EMPERORS))],
    "dynasty": np.random.choice(['Julio-Claudian', 'Flavian', 'Nerva-Antonine', 'Severan', 'Constantinian'], len(EMPERORS)),
    "death_cause": np.random.choice(['Natural', 'Assassination', 'Battle', 'Poisoning', 'Disease'], len(EMPERORS), p=[0.3, 0.3, 0.2, 0.1, 0.1]),
})
save_table(spark, emperors_roman, CATALOG, "roman_history", "emperors")

# 2. Provinces
provinces_roman = pd.DataFrame({
    "province_id": [f"PROV-{i+1:03d}" for i in range(100)],
    "province_name": [fake.country() for _ in range(100)],
    "region": np.random.choice(['Italia', 'Gaul', 'Hispania', 'Africa', 'Asia', 'Britannia', 'Germania'], 100),
    "established_year": np.random.randint(-200, 400, 100),
    "capital_city": [fake.city() for _ in range(100)],
    "population_thousands": np.random.randint(50, 2000, 100),
    "status": np.random.choice(['Imperial', 'Senatorial', 'Client Kingdom'], 100, p=[0.5, 0.35, 0.15]),
})
save_table(spark, provinces_roman, CATALOG, "roman_history", "provinces")

# 3. Legions
legions_roman = pd.DataFrame({
    "legion_id": [f"LEG-{i+1:02d}" for i in range(50)],
    "legion_number": np.random.choice(['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XX'], 50),
    "cognomen": [fake.word().capitalize() for _ in range(50)],
    "strength": np.random.randint(3000, 6000, 50),
    "stationed_province_id": np.random.choice(provinces_roman['province_id'].tolist(), 50),
    "formed_year": np.random.randint(-100, 300, 50),
    "disbanded": np.random.choice([True, False], 50, p=[0.4, 0.6]),
})
save_table(spark, legions_roman, CATALOG, "roman_history", "legions")

# 4. Battles
battles_roman_data = []
legion_ids_roman = legions_roman['legion_id'].tolist()
emperor_ids_roman = emperors_roman['emperor_id'].tolist()

for i in range(120):
    battles_roman_data.append({
        "battle_id": f"ROMB-{i+1:04d}",
        "battle_name": fake.city() + " " + np.random.choice(['Battle', 'Siege', 'Skirmish']),
        "year": np.random.randint(-50, 450),
        "commander_emperor_id": np.random.choice(emperor_ids_roman) if np.random.random() > 0.6 else None,
        "legion_id": np.random.choice(legion_ids_roman),
        "enemy": np.random.choice(['Barbarians', 'Parthians', 'Gauls', 'Germanic Tribes', 'Carthaginians'], p=[0.3, 0.2, 0.2, 0.2, 0.1]),
        "outcome": np.random.choice(['Victory', 'Defeat', 'Pyrrhic Victory'], p=[0.6, 0.3, 0.1]),
        "casualties_roman": np.random.randint(100, 10000),
        "casualties_enemy": np.random.randint(100, 20000),
    })
battles_roman = pd.DataFrame(battles_roman_data)
save_table(spark, battles_roman, CATALOG, "roman_history", "battles")

# 5. Monuments
monuments_roman = pd.DataFrame({
    "monument_id": [f"MON-{i+1:03d}" for i in range(100)],
    "monument_name": [fake.word().capitalize() + " " + np.random.choice(['Forum', 'Basilica', 'Colosseum', 'Temple', 'Arch', 'Aqueduct']) for _ in range(100)],
    "type": np.random.choice(['Religious', 'Civic', 'Military', 'Infrastructure', 'Entertainment'], 100, p=[0.25, 0.25, 0.15, 0.2, 0.15]),
    "location": [fake.city() for _ in range(100)],
    "province_id": np.random.choice(provinces_roman['province_id'].tolist(), 100),
    "built_year": np.random.randint(-200, 400, 100),
    "height_meters": np.random.uniform(10, 60, 100).round(1),
    "still_standing": np.random.choice([True, False], 100, p=[0.25, 0.75]),
})
save_table(spark, monuments_roman, CATALOG, "roman_history", "monuments")

print(f"\n{'='*80}")
print(f"✓ History data generation complete!")
print(f"  - World War II: 5 tables")
print(f"  - Roman History: 5 tables")
print(f"{'='*80}")

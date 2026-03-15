"""Generate synthetic health & nutrition data: Nutrition, Pharmaceuticals."""
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
SCHEMAS = ["nutrition", "pharmaceuticals"]

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
# NUTRITION SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Nutrition data...")
print(f"{'='*80}")

# 1. Foods
foods_nutr = pd.DataFrame({
    "food_id": [f"FOOD-{i+1:04d}" for i in range(200)],
    "food_name": [fake.word().capitalize() + " " + np.random.choice(['Salad', 'Bowl', 'Smoothie', 'Wrap', 'Plate']) for _ in range(200)],
    "category": np.random.choice(['Vegetable', 'Fruit', 'Grain', 'Protein', 'Dairy', 'Snack'], 200, p=[0.2, 0.18, 0.15, 0.22, 0.15, 0.1]),
    "calories_per_100g": np.random.randint(20, 600, 200),
    "serving_size_g": np.random.choice([50, 100, 150, 200, 250], 200),
    "organic": np.random.choice([True, False], 200, p=[0.3, 0.7]),
})
save_table(spark, foods_nutr, CATALOG, "nutrition", "foods")

# 2. Nutrients
nutrients_nutr = pd.DataFrame({
    "nutrient_id": [f"NUT-{i+1:03d}" for i in range(80)],
    "nutrient_name": np.random.choice(['Protein', 'Carbohydrate', 'Fat', 'Fiber', 'Vitamin A', 'Vitamin C', 'Vitamin D', 'Calcium', 'Iron', 'Potassium', 'Sodium', 'Zinc'], 80),
    "unit": np.random.choice(['g', 'mg', 'mcg', 'IU'], 80, p=[0.4, 0.35, 0.15, 0.1]),
    "category": np.random.choice(['Macronutrient', 'Vitamin', 'Mineral', 'Trace Element'], 80, p=[0.25, 0.35, 0.3, 0.1]),
    "rda_adult": np.random.uniform(0.1, 2000, 80).round(2),
})
save_table(spark, nutrients_nutr, CATALOG, "nutrition", "nutrients")

# 3. Daily Intake (nutrition tracking)
daily_intake_nutr_data = []
food_ids_nutr = foods_nutr['food_id'].tolist()
nutrient_ids_nutr = nutrients_nutr['nutrient_id'].tolist()

for i in range(400):
    daily_intake_nutr_data.append({
        "intake_id": f"INTAKE-{i+1:05d}",
        "user_id": f"USER-{np.random.randint(1, 50):03d}",
        "food_id": np.random.choice(food_ids_nutr),
        "date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "servings": round(np.random.uniform(0.5, 3), 1),
        "meal_type": np.random.choice(['Breakfast', 'Lunch', 'Dinner', 'Snack'], p=[0.25, 0.3, 0.3, 0.15]),
    })
daily_intake_nutr = pd.DataFrame(daily_intake_nutr_data)
save_table(spark, daily_intake_nutr, CATALOG, "nutrition", "daily_intake")

# 4. Recipes
recipes_nutr = pd.DataFrame({
    "recipe_id": [f"RCP-{i+1:04d}" for i in range(150)],
    "recipe_name": [fake.word().capitalize() + " " + np.random.choice(['Delight', 'Medley', 'Special', 'Surprise']) for _ in range(150)],
    "cuisine": np.random.choice(['Italian', 'Mexican', 'Asian', 'Mediterranean', 'American', 'Indian'], 150),
    "prep_time_min": np.random.randint(10, 120, 150),
    "difficulty": np.random.choice(['Easy', 'Medium', 'Hard'], 150, p=[0.4, 0.45, 0.15]),
    "servings": np.random.randint(2, 8, 150),
    "total_calories": np.random.randint(200, 1500, 150),
})
save_table(spark, recipes_nutr, CATALOG, "nutrition", "recipes")

# 5. Dietary Plans
dietary_plans_nutr = pd.DataFrame({
    "plan_id": [f"PLAN-{i+1:03d}" for i in range(100)],
    "plan_name": [np.random.choice(['Keto', 'Paleo', 'Mediterranean', 'Vegan', 'Vegetarian', 'Low-Carb', 'High-Protein']) + " Plan " + str(i) for i in range(100)],
    "goal": np.random.choice(['Weight Loss', 'Muscle Gain', 'Maintenance', 'Athletic Performance'], 100),
    "duration_weeks": np.random.randint(4, 24, 100),
    "daily_calories": np.random.randint(1200, 3500, 100),
    "protein_percentage": np.random.randint(15, 40, 100),
    "carb_percentage": np.random.randint(20, 60, 100),
    "fat_percentage": np.random.randint(15, 40, 100),
})
save_table(spark, dietary_plans_nutr, CATALOG, "nutrition", "dietary_plans")

# =============================================================================
# PHARMACEUTICALS SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Pharmaceuticals data...")
print(f"{'='*80}")

# 1. Drugs
drugs_pharma = pd.DataFrame({
    "drug_id": [f"DRG-{i+1:04d}" for i in range(180)],
    "drug_name": [fake.word().capitalize() + np.random.choice(['tin', 'zole', 'pril', 'mab', 'ib']) for _ in range(180)],
    "generic_name": [fake.word().lower() + np.random.choice(['amine', 'ide', 'ol', 'ate']) for _ in range(180)],
    "drug_class": np.random.choice(['Antibiotic', 'Antihypertensive', 'Analgesic', 'Antidiabetic', 'Statin', 'Immunosuppressant'], 180),
    "approval_date": [fake.date_between(start_date='-20y', end_date=END_DATE) for _ in range(180)],
    "route": np.random.choice(['Oral', 'Injectable', 'Topical', 'Inhalation'], 180, p=[0.6, 0.25, 0.1, 0.05]),
    "prescription_required": np.random.choice([True, False], 180, p=[0.75, 0.25]),
})
save_table(spark, drugs_pharma, CATALOG, "pharmaceuticals", "drugs")

# 2. Manufacturers
manufacturers_pharma = pd.DataFrame({
    "manufacturer_id": [f"MFG-{i+1:03d}" for i in range(50)],
    "company_name": [fake.company() + " Pharmaceuticals" for _ in range(50)],
    "headquarters": [fake.city() for _ in range(50)],
    "country": np.random.choice(['USA', 'Germany', 'Switzerland', 'UK', 'Japan', 'India', 'China'], 50, p=[0.3, 0.15, 0.1, 0.1, 0.1, 0.15, 0.1]),
    "founded_year": np.random.randint(1950, 2020, 50),
    "annual_revenue_billions": np.random.lognormal(2, 1.5, 50).round(2),
})
save_table(spark, manufacturers_pharma, CATALOG, "pharmaceuticals", "manufacturers")

# 3. Prescriptions
prescriptions_pharma_data = []
drug_ids_pharma = drugs_pharma['drug_id'].tolist()

for i in range(500):
    prescriptions_pharma_data.append({
        "prescription_id": f"RX-{i+1:06d}",
        "drug_id": np.random.choice(drug_ids_pharma),
        "patient_id": f"PAT-{np.random.randint(1, 200):05d}",
        "prescriber_id": f"DOC-{np.random.randint(1, 50):03d}",
        "prescription_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "dosage": str(np.random.choice([5, 10, 20, 50, 100, 250, 500])) + np.random.choice(['mg', 'mcg', 'mL']),
        "frequency": np.random.choice(['Once daily', 'Twice daily', 'Three times daily', 'As needed'], p=[0.4, 0.35, 0.2, 0.05]),
        "duration_days": np.random.randint(7, 90),
    })
prescriptions_pharma = pd.DataFrame(prescriptions_pharma_data)
save_table(spark, prescriptions_pharma, CATALOG, "pharmaceuticals", "prescriptions")

# 4. Adverse Events
adverse_events_pharma_data = []
for i in range(250):
    adverse_events_pharma_data.append({
        "event_id": f"AE-{i+1:05d}",
        "drug_id": np.random.choice(drug_ids_pharma),
        "patient_id": f"PAT-{np.random.randint(1, 200):05d}",
        "event_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "event_type": np.random.choice(['Nausea', 'Dizziness', 'Headache', 'Rash', 'Fatigue', 'Insomnia'], p=[0.25, 0.2, 0.2, 0.15, 0.15, 0.05]),
        "severity": np.random.choice(['Mild', 'Moderate', 'Severe'], p=[0.6, 0.3, 0.1]),
        "reported": np.random.choice([True, False], p=[0.7, 0.3]),
    })
adverse_events_pharma = pd.DataFrame(adverse_events_pharma_data)
save_table(spark, adverse_events_pharma, CATALOG, "pharmaceuticals", "adverse_events")

# 5. Patents
patents_pharma_data = []
manufacturer_ids_pharma = manufacturers_pharma['manufacturer_id'].tolist()

for i in range(150):
    patents_pharma_data.append({
        "patent_id": f"PAT-{i+1:04d}",
        "drug_id": np.random.choice(drug_ids_pharma) if np.random.random() > 0.2 else None,
        "manufacturer_id": np.random.choice(manufacturer_ids_pharma),
        "patent_number": fake.bothify("US-########"),
        "filing_date": fake.date_between(start_date='-15y', end_date=END_DATE),
        "expiration_date": fake.date_between(start_date=END_DATE, end_date='+10y'),
        "status": np.random.choice(['Active', 'Expired', 'Pending'], p=[0.6, 0.3, 0.1]),
    })
patents_pharma = pd.DataFrame(patents_pharma_data)
save_table(spark, patents_pharma, CATALOG, "pharmaceuticals", "patents")

print(f"\n{'='*80}")
print(f"✓ Health & Nutrition data generation complete!")
print(f"  - Nutrition: 5 tables")
print(f"  - Pharmaceuticals: 5 tables")
print(f"{'='*80}")

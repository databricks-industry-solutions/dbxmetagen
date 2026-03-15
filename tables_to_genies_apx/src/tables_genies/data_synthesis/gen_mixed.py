"""Generate synthetic mixed domain data in serverless_dbx_unifiedchat_catalog.demo_mixed."""
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
SCHEMAS = ["demo_mixed"]

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
# DEMO_MIXED SCHEMA (cross-domain tables)
# =============================================================================
print(f"\n{'='*80}")
print("Generating Mixed Domain data...")
print(f"{'='*80}")

# 1. GenAI Leaderboard (from AI domain)
genai_leaderboard = pd.DataFrame({
    "rank": range(1, 101),
    "model_name": [np.random.choice(['GPT', 'Claude', 'Gemini', 'LLaMA', 'Mistral']) + "-" + fake.bothify("##?") for _ in range(100)],
    "benchmark": np.random.choice(['MMLU', 'HumanEval', 'GSM8K', 'HellaSwag'], 100),
    "score": np.random.uniform(40, 95, 100).round(2),
    "parameters_billions": np.random.choice([1, 3, 7, 13, 34, 70, 175], 100),
    "release_date": [fake.date_between(start_date=START_DATE, end_date=END_DATE) for _ in range(100)],
    "organization": [fake.company() for _ in range(100)],
})
save_table(spark, genai_leaderboard, CATALOG, "demo_mixed", "genai_leaderboard")

# 2. NFL Superbowl Winners (from NFL domain)
superbowl_winners = pd.DataFrame({
    "year": range(1967, 2026),
    "superbowl_number": [f"SB-{i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 
                                              'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX'] + 
                          list(range(21, len(range(1967, 2026)) + 1))],
    "winning_team": [np.random.choice([
        'Green Bay Packers', 'Kansas City Chiefs', 'New England Patriots', 'Pittsburgh Steelers',
        'Dallas Cowboys', 'San Francisco 49ers', 'New York Giants', 'Denver Broncos'
    ]) for _ in range(len(range(1967, 2026)))],
    "losing_team": [np.random.choice([
        'Minnesota Vikings', 'Buffalo Bills', 'Atlanta Falcons', 'Carolina Panthers',
        'Cincinnati Bengals', 'Philadelphia Eagles', 'Seattle Seahawks', 'Los Angeles Rams'
    ]) for _ in range(len(range(1967, 2026)))],
    "winning_score": np.random.randint(17, 55, len(range(1967, 2026))),
    "losing_score": np.random.randint(3, 35, len(range(1967, 2026))),
    "stadium": [fake.company() + " Stadium" for _ in range(len(range(1967, 2026)))],
    "mvp": [fake.name() for _ in range(len(range(1967, 2026)))],
})
save_table(spark, superbowl_winners, CATALOG, "demo_mixed", "nfl_superbowl_winners")

# 3. Nutrition Vitamins (from Nutrition domain)
vitamins_mixed = pd.DataFrame({
    "vitamin_id": [f"VIT-{i+1:02d}" for i in range(20)],
    "vitamin_name": ['Vitamin A', 'Vitamin B1', 'Vitamin B2', 'Vitamin B3', 'Vitamin B5', 
                     'Vitamin B6', 'Vitamin B7', 'Vitamin B9', 'Vitamin B12', 'Vitamin C',
                     'Vitamin D', 'Vitamin E', 'Vitamin K'] + [fake.bothify("Vitamin ?#") for _ in range(7)],
    "chemical_name": [fake.word().capitalize() + 'ine' for _ in range(20)],
    "rda_mg": np.random.uniform(0.001, 1000, 20).round(3),
    "upper_limit_mg": np.random.uniform(10, 5000, 20).round(1),
    "function": np.random.choice(['Antioxidant', 'Metabolism', 'Immune Support', 'Bone Health', 'Blood Clotting'], 20),
    "deficiency_symptoms": [fake.sentence() for _ in range(20)],
    "solubility": np.random.choice(['Water', 'Fat'], 20, p=[0.65, 0.35]),
})
save_table(spark, vitamins_mixed, CATALOG, "demo_mixed", "nutrition_vitamins")

print(f"\n{'='*80}")
print(f"✓ Mixed Domain data generation complete!")
print(f"  - serverless_dbx_unifiedchat_catalog.demo_mixed: 3 tables")
print(f"{'='*80}")

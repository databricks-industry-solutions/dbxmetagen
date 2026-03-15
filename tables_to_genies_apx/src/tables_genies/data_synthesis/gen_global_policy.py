"""Generate synthetic global policy data: International Policy."""
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
SCHEMAS = ["international_policy"]

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
# INTERNATIONAL POLICY SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating International Policy data...")
print(f"{'='*80}")

# 1. Countries
COUNTRIES_LIST = [
    'USA', 'China', 'Russia', 'UK', 'France', 'Germany', 'Japan', 'India', 'Brazil',
    'Canada', 'Australia', 'South Korea', 'Mexico', 'Italy', 'Spain', 'Netherlands',
    'Saudi Arabia', 'Turkey', 'Indonesia', 'Argentina', 'South Africa', 'Egypt',
    'Pakistan', 'Nigeria', 'Poland', 'Ukraine', 'Thailand', 'Malaysia', 'Singapore',
    'Israel', 'UAE', 'Sweden', 'Norway', 'Finland', 'Denmark', 'Belgium'
]

countries_policy = pd.DataFrame({
    "country_id": [f"CTRY-{i+1:03d}" for i in range(len(COUNTRIES_LIST))],
    "country_name": COUNTRIES_LIST,
    "region": np.random.choice(['North America', 'Europe', 'Asia', 'Middle East', 'Africa', 'South America', 'Oceania'], len(COUNTRIES_LIST)),
    "gdp_trillions": np.random.lognormal(0, 2, len(COUNTRIES_LIST)).round(2),
    "population_millions": np.random.lognormal(3, 1.5, len(COUNTRIES_LIST)).round(1),
    "un_member": np.random.choice([True, False], len(COUNTRIES_LIST), p=[0.95, 0.05]),
})
save_table(spark, countries_policy, CATALOG, "international_policy", "countries")

# 2. Organizations
organizations_policy = pd.DataFrame({
    "org_id": [f"ORG-{i+1:03d}" for i in range(80)],
    "org_name": [fake.company() for _ in range(80)],
    "acronym": [fake.bothify("???") for _ in range(80)],
    "type": np.random.choice(['UN Agency', 'Trade Bloc', 'Military Alliance', 'NGO', 'Financial Institution'], 80, p=[0.25, 0.2, 0.15, 0.3, 0.1]),
    "founded_year": np.random.randint(1945, 2020, 80),
    "headquarters": [fake.city() for _ in range(80)],
    "member_count": np.random.randint(5, 195, 80),
})
save_table(spark, organizations_policy, CATALOG, "international_policy", "organizations")

# 3. Treaties
treaties_policy_data = []
country_ids_policy = countries_policy['country_id'].tolist()
org_ids_policy = organizations_policy['org_id'].tolist()

for i in range(150):
    num_signatories = np.random.randint(2, 10)
    signatories = np.random.choice(country_ids_policy, num_signatories, replace=False)
    
    treaties_policy_data.append({
        "treaty_id": f"TREATY-{i+1:04d}",
        "treaty_name": fake.catch_phrase() + " " + np.random.choice(['Agreement', 'Convention', 'Pact', 'Protocol']),
        "signing_date": fake.date_between(start_date=datetime(1950, 1, 1), end_date=END_DATE),
        "signatory_countries": ','.join(signatories),
        "facilitating_org_id": np.random.choice(org_ids_policy) if np.random.random() > 0.3 else None,
        "type": np.random.choice(['Trade', 'Climate', 'Security', 'Human Rights', 'Nuclear'], p=[0.3, 0.2, 0.25, 0.15, 0.1]),
        "status": np.random.choice(['Active', 'Expired', 'Withdrawn', 'Under Negotiation'], p=[0.6, 0.2, 0.1, 0.1]),
    })
treaties_policy = pd.DataFrame(treaties_policy_data)
save_table(spark, treaties_policy, CATALOG, "international_policy", "treaties")

# 4. Sanctions
sanctions_policy_data = []
for i in range(120):
    sanctions_policy_data.append({
        "sanction_id": f"SANC-{i+1:04d}",
        "imposing_country_id": np.random.choice(country_ids_policy),
        "target_country_id": np.random.choice(country_ids_policy),
        "sanction_type": np.random.choice(['Economic', 'Trade Embargo', 'Arms Embargo', 'Travel Ban', 'Asset Freeze'], p=[0.35, 0.25, 0.15, 0.15, 0.1]),
        "start_date": fake.date_between(start_date=datetime(2000, 1, 1), end_date=END_DATE),
        "end_date": fake.date_between(start_date=END_DATE, end_date='+5y') if np.random.random() > 0.4 else None,
        "reason": np.random.choice(['Human Rights', 'Security Threat', 'Nuclear Program', 'Trade Dispute'], p=[0.3, 0.35, 0.2, 0.15]),
    })
sanctions_policy = pd.DataFrame(sanctions_policy_data)
save_table(spark, sanctions_policy, CATALOG, "international_policy", "sanctions")

# 5. Resolutions
resolutions_policy_data = []
for i in range(200):
    resolutions_policy_data.append({
        "resolution_id": f"RES-{i+1:04d}",
        "resolution_number": f"UN-{np.random.randint(1000, 9999)}",
        "title": fake.catch_phrase(),
        "adoption_date": fake.date_between(start_date=datetime(1990, 1, 1), end_date=END_DATE),
        "votes_for": np.random.randint(50, 180),
        "votes_against": np.random.randint(0, 50),
        "abstentions": np.random.randint(0, 40),
        "subject": np.random.choice(['Peace & Security', 'Human Rights', 'Development', 'Environment', 'Disarmament'], p=[0.35, 0.25, 0.2, 0.12, 0.08]),
        "binding": np.random.choice([True, False], p=[0.4, 0.6]),
    })
resolutions_policy = pd.DataFrame(resolutions_policy_data)
save_table(spark, resolutions_policy, CATALOG, "international_policy", "resolutions")

print(f"\n{'='*80}")
print(f"✓ Global Policy data generation complete!")
print(f"  - International Policy: 5 tables")
print(f"{'='*80}")

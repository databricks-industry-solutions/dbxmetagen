"""Generate synthetic insurance data: Claims, Providers."""
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
SCHEMAS = ["claims", "providers"]

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
# CLAIMS SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Claims data...")
print(f"{'='*80}")

# 1. Policies
policies_claims = pd.DataFrame({
    "policy_id": [f"POL-{i+1:06d}" for i in range(300)],
    "policy_number": [fake.bothify("??-########") for _ in range(300)],
    "policy_type": np.random.choice(['Auto', 'Home', 'Health', 'Life', 'Business'], 300, p=[0.35, 0.25, 0.25, 0.1, 0.05]),
    "premium_annual": np.random.lognormal(7, 0.8, 300).round(2),
    "coverage_amount": np.random.choice([100000, 250000, 500000, 1000000], 300, p=[0.3, 0.4, 0.2, 0.1]),
    "start_date": [fake.date_between(start_date='-3y', end_date=END_DATE) for _ in range(300)],
    "status": np.random.choice(['Active', 'Expired', 'Cancelled'], 300, p=[0.75, 0.15, 0.1]),
})
save_table(spark, policies_claims, CATALOG, "claims", "policies")

# 2. Claimants
claimants_claims = pd.DataFrame({
    "claimant_id": [f"CLM-{i+1:05d}" for i in range(250)],
    "name": [fake.name() for _ in range(250)],
    "date_of_birth": [fake.date_of_birth(minimum_age=18, maximum_age=85) for _ in range(250)],
    "email": [fake.email() for _ in range(250)],
    "phone": [fake.phone_number() for _ in range(250)],
    "address": [fake.address().replace('\n', ', ') for _ in range(250)],
    "risk_score": np.random.uniform(0, 100, 250).round(1),
})
save_table(spark, claimants_claims, CATALOG, "claims", "claimants")

# 3. Claims
claims_data = []
policy_ids_claims = policies_claims['policy_id'].tolist()
claimant_ids_claims = claimants_claims['claimant_id'].tolist()

for i in range(400):
    policy_id = np.random.choice(policy_ids_claims)
    policy_type = policies_claims[policies_claims['policy_id'] == policy_id]['policy_type'].iloc[0]
    
    # Claim amount correlates with policy type
    if policy_type == 'Auto':
        amount = np.random.lognormal(8, 1)
    elif policy_type == 'Home':
        amount = np.random.lognormal(9, 1.2)
    elif policy_type == 'Health':
        amount = np.random.lognormal(7.5, 1.5)
    elif policy_type == 'Life':
        amount = np.random.lognormal(12, 0.8)
    else:
        amount = np.random.lognormal(10, 1)
    
    claims_data.append({
        "claim_id": f"CLM-{i+1:06d}",
        "policy_id": policy_id,
        "claimant_id": np.random.choice(claimant_ids_claims),
        "claim_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "claim_amount": round(amount, 2),
        "status": np.random.choice(['Pending', 'Approved', 'Denied', 'Under Review'], p=[0.2, 0.6, 0.1, 0.1]),
        "description": fake.sentence(),
    })
claims_df = pd.DataFrame(claims_data)
save_table(spark, claims_df, CATALOG, "claims", "claims")

# 4. Adjusters
adjusters_claims = pd.DataFrame({
    "adjuster_id": [f"ADJ-{i+1:03d}" for i in range(60)],
    "name": [fake.name() for _ in range(60)],
    "specialization": np.random.choice(['Auto', 'Home', 'Health', 'General'], 60, p=[0.3, 0.25, 0.25, 0.2]),
    "experience_years": np.random.randint(1, 30, 60),
    "claims_handled": np.random.randint(50, 2000, 60),
    "avg_resolution_days": np.random.uniform(5, 45, 60).round(1),
})
save_table(spark, adjusters_claims, CATALOG, "claims", "adjusters")

# 5. Payments
payments_claims_data = []
claim_ids_claims = claims_df['claim_id'].tolist()
adjuster_ids_claims = adjusters_claims['adjuster_id'].tolist()

for i in range(350):
    claim_id = np.random.choice(claim_ids_claims)
    claim_amount = claims_df[claims_df['claim_id'] == claim_id]['claim_amount'].iloc[0]
    
    payments_claims_data.append({
        "payment_id": f"PAY-{i+1:06d}",
        "claim_id": claim_id,
        "adjuster_id": np.random.choice(adjuster_ids_claims),
        "payment_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "payment_amount": round(claim_amount * np.random.uniform(0.7, 1.0), 2),
        "payment_method": np.random.choice(['Check', 'Direct Deposit', 'Wire Transfer'], p=[0.3, 0.6, 0.1]),
    })
payments_claims = pd.DataFrame(payments_claims_data)
save_table(spark, payments_claims, CATALOG, "claims", "payments")

# =============================================================================
# PROVIDERS SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Providers data...")
print(f"{'='*80}")

# 1. Providers
providers_prov = pd.DataFrame({
    "provider_id": [f"PROV-{i+1:05d}" for i in range(200)],
    "provider_name": [fake.company() + " " + np.random.choice(['Medical Center', 'Clinic', 'Hospital']) for _ in range(200)],
    "provider_type": np.random.choice(['Hospital', 'Clinic', 'Specialist', 'Lab', 'Pharmacy'], 200, p=[0.25, 0.3, 0.2, 0.15, 0.1]),
    "tax_id": [fake.bothify("##-#######") for _ in range(200)],
    "phone": [fake.phone_number() for _ in range(200)],
    "address": [fake.address().replace('\n', ', ') for _ in range(200)],
})
save_table(spark, providers_prov, CATALOG, "providers", "providers")

# 2. Facilities
facilities_prov_data = []
provider_ids_prov = providers_prov['provider_id'].tolist()

for provider_id in provider_ids_prov[:150]:  # 150 providers have facilities
    num_facilities = np.random.randint(1, 4)
    for i in range(num_facilities):
        facilities_prov_data.append({
            "facility_id": f"FAC-{len(facilities_prov_data)+1:05d}",
            "provider_id": provider_id,
            "facility_name": fake.city() + " " + np.random.choice(['Branch', 'Campus', 'Center']),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "beds": np.random.randint(10, 500) if np.random.random() > 0.3 else None,
            "accredited": np.random.choice([True, False], p=[0.85, 0.15]),
        })
facilities_prov = pd.DataFrame(facilities_prov_data)
save_table(spark, facilities_prov, CATALOG, "providers", "facilities")

# 3. Specialties
specialties_prov = pd.DataFrame({
    "specialty_id": [f"SPEC-{i+1:03d}" for i in range(80)],
    "specialty_name": np.random.choice([
        'Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'Oncology',
        'Dermatology', 'Psychiatry', 'Radiology', 'Anesthesiology', 'Emergency Medicine'
    ], 80),
    "provider_id": np.random.choice(provider_ids_prov, 80),
    "num_practitioners": np.random.randint(1, 50, 80),
    "certification_date": [fake.date_between(start_date='-10y', end_date=END_DATE) for _ in range(80)],
})
save_table(spark, specialties_prov, CATALOG, "providers", "specialties")

# 4. Networks
networks_prov = pd.DataFrame({
    "network_id": [f"NET-{i+1:03d}" for i in range(50)],
    "network_name": [fake.company() + " Network" for _ in range(50)],
    "network_type": np.random.choice(['HMO', 'PPO', 'EPO', 'POS'], 50, p=[0.3, 0.4, 0.2, 0.1]),
    "coverage_states": [', '.join(np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH'], np.random.randint(1, 7), replace=False)) for _ in range(50)],
    "providers_count": np.random.randint(50, 5000, 50),
    "active": np.random.choice([True, False], 50, p=[0.9, 0.1]),
})
save_table(spark, networks_prov, CATALOG, "providers", "networks")

# 5. Contracts
contracts_prov_data = []
network_ids_prov = networks_prov['network_id'].tolist()

for i in range(250):
    contracts_prov_data.append({
        "contract_id": f"CONT-{i+1:05d}",
        "provider_id": np.random.choice(provider_ids_prov),
        "network_id": np.random.choice(network_ids_prov),
        "start_date": fake.date_between(start_date='-5y', end_date=END_DATE),
        "end_date": fake.date_between(start_date=END_DATE, end_date='+5y'),
        "reimbursement_rate": np.random.uniform(0.5, 0.95, 1)[0].round(3),
        "status": np.random.choice(['Active', 'Expired', 'Pending'], p=[0.7, 0.2, 0.1]),
    })
contracts_prov = pd.DataFrame(contracts_prov_data)
save_table(spark, contracts_prov, CATALOG, "providers", "contracts")

print(f"\n{'='*80}")
print(f"✓ Insurance Claims data generation complete!")
print(f"  - Claims: 5 tables")
print(f"  - Providers: 5 tables")
print(f"{'='*80}")

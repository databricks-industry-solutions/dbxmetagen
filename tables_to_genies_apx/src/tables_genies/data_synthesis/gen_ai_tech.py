"""Generate synthetic AI/GenAI technology data."""
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
SCHEMAS = ["genai"]

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
# GENAI SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating GenAI data...")
print(f"{'='*80}")

# Model families and architectures
MODEL_FAMILIES = ['GPT', 'Claude', 'Gemini', 'LLaMA', 'Mistral', 'Phi', 'BERT', 'T5']
ARCHITECTURES = ['Transformer', 'Mixture-of-Experts', 'Diffusion', 'Autoregressive', 'Encoder-Decoder']

# 1. Models
models_genai = pd.DataFrame({
    "model_id": [f"MOD-{i+1:04d}" for i in range(150)],
    "model_name": [np.random.choice(MODEL_FAMILIES) + "-" + fake.bothify("###?") for _ in range(150)],
    "architecture": np.random.choice(ARCHITECTURES, 150, p=[0.5, 0.2, 0.15, 0.1, 0.05]),
    "parameters_billions": np.random.choice([0.5, 1, 3, 7, 13, 34, 70, 175, 405], 150, p=[0.1, 0.15, 0.2, 0.2, 0.15, 0.1, 0.05, 0.03, 0.02]),
    "release_date": [fake.date_between(start_date='-2y', end_date=END_DATE) for _ in range(150)],
    "modality": np.random.choice(['Text', 'Multimodal', 'Vision', 'Audio'], 150, p=[0.6, 0.25, 0.1, 0.05]),
    "context_length": np.random.choice([2048, 4096, 8192, 32768, 128000, 1000000], 150, p=[0.1, 0.2, 0.3, 0.2, 0.15, 0.05]),
})
save_table(spark, models_genai, CATALOG, "genai", "models")

# 2. Benchmarks
benchmarks_genai = pd.DataFrame({
    "benchmark_id": [f"BM-{i+1:03d}" for i in range(100)],
    "benchmark_name": np.random.choice(['MMLU', 'HumanEval', 'GSM8K', 'HellaSwag', 'TruthfulQA', 'BBH', 'MATH'], 100),
    "category": np.random.choice(['Reasoning', 'Coding', 'Math', 'Knowledge', 'Safety'], 100),
    "difficulty": np.random.choice(['Easy', 'Medium', 'Hard', 'Expert'], 100, p=[0.2, 0.35, 0.35, 0.1]),
    "total_questions": np.random.randint(100, 15000, 100),
    "created_date": [fake.date_between(start_date='-3y', end_date='-6m') for _ in range(100)],
})
save_table(spark, benchmarks_genai, CATALOG, "genai", "benchmarks")

# 3. Training Runs (reference models)
training_runs_genai_data = []
model_ids_genai = models_genai['model_id'].tolist()

for i in range(180):
    params = float(models_genai[models_genai['model_id'] == np.random.choice(model_ids_genai)]['parameters_billions'].iloc[0])
    
    # Training cost correlates with model size
    compute_hours = np.random.lognormal(np.log(params * 1000), 0.5)
    cost = compute_hours * np.random.uniform(2, 8)
    
    training_runs_genai_data.append({
        "run_id": f"RUN-{i+1:04d}",
        "model_id": np.random.choice(model_ids_genai),
        "start_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "compute_hours": round(compute_hours, 1),
        "num_gpus": np.random.choice([8, 16, 64, 256, 1024, 4096], p=[0.3, 0.25, 0.2, 0.15, 0.07, 0.03]),
        "cost_usd": round(cost, 2),
        "status": np.random.choice(['Completed', 'Failed', 'Running'], p=[0.75, 0.15, 0.1]),
    })
training_runs_genai = pd.DataFrame(training_runs_genai_data)
save_table(spark, training_runs_genai, CATALOG, "genai", "training_runs")

# 4. Datasets
datasets_genai = pd.DataFrame({
    "dataset_id": [f"DS-{i+1:04d}" for i in range(130)],
    "dataset_name": [fake.company() + "-" + np.random.choice(['corpus', 'dataset', 'collection']) for _ in range(130)],
    "size_gb": np.random.lognormal(4, 2, 130).round(2),
    "num_samples": (np.random.lognormal(12, 2, 130) * 1000).round(0).astype(int),
    "data_type": np.random.choice(['Text', 'Images', 'Code', 'Multimodal', 'Audio'], 130, p=[0.45, 0.25, 0.15, 0.1, 0.05]),
    "license": np.random.choice(['MIT', 'Apache-2.0', 'CC-BY', 'Proprietary', 'Research-Only'], 130, p=[0.2, 0.2, 0.25, 0.25, 0.1]),
    "created_date": [fake.date_between(start_date='-3y', end_date=END_DATE) for _ in range(130)],
})
save_table(spark, datasets_genai, CATALOG, "genai", "datasets")

# 5. Papers (reference models and datasets)
papers_genai_data = []
dataset_ids_genai = datasets_genai['dataset_id'].tolist()

for i in range(200):
    papers_genai_data.append({
        "paper_id": f"PAPER-{i+1:04d}",
        "model_id": np.random.choice(model_ids_genai) if np.random.random() > 0.2 else None,
        "dataset_id": np.random.choice(dataset_ids_genai) if np.random.random() > 0.3 else None,
        "title": fake.sentence(),
        "authors": fake.name() + ", " + fake.name(),
        "venue": np.random.choice(['NeurIPS', 'ICML', 'ICLR', 'ACL', 'CVPR', 'arXiv'], p=[0.15, 0.15, 0.15, 0.1, 0.1, 0.35]),
        "publication_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "citations": np.random.randint(0, 1000, 1)[0],
    })
papers_genai = pd.DataFrame(papers_genai_data)
save_table(spark, papers_genai, CATALOG, "genai", "papers")

print(f"\n{'='*80}")
print(f"✓ AI Tech data generation complete!")
print(f"  - GenAI: 5 tables")
print(f"{'='*80}")

# Databricks notebook source
# MAGIC %md
# MAGIC # dbxmetagen Prompt Evaluation - Example
# MAGIC
# MAGIC This notebook demonstrates how to use the evaluation system to test and improve dbxmetagen's metadata generation.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Setup and configuration
# MAGIC 2. Run quick test
# MAGIC 3. Compare models
# MAGIC 4. Review results in MLflow UI
# MAGIC 5. Add SME feedback
# MAGIC 6. Currently generalized, not intented to be run by end users.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import sys
import os

# Add dbxmetagen to path if needed
sys.path.insert(0, "/Workspace/path/to/dbxmetagen")  # Update this path

# Import evaluation modules
from eval.evaluation_runner import EvaluationRunner
from eval.datasets.evaluation_data import create_eval_dataset

print("✅ Imports successful")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Experiment
# MAGIC
# MAGIC **Important**: Update the experiment name with YOUR email address

# COMMAND ----------

# Get current user email
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(f"Current user: {current_user}")

# Create experiment name
experiment_name = f"/Users/{current_user}/dbxmetagen_prompt_eval"
print(f"Experiment: {experiment_name}")

# Initialize runner
runner = EvaluationRunner(experiment_name=experiment_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preview Evaluation Data
# MAGIC
# MAGIC Let's look at what we're evaluating

# COMMAND ----------

# Load evaluation dataset
eval_df = create_eval_dataset()

print(f"Total examples: {len(eval_df)}")
print(f"\nModes: {eval_df['inputs'].apply(lambda x: x['mode']).unique()}")
print(f"\nExample IDs:")
for idx, row in eval_df.iterrows():
    print(f"  - {row['request_id']}: {row['inputs']['table_name']} ({row['inputs']['mode']} mode)")

# COMMAND ----------

# Display first example
first_example = eval_df.iloc[0]

print("="*70)
print(f"Example: {first_example['request_id']}")
print("="*70)

print("\n📊 Inputs:")
print(f"  Table: {first_example['inputs']['table_name']}")
print(f"  Columns: {first_example['inputs']['columns']}")
print(f"  Mode: {first_example['inputs']['mode']}")

print("\n✅ Ground Truth:")
if 'table' in first_example['ground_truth']:
    print(f"  Table comment: {first_example['ground_truth']['table'][:100]}...")
    
display(eval_df[['request_id', 'inputs', 'ground_truth']].head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Quick Test Evaluation
# MAGIC
# MAGIC Start with a single model and mode to verify everything works

# COMMAND ----------

print("Running test evaluation...")
print("This will take 2-3 minutes...")

result = runner.run_single_evaluation(
    run_name="test_claude_comment",
    mode="comment",
    model_endpoint="databricks-claude-3-7-sonnet",
    temperature=0.1
)

print("\n✅ Test evaluation complete!")
print("\n📊 Metrics:")
for metric_name, value in result.metrics.items():
    if isinstance(value, (int, float)):
        print(f"  {metric_name}: {value:.4f}")
    else:
        print(f"  {metric_name}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compare Models
# MAGIC
# MAGIC Compare Claude vs Llama for comment generation

# COMMAND ----------

print("Running model comparison...")
print("This will take 5-10 minutes...")

comparison_df = runner.run_model_comparison(
    mode="comment",
    models=[
        "databricks-claude-3-7-sonnet",
        "databricks-meta-llama-3-1-405b-instruct"
    ],
    temperature=0.1
)

print("\n✅ Model comparison complete!")

# COMMAND ----------

# Display results
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Review Results in MLflow UI
# MAGIC
# MAGIC ### Steps:
# MAGIC 1. Click **Experiments** in left sidebar
# MAGIC 2. Navigate to your experiment (path shown above)
# MAGIC 3. Select multiple runs to compare
# MAGIC 4. Click **Compare** button
# MAGIC 5. Review:
# MAGIC    - Metrics table
# MAGIC    - Individual predictions
# MAGIC    - Token usage
# MAGIC 6. Add SME feedback:
# MAGIC    - Click on predictions
# MAGIC    - Add 👍 /

 👎 ratings
# MAGIC    - Add comments

# COMMAND ----------

# Get experiment URL
experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

mlflow_url = f"https://{workspace_url}/#mlflow/experiments/{experiment_id}"
print(f"📊 MLflow UI: {mlflow_url}")
displayHTML(f'<a href="{mlflow_url}" target="_blank">Open MLflow Experiment UI</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Advanced: Temperature Sweep
# MAGIC
# MAGIC Find optimal temperature setting

# COMMAND ----------

sweep_df = runner.run_temperature_sweep(
    mode="comment",
    model_endpoint="databricks-claude-3-7-sonnet",
    temperatures=[0.0, 0.1, 0.3, 0.5]
)

display(sweep_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Advanced: Evaluate PI Mode

# COMMAND ----------

pi_result = runner.run_single_evaluation(
    run_name="test_claude_pi",
    mode="pi",
    model_endpoint="databricks-claude-3-7-sonnet",
    temperature=0.1
)

print("\n✅ PI mode evaluation complete!")
print("\n📊 PII Detection Metrics:")
for metric_name, value in pi_result.metrics.items():
    if isinstance(value, (int, float)):
        print(f"  {metric_name}: {value:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Next Steps
# MAGIC
# MAGIC ### Iterate on Prompts:
# MAGIC 1. Modify prompts in `src/dbxmetagen/prompts.py`
# MAGIC 2. Re-run evaluation
# MAGIC 3. Compare new run to baseline
# MAGIC 4. Review improvements
# MAGIC
# MAGIC ### Add More Examples:
# MAGIC 1. Edit `eval/datasets/evaluation_data.py`
# MAGIC 2. Add tables from your domain
# MAGIC 3. Include edge cases
# MAGIC 4. Re-run evaluations
# MAGIC
# MAGIC ### Collect Feedback:
# MAGIC 1. Share MLflow UI with SMEs
# MAGIC 2. Have them rate predictions
# MAGIC 3. Collect comments
# MAGIC 4. Use feedback to improve
# MAGIC
# MAGIC ### Monitor Over Time:
# MAGIC 1. Track metrics across runs
# MAGIC 2. Compare prompt versions
# MAGIC 3. Measure improvement
# MAGIC 4. Document learnings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've successfully:
# MAGIC - ✅ Run evaluation
# MAGIC - ✅ Compared models
# MAGIC - ✅ Viewed results in MLflow
# MAGIC - ✅ Understand next steps
# MAGIC
# MAGIC **Key Resources:**
# MAGIC - Evaluation code: `eval/` directory
# MAGIC - Documentation: `eval/README.md`
# MAGIC - MLflow UI: Link above
# MAGIC
# MAGIC **Questions?**
# MAGIC - Check `eval/README.md` for troubleshooting
# MAGIC - Review MLflow experiment runs
# MAGIC - Examine individual predictions


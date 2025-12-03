# dbxmetagen Prompt Evaluation System

Systematic evaluation framework for dbxmetagen's LLM-based metadata generation using MLflow 3.x.

## Overview

This evaluation system allows you to:
- **Test different prompts** and compare results
- **Compare LLM models** (Claude, Llama, etc.)
- **Optimize parameters** (temperature, max_tokens)
- **Get automated metrics** (accuracy, completeness, quality scores)
- **Enable SME review** via Databricks MLflow UI
- **Track improvements** over time

## Architecture

```
eval/
├── datasets/
│   └── evaluation_data.py      # Ground truth examples
├── prompt_extraction.py         # Extracts prompts from dbxmetagen
├── custom_metrics.py            # Evaluation metrics
├── evaluation_runner.py         # Main evaluation orchestration
└── README.md                    # This file
```

## Quick Start

### 1. Update Experiment Name

Edit `evaluation_runner.py` and update the experiment name with your email:

```python
experiment_name="/Users/your.email@databricks.com/dbxmetagen_prompt_eval"
```

### 2. Run Test Evaluation

```python
from eval.evaluation_runner import run_quick_test

run_quick_test()
```

### 3. View Results in Databricks UI

1. Open Databricks workspace
2. Go to **Experiments** in left sidebar
3. Find your experiment: `/Users/your.email@databricks.com/dbxmetagen_prompt_eval`
4. Click on the run to see:
   - Metrics (quality scores, accuracy)
   - Predictions vs ground truth
   - Token usage
   - Model parameters

### 4. Add SME Feedback

In the MLflow UI:
1. Click on individual predictions
2. Add thumbs up/down ratings
3. Add comments
4. Review side-by-side comparisons

## Usage Examples

### Example 1: Compare Models

```python
from eval.evaluation_runner import EvaluationRunner

runner = EvaluationRunner(
    experiment_name="/Users/your.email@databricks.com/dbxmetagen_eval"
)

# Compare Claude vs Llama for comment generation
results = runner.run_model_comparison(
    mode="comment",
    models=[
        "databricks-claude-3-7-sonnet",
        "databricks-meta-llama-3-1-405b-instruct"
    ]
)

print(results)
```

### Example 2: Temperature Sweep

```python
# Find optimal temperature setting
runner.run_temperature_sweep(
    mode="comment",
    model_endpoint="databricks-claude-3-7-sonnet",
    temperatures=[0.0, 0.1, 0.3, 0.5, 0.7]
)
```

### Example 3: Single Evaluation

```python
# Evaluate specific configuration
runner.run_single_evaluation(
    run_name="claude_comment_baseline",
    mode="comment",
    model_endpoint="databricks-claude-3-7-sonnet",
    temperature=0.1
)
```

## Evaluation Modes

### Comment Mode
Evaluates generation of descriptive comments for tables and columns.

**Metrics**:
- `comment_quality` (LLM-as-judge): Overall quality 1-5
- `technical_accuracy` (LLM-as-judge): Technical correctness 1-5  
- `comment_completeness`: % of columns with comments
- `comment_length_appropriateness`: Length scoring

### PI Mode
Evaluates PII (Personally Identifiable Information) detection.

**Metrics**:
- `pii_classification_accuracy` (LLM-as-judge): Overall PII accuracy 1-5
- `pii_detection_accuracy`: Column-level classification accuracy

## Evaluation Dataset

### Current Examples

**Comment Mode** (3 examples):
- `customer_pii_comment_1`: Customer profiles with PII
- `sales_transactions_comment_1`: Transaction data
- `employee_sensitive_comment_1`: Sensitive HR data

**PI Mode** (3 examples):
- `customer_pii_pi_1`: PII classification for customers
- `employee_sensitive_pi_1`: Sensitive employee data
- `sales_transactions_pi_1`: Non-PII transactional data

### Adding More Examples

Edit `eval/datasets/evaluation_data.py`:

```python
def create_comment_mode_examples():
    return [
        {
            "request_id": "your_example_id",
            "inputs": {
                "table_name": "your_table",
                "columns": ["col1", "col2"],
                "column_types": {"col1": "INT", "col2": "STRING"},
                "sample_data": [{"col1": 1, "col2": "test"}],
                "metadata": {"row_count": 100},
                "mode": "comment"
            },
            "ground_truth": {
                "table": "Expected table description...",
                "columns": ["col1", "col2"],
                "column_contents": [
                    "Expected col1 description...",
                    "Expected col2 description..."
                ]
            }
        }
        # Add more examples
    ]
```

## Custom Metrics

### Creating New Metrics

#### Programmatic Metric

```python
from mlflow.metrics import MetricValue

def my_custom_metric(predictions, targets, metrics):
    scores = []
    for pred, gt in zip(predictions, targets):
        # Your scoring logic
        score = calculate_score(pred, gt)
        scores.append(score)
    
    return MetricValue(
        scores=scores,
        aggregate_results={"mean": sum(scores)/len(scores)}
    )
```

#### LLM-as-Judge Metric

```python
from mlflow.metrics import make_genai_metric

my_judge_metric = make_genai_metric(
    name="my_metric",
    definition="What this metric evaluates",
    grading_prompt="Your evaluation instructions...",
    model="endpoints:/databricks-claude-3-7-sonnet",
    parameters={"temperature": 0.0},
    aggregations=["mean", "variance"],
    greater_is_better=True
)
```

Add to `custom_metrics.py` and include in `get_metrics_for_mode()`.

## Integration with dbxmetagen

This evaluation system **does not modify** dbxmetagen's core pipelines. It:

1. **Imports** existing prompt classes (`CommentPrompt`, `PIPrompt`)
2. **Reuses** prompt generation logic
3. **Evaluates** in isolation

To test prompt changes:
1. Modify prompts in `src/dbxmetagen/prompts.py`
2. Run evaluation
3. Compare metrics before/after
4. Collect SME feedback
5. Iterate

## Best Practices

### 1. Establish Baseline
Run initial evaluation with current production prompts:
```python
runner.run_single_evaluation(
    run_name="baseline_v1",
    mode="comment",
    model_endpoint="databricks-claude-3-7-sonnet"
)
```

### 2. Iterate Incrementally
Make small prompt changes and evaluate:
```python
# After modifying prompts.py
runner.run_single_evaluation(
    run_name="experiment_shorter_prompts",
    ...
)
```

### 3. Use MLflow UI Comparison
In Databricks:
- Select multiple runs
- Click "Compare"
- Review side-by-side metrics
- Examine individual predictions

### 4. Collect SME Feedback
- Have domain experts review predictions
- Add ratings and comments in UI
- Use feedback to refine prompts

### 5. Track Costs
MLflow automatically logs:
- `prompt_tokens`
- `completion_tokens`
- `total_tokens`

Monitor these to balance quality vs. cost.

## Running Full Evaluation

```bash
# In Databricks notebook
%run ./eval/evaluation_runner.py
```

Or programmatically:

```python
from eval.evaluation_runner import run_full_evaluation

run_full_evaluation()
```

This runs:
1. Comment mode model comparison
2. PI mode model comparison
3. Temperature sweep for best model

Takes ~10-15 minutes depending on model availability.

## Troubleshooting

### Import Errors

If you see import errors for dbxmetagen modules:
```python
import sys
sys.path.insert(0, "/path/to/dbxmetagen")
```

### Model Endpoint Not Found

Verify endpoint names in Databricks:
1. Go to **Serving** → **Endpoints**
2. Find available models
3. Update model names in code

### Evaluation Data Issues

Test dataset loading:
```python
from eval.datasets.evaluation_data import create_eval_dataset

df = create_eval_dataset()
print(f"Loaded {len(df)} examples")
print(df.head())
```

## Next Steps

1. ✅ Run test evaluation
2. ✅ Review results in MLflow UI  
3. ✅ Add more evaluation examples
4. ✅ Customize metrics for your domain
5. ✅ Iterate on prompts based on results
6. ✅ Collect SME feedback
7. ✅ Monitor improvements over time

## Resources

- [MLflow Evaluation Guide](https://mlflow.org/docs/latest/llms/llm-evaluate/)
- [GenAI Metrics](https://mlflow.org/docs/latest/python_api/mlflow.metrics.html#mlflow.metrics.make_genai_metric)
- [Databricks Model Serving](https://docs.databricks.com/machine-learning/model-serving/index.html)


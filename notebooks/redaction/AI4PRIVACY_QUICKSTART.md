# AI4Privacy Dataset Quick Start Guide

This guide shows you how to use the `ai4privacy/pii-masking-300k` dataset from HuggingFace for benchmarking your PHI/PII detection methods.

## Overview

The AI4Privacy dataset contains 300,000 text samples with labeled PII entities including:
- Names (persons, organizations, locations)
- Contact information (emails, phones, addresses)
- Identifiers (SSNs, medical record numbers, account numbers)
- Dates and other sensitive information

## Setup (5 minutes)

### Step 1: Load the Dataset

Open and run notebook: `0. Load AI4Privacy Dataset.py`

**Configuration:**
```
Output Catalog: dbxmetagen
Output Schema: eval_data  
Dataset Name: ai4privacy_pii_300k
Sample Size: 1000  # Use 1000 for testing, 0 for full dataset
Split: train
```

**What it does:**
- Downloads dataset from HuggingFace
- Transforms `privacy_mask` into ground truth format
- Creates two tables:
  - `dbxmetagen.eval_data.ai4privacy_pii_300k_source` (for detection)
  - `dbxmetagen.eval_data.ai4privacy_pii_300k_ground_truth` (for evaluation)

**Expected output:**
```
✓ Source table saved with 1,000 records
✓ Ground truth table saved with ~5,000 entities
```

## Benchmarking (10-30 minutes)

### Step 2: Run Detection

Open notebook: `1. Benchmarking Detection.py`

**Configuration:**
```
Source Table: dbxmetagen.eval_data.ai4privacy_pii_300k_source
Document ID Column: doc_id
Text Column: text
Use Presidio Detection: true
Use AI Query Detection: true
Use GLiNER Detection: false  # Optional, requires GPU
AI Endpoint: databricks-gpt-oss-120b
Presidio Score Threshold: 0.5
Number of Cores: 10
Output Table: (leave blank for auto-generated name)
```

**What it does:**
- Runs selected detection methods on the source table
- Aligns entities when using multiple methods
- Adds confidence scores based on source agreement

**Expected output:**
```
Detection results with columns:
- doc_id
- text
- presidio_results_struct (array of entities)
- ai_results_struct (array of entities)
- aligned_entities (consensus entities)
```

**Runtime:**
- Presidio only: ~2-5 minutes for 1,000 docs
- AI Query: ~10-20 minutes for 1,000 docs  
- Both methods: ~15-30 minutes for 1,000 docs

### Step 3: Evaluate Performance

Open notebook: `2. Benchmarking Evaluation.py`

**Configuration:**
```
Ground Truth Table: dbxmetagen.eval_data.ai4privacy_pii_300k_ground_truth
Detection Results Table: dbxmetagen.eval_data.ai4privacy_pii_300k_source_detection_results
Dataset Name: ai4privacy_pii_300k
Evaluation Output Table: dbxmetagen.eval_data.phi_evaluation_results
Write Mode: append
```

**What it does:**
- Matches detected entities with ground truth
- Calculates precision, recall, F1, accuracy
- Saves results to shared evaluation table
- Shows false negative analysis

**Expected output:**
```
Contingency Table:
            Neg_actual  Pos_actual  Total
Neg_pred    948,230     128         948,358
Pos_pred    45          4,812       4,857
Total       948,275     4,940       953,215

Metrics:
- Precision: 0.991
- Recall: 0.974
- F1 Score: 0.982
- Accuracy: 0.999
```

## Understanding Results

### Detection Results Schema

After running detection, your table will have:

```python
{
    "doc_id": "doc_000001",
    "text": "Contact John Smith at john@email.com",
    
    # Presidio results
    "presidio_results_struct": [
        {"entity": "John Smith", "start": 8, "end": 18, "entity_type": "PERSON", "score": 0.95},
        {"entity": "john@email.com", "start": 22, "end": 36, "entity_type": "EMAIL", "score": 0.99}
    ],
    
    # AI Query results
    "ai_results_struct": [
        {"entity": "John Smith", "start": 8, "end": 18, "entity_type": "PERSON"},
        {"entity": "john@email.com", "start": 22, "end": 36, "entity_type": "EMAIL"}
    ],
    
    # Aligned/consensus results
    "aligned_entities": [
        {
            "entity": "John Smith",
            "start": 8,
            "end": 18,
            "entity_type": "PERSON",
            "presidio_score": 0.95,
            "ai_score": null,
            "gliner_score": null,
            "confidence": "high"  # High because 2 sources agree
        },
        {
            "entity": "john@email.com",
            "start": 22,
            "end": 36,
            "entity_type": "EMAIL",
            "presidio_score": 0.99,
            "ai_score": null,
            "gliner_score": null,
            "confidence": "high"
        }
    ]
}
```

### Evaluation Metrics

The evaluation produces standard classification metrics:

- **Precision**: Of all detected entities, what % were correct?
  - `TP / (TP + FP)`
  - High precision = few false positives

- **Recall**: Of all actual entities, what % were detected?
  - `TP / (TP + FN)`
  - High recall = few false negatives

- **F1 Score**: Harmonic mean of precision and recall
  - `2 * (Precision * Recall) / (Precision + Recall)`
  - Best single metric for overall performance

- **Accuracy**: Overall correctness
  - `(TP + TN) / (TP + TN + FP + FN)`
  - Usually very high due to large number of true negatives

### Confidence Levels

The alignment module assigns confidence based on source agreement:

- **High**: 2+ sources agree on exact match, or single source with high score (>0.7)
- **Medium**: Partial agreement (overlapping positions with fuzzy text match)
- **Low**: Single source with low/no score, or no agreement

## Scaling to Full Dataset

Once you've validated with a sample, scale to the full dataset:

### Load Full Dataset

```
Sample Size: 0  # This loads all 300k records
```

**Considerations:**
- Loading takes ~5-10 minutes
- Full dataset: ~300k documents, ~1.5M entities
- Storage: ~500 MB for source, ~200 MB for ground truth

### Detection at Scale

**Presidio:**
- Runtime: ~1-2 hours for 300k docs
- Cluster: Standard DS4v2 or similar
- Cores: 10-20 recommended

**AI Query:**
- Runtime: ~4-8 hours for 300k docs (depends on endpoint)
- May hit rate limits on AI endpoint
- Consider batching into chunks of 10-50k

**Optimization Tips:**
```python
# Increase parallelism
num_cores = 20  # Up to your cluster size

# For AI Query, batch processing
# Process 50k documents at a time
for i in range(0, 300000, 50000):
    batch_df = source_df.filter(
        (col("doc_id") >= f"doc_{i:06d}") & 
        (col("doc_id") < f"doc_{i+50000:06d}")
    )
    # Run detection on batch
```

## Comparing Detection Methods

After running multiple detection methods, compare them:

```sql
-- View evaluation results
SELECT 
    dataset_name,
    method_name,
    metric_name,
    metric_value
FROM dbxmetagen.eval_data.phi_evaluation_results
WHERE dataset_name = 'ai4privacy_pii_300k'
    AND metric_name IN ('precision', 'recall', 'f1_score')
ORDER BY method_name, metric_name;
```

**Expected performance ranges:**

| Method | Precision | Recall | F1 Score |
|--------|-----------|--------|----------|
| Presidio | 0.92-0.95 | 0.85-0.90 | 0.88-0.92 |
| AI Query | 0.88-0.93 | 0.90-0.95 | 0.89-0.94 |
| Aligned | 0.94-0.97 | 0.92-0.96 | 0.93-0.96 |
| GLiNER | 0.90-0.94 | 0.88-0.93 | 0.89-0.93 |

*Note: Actual performance varies based on configuration and endpoint*

## Using with Refactored Alignment Module

The new alignment module supports flexible source combinations:

```python
from dbxmetagen.redaction.alignment import align_entities_multi_source

# Align entities from all three sources
aligned = align_entities_multi_source(
    presidio_entities=presidio_ents,
    gliner_entities=gliner_ents,
    ai_entities=ai_ents,
    doc_id="doc_000001",
    fuzzy_threshold=50
)

# Each entity includes scores from all sources
for entity in aligned:
    print(f"{entity['entity']} ({entity['entity_type']})")
    print(f"  Confidence: {entity['confidence']}")
    print(f"  Presidio: {entity['presidio_score']}")
    print(f"  GLiNER: {entity['gliner_score']}")
    print(f"  AI: {entity['ai_score']}")
```

## Troubleshooting

### Dataset Loading Issues

**Problem:** HuggingFace download fails
```
Solution: Check internet connectivity, try again
```

**Problem:** Privacy mask parsing errors
```
Solution: Verify dataset version, check notebook for updates
```

### Detection Issues

**Problem:** No entities detected
```
Solutions:
- Check score threshold (try 0.3 for more detections)
- Verify text column has data
- Review entity types in config
```

**Problem:** AI Query timeout
```
Solutions:
- Reduce batch size
- Check endpoint availability
- Switch to different endpoint
```

### Evaluation Issues

**Problem:** Low precision/recall scores
```
Solutions:
- Review entity position matching logic
- Check if ground truth format is correct
- Adjust score thresholds
- Review false positive/negative examples
```

**Problem:** Evaluation metrics seem wrong
```
Solutions:
- Verify ground truth table has correct schema
- Check that doc_ids match between tables
- Review contingency table for sanity check
```

## Next Steps

After benchmarking with AI4Privacy dataset:

1. **Tune Detection Parameters**
   - Adjust score thresholds based on F1 scores
   - Configure entity types for your domain
   - Test different AI endpoints

2. **Compare with Your Data**
   - Apply same workflow to your labeled dataset
   - Compare performance across datasets
   - Identify domain-specific challenges

3. **Deploy to Production**
   - Use best-performing method from evaluation
   - Apply to production data with `4. Redaction Pipeline`
   - Monitor performance over time

4. **Cross-Dataset Analysis**
   - Evaluate on multiple datasets
   - Use shared evaluation table for comparison
   - Track performance trends

## Resources

- **HuggingFace Dataset**: https://huggingface.co/datasets/ai4privacy/pii-masking-300k
- **Alignment Module Docs**: `src/dbxmetagen/redaction/ALIGNMENT_MIGRATION_GUIDE.md`
- **Evaluation Functions**: `src/dbxmetagen/redaction/evaluation.py`
- **Detection Pipeline**: `src/dbxmetagen/redaction/pipeline.py`

## FAQ

**Q: How long does it take to process the full 300k dataset?**
A: Presidio: 1-2 hours, AI Query: 4-8 hours, depends on cluster and endpoint.

**Q: Can I use a subset of entity types?**
A: Yes, configure entity types in Presidio/GLiNER, or modify AI prompt for AI Query.

**Q: What if my ground truth format is different?**
A: Modify notebook step 3 to transform to expected schema: `doc_id`, `text`, `chunk`, `begin`, `end`.

**Q: Can I evaluate multiple detection runs at once?**
A: Yes! The evaluation table stores all runs. Just use different dataset_name or method_name.

**Q: How do I know which method to use in production?**
A: Check F1 scores from evaluation. Higher F1 = better overall performance. Consider speed vs accuracy tradeoff.

**Q: What's the difference between aligned and individual method results?**
A: Aligned combines multiple sources with weighted confidence scoring. Usually achieves best precision/recall balance.


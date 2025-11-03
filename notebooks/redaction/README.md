# PHI/PII Detection and Redaction

This module provides a comprehensive pipeline for detecting and redacting Protected Health Information (PHI) and Personally Identifiable Information (PII) in unstructured text data.

## Architecture

The redaction system consists of:

1. **Core Modules** (`src/dbxmetagen/redaction/`)
   - Detection engines (Presidio, AI Query, GLiNER)
   - Entity alignment for consensus detection
   - Text redaction with configurable strategies
   - Unity Catalog metadata integration
   - Evaluation and benchmarking framework

2. **Notebooks** (workflow orchestration)
   - Detection benchmarking
   - Evaluation and metrics
   - Redaction application
   - End-to-end pipeline

## Installation

```bash
# Install from requirements.txt
pip install -r requirements.txt
```

**Databricks Cluster:**
```python
%pip install -r /Workspace/.../requirements.txt
dbutils.library.restartPython()
```

**Key Dependencies:**
- `presidio_analyzer` and `presidio_anonymizer` for rule-based PHI detection
- `transformers` and `torch` for GLiNER biomedical NER (no gliner package needed)
- `spacy` with `en_core_web_lg` model for linguistic analysis

## Notebooks

### 1. Benchmarking Detection

Runs PHI/PII detection on a dataset using configurable detection methods.

**Inputs:**
- Source table with text data
- Document ID and text column names
- Detection method (Presidio, AI Query, or both)
- Model endpoint for AI detection
- Score thresholds

**Outputs:**
- Detection results with entity positions and types
- Aligned entities when using multiple methods

**Use Case:** Evaluate different detection methods on labeled data.

### 2. Benchmarking Evaluation

Evaluates detection performance against ground truth data.

**Inputs:**
- Ground truth table with labeled entities
- Detection results from Benchmarking Detection
- Dataset identifier for tracking
- Evaluation output table

**Outputs:**
- Performance metrics (accuracy, precision, recall, F1, etc.)
- Long-format evaluation table for cross-dataset comparison
- False negative analysis

**Use Case:** Compare detection methods and track performance across datasets.

### 3. Benchmarking Redaction

Applies redaction to text based on detected entities.

**Inputs:**
- Detection results table
- Text column to redact
- Redaction strategy (generic or typed)

**Outputs:**
- Table with redacted text
- Before/after comparisons
- Redaction statistics

**Use Case:** Create redacted versions of benchmarking datasets.

### 4. Redaction Pipeline

End-to-end detection and redaction for production use.

**Input Modes:**
- **Table + Column**: Specify table name and text column directly
- **Table + Tag**: Query Unity Catalog for columns with classification tags

**Inputs:**
- Source table (with optional Unity Catalog tags)
- Detection method and parameters
- Redaction strategy
- Output table location

**Outputs:**
- Fully redacted table with original and redacted columns
- Detection statistics

**Use Case:** Production redaction of sensitive data.

## Workflows

### Benchmarking Workflow

For evaluating detection methods on labeled data:

1. **Detection**: Run `1. Benchmarking Detection` to detect entities
2. **Evaluation**: Run `2. Benchmarking Evaluation` to compare with ground truth
3. **Redaction** (optional): Run `3. Benchmarking Redaction` to create clean dataset

### Production Workflow

For redacting production data:

**Option A - Explicit Column:**
```
Run 4. Redaction Pipeline with:
- Input Mode: table_column
- Source Table: your.catalog.table
- Text Column: your_text_column
```

**Option B - Tag-Based:**
```
Run 4. Redaction Pipeline with:
- Input Mode: table_tag
- Source Table: your.catalog.table
- Tag Name: data_classification
- Tag Value: protected
```

## Detection Methods

### Presidio

Rule-based detection using Microsoft Presidio analyzer.

**Strengths:**
- Fast and deterministic
- No API calls required
- Configurable confidence thresholds

**Use When:**
- High throughput required
- Deterministic results needed
- Well-defined entity patterns

### AI Query

LLM-based detection using Databricks AI endpoints.

**Strengths:**
- Understands context
- Detects complex entity patterns
- Can identify domain-specific entities

**Use When:**
- Complex medical terminology
- Context-dependent entities
- High accuracy required

### Combined (All)

Runs both methods and aligns results for consensus.

**Strengths:**
- Best of both approaches
- Confidence scoring based on agreement
- More robust detection

**Use When:**
- Maximum accuracy needed
- Can afford additional compute
- Benchmarking and comparison

### NER (Coming Soon)

HuggingFace transformer-based NER models.

**Status:** Stub implementation - to be added in future release.

## Redaction Strategies

### Generic Redaction

Replaces all entities with `[REDACTED]`.

**Example:**
```
Original: "John Smith called on 555-1234"
Redacted: "[REDACTED] called on [REDACTED]"
```

**Use When:**
- Simple compliance requirements
- Uniform redaction needed
- Entity types don't matter

### Typed Redaction

Replaces entities with type-specific placeholders.

**Example:**
```
Original: "John Smith called on 555-1234"
Redacted: "[PERSON] called on [PHONE_NUMBER]"
```

**Use When:**
- Preserving data structure
- Analytics on redacted data
- Understanding entity distribution

## Evaluation Schema

The evaluation framework writes results to a shared table with this schema:

```
dataset_name: STRING      - Name of the dataset evaluated
method_name: STRING       - Detection method (presidio, ai, aligned)
metric_name: STRING       - Metric name (accuracy, precision, recall, etc.)
metric_value: DOUBLE      - Metric value (0.0 to 1.0)
timestamp: TIMESTAMP      - When evaluation was run
```

This long-format schema enables:
- Cross-dataset comparison
- Method performance tracking
- Aggregated reporting

## Module Functions

### Detection (`src/dbxmetagen/redaction/detection.py`)

```python
from dbxmetagen.redaction import run_detection

# Run detection
result_df = run_detection(
    spark=spark,
    df=source_df,
    doc_id_column="doc_id",
    text_column="text",
    method="all",  # or "presidio", "ai_query"
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    score_threshold=0.5
)
```

### Redaction (`src/dbxmetagen/redaction/redaction.py`)

```python
from dbxmetagen.redaction import create_redacted_table

# Apply redaction
redacted_df = create_redacted_table(
    spark=spark,
    source_df=detection_df,
    text_column="text",
    entities_column="aligned_entities",
    output_table="catalog.schema.redacted_table",
    strategy="typed"  # or "generic"
)
```

### Pipeline (`src/dbxmetagen/redaction/pipeline.py`)

```python
from dbxmetagen.redaction import run_redaction_pipeline

# End-to-end pipeline
result_df = run_redaction_pipeline(
    spark=spark,
    source_table="catalog.schema.source",
    text_column="medical_notes",
    output_table="catalog.schema.source_redacted",
    detection_method="all",
    redaction_strategy="typed"
)
```

### Metadata (`src/dbxmetagen/redaction/metadata.py`)

```python
from dbxmetagen.redaction import get_protected_columns

# Find protected columns
columns = get_protected_columns(
    spark=spark,
    table_name="catalog.schema.table"
)
# Returns: ['ssn', 'email', 'phone']
```

### Evaluation (`src/dbxmetagen/redaction/evaluation.py`)

```python
from dbxmetagen.redaction import (
    evaluate_detection,
    calculate_metrics,
    save_evaluation_results
)

# Evaluate detection
eval_df = evaluate_detection(ground_truth_df, detection_df)
metrics = calculate_metrics(eval_df, total_tokens=100000)

# Save to shared table
save_evaluation_results(
    spark=spark,
    metrics=metrics,
    dataset_name="my_dataset",
    method_name="presidio",
    output_table="catalog.schema.evaluations"
)
```

## Configuration

### Widget Parameters

All notebooks use Databricks widgets for configuration:

- **Source Table**: Fully qualified table name (catalog.schema.table)
- **Text Column**: Name of column containing text to process
- **Detection Method**: presidio | ai_query | all
- **AI Endpoint**: Databricks model serving endpoint name
- **Redaction Strategy**: generic | typed
- **Score Threshold**: Minimum confidence (0.0 - 1.0)

### Unity Catalog Tags

For tag-based column identification:

```sql
-- Tag a column as protected
ALTER TABLE catalog.schema.table 
ALTER COLUMN sensitive_column 
SET TAGS ('data_classification' = 'protected');

-- Query tagged columns
SELECT column_name 
FROM system.information_schema.column_tags
WHERE tag_name = 'data_classification' 
  AND tag_value = 'protected';
```

## Output Tables

### Detection Results

Columns created by detection:
- `presidio_results`: JSON string of Presidio entities
- `presidio_results_struct`: Structured array of entities
- `ai_query_results`: JSON string of AI entities
- `ai_results_struct`: Structured array of entities
- `aligned_entities`: Consensus entities from multiple methods

### Redacted Tables

Columns in redacted tables:
- Original columns preserved
- `{text_column}_redacted`: Redacted version of text
- Detection result columns (optional, for debugging)

### Evaluation Results

Long-format table with metrics:
- `dataset_name`: Dataset identifier
- `method_name`: Detection method
- `metric_name`: Specific metric (precision, recall, etc.)
- `metric_value`: Numeric value
- `timestamp`: Evaluation time

## Best Practices

1. **Start with Benchmarking**: Use labeled data to evaluate methods before production use

2. **Choose Detection Method Based on Use Case**:
   - Presidio: Fast, deterministic, rule-based
   - AI Query: Context-aware, flexible, slower
   - All: Best accuracy, higher cost

3. **Use Typed Redaction for Analytics**: Preserves data structure while protecting PII

4. **Track Evaluations**: Use the shared evaluation table to compare performance over time

5. **Tag Sensitive Columns**: Use Unity Catalog tags for production data governance

6. **Test Thresholds**: Adjust score thresholds based on precision/recall requirements

7. **Monitor False Negatives**: Review missed entities to improve detection

## Troubleshooting

### Detection Issues

**No entities detected:**
- Check score threshold (lower for more detections)
- Verify text column contains data
- Test with known PII examples

**Too many false positives:**
- Increase score threshold
- Use "all" method for consensus detection
- Review entity type configuration

### Performance Issues

**Slow processing:**
- Increase num_cores parameter
- Use Presidio instead of AI Query
- Batch smaller datasets

**Out of memory:**
- Reduce batch size in Presidio
- Process in chunks
- Increase cluster size

### Evaluation Issues

**Metrics don't match expectations:**
- Verify ground truth format
- Check entity position alignment
- Review overlap tolerance settings

## Support

For issues or questions:
1. Check notebook markdown cells for inline documentation
2. Review function docstrings in source code
3. Examine example outputs in notebooks
4. Consult Databricks documentation for AI Query and Unity Catalog


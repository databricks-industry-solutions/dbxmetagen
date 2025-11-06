# Pipeline Alignment Fix Summary

## Issue

The refactored `align_entities_udf()` function now requires **4 columns** (ai, presidio, gliner, doc_id) but the detection pipeline was calling it with only **3 columns** (ai, presidio, doc_id), causing a `TypeError`:

```
TypeError: align_entities_udf.<locals>._align_udf() missing 1 required positional argument: 'doc_id_col'
```

## Root Cause

The alignment module was refactored to support flexible multi-source alignment (Presidio, GLiNER, and AI Query), but the `run_detection_pipeline()` function in `pipeline.py` was still using the old 2-source calling pattern.

### Old Code (Broken)
```python
align_entities_udf()(
    col("ai_results_struct"),
    col("presidio_results_struct"),
    col(doc_id_column),  # Missing gliner_results_struct!
)
```

### New Code (Fixed)
```python
align_udf = align_entities_udf(
    fuzzy_threshold=50,
    include_presidio=use_presidio,
    include_gliner=use_gliner,
    include_ai=use_ai_query
)

result_df = result_df.withColumn(
    "aligned_entities",
    align_udf(
        col("ai_results_struct"),
        col("presidio_results_struct"),
        col("gliner_results_struct"),  # Now included!
        col(doc_id_column),
    ),
)
```

## Changes Made

### 1. Updated `pipeline.py`

**File**: `src/dbxmetagen/redaction/pipeline.py`

#### Import Changes
- Added `array` from `pyspark.sql.functions`
- Added schema types: `ArrayType`, `StructType`, `StructField`, `StringType`, `IntegerType`, `DoubleType`

#### Logic Changes in `run_detection_pipeline()`

**Before:**
- Only aligned when both Presidio AND AI Query were enabled
- Called UDF with 3 columns

**After:**
- Aligns for any combination of 1-3 sources
- Creates empty array columns for missing sources
- Uses `include_*` flags to tell UDF which sources to actually use
- Always passes all 4 columns to UDF in correct order

#### Key Implementation Details

```python
# Define empty array schema for missing sources
empty_entity_array = array().cast(
    ArrayType(
        StructType([
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("score", DoubleType()),
            StructField("doc_id", StringType()),
        ])
    )
)

# Ensure all three columns exist (use empty array if not)
if "ai_results_struct" not in result_df.columns:
    result_df = result_df.withColumn("ai_results_struct", empty_entity_array)
if "presidio_results_struct" not in result_df.columns:
    result_df = result_df.withColumn("presidio_results_struct", empty_entity_array)
if "gliner_results_struct" not in result_df.columns:
    result_df = result_df.withColumn("gliner_results_struct", empty_entity_array)

# Create alignment UDF with appropriate source flags
align_udf = align_entities_udf(
    fuzzy_threshold=50,
    include_presidio=use_presidio,
    include_gliner=use_gliner,
    include_ai=use_ai_query
)
```

### 2. Code Cleanup

**alignment.py:**
- Removed unused `Set` import
- Removed unused `FUZZY_MATCH_SCORE` import  
- Renamed unused `score` variable to `_score`

**pipeline.py:**
- Removed unused `Literal` import
- Removed unused `lit` import
- Removed unused `source_df` variable

## Backward Compatibility

✅ **Maintained**: The changes are fully backward compatible:

1. **Existing notebooks work**: Notebooks that use `use_presidio=True` and `use_ai_query=True` will continue to work
2. **New functionality**: Now supports GLiNER as a third source
3. **Flexible combinations**: Works with any combination of 1-3 sources
4. **Legacy function preserved**: `align_entities_row()` still exists for old code

## Testing Recommendations

Test these scenarios to verify the fix:

### 1. Two Sources (Presidio + AI) - Original Use Case
```python
result_df = run_detection_pipeline(
    spark=spark,
    source_df=source_df,
    doc_id_column="doc_id",
    text_column="text",
    use_presidio=True,
    use_ai_query=True,
    use_gliner=False,
    endpoint="databricks-claude-sonnet-4",
    align_results=True
)
```

### 2. Three Sources (All Methods)
```python
result_df = run_detection_pipeline(
    spark=spark,
    source_df=source_df,
    doc_id_column="doc_id",
    text_column="text",
    use_presidio=True,
    use_ai_query=True,
    use_gliner=True,
    endpoint="databricks-claude-sonnet-4",
    gliner_model="Ihor/gliner-biomed-large-v1.0",
    align_results=True
)
```

### 3. Single Source
```python
result_df = run_detection_pipeline(
    spark=spark,
    source_df=source_df,
    doc_id_column="doc_id",
    text_column="text",
    use_presidio=True,
    use_ai_query=False,
    use_gliner=False,
    align_results=True  # Still works, normalizes single source
)
```

### 4. GLiNER + AI (No Presidio)
```python
result_df = run_detection_pipeline(
    spark=spark,
    source_df=source_df,
    doc_id_column="doc_id",
    text_column="text",
    use_presidio=False,
    use_ai_query=True,
    use_gliner=True,
    endpoint="databricks-claude-sonnet-4",
    align_results=True
)
```

## Expected Behavior

### Output Schema

The aligned entities will have:

```python
{
    "entity": str,           # The entity text
    "entity_type": str,      # Type (PERSON, EMAIL, etc.)
    "start": int,            # Start position
    "end": int,              # End position
    "doc_id": str,           # Document ID
    "presidio_score": float, # Presidio confidence (or null)
    "gliner_score": float,   # GLiNER confidence (or null)
    "ai_score": float,       # AI confidence (or null)
    "confidence": str        # Overall: "high", "medium", "low"
}
```

### Confidence Levels

Based on weighted source agreement:

- **High**: 2+ sources agree (exact match) OR single high-confidence source
- **Medium**: Partial agreement (overlap + fuzzy match)
- **Low**: Single source with low/no confidence score

### Weighting

From `config.py`:
```python
SOURCE_WEIGHTS = {
    "presidio": 0.35,
    "gliner": 0.30,
    "ai": 0.35,
}
```

## Migration Notes

If you have custom code calling the alignment UDF directly, update it:

### Old Pattern
```python
from dbxmetagen.redaction.alignment import align_entities_udf

df = df.withColumn(
    "aligned",
    align_entities_udf()(
        col("ai_entities"),
        col("presidio_entities"),
        col("doc_id")
    )
)
```

### New Pattern
```python
from dbxmetagen.redaction.alignment import align_entities_udf
from pyspark.sql.functions import array, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType

# Add gliner column if missing
if "gliner_entities" not in df.columns:
    empty_array = array().cast(ArrayType(StructType([
        StructField("entity", StringType()),
        StructField("start", IntegerType()),
        StructField("end", IntegerType()),
    ])))
    df = df.withColumn("gliner_entities", empty_array)

# Create UDF with source flags
align_udf = align_entities_udf(
    include_presidio=True,
    include_gliner=False,
    include_ai=True
)

# Apply with all 4 columns
df = df.withColumn(
    "aligned",
    align_udf(
        col("ai_entities"),
        col("presidio_entities"),
        col("gliner_entities"),  # Required even if empty
        col("doc_id")
    )
)
```

## Related Documentation

- **Alignment Module Guide**: `src/dbxmetagen/redaction/ALIGNMENT_MIGRATION_GUIDE.md`
- **API Documentation**: See function docstrings in `alignment.py`
- **Unit Tests**: `tests/test_alignment.py`
- **Example Usage**: `tests/test_alignment_examples.py`

## Summary

The fix ensures that the refactored alignment module works seamlessly with the detection pipeline, supporting flexible multi-source entity alignment while maintaining backward compatibility with existing code.


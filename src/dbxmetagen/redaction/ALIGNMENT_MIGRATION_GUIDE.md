# Entity Alignment Module - Migration Guide

## Overview

The alignment module has been refactored to support flexible multi-source entity alignment with modular, testable code following Python best practices.

## What's New

### 1. Multi-Source Support
- Now supports 1-3 detection sources in any combination: Presidio, GLiNER, AI Query
- Previous version only supported AI + Presidio
- Flexible source selection via function parameters

### 2. Weighted Confidence Scoring
- Replaces simple high/medium/low confidence with weighted scoring
- Based on source agreement and match quality
- Configurable weights per source in `config.py`

### 3. Modular Architecture
- `Entity` dataclass for normalized representation
- `EntityMatcher` logic separated into testable functions
- `MultiSourceAligner` class orchestrates alignment
- Individual functions for match scoring, merging, confidence calculation

### 4. Robust Field Handling
- Extra fields in entity dictionaries don't cause errors
- Only requires minimal fields: `entity`, `start`, `end`
- Flexible schema accommodates different detection outputs

### 5. Comprehensive Testing
- 100+ unit tests covering all functions
- Edge case handling
- Integration tests with realistic medical text

## Migration Guide

### For Simple Two-Source Alignment (AI + Presidio)

**Before:**
```python
from dbxmetagen.redaction.alignment import align_entities_row

result = align_entities_row(
    ai_entities=ai_ents,
    presidio_entities=pres_ents,
    doc_id="doc1"
)
```

**After (Option 1 - Backward Compatible):**
```python
from dbxmetagen.redaction.alignment import align_entities_row

# Same function still works!
result = align_entities_row(
    ai_entities=ai_ents,
    presidio_entities=pres_ents,
    doc_id="doc1"
)
```

**After (Option 2 - New Function):**
```python
from dbxmetagen.redaction.alignment import align_entities_multi_source

result = align_entities_multi_source(
    presidio_entities=pres_ents,
    gliner_entities=None,
    ai_entities=ai_ents,
    doc_id="doc1"
)
```

### For Three-Source Alignment (NEW)

```python
from dbxmetagen.redaction.alignment import align_entities_multi_source

result = align_entities_multi_source(
    presidio_entities=presidio_ents,
    gliner_entities=gliner_ents,
    ai_entities=ai_ents,
    doc_id="doc1"
)
```

### Using the PySpark UDF

**Before:**
```python
from pyspark.sql.functions import col
from dbxmetagen.redaction.alignment import align_entities_udf

align_udf = align_entities_udf(fuzzy_threshold=50)

df = df.withColumn(
    "aligned",
    align_udf(col("ai_entities"), col("presidio_entities"), col("doc_id"))
)
```

**After:**
```python
from pyspark.sql.functions import col
from dbxmetagen.redaction.alignment import align_entities_udf

# Specify which sources to include
align_udf = align_entities_udf(
    fuzzy_threshold=50,
    include_presidio=True,
    include_gliner=True,  # NEW: Enable GLiNER
    include_ai=True
)

# Note: All columns must be provided, even if not used
df = df.withColumn(
    "aligned",
    align_udf(
        col("ai_entities"),
        col("presidio_entities"),
        col("gliner_entities"),  # NEW column
        col("doc_id")
    )
)
```

### Using the MultiSourceAligner Class

```python
from dbxmetagen.redaction.alignment import MultiSourceAligner

# Create aligner with custom threshold
aligner = MultiSourceAligner(fuzzy_threshold=60)

# Align entities
result = aligner.align(
    doc_id="doc1",
    presidio_entities=presidio_ents,
    gliner_entities=gliner_ents,
    ai_entities=ai_ents
)
```

## Schema Changes

### Old Schema
```python
{
    "entity": str,
    "entity_type": str,
    "start": int,
    "end": int,
    "doc_id": str,
    "presidio_score": float,  # Only Presidio
    "confidence": str  # high/medium/low
}
```

### New Schema
```python
{
    "entity": str,
    "entity_type": str,
    "start": int,
    "end": int,
    "doc_id": str,
    "presidio_score": float,  # Nullable
    "gliner_score": float,     # NEW - Nullable
    "ai_score": float,         # NEW - Nullable
    "confidence": str          # high/medium/low (weighted)
}
```

**Note:** The new schema is backward compatible. Code expecting only `presidio_score` will continue to work, and the additional score fields will be `None` if those sources aren't used.

## Configuration

New configuration options in `config.py`:

```python
# Source weights for confidence calculation
SOURCE_WEIGHTS = {
    "presidio": 0.35,
    "gliner": 0.30,
    "ai": 0.35,
}

# Match quality thresholds
EXACT_MATCH_SCORE = 1.0
OVERLAP_MATCH_SCORE = 0.7
FUZZY_MATCH_SCORE = 0.5

# Confidence thresholds (weighted)
CONFIDENCE_THRESHOLDS = {
    "high": 0.7,
    "medium": 0.4,
    "low": 0.0,
}
```

## Testing

Run the test suite:

```bash
cd dbxmetagen
pytest tests/test_alignment.py -v
```

Run example usage patterns:

```bash
python tests/test_alignment_examples.py
```

## Breaking Changes

None! The refactored module maintains backward compatibility:
- `align_entities_row()` function still works
- Old UDF signature still supported (with updated internals)
- Schema is backward compatible (new fields are additions)

## Best Practices

1. **Use the new multi-source function** for better flexibility:
   ```python
   align_entities_multi_source(...)
   ```

2. **Specify which sources you're using** in the UDF:
   ```python
   align_entities_udf(include_presidio=True, include_gliner=False, include_ai=True)
   ```

3. **Leverage the MultiSourceAligner class** for custom alignment logic:
   ```python
   aligner = MultiSourceAligner(fuzzy_threshold=custom_value)
   ```

4. **Don't worry about extra fields** in entity dictionaries - they're handled gracefully

5. **Test alignment quality** by examining the confidence scores and reviewing matched entities

## Troubleshooting

### Issue: Entities not matching as expected

**Solution:** Adjust the `fuzzy_threshold` parameter:
- Lower value (30-50): More lenient matching
- Higher value (70-90): Stricter matching

### Issue: Confidence scores seem wrong

**Solution:** Check the source weights in `config.py` and adjust based on your use case. Some sources may be more reliable than others.

### Issue: Too many duplicate entities

**Solution:** The aligner tries to match entities across sources. If you're getting duplicates, check that:
1. `doc_id` fields are consistent
2. Position information is accurate
3. Entity text is properly normalized

### Issue: Missing entities in output

**Solution:** 
1. Verify all source entity lists are being passed correctly
2. Check that `doc_id` filtering isn't excluding entities
3. Ensure entity dictionaries have required fields: `entity`, `start`, `end`

## Performance Considerations

- The aligner processes entities sequentially within each document
- For large batches, use the PySpark UDF for parallelization
- Match scoring uses fuzzy matching which can be CPU-intensive with very long entity texts

## Future Enhancements

Potential improvements for future versions:
- Configurable entity type preference order
- Support for confidence boost/penalty rules
- Integration with entity linking/normalization
- Performance optimizations for large entity lists


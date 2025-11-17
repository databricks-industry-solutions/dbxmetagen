# Implementation Summary - Alignment Investigation

## What Was Implemented

### 1. Added Alignment Diagnostic Section to Evaluation Notebook

**File**: `notebooks/redaction/2. Benchmarking Evaluation.py`

**Location**: New section added after line 529, before "Cross-Dataset Comparison"

**Features**:

#### a) Entity Count Comparison
Shows total and unique entity counts for each detection method:
```
  presidio    :   100 total |    95 unique
  ai          :    90 total |    88 unique
  gliner      :    50 total |    48 unique
  aligned     :   110 total |   105 unique
```

#### b) Union vs Dedup Behavior Check
Compares aligned count against sum of individual methods:
```
  Sum of individual methods: 240
  Aligned result count:      110
  ⚠ Aligned has 10 MORE entities than largest source (union behavior)
```

#### c) Alignment Artifacts Detection
Finds entities in aligned that don't exist in ANY individual method (created by boundary modifications):
```
  ⚠ Found 6 entities in aligned that don't match ANY individual detection
    These are likely boundary modifications by alignment
    Sample artifacts:
    doc_id  entity             start  end
    1       CA SHUFF , M.D.    100    116
    1       M.D.               105    108
```

#### d) Missing Entity Detection
Identifies entities present in individual methods but missing from aligned:
```
  ⚠ presidio   :   5 entities not in aligned
    Sample missing entities from presidio:
    doc_id  entity     start  end
    2       718719     4732   4738
```

#### e) Specific Case Investigation
Checks specific problematic cases mentioned by user:
```
  5. Specific Case Investigation (Doc ID 2, pos 4732-4738):
    ✓ gliner    : Found entity at position 4732-4738
    ✗ aligned   : NOT found
```

### 2. Created Investigation Findings Document

**File**: `ALIGNMENT_INVESTIGATION_FINDINGS.md`

**Contents**:
- **Executive Summary**: Two main issues identified
- **Root Cause Analysis**: 
  - Issue 1: Alignment creates new boundaries (takes longest entity)
  - Issue 2: Alignment uses union (adds all unmatched entities)
  - Issue 3: Missing true positives due to boundary mismatches
- **Impact on Evaluation**: Why FPs increase and FNs persist
- **Redaction Analysis**: Confirms redaction logic is correct
- **Recommendations**: Three options (A, B, C) with pros/cons
- **Testing Recommendations**: Steps to verify findings
- **Decision Required**: Questions for stakeholder
- **Next Steps**: Action items

### 3. Verified Redaction Implementation

**Files Reviewed**:
- `src/dbxmetagen/redaction/redaction.py`
- `src/dbxmetagen/redaction/pipeline.py`
- `notebooks/redaction/4. Redaction Pipeline.py`

**Findings**:
✓ Redaction uses `aligned_entities` by default (`use_aligned=True`)
✓ Simple position-based replacement: `text[:start] + "[REDACTED]" + text[end+1:]`
✓ No additional merging or processing
✓ **Redaction logic is NOT the problem** - it correctly uses aligned entities

**Conclusion**: The issue is that aligned entities have incorrect boundaries, not that redaction is using them incorrectly.

## Key Findings

### Root Cause 1: Longest Entity Selection

**Location**: `alignment.py:252`

```python
longest_entity = max(entities, key=lambda e: len(e.entity))
return {
    "entity": longest_entity.entity,
    "start": longest_entity.start,
    "end": longest_entity.end,
    ...
}
```

**Impact**: 
- When Presidio detects "CA SHUFF" and AI detects "CA SHUFF , M.D.", alignment outputs the longer one
- Creates NEW boundaries that don't match ground truth
- Explains why identical-looking entities show as FPs (boundary mismatch)

### Root Cause 2: Union Behavior

**Location**: `alignment.py:471-480`

```python
# Add unmatched entities from other sources
for source in ["presidio", "gliner", "ai"]:
    for idx, entity in enumerate(normalized_entities[source]):
        if idx not in used_entities[source]:
            results.append(merged)  # Add EVERY unmatched entity
```

**Impact**:
- All FPs from individual methods propagate to aligned
- Aligned count = union of all sources (after fuzzy dedup)
- Explains why doc_id 1 has 20 FPs in aligned vs 14 total from individual methods

### Root Cause 3: Missing Entities

**Example**: Doc ID 2, entity at position 4732-4738
- GLiNER detects it correctly
- AI or Presidio detected something overlapping with different boundaries
- Fuzzy matching fails
- Entity gets added but evaluation fails due to boundary mismatch

## Diagnostic Output Examples

### Example 1: Alignment Creates More Entities
```
Presidio:  100 entities
AI:         90 entities  
GLiNER:     50 entities
Aligned:   110 entities  ← More than any individual source!
```

### Example 2: Boundary Modifications
```
Artifacts (in aligned, not in any source):
doc_id  entity             start  end
1       CA SHUFF , M.D.    100    116  ← Combination of two detections
1       M.D.               105    108  ← Partial extraction
```

### Example 3: Lost Entities
```
Missing from aligned (was in GLiNER):
doc_id  entity   start  end
2       718719   4732   4738
```

## Recommendations for User

### Immediate Actions

1. **Run evaluation notebook** with new diagnostic section
2. **Review diagnostic output** to confirm findings
3. **Answer decision questions** in findings document:
   - Maximum recall vs precision?
   - Merge overlapping entities or keep separate?
   - Union vs consensus (require multi-source agreement)?
   - Which methods to evaluate?

### Short-term Solutions

**Option A: For better evaluation accuracy**
- Evaluate individual methods separately (not aligned)
- Aligned results will always have more FPs due to union + boundary modifications

**Option B: For better production redaction**
- Keep alignment but accept that it's conservative (over-redacts)
- Document that aligned results prioritize recall over precision
- Consider using only high-confidence aligned entities

**Option C: Fix alignment logic**
- Modify `merge_entities()` to preserve original boundaries
- Remove unmatched entity addition (lines 471-480)
- Requires code changes and retesting

### Long-term Solutions

Implement configurable alignment strategy:
```python
def align_entities(..., strategy="union"):
    if strategy == "union":
        # Current behavior - include all
    elif strategy == "consensus":
        # Only include if 2+ sources agree
    elif strategy == "intersection":
        # Only include if ALL sources agree
```

## Files Modified

1. `notebooks/redaction/2. Benchmarking Evaluation.py`
   - Added alignment diagnostic section (lines ~530-642)
   - Shows entity counts, artifacts, missing entities, specific cases

2. `ALIGNMENT_INVESTIGATION_FINDINGS.md` (NEW)
   - Comprehensive analysis of alignment issues
   - Root causes with code references
   - Recommendations with pros/cons
   - Decision framework for stakeholder

3. `IMPLEMENTATION_SUMMARY.md` (NEW - this file)
   - Summary of what was implemented
   - Key findings
   - Next steps

## Next Steps

1. User runs evaluation notebook to see diagnostic output
2. User reviews findings document
3. User decides on alignment strategy based on use case:
   - Production redaction: recall vs precision tradeoff
   - Evaluation: individual methods vs aligned
4. Implement selected approach (if code changes needed)
5. Re-run evaluation to verify improvements

## Questions for User

Before making alignment code changes, need to understand requirements:

1. **What's the primary use case?**
   - Evaluation/benchmarking (focus on accuracy metrics)
   - Production redaction (focus on not missing PII)
   - Both (need balanced approach)

2. **For production redaction, which is worse?**
   - Missing PII (false negative in redaction)
   - Redacting non-PII (false positive in redaction)

3. **Should we change alignment or evaluation?**
   - Option A: Fix alignment to be more precise
   - Option B: Keep alignment as-is, evaluate differently
   - Option C: Make alignment configurable

## Testing Plan

When changes are made:

1. Run diagnostic section - confirm artifacts are reduced/eliminated
2. Check doc_id 1 FP count - should decrease from 20
3. Check doc_id 2 missing entity - should be found
4. Verify redaction output - ensure it still works correctly
5. Compare metrics - precision should improve, recall should not decrease significantly


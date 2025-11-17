# Alignment Investigation Findings

## Executive Summary

The alignment process is creating false positives and missing true positives due to two design decisions:

1. **Boundary Modification**: Alignment selects the LONGEST entity text when merging overlapping detections, creating new boundaries that don't match ground truth
2. **Union Behavior**: Alignment adds ALL unmatched entities from all sources, resulting in more entities than any individual method

## Root Cause Analysis

### Issue 1: Alignment Creates New Boundaries

**Location**: `src/dbxmetagen/redaction/alignment.py`, line 252

```python
def merge_entities(entities: List[Entity], match_type: MatchType) -> Dict[str, Any]:
    # Select the longest entity text (most specific)
    longest_entity = max(entities, key=lambda e: len(e.entity))
    
    return {
        "entity": longest_entity.entity,
        "start": longest_entity.start,
        "end": longest_entity.end,
        ...
    }
```

**Problem**: When multiple methods detect overlapping entities with different boundaries, alignment takes the LONGEST one and uses its start/end positions.

**Example**:
- Presidio detects: "CA SHUFF" at position 100-108
- AI detects: "CA SHUFF , M.D." at position 100-116
- **Alignment output**: "CA SHUFF , M.D." at position 100-116
- **Ground truth**: "CA SHUFF" at position 100-108
- **Result**: FALSE POSITIVE (boundary mismatch with GT)

This explains why identical-looking entities in visual inspection still show as FPs - the boundaries don't match exactly.

### Issue 2: Alignment Uses Union (Not Intersection)

**Location**: `src/dbxmetagen/redaction/alignment.py`, lines 471-480

```python
# Add unmatched entities from other sources
for source in ["presidio", "gliner", "ai"]:
    if source == primary_source:
        continue
    
    for idx, entity in enumerate(normalized_entities[source]):
        if idx not in used_entities[source]:
            # Add entity that was NOT matched
            merged = merge_entities([entity], MatchType.EXACT)
            results.append(merged)
```

**Problem**: ALL unmatched entities from ALL sources get added to aligned results.

**Example**:
- Presidio detects: 100 entities (80 valid, 20 FPs)
- AI detects: 90 entities (80 valid, 10 FPs)  
- 80 entities match between them
- **Alignment output**: 80 matched + 20 unmatched Presidio + 10 unmatched AI = **110 entities**
- **Result**: All individual FPs propagate to aligned + boundary modifications create NEW FPs

This explains:
- Why aligned has MORE entities than the largest source (doc_id 1: 14 FPs → 20 FPs)
- Why aligned shows FPs not present in any individual method (the new boundaries)

### Issue 3: Missing True Positives

**Example**: Doc ID 2, entity "718719" at position 4732-4738

**Problem**: If an entity is detected by a non-primary source (e.g., GLiNER) but:
1. The primary source (e.g., AI or Presidio) detected something overlapping with DIFFERENT boundaries
2. The fuzzy matching fails to match them
3. The non-primary entity gets added but with potentially modified position during alignment

**Result**: Entity may be in aligned but with wrong boundaries, causing evaluation mismatch.

## Impact on Evaluation

### False Positives Increase
1. **Boundary modifications** create entities with positions that don't match GT
2. **Union behavior** propagates all individual method FPs
3. **New combinations** from longest-text selection create artifacts

### False Negatives Persist
1. Entities with modified boundaries don't match GT in evaluation
2. Even if the text is correct, position mismatch causes FN

### Example from Data

**Doc ID 1 FPs**:
```
Individual methods total FPs: 14
  - Presidio: 2 FPs
  - AI: 0 FPs  
  - GLiNER: ~12 FPs

Aligned FPs: 20

New FPs (6 entities) are likely:
  - Boundary modifications (longest entity selected)
  - Combinations like "CA SHUFF" + ", M.D." → "CA SHUFF , M.D."
```

## Redaction Analysis

**Location**: `src/dbxmetagen/redaction/redaction.py`, `notebooks/redaction/4. Redaction Pipeline.py`

**Finding**: Redaction logic is CORRECT and SIMPLE:
- Uses aligned_entities by default (pipeline.py line 296: `use_aligned=True`)
- Simple position-based replacement: `text[:start] + "[REDACTED]" + text[end+1:]`
- No additional merging or processing

**Conclusion**: Redaction itself is not the problem - it correctly uses aligned entities. The issue is that the aligned entities themselves have incorrect boundaries.

## Recommendations

### Option A: Fix Alignment Logic (Preferred for Production)

**Preserve original boundaries instead of taking longest**:

```python
def merge_entities(entities: List[Entity], match_type: MatchType) -> Dict[str, Any]:
    # Instead of longest, use UNION of boundaries (earliest start, latest end)
    # OR use PRIMARY source boundary (don't modify it)
    
    # Option A1: Use union of all boundaries
    min_start = min(e.start for e in entities)
    max_end = max(e.end for e in entities)
    entity_text = text[min_start:max_end+1]  # Extract from source text
    
    # Option A2: Use primary entity boundaries (first in list)
    primary_entity = entities[0]
    
    return {
        "entity": entity_text,
        "start": min_start,
        "end": max_end,
        ...
    }
```

**Pros**: 
- More conservative redaction (covers all detected boundaries)
- Improves recall (catches wider spans)

**Cons**: 
- May decrease precision (larger spans = more FPs)

### Option B: Use Intersection (Consensus) Instead of Union

**Require agreement from multiple sources**:

```python
# Don't add unmatched entities - only include entities detected by 2+ sources
for primary_idx, primary_entity in enumerate(normalized_entities[primary_source]):
    matches = [primary_entity]
    
    # Find matches in other sources
    for other_source in ["presidio", "gliner", "ai"]:
        # ... find matches ...
    
    # Only include if matched by at least 2 sources
    if len(matches) >= 2:
        merged = merge_entities(matches, overall_match_type)
        results.append(merged)

# Remove lines 471-480 (don't add unmatched entities)
```

**Pros**: 
- Fewer FPs (only high-confidence entities)
- Better precision

**Cons**: 
- Fewer detections overall (lower recall)
- Defeats purpose of having multiple methods

### Option C: Keep Individual Method Results (Recommended for Evaluation)

**For evaluation purposes, compare individual methods instead of aligned**:
- Presidio vs GT
- AI vs GT
- GLiNER vs GT
- Aligned vs GT (with understanding of its behavior)

**For production redaction, use most conservative approach**:
- Option: Union of all individual detections (no alignment merging)
- Ensures maximum coverage but may over-redact

## Testing Recommendations

1. **Run diagnostic notebook section** to confirm findings
2. **Compare per-document FP counts**: individual vs aligned
3. **Identify specific alignment artifacts**: entities in aligned but not in any source
4. **Check boundary modifications**: same entity text, different positions
5. **Verify redaction quality**: does it over-redact or under-redact?

## Decision Required

**Questions for stakeholder**:

1. **For production redaction, what's more important?**
   - a) Maximum recall (catch everything, accept over-redaction)
   - b) Maximum precision (minimize false redactions)
   - c) Balanced approach

2. **Should alignment merge overlapping entities?**
   - a) Yes, use longest span (current behavior)
   - b) Yes, use union of spans (most conservative)
   - c) No, keep individual boundaries separate
   - d) Only merge if exact match

3. **Should alignment include entities detected by only one method?**
   - a) Yes, union of all (current behavior)
   - b) No, require 2+ source agreement (consensus)
   - c) Configurable threshold

4. **For evaluation, which should we measure?**
   - a) Individual methods only
   - b) Aligned only
   - c) Both (current approach)

## Next Steps

1. Run the new diagnostic section in evaluation notebook
2. Review diagnostic output with findings
3. Make decision on alignment strategy
4. Implement selected approach
5. Re-run evaluation to verify improvements


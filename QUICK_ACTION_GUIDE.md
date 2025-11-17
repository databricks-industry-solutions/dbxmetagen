# Quick Action Guide - Alignment Issues

## TL;DR - What's Wrong

1. **Alignment creates false positives** by taking the longest entity text and modifying boundaries
2. **Alignment creates more entities** than any individual method (union behavior)
3. **Redaction code is fine** - the problem is in the aligned entities themselves

## Immediate Next Steps

### Step 1: Run Diagnostic (5 minutes)

Open `notebooks/redaction/2. Benchmarking Evaluation.py` and run it completely.

**New section added**: "Alignment Diagnostic Analysis" (after multi-strategy breakdown)

**What to look for**:
- ⚠ "Aligned has X MORE entities than largest source"
- ⚠ "Found X entities in aligned that don't match ANY individual detection"
- ⚠ "X entities not in aligned" for each method

### Step 2: Review Findings (10 minutes)

Read `ALIGNMENT_INVESTIGATION_FINDINGS.md`

**Focus on**:
- Root Cause Analysis (explains the 2 main issues)
- Impact on Evaluation (why you see FPs/FNs)
- Recommendations (3 options with pros/cons)

### Step 3: Make Decisions (need your input!)

Answer these questions:

**Q1: What's your primary goal?**
- a) Accurate evaluation metrics (measure detection quality)
- b) Production redaction (minimize PII leakage)
- c) Both equally important

**Q2: For production, which is worse?**
- a) Missing PII in redaction (false negative)
- b) Redacting non-PII (false positive)

**Q3: Current alignment behavior**
- **Pros**: Maximizes recall (catches everything any method found)
- **Cons**: Lower precision (more false positives)
- **Keep it?**: Yes / No / Make it configurable

**Q4: Should evaluation measure aligned results?**
- a) Yes, aligned is what we use in production
- b) No, measure individual methods only
- c) Both, but understand aligned will have more FPs

## Common Scenarios & Recommendations

### Scenario 1: "I care most about evaluation accuracy"

**Recommendation**: 
- Evaluate INDIVIDUAL methods, not aligned
- Use aligned for production redaction (it's conservative)
- Document that aligned prioritizes recall over precision

**Why**: Alignment's union behavior and boundary modifications will always show more FPs in evaluation.

**Action**: Update evaluation to focus on individual method metrics, treat aligned as "ensemble" approach.

---

### Scenario 2: "I care most about production redaction quality"

**Recommendation**:
- KEEP current alignment (union + longest entity)
- Accept over-redaction as acceptable tradeoff
- Monitor false positive rate but prioritize recall

**Why**: Missing PII is worse than redacting extra text. Current alignment is conservative.

**Action**: No code changes needed. Document behavior and set expectations.

---

### Scenario 3: "I need high precision AND high recall"

**Recommendation**:
- Make alignment configurable (consensus vs union)
- Use consensus mode (require 2+ sources) for evaluation
- Use union mode (current) for production redaction
- Add confidence thresholds

**Why**: Different use cases need different tradeoffs.

**Action**: Implement configurable alignment strategy (requires code changes).

---

### Scenario 4: "Evaluation shows too many FPs, but I trust the methods"

**Recommendation**:
- Check if FPs are due to boundary mismatches (not wrong detections)
- Use "partial match" evaluation (already implemented)
- Focus on text match quality, not exact position match

**Why**: Many "FPs" are actually correct detections with slightly different boundaries.

**Action**: Review partial match metrics instead of complete overlap metrics.

## Code Changes Required (If Needed)

### Option A: Preserve Original Boundaries (Medium effort)

**File**: `src/dbxmetagen/redaction/alignment.py`

**Change**: Line 252 in `merge_entities()`

```python
# BEFORE (takes longest)
longest_entity = max(entities, key=lambda e: len(e.entity))
return {
    "entity": longest_entity.entity,
    "start": longest_entity.start,
    "end": longest_entity.end,
}

# AFTER (uses union of boundaries - most conservative)
min_start = min(e.start for e in entities)
max_end = max(e.end for e in entities)
# Note: Would need access to original text to extract entity text
return {
    "entity": "[combined]",  # Or extract from text
    "start": min_start,
    "end": max_end,
}

# OR (use primary entity boundaries - no modification)
primary_entity = entities[0]
return {
    "entity": primary_entity.entity,
    "start": primary_entity.start,
    "end": primary_entity.end,
}
```

### Option B: Require Consensus (Low effort)

**File**: `src/dbxmetagen/redaction/alignment.py`

**Change**: Lines 471-480, add condition

```python
# BEFORE (adds all unmatched)
for source in ["presidio", "gliner", "ai"]:
    if source == primary_source:
        continue
    for idx, entity in enumerate(normalized_entities[source]):
        if idx not in used_entities[source]:
            results.append(merged)  # Always add

# AFTER (only add if high confidence or from specific source)
for source in ["presidio", "gliner", "ai"]:
    if source == primary_source:
        continue
    for idx, entity in enumerate(normalized_entities[source]):
        if idx not in used_entities[source]:
            # Only add if confidence is high
            if entity.score and entity.score > 0.8:  # Configurable threshold
                results.append(merged)
```

### Option C: Make It Configurable (High effort, best long-term)

**File**: `src/dbxmetagen/redaction/alignment.py`

**Add**: Parameters to control behavior

```python
class MultiSourceAligner:
    def __init__(
        self,
        fuzzy_threshold: int = 80,
        strategy: str = "union",  # "union", "consensus", "intersection"
        require_min_sources: int = 1,  # 1=union, 2=consensus, 3=intersection
        boundary_strategy: str = "longest",  # "longest", "union", "primary"
    ):
        self.strategy = strategy
        self.require_min_sources = require_min_sources
        self.boundary_strategy = boundary_strategy
```

## Testing After Changes

1. **Run diagnostic**: Check artifact count decreased
2. **Check doc_id 1**: FPs should decrease from 20
3. **Check doc_id 2**: Entity at 4732-4738 should be found
4. **Run full evaluation**: Compare metrics before/after
5. **Test redaction**: Ensure it still works correctly

## Quick Decision Tree

```
START: Are you happy with current evaluation results?
│
├─ YES → No changes needed, document behavior
│
└─ NO → What's the main problem?
    │
    ├─ Too many FPs in aligned → 
    │   ├─ Are they wrong detections? → Review alignment (Option B)
    │   └─ Just boundary mismatches? → Use partial match evaluation
    │
    ├─ Missing TPs in aligned →
    │   └─ Check diagnostics for "entities not in aligned"
    │       → Fix boundary matching logic (Option A)
    │
    └─ Need different behavior for evaluation vs production →
        └─ Make alignment configurable (Option C)
```

## Files You Need to Review

1. **`notebooks/redaction/2. Benchmarking Evaluation.py`**
   - Run this to see diagnostics
   - New section: "Alignment Diagnostic Analysis"

2. **`ALIGNMENT_INVESTIGATION_FINDINGS.md`**
   - Complete analysis with code references
   - Recommendations with pros/cons

3. **`IMPLEMENTATION_SUMMARY.md`**
   - What was implemented
   - Key findings summary

4. **`src/dbxmetagen/redaction/alignment.py`**
   - Only if making code changes
   - Lines 252, 471-480 are key areas

5. **`src/dbxmetagen/redaction/redaction.py`**
   - FYI: Redaction logic is correct, no changes needed

## Contact/Questions

If you need clarification on:
- **Diagnostic output**: Run notebook and share specific numbers
- **Which option to choose**: Answer Q1-Q4 above
- **How to implement changes**: Specify which option (A, B, or C)
- **Performance concerns**: Share scale (# docs, # entities)

## Summary

✓ **Investigation complete** - root causes identified  
✓ **Diagnostics added** - run notebook to see specifics  
✓ **Redaction verified** - code is correct, uses aligned entities properly  
⏸ **Waiting on decision** - which approach to take (A, B, C, or no change)

**Recommendation**: Run diagnostics first, review output, then decide if changes are needed.


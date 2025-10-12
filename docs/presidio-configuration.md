# Presidio PII Detection Configuration

## Overview

Presidio provides deterministic (rule-based) PII detection to complement LLM-based classification. It scans sample data for patterns matching credit cards, SSNs, emails, phone numbers, and other identifiers.

## Configuration

### Score Threshold

Set in `variables.advanced.yml`:

```yaml
presidio_score_threshold:
  default: 0.6
```

**Range**: 0.0 to 1.0

**Default**: 0.6 (recommended for most use cases)

### How it works

- Presidio assigns each match a confidence score
- Only matches above the threshold are reported
- Higher threshold = fewer false positives, may miss some real PII
- Lower threshold = more coverage, more false positives

## Threshold Selection

| Threshold | Use Case | Trade-off |
|-----------|----------|-----------|
| 0.5 | Maximum PII coverage | Expect some false positives on numeric IDs |
| 0.6 | **Recommended default** | Good balance of accuracy and coverage |
| 0.7 | Stricter filtering | May miss edge cases, fewer false positives |
| 0.8+ | High-security environments | Minimal false positives, only obvious PII detected |

## When to Adjust

### Increase threshold (0.7+) if:
- Seeing false positives on timestamps, order IDs, or short numeric codes
- Working with data that has many numeric identifiers
- Prefer precision over recall

### Decrease threshold (0.5) if:
- Missing legitimate PII in validation
- Data contains non-standard PII formats
- Prefer recall over precision

## Filtered Entities

The following Presidio entities are **ignored** regardless of threshold:

- `DATE_TIME` - Overly aggressive, matches timestamps and numeric codes

This filtering is hardcoded to prevent common false positives.

## Integration with LLM Classification

Presidio results are provided to the LLM in the PI classification prompt. The LLM:
1. Reviews Presidio detections
2. Adjusts confidence based on agreement/disagreement
3. Makes final classification decision

This hybrid approach combines rule-based precision with LLM contextual understanding.

## Validation

Check Presidio output logs:
```
/Volumes/{catalog}/{schema}/generated_metadata/{user}/{date}/presidio_logs/
```

Each run creates a timestamped file showing:
- Detected entities per column
- Classification results
- Score threshold used

## Example Configurations

### Standard deployment:
```yaml
presidio_score_threshold:
  default: 0.6
```

### Healthcare with many patient IDs:
```yaml
presidio_score_threshold:
  default: 0.7  # Reduce false positives on MRNs, encounter IDs
```

### E-commerce with order/product IDs:
```yaml
presidio_score_threshold:
  default: 0.7  # Reduce false positives on order numbers, SKUs
```

### Financial services:
```yaml
presidio_score_threshold:
  default: 0.5  # Prioritize detecting all PCI data
```

## Troubleshooting

**Issue**: Column with obvious PII (email, SSN) marked as "Non-sensitive"

**Solution**: Lower threshold to 0.5 or check if sample data actually contains PII

---

**Issue**: Random numeric columns flagged as PII/PCI

**Solution**: Increase threshold to 0.7 or verify sample data quality

---

**Issue**: All columns showing "Non-sensitive" 

**Solution**: 
- Verify `include_deterministic_pi` is `true` in `variables.yml`
- Check Presidio logs for analysis results
- Confirm sample data contains actual values (not just column names)


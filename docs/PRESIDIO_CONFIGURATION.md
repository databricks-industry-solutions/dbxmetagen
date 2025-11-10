# Presidio PII Detection Configuration

## Overview

Presidio provides deterministic (rule-based) PII detection to complement LLM-based classification. It scans sample data for patterns matching credit cards, SSNs, emails, phone numbers, and other identifiers.

**Important**: Presidio analyzes **data values only**, not column names. A column named `patient_id` containing values like `100524, 100550` will not be detected as PII unless the values match recognizer patterns (e.g., "PT-100524" or "PATIENT 100524"). Note that the LLM will pay attention to column names though.

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

## Pattern Recognition

### Custom Recognizers

We use custom pattern recognizers instead of Presidio's defaults:

**PCI (Payment Card Industry)**:
- Credit cards with separators: 13-19 digits in 4-digit groups (e.g., "4532 1234 5678 9010")
- Credit cards without separators: 13-19 consecutive digits, excluding date formats (20220214)
- IBAN: Country code + 13-30 alphanumeric chars
- SWIFT: Bank identifier codes
- Requires financial context words nearby (card, payment, visa, etc.)
- **Explicitly excludes**: Date patterns (4-2-2 or 8-digit YYYYMMDD), non-numeric text

**PHI (Protected Health Information)**:
- Medical Record Numbers: "MRN-123456" or "MRN: 123456"
- Patient IDs: "PT-123456", "PATIENT 123456", "PAT-123456"
- Health insurance: 3 letters + 9-12 digits
- Medical licenses: "MD-123456", "RN-123456"
- Requires medical context words nearby (patient, medical, health, etc.)

### Built-in Recognizer Removal

- Added Luhn algo for CC now

### Filtered Entities

The following entities are **ignored** regardless of threshold:

- `DATE_TIME` - Overly aggressive, matches timestamps and numeric codes
- Built-in credit card patterns - Replaced with stricter custom patterns

This filtering prevents common false positives on dates, IDs, and medical terminology.

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

## Troubleshooting

**Issue**: Column with obvious PII (email, first and last name) marked as "Non-sensitive"

**Solution**: Lower threshold to 0.5 or check if sample data actually contains PII that is able to be captured without column names

---

**Issue**: Column named `patient_id` or `customer_id` not detected as PII

**Explanation**: Presidio only analyzes data values. Plain numeric IDs (e.g., `100524`) without prefixes are not recognized.

**Solutions**:
- Rely on LLM classification (sees column names + context)
- If data format allows, add prefixes: "PT-100524" instead of "100524"
- Use custom recognizers with looser patterns (may increase false positives)

---

**Issue**: Dates or medical terms flagged as PCI (CREDIT_CARD)

**Solution**: This was caused by Presidio's built-in CreditCardRecognizer. Rather than disabling Recognizer, added Luhn algorithm.

---

**Issue**: Random numeric columns flagged as PII/PCI

**Solution**: Increase threshold to 0.7 or verify sample data quality

---

**Issue**: All columns showing "Non-sensitive" 

**Solution**: 
- Verify `include_deterministic_pi` is `true` in `variables.yml`
- Check Presidio logs for analysis results
- Confirm sample data contains actual values (not just column names)


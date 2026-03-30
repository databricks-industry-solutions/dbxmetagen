"""
Custom scorers for dbxmetagen evaluation.

Defines both programmatic scorers and LLM judge scorers for evaluating
metadata generation quality. Uses MLflow 3.x scorer and make_judge APIs.

Currently generalized, not intented to be run by end users.
"""

from mlflow.genai.scorers import scorer
from mlflow.genai.judges import make_judge
from typing import List, Literal


# ============================================================================
# Programmatic Scorers (Deterministic, per-row)
# ============================================================================


@scorer
def pii_detection_accuracy(outputs: dict, expectations: dict) -> float:
    """Calculate PII classification accuracy for PI mode."""
    if not isinstance(outputs, dict) or not isinstance(expectations, dict):
        return 0.0

    pred_response = outputs.get("response", {})
    if "classification" not in expectations:
        return 0.0

    gt_classifications = expectations.get("classification", [])
    pred_classifications = pred_response.get("classification", [])

    total_correct = sum(
        1 for gt, pred in zip(gt_classifications, pred_classifications)
        if gt == pred
    )
    total = min(len(gt_classifications), len(pred_classifications))
    return total_correct / total if total > 0 else 0.0


@scorer
def comment_completeness(outputs: dict, expectations: dict) -> float:
    """Check if all expected columns received comments."""
    if not isinstance(outputs, dict) or not isinstance(expectations, dict):
        return 0.0

    pred_response = outputs.get("response", {})
    expected_cols = set(expectations.get("columns", []))
    predicted_cols = set(pred_response.get("columns", []))

    if not expected_cols:
        return 0.0
    return len(expected_cols & predicted_cols) / len(expected_cols)


@scorer
def comment_length_appropriateness(outputs: dict) -> float:
    """Check if comment lengths are appropriate (not too short or too long)."""
    if not isinstance(outputs, dict):
        return 0.0

    pred_response = outputs.get("response", {})
    scores = []

    table_comment = pred_response.get("table", "")
    if table_comment:
        length = len(table_comment)
        if 100 <= length <= 300:
            scores.append(1.0)
        elif 50 <= length < 100 or 300 < length <= 400:
            scores.append(0.7)
        elif length < 50:
            scores.append(0.3)
        else:
            scores.append(0.5)

    for comment in pred_response.get("column_contents", []):
        length = len(comment)
        if 50 <= length <= 200:
            scores.append(1.0)
        elif 30 <= length < 50 or 200 < length <= 300:
            scores.append(0.7)
        elif length < 30:
            scores.append(0.3)
        else:
            scores.append(0.5)

    return sum(scores) / len(scores) if scores else 0.0


# ============================================================================
# LLM Judge Scorers (via make_judge)
# ============================================================================

comment_quality_judge = make_judge(
    name="comment_quality",
    instructions=(
        "You are evaluating metadata comments generated for a database table.\n\n"
        "**Table Info (inputs):**\n{{ inputs }}\n\n"
        "**Generated Comment (outputs):**\n{{ outputs }}\n\n"
        "**Expected/Ground Truth (expectations):**\n{{ expectations }}\n\n"
        "Evaluate the generated comment on a scale of 1-5:\n\n"
        "5 = Excellent: Accurate description of table/column purpose, identifies all "
        "key relationships/constraints, concise yet complete, properly identifies PII, "
        "professional language\n"
        "4 = Good: Mostly accurate with minor omissions\n"
        "3 = Fair: Somewhat accurate but missing important details\n"
        "2 = Poor: Significant inaccuracies or missing critical information\n"
        "1 = Very Poor: Completely inaccurate or unhelpful\n\n"
        "Return your score as one of: 1, 2, 3, 4, 5"
    ),
    feedback_value_type=Literal["1", "2", "3", "4", "5"],
    model="databricks:/databricks-claude-sonnet-4-6",
)

pii_accuracy_judge = make_judge(
    name="pii_classification_accuracy",
    instructions=(
        "You are evaluating PII (Personally Identifiable Information) classifications.\n\n"
        "**Table Info (inputs):**\n{{ inputs }}\n\n"
        "**Generated Classifications (outputs):**\n{{ outputs }}\n\n"
        "**Expected Classifications (expectations):**\n{{ expectations }}\n\n"
        "Evaluate PII classification accuracy on a scale of 1-5:\n\n"
        "5 = Perfect: All columns correctly classified\n"
        "4 = Good: 80%+ correct, minor errors only\n"
        "3 = Fair: 60-80% correct, some significant misses\n"
        "2 = Poor: <60% correct, missed important PII\n"
        "1 = Very Poor: Fundamentally wrong, missed critical PII like SSN/email\n\n"
        "Focus especially on whether sensitive data (SSN, email, phone, names, "
        "addresses) is correctly identified.\n"
        "Return your score as one of: 1, 2, 3, 4, 5"
    ),
    feedback_value_type=Literal["1", "2", "3", "4", "5"],
    model="databricks:/databricks-claude-sonnet-4-6",
)

technical_accuracy_judge = make_judge(
    name="technical_accuracy",
    instructions=(
        "Evaluate the technical accuracy of the metadata description.\n\n"
        "**Inputs (including sample data):**\n{{ inputs }}\n\n"
        "**Generated Description (outputs):**\n{{ outputs }}\n\n"
        "**Expected Description (expectations):**\n{{ expectations }}\n\n"
        "Rate technical accuracy (1-5):\n\n"
        "5 = All technical details correct (types, relationships, constraints)\n"
        "4 = Mostly correct, minor technical errors\n"
        "3 = Some technical errors or omissions\n"
        "2 = Significant technical inaccuracies\n"
        "1 = Fundamentally wrong technical understanding\n\n"
        "Check: data types, foreign keys, primary keys, null handling, formats, "
        "patterns.\n"
        "Return your score as one of: 1, 2, 3, 4, 5"
    ),
    feedback_value_type=Literal["1", "2", "3", "4", "5"],
    model="databricks:/databricks-claude-sonnet-4-6",
)


# ============================================================================
# Scorer Collections by Mode
# ============================================================================


def get_scorers_for_mode(mode: str) -> List:
    """Get appropriate scorers for a specific mode."""
    if mode == "comment":
        return [
            comment_quality_judge,
            technical_accuracy_judge,
            comment_completeness,
            comment_length_appropriateness,
        ]
    elif mode == "pi":
        return [pii_accuracy_judge, pii_detection_accuracy]
    return []


if __name__ == "__main__":
    print("Available scorers:")
    print("\nComment Mode:")
    for s in get_scorers_for_mode("comment"):
        name = s.name if hasattr(s, "name") else s.__name__
        print(f"  - {name}")

    print("\nPI Mode:")
    for s in get_scorers_for_mode("pi"):
        name = s.name if hasattr(s, "name") else s.__name__
        print(f"  - {name}")

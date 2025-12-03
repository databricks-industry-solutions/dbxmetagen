"""
Custom metrics for dbxmetagen evaluation.

Defines both programmatic metrics and LLM-as-judge metrics for evaluating
metadata generation quality.
"""

import mlflow
from mlflow.metrics import make_genai_metric, MetricValue
from typing import Dict, Any, List


# ============================================================================
# Programmatic Metrics (Deterministic)
# ============================================================================


def pii_detection_accuracy(
    predictions: List[Dict], targets: List[Dict], metrics: Dict
) -> MetricValue:
    """
    Calculate PII classification accuracy for PI mode.

    Compares predicted vs ground truth PII classifications at column level.
    """
    total_correct = 0
    total_columns = 0

    for pred, gt in zip(predictions, targets):
        if not isinstance(pred, dict) or not isinstance(gt, dict):
            continue

        # Extract response
        pred_response = pred.get("response", {}) if isinstance(pred, dict) else {}

        # Check if this is PI mode evaluation
        if "classification" not in gt:
            continue

        gt_classifications = gt.get("classification", [])
        pred_classifications = pred_response.get("classification", [])

        # Compare each column
        for gt_class, pred_class in zip(gt_classifications, pred_classifications):
            total_columns += 1
            if gt_class == pred_class:
                total_correct += 1

    accuracy = total_correct / total_columns if total_columns > 0 else 0.0

    return MetricValue(scores=[accuracy], aggregate_results={"mean": accuracy})


def comment_completeness(
    predictions: List[Dict], targets: List[Dict], metrics: Dict
) -> MetricValue:
    """
    Check if all expected columns have comments in comment mode.

    Measures what percentage of columns received comments.
    """
    completeness_scores = []

    for pred, gt in zip(predictions, targets):
        if not isinstance(pred, dict) or not isinstance(gt, dict):
            continue

        pred_response = pred.get("response", {}) if isinstance(pred, dict) else {}

        # Check if this is comment mode evaluation
        if "columns" not in gt:
            continue

        expected_cols = set(gt.get("columns", []))
        predicted_cols = set(pred_response.get("columns", []))

        if not expected_cols:
            continue

        completeness = len(expected_cols & predicted_cols) / len(expected_cols)
        completeness_scores.append(completeness)

    avg_completeness = (
        sum(completeness_scores) / len(completeness_scores)
        if completeness_scores
        else 0.0
    )

    return MetricValue(
        scores=completeness_scores,
        aggregate_results={
            "mean": avg_completeness,
            "min": min(completeness_scores) if completeness_scores else 0.0,
            "max": max(completeness_scores) if completeness_scores else 0.0,
        },
    )


def comment_length_appropriateness(
    predictions: List[Dict], targets: List[Dict], metrics: Dict
) -> MetricValue:
    """
    Check if comment lengths are appropriate (not too short or too long).

    Penalizes comments that are < 50 chars (too brief) or > 500 chars (too verbose).
    """
    length_scores = []

    for pred, gt in zip(predictions, targets):
        if not isinstance(pred, dict):
            continue

        pred_response = pred.get("response", {}) if isinstance(pred, dict) else {}

        # Check table comment length
        table_comment = pred_response.get("table", "")
        if table_comment:
            length = len(table_comment)
            # Score: 1.0 if between 100-300 chars, penalize if outside
            if 100 <= length <= 300:
                score = 1.0
            elif 50 <= length < 100 or 300 < length <= 400:
                score = 0.7  # Slightly short or long
            elif length < 50:
                score = 0.3  # Too short
            else:
                score = 0.5  # Too long
            length_scores.append(score)

        # Check column comment lengths
        column_comments = pred_response.get("column_contents", [])
        for comment in column_comments:
            length = len(comment)
            # Column comments can be shorter: 50-200 ideal
            if 50 <= length <= 200:
                score = 1.0
            elif 30 <= length < 50 or 200 < length <= 300:
                score = 0.7
            elif length < 30:
                score = 0.3
            else:
                score = 0.5
            length_scores.append(score)

    avg_score = sum(length_scores) / len(length_scores) if length_scores else 0.0

    return MetricValue(scores=length_scores, aggregate_results={"mean": avg_score})


# ============================================================================
# LLM-as-Judge Metrics
# ============================================================================

comment_quality_metric = make_genai_metric(
    name="comment_quality",
    definition=(
        "Evaluate the quality of generated table and column comments for database metadata. "
        "Consider technical accuracy, completeness, conciseness, clarity, and usefulness."
    ),
    grading_prompt=(
        "You are evaluating metadata comments generated for a database table.\n\n"
        "**Table Name:** {inputs[table_name]}\n"
        "**Columns:** {inputs[columns]}\n\n"
        "**Generated Comment:**\n"
        "{predictions[response]}\n\n"
        "**Expected/Ground Truth:**\n"
        "{targets}\n\n"
        "Evaluate the generated comment on a scale of 1-5:\n\n"
        "5 = Excellent\n"
        "- Accurate description of table/column purpose\n"
        "- Identifies all key information (relationships, constraints, data types)\n"
        "- Concise yet complete\n"
        "- Properly identifies PII when present\n"
        "- Uses professional, clear language\n\n"
        "4 = Good\n"
        "- Mostly accurate with minor omissions\n"
        "- Covers most key information\n"
        "- Reasonably concise\n"
        "- Generally identifies PII\n\n"
        "3 = Fair\n"
        "- Somewhat accurate but missing important details\n"
        "- Either too brief or too verbose\n"
        "- May miss some PII\n"
        "- Could be clearer\n\n"
        "2 = Poor\n"
        "- Significant inaccuracies\n"
        "- Missing critical information\n"
        "- Misidentifies PII\n"
        "- Unclear or confusing\n\n"
        "1 = Very Poor\n"
        "- Completely inaccurate or unhelpful\n"
        "- No useful information\n"
        "- Fundamentally misunderstands data\n\n"
        "Provide your score and a brief (1-2 sentence) justification."
    ),
    model="endpoints:/databricks-claude-3-7-sonnet",
    parameters={"temperature": 0.0, "max_tokens": 500},
    aggregations=["mean", "variance", "p90"],
    greater_is_better=True,
)


pii_accuracy_metric = make_genai_metric(
    name="pii_classification_accuracy",
    definition=(
        "Evaluate the accuracy of PII classification for database columns. "
        "Checks if sensitive data is correctly identified."
    ),
    grading_prompt=(
        "You are evaluating PII (Personally Identifiable Information) classifications.\n\n"
        "**Table:** {inputs[table_name]}\n"
        "**Columns:** {inputs[columns]}\n\n"
        "**Generated Classifications:**\n"
        "{predictions[response]}\n\n"
        "**Expected Classifications:**\n"
        "{targets}\n\n"
        "Evaluate the PII classification accuracy on a scale of 1-5:\n\n"
        "5 = Perfect - All columns correctly classified\n"
        "4 = Good - 80%+ columns correct, minor errors only\n"
        "3 = Fair - 60-80% correct, some significant misses\n"
        "2 = Poor - <60% correct, missed important PII\n"
        "1 = Very Poor - Fundamentally wrong, missed critical PII like SSN/email\n\n"
        "Focus especially on whether sensitive data (SSN, email, phone, names, addresses) "
        "is correctly identified.\n\n"
        "Provide your score and justification."
    ),
    model="endpoints:/databricks-claude-3-7-sonnet",
    parameters={"temperature": 0.0, "max_tokens": 300},
    aggregations=["mean", "variance"],
    greater_is_better=True,
)


technical_accuracy_metric = make_genai_metric(
    name="technical_accuracy",
    definition=(
        "Evaluate whether the generated metadata correctly describes technical aspects "
        "like data types, relationships, constraints, and patterns."
    ),
    grading_prompt=(
        "Evaluate the technical accuracy of the metadata description.\n\n"
        "**Sample Data:**\n"
        "{inputs[sample_data]}\n\n"
        "**Generated Description:**\n"
        "{predictions[response]}\n\n"
        "**Expected Description:**\n"
        "{targets}\n\n"
        "Rate technical accuracy (1-5):\n\n"
        "5 = All technical details correct (types, relationships, constraints)\n"
        "4 = Mostly correct, minor technical errors\n"
        "3 = Some technical errors or omissions\n"
        "2 = Significant technical inaccuracies\n"
        "1 = Fundamentally wrong technical understanding\n\n"
        "Check: data types, foreign keys, primary keys, null handling, formats, patterns.\n\n"
        "Provide score and reasoning."
    ),
    model="endpoints:/databricks-claude-3-7-sonnet",
    parameters={"temperature": 0.0, "max_tokens": 300},
    aggregations=["mean"],
    greater_is_better=True,
)


# ============================================================================
# Metric Collections by Mode
# ============================================================================


def get_metrics_for_mode(mode: str) -> List:
    """
    Get appropriate metrics for a specific mode.

    Args:
        mode: 'comment' or 'pi'

    Returns:
        List of metric functions
    """
    if mode == "comment":
        return [
            comment_quality_metric,
            technical_accuracy_metric,
            comment_completeness,
            comment_length_appropriateness,
        ]
    elif mode == "pi":
        return [pii_accuracy_metric, pii_detection_accuracy]
    else:
        return []


if __name__ == "__main__":
    print("Available metrics:")
    print("\nComment Mode:")
    for metric in get_metrics_for_mode("comment"):
        metric_name = metric.name if hasattr(metric, "name") else metric.__name__
        print(f"  - {metric_name}")

    print("\nPI Mode:")
    for metric in get_metrics_for_mode("pi"):
        metric_name = metric.name if hasattr(metric, "name") else metric.__name__
        print(f"  - {metric_name}")

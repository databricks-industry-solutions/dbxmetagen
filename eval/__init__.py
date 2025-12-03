"""
dbxmetagen Evaluation System

Systematic prompt evaluation framework using MLflow 3.x for testing and
improving metadata generation quality.

Quick Start:
    from eval.evaluation_runner import EvaluationRunner

    runner = EvaluationRunner(experiment_name="/Users/your.email/dbxmetagen_eval")
    runner.run_quick_test()

See eval/README.md for detailed documentation.
"""

__version__ = "0.1.0"

from eval.evaluation_runner import EvaluationRunner
from eval.datasets.evaluation_data import create_eval_dataset
from eval.prompt_extraction import create_model_function
from eval.custom_metrics import get_metrics_for_mode

__all__ = [
    "EvaluationRunner",
    "create_eval_dataset",
    "create_model_function",
    "get_metrics_for_mode",
]

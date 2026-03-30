"""
Main evaluation runner for dbxmetagen prompt testing.

Uses MLflow 3.x mlflow.genai.evaluate() with custom scorers to evaluate
metadata generation across different models and configurations.

Currently generalized, not intented to be run by end users.
"""

import mlflow
import pandas as pd
from typing import List, Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eval.datasets.evaluation_data import create_eval_dataset
from eval.prompt_extraction import create_model_function
from eval.custom_metrics import get_scorers_for_mode


class EvaluationRunner:
    """Manages evaluation experiments for dbxmetagen."""

    def __init__(
        self, experiment_name: str = "/Users/your_email/dbxmetagen_prompt_eval"
    ):
        self.experiment_name = experiment_name
        mlflow.set_experiment(experiment_name)
        print(f"Using MLflow experiment: {experiment_name}")

    def run_single_evaluation(
        self,
        run_name: str,
        mode: str,
        model_endpoint: str,
        temperature: float = 0.1,
        eval_data: Optional[List[dict]] = None,
    ):
        """
        Run a single evaluation configuration.

        Args:
            run_name: Name for this evaluation run
            mode: 'comment' or 'pi'
            model_endpoint: Databricks model serving endpoint
            temperature: LLM temperature
            eval_data: Optional pre-filtered evaluation data
        """
        print(f"\n{'='*70}")
        print(f"Running Evaluation: {run_name}")
        print(f"Mode: {mode} | Model: {model_endpoint} | Temp: {temperature}")
        print(f"{'='*70}\n")

        if eval_data is None:
            full_data = create_eval_dataset(modes=[mode])
            eval_data = [
                row for row in full_data
                if row["inputs"].get("mode") == mode
            ]

        print(f"Evaluation dataset: {len(eval_data)} examples")

        predict_fn = create_model_function(
            mode=mode, model_endpoint=model_endpoint, temperature=temperature
        )

        scorers = get_scorers_for_mode(mode)
        print(f"Using {len(scorers)} scorers")

        with mlflow.start_run(run_name=run_name) as run:
            mlflow.log_param("mode", mode)
            mlflow.log_param("model_endpoint", model_endpoint)
            mlflow.log_param("temperature", temperature)
            mlflow.log_param("num_examples", len(eval_data))

            print(f"MLflow Run ID: {run.info.run_id}")
            print("Running mlflow.genai.evaluate()...")

            results = mlflow.genai.evaluate(
                predict_fn=predict_fn,
                data=eval_data,
                scorers=scorers,
            )

            print(f"\n[OK] Evaluation complete!")
            print(f"\nMetrics summary:")
            for metric_name, value in results.metrics.items():
                print(
                    f"  {metric_name}: {value:.4f}"
                    if isinstance(value, float)
                    else f"  {metric_name}: {value}"
                )

            return results

    def run_model_comparison(
        self, mode: str, models: List[str] = None, temperature: float = 0.1
    ) -> pd.DataFrame:
        """Run evaluation across multiple models for comparison."""
        if models is None:
            models = [
                "databricks-claude-sonnet-4-6",
                "databricks-meta-llama-3-1-405b-instruct",
            ]

        print(f"\n{'#'*70}")
        print(f"# Model Comparison: {mode} mode")
        print(f"# Models: {', '.join(models)}")
        print(f"{'#'*70}\n")

        full_data = create_eval_dataset(modes=[mode])
        eval_data = [
            row for row in full_data
            if row["inputs"].get("mode") == mode
        ]

        results = []
        for model in models:
            run_name = f"{mode}_{model.split('-')[-1]}_t{temperature}"
            result = self.run_single_evaluation(
                run_name=run_name,
                mode=mode,
                model_endpoint=model,
                temperature=temperature,
                eval_data=eval_data,
            )
            results.append({
                "model": model,
                "mode": mode,
                "temperature": temperature,
                **result.metrics,
            })

        comparison_df = pd.DataFrame(results)
        print(f"\n{'='*70}")
        print("Comparison Summary:")
        print(f"{'='*70}")
        print(comparison_df.to_string(index=False))
        return comparison_df

    def run_temperature_sweep(
        self, mode: str, model_endpoint: str, temperatures: List[float] = None
    ) -> pd.DataFrame:
        """Test different temperature settings."""
        if temperatures is None:
            temperatures = [0.0, 0.1, 0.3, 0.5]

        print(f"\n{'#'*70}")
        print(f"# Temperature Sweep: {model_endpoint}")
        print(f"# Mode: {mode} | Temperatures: {temperatures}")
        print(f"{'#'*70}\n")

        full_data = create_eval_dataset(modes=[mode])
        eval_data = [
            row for row in full_data
            if row["inputs"].get("mode") == mode
        ]

        results = []
        for temp in temperatures:
            run_name = f"{mode}_{model_endpoint.split('-')[-1]}_t{temp}"
            result = self.run_single_evaluation(
                run_name=run_name,
                mode=mode,
                model_endpoint=model_endpoint,
                temperature=temp,
                eval_data=eval_data,
            )
            results.append({
                "temperature": temp,
                "model": model_endpoint,
                "mode": mode,
                **result.metrics,
            })

        sweep_df = pd.DataFrame(results)
        print(f"\n{'='*70}")
        print("Temperature Sweep Summary:")
        print(f"{'='*70}")
        print(sweep_df.to_string(index=False))
        return sweep_df


def run_quick_test():
    """Run a quick test evaluation."""
    print("Running quick test evaluation...")
    runner = EvaluationRunner(experiment_name="/Users/your_email/dbxmetagen_eval_test")
    runner.run_single_evaluation(
        run_name="test_comment_claude",
        mode="comment",
        model_endpoint="databricks-claude-sonnet-4-6",
        temperature=0.1,
    )
    print("\n[OK] Test complete! Check MLflow UI for results.")


def run_full_evaluation():
    """Run comprehensive evaluation across models and modes."""
    print("Running full evaluation suite...")
    runner = EvaluationRunner(
        experiment_name="/Users/your_email/dbxmetagen_prompt_eval"
    )

    print("\n" + "=" * 70)
    print("PART 1: Comment Mode Model Comparison")
    print("=" * 70)
    runner.run_model_comparison(
        mode="comment",
        models=[
            "databricks-claude-sonnet-4-6",
            "databricks-meta-llama-3-1-405b-instruct",
        ],
    )

    print("\n" + "=" * 70)
    print("PART 2: PI Mode Model Comparison")
    print("=" * 70)
    runner.run_model_comparison(
        mode="pi",
        models=[
            "databricks-claude-sonnet-4-6",
            "databricks-meta-llama-3-1-405b-instruct",
        ],
    )

    print("\n" + "=" * 70)
    print("PART 3: Temperature Sweep (Claude)")
    print("=" * 70)
    runner.run_temperature_sweep(
        mode="comment",
        model_endpoint="databricks-claude-sonnet-4-6",
        temperatures=[0.0, 0.1, 0.3],
    )

    print("\n" + "=" * 70)
    print("[OK] Full evaluation complete!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Open Databricks MLflow UI")
    print("2. Navigate to your experiment")
    print("3. Compare runs and review predictions")
    print("4. Add human feedback through the UI")
    print("5. Use insights to improve prompts")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_quick_test()
    else:
        run_full_evaluation()

"""Benchmarking utilities for tracking token usage."""

import uuid
import mlflow
from mlflow import MlflowClient
from pyspark.sql import SparkSession
from datetime import datetime


def setup_benchmarking(config):
    """Setup benchmarking."""
    if not config.enable_benchmarking:
        return None

    mlflow.openai.autolog()
    benchmarking_id = str(uuid.uuid4())
    print("Benchmarking ID", benchmarking_id)
    notebook_path = config.notebook_path
    experiment_name = (
        notebook_path
        if notebook_path
        else (
            mlflow.get_experiment(mlflow.active_run().info.experiment_id).name
            if mlflow.active_run()
            else None
        )
    )
    return experiment_name


def log_token_usage(config, experiment_name: str):
    """Log token usage from MLflow traces to a Delta table."""

    if not config.enable_benchmarking:
        return

    try:
        exp = mlflow.get_experiment_by_name(experiment_name)
        if not exp:
            print(f"Experiment {experiment_name} not found, skipping benchmarking")
            return

        client = MlflowClient()
        traces = client.search_traces(
            experiment_ids=[exp.experiment_id],
            max_results=50,
        )

        # Filter to recent traces (last 5 minutes)
        from datetime import datetime, timedelta

        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        recent_traces = [
            trace
            for trace in traces
            if datetime.fromtimestamp(trace.info.timestamp_ms / 1000)
            >= five_minutes_ago
        ]
        traces = recent_traces

        if not traces:
            print("No traces found, skipping benchmarking")
            return

        spark = SparkSession.builder.getOrCreate()
        rows = []

        # Debug: Check what run_ids are actually in the traces
        if traces:
            print(f"[BENCHMARK DEBUG] Checking trace run_ids:")
            for i, trace in enumerate(traces[:3]):  # Show first 3
                trace_run_id = (
                    trace.info.request_metadata.get("run_id")
                    if hasattr(trace.info, "request_metadata")
                    else "N/A"
                )
                print(
                    f"  Trace {i}: run_id={trace_run_id}, request_id={trace.info.request_id}"
                )

        for trace in traces:
            for span in trace.data.spans:
                # Only process "Completions" spans (the actual LLM calls)
                if span.name != "Completions":
                    continue

                usage = span.outputs.get("usage") if span.outputs else None
                if usage:
                    prompt_tokens = usage.get("prompt_tokens")
                    completion_tokens = usage.get("completion_tokens")
                    total_tokens = usage.get("total_tokens")

                    rows.append(
                        {
                            "timestamp": datetime.now(),
                            "trace_id": trace.info.request_id,
                            "span_name": span.name,
                            "prompt_tokens": prompt_tokens,
                            "completion_tokens": completion_tokens,
                            "total_tokens": total_tokens,
                            "job_id": str(config.job_id) if config.job_id else None,
                            "run_id": str(config.run_id),
                        }
                    )

        if rows:
            from pyspark.sql.types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
                TimestampType,
            )

            schema = StructType(
                [
                    StructField("timestamp", TimestampType(), True),
                    StructField("trace_id", StringType(), True),
                    StructField("span_name", StringType(), True),
                    StructField("prompt_tokens", IntegerType(), True),
                    StructField("completion_tokens", IntegerType(), True),
                    StructField("total_tokens", IntegerType(), True),
                    StructField("job_id", StringType(), True),
                    StructField("run_id", StringType(), True),
                ]
            )

            df = spark.createDataFrame(rows, schema=schema)
            table_name = f"{config.catalog_name}.{config.schema_name}.{config.benchmark_table_name}"
            df.write.mode("append").saveAsTable(table_name)

            total_calls = len(rows)
            total_prompt_tokens = sum(r["prompt_tokens"] for r in rows)
            total_completion_tokens = sum(r["completion_tokens"] for r in rows)
            total_tokens = sum(r["total_tokens"] for r in rows)

            print(f"Logged {total_calls} LLM calls to {table_name}")
            print(f"  - Total tokens: {total_tokens}")
            print(f"  - Total prompt tokens: {total_prompt_tokens}")
            print(f"  - Total completion tokens: {total_completion_tokens}")
        else:
            print("No token usage data found in traces")

    except Exception as e:
        print(f"Benchmarking failed: {e}")

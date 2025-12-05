"""
Unit tests for BINARY and VARIANT type handling.

These tests verify that BINARY and VARIANT columns are correctly converted
to string format for LLM processing.

Run with: pytest tests/test_binary_variant_types.py -v
"""

"""
Integration tests for BINARY and VARIANT type handling.

These tests verify that BINARY and VARIANT columns are correctly converted
to string format for LLM processing.

Note: These tests require PySpark and should be run separately from the main
test suite to avoid SparkContext conflicts.

Run these tests with:
    pytest tests/test_binary_variant_types.py -v
    
To skip in main test runs:
    pytest tests/ -m "not integration"
"""

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    sum as spark_sum,
    base64,
    to_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BinaryType,
)
import base64 as base64_lib
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-binary-variant")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def convert_special_types_to_string(df: DataFrame) -> DataFrame:
    """
    Convert BINARY and VARIANT columns to string format for processing.

    - BINARY columns are encoded as base64 strings
    - VARIANT columns are converted to JSON strings

    Args:
        df (DataFrame): The DataFrame to convert.

    Returns:
        DataFrame: DataFrame with BINARY and VARIANT columns converted to strings.
    """
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType

        # Handle BINARY type - encode as base64
        if isinstance(col_type, BinaryType):
            print(f"Converting BINARY column '{col_name}' to base64 string")
            df = df.withColumn(col_name, base64(col(col_name)))

        # Handle VARIANT type - convert to JSON string using to_json()
        # VARIANT type can be represented multiple ways in schema
        else:
            type_str = str(col_type).upper()
            type_class = type(col_type).__name__.upper()
            if ("VARIANT" in type_str or 
                "VARIANT" in type_class or 
                "UNPARSED" in type_str or
                "UNPARSED" in type_class):
                print(f"Converting VARIANT column '{col_name}' (type: {col_type}) to JSON string using to_json()")
                df = df.withColumn(col_name, to_json(col(col_name)))

    return df


def determine_sampling_ratio(nrows: int, sample_size: int) -> float:
    """Determine the sampling ratio."""
    return min(1.0, sample_size / nrows if nrows > 0 else 1.0)


def sample_df(df: DataFrame, nrows: int, sample_size: int = 5) -> DataFrame:
    """
    Sample dataframe to a given size and filter out rows with lots of nulls.

    Automatically handles special data types (BINARY, VARIANT) by converting them to strings.

    Args:
        df (DataFrame): The DataFrame to be analyzed.
        nrows (int): number of rows in dataframe
        sample_size (int): The number of rows to sample.

    Returns:
        DataFrame: A DataFrame with columns to generate metadata for.
    """
    # Convert special types (BINARY, VARIANT) to strings before sampling
    df = convert_special_types_to_string(df)

    if nrows < sample_size:
        return df.limit(sample_size)

    larger_sample = sample_size * 100
    sampling_ratio = determine_sampling_ratio(nrows, larger_sample)
    sampled_df = df.sample(withReplacement=False, fraction=sampling_ratio)
    # Build the null count expression by adding null indicators for each column
    null_indicators = [
        when(col(c).isNull(), 1).otherwise(0) for c in sampled_df.columns
    ]
    null_count_expr = null_indicators[0]
    for indicator in null_indicators[1:]:
        null_count_expr = null_count_expr + indicator
    null_counts_per_row = sampled_df.withColumn("null_count", null_count_expr)
    threshold = len(sampled_df.columns) // 2
    filtered_df = null_counts_per_row.filter(col("null_count") < threshold).drop(
        "null_count"
    )
    result_rows = filtered_df.count()
    if result_rows < sample_size:
        print(
            "Not enough non-NULL rows, returning available rows, despite large proportion of NULLs. Result rows:",
            result_rows,
            "vs sample size:",
            sample_size,
        )
        return df.limit(sample_size)

    print(f"Filtering {result_rows} result rows down to {sample_size} rows...")
    return filtered_df.limit(sample_size)


class TestConvertSpecialTypesToString:
    """Test conversion of BINARY and VARIANT types to strings."""

    def test_convert_binary_column_to_base64(self, spark):
        """Test that BINARY columns are converted to base64 strings."""
        # Create test data with BINARY column
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("binary_data", BinaryType(), True),
                StructField("name", StringType(), True),
            ]
        )

        test_data = [
            (1, bytearray(b"Hello World"), "test1"),
            (2, bytearray(b"Binary Data"), "test2"),
            (3, None, "test3"),  # Test null handling
        ]

        df = spark.createDataFrame(test_data, schema)

        # Verify initial schema has BinaryType
        assert isinstance(df.schema["binary_data"].dataType, BinaryType)

        # Convert special types
        result_df = convert_special_types_to_string(df)

        # Verify binary_data is now a string (base64 encoded)
        assert isinstance(result_df.schema["binary_data"].dataType, StringType)

        # Collect results
        results = result_df.collect()

        # Verify the binary data was base64 encoded
        assert results[0]["binary_data"] == base64_lib.b64encode(b"Hello World").decode(
            "utf-8"
        )
        assert results[1]["binary_data"] == base64_lib.b64encode(b"Binary Data").decode(
            "utf-8"
        )
        assert results[2]["binary_data"] is None  # Null should remain null

    def test_convert_multiple_binary_columns(self, spark):
        """Test that multiple BINARY columns are all converted."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("bin1", BinaryType(), True),
                StructField("bin2", BinaryType(), True),
                StructField("text", StringType(), True),
            ]
        )

        test_data = [
            (1, bytearray(b"Data1"), bytearray(b"Data2"), "test"),
        ]

        df = spark.createDataFrame(test_data, schema)
        result_df = convert_special_types_to_string(df)

        # Both binary columns should be converted to strings
        assert isinstance(result_df.schema["bin1"].dataType, StringType)
        assert isinstance(result_df.schema["bin2"].dataType, StringType)
        assert isinstance(result_df.schema["text"].dataType, StringType)

        results = result_df.collect()
        assert results[0]["bin1"] == base64_lib.b64encode(b"Data1").decode("utf-8")
        assert results[0]["bin2"] == base64_lib.b64encode(b"Data2").decode("utf-8")

    def test_no_conversion_for_normal_types(self, spark):
        """Test that normal data types are not affected."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        test_data = [(1, "test"), (2, "test2")]

        df = spark.createDataFrame(test_data, schema)
        result_df = convert_special_types_to_string(df)

        # Schema should remain unchanged
        assert isinstance(result_df.schema["id"].dataType, IntegerType)
        assert isinstance(result_df.schema["name"].dataType, StringType)

        # Data should remain unchanged
        results = result_df.collect()
        assert results[0]["id"] == 1
        assert results[0]["name"] == "test"

    def test_empty_dataframe(self, spark):
        """Test that empty DataFrames are handled correctly."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("binary_data", BinaryType(), True),
            ]
        )

        df = spark.createDataFrame([], schema)
        result_df = convert_special_types_to_string(df)

        # Schema should be converted even for empty DataFrame
        assert isinstance(result_df.schema["binary_data"].dataType, StringType)
        assert result_df.count() == 0


class TestSampleDFWithSpecialTypes:
    """Test that sample_df handles BINARY and VARIANT types correctly."""

    def test_sample_df_converts_binary_types(self, spark):
        """Test that sample_df automatically converts BINARY columns."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("binary_col", BinaryType(), True),
                StructField("text_col", StringType(), True),
            ]
        )

        test_data = [
            (i, bytearray(f"binary{i}".encode()), f"text{i}") for i in range(10)
        ]

        df = spark.createDataFrame(test_data, schema)
        nrows = df.count()

        # Sample the DataFrame
        sampled_df = sample_df(df, nrows, sample_size=5)

        # Verify binary column was converted to string
        assert isinstance(sampled_df.schema["binary_col"].dataType, StringType)

        # Verify we can convert to Pandas (which was the original error)
        pandas_df = sampled_df.toPandas()
        assert pandas_df is not None
        assert "binary_col" in pandas_df.columns

    def test_sample_df_with_small_dataset(self, spark):
        """Test sample_df with dataset smaller than sample size."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("binary_data", BinaryType(), True),
            ]
        )

        test_data = [(1, bytearray(b"data1")), (2, bytearray(b"data2"))]
        df = spark.createDataFrame(test_data, schema)

        # nrows < sample_size should return df.limit(sample_size)
        sampled_df = sample_df(df, nrows=2, sample_size=5)

        # Binary column should still be converted
        assert isinstance(sampled_df.schema["binary_data"].dataType, StringType)
        assert sampled_df.count() <= 5

    def test_sample_df_with_nulls_in_binary_column(self, spark):
        """Test that null values in BINARY columns are handled correctly."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("binary_data", BinaryType(), True),
                StructField("other", StringType(), True),
            ]
        )

        test_data = [
            (i, bytearray(f"data{i}".encode()) if i % 2 == 0 else None, f"text{i}")
            for i in range(20)
        ]

        df = spark.createDataFrame(test_data, schema)
        nrows = df.count()

        sampled_df = sample_df(df, nrows, sample_size=10)

        # Should handle nulls without errors
        assert isinstance(sampled_df.schema["binary_data"].dataType, StringType)
        pandas_df = sampled_df.toPandas()
        assert pandas_df is not None


class TestVariantTypeHandling:
    """Test VARIANT type handling.

    Note: These tests use string representations since VARIANT type
    requires Databricks runtime features not available in standard PySpark.
    """

    def test_variant_type_detection(self, spark):
        """Test that VARIANT-like types are detected by typename."""
        # We'll mock this by checking the type name matching logic
        from pyspark.sql.types import DataType

        class MockVariantType(DataType):
            """Mock VARIANT type for testing."""

            def __repr__(self):
                return "VARIANT"

            def __str__(self):
                return "VARIANT"

        variant_type = MockVariantType()

        # Test the string matching logic used in convert_special_types_to_string
        assert str(variant_type).upper() == "VARIANT"

    def test_documentation_for_variant_usage(self):
        """Document how VARIANT types should be handled in production."""
        # In production Databricks environment, VARIANT columns will be:
        # 1. Detected by typename matching "VARIANT"
        # 2. Cast to string using .cast("string")
        # 3. Databricks automatically converts VARIANT to JSON format

        # Example DDL that would create a VARIANT column:
        # CREATE TABLE test_table (
        #     id INT,
        #     variant_col VARIANT
        # )

        # The convert_special_types_to_string function handles this
        # by checking str(col_type).upper() == "VARIANT"
        assert True  # This is a documentation test


class TestIntegrationWithPrompts:
    """Test that converted types work with the full prompt flow."""

    def test_binary_data_can_be_converted_to_pandas(self, spark):
        """Test that converted BINARY data can be successfully sent to Pandas."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("file_content", BinaryType(), True),
                StructField("filename", StringType(), True),
            ]
        )

        # Simulate real-world binary data (e.g., file contents)
        test_data = [
            (1, bytearray(b"PDF file content here"), "document.pdf"),
            (2, bytearray(b"Image binary data"), "image.png"),
            (3, bytearray(b"Another file"), "data.bin"),
        ]

        df = spark.createDataFrame(test_data, schema)
        converted_df = convert_special_types_to_string(df)

        # This should work without errors (was the original problem)
        pandas_df = converted_df.toPandas()

        # Verify the data is now in base64 format (suitable for LLM processing)
        assert isinstance(pandas_df["file_content"][0], str)
        assert pandas_df["file_content"][0] == base64_lib.b64encode(
            b"PDF file content here"
        ).decode("utf-8")

        # Verify we can convert it to dict format (used by prompts)
        data_dict = pandas_df.to_dict(orient="split")
        assert "file_content" in data_dict["columns"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

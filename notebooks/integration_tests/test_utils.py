# Databricks notebook source
"""
Integration Test Utilities for dbxmetagen

Provides helper functions for creating test data, running assertions,
and cleaning up test artifacts in integration tests.
"""

import sys

sys.path.append("../../")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
import json
from src.dbxmetagen.databricks_utils import setup_databricks_environment

# Setup Databricks authentication for all integration tests
# This sets DATABRICKS_TOKEN and DATABRICKS_HOST environment variables
setup_databricks_environment(dbutils)


class TestFailure(Exception):
    """Exception raised when a test assertion fails."""

    pass


class IntegrationTestUtils:
    """Utility class for integration testing."""

    def __init__(self, spark, test_catalog, test_schema):
        """Initialize test utilities.

        Args:
            spark: SparkSession
            test_catalog: Catalog to use for tests
            test_schema: Schema to use for tests
        """
        self.spark = spark
        self.test_catalog = test_catalog
        self.test_schema = test_schema
        self.test_artifacts = []  # Track artifacts for cleanup

    def setup_test_environment(self):
        """Setup test catalog, schema, and volume."""
        print(f"Setting up test environment: {self.test_catalog}.{self.test_schema}")

        # Create catalog if it doesn't exist
        try:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.test_catalog}")
            print(f"Catalog {self.test_catalog} ready")
        except Exception as e:
            print(f"Note: Could not create catalog (may need admin): {e}")

        # Create schema
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self.test_catalog}.{self.test_schema}"
        )
        print(f"Schema {self.test_schema} ready")

        # Create volume for test outputs
        try:
            self.spark.sql(
                f"CREATE VOLUME IF NOT EXISTS {self.test_catalog}.{self.test_schema}.test_volume"
            )
            print(f"Volume test_volume ready")
        except Exception as e:
            print(f"Note: Could not create volume: {e}")

    def create_test_table(self, table_name, with_data=True, with_comment=False):
        """Create a test table.

        Args:
            table_name: Name of the table (without catalog.schema prefix)
            with_data: Whether to insert sample data
            with_comment: Whether to add table/column comments

        Returns:
            str: Full table name (catalog.schema.table)
        """
        full_table_name = f"{self.test_catalog}.{self.test_schema}.{table_name}"

        # Drop if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

        # Create table
        comment_clause = "COMMENT 'Existing table comment'" if with_comment else ""
        col_comment = "COMMENT 'Existing column comment'" if with_comment else ""

        self.spark.sql(
            f"""
            CREATE TABLE {full_table_name} (
                id INT {col_comment},
                name STRING,
                email STRING,
                age INT,
                created_date DATE
            ) {comment_clause}
            USING DELTA
        """
        )

        # Insert sample data
        if with_data:
            self.spark.sql(
                f"""
                INSERT INTO {full_table_name} VALUES
                (1, 'Alice Smith', 'alice@example.com', 30, '2024-01-01'),
                (2, 'Bob Jones', 'bob@example.com', 25, '2024-01-02'),
                (3, 'Charlie Brown', 'charlie@example.com', 35, '2024-01-03'),
                (4, 'Diana Prince', 'diana@example.com', 28, '2024-01-04'),
                (5, 'Eve Adams', 'eve@example.com', 32, '2024-01-05')
            """
            )

        self.test_artifacts.append(("table", full_table_name))
        print(f"Created test table: {full_table_name}")
        return full_table_name

    def get_table_comment(self, full_table_name):
        """Get table comment.

        Args:
            full_table_name: Full table name (catalog.schema.table)

        Returns:
            str: Table comment or None
        """
        desc = self.spark.sql(f"DESCRIBE EXTENDED {full_table_name}").collect()
        for row in desc:
            if row.col_name == "Comment":
                return row.data_type if row.data_type != "NULL" else None
        return None

    def get_column_comment(self, full_table_name, column_name):
        """Get column comment.

        Args:
            full_table_name: Full table name
            column_name: Column name

        Returns:
            str: Column comment or None
        """
        desc = self.spark.sql(f"DESCRIBE {full_table_name}").collect()
        for row in desc:
            if row.col_name == column_name:
                return row.comment if hasattr(row, "comment") and row.comment else None
        return None

    def table_exists(self, full_table_name):
        """Check if table exists.

        Args:
            full_table_name: Full table name

        Returns:
            bool: True if table exists
        """
        try:
            self.spark.sql(f"DESCRIBE {full_table_name}")
            return True
        except Exception:
            return False

    def find_temp_tables(self, user_identifier):
        """Find temp metadata generation log tables for a user.

        Args:
            user_identifier: Sanitized user identifier

        Returns:
            list: List of temp table names found
        """
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.test_catalog}.{self.test_schema}"
            ).collect()

            temp_prefix = f"temp_metadata_generation_log_{user_identifier}_"
            temp_tables = [
                row.tableName for row in tables if row.tableName.startswith(temp_prefix)
            ]
            return temp_tables
        except Exception as e:
            print(f"Error finding temp tables: {e}")
            return []

    def find_control_tables(self, user_identifier):
        """Find control tables for a user.

        Args:
            user_identifier: Sanitized user identifier

        Returns:
            list: List of control table names found
        """
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.test_catalog}.{self.test_schema}"
            ).collect()

            control_prefix = f"metadata_control_{user_identifier}"
            control_tables = [
                row.tableName
                for row in tables
                if row.tableName.startswith(control_prefix)
            ]
            return control_tables
        except Exception as e:
            print(f"Error finding control tables: {e}")
            return []

    def cleanup_test_artifacts(self):
        """Clean up all test artifacts created during the test."""
        print("\nCleaning up test artifacts...")
        for artifact_type, artifact_name in self.test_artifacts:
            try:
                if artifact_type == "table":
                    self.spark.sql(f"DROP TABLE IF EXISTS {artifact_name}")
                    print(f"Dropped table: {artifact_name}")
            except Exception as e:
                print(f"Could not cleanup {artifact_name}: {e}")

        self.test_artifacts = []

    # Assertion helpers
    def assert_true(self, condition, message):
        """Assert that condition is True."""
        if not condition:
            raise TestFailure(f"ASSERTION FAILED: {message}")
        print(f"PASS: {message}")

    def assert_false(self, condition, message):
        """Assert that condition is False."""
        if condition:
            raise TestFailure(f"ASSERTION FAILED: {message}")
        print(f"PASS: {message}")

    def assert_equals(self, actual, expected, message):
        """Assert that actual equals expected."""
        if actual != expected:
            raise TestFailure(
                f"ASSERTION FAILED: {message}\n"
                f"  Expected: {expected}\n"
                f"  Actual: {actual}"
            )
        print(f"PASS: {message}")

    def assert_not_none(self, value, message):
        """Assert that value is not None."""
        if value is None:
            raise TestFailure(f"ASSERTION FAILED: {message} (value is None)")
        print(f"PASS: {message}")

    def assert_none(self, value, message):
        """Assert that value is None."""
        if value is not None:
            raise TestFailure(
                f"ASSERTION FAILED: {message} (value is not None: {value})"
            )
        print(f"PASS: {message}")

    def assert_contains(self, text, substring, message):
        """Assert that text contains substring."""
        if substring not in str(text):
            raise TestFailure(
                f"ASSERTION FAILED: {message}\n" f"  Expected '{substring}' in: {text}"
            )
        print(f"PASS: {message}")


def get_metadata_generation_log(spark, catalog, schema, table_name):
    """Query metadata_generation_log for a specific table.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name to search for

    Returns:
        DataFrame or None if table doesn't exist
    """
    from pyspark.sql.utils import AnalysisException

    try:
        log_df = spark.sql(
            f"SELECT * FROM {catalog}.{schema}.metadata_generation_log "
            f"WHERE table_name = '{table_name}' "
            f"ORDER BY _created_at DESC LIMIT 1"
        )
        return log_df
    except AnalysisException:
        return None


def verify_metadata_generation_log(spark, catalog, schema, table_name):
    """Verify that metadata_generation_log has an entry for the table.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name to check (can be full or simple name)

    Returns:
        DataFrame: The log entry if it exists

    Raises:
        TestFailure: If no log entry found
    """
    # Extract simple table name if fully qualified name provided
    simple_table_name = table_name.split(".")[-1] if "." in table_name else table_name

    log_df = get_metadata_generation_log(spark, catalog, schema, simple_table_name)

    if log_df is None or log_df.count() == 0:
        raise TestFailure(
            f"No metadata_generation_log entry found for table {table_name}"
        )

    print(f"  Found metadata_generation_log entry for {table_name}")
    return log_df


def verify_sql_file_exists(spark, catalog, schema, volume_name, user, table_name):
    """Check that SQL DDL file was generated in volume.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        volume_name: Volume name
        user: User identifier (sanitized)
        table_name: Table name

    Returns:
        bool: True if file exists, False otherwise
    """
    from datetime import datetime

    try:
        # Build volume path for today's date
        current_date = datetime.now().strftime("%Y%m%d")
        volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{user}/{current_date}"

        # List files in volume
        files = dbutils.fs.ls(volume_path)

        # Debug: Show all files found
        print(f"  Files in {volume_path}:")
        for file_info in files:
            print(f"    - {file_info.name} ({'dir' if file_info.isDir() else 'file'})")

        # Extract simple table name if fully qualified
        simple_table_name = (
            table_name.split(".")[-1] if "." in table_name else table_name
        )

        # Check for SQL file containing table name
        for file_info in files:
            # Skip directories
            if file_info.isDir():
                continue
            # Check if filename contains table name and ends with .sql
            if simple_table_name in file_info.name and file_info.name.endswith(".sql"):
                print(f"  Found SQL file: {file_info.name}")
                return True

        print(f"  ✗ No SQL file found for {simple_table_name} (.sql extension)")
        return False
    except Exception as e:
        print(f"  ✗ Error checking SQL file: {e}")
        return False


def verify_table_has_comment(spark, full_table_name):
    """Verify table has a comment applied.

    Args:
        spark: SparkSession
        full_table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        str: Comment text if present, None otherwise
    """
    try:
        result = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")
        comment_row = result.filter(result.col_name == "Comment").first()
        if comment_row and comment_row.data_type:
            return comment_row.data_type
        return None
    except Exception as e:
        print(f"  ✗ Error checking table comment: {e}")
        return None


def verify_table_has_tags(spark, full_table_name, expected_tags=None):
    """Verify table has specific tags applied.

    Args:
        spark: SparkSession
        full_table_name: Fully qualified table name (catalog.schema.table)
        expected_tags: Optional list of tag names to check for

    Returns:
        dict: Dictionary of tag key-value pairs, empty dict if no tags
    """
    try:
        result = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")
        tags = {}

        # Look for tag rows in DESCRIBE output
        for row in result.collect():
            if row.col_name and row.col_name.startswith("Tag:"):
                tag_name = row.col_name.replace("Tag:", "").strip()
                tags[tag_name] = row.data_type

        # If specific tags expected, validate them
        if expected_tags:
            for tag in expected_tags:
                if tag not in tags:
                    print(f"  ✗ Expected tag '{tag}' not found")
                    return {}

        if tags:
            print(f"  Found {len(tags)} tag(s): {list(tags.keys())}")

        return tags
    except Exception as e:
        print(f"  ✗ Error checking table tags: {e}")
        return {}


def verify_column_has_comment(spark, full_table_name, column_name):
    """Verify a column has a comment applied.

    Args:
        spark: SparkSession
        full_table_name: Fully qualified table name
        column_name: Column name to check

    Returns:
        str: Comment text if present, None otherwise
    """
    try:
        result = spark.sql(f"DESCRIBE TABLE {full_table_name}")
        col_row = result.filter(result.col_name == column_name).first()
        if col_row and col_row.comment:
            return col_row.comment
        return None
    except Exception as e:
        print(f"  ✗ Error checking column comment: {e}")
        return None


def verify_processing_log_exists(spark, catalog, schema, table_name):
    """Verify processing log entry exists for a table.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name

    Returns:
        bool: True if processing log exists
    """
    from pyspark.sql.utils import AnalysisException

    try:
        log_df = spark.sql(
            f"SELECT * FROM {catalog}.{schema}.metadata_processing_log "
            f"WHERE table_name = '{table_name}' "
            f"ORDER BY timestamp DESC LIMIT 1"
        )
        count = log_df.count()
        if count > 0:
            print(f"  Found processing log entry for {table_name}")
            return True
        else:
            print(f"  ✗ No processing log entry found for {table_name}")
            return False
    except AnalysisException:
        print(f"  ✗ Processing log table does not exist")
        return False


def print_test_header(test_name):
    """Print a formatted test header."""
    print("\n" + "=" * 80)
    print(f"  {test_name}")
    print("=" * 80)


def print_test_result(test_name, passed, error=None):
    """Print final test result."""
    print("\n" + "=" * 80)
    if passed:
        print(f"  TEST PASSED: {test_name}")
    else:
        print(f"  TEST FAILED: {test_name}")
        if error:
            print(f"  Error: {error}")
    print("=" * 80)


def verify_metadata_log_for_mode(spark, catalog, schema, table_name, mode):
    """Verify metadata_generation_log has mode-specific entry with content validation.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name (can be fully qualified)
        mode: Expected mode ('comment', 'pi', or 'domain')

    Returns:
        DataFrame: The log entry for this mode

    Raises:
        TestFailure: If validation fails
    """
    from pyspark.sql.utils import AnalysisException
    from datetime import datetime, timedelta

    simple_table_name = table_name.split(".")[-1] if "." in table_name else table_name

    try:
        # Query for mode-specific entry from last hour
        one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        log_df = spark.sql(
            f"""
            SELECT * FROM {catalog}.{schema}.metadata_generation_log
            WHERE table_name = '{simple_table_name}'
            AND metadata_type = '{mode}'
            AND _created_at >= '{one_hour_ago}'
            ORDER BY _created_at DESC
            LIMIT 1
            """
        )

        if log_df.count() == 0:
            raise TestFailure(
                f"No metadata_generation_log entry found for table '{simple_table_name}' "
                f"with mode '{mode}' in last hour"
            )

        row = log_df.first()

        # Validate metadata_type matches
        if row.metadata_type != mode:
            raise TestFailure(
                f"Metadata type mismatch: expected '{mode}', got '{row.metadata_type}'"
            )

        # Mode-specific content validation
        column_content = str(row.column_content) if row.column_content else ""

        if mode == "comment":
            # Comment mode should have descriptions
            if not column_content or len(column_content) < 10:
                raise TestFailure(
                    f"Comment mode entry has insufficient content (length: {len(column_content)})"
                )
            print(
                f"  Mode '{mode}': Found entry with {len(column_content)} chars of content"
            )

        elif mode == "pi":
            # PI mode should have classification keywords
            pi_keywords = ["PII", "sensitive", "Personal", "classification"]
            if not any(
                keyword.lower() in column_content.lower() for keyword in pi_keywords
            ):
                print(
                    f"  Warning: PI mode content may not contain expected PI classification keywords"
                )
            print(f"  Mode '{mode}': Found entry with PI classifications")

        elif mode == "domain":
            # Domain mode should have domain classification
            if not column_content or len(column_content) < 5:
                raise TestFailure(f"Domain mode entry has insufficient content")
            print(f"  Mode '{mode}': Found entry with domain classification")

        print(f"  Validated metadata_generation_log entry for mode '{mode}'")
        return log_df

    except AnalysisException as e:
        raise TestFailure(f"metadata_generation_log table does not exist: {e}")


def verify_control_table_entry(spark, catalog, schema, control_table_name, table_name):
    """Verify control table has entry for the processed table.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        control_table_name: Name of control table (without catalog.schema prefix)
        table_name: Full table name to check for

    Returns:
        DataFrame: The control table entry

    Raises:
        TestFailure: If validation fails
    """
    from pyspark.sql.utils import AnalysisException
    from datetime import datetime, timedelta

    try:
        control_table_full = f"{catalog}.{schema}.{control_table_name}"

        # Check if control table exists
        if not spark.catalog.tableExists(control_table_full):
            raise TestFailure(f"Control table does not exist: {control_table_full}")

        # Query for table entry
        one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        control_df = spark.sql(
            f"""
            SELECT * FROM {control_table_full}
            WHERE table_name = '{table_name}'
            AND _updated_at >= '{one_hour_ago}'
            ORDER BY _updated_at DESC
            LIMIT 1
            """
        )

        if control_df.count() == 0:
            raise TestFailure(
                f"No control table entry found for '{table_name}' in last hour"
            )

        row = control_df.first()
        print(
            f"  Found control table entry: table_name='{row.table_name}', "
            f"updated_at='{row._updated_at}'"
        )

        return control_df

    except AnalysisException as e:
        raise TestFailure(f"Error querying control table: {e}")


def verify_sql_file_content(spark, catalog, schema, volume_name, user, table_name):
    """Verify SQL file exists and contains valid DDL statements.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        volume_name: Volume name
        user: User identifier (sanitized)
        table_name: Table name

    Returns:
        str: The SQL file content

    Raises:
        TestFailure: If validation fails
    """
    from datetime import datetime

    simple_table_name = table_name.split(".")[-1] if "." in table_name else table_name
    current_date = datetime.now().strftime("%Y%m%d")
    volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{user}/{current_date}"

    try:
        files = dbutils.fs.ls(volume_path)

        # Find SQL file for this table
        sql_files = [
            f for f in files if f.name.endswith(".sql") and simple_table_name in f.name
        ]

        if not sql_files:
            raise TestFailure(
                f"No SQL file found for table '{simple_table_name}' in {volume_path}"
            )

        sql_file = sql_files[0]
        print(f"  Found SQL file: {sql_file.name}")

        # Read file content
        content = dbutils.fs.head(sql_file.path, 100000)  # Read up to 100KB

        if not content or len(content) < 10:
            raise TestFailure(f"SQL file is empty or too small")

        # Validate content has DDL statements
        ddl_keywords = ["ALTER TABLE", "COMMENT ON", "UPDATE", "ALTER"]
        if not any(keyword in content.upper() for keyword in ddl_keywords):
            raise TestFailure(
                f"SQL file does not contain expected DDL keywords (ALTER TABLE, COMMENT ON, etc.)"
            )

        # Validate table name appears in content
        if simple_table_name not in content:
            raise TestFailure(
                f"SQL file does not reference table '{simple_table_name}'"
            )

        print(
            f"  Validated SQL file content ({len(content)} bytes, contains valid DDL)"
        )
        return content

    except Exception as e:
        if "FileNotFoundException" in str(type(e).__name__):
            raise TestFailure(f"Volume path not found: {volume_path}")
        raise TestFailure(f"Error reading SQL file: {e}")


def verify_mode_differentiation(spark, catalog, schema, table_name):
    """Verify all three modes produced different outputs in metadata_generation_log.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name

    Raises:
        TestFailure: If modes didn't produce different outputs
    """
    from pyspark.sql.utils import AnalysisException
    from datetime import datetime, timedelta

    simple_table_name = table_name.split(".")[-1] if "." in table_name else table_name

    try:
        one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Query for all mode entries
        all_modes_df = spark.sql(
            f"""
            SELECT metadata_type, column_content, _created_at
            FROM {catalog}.{schema}.metadata_generation_log
            WHERE table_name = '{simple_table_name}'
            AND _created_at >= '{one_hour_ago}'
            ORDER BY _created_at DESC
            """
        )

        if all_modes_df.count() < 3:
            raise TestFailure(
                f"Expected 3 mode entries (comment, pi, domain), found {all_modes_df.count()}"
            )

        # Collect mode types
        modes = [row.metadata_type for row in all_modes_df.collect()]
        unique_modes = set(modes)

        if len(unique_modes) < 3:
            raise TestFailure(
                f"Expected 3 different modes, found {len(unique_modes)}: {unique_modes}"
            )

        expected_modes = {"comment", "pi", "domain"}
        if unique_modes != expected_modes:
            raise TestFailure(
                f"Mode mismatch: expected {expected_modes}, got {unique_modes}"
            )

        print(f"  Verified all 3 modes (comment, pi, domain) produced log entries")
        print(f"  Total entries in last hour: {all_modes_df.count()}")

    except AnalysisException as e:
        raise TestFailure(f"Error verifying mode differentiation: {e}")

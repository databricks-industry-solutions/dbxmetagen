import os
import re
import logging
from datetime import datetime
from typing import Optional, Union, Tuple
import pandas as pd
import openpyxl
from shutil import copyfile
from pyspark.sql import SparkSession

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.dataframe_utils import split_table_names
from src.dbxmetagen.user_utils import sanitize_user_identifier

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def ensure_directory_exists(path: str) -> None:
    """
    Ensure that a directory exists; create it if it does not.

    Args:
        path (str): Directory path.
    """
    if not os.path.exists(path):
        try:
            os.makedirs(path, exist_ok=True)
            logging.info(f"Created directory: {path}")
        except Exception as e:
            logging.error(f"Failed to create directory {path}: {e}")
            raise


def get_output_file_name(input_file: str, suffix: str) -> str:
    """
    Generate an output file name based on the input file's base name and a suffix.

    Args:
        input_file (str): Input file path.
        suffix (str): Suffix for the output file (e.g., '.sql', '.tsv', '.xlsx').

    Returns:
        str: Output file name.
    """
    base = os.path.splitext(os.path.basename(input_file))[0]
    return f"{base}_reviewed{suffix}"


def load_metadata_file(file_path: str, file_type: str) -> pd.DataFrame:
    """
    Load metadata from a TSV or Excel file.

    Args:
        file_path (str): Path to the input file.
        file_type (str): 'tsv' or 'excel'.

    Returns:
        pd.DataFrame: Loaded data.
    """
    if not os.path.isfile(file_path):
        logging.error(f"Input file does not exist: {file_path}")
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    try:
        if file_type == "tsv":
            df = pd.read_csv(file_path, sep="\t", dtype=str)
        elif file_type == "excel":
            df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        logging.info(f"Loaded file: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to load file {file_path}: {e}")
        raise


def get_comment_from_ddl(ddl: str) -> str:
    """
    Extract comment from DDL.
    """
    match = re.search(r'(COMMENT ON TABLE [^"\']+ IS\s+)(["\'])(.*?)(["\'])', ddl)
    if match:
        return match.group(3)
    return ""


def get_pii_tags_from_ddl(ddl: str, config: MetadataConfig) -> Tuple[str, str]:
    """
    Extract classification and subclassification from DDL statement.
    """
    tag_class = getattr(config, "pi_classification_tag_name", "data_classification")
    tag_subclass = getattr(
        config, "pi_subclassification_tag_name", "data_subclassification"
    )

    if "ALTER COLUMN" not in ddl:
        pattern = rf"ALTER TABLE [^ ]+ SET TAGS \('{tag_class}' = '([^']+)', '{tag_subclass}' = '([^']+)'\);"
    else:
        pattern = rf"ALTER TABLE [^ ]+ ALTER COLUMN [^ ]+ SET TAGS \('{tag_class}' = '([^']+)', '{tag_subclass}' = '([^']+)'\);"

    match = re.search(pattern, ddl)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def replace_comment_in_ddl(ddl: str, new_comment: str) -> str:
    """
    Replace the comment string in a DDL statement with a new comment.
    Uses double quotes to avoid escaping apostrophes in comments.
    """
    is_serverless = False
    if "ALTER TABLE" not in ddl:
        ddl = re.sub(
            r'(COMMENT ON TABLE [^"\']+ IS\s+)(["\'])(.*?)(["\'])',
            lambda m: f'{m.group(1)}"{new_comment}"',
            ddl,
        )
    else:
        dbr_number = os.environ.get("DATABRICKS_RUNTIME_VERSION")
        try:
            dbr_number = float(dbr_number)
        except ValueError:
            is_serverless = bool("client" in dbr_number)
            if is_serverless or dbr_number >= 16:
                ddl = re.sub(
                    r'(COMMENT ON COLUMN [^"\']+ IS\s+)(["\'])(.*?)(["\'])',
                    lambda m: f'{m.group(1)}"{new_comment}"',
                    ddl,
                )
            elif float(dbr_number) >= 14 and float(dbr_number) < 16:
                ddl = re.sub(
                    r'(ALTER TABLE [^ ]+ ALTER COLUMN [^ ]+ COMMENT\s+)(["\'])(.*?)(["\'])',
                    lambda m: f'{m.group(1)}"{new_comment}"',
                    ddl,
                )
            else:
                raise ValueError(
                    f"Unsupported Databricks runtime version: {dbr_number}"
                )
    return ddl


def replace_pii_tags_in_ddl(
    ddl: str, classification: str, subclassification: str, config: MetadataConfig
) -> str:
    """
    Replace the PII tagging strings in a DDL statement with new classification and subclassification.
    """
    tag_class = getattr(config, "pi_classification_tag_name", "data_classification")
    tag_subclass = getattr(
        config, "pi_subclassification_tag_name", "data_subclassification"
    )

    if "ALTER COLUMN" not in ddl:
        ddl = re.sub(
            rf"(ALTER TABLE [^ ]+ SET TAGS \('{tag_class}' = ')[^']+(', '{tag_subclass}' = ')[^']+('\);)",
            lambda m: f"{m.group(1)}{classification}{m.group(2)}{subclassification}{m.group(3)}",
            ddl,
        )
    else:
        ddl = re.sub(
            rf"(ALTER TABLE [^ ]+ ALTER COLUMN [^ ]+ SET TAGS \('{tag_class}' = ')[^']+(', '{tag_subclass}' = ')[^']+('\);)",
            lambda m: f"{m.group(1)}{classification}{m.group(2)}{subclassification}{m.group(3)}",
            ddl,
        )
    return ddl


def update_ddl_row(
    mode: str, reviewed_column: str, row: pd.Series, config: MetadataConfig
) -> Union[str, Tuple[str]]:
    """
    Update a single row's DDL based on classification/type or column_content.
    """
    if mode == "pi":
        if reviewed_column == "ddl":
            new_classification, new_type = get_pii_tags_from_ddl(row["ddl"], config)
            return new_classification, new_type, row["ddl"]
        elif reviewed_column in ("classification", "type", "other", "column_content"):
            new_ddl = replace_pii_tags_in_ddl(
                row["ddl"], row["classification"], row["type"], config
            )
            return row["classification"], row["type"], new_ddl
        else:
            raise ValueError(
                f"Unknown reviewed column for 'pi' mode: {reviewed_column}"
            )
    elif mode == "comment":
        if reviewed_column == "ddl":
            new_comment = get_comment_from_ddl(row["ddl"])
            return new_comment, row["ddl"]
        elif reviewed_column in ("classification", "type", "other", "column_content"):
            new_ddl = replace_comment_in_ddl(row["ddl"], row["column_content"])
            return row["column_content"], new_ddl
        else:
            raise ValueError(
                f"Unknown reviewed column for 'comment' mode: {reviewed_column}"
            )
    else:
        raise ValueError("Unknown mode")


def check_file_type(file_name: str, config: MetadataConfig) -> None:
    """
    Check if the file type matches the specified export format.
    """
    if file_name.endswith(".xlsx") and config.review_input_file_type != "excel":
        raise ValueError(
            f"File {file_name} does not match the specified export format {config.review_input_file_type}."
        )
    elif file_name.endswith(".tsv") and config.review_input_file_type != "tsv":
        raise ValueError(
            f"File {file_name} does not match the specified export format {config.review_input_file_type}."
        )
    else:
        return True


def export_metadata(
    df: pd.DataFrame, output_dir: str, input_file: str, export_format: str
) -> str:
    """
    Export the DataFrame to the specified format.

    Args:
        df (pd.DataFrame): Data to export.
        output_dir (str): Directory to save the file.
        input_file (str): Original input file name.
        export_format (str): 'sql', 'tsv', or 'excel'.

    Returns:
        str: Path to the exported file.
    """
    ensure_directory_exists(output_dir)
    if export_format == "sql":
        output_file = os.path.join(output_dir, get_output_file_name(input_file, ".sql"))
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                for ddl in df["ddl"].dropna():
                    f.write(ddl.strip())
                    if not ddl.strip().endswith(";"):
                        f.write(";")
                    f.write("\n")
            logging.info(f"Exported SQL file: {output_file}")
        except Exception as e:
            logging.error(f"Failed to write SQL file: {e}")
            raise
    elif export_format == "tsv":
        output_file = os.path.join(output_dir, get_output_file_name(input_file, ".tsv"))
        try:
            df.to_csv(output_file, sep="\t", index=False)
            logging.info(f"Exported TSV file: {output_file}")
        except Exception as e:
            logging.error(f"Failed to write TSV file: {e}")
            raise
    elif export_format == "excel":
        output_file = os.path.join(
            output_dir, get_output_file_name(input_file, ".xlsx")
        )
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            local_path = "/local_disk0/tmp/{input_file}.xlsx"
            df.to_excel(local_path, index=False)

            # Use Databricks SDK WorkspaceClient for UC volume compatibility
            try:
                if output_file.startswith("/Volumes/"):
                    from databricks.sdk import WorkspaceClient

                    w = WorkspaceClient()

                    with open(local_path, "rb") as src_file:
                        excel_content = src_file.read()

                    # Upload using WorkspaceClient (handles UC volumes properly)
                    w.files.upload(output_file, excel_content, overwrite=True)
                else:
                    # Direct file write for local paths
                    with open(local_path, "rb") as src_file:
                        with open(output_file, "wb") as dest_file:
                            dest_file.write(src_file.read())
            except Exception:
                # Fallback to direct file write
                with open(local_path, "rb") as src_file:
                    with open(output_file, "wb") as dest_file:
                        dest_file.write(src_file.read())
            logging.info(f"Exported Excel file: {output_file}")
        except Exception as e:
            logging.error(f"Failed to write Excel file: {e}")
            raise
    else:
        raise ValueError(f"Unsupported export format: {export_format}")
    return output_file


def extract_ddls_from_file(file_path: str, file_type: str) -> list:
    """
    Extract DDL statements from a SQL, XLSX, or TSV file.
    - For .sql: splits by semicolon.
    - For .xlsx/.tsv: looks for 'ddl' column, or uses the first column.
    """
    ddls = []
    if file_type == "sql":
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            ddls = [ddl.strip() for ddl in content.split(";") if ddl.strip()]
    elif file_type in ("excel", "tsv"):
        if file_type == "excel":
            df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
        else:
            df = pd.read_csv(file_path, sep="\t", dtype=str)
        if "ddl" in df.columns:
            ddl_series = df["ddl"]
        else:
            ddl_series = df.iloc[:, 0]
        ddls = ddl_series.dropna().astype(str).str.strip().tolist()
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    return ddls


def apply_ddl_to_databricks(
    sql_file: str, config: MetadataConfig, file_type: str
) -> None:
    """
    Apply DDL statements from a SQL, XLSX, or TSV file to Databricks Delta tables in Unity Catalog.
    """
    spark = SparkSession.builder.getOrCreate()

    try:
        ddls = extract_ddls_from_file(sql_file, file_type)
        for ddl in ddls:
            if ddl:
                try:
                    spark.sql(ddl)
                    logging.info(f"Executed DDL: {ddl[:60]}...")
                except Exception as e:
                    logging.error(f"Failed to execute DDL: {ddl[:60]}... Error: {e}")
        logging.info("All DDL statements applied successfully.")
    except Exception as e:
        logging.error(f"Failed to apply DDLs to Databricks: {e}")
        print(f"Failed to apply DDLs to Databricks: {e}")
        raise


def process_metadata_file(
    config: MetadataConfig, input_file: str, export_format: Optional[str] = None
) -> None:
    """
    Main processing function to load, update, export, and optionally apply DDLs.

    Args:
        config (MetadataConfig): Configuration object.
        input_file (str): Path to the input file.
        export_format (Optional[str]): 'sql', 'tsv', or 'excel'. If None, uses config.
    """
    file_check = check_file_type(input_file, config)
    try:
        sanitized_email = sanitize_user_identifier(config.current_user)
        current_date = datetime.now().strftime("%Y%m%d")
        input_dir = os.path.join(
            "/Volumes",
            config.catalog_name,
            config.schema_name,
            config.volume_name,
            sanitized_email,
            # current_date,
            # "exportable_run_logs",
            "reviewed_outputs",
        )
        output_dir = os.path.join(
            "/Volumes",
            config.catalog_name,
            config.schema_name,
            config.volume_name,
            sanitized_email,
            current_date,
            "exportable_run_logs",
        )
        input_file_type = config.review_input_file_type
        output_file_type = export_format or config.review_output_file_type
        df = load_metadata_file(os.path.join(input_dir, input_file), input_file_type)
        if config.mode == "pi":
            df[["classification", "type", "ddl"]] = df.apply(
                lambda row: update_ddl_row(
                    "pi", config.column_with_reviewed_ddl, row, config
                ),
                axis=1,
                result_type="expand",
            )
        elif config.mode == "comment":
            df[["column_content", "ddl"]] = df.apply(
                lambda row: update_ddl_row(
                    "comment", config.column_with_reviewed_ddl, row, config
                ),
                axis=1,
                result_type="expand",
            )
        exported_file = export_metadata(df, output_dir, input_file, output_file_type)
        if getattr(config, "apply_reviewed_ddl", True):
            apply_ddl_to_databricks(exported_file, config, output_file_type)
        else:
            print("DDL application is only supported for SQL export format.")
            logging.warning("DDL application is only supported for SQL export format.")
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise

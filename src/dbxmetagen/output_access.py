"""Fail-fast preflight for the output catalog/schema/volume.

Metadata generation reads source tables and writes every result to the OUTPUT
location (``catalog_name.schema_name`` plus the review volume). Without write
access there, each table's LLM calls still run and only its final write fails.
That failure is caught per table (the per-table try/except has to stay so one
bad source table can't abort a run), so a principal with read-only access
burned the whole run's LLM budget and the job still ended SUCCESS.

``verify_output_access`` runs once before the per-table loop and raises
``OutputAccessError`` (fatal, never caught) when any output write would fail.
``is_output_write_denied`` lets the per-table loop escalate a write denial on
the output location that slips past the preflight, such as a grant revoked
mid-run, instead of logging it once per table.
"""

import logging
import os
import uuid

logger = logging.getLogger(__name__)

_PERMISSION_MARKERS = (
    "PERMISSION_DENIED",
    "INSUFFICIENT_PERMISSIONS",
    "INSUFFICIENT_PRIVILEGES",
    "DOES NOT HAVE",
    "ACCESS DENIED",
    "NOT AUTHORIZED",
    "FORBIDDEN",
)
# Privileges that are only ever needed to WRITE output. A missing SELECT on a
# source table is a per-table problem and must NOT abort the run.
_WRITE_PRIVILEGES = (
    "MODIFY",
    "CREATE TABLE",
    "CREATE SCHEMA",
    "CREATE VOLUME",
    "WRITE VOLUME",
    "WRITE_VOLUME",
    "CREATE_TABLE",
)


class OutputAccessError(RuntimeError):
    """The output location is missing or not writable. The run must stop."""


def _q(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _is_permission_error(exc: BaseException) -> bool:
    msg = str(exc).upper()
    return any(m in msg for m in _PERMISSION_MARKERS)


def is_output_write_denied(exc: BaseException, catalog_name: str, schema_name: str) -> bool:
    """True if ``exc`` is a write-privilege denial on the output catalog.schema.

    Deliberately narrow: it needs a permission marker, a write privilege, and
    the output ``catalog.schema`` all in the message. A read denial on a source
    table, even one in the same schema, stays a normal per-table failure.
    """
    if isinstance(exc, OutputAccessError):
        return True
    if not _is_permission_error(exc):
        return False
    msg = str(exc).upper().replace("`", "")
    if not any(p in msg for p in _WRITE_PRIVILEGES):
        return False
    return f"{catalog_name}.{schema_name}".upper() in msg


def _fail(what: str, fq: str, exc: BaseException, hint: str) -> None:
    kind = "Permission denied" if _is_permission_error(exc) else "Failed"
    raise OutputAccessError(
        f"FATAL: {kind} {what} {fq}. Metadata generation was stopped before any "
        f"table was processed, because every result is written there.\n"
        f"Fix: {hint}\n"
        f"Underlying error: {str(exc).split('JVM stacktrace:')[0].strip()}"
    ) from exc


def _probe_table_write(spark, fq: str, column: str = "table_name") -> None:
    """Zero-row INSERT. Unity Catalog checks MODIFY when it analyzes the
    statement, so this fails exactly when a real write would, without
    changing any data. It never reads the target, so it's a blind append and
    can't conflict with concurrent writers (parallel-mode tasks updating the
    control table at the same moment)."""
    spark.sql(f"INSERT INTO {fq} ({column}) SELECT CAST(NULL AS STRING) WHERE 1 = 0")


def _probe_volume_write(path: str) -> None:
    probe = os.path.join(path, f".dbxmetagen_write_probe_{uuid.uuid4().hex}")
    with open(probe, "w") as fh:
        fh.write("ok")
    try:
        os.remove(probe)
    except OSError as e:  # writing worked, which is all we need to prove
        logger.warning("Could not remove volume write probe %s: %s", probe, e)


def require_output_location(config):
    """Raise ``OutputAccessError`` if catalog_name or schema_name is unset.

    Returns ``(catalog, schema)``. Call it before setup_ddl: with an empty
    schema_name, setup_ddl silently skips CREATE SCHEMA and fails later with a
    less helpful error.
    """
    catalog = getattr(config, "catalog_name", "") or ""
    schema = getattr(config, "schema_name", "") or ""
    for label, val in (("catalog_name", catalog), ("schema_name", schema)):
        if not val or str(val).strip().lower() in ("none", "null"):
            raise OutputAccessError(
                f"FATAL: output {label} is not set. dbxmetagen writes all results to "
                f"<catalog_name>.<schema_name>; set {label} (job parameter or "
                "variables.yml) and re-run."
            )
    return catalog, schema


def verify_output_access(spark, config, review_output_path: str = None) -> None:
    """Raise ``OutputAccessError`` unless the run can write all its outputs.

    Call this after the schema, volume, and tables are created (setup_ddl and
    create_tables) and before any table is processed. Checks:

    1. catalog_name and schema_name are set (an empty schema used to skip
       CREATE SCHEMA silently and fail later, once per table).
    2. The schema is reachable.
    3. MODIFY on metadata_generation_log (the primary output) and on the
       control table, via a zero-row INSERT that is created first if missing.
    4. WRITE VOLUME on the review-output directory, when a volume is set.
    """
    catalog, schema = require_output_location(config)

    fq_schema = f"{_q(catalog)}.{_q(schema)}"
    try:
        spark.sql(f"DESCRIBE SCHEMA {fq_schema}").collect()
    except Exception as e:
        _fail("accessing output schema", f"{catalog}.{schema}", e,
              f"make sure the catalog and schema exist and grant USE CATALOG on {catalog} "
              f"and USE SCHEMA on {catalog}.{schema} to the identity running this job.")

    from dbxmetagen.processing import ensure_log_table  # lazy: heavy module

    log_fq = f"{catalog}.{schema}.metadata_generation_log"
    try:
        ensure_log_table(spark, catalog, schema)
        _probe_table_write(spark, log_fq)
    except Exception as e:
        _fail("writing to output table", log_fq, e,
              f"grant MODIFY and CREATE TABLE on schema {catalog}.{schema} (plus USE CATALOG "
              f"and USE SCHEMA) to the identity running this job. Read access alone is "
              "not enough.")

    if getattr(config, "control_table", None):
        from dbxmetagen.processing import get_control_table

        ctrl_fq = f"{catalog}.{schema}.{get_control_table(config)}"
        try:
            _probe_table_write(spark, ctrl_fq)
        except Exception as e:
            _fail("writing to control table", ctrl_fq, e,
                  f"grant MODIFY on {ctrl_fq} (or on schema {catalog}.{schema}).")

    if getattr(config, "volume_name", None) and review_output_path:
        try:
            _probe_volume_write(review_output_path)
        except Exception as e:
            _fail("writing to output volume", review_output_path, e,
                  f"grant READ VOLUME and WRITE VOLUME on "
                  f"{catalog}.{schema}.{config.volume_name}.")

    print(f"Output access verified: {catalog}.{schema} is writable.")

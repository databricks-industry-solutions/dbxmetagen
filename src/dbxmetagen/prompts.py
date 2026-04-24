import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Tuple
import pandas as pd
from dbxmetagen.deterministic_pi import detect_pi

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import collect_list, struct, to_json, col

STATS_FIELDS = {"min", "max", "num_nulls", "distinct_count", "avg_col_len", "max_col_len"}
_INFO_SCHEMA_TO_DESCRIBE = {"column_name": "col_name", "data_type": "data_type", "comment": "comment"}

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _format_lineage_section(lineage: dict) -> str:
    """Format a lineage dict as labeled text for LLM prompts."""
    parts = []
    if lineage.get("upstream_tables"):
        parts.append(f"Upstream Tables (data sources): {', '.join(lineage['upstream_tables'])}")
    if lineage.get("downstream_tables"):
        parts.append(f"Downstream Tables (consumers): {', '.join(lineage['downstream_tables'])}")
    return "\n\n".join(parts)


class Prompt(ABC):
    """Prompt class for generating prompts for the database metadata classifier.

    Args:
        ABC: Abstract base class for prompts.
    """

    def __init__(self, config: Any, df: DataFrame, full_table_name: str):
        """
        Initialize the Prompt class.

        Args:
            config (Any): Configuration object.
            df (DataFrame): Spark DataFrame.
            full_table_name (str): Full table name in the format 'catalog.schema.table'.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.config = config
        self.df = df
        self.full_table_name = full_table_name
        self.prompt_content = self.convert_to_comment_input()
        if self.config.add_metadata:
            self.add_metadata_to_comment_input()
        if getattr(self.config, "include_lineage", False):
            self._add_lineage()
        if getattr(self.config, "include_profiling_context", False):
            self._add_profiling_context()
        if getattr(self.config, "include_constraint_context", False):
            self._add_constraint_context()
        logger.debug("Instantiating chat completion response...")

    def _add_lineage(self) -> None:
        """Fetch lineage (cached or live) and attach to prompt_content."""
        from dbxmetagen.processing import fetch_lineage  # avoid circular import

        lin = fetch_lineage(
            self.spark,
            self.full_table_name,
            catalog_name=getattr(self.config, "catalog_name", None),
            schema_name=getattr(self.config, "schema_name", None),
        )
        if lin:
            self.prompt_content["lineage"] = lin

    def _add_profiling_context(self) -> None:
        """Inject profiling statistics into prompt_content column metadata."""
        catalog = getattr(self.config, "catalog_name", None)
        schema = getattr(self.config, "schema_name", None)
        if not catalog or not schema:
            return
        fqtn = self.full_table_name
        try:
            stats_rows = self.spark.sql(
                f"SELECT column_name, distinct_count, null_rate, min_value, max_value "
                f"FROM {catalog}.{schema}.column_profiling_stats "
                f"WHERE table_name = '{fqtn}'"
            ).collect()
        except Exception:
            return
        if not stats_rows:
            return
        # table-level row count
        try:
            snap = self.spark.sql(
                f"SELECT row_count FROM {catalog}.{schema}.profiling_snapshots "
                f"WHERE table_name = '{fqtn}' ORDER BY snapshot_time DESC LIMIT 1"
            ).collect()
            if snap:
                self.prompt_content.setdefault("profiling", {})["row_count"] = snap[0]["row_count"]
        except Exception:
            pass
        col_meta = self.prompt_content.get("column_contents", {}).get("column_metadata", {})
        for r in stats_rows:
            cn = r["column_name"]
            if cn in col_meta:
                col_meta[cn]["profiling"] = {
                    k: r[k] for k in ("distinct_count", "null_rate", "min_value", "max_value") if r[k] is not None
                }

    def _add_constraint_context(self) -> None:
        """Inject PK/FK constraint roles into prompt_content column metadata."""
        catalog = getattr(self.config, "catalog_name", None)
        schema = getattr(self.config, "schema_name", None)
        if not catalog or not schema:
            return
        fqtn = self.full_table_name
        try:
            row = self.spark.sql(
                f"SELECT primary_key_columns, foreign_keys "
                f"FROM {catalog}.{schema}.extended_table_metadata "
                f"WHERE table_name = '{fqtn}' LIMIT 1"
            ).collect()
        except Exception:
            return
        if not row:
            return
        pk_cols = row[0]["primary_key_columns"] or []
        fk_map = row[0]["foreign_keys"] or {}
        col_meta = self.prompt_content.get("column_contents", {}).get("column_metadata", {})
        for cn in pk_cols:
            if cn in col_meta:
                col_meta[cn]["role"] = "PRIMARY KEY"
        for cn, ref in fk_map.items():
            if cn in col_meta and ref:
                col_meta[cn]["role"] = f"FOREIGN KEY -> {ref}"

    def enrich_from_customer_context(self, cache: list) -> None:
        """Inject matching customer context from the prefetched cache (no SQL)."""
        from dbxmetagen.customer_context import resolve_customer_context
        ctx = resolve_customer_context(cache, self.full_table_name)
        if ctx:
            self.prompt_content["customer_context"] = ctx

    def enrich_from_ontology(self) -> None:
        """Inject entity type context from ontology_entities into prompt_content.

        When an entity classification already exists for this table (from a prior
        ontology discovery run), add it as context so the LLM can generate
        better comments, PII classifications, and domain predictions.
        """
        catalog = getattr(self.config, "catalog_name", None)
        schema = getattr(self.config, "schema_name", None)
        if not catalog or not schema:
            return
        fqtn = self.full_table_name
        try:
            rows = self.spark.sql(
                f"SELECT entity_type, confidence, entity_role "
                f"FROM {catalog}.{schema}.ontology_entities "
                f"WHERE ARRAY_CONTAINS(source_tables, '{fqtn}') "
                f"AND entity_role = 'primary' "
                f"ORDER BY confidence DESC LIMIT 1"
            ).collect()
        except Exception:
            return
        if not rows:
            return
        entity_type = rows[0]["entity_type"]
        confidence = rows[0]["confidence"]
        self.prompt_content["ontology_context"] = {
            "entity_type": entity_type,
            "confidence": float(confidence) if confidence else 0.0,
            "hint": (
                f"This table has been classified as a '{entity_type}' entity "
                f"(confidence: {confidence:.2f}). Use this context to inform "
                f"your descriptions -- for example, columns likely represent "
                f"properties or relationships of a {entity_type}."
            ),
        }

    def enrich_from_knowledge_base(self) -> None:
        """Supplement empty UC comments with descriptions from KB tables.

        Queries ``table_knowledge_base`` and ``column_knowledge_base`` in the
        target catalog/schema and injects their ``comment`` values into
        ``self.prompt_content`` wherever the UC-sourced comment is missing.
        Must be called **after** ``add_metadata_to_comment_input()``.
        """
        catalog = self.config.catalog_name
        schema = self.config.schema_name
        fqtn = self.full_table_name

        # --- table-level comment ---
        try:
            tbl_kb = self.spark.sql(
                f"SELECT comment FROM {catalog}.{schema}.table_knowledge_base "
                f"WHERE table_name = '{fqtn}' LIMIT 1"
            ).collect()
        except Exception:
            tbl_kb = []

        if tbl_kb:
            kb_comment = tbl_kb[0]["comment"]
            cc = self.prompt_content.get("column_contents", {})
            existing = cc.get("table_comments", "")
            if not existing or existing == "{}" or existing == "[]":
                cc["table_comments"] = f'[{{"table_name":"{fqtn}","comment":"{kb_comment}"}}]'
                logger.info("KB table comment injected for %s", fqtn)

        # --- column-level comments ---
        try:
            col_rows = self.spark.sql(
                f"SELECT column_name, comment FROM {catalog}.{schema}.column_knowledge_base "
                f"WHERE table_name = '{fqtn}'"
            ).collect()
        except Exception:
            col_rows = []

        if col_rows:
            kb_col_map = {r["column_name"]: r["comment"] for r in col_rows if r["comment"]}
            col_meta = self.prompt_content.get("column_contents", {}).get("column_metadata", {})
            for col_name, kb_desc in kb_col_map.items():
                if col_name in col_meta:
                    existing_comment = col_meta[col_name].get("comment", "")
                    if not existing_comment:
                        col_meta[col_name]["comment"] = kb_desc
            logger.info("KB column comments injected for %s (%d cols)", fqtn, len(kb_col_map))

    @abstractmethod
    def convert_to_comment_input(self) -> Dict[str, Any]:
        """
        Convert DataFrame to a dictionary format suitable for comment input.

        Returns:
            Dict[str, Any]: Dictionary containing table and column contents.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def calculate_cell_length(self, pandas_df) -> pd.DataFrame:
        """
        Calculate the length of every cell in the original DataFrame and truncate values longer than the word limit specified in the config.

        Returns:
            pd.DataFrame: Modified Pandas DataFrame with truncated values.
        """

        def truncate_value(value: str, word_limit: int) -> str:
            words = value.split()
            if len(words) > word_limit:
                return " ".join(words[:word_limit])
            # Fallback: character-based truncation (10x word limit)
            char_limit = word_limit * 10
            if len(value) > char_limit:
                return value[:char_limit]
            return value

        word_limit = getattr(self.config, "word_limit_per_cell", 100)
        truncated_count = 0

        for column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(str)

            truncated_values = pandas_df[column].apply(
                lambda x: truncate_value(x, word_limit)
            )
            char_limit = word_limit * 10
            truncation_flags = pandas_df[column].apply(
                lambda x: len(x.split()) > word_limit or len(x) > char_limit
            )
            pandas_df[column] = truncated_values
            truncated_count += truncation_flags.sum()

        if truncated_count > 0:
            print(f"{truncated_count} cells were truncated.")
            logger.info("%s cells were truncated.", truncated_count)

        return pandas_df

    def filter_extended_metadata_fields(
        self, extended_metadata_df: DataFrame
    ) -> DataFrame:
        """
        Filter extended metadata fields based on the current configuration mode.

        In 'pi' mode: Filters out NULL info_values
        In 'comment' mode: Filters NULL values, descriptions, comments, and optionally data_type

        Args:
            extended_metadata_df: DataFrame containing extended metadata

        Returns:
            Filtered DataFrame

        Raises:
            ValueError: For invalid mode configuration
        """
        mode_handlers = {
            "pi": self._filter_pi_mode,
            "comment": self._filter_comment_mode,
            "domain": self._filter_domain_mode,
        }

        handler = mode_handlers.get(self.config.mode)
        if not handler:
            raise ValueError(
                "Invalid mode provided. Please use either 'pi' or 'comment'"
            )

        return handler(extended_metadata_df)

    def _filter_pi_mode(self, df: DataFrame) -> DataFrame:
        """Filter metadata for PI mode (remove NULL values and existing PI tags to avoid bias)"""
        # Get configured tag names (with defaults)
        pi_classification_tag = getattr(
            self.config, "pi_classification_tag_name", "data_classification"
        )
        pi_subclassification_tag = getattr(
            self.config, "pi_subclassification_tag_name", "data_subclassification"
        )

        return df.filter(
            (df["info_value"] != "NULL")
            & ~df["info_name"].isin([pi_classification_tag, pi_subclassification_tag])
        )

    def _filter_domain_mode(self, df: DataFrame) -> DataFrame:
        """Filter metadata for domain mode (remove NULL values and existing domain tags to avoid bias)"""
        # Get configured tag names (with defaults)
        domain_tag = getattr(self.config, "domain_tag_name", "domain")
        subdomain_tag = getattr(self.config, "subdomain_tag_name", "subdomain")

        return df.filter(
            (df["info_value"] != "NULL")
            & ~df["info_name"].isin([domain_tag, subdomain_tag])
        )

    def _filter_comment_mode(self, df: DataFrame) -> DataFrame:
        """Filter metadata for comment mode with additional exclusions"""
        # Get configured tag names (with defaults) to avoid bias
        pi_classification_tag = getattr(
            self.config, "pi_classification_tag_name", "data_classification"
        )
        pi_subclassification_tag = getattr(
            self.config, "pi_subclassification_tag_name", "data_subclassification"
        )

        filtered_df = df.filter(
            (df["info_value"] != "NULL")
            & ~df["info_name"].isin(
                [
                    "description",
                    "comment",
                    pi_classification_tag,
                    pi_subclassification_tag,
                ]
            )
        )

        if not self.config.include_datatype_from_metadata:
            filtered_df = filtered_df.filter(df["info_name"] != "data_type")

        if not self.config.include_possible_data_fields_in_metadata:
            filtered_df = filtered_df.filter(~df["info_name"].isin(["min", "max"]))

        return filtered_df

    def add_metadata_to_comment_input(self) -> None:
        """Add metadata to the comment input.

        Fetches column tags once and shares the result with both
        extract_column_metadata and the table-metadata path, avoiding
        redundant information_schema queries.  The three table-level
        queries run concurrently via a thread pool.
        """
        column_tags = self.get_column_tags()
        column_metadata_dict = self.extract_column_metadata(column_tags=column_tags)

        with ThreadPoolExecutor(max_workers=3) as pool:
            f_tags = pool.submit(self.get_table_tags)
            f_constraints = pool.submit(self.get_table_constraints)
            f_comment = pool.submit(self.get_table_comment)
        table_metadata = (column_tags, f_tags.result(), f_constraints.result(), f_comment.result())

        self.add_table_metadata_to_column_contents(table_metadata)
        self.prompt_content["column_contents"]["column_metadata"] = column_metadata_dict

    def _excluded_info_names(self) -> set:
        """Return info_name keys to exclude, replicating mode-specific filter logic."""
        excluded = set()
        if self.config.mode == "comment":
            excluded.update({"description", "comment"})
            excluded.update({
                getattr(self.config, "pi_classification_tag_name", "data_classification"),
                getattr(self.config, "pi_subclassification_tag_name", "data_subclassification"),
            })
            if not self.config.include_datatype_from_metadata:
                excluded.add("data_type")
            if not self.config.include_possible_data_fields_in_metadata:
                excluded.update({"min", "max"})
        elif self.config.mode == "pi":
            excluded.update({
                getattr(self.config, "pi_classification_tag_name", "data_classification"),
                getattr(self.config, "pi_subclassification_tag_name", "data_subclassification"),
            })
        elif self.config.mode == "domain":
            excluded.update({
                getattr(self.config, "domain_tag_name", "domain"),
                getattr(self.config, "subdomain_tag_name", "subdomain"),
            })
        return excluded

    def _fetch_batch_column_metadata(
        self, columns, column_tags=None,
    ) -> Dict[str, Dict[str, Any]]:
        """Batch-fetch basic column metadata from information_schema.columns.

        Returns a dict matching the shape of the old per-column DESCRIBE
        EXTENDED output: ``{column_name: {info_name: info_value, ...}}``.
        """
        if not columns:
            return {}
        catalog, schema, table = self.full_table_name.split(".")
        quoted = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in columns)
        df = self.spark.sql(
            f"SELECT column_name, data_type, comment "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' "
            f"AND table_name = '{table}' AND column_name IN ({quoted})"
        )
        rows = df.collect()
        excluded = self._excluded_info_names()
        result = {}
        for row in rows:
            col_dict = {}
            for src_col, info_name in _INFO_SCHEMA_TO_DESCRIBE.items():
                if info_name in excluded:
                    continue
                val = row[src_col]
                if val is None:
                    continue
                col_dict[info_name] = str(val)
            if column_tags and row["column_name"] in column_tags:
                col_dict["tags"] = column_tags[row["column_name"]]
            result[row["column_name"]] = col_dict
        return result

    def _fetch_column_stats_concurrent(self, columns) -> Dict[str, Dict[str, str]]:
        """Fetch column statistics via concurrent DESCRIBE EXTENDED calls.

        Only extracts STATS_FIELDS (min, max, num_nulls, etc.) that are
        unavailable from information_schema.  Per-column failures are
        logged and skipped so Tier 1 data is still usable.
        """
        if not columns:
            return {}
        excluded = self._excluded_info_names()

        def _fetch_one(col_name):
            df = self.spark.sql(
                f"DESCRIBE EXTENDED {self.full_table_name} `{col_name}`"
            )
            rows = df.filter(df["info_name"].isin(STATS_FIELDS)).toPandas()
            stats = {}
            for _, r in rows.iterrows():
                if r["info_name"] in excluded or r["info_value"] == "NULL":
                    continue
                stats[r["info_name"]] = str(r["info_value"])
            return col_name, stats

        result = {}
        with ThreadPoolExecutor(max_workers=min(len(columns), 8)) as pool:
            futures = {pool.submit(_fetch_one, c): c for c in columns}
            for f in as_completed(futures):
                try:
                    col, stats = f.result()
                    result[col] = stats
                except Exception as e:
                    logger.warning("Failed to fetch stats for %s: %s", futures[f], e)
        return result

    def extract_column_metadata(self, column_tags=None) -> Dict[str, Dict[str, Any]]:
        """Extract metadata for each column via batched info_schema + concurrent stats.

        Args:
            column_tags: Pre-fetched column tags dict. If None, fetched internally.

        Returns:
            Dict keyed by column name, each value a dict of info_name -> info_value.
        """
        if column_tags is None:
            column_tags = self.get_column_tags()
        columns = self.prompt_content["column_contents"]["columns"]

        # Tier 1: batch basic metadata from information_schema
        column_metadata_dict = self._fetch_batch_column_metadata(columns, column_tags)

        # Tier 2: concurrent stats from DESCRIBE EXTENDED
        stats = self._fetch_column_stats_concurrent(columns)
        for col, col_stats in stats.items():
            column_metadata_dict.setdefault(col, {}).update(col_stats)

        return column_metadata_dict

    def get_column_constraints(
        self, column_name: str, combined_metadata: Dict[str, str]
    ):
        """
        Add column constraints to the column contents.

        Args:
            column_metadata (Tuple[Dict[str, str], str, str, str]): Tuple containing column constraints.
        """
        catalog_name, schema_name, table_name = self.full_table_name.split(".")
        query = f"""
        SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
        FROM system.information_schema.column_tags
        WHERE catalog_name = '{catalog_name}'
        AND schema_name = '{schema_name}'
        AND table_name = '{table_name}';
        """
        result_df = self.spark.sql(query)
        column_tags = (
            result_df.groupBy("column_name")
            .agg(collect_list(struct("tag_name", "tag_value")).alias("tags"))
            .collect()
        )
        column_tags_dict = {
            row["column_name"]: {
                tag["tag_name"]: tag["tag_value"] for tag in row["tags"]
            }
            for row in column_tags
        }
        logger.debug("column tags dict: %s", column_tags_dict)
        return column_tags_dict

    @staticmethod
    def _merge_column_tags(
        column_name: str, combined_metadata: Dict[str, Any],
        column_tags: Dict[str, Dict[str, str]],
    ) -> Dict[str, Any]:
        """Merge pre-fetched column tags into column metadata dict."""
        if column_name in column_tags:
            combined_metadata["tags"] = column_tags[column_name]
        return combined_metadata

    def add_column_metadata_to_column_contents(
        self, column_name: str, combined_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Backward-compat wrapper around _merge_column_tags."""
        return self._merge_column_tags(column_name, combined_metadata, self.get_column_tags())

    def add_table_metadata_to_column_contents(
        self, table_metadata: Tuple[Dict[str, str], str, str, str]
    ) -> None:
        """
        Add table metadata to the column contents.

        Args:
            table_metadata (Tuple[Dict[str, str], str, str, str]): Tuple containing column tags, table tags, table constraints, and table comments.
        """
        column_tags, table_tags, table_constraints, table_comments = table_metadata
        self.prompt_content["column_contents"]["table_tags"] = table_tags
        self.prompt_content["column_contents"]["table_constraints"] = table_constraints
        if self.config.include_existing_table_comment:
            self.prompt_content["column_contents"]["table_comments"] = table_comments

    def _tags_to_exclude(self) -> list:
        """Return tag names to filter out based on current mode to avoid biasing the LLM."""
        mode = self.config.mode
        if mode in ("pi", "comment"):
            return [
                getattr(self.config, "pi_classification_tag_name", "data_classification"),
                getattr(self.config, "pi_subclassification_tag_name", "data_subclassification"),
            ]
        elif mode == "domain":
            return [
                getattr(self.config, "domain_tag_name", "domain"),
                getattr(self.config, "subdomain_tag_name", "subdomain"),
            ]
        return []

    def get_column_tags(self) -> Dict[str, Dict[str, str]]:
        """
        Get column tags from the information schema, filtering out biasing tags based on mode.

        Returns:
            Dict[str, Dict[str, str]]: Dictionary containing column tags (excluding biasing tags).
        """
        catalog_name, schema_name, table_name = self.full_table_name.split(".")

        tags_to_exclude = self._tags_to_exclude()

        query = f"""
        SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
        FROM system.information_schema.column_tags
        WHERE catalog_name = '{catalog_name}'
        AND schema_name = '{schema_name}'
        AND table_name = '{table_name}';
        """
        result_df = self.spark.sql(query)

        # Filter out biasing tags
        if tags_to_exclude:
            result_df = result_df.filter(~col("tag_name").isin(tags_to_exclude))

        column_tags = (
            result_df.groupBy("column_name")
            .agg(collect_list(struct("tag_name", "tag_value")).alias("tags"))
            .collect()
        )
        column_tags_dict = {
            row["column_name"]: {
                tag["tag_name"]: tag["tag_value"] for tag in row["tags"]
            }
            for row in column_tags
        }
        logger.debug("column tags dict (after filtering): %s", column_tags_dict)
        return column_tags_dict

    def get_table_tags(self) -> str:
        """
        Get table tags from the information schema, filtering out biasing tags based on mode.

        Returns:
            str: JSON string containing table tags (excluding biasing tags).
        """
        catalog_name, schema_name, table_name = self.full_table_name.split(".")

        tags_to_exclude = self._tags_to_exclude()

        query = f"""
        SELECT tag_name, tag_value
        FROM system.information_schema.table_tags
        WHERE catalog_name = '{catalog_name}'
        AND schema_name = '{schema_name}'
        AND table_name = '{table_name}';
        """
        result_df = self.spark.sql(query)

        # Filter out biasing tags
        if tags_to_exclude:
            result_df = result_df.filter(~col("tag_name").isin(tags_to_exclude))

        return self.df_to_json(result_df)

    def get_table_constraints(self) -> str:
        """
        Get table constraints from the information schema.

        Returns:
            str: JSON string containing table constraints.
        """
        catalog_name, schema_name, table_name = self.full_table_name.split(".")
        query = f"""
        SELECT 
        c.table_name, 
        c.constraint_name, 
        t.constraint_type, 
        c.column_name
        FROM system.information_schema.table_constraints t
        LEFT JOIN system.information_schema.constraint_column_usage c
                ON t.constraint_catalog = c.constraint_catalog 
                AND t.constraint_schema = c.constraint_schema 
                AND t.constraint_name = c.constraint_name
        WHERE t.constraint_catalog = '{catalog_name}'
        AND t.constraint_schema = '{schema_name}'
        AND t.table_name = '{table_name}';
        """
        return self.df_to_json(self.spark.sql(query))

    def get_table_comment(self) -> str:
        """
        Get table comment from the information schema.

        Returns:
            str: JSON string containing table comment.
        """
        catalog_name, schema_name, table_name = self.full_table_name.split(".")
        query = f"""
        SELECT table_name, comment
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog_name}'
        AND table_schema = '{schema_name}'
        AND table_name = '{table_name}';
        """
        return self.df_to_json(self.spark.sql(query))

    def get_table_metadata(self) -> Tuple[Dict[str, Dict[str, str]], str, str, str]:
        """
        Get table metadata including column tags, table tags, table constraints, and table comments.

        Returns:
            Tuple[Dict[str, Dict[str, str]], str, str, str]: Tuple containing column tags, table tags, table constraints, and table comments.
        """
        column_tags = self.get_column_tags()
        table_tags = self.get_table_tags()
        table_constraints = self.get_table_constraints()
        table_comments = self.get_table_comment()
        return column_tags, table_tags, table_constraints, table_comments

    @staticmethod
    def df_to_json(df: DataFrame) -> str:
        """
        Convert DataFrame to JSON string.

        Args:
            df (DataFrame): Spark DataFrame.

        Returns:
            str: JSON string representation of the DataFrame.
        """
        if df.isEmpty():
            return {}
        else:
            json_df = df.select(to_json(struct("*")).alias("json_data"))
            json_strings = json_df.collect()
            json_response = ",".join([row.json_data for row in json_strings])
            json_response = "[" + json_response + "]"
            logger.debug("json response in prompt: %s", json_response)
        return json_response


class CommentPrompt(Prompt):
    """
    Prompt for generating metadata for tables and columns in Databricks.
    """

    def convert_to_comment_input(self) -> Dict[str, Any]:
        pandas_df = self.df.toPandas()
        if self.config.limit_prompt_based_on_cell_len:
            truncated_pandas_df = self.calculate_cell_length(pandas_df)
        else:
            truncated_pandas_df = pandas_df
        return {
            "table_name": self.full_table_name,
            "column_contents": truncated_pandas_df.to_dict(orient="split"),
        }

    def create_prompt_template(self) -> Dict[str, Any]:
        """
        Create a prompt template for generating metadata for tables and columns in Databricks.

        Returns:
            Dict[str, Any]: Dictionary containing the prompt template.
        """
        logger.debug("Creating comment prompt template...")
        content = self.prompt_content
        acro_content = self.config.acro_content
        return {
            "comment": [
                {
                    "role": "system",
                    "content": """Generate comprehensive metadata comments for Databricks tables and columns. Analyze all provided information (table name, column names, data samples, metadata statistics, acronyms) to create well-reasoned descriptions.

                    Response Format (MUST be valid JSON with arrays, not stringified arrays):
                    {"table": "description", "columns": ["col1", "col2"], "column_contents": ["col1 desc", "col2 desc"]}
                    
                    IMPORTANT: column_contents must be a JSON array [...], NOT a string containing an array.

                    Guidelines:
                    1. Scale comment length with information richness: 3-5 sentences for simple columns, 4-8 sentences when rich metadata/patterns emerge
                    2. Synthesize insights from: column name -> table context -> sample data -> metadata statistics
                    3. Unpack acronyms confidently. Note anomalies (e.g., unexpectedly low distinct counts, suspicious nulls, data type mismatches)
                    4. Sample data may not represent full distribution - use metadata to validate/contradict sample observations
                    5. Use double quotes for strings. Escape apostrophes with '' (SQL style) for DDL compatibility
                    6. 'index' key is from Pandas to_dict() - ignore unless in 'columns' list
                    7. Return ONLY the JSON dictionary
                    8. Do not include example values in the comment if the values are PII.
                    9. If lineage information (upstream/downstream tables) is provided, use it to understand data provenance and inform your descriptions (e.g. note that a table is derived from specific sources).
                    10. Do NOT echo exact large numeric statistics from metadata (row counts, null counts like "5,435 nulls", large distinct counts, min/max values, specific date ranges). These go stale quickly. Instead describe what the statistic implies: "high cardinality suggesting uniqueness", "very few nulls", "moderate null rate", "mostly populated". Small structural numbers that reveal column semantics ARE appropriate (e.g. "2 distinct values indicating a boolean flag", "values 1-12 representing months"). Structural observations like formats, data types, and encoding patterns are always appropriate.
                    """,
                },
                {
                    "role": "user",
                    "content": """Content is here - {"table_name": "retail.transactions.daily_sales", "column_contents": {"index": [0,1,2], "columns": ["transaction_id", "sale_amount", "is_refund"], "data": [["TXN001", "49.99", "false"], ["TXN002", "125.00", "false"], ["TXN003", "89.50", "false"]], "column_metadata": {"transaction_id": {"col_name": "transaction_id", "data_type": "string", "num_nulls": "0", "distinct_count": "50000", "avg_col_len": "6", "max_col_len": "6"}, "sale_amount": {"col_name": "sale_amount", "data_type": "decimal", "num_nulls": "0", "distinct_count": "15000", "avg_col_len": "6", "max_col_len": "8"}, "is_refund": {"col_name": "is_refund", "data_type": "string", "num_nulls": "0", "distinct_count": "2", "avg_col_len": "5", "max_col_len": "5"}}}} and abbreviations and acronyms are here - {}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "Daily retail transaction records tracking individual sales and refunds. Located in the retail catalog under transactions schema, indicating transactional operational data.", "columns": ["transaction_id", "sale_amount", "is_refund"], "column_contents": ["Unique transaction identifier following 'TXN###' format. Sample shows sequential numbering. No nulls with high cardinality indicating good uniqueness across the full dataset.", "Sale amount in decimal format representing transaction total with two decimal precision. High distinct count suggests diverse pricing, no nulls indicates required field.", "Refund flag stored as string ('true'/'false') rather than boolean data type. Sample shows only 'false' values but metadata confirms 2 distinct values exist in full dataset. No nulls indicates system always populates this field, likely defaulting to 'false' for new transactions."]}""",
                },
                {
                    "role": "user",
                    "content": """Content is here - {"table_name": "healthcare.clinical.patient_lab_results", "column_contents": {"index": [0,1,2,3], "columns": ["patient_mrn", "test_date", "test_code", "result_value", "ref_range_low", "ref_range_high", "abnormal_flag", "ordering_physician"], "data": [["MRN-2024-8901", "2024-03-15", "GLUC", "105", "70", "100", "H", "Dr. Smith"], ["MRN-2024-8902", "2024-03-15", "HBA1C", "6.2", "4.0", "5.6", "H", "Dr. Johnson"], ["MRN-2024-8903", "2024-03-16", "CHOL", "185", "125", "200", "N", "Dr. Smith"], ["MRN-2024-8904", "2024-03-16", "GLUC", "92", "70", "100", "N", "Dr. Williams"]], "column_metadata": {"patient_mrn": {"col_name": "patient_mrn", "data_type": "string", "num_nulls": "0", "distinct_count": "8500", "avg_col_len": "14", "max_col_len": "14"}, "test_date": {"col_name": "test_date", "data_type": "date", "num_nulls": "0", "distinct_count": "365", "avg_col_len": "10", "max_col_len": "10"}, "test_code": {"col_name": "test_code", "data_type": "string", "num_nulls": "0", "distinct_count": "250", "avg_col_len": "5", "max_col_len": "8"}, "result_value": {"col_name": "result_value", "data_type": "string", "num_nulls": "12", "distinct_count": "15000", "avg_col_len": "6", "max_col_len": "20"}, "ref_range_low": {"col_name": "ref_range_low", "data_type": "string", "num_nulls": "50", "distinct_count": "150", "avg_col_len": "4", "max_col_len": "6"}, "ref_range_high": {"col_name": "ref_range_high", "data_type": "string", "num_nulls": "50", "distinct_count": "150", "avg_col_len": "4", "max_col_len": "6"}, "abnormal_flag": {"col_name": "abnormal_flag", "data_type": "string", "num_nulls": "0", "distinct_count": "4", "avg_col_len": "1", "max_col_len": "2"}, "ordering_physician": {"col_name": "ordering_physician", "data_type": "string", "num_nulls": "5", "distinct_count": "45", "avg_col_len": "12", "max_col_len": "30"}}}} and abbreviations and acronyms are here - {"MRN - Medical Record Number", "GLUC - Glucose", "HBA1C - Hemoglobin A1C", "CHOL - Cholesterol"}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "Clinical laboratory test results for patients, combining patient identifiers with test metadata, results, and ordering physician information. Located in healthcare.clinical schema indicating protected health information requiring appropriate access controls. The table structure supports multiple test types per patient over time, with reference ranges for result interpretation.", "columns": ["patient_mrn", "test_date", "test_code", "result_value", "ref_range_low", "ref_range_high", "abnormal_flag", "ordering_physician"], "column_contents": ["Medical Record Number serving as the patient identifier. Consistent 14-character format with high cardinality and no nulls, indicating strong data quality. The 'MRN-YYYY-####' format visible in samples suggests year-based record numbering.", "Date when the laboratory test was performed. Fully populated with a distinct count suggesting approximately daily testing activity over roughly a year. Consistent 10-character length indicates standard date format (YYYY-MM-DD). Sample shows clustering of tests on same dates, which is typical for batch processing of lab orders.", "Standardized laboratory test code identifier. Sample shows common codes (GLUC for Glucose, HBA1C for Hemoglobin A1C, CHOL for Cholesterol) with a moderate number of distinct test types available. Variable length (5-8 characters) accommodates different coding standards. No nulls indicates required field for all lab orders.", "Numeric test result stored as string to accommodate diverse result formats across test types. High cardinality appropriate for continuous measurements. Contains a very small number of nulls which likely represent pending, cancelled, or failed tests - important for downstream processing to handle missing results. Variable length suggests accommodation of text qualifiers or complex results beyond simple numerics.", "Lower bound of the normal reference range for test interpretation. A small fraction of nulls correlate with potential gaps where tests may not have established reference ranges or use alternative interpretation methods. Moderate distinct count indicates reference ranges vary by test type and possibly by patient demographics (age, gender).", "Upper bound of the normal reference range. Mirrors the null pattern of 'ref_range_low', confirming these are paired values that should be populated or null together. The sample data shows reference ranges contextualizing result values. Consistent character lengths with low bound suggests standardized numeric formatting.", "Result interpretation flag indicating whether the value falls outside normal parameters. Sample shows 'H' (High) and 'N' (Normal) with metadata indicating 4 distinct values total (likely H, L, N, and possibly Critical). Single-character format with max length 2 suggests occasional use of two-character codes. Zero nulls indicates this is a required calculated/derived field, essential for clinical decision support and alerts.", "Name of the physician who ordered the laboratory test. Very few nulls suggesting some tests may be ordered by non-physician providers, through automated standing orders, or via clinical protocols. Relatively low cardinality given the patient volume indicates a small practice, specialized facility, or limited provider network. Variable length accommodates different name formats and titles."]}""",
                },
                {
                    "role": "user",
                    "content": self._build_user_content(content, acro_content),
                },
            ]
        }

    @staticmethod
    def _build_user_content(content: dict, acro_content: Any) -> str:
        lineage = content.pop("lineage", None)
        customer_ctx = content.pop("customer_context", None)
        base = f"Content is here - {content} and abbreviations are here - {acro_content}"
        if lineage:
            base += "\n\n" + _format_lineage_section(lineage)
        ontology_ctx = content.get("ontology_context")
        if ontology_ctx:
            base += f"\n\nOntology context: {ontology_ctx['hint']}"
        if customer_ctx:
            base += f"\n\nCustomer-provided context: {customer_ctx}"
        return base


class PIPrompt(Prompt):
    """
    Prompt for generating metadata for tables and columns in Databricks.
    """

    def convert_to_comment_input(self) -> Dict[str, Any]:
        pandas_df = self.df.toPandas()
        if self.config.limit_prompt_based_on_cell_len:
            truncated_pandas_df = self.calculate_cell_length(pandas_df)
        else:
            truncated_pandas_df = pandas_df
        return {
            "table_name": self.full_table_name,
            "column_contents": truncated_pandas_df.to_dict(orient="split"),
        }

    def create_prompt_template(self) -> Dict[str, Any]:
        """
        Create a prompt template for generating metadata for tables and columns in Databricks.

        Returns:
            Dict[str, Any]: Dictionary containing the prompt template.
        """
        logger.debug("Creating PI prompt template...")
        content = self.prompt_content
        if self.config.include_deterministic_pi:
            self.deterministic_results = detect_pi(self.config, self.prompt_content)
            print(
                f"[PIPrompt] Presidio deterministic_results: {self.deterministic_results[:300]}..."
            )
        else:
            self.deterministic_results = ""
            print(
                "[PIPrompt] Presidio detection disabled (include_deterministic_pi=False)"
            )
        acro_content = self.config.acro_content
        return {
            "pi": [
                {
                    "role": "system",
                    "content": """You are an AI assistant identifying personally identifying information (PII/PHI/PCI). Analyze column names, data samples, and metadata to classify data types. Respond ONLY with a JSON dictionary - no notes or explanations.
                    ### 
                    Input Format
                    {"index": [0, 1], "columns": ["name", "address", "email", "MRR", "eap_created", "delete_flag"], "data": [["John Johnson", "123 Main St", "jj@msn.com", "$1545.50", "2024-03-05", "False"], ["Alice Ericks", "6789 Fake Ave", "alice.ericks@aol.com", "$124555.32", "2023-01-03", "False"]], "column_metadata": {'name': {'col_name': 'name', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '5', 'avg_col_len': '16', 'max_col_len': '23'}, 'address': {'col_name': 'address', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '46', 'avg_col_len': '4', 'max_col_len': '4'}, 'email': {{'col_name': 'email', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2', 'avg_col_len': '15', 'max_col_len': '15'}, 'MRR': {'col_name': 'MRR', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '1', 'avg_col_len': '11', 'max_col_len': '11'}, 'eap_created': {'col_name': 'eap_created', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '3', 'avg_col_len': '12', 'max_col_len': '12'}, 'delete_flag': {'col_name': 'delete_flag', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2', 'avg_col_len': '5', 'max_col_len': '5'}}}

                    ###
                    Please provide a response in this format, putting under classification either "pi", "medical_information", or "None", and under type either "pii", "pci", "medical_information", "phi", or "all". All should only be used if a field contains both PHI and PCI. Do not add additional fields.
                    ###

                    {"table": "pi", "columns": ["name", "address", "email", "revenue", "eap_created", "delete_flag"], "column_contents": [{"classification": "pi", "type": "pii", "confidence": 0.85}, {"classification": "pi", "type": "pii", "confidence": 0.9}, {"classification": "pi", "type": "pii", "confidence": 0.9}, {"classification": "None", "type": "None", "confidence": 0.9}, {"classification": "None", "type": "None", "confidence": 0.9}, {"classification": "None", "type": "None", "confidence": 0.98]}

                    ###

                    Specific Considerations
                    1. Please don't respond with anything other than the dictionary.
                    2. Attempt to classify into None, PII, PCI, medical information, and PHI based on common definitions, and for PHI following the 18 HIPAA Guidelines. Follow the PII classification rules here: 
                    ###
                    \n 
                    """
                    + f"""
                    PI Classification Rules: {self.config.pi_classification_rules}.
                    """
                    + f"""
                    \n
                    3. Baseline confidence: Set the confidence if you have no reason to modify it.
                        - For obvious results, such as full_name, full_address, email, etc. set confidence to 0.98.
                        - Similarly, for completely obvious results that are not PII such as Boolean values, years with no other information, or locales such as Country, set confidence to 0.98.
                        - For results such as state or zip code, consider context, and set confidence lower, between 0.6 and 0.8.
                        - For free-form text, use low confidence (0.3) by default, because we are not sampling every row nor the entirety of the text cell.
                    4. Column-level classification priority: Classify columns based on their intrinsic content primarily, and secondarily by their context. Any of the 18 HIPAA PII identifiers should be classified as PII regardless of whether they appear in a healthcare, financial, or retail context. Medical data without embedded identifiers should be classified as medical_information. Only use PHI for columns that either: (a) are healthcare-specific identifiers, or (b) contain medical information or freeform text with embedded patient identifiers.
                    5. The value for classification should always be either "pi", "medical_information", or "None". The value for type should always be either "None", "pii", "pci", "medical_information", or "phi". When type is pii, pci, or phi, classification should be set to pi. When type is None, classification should be None, and when type is medical information, classification should be medical_information.
                    6. Freeform medical text with embedded identifiers: Clinical notes, physician notes, or any freeform text that mentions patient names or other identifiers within the text itself should be classified as PHI. For example, "Patient John Smith reports..." makes the entire text field PHI.
                    7. Healthcare-specific identifiers: When completely alone in a field (no other medical information present in the column), medical Record Numbers (MRN), patient account numbers, insurance member IDs, and similar healthcare-specific identifiers should be set to {self.config.solo_medical_identifier}.
                    8. The medical_information type should be used for medical text that has clearly been de-identified or contains no embedded identifiers: diagnosis codes, medication names, lab test names, procedure codes, or structured lab result values. These become part of PHI at the table level when combined with PII, but at the column level they are medical_information.
                    9. Within strings, use single quotes as would be needed if the comment or column_content would be used as a Python string or in SQL DDL. For example format responses like: "The column 'scope' is a summary column.", rather than "The column "scope" is a summary column."
                    10. Presidio Confidence Adjustment: When Presidio results are provided, assume that Presidio will generally identify PII effectively (high recall) but may often find PII where there is none. Trust Presidio, but use your judgement if it is obviously incorrect, and adjust confidence accordingly.                        
                        - Both agree on PII: high confidence (max 0.98)
                        - Presidio finds PII you missed: Strongly consider what Presidio suggests, aware of the fact that it sometimes finds PII where there is none. If Presidio is clearly incorrect, overrule it, but use a low confidence. If you aren't sure, trust Presidio and use a low confidence.
                        - If Presidio finds PII, but you recognize that there is also medical information present, classify as phi with high confidence.
                        - You find PII Presidio missed: Trust your assessment, confidence 0.6-0.8.
                        - Fundamental disagreement on classification: Reduce confidence to 0.3.
                    11. If lineage information (upstream/downstream tables) is provided, use it as additional context for understanding the data flow and purpose of the table. This can help disambiguate borderline classifications.
                    """,
                },
                {
                    "role": "user",
                    "content": """{"index": [0, 1], "columns": ["customer_name", "billing_address", "email_address", "card_number", "ssn", "order_id", "created_date"], "data": [["Sarah Martinez", "456 Oak Street, Austin TX 78701", "sarah.m@email.com", "4532 1234 5678 9010", "123-45-6789", "ORD-2024-1001", "2024-01-15"], ["Michael Chen", "789 Pine Ave, Seattle WA 98101", "mchen@work.com", "5105 1051 0510 5100", "987-65-4321", "ORD-2024-1002", "2024-01-16"]], "column_metadata": {'customer_name': {'col_name': 'customer_name', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2500', 'avg_col_len': '18', 'max_col_len': '35'}, 'billing_address': {'col_name': 'billing_address', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2450', 'avg_col_len': '42', 'max_col_len': '80'}, 'email_address': {'col_name': 'email_address', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2500', 'avg_col_len': '22', 'max_col_len': '50'}, 'card_number': {'col_name': 'card_number', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2100', 'avg_col_len': '19', 'max_col_len': '19'}, 'ssn': {'col_name': 'ssn', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '2500', 'avg_col_len': '11', 'max_col_len': '11'}, 'order_id': {'col_name': 'order_id', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '5000', 'avg_col_len': '14', 'max_col_len': '14'}, 'created_date': {'col_name': 'created_date', 'data_type': 'date', 'num_nulls': '0', 'distinct_count': '365', 'avg_col_len': '10', 'max_col_len': '10'}}}. 
                    
                    Presidio results: {"deterministic_results": [{"column": "customer_name", "classification": "PII", "entities": ["PERSON"]}, {"column": "billing_address", "classification": "PII", "entities": ["ADDRESS"]}, {"column": "email_address", "classification": "PII", "entities": ["EMAIL_ADDRESS"]}, {"column": "card_number", "classification": "PCI", "entities": ["CREDIT_CARD"]}, {"column": "ssn", "classification": "PII", "entities": ["US_SSN"]}, {"column": "order_id", "classification": "Non-sensitive", "entities": []}, {"column": "created_date", "classification": "Non-sensitive", "entities": []}]}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "orders", "columns": ["customer_name", "billing_address", "email_address", "card_number", "ssn", "order_id", "created_date"], "column_contents": [{"classification": "pi", "type": "pii", "confidence": 0.97}, {"classification": "pi", "type": "pii", "confidence": 0.92}, {"classification": "pi", "type": "pii", "confidence": 0.95}, {"classification": "pi", "type": "pci", "confidence": 0.98}, {"classification": "pi", "type": "pii", "confidence": 0.98}, {"classification": "None", "type": "None", "confidence": 0.97}, {"classification": "None", "type": "None", "confidence": 0.96}]}""",
                },
                {
                    "role": "user",
                    "content": """{"index": [0, 1, 2], "columns": ["user_email", "ip_address", "session_token", "device_id", "login_timestamp", "page_views", "cart_total"], "data": [["john.doe@company.com", "203.0.113.45", "tk_a8f3e2b1c4d5", "dev_98765abcd", "2024-03-05 14:23:01", "12", "125.50"], ["jane.smith@email.net", "198.51.100.22", "tk_9d2f1e3c5b4a", "dev_54321fghi", "2024-03-05 15:10:42", "8", "89.99"], ["alex.wong@work.org", "192.0.2.15", "tk_b7e4d3a2c1f6", "dev_11223jklm", "2024-03-05 16:05:33", "5", "0.00"]], "column_metadata": {'user_email': {'col_name': 'user_email', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '15000', 'avg_col_len': '24', 'max_col_len': '50'}, 'ip_address': {'col_name': 'ip_address', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '8500', 'avg_col_len': '13', 'max_col_len': '15'}, 'session_token': {'col_name': 'session_token', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '50000', 'avg_col_len': '16', 'max_col_len': '16'}, 'device_id': {'col_name': 'device_id', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '12000', 'avg_col_len': '13', 'max_col_len': '13'}, 'login_timestamp': {'col_name': 'login_timestamp', 'data_type': 'timestamp', 'num_nulls': '0', 'distinct_count': '50000', 'avg_col_len': '19', 'max_col_len': '19'}, 'page_views': {'col_name': 'page_views', 'data_type': 'int', 'num_nulls': '0', 'distinct_count': '150', 'avg_col_len': '2', 'max_col_len': '3'}, 'cart_total': {'col_name': 'cart_total', 'data_type': 'decimal', 'num_nulls': '0', 'distinct_count': '2500', 'avg_col_len': '6', 'max_col_len': '8'}}}. 
                    
                    Presidio results: {"deterministic_results": [{"column": "user_email", "classification": "PII", "entities": ["EMAIL_ADDRESS"]}, {"column": "ip_address", "classification": "PII", "entities": ["IP_ADDRESS"]}, {"column": "session_token", "classification": "Non-sensitive", "entities": []}, {"column": "device_id", "classification": "Non-sensitive", "entities": []}, {"column": "login_timestamp", "classification": "Non-sensitive", "entities": []}, {"column": "page_views", "classification": "Non-sensitive", "entities": []}, {"column": "cart_total", "classification": "Non-sensitive", "entities": []}]}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "web_sessions", "columns": ["user_email", "ip_address", "session_token", "device_id", "login_timestamp", "page_views", "cart_total"], "column_contents": [{"classification": "pi", "type": "pii", "confidence": 0.96}, {"classification": "pi", "type": "pii", "confidence": 0.98}, {"classification": "None", "type": "None", "confidence": 0.6}, {"classification": "None", "type": "None", "confidence": 0.90}, {"classification": "None", "type": "None", "confidence": 0.94}, {"classification": "None", "type": "None", "confidence": 0.96}, {"classification": "None", "type": "None", "confidence": 0.95}]}""",
                },
                {
                    "role": "user",
                    "content": """{"index": [0, 1, 2], "columns": ["patient_full_name", "mrn", "diagnosis_code", "diagnosis_desc", "medication_orders", "physician_notes", "lab_results", "visit_date"], "data": [["Emily Rodriguez", "MRN-2024-8901", "E11.9", "Type 2 diabetes without complications", "Metformin 500mg BID", "Patient Emily Rodriguez reports improved glucose control. Continue current regimen.", "Glucose: 105 mg/dL, A1C: 6.2%", "2024-02-15"], ["Robert Johnson", "MRN-2024-8902", "I10", "Essential hypertension", "Lisinopril 10mg daily", "BP well controlled. Advised sodium reduction and exercise.", "BP: 128/82, HR: 72", "2024-02-16"], ["Maria Santos", "MRN-2024-8903", "J45.909", "Asthma, unspecified", "Albuterol inhaler PRN", "No recent exacerbations. Refill rescue inhaler.", "SpO2: 98%, Peak flow: 380 L/min", "2024-02-17"]], "column_metadata": {'patient_full_name': {'col_name': 'patient_full_name', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '8500', 'avg_col_len': '22', 'max_col_len': '45'}, 'mrn': {'col_name': 'mrn', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '8500', 'avg_col_len': '14', 'max_col_len': '14'}, 'diagnosis_code': {'col_name': 'diagnosis_code', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '450', 'avg_col_len': '6', 'max_col_len': '8'}, 'diagnosis_desc': {'col_name': 'diagnosis_desc', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '450', 'avg_col_len': '35', 'max_col_len': '100'}, 'medication_orders': {'col_name': 'medication_orders', 'data_type': 'string', 'num_nulls': '12', 'distinct_count': '1200', 'avg_col_len': '28', 'max_col_len': '150'}, 'physician_notes': {'col_name': 'physician_notes', 'data_type': 'string', 'num_nulls': '5', 'distinct_count': '8495', 'avg_col_len': '120', 'max_col_len': '500'}, 'lab_results': {'col_name': 'lab_results', 'data_type': 'string', 'num_nulls': '800', 'distinct_count': '5000', 'avg_col_len': '45', 'max_col_len': '200'}, 'visit_date': {'col_name': 'visit_date', 'data_type': 'date', 'num_nulls': '0', 'distinct_count': '365', 'avg_col_len': '10', 'max_col_len': '10'}}}. 
                    
                    Presidio results: {"deterministic_results": [{"column": "patient_full_name", "classification": "PII", "entities": ["PERSON"]}, {"column": "mrn", "classification": "PHI", "entities": ["MEDICAL_RECORD_NUMBER"]}, {"column": "diagnosis_code", "classification": "Non-sensitive", "entities": []}, {"column": "diagnosis_desc", "classification": "Non-sensitive", "entities": []}, {"column": "medication_orders", "classification": "Non-sensitive", "entities": []}, {"column": "physician_notes", "classification": "PII", "entities": ["PERSON"]}, {"column": "lab_results", "classification": "Non-sensitive", "entities": []}, {"column": "visit_date", "classification": "Non-sensitive", "entities": []}]}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "patient_encounters", "columns": ["patient_full_name", "mrn", "diagnosis_code", "diagnosis_desc", "medication_orders", "physician_notes", "lab_results", "visit_date"], "column_contents": [{"classification": "pi", "type": "pii", "confidence": 0.97}, {"classification": "pi", "type": "pii", "confidence": 0.96}, {"classification": "medical_information", "type": "medical_information", "confidence": 0.94}, {"classification": "medical_information", "type": "medical_information", "confidence": 0.93}, {"classification": "medical_information", "type": "medical_information", "confidence": 0.90}, {"classification": "pi", "type": "phi", "confidence": 0.93}, {"classification": "medical_information", "type": "medical_information", "confidence": 0.91}, {"classification": "pi", "type": "pii", "confidence": 0.92}]}""",
                },
                {
                    "role": "user",
                    "content": """{"index": [0, 1, 2], "columns": ["customer_notes", "account_id", "state", "zip_code", "last_contact", "feedback_text", "internal_tags"], "data": [["Customer expressed interest in premium tier. Follow up in Q2. Contact: Sarah (sarah@email.com)", "ACC-89234", "CA", "94102", "2024-03-15", "The service was generally good but I had some issues with...", "high_value,needs_followup"], ["Resolved billing issue. Refund processed. Thanks!", "ACC-45677", "NY", "10001", "2024-03-14", "Great support team, very responsive and helpful with my problem!", "resolved,satisfied"], ["Initial consultation completed. Waiting for approval from legal team.", "ACC-12389", "TX", "78701", "2024-03-13", "I'm not sure if this is the right product for our needs. Would like more information about enterprise options and pricing for teams over 100 users. Can we schedule a demo?", "enterprise,demo_requested"]], "column_metadata": {'customer_notes': {'col_name': 'customer_notes', 'data_type': 'string', 'num_nulls': '150', 'distinct_count': '4500', 'avg_col_len': '180', 'max_col_len': '2000'}, 'account_id': {'col_name': 'account_id', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '5000', 'avg_col_len': '9', 'max_col_len': '10'}, 'state': {'col_name': 'state', 'data_type': 'string', 'num_nulls': '45', 'distinct_count': '50', 'avg_col_len': '2', 'max_col_len': '2'}, 'zip_code': {'col_name': 'zip_code', 'data_type': 'string', 'num_nulls': '0', 'distinct_count': '1200', 'avg_col_len': '5', 'max_col_len': '10'}, 'last_contact': {'col_name': 'last_contact', 'data_type': 'date', 'num_nulls': '10', 'distinct_count': '890', 'avg_col_len': '10', 'max_col_len': '10'}, 'feedback_text': {'col_name': 'feedback_text', 'data_type': 'string', 'num_nulls': '230', 'distinct_count': '4800', 'avg_col_len': '250', 'max_col_len': '5000'}, 'internal_tags': {'col_name': 'internal_tags', 'data_type': 'string', 'num_nulls': '500', 'distinct_count': '350', 'avg_col_len': '25', 'max_col_len': '100'}}}. 
                    
                    Presidio results: {"deterministic_results": [{"column": "customer_notes", "classification": "PII", "entities": ["PERSON", "EMAIL_ADDRESS"]}, {"column": "account_id", "classification": "Non-sensitive", "entities": []}, {"column": "state", "classification": "PII", "entities": ["LOCATION"]}, {"column": "zip_code", "classification": "PII", "entities": ["US_SSN"]}, {"column": "last_contact", "classification": "Non-sensitive", "entities": []}, {"column": "feedback_text", "classification": "Non-sensitive", "entities": []}, {"column": "internal_tags", "classification": "Non-sensitive", "entities": []}]}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "customer_feedback", "columns": ["customer_notes", "account_id", "state", "zip_code", "last_contact", "feedback_text", "internal_tags"], "column_contents": [{"classification": "pi", "type": "pii", "confidence": 0.35}, {"classification": "None", "type": "None", "confidence": 0.85}, {"classification": "pi", "type": "pii", "confidence": 0.65}, {"classification": "pi", "type": "pii", "confidence": 0.70}, {"classification": "None", "type": "None", "confidence": 0.92}, {"classification": "None", "type": "None", "confidence": 0.40}, {"classification": "None", "type": "None", "confidence": 0.88}]}""",
                },
                {
                    "role": "user",
                    "content": self._build_pi_user_content(content, acro_content),
                },
            ]
        }

    def _build_pi_user_content(self, content: dict, acro_content: Any) -> str:
        lineage = content.pop("lineage", None)
        customer_ctx = content.pop("customer_context", None)
        base = f"{content} + {acro_content}. Deterministic results from Presidio or other outside checks to consider to help check your outputs are here: {self.deterministic_results}."
        if lineage:
            base += "\n\n" + _format_lineage_section(lineage)
        if customer_ctx:
            base += f"\n\nCustomer-provided context: {customer_ctx}"
        return base


class CommentNoDataPrompt(Prompt):
    """
    Prompt for generating metadata for tables and columns in Databricks.
    """

    def convert_to_comment_input(self) -> Dict[str, Any]:
        """
        Convert DataFrame to a dictionary format suitable for comment input.

        Returns:
            Dict[str, Any]: Dictionary containing table and column contents.
        """
        pandas_df = self.df.toPandas()
        if self.config.limit_prompt_based_on_cell_len:
            truncated_pandas_df = self.calculate_cell_length(pandas_df)
        else:
            truncated_pandas_df = pandas_df
        return {
            "table_name": self.full_table_name,
            "column_contents": truncated_pandas_df.to_dict(orient="split"),
        }

    def create_prompt_template(self) -> Dict[str, Any]:
        """
        Create a prompt template for generating metadata for tables and columns in Databricks.

        Returns:
            Dict[str, Any]: Dictionary containing the prompt template.
        """
        print("Creating comment prompt template with no data in comments...")
        content = self.prompt_content
        acro_content = self.config.acro_content
        return {
            "comment": [
                {
                    "role": "system",
                    "content": """Generate comprehensive metadata comments for Databricks tables and columns. Analyze all provided information (table name, column names, data samples, metadata statistics, acronyms) to create well-reasoned descriptions. **CRITICAL: Do NOT include any actual data values in your descriptions - this data may be sensitive.**

                    Response Format (MUST be valid JSON with arrays, not stringified arrays):
                    {"table": "description", "columns": ["col1", "col2"], "column_contents": ["col1 desc", "col2 desc"]}
                    
                    IMPORTANT: column_contents must be a JSON array [...], NOT a string containing an array.

                    Guidelines:
                    1. Scale comment length with information richness: 2-3 sentences for simple columns, 4-8 sentences when rich metadata/patterns emerge
                    2. Synthesize insights from: column name -> table context -> sample data patterns -> metadata statistics (WITHOUT citing specific values)
                    3. Unpack acronyms confidently. Note anomalies (e.g., unexpectedly low distinct counts, suspicious nulls, data type mismatches)
                    4. Use sample data to understand patterns/formats/types, but describe generically without quoting specific values
                    5. Use double quotes for strings. Escape apostrophes with '' (SQL style) for DDL compatibility
                    6. 'index' key is from Pandas to_dict() - ignore unless in 'columns' list
                    7. Return ONLY the JSON dictionary
                    8. Do not include example values in the comment ever.
                    9. If lineage information (upstream/downstream tables) is provided, use it to understand data provenance and inform your descriptions (e.g. note that a table is derived from specific sources).
                    10. Do NOT echo exact large numeric statistics from metadata (row counts, null counts like "5,435 nulls", large distinct counts, min/max values, specific date ranges). These go stale quickly. Instead describe what the statistic implies: "high cardinality suggesting uniqueness", "very few nulls", "moderate null rate", "mostly populated". Small structural numbers that reveal column semantics ARE appropriate (e.g. "2 distinct values indicating a boolean flag", "values 1-12 representing months"). Structural observations like formats, data types, and encoding patterns are always appropriate.
                    """,
                },
                {
                    "role": "user",
                    "content": """Content is here - {"table_name": "retail.transactions.daily_sales", "column_contents": {"index": [0,1,2], "columns": ["transaction_id", "sale_amount", "is_refund"], "data": [["TXN001", "49.99", "false"], ["TXN002", "125.00", "false"], ["TXN003", "89.50", "false"]], "column_metadata": {"transaction_id": {"col_name": "transaction_id", "data_type": "string", "num_nulls": "0", "distinct_count": "50000", "avg_col_len": "6", "max_col_len": "6"}, "sale_amount": {"col_name": "sale_amount", "data_type": "decimal", "num_nulls": "0", "distinct_count": "15000", "avg_col_len": "6", "max_col_len": "8"}, "is_refund": {"col_name": "is_refund", "data_type": "string", "num_nulls": "0", "distinct_count": "2", "avg_col_len": "5", "max_col_len": "5"}}}} and abbreviations and acronyms are here - {}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "Daily retail transaction records tracking individual sales and refunds. Located in the retail catalog under transactions schema, indicating transactional operational data.", "columns": ["transaction_id", "sale_amount", "is_refund"], "column_contents": ["Unique transaction identifier following alphanumeric format with consistent length. No nulls with high cardinality indicating good uniqueness across the full dataset.", "Sale amount in decimal format representing transaction total with two decimal precision. High distinct count suggests diverse pricing, no nulls indicates required field.", "Refund flag stored as string rather than boolean data type. Metadata confirms 2 distinct values exist in full dataset. No nulls indicates system always populates this field."]}""",
                },
                {
                    "role": "user",
                    "content": """Content is here - {"table_name": "healthcare.clinical.patient_lab_results", "column_contents": {"index": [0,1,2,3], "columns": ["patient_mrn", "test_date", "test_code", "result_value", "ref_range_low", "ref_range_high", "abnormal_flag", "ordering_physician"], "data": [["MRN-2024-8901", "2024-03-15", "GLUC", "105", "70", "100", "H", "Dr. Smith"], ["MRN-2024-8902", "2024-03-15", "HBA1C", "6.2", "4.0", "5.6", "H", "Dr. Johnson"], ["MRN-2024-8903", "2024-03-16", "CHOL", "185", "125", "200", "N", "Dr. Smith"], ["MRN-2024-8904", "2024-03-16", "GLUC", "92", "70", "100", "N", "Dr. Williams"]], "column_metadata": {"patient_mrn": {"col_name": "patient_mrn", "data_type": "string", "num_nulls": "0", "distinct_count": "8500", "avg_col_len": "14", "max_col_len": "14"}, "test_date": {"col_name": "test_date", "data_type": "date", "num_nulls": "0", "distinct_count": "365", "avg_col_len": "10", "max_col_len": "10"}, "test_code": {"col_name": "test_code", "data_type": "string", "num_nulls": "0", "distinct_count": "250", "avg_col_len": "5", "max_col_len": "8"}, "result_value": {"col_name": "result_value", "data_type": "string", "num_nulls": "12", "distinct_count": "15000", "avg_col_len": "6", "max_col_len": "20"}, "ref_range_low": {"col_name": "ref_range_low", "data_type": "string", "num_nulls": "50", "distinct_count": "150", "avg_col_len": "4", "max_col_len": "6"}, "ref_range_high": {"col_name": "ref_range_high", "data_type": "string", "num_nulls": "50", "distinct_count": "150", "avg_col_len": "4", "max_col_len": "6"}, "abnormal_flag": {"col_name": "abnormal_flag", "data_type": "string", "num_nulls": "0", "distinct_count": "4", "avg_col_len": "1", "max_col_len": "2"}, "ordering_physician": {"col_name": "ordering_physician", "data_type": "string", "num_nulls": "5", "distinct_count": "45", "avg_col_len": "12", "max_col_len": "30"}}}} and abbreviations and acronyms are here - {"MRN - Medical Record Number", "GLUC - Glucose", "HBA1C - Hemoglobin A1C", "CHOL - Cholesterol"}""",
                },
                {
                    "role": "assistant",
                    "content": """{"table": "Clinical laboratory test results for patients, combining patient identifiers with test metadata, results, and ordering physician information. Located in healthcare.clinical schema indicating protected health information requiring appropriate access controls. The table structure supports multiple test types per patient over time, with reference ranges for result interpretation.", "columns": ["patient_mrn", "test_date", "test_code", "result_value", "ref_range_low", "ref_range_high", "abnormal_flag", "ordering_physician"], "column_contents": ["Medical Record Number serving as the patient identifier. Consistent 14-character format with high cardinality and no nulls, indicating strong data quality. Format suggests year-based record numbering.", "Date when the laboratory test was performed. Fully populated with a distinct count suggesting approximately daily testing activity over roughly a year. Consistent 10-character length indicates standard date format. Sample patterns show clustering of tests on same dates, typical for batch processing of lab orders.", "Standardized laboratory test code identifier for common clinical tests including glucose, hemoglobin A1C, and cholesterol measurements. Moderate number of distinct test types with variable length (5-8 characters) accommodating different coding standards. No nulls indicates required field for all lab orders.", "Numeric test result stored as string to accommodate diverse result formats across test types. High cardinality appropriate for continuous measurements. Contains a very small number of nulls which likely represent pending, cancelled, or failed tests - important for downstream processing to handle missing results. Variable length suggests accommodation of text qualifiers or complex results beyond simple numerics.", "Lower bound of the normal reference range for test interpretation. A small fraction of nulls correlate with potential gaps where tests may not have established reference ranges or use alternative interpretation methods. Moderate distinct count indicates reference ranges vary by test type and possibly by patient demographics. Consistent character lengths suggest standardized numeric formatting.", "Upper bound of the normal reference range. Mirrors the null pattern of 'ref_range_low', confirming these are paired values that should be populated or null together. Format provides context for result interpretation alongside lower bounds. Consistent character lengths with low bound suggests standardized numeric formatting.", "Result interpretation flag indicating whether values fall outside normal parameters. Single-character format with metadata indicating 4 distinct values (likely High, Low, Normal, and possibly Critical). Max length of 2 suggests occasional use of two-character codes. Zero nulls indicates this is a required calculated/derived field, essential for clinical decision support and alerts.", "Name of the physician who ordered the laboratory test. Very few nulls suggesting some tests may be ordered by non-physician providers, through automated standing orders, or via clinical protocols. Relatively low cardinality given the patient volume indicates a small practice, specialized facility, or limited provider network. Variable length accommodates different name formats and titles with professional prefixes."]}""",
                },
                {
                    "role": "user",
                    "content": self._build_nodata_user_content(content, acro_content),
                },
            ]
        }

    @staticmethod
    def _build_nodata_user_content(content: dict, acro_content: Any) -> str:
        lineage = content.pop("lineage", None)
        base = f"Content is here - {content} and abbreviations are here - {acro_content}"
        if lineage:
            base += "\n\n" + _format_lineage_section(lineage)
        return base


class DomainPrompt(Prompt):
    """
    Prompt for domain classification of tables.
    Domain classification is table-level only and uses metadata + column info.
    """

    def convert_to_comment_input(self) -> Dict[str, Any]:
        """
        Convert DataFrame to a dictionary format for domain classification.
        Includes table name, column names, and sample data.
        """
        pandas_df = self.df.toPandas()
        if self.config.limit_prompt_based_on_cell_len:
            truncated_pandas_df = self.calculate_cell_length(pandas_df)
        else:
            truncated_pandas_df = pandas_df
        return {
            "table_name": self.full_table_name,
            "column_contents": truncated_pandas_df.to_dict(orient="split"),
        }

    def create_prompt_template(self) -> Dict[str, Any]:
        """
        Create prompt template for domain classification.
        This is used to prepare data for the agent, not as a chat template.
        """
        content = self.prompt_content
        return {"domain": content}


class PromptFactory:
    """
    Factory class for creating prompts.
    """

    @staticmethod
    def create_prompt(config, df, full_table_name) -> Prompt:
        """
        Create a prompt based on the configuration.

        Args:
            config (Any): Configuration object.
            df (DataFrame): Spark DataFrame.
            full_table_name (str): Full table name in the format 'catalog.schema.table'.

        Returns:
            Prompt: A prompt object.
        """
        if config.mode == "comment" and config.allow_data_in_comments:
            return CommentPrompt(config, df, full_table_name)
        if config.mode == "comment":
            return CommentNoDataPrompt(config, df, full_table_name)
        if config.mode == "pi":
            return PIPrompt(config, df, full_table_name)
        if config.mode == "domain":
            return DomainPrompt(config, df, full_table_name)
        raise ValueError("Invalid mode. Use 'pi', 'comment', or 'domain'.")

"""
Knowledge Graph Builder for Metadata
Creates nodes and edges from metadata_generation_log to build a queryable knowledge graph.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    sha2,
    concat,
    current_timestamp,
    when,
    explode,
    max as spark_max,
    get_json_object,
    from_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
)

from src.dbxmetagen.config import MetadataConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class KnowledgeGraphBuilder:
    """Builds knowledge graph from metadata generation logs."""

    def __init__(self, config: MetadataConfig):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()
        self.nodes_table = (
            f"{config.catalog_name}.{config.schema_name}.metadata_knowledge_base_nodes"
        )
        self.edges_table = (
            f"{config.catalog_name}.{config.schema_name}.metadata_knowledge_base_edges"
        )

    def build_knowledge_graph(self) -> None:
        """Main entry point to build/update the knowledge graph."""
        logger.info("Starting knowledge graph build process...")

        # Create tables if they don't exist
        self._create_nodes_table()
        self._create_edges_table()

        # Get the last processed timestamp for incremental processing
        last_processed = self._get_last_processed_timestamp()

        # Extract nodes from metadata_generation_log
        nodes_df = self._extract_nodes(last_processed)

        if nodes_df.count() == 0:
            logger.info("No new metadata to process")
            return

        # Write nodes (merge/upsert)
        self._write_nodes(nodes_df)

        # Build edges from nodes
        edges_df = self._build_edges()

        # Write edges (merge/upsert)
        self._write_edges(edges_df)

        logger.info("Knowledge graph build complete")

    def _create_nodes_table(self) -> None:
        """Create the nodes table if it doesn't exist."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.nodes_table} (
            node_id STRING NOT NULL,
            node_type STRING NOT NULL,
            object_name STRING NOT NULL,
            catalog STRING,
            schema STRING,
            table_name STRING,
            column_name STRING,
            comment STRING,
            pi_classification STRING,
            pi_type STRING,
            pi_confidence DOUBLE,
            domain STRING,
            subdomain STRING,
            domain_confidence DOUBLE,
            data_type STRING,
            num_queries_last_7_days INT,
            metadata STRING,
            tags MAP<STRING, STRING>,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            CONSTRAINT nodes_pk PRIMARY KEY (node_id)
        ) USING DELTA
        """
        self.spark.sql(create_sql)
        logger.info(f"Ensured nodes table exists: {self.nodes_table}")

    def _create_edges_table(self) -> None:
        """Create the edges table if it doesn't exist."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.edges_table} (
            edge_id STRING NOT NULL,
            source_node_id STRING NOT NULL,
            target_node_id STRING NOT NULL,
            relationship_type STRING NOT NULL,
            metadata STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            CONSTRAINT edges_pk PRIMARY KEY (edge_id)
        ) USING DELTA
        """
        self.spark.sql(create_sql)
        logger.info(f"Ensured edges table exists: {self.edges_table}")

    def _get_last_processed_timestamp(self) -> Optional[str]:
        """Get the last processed timestamp for incremental updates."""
        try:
            result = self.spark.sql(
                f"""
                SELECT MAX(updated_at) as max_ts
                FROM {self.nodes_table}
            """
            ).first()

            if result and result["max_ts"]:
                logger.info(f"Last processed timestamp: {result['max_ts']}")
                return result["max_ts"]
        except Exception as e:
            logger.info(f"No previous timestamp found (first run): {e}")
        return None

    def _extract_nodes(self, last_processed: Optional[str]) -> DataFrame:
        """Extract nodes from metadata_generation_log."""
        log_table = f"{self.config.catalog_name}.{self.config.schema_name}.metadata_generation_log"

        # Read metadata generation log
        log_df = self.spark.table(log_table)

        # Filter for incremental processing
        if last_processed:
            log_df = log_df.filter(col("_created_at") > last_processed)

        # Create table nodes
        table_nodes = self._create_table_nodes(log_df)

        # Create column nodes
        column_nodes = self._create_column_nodes(log_df)

        # Union all nodes
        all_nodes = table_nodes.union(column_nodes)

        return all_nodes

    def _create_table_nodes(self, log_df: DataFrame) -> DataFrame:
        """Create nodes for tables."""
        # Get latest entry per table (most recent metadata)
        table_df = log_df.filter(col("ddl_type") == "table")

        # Aggregate all metadata types for each table
        table_agg = table_df.groupBy("catalog", "schema", "table_name").agg(
            spark_max(
                when(col("metadata_type") == "comment", col("column_content"))
            ).alias("comment"),
            spark_max(when(col("metadata_type") == "pi", col("classification"))).alias(
                "pi_classification"
            ),
            spark_max(when(col("metadata_type") == "pi", col("type"))).alias("pi_type"),
            spark_max(when(col("metadata_type") == "pi", col("confidence"))).alias(
                "pi_confidence"
            ),
            spark_max(when(col("metadata_type") == "domain", col("domain"))).alias(
                "domain"
            ),
            spark_max(when(col("metadata_type") == "domain", col("subdomain"))).alias(
                "subdomain"
            ),
            spark_max(when(col("metadata_type") == "domain", col("confidence"))).alias(
                "domain_confidence"
            ),
            spark_max("input_metadata").alias("input_metadata"),
            spark_max("_created_at").alias("created_at"),
        )

        # Create node structure with usage stats extracted from input_metadata
        nodes = table_agg.select(
            sha2(
                concat(
                    col("catalog"), lit("."), col("schema"), lit("."), col("table_name")
                ),
                256,
            ).alias("node_id"),
            lit("table").alias("node_type"),
            concat(
                col("catalog"), lit("."), col("schema"), lit("."), col("table_name")
            ).alias("object_name"),
            col("catalog"),
            col("schema"),
            col("table_name"),
            lit(None).cast("string").alias("column_name"),
            col("comment"),
            col("pi_classification"),
            col("pi_type"),
            col("pi_confidence"),
            col("domain"),
            col("subdomain"),
            col("domain_confidence"),
            lit(None).cast("string").alias("data_type"),
            # Extract num_queries_last_7_days from input_metadata JSON
            get_json_object(
                col("input_metadata"), "$.table_usage_stats.num_queries_last_7_days"
            )
            .cast("int")
            .alias("num_queries_last_7_days"),
            col("input_metadata").alias("metadata"),
            lit(None).cast("map<string,string>").alias("tags"),
            col("created_at"),
            current_timestamp().alias("updated_at"),
        )

        return nodes

    def _create_column_nodes(self, log_df: DataFrame) -> DataFrame:
        """Create nodes for columns."""
        # Get latest entry per column
        column_df = log_df.filter(col("ddl_type") == "column")

        # Aggregate all metadata types for each column
        column_agg = column_df.groupBy(
            "catalog", "schema", "table_name", "column_name"
        ).agg(
            spark_max(
                when(col("metadata_type") == "comment", col("column_content"))
            ).alias("comment"),
            spark_max(when(col("metadata_type") == "pi", col("classification"))).alias(
                "pi_classification"
            ),
            spark_max(when(col("metadata_type") == "pi", col("type"))).alias("pi_type"),
            spark_max(when(col("metadata_type") == "pi", col("confidence"))).alias(
                "pi_confidence"
            ),
            spark_max(when(col("metadata_type") == "domain", col("domain"))).alias(
                "domain"
            ),
            spark_max(when(col("metadata_type") == "domain", col("subdomain"))).alias(
                "subdomain"
            ),
            spark_max(when(col("metadata_type") == "domain", col("confidence"))).alias(
                "domain_confidence"
            ),
            spark_max("input_metadata").alias("input_metadata"),
            spark_max("_created_at").alias("created_at"),
        )

        # Parse input_metadata to extract data_type if available
        # This is a simplified version - you may want to enhance this
        nodes = column_agg.select(
            sha2(
                concat(
                    col("catalog"),
                    lit("."),
                    col("schema"),
                    lit("."),
                    col("table_name"),
                    lit("."),
                    col("column_name"),
                ),
                256,
            ).alias("node_id"),
            lit("column").alias("node_type"),
            concat(
                col("catalog"),
                lit("."),
                col("schema"),
                lit("."),
                col("table_name"),
                lit("."),
                col("column_name"),
            ).alias("object_name"),
            col("catalog"),
            col("schema"),
            col("table_name"),
            col("column_name"),
            col("comment"),
            col("pi_classification"),
            col("pi_type"),
            col("pi_confidence"),
            col("domain"),
            col("subdomain"),
            col("domain_confidence"),
            lit(None)
            .cast("string")
            .alias("data_type"),  # TODO: Extract from input_metadata JSON
            lit(None)
            .cast("int")
            .alias("num_queries_last_7_days"),  # Columns don't have query stats
            col("input_metadata").alias("metadata"),
            lit(None).cast("map<string,string>").alias("tags"),
            col("created_at"),
            current_timestamp().alias("updated_at"),
        )

        return nodes

    def _write_nodes(self, nodes_df: DataFrame) -> None:
        """Write nodes to the nodes table using merge."""
        logger.info(f"Writing {nodes_df.count()} nodes...")

        # Create temp view for merge
        nodes_df.createOrReplaceTempView("new_nodes")

        # Merge (upsert) nodes
        merge_sql = f"""
        MERGE INTO {self.nodes_table} target
        USING new_nodes source
        ON target.node_id = source.node_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(merge_sql)
        logger.info("Nodes written successfully")

    def _build_edges(self) -> DataFrame:
        """Build edges from nodes."""
        nodes_df = self.spark.table(self.nodes_table)

        # Get tables and columns
        tables = nodes_df.filter(col("node_type") == "table").select(
            col("node_id").alias("table_node_id"),
            col("catalog").alias("table_catalog"),
            col("schema").alias("table_schema"),
            col("table_name").alias("table_table_name"),
            col("domain").alias("table_domain"),
        )

        columns = nodes_df.filter(col("node_type") == "column").select(
            col("node_id").alias("column_node_id"),
            col("catalog").alias("column_catalog"),
            col("schema").alias("column_schema"),
            col("table_name").alias("column_table_name"),
            col("column_name"),
            col("domain").alias("column_domain"),
        )

        # Build different relationship types
        edges_list = []

        # 1. IS_COLUMN_OF - columns to their tables
        column_of_edges = columns.join(
            tables,
            (columns["column_catalog"] == tables["table_catalog"])
            & (columns["column_schema"] == tables["table_schema"])
            & (columns["column_table_name"] == tables["table_table_name"]),
        ).select(
            sha2(
                concat(
                    col("column_node_id"), lit("_IS_COLUMN_OF_"), col("table_node_id")
                ),
                256,
            ).alias("edge_id"),
            col("column_node_id").alias("source_node_id"),
            col("table_node_id").alias("target_node_id"),
            lit("IS_COLUMN_OF").alias("relationship_type"),
            lit(None).cast("string").alias("metadata"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
        edges_list.append(column_of_edges)

        # 2. IN_SAME_TABLE - columns in the same table
        same_table_edges = (
            columns.alias("c1")
            .join(
                columns.alias("c2"),
                (col("c1.column_catalog") == col("c2.column_catalog"))
                & (col("c1.column_schema") == col("c2.column_schema"))
                & (col("c1.column_table_name") == col("c2.column_table_name"))
                & (
                    col("c1.column_node_id") < col("c2.column_node_id")
                ),  # Avoid duplicates
            )
            .select(
                sha2(
                    concat(
                        col("c1.column_node_id"),
                        lit("_IN_SAME_TABLE_"),
                        col("c2.column_node_id"),
                    ),
                    256,
                ).alias("edge_id"),
                col("c1.column_node_id").alias("source_node_id"),
                col("c2.column_node_id").alias("target_node_id"),
                lit("IN_SAME_TABLE").alias("relationship_type"),
                lit(None).cast("string").alias("metadata"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at"),
            )
        )
        edges_list.append(same_table_edges)

        # 3. IN_SAME_SCHEMA - tables in the same schema
        same_schema_edges = (
            tables.alias("t1")
            .join(
                tables.alias("t2"),
                (col("t1.table_catalog") == col("t2.table_catalog"))
                & (col("t1.table_schema") == col("t2.table_schema"))
                & (
                    col("t1.table_node_id") < col("t2.table_node_id")
                ),  # Avoid duplicates
            )
            .select(
                sha2(
                    concat(
                        col("t1.table_node_id"),
                        lit("_IN_SAME_SCHEMA_"),
                        col("t2.table_node_id"),
                    ),
                    256,
                ).alias("edge_id"),
                col("t1.table_node_id").alias("source_node_id"),
                col("t2.table_node_id").alias("target_node_id"),
                lit("IN_SAME_SCHEMA").alias("relationship_type"),
                lit(None).cast("string").alias("metadata"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at"),
            )
        )
        edges_list.append(same_schema_edges)

        # 4. HAS_SAME_DOMAIN - entities with the same domain
        # For tables
        same_domain_tables = (
            tables.alias("t1")
            .join(
                tables.alias("t2"),
                (col("t1.table_domain").isNotNull())
                & (col("t1.table_domain") == col("t2.table_domain"))
                & (
                    col("t1.table_node_id") < col("t2.table_node_id")
                ),  # Avoid duplicates
            )
            .select(
                sha2(
                    concat(
                        col("t1.table_node_id"),
                        lit("_HAS_SAME_DOMAIN_"),
                        col("t2.table_node_id"),
                    ),
                    256,
                ).alias("edge_id"),
                col("t1.table_node_id").alias("source_node_id"),
                col("t2.table_node_id").alias("target_node_id"),
                lit("HAS_SAME_DOMAIN").alias("relationship_type"),
                lit(None).cast("string").alias("metadata"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at"),
            )
        )
        edges_list.append(same_domain_tables)

        # 5. IS_UPSTREAM_OF - lineage edges from metadata
        lineage_edges = self._build_lineage_edges()
        if lineage_edges is not None:
            edges_list.append(lineage_edges)

        # Union all edge types
        all_edges = edges_list[0]
        for edge_df in edges_list[1:]:
            all_edges = all_edges.union(edge_df)

        return all_edges

    def _build_lineage_edges(self) -> Optional[DataFrame]:
        """
        Build lineage edges from table_lineage information in metadata.
        Extracts upstream_tables and downstream_tables from the metadata JSON field.
        Creates IS_UPSTREAM_OF edges.
        """
        try:
            nodes_df = self.spark.table(self.nodes_table)

            # Get table nodes with lineage information
            tables_with_lineage = nodes_df.filter(
                (col("node_type") == "table") & col("metadata").isNotNull()
            ).select(
                col("node_id").alias("source_node_id"),
                col("object_name").alias("source_table_name"),
                col("metadata"),
            )

            # Extract upstream tables from metadata JSON as an array
            # The JSON structure is: {"table_lineage": {"upstream_tables": ["table1", "table2"]}}

            # Parse the lineage info
            lineage_schema = StructType(
                [
                    StructField(
                        "table_lineage",
                        StructType(
                            [
                                StructField(
                                    "upstream_tables", ArrayType(StringType()), True
                                ),
                                StructField(
                                    "downstream_tables", ArrayType(StringType()), True
                                ),
                            ]
                        ),
                        True,
                    )
                ]
            )

            # Parse JSON and explode downstream_tables (tables that depend on the current table)
            tables_with_parsed_lineage = (
                tables_with_lineage.select(
                    col("source_node_id"),
                    col("source_table_name"),
                    from_json(col("metadata"), lineage_schema).alias("parsed"),
                )
                .select(
                    col("source_node_id"),
                    col("source_table_name"),
                    col("parsed.table_lineage.downstream_tables").alias(
                        "downstream_tables"
                    ),
                )
                .filter(col("downstream_tables").isNotNull())
            )

            # Explode downstream tables to get individual edges
            exploded = tables_with_parsed_lineage.select(
                col("source_node_id"),
                col("source_table_name"),
                explode(col("downstream_tables")).alias("downstream_table"),
            )

            # Join with nodes to get target node IDs
            all_tables = nodes_df.filter(col("node_type") == "table").select(
                col("node_id").alias("target_node_id"),
                col("object_name").alias("target_table_name"),
            )

            # Create edges: source IS_UPSTREAM_OF target (source -> downstream)
            lineage_edges = exploded.join(
                all_tables,
                exploded["downstream_table"] == all_tables["target_table_name"],
                "inner",
            ).select(
                sha2(
                    concat(
                        col("source_node_id"),
                        lit("_IS_UPSTREAM_OF_"),
                        col("target_node_id"),
                    ),
                    256,
                ).alias("edge_id"),
                col("source_node_id"),
                col("target_node_id"),
                lit("IS_UPSTREAM_OF").alias("relationship_type"),
                lit(None).cast("string").alias("metadata"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at"),
            )

            if lineage_edges.count() > 0:
                logger.info(f"Created {lineage_edges.count()} lineage edges")
                return lineage_edges
            else:
                logger.info("No lineage edges found")
                return None

        except Exception as e:
            logger.warning(f"Could not build lineage edges: {e}")
            return None

    def _write_edges(self, edges_df: DataFrame) -> None:
        """Write edges to the edges table using merge."""
        logger.info(f"Writing {edges_df.count()} edges...")

        # Create temp view for merge
        edges_df.createOrReplaceTempView("new_edges")

        # Merge (upsert) edges
        merge_sql = f"""
        MERGE INTO {self.edges_table} target
        USING new_edges source
        ON target.edge_id = source.edge_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(merge_sql)
        logger.info("Edges written successfully")


def build_knowledge_graph(config: MetadataConfig) -> None:
    """
    Main entry point to build the knowledge graph.

    Args:
        config: MetadataConfig with build_knowledge_graph flag
    """
    if not getattr(config, "build_knowledge_graph", False):
        logger.info("Knowledge graph building disabled (build_knowledge_graph=False)")
        return

    logger.info("Building knowledge graph...")
    builder = KnowledgeGraphBuilder(config)
    builder.build_knowledge_graph()
    logger.info("Knowledge graph build complete")

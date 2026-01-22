"""
Payload Logger module for capturing LLM prompts and responses.

Persists prompts sent to LLMs in an append-only table for profiling,
debugging, and analysis purposes. Gated by generate_profiling_data config.
"""

import logging
import hashlib
import json
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

logger = logging.getLogger(__name__)


@dataclass
class PayloadLoggerConfig:
    """Configuration for payload logging."""
    catalog_name: str
    schema_name: str
    target_table: str = "llm_payloads"
    enabled: bool = False
    batch_size: int = 100
    
    @property
    def fully_qualified_target(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.target_table}"


class PayloadBuffer:
    """In-memory buffer for batching payload writes."""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.buffer: List[Dict[str, Any]] = []
    
    def add(self, payload: Dict[str, Any]) -> bool:
        """Add payload to buffer. Returns True if buffer is full."""
        self.buffer.append(payload)
        return len(self.buffer) >= self.max_size
    
    def flush(self) -> List[Dict[str, Any]]:
        """Return and clear buffer contents."""
        payloads = self.buffer.copy()
        self.buffer.clear()
        return payloads
    
    def __len__(self) -> int:
        return len(self.buffer)


class PayloadLogger:
    """
    Logger for capturing LLM payloads in an append-only table.
    
    Usage:
        config = PayloadLoggerConfig(catalog_name="cat", schema_name="sch", enabled=True)
        logger = PayloadLogger(spark, config)
        
        # Log a payload
        logger.log_payload(
            table_name="catalog.schema.table",
            column_names=["col1", "col2"],
            metadata_type="comment",
            prompt_content={"messages": [...]},
            model="gpt-4"
        )
        
        # Flush remaining payloads at end
        logger.flush()
    """
    
    def __init__(self, spark: SparkSession, config: PayloadLoggerConfig):
        self.spark = spark
        self.config = config
        self.buffer = PayloadBuffer(config.batch_size)
        
        if config.enabled:
            self._ensure_table_exists()
    
    def _ensure_table_exists(self) -> None:
        """Create the payloads table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_target} (
            payload_id STRING NOT NULL,
            table_name STRING,
            column_names ARRAY<STRING>,
            metadata_type STRING,
            prompt_content STRING,
            sample_data_hash STRING,
            model STRING,
            created_at TIMESTAMP
        )
        COMMENT 'Append-only log of LLM prompts for profiling and analysis'
        """
        self.spark.sql(ddl)
        logger.info(f"Payload table {self.config.fully_qualified_target} ready")
    
    @staticmethod
    def _hash_content(content: Any) -> str:
        """Create a hash of content for deduplication checking."""
        if isinstance(content, dict):
            content = json.dumps(content, sort_keys=True)
        elif not isinstance(content, str):
            content = str(content)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def log_payload(
        self,
        table_name: str,
        column_names: List[str],
        metadata_type: str,
        prompt_content: Dict[str, Any],
        model: str,
        sample_data: Optional[Any] = None
    ) -> Optional[str]:
        """
        Log a payload to the buffer.
        
        Args:
            table_name: Full table name being processed
            column_names: List of columns included in the prompt
            metadata_type: Type of metadata (comment/pi/domain)
            prompt_content: The prompt messages/content
            model: Model being called
            sample_data: Optional sample data to hash
            
        Returns:
            payload_id if logging is enabled, None otherwise
        """
        if not self.config.enabled:
            return None
        
        payload_id = str(uuid.uuid4())
        
        payload = {
            "payload_id": payload_id,
            "table_name": table_name,
            "column_names": column_names,
            "metadata_type": metadata_type,
            "prompt_content": json.dumps(prompt_content) if isinstance(prompt_content, dict) else str(prompt_content),
            "sample_data_hash": self._hash_content(sample_data) if sample_data else None,
            "model": model,
            "created_at": datetime.utcnow()
        }
        
        if self.buffer.add(payload):
            self._write_buffer()
        
        return payload_id
    
    def _write_buffer(self) -> int:
        """Write buffered payloads to the table."""
        payloads = self.buffer.flush()
        if not payloads:
            return 0
        
        # Create DataFrame from payloads
        schema = StructType([
            StructField("payload_id", StringType(), False),
            StructField("table_name", StringType(), True),
            StructField("column_names", ArrayType(StringType()), True),
            StructField("metadata_type", StringType(), True),
            StructField("prompt_content", StringType(), True),
            StructField("sample_data_hash", StringType(), True),
            StructField("model", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        
        df = self.spark.createDataFrame(payloads, schema)
        
        # Append to table
        df.write.mode("append").saveAsTable(self.config.fully_qualified_target)
        
        logger.debug(f"Wrote {len(payloads)} payloads to {self.config.fully_qualified_target}")
        return len(payloads)
    
    def flush(self) -> int:
        """Flush any remaining buffered payloads."""
        if not self.config.enabled:
            return 0
        return self._write_buffer()
    
    def get_payload_count(self) -> int:
        """Get total count of logged payloads."""
        if not self.config.enabled:
            return 0
        try:
            return self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM {self.config.fully_qualified_target}"
            ).collect()[0]["cnt"]
        except Exception:
            return 0


# Global instance for singleton pattern
_payload_logger: Optional[PayloadLogger] = None


def get_payload_logger(
    spark: SparkSession = None,
    catalog_name: str = None,
    schema_name: str = None,
    enabled: bool = False
) -> PayloadLogger:
    """
    Get or create the global payload logger instance.
    
    Args:
        spark: SparkSession (required on first call)
        catalog_name: Catalog name (required on first call)
        schema_name: Schema name (required on first call)
        enabled: Whether logging is enabled
        
    Returns:
        PayloadLogger instance
    """
    global _payload_logger
    
    if _payload_logger is None:
        if spark is None or catalog_name is None or schema_name is None:
            raise ValueError("Must provide spark, catalog_name, and schema_name on first call")
        
        config = PayloadLoggerConfig(
            catalog_name=catalog_name,
            schema_name=schema_name,
            enabled=enabled
        )
        _payload_logger = PayloadLogger(spark, config)
    
    return _payload_logger


def reset_payload_logger() -> None:
    """Reset the global payload logger (useful for testing)."""
    global _payload_logger
    if _payload_logger is not None:
        _payload_logger.flush()
    _payload_logger = None


"""
Pydantic models following APX 3-model pattern.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


# ============================================================================
# UC BROWSER MODELS
# ============================================================================

class CatalogOut(BaseModel):
    """Catalog output model."""
    name: str
    comment: Optional[str] = None
    owner: Optional[str] = None


class SchemaOut(BaseModel):
    """Schema output model."""
    name: str
    catalog_name: str
    comment: Optional[str] = None
    owner: Optional[str] = None


class TableOut(BaseModel):
    """Table output model."""
    name: str
    catalog_name: str
    schema_name: str
    table_type: str
    comment: Optional[str] = None
    owner: Optional[str] = None
    fqn: str


class ColumnOut(BaseModel):
    """Column output model."""
    name: str
    type_text: str
    type_name: str
    comment: Optional[str] = None
    nullable: bool
    position: int


class TableSelectionIn(BaseModel):
    """Input for saving table selection."""
    table_fqns: List[str]


class TableSelectionOut(BaseModel):
    """Table selection output."""
    table_fqns: List[str]
    count: int


# ============================================================================
# ENRICHMENT MODELS
# ============================================================================

class EnrichmentStatus(str, Enum):
    """Enrichment job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class EnrichmentRunIn(BaseModel):
    """Input for running enrichment."""
    table_fqns: List[str]
    metadata_table: Optional[str] = "serverless_dbx_unifiedchat_catalog.gold.enriched_table_metadata"
    chunks_table: Optional[str] = "serverless_dbx_unifiedchat_catalog.gold.enriched_table_chunks"
    write_mode: Optional[str] = "overwrite"  # overwrite, append, or error


class EnrichmentJobOut(BaseModel):
    """Response when submitting enrichment job."""
    run_id: int
    job_url: str
    status: EnrichmentStatus
    table_count: int
    submitted_at: str


class EnrichmentJobStatusOut(BaseModel):
    """Job status response."""
    run_id: int
    status: EnrichmentStatus
    job_url: str
    life_cycle_state: Optional[str] = None  # PENDING, RUNNING, TERMINATING, TERMINATED
    result_state: Optional[str] = None  # SUCCESS, FAILED, CANCELLED
    state_message: Optional[str] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    duration_ms: Optional[int] = None


class EnrichmentStatusOut(BaseModel):
    """Enrichment job status (legacy - kept for compatibility)."""
    job_id: str
    status: EnrichmentStatus
    progress: int
    total: int
    error: Optional[str] = None


class ColumnEnriched(BaseModel):
    """Enriched column model."""
    name: str
    type: str
    comment: str
    sample_values: List[str]


class EnrichmentResultOut(BaseModel):
    """Full enrichment result for a table."""
    fqn: str
    catalog: str
    schema_name: str = Field(..., alias="schema")
    table: str
    column_count: int
    columns: List[ColumnEnriched]
    enriched: bool
    timestamp: str

    class Config:
        populate_by_name = True


class TablePreviewOut(BaseModel):
    """Preview of a table."""
    table_fqn: str
    row_count: int
    columns: List[str]
    sample_rows: List[Dict[str, Any]]


class EnrichmentResultListOut(BaseModel):
    """Summary enrichment result."""
    fqn: str
    column_count: int
    enriched: bool
    columns: List[str] = []


# ============================================================================
# GRAPH MODELS
# ============================================================================

class GraphBuildStatus(str, Enum):
    """Graph build status."""
    PENDING = "pending"
    BUILDING = "building"
    COMPLETED = "completed"
    FAILED = "failed"


class GraphBuildStatusOut(BaseModel):
    """Graph build status."""
    job_id: str
    status: GraphBuildStatus
    error: Optional[str] = None


class GraphBuildLogOut(BaseModel):
    """Graph build log entry."""
    timestamp: str
    message: str
    level: str = "info"


class GraphNodeOut(BaseModel):
    """Graph node (table) model."""
    id: str
    label: str
    catalog: str
    schema_name: str = Field(..., alias="schema")
    column_count: int
    community: int
    columns: List[str]

    class Config:
        populate_by_name = True


class GraphEdgeOut(BaseModel):
    """Graph edge (relationship) model."""
    source: str
    target: str
    weight: float
    types: str


class GraphDataOut(BaseModel):
    """Full graph data."""
    elements: List[dict]
    node_count: int
    edge_count: int
    communities: Optional[dict] = None


class GroupDescriptionsOut(BaseModel):
    """LLM-generated verbal descriptions for each schema and community group."""
    schemas: Dict[str, str]
    communities: Dict[str, str]


# ============================================================================
# GENIE ROOM MODELS
# ============================================================================

class GenieRoomStatus(str, Enum):
    """Genie room status."""
    PENDING = "pending"
    CREATING = "creating"
    CREATED = "created"
    FAILED = "failed"


class GenieRoomIn(BaseModel):
    """Input for creating a Genie room definition."""
    name: str = Field(..., min_length=1)
    table_fqns: List[str] = Field(..., min_items=1)


class GenieRoomUpdateIn(BaseModel):
    """Input for updating a Genie room definition."""
    name: Optional[str] = None
    table_fqns: Optional[List[str]] = None


class GenieRoomOut(BaseModel):
    """Full Genie room output."""
    id: str
    name: str
    tables: List[str]
    table_count: int
    community_id: Optional[str] = None  # Set when generated from graph communities


class GenieRoomListOut(BaseModel):
    """Summary Genie room for lists."""
    id: str
    name: str
    table_count: int


class GenieCreationStatusOut(BaseModel):
    """Status of Genie room creation process."""
    status: str
    rooms: List[dict]


class CreatedGenieRoomOut(BaseModel):
    """Created Genie room with URL."""
    id: str
    name: str
    space_id: str
    url: str
    table_count: int
    status: GenieRoomStatus

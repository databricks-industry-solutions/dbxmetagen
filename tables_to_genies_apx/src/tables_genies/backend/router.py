"""
FastAPI routes following APX patterns.
All routes have response_model and operation_id (required for OpenAPI generation).

Service Principal Authentication:
This backend uses Service Principal (App SP) authentication for Databricks.
Required environment variables:
- DATABRICKS_HOST: Workspace URL (e.g., https://adb-1234567890123456.7.azuredatabricks.net/)
- DATABRICKS_CLIENT_ID: Service Principal Application ID
- DATABRICKS_CLIENT_SECRET: Service Principal Secret
- DATABRICKS_SQL_WAREHOUSE_ID: SQL Warehouse ID for Genie spaces (optional, defaults to configured ID)
"""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from .models import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.jobs import Task, NotebookTask, JobEnvironment, Source
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from databricks import sql
import uuid
import threading
import os
import json
import asyncio
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Initialize SDK with environment-aware authentication
# - Local development: Uses PROD profile with PAT from ~/.databrickscfg
# - Databricks App: Uses App Service Principal (SP) authentication automatically
def _is_running_in_databricks_app() -> bool:
    """Detect if running in a Databricks App environment."""
    # Check for Databricks App environment variables
    return (
        os.getenv("DATABRICKS_RUNTIME_VERSION") is not None or
        os.getenv("DB_IS_DRIVER") == "TRUE" or
        (os.getenv("DATABRICKS_CLIENT_ID") is not None and 
         os.getenv("DATABRICKS_CLIENT_SECRET") is not None)
    )

# Configure authentication based on environment
if _is_running_in_databricks_app():
    logger.info("[Auth] Running in Databricks App - using App SP authentication")
    print("[Auth] Running in Databricks App - using App SP authentication", flush=True)
    
    # Validate required Service Principal environment variables
    required_env_vars = ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required Service Principal environment variables: {missing_vars}")
    
    config = Config(
        host=os.getenv("DATABRICKS_HOST"),
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
    )
else:
    logger.info("[Auth] Running locally - using PROD profile with PAT")
    print("[Auth] Running locally - using PROD profile with PAT", flush=True)
    config = Config(profile="PROD")  # Local dev with PAT from ~/.databrickscfg

try:
    client = WorkspaceClient(config=config)
    # Test the authentication by getting current user
    current_user = client.current_user.me()
    logger.info(f"[Auth] ✅ Successfully authenticated: {current_user.display_name}")
    print(f"[Auth] ✅ Successfully authenticated: {current_user.display_name}", flush=True)
except Exception as e:
    logger.error(f"[Auth] ❌ Authentication failed: {e}")
    print(f"[Auth] ❌ Authentication failed: {e}", flush=True)
    raise

warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "a4ed2ccbda385db9")
logger.info(f"[Config] Using SQL Warehouse ID: {warehouse_id}")
print(f"[Config] Using SQL Warehouse ID: {warehouse_id}", flush=True)

# In-memory storage with file persistence for development
CACHE_DIR = "/tmp/tables_to_genies_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
GRAPH_DATA_FILE = os.path.join(CACHE_DIR, "graph_data.json")

_selection: TableSelectionOut = TableSelectionOut(table_fqns=[], count=0)
_graph_data: Optional[GraphDataOut] = None

# Load cached graph data if exists
if os.path.exists(GRAPH_DATA_FILE):
    try:
        with open(GRAPH_DATA_FILE, 'r') as f:
            data = json.load(f)
            _graph_data = GraphDataOut(**data)
            print(f"Loaded cached graph data from {GRAPH_DATA_FILE}")
    except Exception as e:
        print(f"Failed to load cached graph data: {e}")

_graph_build_logs: List[GraphBuildLogOut] = []
_genie_rooms = []
_genie_creation_status: Optional[GenieCreationStatusOut] = None
_group_descriptions: Optional[GroupDescriptionsOut] = None

api = APIRouter(prefix="/api")

# ============================================================================
# WORKFLOW RESET
# ============================================================================

@api.post("/reset", operation_id="resetWorkflow")
async def reset_workflow():
    """Reset all server-side workflow state."""
    global _selection, _graph_data, _graph_build_logs, _genie_rooms, _genie_creation_status, _group_descriptions
    _selection = TableSelectionOut(table_fqns=[], count=0)
    _graph_data = None
    _graph_build_logs = []
    _genie_rooms = []
    _genie_creation_status = None
    _group_descriptions = None

    # Remove cached graph data from disk
    if os.path.exists(GRAPH_DATA_FILE):
        try:
            os.remove(GRAPH_DATA_FILE)
        except Exception as e:
            logger.warning(f"Failed to remove cached graph data: {e}")

    return {"message": "Workflow state reset"}


# ============================================================================
# UC CATALOG BROWSER ROUTES
# ============================================================================

@api.get("/uc/catalogs", response_model=List[CatalogOut], operation_id="listCatalogs")
async def list_catalogs():
    """List all catalogs."""
    try:
        catalogs = list(client.catalogs.list())
        return [CatalogOut(
            name=cat.name,
            comment=cat.comment,
            owner=cat.owner
        ) for cat in catalogs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api.get("/uc/catalogs/{catalog}/schemas", response_model=List[SchemaOut], operation_id="listSchemas")
async def list_schemas(catalog: str):
    """List schemas in a catalog."""
    try:
        schemas = list(client.schemas.list(catalog_name=catalog))
        return [SchemaOut(
            name=schema.name,
            catalog_name=schema.catalog_name,
            comment=schema.comment,
            owner=schema.owner
        ) for schema in schemas]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api.get("/uc/catalogs/{catalog}/schemas/{schema}/tables", response_model=List[TableOut], operation_id="listTables")
async def list_tables(catalog: str, schema: str):
    """List tables in a schema."""
    try:
        tables = list(client.tables.list(catalog_name=catalog, schema_name=schema))
        return [TableOut(
            name=table.name,
            catalog_name=table.catalog_name,
            schema_name=table.schema_name,
            table_type=table.table_type.value if table.table_type else 'TABLE',
            comment=table.comment,
            owner=table.owner,
            fqn=f"{table.catalog_name}.{table.schema_name}.{table.name}"
        ) for table in tables]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api.get("/uc/tables/{fqn:path}/columns", response_model=List[ColumnOut], operation_id="getTableColumns")
async def get_table_columns(fqn: str):
    """Get columns for a table."""
    try:
        table = client.tables.get(full_name=fqn)
        if not table.columns:
            return []
        
        return [ColumnOut(
            name=col.name,
            type_text=col.type_text,
            type_name=col.type_name.value if col.type_name else col.type_text,
            comment=col.comment or '',
            nullable=col.nullable if col.nullable is not None else True,
            position=col.position
        ) for col in table.columns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api.post("/uc/selection", response_model=TableSelectionOut, operation_id="saveSelection")
async def save_selection(selection: TableSelectionIn):
    """Save table selection."""
    global _selection
    _selection = TableSelectionOut(
        table_fqns=selection.table_fqns,
        count=len(selection.table_fqns)
    )
    return _selection


@api.get("/uc/selection", response_model=TableSelectionOut, operation_id="getSelection")
async def get_selection():
    """Get current table selection."""
    return _selection


# ============================================================================
# ENRICHMENT ROUTES - DATABRICKS JOB SUBMISSION
# ============================================================================

# Configuration for enrichment job
# Path is set via environment variable in app.yaml for deployed apps
# Default points to merged folder location in Databricks Workspace
ENRICHMENT_SCRIPT_PATH = os.getenv(
    "ENRICHMENT_SCRIPT_PATH", 
    "/Workspace/Users/yang.yang@databricks.com/tables_to_genies_apx/src/tables_genies/etl/enrich_tables_direct.py"
)

@api.post("/enrichment/run", response_model=EnrichmentJobOut, operation_id="runEnrichment")
async def run_enrichment(enrichment_in: EnrichmentRunIn):
    """Run table enrichment job on Databricks."""
    
    # Clean up old temporary tables (older than 24 hours)
    def _cleanup_old_temp_tables():
        try:
            local_client = WorkspaceClient(config=config)
            cleanup_stmt = """
            DROP TABLE IF EXISTS serverless_dbx_unifiedchat_catalog.gold.temp_enrichment_tables_old;
            -- Note: Individual temp tables are named with UUID suffixes
            -- Manual cleanup may be needed for very old temp tables
            """
            local_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=cleanup_stmt,
                wait_timeout="10s"
            )
        except Exception as e:
            logger.warning(f"Failed to cleanup old temp tables: {e}")
    
    # Run cleanup in background (non-blocking)
    try:
        await asyncio.to_thread(_cleanup_old_temp_tables)
    except:
        pass  # Don't fail if cleanup fails
    
    # Generate unique run ID for this enrichment job
    import uuid
    run_uuid = str(uuid.uuid4())[:8]
    
    # Create temporary table with list of tables to enrich
    # This avoids the 10KB parameter size limit when dealing with many tables
    temp_table_name = f"serverless_dbx_unifiedchat_catalog.gold.temp_enrichment_tables_{run_uuid}"
    
    print(f"Creating temporary table with {len(enrichment_in.table_fqns)} tables: {temp_table_name}")
    
    # Write table list to temporary Delta table
    def _write_temp_table():
        local_client = WorkspaceClient(config=config)
        
        # Create SQL statement to create temp table with table list
        tables_values = ', '.join([f"('{fqn}')" for fqn in enrichment_in.table_fqns])
        
        # Correct syntax: CREATE TABLE AS VALUES (without SELECT * FROM)
        create_stmt = f"""
        CREATE OR REPLACE TABLE {temp_table_name} AS
        VALUES {tables_values} AS t(table_fqn)
        """
        
        print(f"[Enrichment] Creating temp table with {len(enrichment_in.table_fqns)} tables")
        print(f"[Enrichment] SQL preview: {create_stmt[:150]}...")
        
        response = local_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=create_stmt,
            wait_timeout="30s"
        )
        
        # Check if statement succeeded
        if response.status.state != StatementState.SUCCEEDED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            print(f"[ERROR] Statement failed. State: {response.status.state}, Error: {error_msg}")
            raise Exception(f"Failed to create temp table: {error_msg}")
        
        print(f"[Enrichment] ✓ Statement succeeded. State: {response.status.state}")
        return response
    
    try:
        response = await asyncio.to_thread(_write_temp_table)
        print(f"✓ Created temporary table: {temp_table_name}")
    except Exception as e:
        print(f"[ERROR] Failed to create temp table: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create temp table: {str(e)}")
    
    # Job name for persistent job definition
    job_name = "Table Enrichment Job"
    
    # Check if job already exists
    existing_jobs = list(client.jobs.list(name=job_name))
    
    if existing_jobs:
        # Use existing job
        job_id = existing_jobs[0].job_id
        print(f"Using existing job: {job_id}")
    else:
        # Create new persistent job using notebook task
        print(f"Creating new job: {job_name}")
        job = client.jobs.create(
            name=job_name,
            tasks=[
                Task(
                    task_key="enrich_tables",
                    notebook_task=NotebookTask(
                        notebook_path=ENRICHMENT_SCRIPT_PATH,
                        source=Source.WORKSPACE
                        # base_parameters will be overridden on each run via run_now()
                    ),
                    environment_key="serverless_env"
                )
            ],
            environments=[
                JobEnvironment(
                    environment_key="serverless_env",
                    spec=Environment(
                        client="1"  # Serverless environment
                    )
                )
            ]
        )
        job_id = job.job_id
        print(f"Created job: {job_id}")
    
    # Run the job with notebook parameters
    # Pass the temp table name instead of the full comma-separated list
    run_result = client.jobs.run_now(
        job_id=job_id,
        notebook_params={
            "table_list_table": temp_table_name,  # NEW: pass temp table name
            "sample_size": "20",
            "max_unique_values": "50",
            "llm_endpoint": "databricks-claude-sonnet-4-5",
            "metadata_table": enrichment_in.metadata_table,
            "chunks_table": enrichment_in.chunks_table,
            "write_mode": enrichment_in.write_mode
        }
    )
    
    # Get run details to access run_page_url
    run_details = client.jobs.get_run(run_id=run_result.run_id)
    job_url = run_details.run_page_url
    
    return EnrichmentJobOut(
        run_id=run_result.run_id,
        job_url=job_url,
        status=EnrichmentStatus.PENDING,
        table_count=len(enrichment_in.table_fqns),
        submitted_at=datetime.now().isoformat()
    )


@api.get("/enrichment/status/{run_id}", response_model=EnrichmentJobStatusOut, operation_id="getEnrichmentStatus")
async def get_enrichment_status(run_id: int):
    """Get Databricks job status."""
    
    try:
        run = client.jobs.get_run(run_id=run_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Job run not found: {str(e)}")
    
    # Map Databricks states to our status
    life_cycle_state = run.state.life_cycle_state.value if run.state.life_cycle_state else None
    result_state = run.state.result_state.value if run.state.result_state else None
    
    status = EnrichmentStatus.PENDING
    if life_cycle_state == "RUNNING" or life_cycle_state == "PENDING":
        status = EnrichmentStatus.RUNNING
    elif life_cycle_state == "TERMINATING":
        status = EnrichmentStatus.RUNNING  # Still considered running
    elif life_cycle_state == "TERMINATED":
        if result_state == "SUCCESS":
            status = EnrichmentStatus.COMPLETED
        elif result_state == "FAILED":
            status = EnrichmentStatus.FAILED
        elif result_state == "CANCELED":
            status = EnrichmentStatus.CANCELLED
        else:
            status = EnrichmentStatus.FAILED  # Default to failed for unknown result states
    elif life_cycle_state == "SKIPPED":
        status = EnrichmentStatus.CANCELLED
    elif life_cycle_state == "INTERNAL_ERROR":
        status = EnrichmentStatus.FAILED
    
    # Use the correct job URL from Databricks
    job_url = run.run_page_url
    
    return EnrichmentJobStatusOut(
        run_id=run_id,
        status=status,
        job_url=job_url,
        life_cycle_state=life_cycle_state,
        result_state=result_state,
        state_message=run.state.state_message,
        start_time=run.start_time,
        end_time=run.end_time,
        duration_ms=(run.end_time - run.start_time) if run.end_time and run.start_time else None
    )


@api.get("/enrichment/results", response_model=List[EnrichmentResultListOut], operation_id="listEnrichmentResults")
async def list_enrichment_results():
    """List enriched tables from Unity Catalog."""
    
    try:
        # Use Statement Execution API (non-blocking)
        statement = """
            SELECT table_fqn, enriched_doc 
            FROM serverless_dbx_unifiedchat_catalog.gold.enriched_table_metadata 
            WHERE enriched = true
            ORDER BY id DESC
            LIMIT 1000
        """
        
        # Run blocking SDK call in thread pool
        def _fetch():
            # Create a fresh client for this thread
            local_client = WorkspaceClient(config=config)
            return local_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                wait_timeout="30s"
            )
            
        res = await asyncio.to_thread(_fetch)
        
        results = []
        if res.result and res.result.data_array:
            for row in res.result.data_array:
                table_fqn = row[0]
                enriched_doc = json.loads(row[1])
                
                results.append(EnrichmentResultListOut(
                    fqn=table_fqn,
                    column_count=enriched_doc.get('total_columns', 0),
                    enriched=enriched_doc.get('enriched', False),
                    columns=[col['column_name'] for col in enriched_doc.get('enriched_columns', [])]
                ))
        
        return results
        
    except Exception as e:
        print(f"Error fetching enrichment results: {e}")
        return []


@api.get("/enrichment/results/{fqn:path}", response_model=EnrichmentResultOut, operation_id="getEnrichmentResult")
async def get_enrichment_result(fqn: str):
    """Get enrichment result for specific table from Unity Catalog."""
    
    try:
        # Use Statement Execution API (non-blocking)
        statement = f"SELECT enriched_doc FROM serverless_dbx_unifiedchat_catalog.gold.enriched_table_metadata WHERE table_fqn = '{fqn}'"
        
        def _fetch():
            local_client = WorkspaceClient(config=config)
            return local_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                wait_timeout="30s"
            )
            
        res = await asyncio.to_thread(_fetch)
        
        if not res.result or not res.result.data_array:
            raise HTTPException(status_code=404, detail=f"Table {fqn} not found")
        
        enriched_doc = json.loads(res.result.data_array[0][0])
        
        # Convert to response model
        return EnrichmentResultOut(
            fqn=enriched_doc['table_fqn'],
            catalog=enriched_doc['catalog'],
            schema=enriched_doc['schema'],
            table=enriched_doc['table'],
            column_count=enriched_doc['total_columns'],
            columns=[
                ColumnEnriched(
                    name=col['column_name'],
                    type=col['data_type'],
                    comment=col.get('enhanced_comment', col.get('comment', '')),
                    sample_values=col.get('sample_values', [])
                )
                for col in enriched_doc['enriched_columns'][:10]
            ],
            enriched=enriched_doc['enriched'],
            timestamp=enriched_doc['enrichment_timestamp']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching result: {str(e)}")


# ============================================================================
# GRAPH ROUTES
# ============================================================================

def _add_graph_log(message: str, level: str = "info"):
    """Add a log entry for graph building."""
    global _graph_build_logs
    _graph_build_logs.append(GraphBuildLogOut(
        timestamp=datetime.now().strftime("%H:%M:%S"),
        message=message,
        level=level
    ))
    print(f"[{level.upper()}] {message}")

@api.get("/graph/build-logs", response_model=List[GraphBuildLogOut], operation_id="getGraphBuildLogs")
async def get_graph_build_logs():
    """Get graph build logs."""
    return _graph_build_logs

@api.post("/graph/build", response_model=GraphBuildStatusOut, operation_id="buildGraph")
async def build_graph():
    """Build table relationship graph using LLM-powered GraphRAG approach."""
    global _graph_data, _graph_build_logs, _group_descriptions
    
    _graph_build_logs = []
    _group_descriptions = None  # Invalidate cached descriptions so they regenerate for the new graph
    _add_graph_log("Starting LLM-powered GraphRAG graph build process...")
    
        # Fetch full enriched documents with table descriptions
    _add_graph_log("Fetching full enrichment metadata from Unity Catalog...")
    
    try:
        # Fetch full enriched_doc JSON
        statement = """
            SELECT table_fqn, enriched_doc 
            FROM serverless_dbx_unifiedchat_catalog.gold.enriched_table_metadata 
            WHERE enriched = true
            ORDER BY id DESC
            LIMIT 1000
        """
        
        def _fetch():
            local_client = WorkspaceClient(config=config)
            return local_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                wait_timeout="30s"
            )
            
        res = await asyncio.to_thread(_fetch)
        
        if not res.result or not res.result.data_array:
            _add_graph_log("No enrichment results available", level="error")
            raise HTTPException(status_code=400, detail="No enrichment results available")
        
        _add_graph_log(f"Found {len(res.result.data_array)} enriched tables.")
        
        # Import GraphRAG module from new location
        import sys
        from pathlib import Path
        
        current_file = Path(__file__).resolve()
        # graphrag is now a sibling to backend/
        graphrag_path = current_file.parent.parent / "graphrag"
        
        if not graphrag_path.exists():
            _add_graph_log(f"Error: GraphRAG path does not exist: {graphrag_path}", level="error")
            raise HTTPException(status_code=500, detail=f"GraphRAG path not found: {graphrag_path}")
            
        if str(graphrag_path) not in sys.path:
            sys.path.insert(0, str(graphrag_path))
        
        from build_table_graph import GraphRAGTableGraphBuilder
        
        _add_graph_log("Initializing GraphRAG Table Graph Builder...")
        builder = GraphRAGTableGraphBuilder()
        
        # Parse enriched documents
        _add_graph_log("Parsing enriched metadata (descriptions, columns)...")
        enriched_tables_data = []
        for row in res.result.data_array:
            table_fqn = row[0]
            enriched_doc = json.loads(row[1])
            parts = table_fqn.split('.')
            
            enriched_tables_data.append({
                'fqn': table_fqn,
                'catalog': parts[0],
                'schema': parts[1],
                'table': parts[2],
                'column_count': enriched_doc.get('total_columns', 0),
                'columns': [{'name': col['column_name']} for col in enriched_doc.get('enriched_columns', [])],
                'enriched': enriched_doc.get('enriched', False),
                'table_description': enriched_doc.get('table_description', ''),
                'enriched_columns': enriched_doc.get('enriched_columns', [])
            })
        
        # Define LLM function wrapper
        async def llm_func(prompt: str) -> str:
            """Call LLM via ai_query through statement execution."""
            _add_graph_log("Calling LLM for analysis...")
            
            def _llm_call():
                local_client = WorkspaceClient(config=config)
                # Use ai_query with Claude Sonnet
                llm_statement = "SELECT ai_query('databricks-claude-sonnet-4-5', :prompt) as result"
                param = StatementParameterListItem(name='prompt', value=prompt, type='STRING')
                return local_client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=llm_statement,
                    parameters=[param],
                    wait_timeout="50s"
                )
            
            llm_res = await asyncio.to_thread(_llm_call)
            
            if llm_res.result and llm_res.result.data_array:
                return llm_res.result.data_array[0][0]
            else:
                raise Exception("LLM call returned no results")
        
        # Build graph with LLM-powered semantic analysis
        _add_graph_log("Building graph with structural + semantic analysis...")
        _add_graph_log("  → Phase 1: Structural analysis (schema, columns, FK hints)...")
        _add_graph_log("  → Phase 2: LLM entity extraction...")
        _add_graph_log("  → Phase 3: LLM semantic relationship detection...")
        
        G = await builder.build_graph(enriched_tables_data, llm_func=llm_func)
        
        _add_graph_log(f"Graph constructed: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")
        
        # Log semantic entities if extracted
        if builder.semantic_entities:
            _add_graph_log(f"LLM extracted entities for {len(builder.semantic_entities)} tables.")
            sample_fqn = list(builder.semantic_entities.keys())[0]
            sample_entities = builder.semantic_entities[sample_fqn]
            _add_graph_log(f"  Example: {sample_fqn.split('.')[-1]} → domain: {sample_entities.get('domain', 'N/A')}")
        
        # Count semantic edges
        semantic_edge_count = sum(1 for _, _, data in G.edges(data=True) if 'semantic' in data.get('types', ''))
        if semantic_edge_count > 0:
            _add_graph_log(f"Discovered {semantic_edge_count} semantic relationships via LLM.", level="success")
            
            # Log sample semantic relationships
            for u, v, data in list(G.edges(data=True))[:3]:
                if 'semantic' in data.get('types', ''):
                    reason = data.get('semantic_reason', 'N/A')
                    _add_graph_log(f"  {u.split('.')[-1]} <-> {v.split('.')[-1]}: {reason}")
        
        # Detect communities
        _add_graph_log("Running community detection...")
        num_communities = len(set(G.nodes[n].get('community', 0) for n in G.nodes()))
        _add_graph_log(f"Identified {num_communities} distinct communities.")
        
        # Convert to Cytoscape format
        _add_graph_log("Formatting graph for visualization...")
        graph_output = builder.to_cytoscape_format()
        
        _graph_data = GraphDataOut(
            elements=graph_output['elements'],
            node_count=graph_output['node_count'],
            edge_count=graph_output['edge_count'],
            communities=graph_output.get('communities')
        )
        
        # Persist to disk for development
        try:
            with open(GRAPH_DATA_FILE, 'w') as f:
                f.write(_graph_data.model_dump_json())
            _add_graph_log("Graph data persisted to disk for faster reloads.")
        except Exception as e:
            _add_graph_log(f"Warning: Failed to persist graph data to disk: {e}", level="warn")
        
        _add_graph_log("LLM-powered GraphRAG build completed successfully.", level="success")
        return GraphBuildStatusOut(job_id="graph-1", status=GraphBuildStatus.COMPLETED)
        
    except Exception as e:
        import traceback
        _add_graph_log(f"Error building graph: {str(e)}", level="error")
        _add_graph_log(f"Traceback: {traceback.format_exc()}", level="error")
        raise HTTPException(status_code=500, detail=str(e))


@api.get("/graph/data", response_model=GraphDataOut, operation_id="getGraphData")
async def get_graph_data():
    """Get graph data."""
    if not _graph_data:
        raise HTTPException(status_code=404, detail="Graph not built yet")
    return _graph_data


@api.get("/graph/group-descriptions", response_model=GroupDescriptionsOut, operation_id="getGroupDescriptions")
async def get_group_descriptions():
    """Get LLM-generated verbal descriptions for each schema and community in the graph.
    Results are cached in memory until the workflow is reset."""
    global _group_descriptions

    if _group_descriptions is not None:
        return _group_descriptions

    if not _graph_data:
        raise HTTPException(status_code=404, detail="Graph not built yet")

    # Collect tables and their descriptions grouped by schema and community
    schema_tables: dict[str, list[str]] = {}
    community_tables: dict[str, list[str]] = {}

    for elem in _graph_data.elements:
        if "source" not in elem["data"]:  # node, not edge
            fqn = elem["data"]["id"]
            short_name = fqn.split(".")[-1]
            schema = elem["data"].get("schema", "")
            community = str(elem["data"].get("community", 0))
            desc = (elem["data"].get("table_description") or "").strip()
            entry = f"{short_name}: {desc}" if desc else short_name

            if schema:
                schema_tables.setdefault(schema, []).append(entry)
            community_tables.setdefault(community, []).append(entry)

    async def describe_group(key: str, entries: list[str], group_type: str) -> tuple[str, str]:
        """Call databricks-gpt-oss-20b via ai_query to summarize a group."""
        if not entries:
            return key, ""
        tables_text = "\n".join(entries[:8])  # cap to 8 tables to keep prompt short
        prompt = (
            f"The following {group_type} contains these database tables:\n{tables_text}\n\n"
            f"Write exactly one sentence (max 25 words) describing what this {group_type} covers, "
            f"mentioning the data domain and key analysis possibilities. "
            f"Do not start with 'This schema' or 'This community'. Be specific and direct."
        )

        def _call():
            local_client = WorkspaceClient(config=config)
            stmt = "SELECT ai_query('databricks-gpt-oss-20b', :prompt) as result"
            param = StatementParameterListItem(name="prompt", value=prompt, type="STRING")
            return local_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=stmt,
                parameters=[param],
                wait_timeout="30s",
            )

        try:
            res = await asyncio.to_thread(_call)
            if res.result and res.result.data_array:
                raw = res.result.data_array[0][0] or ""
                return key, raw.strip().strip('"').strip("'")
        except Exception as e:
            logger.warning(f"LLM description failed for {group_type} '{key}': {e}")
        return key, ""

    # Fire all LLM calls concurrently to keep total latency low
    schema_tasks = [describe_group(s, entries, "database schema") for s, entries in schema_tables.items()]
    community_tasks = [describe_group(c, entries, "data community") for c, entries in community_tables.items()]

    schema_results, community_results = await asyncio.gather(
        asyncio.gather(*schema_tasks),
        asyncio.gather(*community_tasks),
    )

    _group_descriptions = GroupDescriptionsOut(
        schemas={k: v for k, v in schema_results},
        communities={k: v for k, v in community_results},
    )
    return _group_descriptions


# ============================================================================
# GENIE ROOM ROUTES
# ============================================================================

@api.post("/genie/generate-from-communities", response_model=List[GenieRoomOut], operation_id="generateFromCommunities")
async def generate_from_communities():
    """Generate Genie rooms based on graph communities."""
    global _genie_rooms
    
    if not _graph_data:
        raise HTTPException(status_code=400, detail="Graph must be built first to detect communities")
    
    # Group tables by community
    community_groups = {}
    for elem in _graph_data.elements:
        if "source" not in elem["data"]: # It's a node
            community_id = elem["data"].get("community", 0)
            table_fqn = elem["data"]["id"]
            
            if community_id not in community_groups:
                community_groups[community_id] = []
            community_groups[community_id].append(table_fqn)
    
    # Create rooms for each community
    new_rooms = []
    for community_id, tables in community_groups.items():
        # Generate a descriptive name for the community if possible, or use a default
        room_name = f"Community {community_id} Room"
        
        # Try to find a better name from the tables (e.g., most common schema or a keyword)
        schemas = [t.split('.')[1] for t in tables]
        if schemas:
            most_common_schema = max(set(schemas), key=schemas.count)
            room_name = f"{most_common_schema.replace('_', ' ').title()} Community {community_id}"

        room = GenieRoomOut(
            id=f"room-comm-{community_id}-{str(uuid.uuid4())[:4]}",
            name=room_name,
            tables=tables,
            table_count=len(tables),
            community_id=str(community_id),  # Stored so the frontend can match by ID, not fragile name
        )
        new_rooms.append(room)
    
    # Replace or append? User said "populate the Panel", usually means replace or add to.
    # Let's append but avoid duplicates by community_id (more reliable than name)
    existing_community_ids = {r.community_id for r in _genie_rooms if r.community_id is not None}
    existing_names = {r.name for r in _genie_rooms}
    for nr in new_rooms:
        if nr.community_id not in existing_community_ids and nr.name not in existing_names:
            _genie_rooms.append(nr)
            existing_community_ids.add(nr.community_id)
            existing_names.add(nr.name)
            
    return _genie_rooms


@api.post("/genie/generate-from-schemas", response_model=List[GenieRoomOut], operation_id="generateFromSchemas")
async def generate_from_schemas():
    """Generate Genie rooms by grouping tables by their database schema (catalog.schema.table)."""
    global _genie_rooms

    if not _graph_data:
        raise HTTPException(status_code=400, detail="Graph must be built first")

    # Group table FQNs by schema (middle segment of catalog.schema.table)
    schema_groups: dict[str, list[str]] = {}
    for elem in _graph_data.elements:
        if "source" not in elem["data"]:  # node, not edge
            table_fqn = elem["data"]["id"]
            parts = table_fqn.split(".")
            schema = parts[1] if len(parts) >= 3 else "default"
            schema_groups.setdefault(schema, []).append(table_fqn)

    new_rooms = []
    for schema, tables in sorted(schema_groups.items()):
        room_name = schema.replace("_", " ").title()
        room = GenieRoomOut(
            id=f"room-schema-{schema}-{str(uuid.uuid4())[:4]}",
            name=room_name,
            tables=tables,
            table_count=len(tables),
        )
        new_rooms.append(room)

    # Append, avoiding name duplicates
    existing_names = {r.name for r in _genie_rooms}
    for nr in new_rooms:
        if nr.name not in existing_names:
            _genie_rooms.append(nr)
            existing_names.add(nr.name)

    return _genie_rooms


@api.post("/genie/rooms", response_model=GenieRoomOut, operation_id="createGenieRoom")
async def create_genie_room(room_in: GenieRoomIn):
    """Create a Genie room definition."""
    room = GenieRoomOut(
        id=f"room-{str(uuid.uuid4())[:8]}",
        name=room_in.name,
        tables=room_in.table_fqns,
        table_count=len(room_in.table_fqns)
    )
    _genie_rooms.append(room)
    return room


@api.get("/genie/rooms", response_model=List[GenieRoomOut], operation_id="listGenieRooms")
async def list_genie_rooms():
    """List all planned Genie rooms."""
    return _genie_rooms


@api.patch("/genie/rooms/{room_id}", response_model=GenieRoomOut, operation_id="updateGenieRoom")
async def update_genie_room(room_id: str, room_update: GenieRoomUpdateIn):
    """Update a Genie room definition."""
    global _genie_rooms
    for room in _genie_rooms:
        if room.id == room_id:
            if room_update.name is not None:
                room.name = room_update.name
            if room_update.table_fqns is not None:
                room.tables = room_update.table_fqns
                room.table_count = len(room_update.table_fqns)
            return room
    raise HTTPException(status_code=404, detail="Room not found")


@api.delete("/genie/rooms/{room_id}", operation_id="deleteGenieRoom")
async def delete_genie_room(room_id: str):
    """Delete a planned room."""
    global _genie_rooms
    _genie_rooms = [r for r in _genie_rooms if r.id != room_id]
    return {"message": "Room deleted"}


@api.delete("/genie/rooms", operation_id="clearAllGenieRooms")
async def clear_all_genie_rooms():
    """Delete all planned rooms."""
    global _genie_rooms
    _genie_rooms = []
    return {"message": "All rooms cleared"}


def _create_rooms_task():
    """
    Background task for creating Genie spaces.
    Uses WorkspaceClient with App SP authentication (automatic when running in Databricks App)
    or PROD profile with PAT (when running locally).
    """
    global _genie_creation_status
    
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # List existing spaces to check for updates vs creates
        existing_spaces = {}
        try:
            logger.info("[Genie Creation] Listing existing Genie spaces...")
            print(f"[Genie Creation] Listing existing Genie spaces...", flush=True)
            spaces_list = list(client.genie.list_spaces())
            for space in spaces_list:
                if hasattr(space, 'title') and space.title:
                    existing_spaces[space.title] = space
            logger.info(f"[Genie Creation] Found {len(existing_spaces)} existing spaces")
            print(f"[Genie Creation] Found {len(existing_spaces)} existing spaces", flush=True)
        except Exception as e:
            logger.warning(f"[Genie Creation] Could not list existing spaces: {e}")
            print(f"[Genie Creation] Warning: Could not list existing spaces: {e}", flush=True)
        
        for i, room in enumerate(_genie_creation_status.rooms):
            try:
                room['status'] = 'creating'
                logger.info(f"[Genie Creation] Starting room: {room['name']}")
                print(f"\n{'='*80}", flush=True)
                print(f"[Genie Creation] Processing room: {room['name']}", flush=True)
                print(f"[Genie Creation] Tables ({len(room['tables'])}): {room['tables'][:3]}{'...' if len(room['tables']) > 3 else ''}", flush=True)
                
                # Check if space already exists with this name
                if room['name'] in existing_spaces:
                    # Update existing space
                    existing_space = existing_spaces[room['name']]
                    print(f"[Genie Creation] Found existing space, updating: {existing_space.space_id}", flush=True)
                    
                    try:
                        # Prepare serialized_space for update
                        # Tables MUST be sorted by identifier.
                        sorted_tables = sorted(room['tables'])
                        ser_space_obj = {
                            "version": 1,
                            "data_sources": {
                                "tables": [{"identifier": fqn} for fqn in sorted_tables]
                            }
                        }
                        serialized_space = json.dumps(ser_space_obj, separators=(",", ":"))
                        
                        # Use SDK's update_space method with correct parameters
                        update_payload = {
                            "title": room['name'],
                            "description": f"Genie space with {room['table_count']} tables",
                            "warehouse_id": warehouse_id,
                            "serialized_space": serialized_space
                        }
                        
                        client.api_client.do(
                            "PUT",
                            f"/api/2.0/genie/spaces/{existing_space.space_id}",
                            body=update_payload
                        )
                        
                        space_id = existing_space.space_id
                        print(f"[Genie Creation] ✅ Updated space: {space_id}", flush=True)
                        
                    except Exception as update_error:
                        print(f"[Genie Creation] Update failed, will try to recreate: {update_error}", flush=True)
                        raise update_error
                        
                else:
                    # Create new space
                    print(f"[Genie Creation] Creating new Genie space...", flush=True)
                    
                    # Use the Python SDK's internal API client for direct REST calls
                    # This mimics what the MCP tool does successfully
                    try:
                        # Prepare the serialized_space placeholder
                        # Genie space APIs expect serialized_space as a JSON-escaped string.
                        # Tables MUST be sorted by identifier.
                        sorted_tables = sorted(room['tables'])
                        ser_space_obj = {
                            "version": 1,
                            "data_sources": {
                                "tables": [{"identifier": fqn} for fqn in sorted_tables]
                            }
                        }
                        serialized_space = json.dumps(ser_space_obj, separators=(",", ":"))
                        
                        payload = {
                            "warehouse_id": warehouse_id,
                            "title": room['name'],
                            "description": f"Genie space with {room['table_count']} tables",
                            "serialized_space": serialized_space
                        }
                        
                        print(f"[Genie Creation] Payload: {json.dumps(payload, indent=2)}", flush=True)
                        
                        # Use the SDK's internal API client to make the REST call
                        response = client.api_client.do(
                            "POST",
                            "/api/2.0/genie/spaces",
                            body=payload
                        )
                        
                        space_id = response.get('space_id') or response.get('id')
                        
                        if not space_id:
                            raise Exception(f"No space_id in response: {response}")
                        
                        print(f"[Genie Creation] ✅ Created space: {space_id}", flush=True)
                        
                        # No need for separate update if we include serialized_space in POST
                        print(f"[Genie Creation] ✅ Tables added to space via serialized_space", flush=True)
                        
                    except Exception as create_error:
                        print(f"[Genie Creation] Create failed: {create_error}", flush=True)
                        raise create_error
                
                # Extract workspace ID from host if possible (e.g., adb-7474651667509820.0.azuredatabricks.net)
                workspace_id = ""
                host = config.host or ""
                if "adb-" in host:
                    workspace_id = host.split("adb-")[1].split(".")[0]
                
                room['status'] = 'created'
                room['space_id'] = space_id
                
                # Format URL as /genie/rooms/{space_id}?o={workspace_id}
                if workspace_id:
                    room['url'] = f"{config.host}/genie/rooms/{space_id}?o={workspace_id}"
                else:
                    room['url'] = f"{config.host}/genie/rooms/{space_id}"
                
                logger.info(f"[Genie Creation] ✅ Successfully created room: {room['name']}")
                print(f"[Genie Creation] ✅ Room URL: {room['url']}", flush=True)
                print(f"{'='*80}\n", flush=True)
                
            except Exception as e:
                import traceback
                error_msg = str(e)
                traceback_str = traceback.format_exc()
                
                logger.error(f"[Genie Creation] ❌ Error creating room {room['name']}: {error_msg}")
                logger.error(f"[Genie Creation] Traceback:\n{traceback_str}")
                
                print(f"\n{'='*80}", flush=True)
                print(f"[Genie Creation] ❌ ERROR creating room: {room['name']}", flush=True)
                print(f"Error: {error_msg}", flush=True)
                print(f"Traceback:\n{traceback_str}", flush=True)
                print(f"{'='*80}\n", flush=True)
                
                room['status'] = 'failed'
                room['error'] = error_msg
        
        _genie_creation_status.status = 'completed'
        logger.info("[Genie Creation] All rooms processed")
        print("[Genie Creation] All rooms processed", flush=True)
        
    except Exception as e:
        import traceback
        logger.error(f"[Genie Creation] Fatal error in task: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"[Genie Creation] Fatal error: {str(e)}", flush=True)
        print(traceback.format_exc(), flush=True)
        _genie_creation_status.status = 'failed'


@api.post("/genie/create-all", response_model=GenieCreationStatusOut, operation_id="createAllGenieRooms")
async def create_all_genie_rooms():
    """Create all planned Genie rooms."""
    global _genie_creation_status
    
    if not _genie_rooms:
        raise HTTPException(status_code=400, detail="No rooms planned")
    
    _genie_creation_status = GenieCreationStatusOut(
        status='creating',
        rooms=[{
            'id': r.id,
            'name': r.name,
            'tables': r.tables,
            'table_count': r.table_count,
            'status': 'pending',
            'space_id': None,
            'url': None,
            'error': None
        } for r in _genie_rooms]
    )
    
    thread = threading.Thread(target=_create_rooms_task)
    thread.daemon = True
    thread.start()
    
    return _genie_creation_status


@api.get("/genie/create-status", response_model=GenieCreationStatusOut, operation_id="getGenieCreationStatus")
async def get_genie_creation_status():
    """Get creation status."""
    if not _genie_creation_status:
        raise HTTPException(status_code=404, detail="No creation in progress")
    return _genie_creation_status


@api.get("/genie/created", response_model=List[CreatedGenieRoomOut], operation_id="listCreatedGenieRooms")
async def list_created_genie_rooms():
    """List created Genie rooms."""
    if not _genie_creation_status or _genie_creation_status.status != 'completed':
        return []
    
    return [CreatedGenieRoomOut(
        id=r['id'],
        name=r['name'],
        space_id=r['space_id'],
        url=r['url'],
        table_count=r['table_count'],
        status=GenieRoomStatus.CREATED
    ) for r in _genie_creation_status.rooms if r['status'] == 'created']


def _execute_preview_query(table_fqn: str):
    """Helper function to execute preview queries using Spark SQL."""
    from pyspark.sql import SparkSession
    from datetime import date, datetime
    from decimal import Decimal
    
    # Get or create Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Get row count
    count_df = spark.sql(f"SELECT COUNT(*) as count FROM {table_fqn}")
    row_count = count_df.collect()[0][0]
    
    # Get sample rows
    sample_df = spark.sql(f"SELECT * FROM {table_fqn} LIMIT 10")
    
    # Get column names
    columns = sample_df.columns
    
    # Convert rows to dictionaries with JSON-serializable types
    rows = []
    for row in sample_df.collect():
        row_dict = {}
        for col in columns:
            val = row[col]
            
            # Convert to JSON-serializable types
            if val is None:
                row_dict[col] = None
            elif isinstance(val, (date, datetime)):
                row_dict[col] = val.isoformat()
            elif isinstance(val, Decimal):
                row_dict[col] = float(val)
            elif isinstance(val, (bytes, bytearray)):
                row_dict[col] = val.hex()
            elif isinstance(val, (list, dict)):
                row_dict[col] = str(val) if len(str(val)) < 1000 else str(val)[:1000] + "..."
            else:
                val_str = str(val)
                row_dict[col] = val_str if len(val_str) < 1000 else val_str[:1000] + "..."
        
        rows.append(row_dict)
    
    return row_count, columns, rows

@api.get("/enrichment/preview/{table_fqn:path}", response_model=TablePreviewOut, operation_id="previewEnrichmentTable")
async def preview_enrichment_table(table_fqn: str):
    """Preview a table from Unity Catalog."""
    
    try:
        # Parse table FQN
        parts = table_fqn.split(".")
        if len(parts) != 3:
            raise HTTPException(status_code=400, detail="Table FQN must be in format catalog.schema.table")
        
        catalog_name, schema_name, table_name = parts
        
        # Run blocking SDK call in thread pool to avoid blocking event loop
        row_count, columns, rows = await asyncio.to_thread(_execute_preview_query, table_fqn)
        
        return TablePreviewOut(
            table_fqn=table_fqn,
            row_count=row_count,
            columns=columns,
            sample_rows=rows
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview table: {str(e)}")

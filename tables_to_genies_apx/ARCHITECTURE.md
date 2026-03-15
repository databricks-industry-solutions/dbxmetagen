# Tables to Genies APX App - Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Client Browser (Port 3000)                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              React + TypeScript + Tailwind              │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │ TanStack Router (File-based Routing)              │ │  │
│  │  │                                                    │ │  │
│  │  │  / (index) → Redirect to /catalog-browser         │ │  │
│  │  │  /_sidebar (Layout)                               │ │  │
│  │  │  ├─ /catalog-browser (Page 1)                     │ │  │
│  │  │  ├─ /enrichment (Page 2)                          │ │  │
│  │  │  ├─ /graph-explorer (Page 3)                      │ │  │
│  │  │  ├─ /genie-builder (Page 4)                       │ │  │
│  │  │  └─ /genie-create (Page 5)                        │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  │                                                          │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │ TanStack Query (Data Fetching with Suspense)      │ │  │
│  │  │                                                    │ │  │
│  │  │  useListCatalogsSuspense()                        │ │  │
│  │  │  useListSchemasSuspense()                         │ │  │
│  │  │  useListTablesSuspense()                          │ │  │
│  │  │  useRunEnrichment()                               │ │  │
│  │  │  useBuildGraph()                                  │ │  │
│  │  │  useCreateGenieRoom()                             │ │  │
│  │  │  useCreateAllGenieRooms()                         │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  │                                                          │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │ UI Components (shadcn/ui)                         │ │  │
│  │  │                                                    │ │  │
│  │  │  - Button, Card, Skeleton                         │ │  │
│  │  │  - Tree view (Catalog Browser)                    │ │  │
│  │  │  - Progress bar (Enrichment)                      │ │  │
│  │  │  - Cytoscape.js (Graph Explorer)                 │ │  │
│  │  │  - Multi-select (Room Builder)                    │ │  │
│  │  │  - Status indicators (Room Creator)               │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓ Axios
                         HTTP Requests
                          /api/...
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              FastAPI Backend (Port 8000)                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ src/tables_genies/backend/main.py                       │  │
│  │                                                          │  │
│  │  • Serves React static files (dist/)                    │  │
│  │  • Mounts API routes                                    │  │
│  │  • CORS middleware (allow all origins)                  │  │
│  │  • Health check endpoint (/health)                      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ src/tables_genies/backend/router.py                     │  │
│  │                                                          │  │
│  │  13 Routes organized in 4 groups:                        │  │
│  │                                                          │  │
│  │  1. UC Catalog Browser (5 routes)                       │  │
│  │     GET    /api/uc/catalogs                             │  │
│  │     GET    /api/uc/catalogs/{catalog}/schemas           │  │
│  │     GET    /api/uc/catalogs/{catalog}/schemas/{}/tables │  │
│  │     POST   /api/uc/selection                            │  │
│  │     GET    /api/uc/selection                            │  │
│  │                                                          │  │
│  │  2. Enrichment Pipeline (3 routes)                      │  │
│  │     POST   /api/enrichment/run                          │  │
│  │     GET    /api/enrichment/status/{job_id}              │  │
│  │     GET    /api/enrichment/results                      │  │
│  │                                                          │  │
│  │  3. Graph Building (2 routes)                           │  │
│  │     POST   /api/graph/build                             │  │
│  │     GET    /api/graph/data                              │  │
│  │                                                          │  │
│  │  4. Genie Room Management (5 routes)                    │  │
│  │     POST   /api/genie/rooms                             │  │
│  │     GET    /api/genie/rooms                             │  │
│  │     DELETE /api/genie/rooms/{id}                        │  │
│  │     POST   /api/genie/create-all                        │  │
│  │     GET    /api/genie/create-status                     │  │
│  │     GET    /api/genie/created                           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ src/tables_genies/backend/models.py                     │  │
│  │                                                          │  │
│  │  17 Pydantic Models (Following 3-model pattern):         │  │
│  │                                                          │  │
│  │  • CatalogOut, SchemaOut, TableOut                       │  │
│  │  • TableSelectionIn, TableSelectionOut                   │  │
│  │  • EnrichmentRunIn, EnrichmentStatusOut                  │  │
│  │  • EnrichmentResultOut, EnrichmentResultListOut          │  │
│  │  • GraphDataOut, GraphNodeOut, GraphEdgeOut              │  │
│  │  • GenieRoomIn, GenieRoomOut, GenieRoomListOut           │  │
│  │  • GenieCreationStatusOut, CreatedGenieRoomOut           │  │
│  │                                                          │  │
│  │  All models have:                                         │  │
│  │  • Full type hints                                        │  │
│  │  • Pydantic validation                                    │  │
│  │  • OpenAPI schema generation                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ In-Memory State Management                              │  │
│  │                                                          │  │
│  │  • _selection: TableSelectionOut                         │  │
│  │  • _enrichment_jobs: Dict[str, JobInfo]                 │  │
│  │  • _enrichment_results: List[EnrichmentResultOut]        │  │
│  │  • _graph_data: Optional[GraphDataOut]                   │  │
│  │  • _genie_rooms: List[GenieRoomOut]                      │  │
│  │  • _genie_creation_status: Optional[GenieCreationStatusOut]│  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                    ↓ Databricks SDK
         ┌──────────────────────────────────┐
         ↓                                   ↓
   ┌──────────────────────┐      ┌──────────────────────┐
   │  Unity Catalog API   │      │  SQL Warehouse       │
   │                      │      │                      │
   │  • List catalogs     │      │  • Execute queries   │
   │  • List schemas      │      │  • Get results       │
   │  • List tables       │      │  • Table metadata    │
   │  • Get permissions   │      │  • Create tables     │
   └──────────────────────┘      └──────────────────────┘
         ↓                                   ↓
   ┌──────────────────────┐      ┌──────────────────────┐
   │  Genie Spaces API    │      │  LLM Endpoint        │
   │                      │      │  (for enrichment)    │
   │  • Create spaces     │      │                      │
   │  • List spaces       │      │  • Generate metadata │
   │  • Get space info    │      │  • Column analysis   │
   └──────────────────────┘      └──────────────────────┘
```

## Data Flow Examples

### Example 1: Browse Catalogs

```
User clicks "Expand Catalog"
    ↓
Frontend: useListSchemasSuspense(catalog)
    ↓
Axios: GET /api/uc/catalogs/{catalog}/schemas
    ↓
Backend: router.list_schemas(catalog)
    ↓
Databricks SDK: client.schemas.list(catalog_name=catalog)
    ↓
Return: List[SchemaOut]
    ↓
Frontend: Render nested schema list with checkboxes
```

### Example 2: Run Enrichment

```
User clicks "Run Enrichment"
    ↓
Frontend: useRunEnrichment().mutate(selection)
    ↓
Axios: POST /api/enrichment/run { table_fqns: [...] }
    ↓
Backend: run_enrichment(selection)
    ├─ Store job_id in _enrichment_jobs
    ├─ Return status { job_id, status: "pending" }
    └─ Execute enrichment in background
    ↓
Frontend: Poll useGetEnrichmentStatusSuspense(job_id)
    ├─ Auto-refetch every 2s while status="running"
    └─ Display progress bar
    ↓
Backend returns { status: "completed", progress: 100 }
    ↓
Frontend: Fetch useListEnrichmentResultsSuspense()
    ↓
Display results table
```

### Example 3: Create Genie Rooms

```
User clicks "Create All Rooms"
    ↓
Frontend: useCreateAllGenieRooms().mutate()
    ↓
Axios: POST /api/genie/create-all
    ↓
Backend: create_all_genie_rooms()
    ├─ For each room in _genie_rooms:
    │  └─ Call Databricks SDK genie_spaces.create()
    └─ Update _genie_creation_status with progress
    ↓
Frontend: Poll useGetGenieCreationStatusSuspense()
    ├─ Auto-refetch every 2s while status="creating"
    ├─ Display per-room status (pending→creating→created)
    └─ When complete, show Genie space URLs
```

## Technology Stack

### Frontend
- **React 18.3** - UI library
- **TypeScript 5.7** - Type safety
- **Vite 6.4** - Bundler & dev server
- **TanStack Router 1.81** - File-based routing
- **TanStack Query 5.62** - Data fetching + caching
- **Tailwind CSS 3.4** - Styling
- **Lucide React 0.468** - Icons
- **Cytoscape.js 3.30** - Graph visualization
- **Axios 1.7** - HTTP client

### Backend
- **FastAPI 0.115** - Web framework
- **Uvicorn 0.32** - ASGI server
- **Pydantic 2.0** - Data validation
- **Databricks SDK 0.20** - Workspace/UC/Genie APIs
- **Databricks SQL Connector 3.0** - SQL queries
- **NetworkX 3.0** - Graph algorithms
- **Python 3.10+**

## Development Environment

### Local Development
- **Python 3.10+**
- **Bun 1.2+** (Faster npm alternative)
- **Node.js 18+** (Required by Bun)
- **Databricks CLI** (for deployment)

### Deployment Target
- **Databricks Workspace** (fevm-serverless-dbx-unifiedchat.cloud.databricks.com)
- **SQL Warehouse** (a4ed2ccbda385db9)
- **Compute**: Serverless (APX)

## Build & Deployment

### Local Build
```bash
# Frontend
cd src/tables_genies/ui
bun run build        # Creates dist/ folder

# Backend (no build needed, uses source directly)
```

### Production Deployment
```bash
# Via Databricks CLI
databricks apps deploy tables-to-genies-apx

# Via Asset Bundles (DABs)
databricks bundle deploy -t prod
```

## Type Safety Flow

```
Backend Models (Pydantic)
    ↓
FastAPI routes with @app.get(..., response_model=X)
    ↓
OpenAPI spec generated automatically
    ↓
Axios types inferred from specs (or manually defined in lib/api.ts)
    ↓
React hooks with full TypeScript support
    ↓
Frontend components with complete IDE autocomplete
```

## State Management

### Backend State (In-Memory)
- Simple dictionaries for demo/dev
- Reset on server restart
- Not persisted to disk
- Thread-safe for single-user sessions

### Frontend State
- TanStack Query cache (memory)
- React component local state
- Query invalidation on mutations
- Auto-refetch for async operations

### Production State
- **Tables Selection**: Could persist to Delta table
- **Enrichment Results**: Already in Delta table
- **Genie Rooms**: Stored in Databricks workspace
- **Graph Data**: Could cache in UC volume

## Performance Optimizations

### Frontend
- Suspense boundaries with skeleton loaders
- Lazy loading (import() for code splitting)
- HMR for instant development feedback
- Query caching to avoid duplicate requests

### Backend
- In-memory caching for catalog structures
- Async enrichment jobs (non-blocking)
- Connection pooling (Databricks SDK)
- Single SQL warehouse for all queries

## Security Considerations

### Current Implementation
- No authentication (development mode)
- CORS allows all origins
- Databricks auth via environment variables

### Production Hardening
- Add OAuth 2.0 with Databricks identity
- Restrict CORS to known origins
- Role-based access control (RBAC)
- Audit logging
- Rate limiting

## Next Steps for Enhancement

1. **Error Handling**: Toast notifications for failures
2. **Form Validation**: Client-side validation
3. **State Persistence**: Save to Delta tables
4. **Authentication**: Add OAuth
5. **More Components**: Table, Badge, Dialog components
6. **Dark Mode**: Toggle in header
7. **Export**: Save results to CSV/Parquet
8. **Monitoring**: Integrate with MLflow

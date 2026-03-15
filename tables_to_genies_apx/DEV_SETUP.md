# APX App Local Development Guide

## Quick Start

Run the development servers with a single command:

```bash
cd /Users/yang.yang/CursorProjects/KUMC_POC_hlsfieldtemp/tables_to_genies_apx
./run-dev.sh
```

This will start:
- **FastAPI Backend**: http://localhost:8000
- **React Frontend**: http://localhost:5173
- **API Documentation**: http://localhost:8000/docs (Swagger UI)

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│ React Frontend (Port 5173)                              │
│ - TanStack Router for file-based routing                │
│ - TanStack Query for data fetching with Suspense        │
│ - 5 Pages: Catalog Browser, Enrichment, Graph, etc.     │
└─────────────────────────────────────────────────────────┘
                         ↓↑
                    API Calls (/api)
                         ↓↑
┌─────────────────────────────────────────────────────────┐
│ FastAPI Backend (Port 8000)                             │
│ - UC Catalog Browser routes                             │
│ - Enrichment & Graph Building routes                    │
│ - Genie Room Creation routes                            │
│ - Databricks SDK integration                            │
└─────────────────────────────────────────────────────────┘
                         ↓↑
                  Databricks APIs
                         ↓↑
┌─────────────────────────────────────────────────────────┐
│ Databricks Services                                      │
│ - Unity Catalog (list catalogs, schemas, tables)        │
│ - SQL Warehouse (enrichment queries)                    │
│ - Genie Spaces API (create rooms)                       │
└─────────────────────────────────────────────────────────┘
```

## Prerequisites

### 1. Databricks Configuration
You need a valid `.databrickscfg` profile to connect to Databricks:

```bash
# Option A: If you already have a profile (DEFAULT, PROD, etc.)
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token

# Option B: Use profile-based auth
export DATABRICKS_CONFIG_PROFILE=DEFAULT  # or PROD, DEVELOPMENT, etc.
```

### 2. Backend Dependencies
Already installed via `pip install -r requirements.txt`:
- FastAPI (web framework)
- Uvicorn (ASGI server)
- Databricks SDK (Unity Catalog, Genie API access)
- Pydantic (data validation)
- NetworkX (graph algorithms)

### 3. Frontend Dependencies
Already installed via `bun install`:
- React 18.3+
- Vite (bundler & dev server)
- TanStack Router (routing)
- TanStack Query (data fetching)
- Tailwind CSS (styling)
- Cytoscape.js (graph visualization)

## Running Locally

### Method 1: Using the Launch Script (Recommended)

```bash
cd tables_to_genies_apx
./run-dev.sh
```

This automatically:
- ✅ Sets up Python environment
- ✅ Starts FastAPI backend with auto-reload
- ✅ Starts Vite dev server with HMR (hot module replacement)
- ✅ Displays URLs and API documentation links
- ✅ Gracefully shuts down both servers on Ctrl+C

### Method 2: Manual (Terminal 1 + Terminal 2)

**Terminal 1 - Backend:**
```bash
cd tables_to_genies_apx
python3 -m uvicorn src.tables_genies.backend.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --reload
```

**Terminal 2 - Frontend:**
```bash
cd tables_to_genies_apx/src/tables_genies/ui
bun run dev
```

## Development Workflow

### 1. **Backend Development**
- Modify files in `src/tables_genies/backend/`
- The `--reload` flag automatically restarts on changes
- Check http://localhost:8000/docs for the live OpenAPI spec

### 2. **Frontend Development**
- Modify files in `src/tables_genies/ui/`
- Vite provides HMR (Hot Module Replacement)
- Changes appear instantly in the browser
- Check browser console for any errors

### 3. **Type Safety**

**Backend types:**
```bash
cd tables_to_genies_apx
python3 -m basedpyright src/tables_genies/backend/
```

**Frontend types:**
```bash
cd tables_to_genies_apx/src/tables_genies/ui
bun run build:check  # TypeScript + Vite build
```

## API Endpoints (Available Locally)

### UC Catalog Browser
- `GET /api/uc/catalogs` - List all catalogs
- `GET /api/uc/catalogs/{catalog}/schemas` - List schemas
- `GET /api/uc/catalogs/{catalog}/schemas/{schema}/tables` - List tables
- `POST /api/uc/selection` - Save table selection
- `GET /api/uc/selection` - Get current selection

### Enrichment
- `POST /api/enrichment/run` - Start enrichment job
- `GET /api/enrichment/status/{job_id}` - Poll job status
- `GET /api/enrichment/results` - Get enrichment results

### Graph Building
- `POST /api/graph/build` - Build relationship graph
- `GET /api/graph/data` - Get Cytoscape.js compatible data

### Genie Room Management
- `POST /api/genie/rooms` - Create planned room
- `GET /api/genie/rooms` - List planned rooms
- `DELETE /api/genie/rooms/{id}` - Delete room
- `POST /api/genie/create-all` - Create all rooms
- `GET /api/genie/create-status` - Poll creation status
- `GET /api/genie/created` - List created rooms

## Testing the App

### 1. **Test Backend API**
```bash
# Check health
curl http://localhost:8000/health

# List catalogs
curl http://localhost:8000/api/uc/catalogs | jq .

# View interactive API docs
open http://localhost:8000/docs
```

### 2. **Test Frontend**
```bash
# Open the app
open http://localhost:5173

# Navigate through pages:
# 1. Browse Catalogs - Select tables
# 2. Enrich Tables - Run enrichment job
# 3. Explore Graph - View relationship graph
# 4. Build Rooms - Define Genie room groupings
# 5. Create Rooms - Batch create Genie spaces
```

### 3. **Check Console Logs**
- Frontend errors: Browser DevTools (F12)
- Backend logs: Terminal running `uvicorn` command

## Troubleshooting

### Backend won't start
**Error**: `ModuleNotFoundError: No module named 'databricks'`
```bash
# Reinstall dependencies
pip install -r requirements.txt
```

**Error**: `Connection error to Databricks`
```bash
# Check auth
echo $DATABRICKS_HOST
echo $DATABRICKS_CONFIG_PROFILE

# Verify profile
databricks workspace get-status / --profile PROD
```

### Frontend won't load
**Error**: Blank page with console errors
```bash
# Check if backend is running
curl http://localhost:8000/health

# Check API calls in DevTools Network tab
# (should see XHR requests to /api/*)

# Rebuild frontend
cd src/tables_genies/ui
bun run build
```

**Error**: `Cannot find module '@tanstack/react-router'`
```bash
# Reinstall frontend dependencies
cd src/tables_genies/ui
bun install
```

### Port already in use
```bash
# Backend (8000)
lsof -i :8000
kill -9 <PID>

# Frontend (5173)
lsof -i :5173
kill -9 <PID>
```

## File Structure

```
tables_to_genies_apx/
├── run-dev.sh                         # Launch script
├── requirements.txt                    # Python dependencies
├── app.yaml                           # Databricks App config
├── README.md                          # Overview
│
├── src/tables_genies/
│   ├── backend/
│   │   ├── main.py                   # FastAPI app + frontend serving
│   │   ├── router.py                 # 13 API routes
│   │   ├── models.py                 # 17 Pydantic models
│   │   └── __init__.py
│   │
│   └── ui/
│       ├── main.tsx                  # React entry point
│       ├── index.html                # HTML template
│       ├── index.css                 # Tailwind + theme
│       ├── vite.config.ts            # Bundler config
│       ├── tsconfig.json             # TypeScript config
│       ├── package.json              # Frontend dependencies
│       │
│       ├── dist/                     # Built frontend (from `bun run build`)
│       ├── routes/
│       │   ├── __root.tsx            # Root layout
│       │   ├── index.tsx             # Home (redirect)
│       │   └── _sidebar/
│       │       ├── route.tsx         # Sidebar layout + nav
│       │       ├── catalog-browser.tsx
│       │       ├── enrichment.tsx
│       │       ├── graph-explorer.tsx
│       │       ├── genie-builder.tsx
│       │       └── genie-create.tsx
│       │
│       ├── lib/
│       │   ├── api.ts                # React Query hooks + types
│       │   ├── axios-instance.ts     # HTTP client config
│       │   ├── selector.ts           # Query selector helper
│       │   └── utils.ts              # Helper functions
│       │
│       ├── components/
│       │   └── ui/
│       │       ├── button.tsx        # shadcn Button
│       │       ├── card.tsx          # shadcn Card
│       │       └── skeleton.tsx      # Loading skeleton
│       │
│       └── node_modules/             # Frontend packages
```

## Next Steps

### After Local Testing Works:

1. **Deploy to Databricks** (use DABs)
   ```bash
   databricks bundle validate
   databricks bundle deploy -t prod
   ```

2. **Monitor Production App**
   ```bash
   DATABRICKS_CONFIG_PROFILE=PROD databricks apps logs tables-to-genies-apx
   ```

3. **Enhance the App**
   - Add more shadcn/ui components (Table, Badge, Progress)
   - Improve error handling (Toast notifications)
   - Add form validation
   - Persist state to Delta tables

## Support

- **Databricks SDK Docs**: https://docs.databricks.com/en/dev-tools/sdk-python
- **FastAPI Docs**: https://fastapi.tiangolo.com/
- **React Router Docs**: https://tanstack.com/router/latest
- **React Query Docs**: https://tanstack.com/query/latest
- **Vite Docs**: https://vitejs.dev/

---

**Happy Coding! 🚀**

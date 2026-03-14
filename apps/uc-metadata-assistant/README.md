# Unity Catalog Metadata Assistant

A production-ready Databricks app for AI-powered metadata generation, PII detection, and governance analytics. Fully self-contained with zero external dependencies.

## Required Permissions

### Unity Catalog Permissions

```sql
-- READ permissions for metadata discovery
GRANT USE CATALOG ON CATALOG <target_catalog> TO `<service-principal>`;
GRANT USE SCHEMA ON SCHEMA <target_catalog>.* TO `<service-principal>`;
GRANT SELECT ON SCHEMA <target_catalog>.* TO `<service-principal>`;
GRANT SELECT ON TABLE <target_catalog>.*.* TO `<service-principal>`;

-- WRITE permissions for metadata updates (COMMENT statements require MANAGE)
GRANT MANAGE ON CATALOG <target_catalog> TO `<service-principal>`;

-- APP INFRASTRUCTURE permissions
GRANT CREATE CATALOG ON METASTORE TO `<service-principal>`;
GRANT CREATE SCHEMA ON CATALOG uc_metadata_assistant TO `<service-principal>`;
```

### LLM Model Serving Access

```sql
-- Grant access to LLM serving endpoints
GRANT EXECUTE ON MODEL <model_endpoint> TO `<service-principal>`;
```

### SQL Warehouse Access

```sql
-- Grant access to SQL warehouse for queries
GRANT USE ON SQL WAREHOUSE <warehouse_id> TO `<service-principal>`;
```

## Prerequisites

- Databricks Workspace with Unity Catalog enabled
- Service Principal with permissions listed above
- SQL Warehouse (running and accessible)
- At least one LLM Serving Endpoint (Databricks Foundation Models)

## Quick Start

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd uc-metadata-assistant
```

### 2. Configure app.yaml

Update the SQL warehouse path:

env:
  - name: DATABRICKS_HTTP_PATH
    value: /sql/1.0/warehouses/<your-warehouse-id>
  - name: DATABRICKS_SERVER_HOSTNAME
    value: <your-workspace>.cloud.databricks.net
```

Note: `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_HOST`, and `DATABRICKS_WORKSPACE_ID` are automatically provided by Databricks Apps.

### 3. Deploy

```bash
databricks apps deploy governance-app .
```

### 4. Access App

Navigate to the provided app URL and start generating metadata.

## Features

### AI-Powered Metadata Generation
- Multi-model support (GPT, Llama, Gemma, Claude, custom endpoints)
- Configurable generation styles (concise, technical, business)
- Intelligent context analysis using schema relationships and sample data
- Parallel batch processing for performance

### Enterprise PII Detection
- 50+ built-in detection patterns across 8 compliance frameworks
- Pattern-based detection (column names, data types)
- LLM-enhanced detection for context analysis and data sampling
- Configurable tag mappings for governed tag integration
- Redaction support for sensitive data in prompts

### Quality & Governance
- Real-time completeness, accuracy, and tag coverage metrics
- Historical trend analysis and forecasting
- Audit trails for all metadata operations
- Governed tag policy enforcement

### Advanced Settings
- Model management (enable/disable, add custom endpoints)
- PII pattern configuration (50+ patterns, add custom)
- Tag policy management (governed tags, manual tags)
- Data sampling controls
- Prompt customization (length, focus, terminology)

## Architecture

```
governance_app/
├── app_react.py              # React version - Main Flask backend (RECOMMENDED)
├── app.py                    # Legacy version - Original Flask app
├── app.yaml                  # Legacy version - App configuration
├── enhanced_generator.py     # AI metadata generation engine (shared)
├── pii_detector.py           # PII/PHI detection engine (shared)
├── setup_utils.py            # Auto infrastructure setup (shared)
├── settings_manager.py       # Settings persistence (shared)
├── models_config.py          # LLM model management (shared)
├── requirements.txt          # Python dependencies (shared)
└── client/build/             # Pre-built React frontend (React version only)
    ├── index.html
    └── assets/
```

## Automatic Infrastructure

The app automatically creates:

```
uc_metadata_assistant.generated_metadata.*
uc_metadata_assistant.quality_metrics.*
uc_metadata_assistant.cache.*
```

Tables:
- `metadata_results` - Generated descriptions
- `generation_audit` - Audit trail
- `completeness_snapshots` - Quality metrics history
- `pii_column_assessments` - PII analysis results
- `workspace_settings` - App configuration

## Technology Stack

- **Backend:** Flask, Python 3.9+
- **Frontend:** React, Vite, TailwindCSS, Recharts, Framer Motion
- **Database:** Unity Catalog (Delta Lake)
- **AI/ML:** Databricks Foundation Model Serving
- **Infrastructure:** Databricks Apps Platform

## API Endpoints

### Metadata
- `GET /api/catalogs` - List available catalogs
- `GET /api/fast-counts/<catalog>` - Metadata counts
- `GET /api/missing-metadata/<catalog>/<type>` - Missing metadata
- `GET /api/coverage/<catalog>` - Coverage trends

### Generation
- `POST /api/enhanced/run` - Start generation
- `GET /api/enhanced/status/<run_id>` - Generation status
- `POST /api/enhanced/cancel/<run_id>` - Cancel generation
- `GET /api/enhanced/results` - Retrieve results

### Quality
- `GET /api/quality-metrics/<catalog>` - Quality dashboard data
- `GET /api/metadata-history/<catalog>` - Audit history

### Settings
- `GET/POST /api/settings/models` - Model configuration
- `GET/POST /api/settings/sensitive-data` - PII configuration
- `GET/POST /api/settings/tags` - Tag policy
- `GET/POST /api/settings/sampling` - Sampling configuration
- `GET/POST /api/settings/prompts` - Prompt customization

### Unity Catalog
- `POST /api/submit-metadata` - Commit to Unity Catalog
- `GET /api/governed-tags` - Fetch governed tags

## Performance

Generation time varies by:
- LLM model (Gemma fastest, Claude slowest)
- Batch size (configurable 1-50 objects)
- PII detection settings (pattern-only vs. LLM-enhanced)
- Endpoint availability

## Troubleshooting

### Catalog Creation Fails
If you see "Metastore storage root URL does not exist":
1. Go to Catalog in Databricks UI
2. Create catalog manually: `uc_metadata_assistant`
3. Use default storage
4. Redeploy app

### Permissions Errors
- Verify service principal has all required permissions
- Check SQL warehouse is running and accessible
- Confirm LLM endpoints are available
- Ensure MANAGE permission on target catalogs for COMMENT statements

### Slow Performance
- Use smaller batch sizes for faster individual runs
- Enable client-side caching
- Check SQL warehouse size and scaling
- Monitor LLM endpoint queue times

### PII Detection Issues
- Clear tag mappings cache via Settings
- Verify governed tags are accessible
- Check PII detection is enabled in Settings
- Review app logs for validation errors

## Development

### Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABRICKS_HOST=<workspace-url>
export DATABRICKS_TOKEN=<token>
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>

# Run locally
python app_react.py
```

### Frontend Development

```bash
cd client
npm install
npm run dev  # Development server
npm run build  # Production build
```

## Security

- All data processing happens within Databricks
- No external API calls or data exports
- Service principal authentication with OAuth2
- Unity Catalog permission validation
- Encrypted communication via HTTPS/TLS
- Complete audit trails for compliance

## License

MIT License - see LICENSE file for details.

## Support

- GitHub Issues: Bug reports and feature requests
- Databricks Community: General questions
- Enterprise Support: Priority support for production deployments

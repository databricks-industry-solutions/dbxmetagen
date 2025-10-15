# 🏢 Unity Catalog Metadata Assistant

**A comprehensive Databricks app for automated metadata generation, governance, and quality management using AI-powered analysis and enterprise-grade PII detection.**

[![Databricks](https://img.shields.io/badge/Databricks-Apps-orange)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Compatible-blue)](https://docs.databricks.com/data-governance/unity-catalog/)
[![Self-Contained](https://img.shields.io/badge/Setup-Zero%20Config-green)](#-zero-setup-required)

---

## 🎯 Overview

The UC Metadata Assistant is a production-ready Databricks app that automatically discovers, generates, and manages metadata for Unity Catalog objects. It combines AI-powered description generation with enterprise-grade PII detection, data quality monitoring, and governance workflows - all in a single, self-contained application.

### ✨ Key Features

🤖 **AI-Powered Generation** - Uses Databricks LLM serving endpoints (GPT, Llama, Gemma, Claude)  
🛡️ **Enterprise PII Detection** - Built-in pattern matching with 50+ detection rules + AI enhancement  
📊 **Quality Dashboard** - Comprehensive metrics, trends, and compliance monitoring  
🏷️ **Governed Tag Integration** - Unity Catalog tag policies with enforcement controls  
⚙️ **Advanced Settings** - Workspace-wide configuration for models, PII detection, and tag policies  
⚡ **High Performance** - SQL-optimized queries with client-side caching  
🔒 **Zero External Dependencies** - Fully self-contained with automatic setup  
👥 **Multi-User Ready** - Role-based access with audit trails and concurrency safety  
📈 **Real-Time Analytics** - Coverage trends, gap analysis, and quality scoring  

---

## 🚀 Quick Start

### Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **Service Principal** with appropriate permissions (see [Permissions](#-permissions))
- **SQL Warehouse** running and accessible
- **LLM Serving Endpoints** (one or more of: GPT, Llama, Gemma, Claude)

### 🎯 Zero Setup Required!

Unlike traditional metadata tools, this app requires **no external configuration**:

✅ **No repos to import** - Everything is embedded  
✅ **No jobs to create** - Runs directly in the app  
✅ **No tables to setup** - Auto-creates all infrastructure  
✅ **No permissions to configure** - Validates automatically  

### Deployment

1. **Clone this repository**:
   ```bash
   git clone <your-repo-url>
   cd uc-metadata-assistant
   ```

2. **Configure your environment** in `app.yaml`:
   ```yaml
   command: [
     "flask",
     "--app", 
     "app.py",
     "run"
   ]
   env:
     - name: 'DATABRICKS_HTTP_PATH'
       value: '/sql/1.0/warehouses/your-warehouse-id'
     - name: 'DATABRICKS_SERVER_HOSTNAME'
       value: 'your-workspace.cloud.databricks.net'
   ```

   **Note**: Databricks Apps automatically provides these environment variables:
   - `DATABRICKS_CLIENT_ID` - Service principal ID (auto-injected)
   - `DATABRICKS_CLIENT_SECRET` - Service principal secret (auto-injected)
   - `DATABRICKS_HOST` - Workspace hostname (auto-injected)
   - `DATABRICKS_WORKSPACE_ID` - Workspace ID (auto-injected)

   You only need to specify the **SQL Warehouse path** and **hostname** in your `app.yaml`.

3. **Create Required Catalog** (First-time setup):
   
   ⚠️ **Important**: Some Databricks workspaces require manual catalog creation due to storage configuration.
   
   If the app fails with catalog creation errors, create the catalog manually:
   
   1. Go to **Catalog** in the Databricks UI sidebar
   2. Click **"Create Catalog"**
   3. **Name**: `uc_metadata_assistant`
   4. **Storage**: Select **"Use default storage"** or **"Managed location"**
   5. **Comment**: `Auto-created catalog for UC Metadata Assistant`
   6. Click **"Create"**
   
   The app will automatically detect the existing catalog and create all necessary schemas and tables.

4. **Deploy to Databricks**:
   ```bash
   databricks apps deploy
   ```

5. **Access your app** at the provided URL and start generating metadata!

---

## 🎭 User Experience

### Dashboard Overview

![Overview Dashboard](https://github.com/user-attachments/assets/overview-dashboard.png)

The **Overview tab** provides a comprehensive governance dashboard with real-time insights:

**📊 Metrics Cards (Top Row)**:
- **Schemas missing description**: 5 objects with visual trend indicator
- **Tables missing description**: 30 objects with improvement note ("improved after last commit")
- **Columns missing comment**: 268 objects showing the biggest gap (45 columns in training_dataset)
- **Missing tags**: 17 objects across 8 schemas and 9 tables needing governance tags

**📈 Coverage by Month Chart**:
- **Historical data** (Jun-Oct) showing past coverage trends in salmon/red
- **Current month** (Oct) at ~50% coverage
- **Future projections** (Nov-Jan) in cyan showing expected improvements
- **Interactive visualization** with hover details and export capabilities

**🎯 Top Gaps Table**:
- **Priority objects** ranked by metadata completeness needs
- **Current status** showing existing descriptions (or "—" for missing)
- **Proposed descriptions** from AI generation with confidence scores
- **Action buttons** for quick review and status tracking

**🔍 Smart Filtering (Left Panel)**:
- **Object type filters**: Schemas, Tables, Columns with active selection
- **Data objects dropdown**: "All schemas" with drill-down capability  
- **Owner filtering**: "All owners" for team-based governance
- **Save/Unsave filters** for personalized workflows

**👤 User Context (Top Right)**:
- **Environment indicator**: "Serverless" deployment status
- **User identification**: Shows current user (mobeen.vaid) for audit trails
- **Connection status**: Live indicators for system health

### Generate Tab

![Generate Tab](https://github.com/user-attachments/assets/generate-tab.png)

The **Generate tab** provides AI-powered metadata generation with enterprise-grade controls:

**🤖 AI Metadata Generation Panel (Right Side)**:
- **Generation Model**: Dropdown showing "GPT-OSS 120B (General Purpose)" with support for multiple LLM models
- **Generation Style**: Three options with active selection and descriptions
  - **Concise** (selected): "Clear and concise descriptions that get straight to the point. Best for quick documentation"
  - **Technical**: Detailed technical specifications for developers
  - **Business**: Business-focused descriptions for stakeholders

**📋 Select Objects for Generation**:
- **SCHEMAS Section**: 
  - "Select All" / "Deselect All" bulk controls
  - Individual schema selection (claims_sample_data, cloning, hls, ingest, etc.)
  - Full namespace display for clarity
- **TABLES Section**:
  - Hierarchical organization under schemas
  - Bulk selection controls
  - Complete table paths (e.g., mobeenvaid_catalog.care_cost.carecost_compass_agent_payload)
- **Responsive scrolling** for large catalogs with hundreds of objects

**🔍 Smart Filtering Integration (Left Panel)**:
- **Same filtering system** as Overview tab for consistency
- **Object type focus**: "Schemas" selected to show schema-level generation
- **Data objects**: "All schemas" with drill-down capability
- **Owner filtering**: Maintains context across tabs
- **Filter persistence**: Saved filters apply to generation scope

**⚡ Performance Features**:
- **Client-side caching** for instant tab switching
- **Progressive loading** of large object lists
- **Bulk selection** for efficient workflow
- **Real-time filtering** without page reloads

### Review & Commit

![Review & Commit Tab](https://github.com/user-attachments/assets/review-commit-tab.png)

The **Review & Commit tab** provides a streamlined workflow for reviewing and applying AI-generated metadata:

**📋 Review Generated Metadata Panel (Center)**:
- **Clean interface** with clipboard icon indicating review functionality
- **Instructional guidance**: "Generate metadata first to see items for review" - clear user direction
- **Empty state design** that guides users through the proper workflow sequence
- **Spacious layout** optimized for reviewing multiple metadata items when populated

**✅ Bulk Submission Controls**:
- **"Submit All to Unity Catalog"** - Prominent green action button for bulk operations
- **Checkmark icon** indicating successful submission workflow
- **Full-width design** for easy access and clear call-to-action
- **Unity Catalog integration** with proper DDL generation and COMMENT statements

**🔍 Consistent Filtering (Left Panel)**:
- **Same filter system** maintained across all tabs for workflow continuity
- **Object type selection**: "Schemas" active, matching the generation scope
- **Data objects**: "All schemas" dropdown for hierarchical filtering
- **Owner filtering**: "All owners" for team-based review workflows
- **Save Filter** functionality for personalized review processes

**🎯 Workflow Integration**:
- **Sequential design**: Naturally follows Generate → Review → Commit pattern
- **Context preservation**: Maintains filter settings from previous tabs
- **User guidance**: Clear messaging about prerequisite steps
- **Bulk operations**: Designed for efficient mass metadata updates

**📊 Metrics Consistency (Top)**:
- **Same metrics cards** as other tabs showing real-time governance status
- **Progress tracking**: Users can see impact of their submissions on overall completeness
- **Catalog context**: "mobeenvaid_catalog" selection maintained across workflow

### Quality Dashboard

![Quality Dashboard](https://github.com/user-attachments/assets/quality-dashboard.png)

The **Quality tab** provides comprehensive metadata quality assessment and governance analytics:

**🏆 Metadata Quality Assessment Header**:
- **Trophy icon** emphasizing excellence and quality focus
- **"Metadata Quality Assessment"** title with professional styling
- **"QUALITY METRICS"** subtitle indicating comprehensive measurement approach
- **Clean, executive-level presentation** suitable for governance reporting

**📊 Quality Metrics (Three Key KPIs)**:

**📈 Completeness (Left Panel)**:
- **45% completion rate** with cyan donut chart visualization
- **Clear definition**: "Percentage of schemas, tables, and columns with descriptions"
- **Visual progress indicator** showing current state vs. target
- **Actionable metric** directly tied to metadata generation efforts

**🎯 Accuracy (Center Panel)**:
- **100% accuracy score** with orange/salmon donut chart
- **Quality assessment**: "Quality score based on schema drift and constraint validation"
- **Perfect score indication** showing high-quality metadata standards
- **Technical validation** through automated schema analysis

**🏷️ Tag Coverage (Right Panel)**:
- **0% tag coverage** with muted gray donut chart
- **Governance focus**: "Percentage of objects with governance and policy tags applied"
- **Opportunity indicator** highlighting area for improvement
- **Policy compliance** measurement for regulatory requirements

**🎨 Visual Design Excellence**:
- **Consistent donut charts** with clear percentage displays
- **Color coding**: Cyan (completeness), Orange (accuracy), Gray (tags needing attention)
- **Professional typography** with clear metric values (45%, 100%, 0%)
- **Descriptive text** explaining each metric's purpose and calculation
- **Balanced layout** with equal emphasis on all three quality dimensions

**📊 Enterprise Analytics Features**:
- **Real-time calculations** based on current catalog state
- **Actionable insights** showing where to focus governance efforts
- **Executive dashboard** suitable for leadership reporting
- **Comprehensive coverage** of metadata quality dimensions

### Settings & Configuration

![Settings Dashboard](https://github.com/user-attachments/assets/settings-dashboard.png)

The **Settings page** provides comprehensive workspace-wide configuration management with enterprise-grade controls:

**⚙️ Settings Navigation**:
- **User Avatar Dropdown** - Accessible from top-right user avatar with clean dropdown menu
- **Full-Screen Interface** - Settings take over entire screen for focused configuration
- **Tabbed Organization** - Three main configuration areas with consistent navigation
- **Back to Dashboard** - Seamless return to previous tab with state preservation

**🤖 Models Configuration Tab**:
- **Built-in Model Management**: Enable/disable Databricks native models (GPT, Claude, Llama, Gemma)
- **Custom Model Integration**: Add and validate custom Databricks Model Serving endpoints
- **Connectivity Testing**: Real-time validation of model endpoints before activation
- **Smart Defaults**: Automatic max_tokens configuration with user override capability
- **Status Indicators**: Clear "Enabled"/"Disabled" badges for all model configurations
- **Batch Operations**: Save/Reset functionality to prevent concurrency conflicts

**🔍 PII Analysis Configuration Tab**:
- **Comprehensive Pattern Library**: 18+ built-in PII detection patterns across 8 categories
  - **Personal Identifiers**: SSN, Phone, Email, Names, Addresses
  - **Financial Data**: Credit Cards, Bank Accounts, Routing Numbers
  - **Medical/PHI**: Patient IDs, Medical Record Numbers, Insurance Numbers
  - **Government IDs**: Passports, Driver Licenses, Tax IDs
  - **Employment Data**: Employee IDs, Payroll Numbers
  - **Education Records**: Student IDs, Academic Numbers
  - **Biometric Data**: Fingerprints, Facial Recognition IDs
  - **Custom Patterns**: User-defined business-specific patterns

- **Business-Friendly Interface**: 
  - **Keyword-Based Detection**: Simple column name keywords instead of complex regex
  - **Risk Level Classification**: HIGH/MEDIUM/LOW with clear descriptions
  - **Category Organization**: Grouped patterns with responsive counts (e.g., "Personal (5)")
  - **Pattern Management**: Add/remove custom patterns with loading states and animations

- **AI-Enhanced Detection**:
  - **LLM Assessment Toggle**: Enable AI-powered metadata quality analysis
  - **Intelligent PII Detection**: AI enhancement for pattern-based detection
  - **Model Integration**: Uses same LLM models as metadata generation
  - **Configurable Analysis**: Toggle AI features independently

- **Dynamic UI Features**:
  - **Real-Time Updates**: Pattern counts update on add/remove operations
  - **Loading States**: Visual feedback for all operations (15s processing times)
  - **Status Indicators**: "Enabled"/"Disabled" badges for all toggles
  - **Batch Save System**: Prevent concurrency conflicts with grouped saves

**🏷️ Tags Policy Configuration Tab**:
- **Governance Controls**:
  - **Enable Tags Functionality**: Master toggle for all tagging features
  - **Governed Tags Only**: Restrict to Unity Catalog governed tags exclusively
  - **Policy Enforcement**: Real-time integration with Review & Commit workflow

- **Databricks Integration**:
  - **Governed Tags API**: Direct integration with Unity Catalog Tag Policies
  - **Permission Handling**: Graceful fallback when tag policies aren't accessible
  - **Policy Validation**: Real-time enforcement in metadata submission workflow

- **Workflow Integration**:
  - **Review & Commit Enforcement**: Policies applied during metadata submission
  - **Dynamic UI Updates**: Tag options filtered based on policy settings
  - **User Guidance**: Clear messaging about policy restrictions

**🎯 Universal Settings Features**:
- **Fixed Footer Design**: Consistent Save/Reset buttons across all tabs
- **Batch Save System**: Eliminate Delta Lake concurrency conflicts
- **Change Tracking**: Visual indicators for unsaved changes (orange "Save Changes *" button)
- **Error Handling**: Retry logic with exponential backoff for concurrent operations
- **State Management**: Proper navigation state preservation
- **Professional UX**: Enterprise-grade interface with consistent toggle alignment

**📊 Settings Architecture**:
- **Workspace-Wide Persistence**: Settings stored in Delta Lake tables
- **Lazy Initialization**: Non-blocking startup with fast API fallbacks
- **Concurrency Safety**: Retry mechanisms for multi-user environments
- **Performance Optimized**: Minimal logging and efficient database operations

### History & Audit

![History Tab](https://github.com/user-attachments/assets/history-tab.png)

The **History tab** provides comprehensive audit trails and governance tracking for all metadata operations:

**📊 Update History Panel (Center)**:
- **Chronological audit trail** with precise timestamps (10/4/2025 01:46 AM, 01:45 AM, etc.)
- **Object tracking**: Full namespace paths (mobeenvaid_catalog.care_cost.procedure_cost)
- **Operation types**: Clear "Table" type indicators for different metadata operations
- **Action status**: "Generated" actions with blue highlighting for easy identification

**📅 Time Range Filtering (Top Right)**:
- **Flexible date ranges**: "7 days" (active), "30 days", "90 days", "All time"
- **Quick access buttons** for common audit periods
- **Active selection highlighting** with white background for current filter
- **Comprehensive coverage** from recent changes to historical analysis

**🔍 Detailed Audit Columns**:
- **Date**: Precise timestamps for compliance and tracking
- **Object**: Full Unity Catalog paths for complete traceability
- **Type**: Object classification (Table, Schema, Column)
- **Action**: Operation performed (Generated, Committed, etc.)
- **Changes**: Description of modifications ("Generated table description")
- **Source**: AI model attribution (databricks-gpt-oss-120b, databricks-gemma-3-12b)

**🎯 Governance Features**:
- **Multi-model tracking**: Shows different AI models used (GPT-OSS, Gemma-3)
- **Operation differentiation**: Generated vs. committed actions for complete workflow visibility
- **Compliance ready**: Detailed audit trails for regulatory requirements
- **Performance monitoring**: Track generation frequency and model usage patterns

**🔍 Consistent Interface (Left Panel)**:
- **Same filtering system** as other tabs for workflow continuity
- **Object type focus**: "Schemas" selection with cross-tab consistency
- **Data objects**: "All schemas" dropdown for hierarchical audit filtering
- **Owner filtering**: Team-based audit trail analysis
- **Filter persistence**: Maintains context across governance workflows

---

## 🏗️ Architecture

### Core Components

```
📁 UC Metadata Assistant
├── 🐍 app.py                    # Main Flask application (12,400+ lines)
├── 🔍 pii_detector.py           # Enterprise PII/PHI detection engine
├── 🤖 enhanced_generator.py     # Advanced AI generation with sampling
├── ⚙️ setup_utils.py            # Automatic infrastructure management
├── 🛠️ settings_manager.py       # Workspace-wide settings persistence
├── 🤖 models_config.py          # LLM model configuration and validation
├── 📋 requirements.txt          # Minimal dependencies
├── 🚀 app.yaml                  # Databricks app configuration
└── 📖 README.md                 # This comprehensive guide
```

### Technology Stack

- **Backend**: Flask, Python 3.9+
- **Database**: Unity Catalog (Delta tables)
- **AI/ML**: Databricks LLM Serving (GPT/Llama/Gemma/Claude)
- **Frontend**: Vanilla JavaScript, Chart.js, Modern CSS
- **Infrastructure**: Databricks Apps Platform

### Automatic Infrastructure

The app automatically creates and manages:

```sql
-- Auto-created catalog and schema
uc_metadata_assistant.generated_metadata.*
uc_metadata_assistant.quality_metrics.*
uc_metadata_assistant.cache.*

-- Core tables
metadata_results      -- Generated descriptions and metadata
generation_audit      -- Audit trail of all operations
completeness_snapshots -- Historical quality metrics
pii_risk_cache       -- PII assessment caching
pii_column_assessments -- Detailed PII analysis
workspace_settings   -- Settings configuration (models, PII, tags policy)
```

---

## 🔬 Advanced Features

### Enterprise PII Detection & Settings

**Configurable Detection Patterns** (via Settings):
- **18+ Built-in Categories**: Personal, Financial, Medical/PHI, Government, Employment, Education, Biometric, Custom
- **Business-Friendly Interface**: Keyword-based pattern creation instead of complex regex
- **Custom Pattern Management**: Add/remove organization-specific detection rules
- **Risk Level Classification**: HIGH/MEDIUM/LOW with clear business descriptions
- **Real-Time Pattern Counts**: Dynamic category headers (e.g., "Personal (5)")

**AI-Enhanced Analysis** (Configurable):
- **LLM Assessment**: AI-powered metadata quality analysis using configured models
- **Intelligent PII Detection**: AI enhancement for pattern-based detection to catch edge cases
- **Model Integration**: Uses same LLM endpoints as metadata generation
- **Toggle Controls**: Independent enable/disable for different AI features

**Multi-Layer Detection Engine**:
- **Column names** → Keyword matching with 50+ categories + custom patterns
- **Sample data** → Regex pattern detection with AI validation
- **Data types** → Type-specific validation rules
- **Context analysis** → Table and schema relationships
- **Custom Keywords** → Business-specific detection (e.g., "ingredient", "recipe_id")

**Governed Tag Integration**:
- **Unity Catalog Tag Policies**: Direct API integration with Databricks governed tags
- **Policy Enforcement**: Configurable restrictions (governed-only vs. mixed tagging)
- **Workflow Integration**: Real-time policy application in Review & Commit
- **Permission Handling**: Graceful fallback when tag policies aren't accessible

**Workspace-Wide Configuration**:
- **Model Management**: Enable/disable built-in models, add custom Databricks serving endpoints
- **Connectivity Validation**: Real-time testing of custom model endpoints
- **Batch Operations**: Concurrency-safe settings with retry logic
- **Settings Persistence**: Delta Lake storage with workspace-wide scope

### AI-Powered Generation

**Multi-Model Support**:
- **databricks-gpt-oss-120b** - High-quality general purpose
- **databricks-llama-3-1-70b** - Open source alternative
- **databricks-gemma-3-12b** - Lightweight and fast
- **databricks-claude-3-5-sonnet** - Advanced reasoning

**Generation Styles**:
- **Concise** - Brief, essential information only
- **Technical** - Detailed technical specifications  
- **Business** - Business-focused descriptions

**Intelligent Context**:
- **Schema generation** - Includes table names and relationships
- **Table generation** - Includes column names and data types
- **Column generation** - Includes sample data and patterns
- **Chunking strategy** - Optimized batch processing

### Quality & Performance

**SQL-Optimized Queries**:
- **Fast counts** via `information_schema` (sub-second response)
- **Efficient filtering** with proper indexing
- **Parallel processing** for large catalogs
- **Client-side caching** for instant re-renders

**Quality Metrics**:
- **Completeness scoring** - Percentage of documented objects
- **Confidence analysis** - AI generation quality assessment  
- **PII risk scoring** - Weighted sensitivity analysis
- **Accuracy tracking** - Schema drift and constraint validation

---

## 🛡️ Security & Governance

### Authentication & Authorization
- **Service Principal** authentication with OAuth2
- **Unity Catalog permissions** validation
- **Role-based access** control
- **Audit logging** for all operations

### Data Privacy
- **No data export** - All processing happens within Databricks
- **PII detection** without data exposure
- **Secure sampling** with configurable limits
- **Encrypted communication** via HTTPS/TLS

### Compliance Features
- **Audit trails** for all metadata changes
- **Policy tag enforcement** 
- **Data classification** levels (PUBLIC → PHI)
- **Retention policies** for generated metadata

---

## ⚙️ Configuration

### Environment Variables

**Auto-Provided by Databricks Apps** (no configuration needed):
```bash
DATABRICKS_CLIENT_ID=<service-principal-id>                  # Service principal ID
DATABRICKS_CLIENT_SECRET=***                                 # Service principal secret  
DATABRICKS_HOST=<workspace-hostname>                         # Workspace hostname
DATABRICKS_WORKSPACE_ID=<workspace-id>                       # Workspace ID
DATABRICKS_APP_NAME=<your-app-name>                          # App name
DATABRICKS_APP_URL=https://<your-app-name>-<workspace-id>.<region>.databricksapps.com  # App URL
```

**Required in app.yaml**:
```yaml
DATABRICKS_HTTP_PATH: "/sql/1.0/warehouses/<your-warehouse-id>"     # SQL Warehouse path
DATABRICKS_SERVER_HOSTNAME: "<your-workspace-hostname>"            # Server hostname
```

**Optional - Advanced Configuration**:
```bash
DBXMETAGEN_OUT_CATALOG: "uc_metadata_assistant"  # Default catalog
DBXMETAGEN_OUT_SCHEMA: "generated_metadata"      # Default schema
```

### Generation Settings

```python
# Default configuration (customizable via UI)
{
    'sample_rows': 50,           # Data samples per column
    'max_chunk_size': 10,        # Objects per LLM request  
    'confidence_threshold': 0.7,  # Quality threshold
    'max_tokens': 512,           # LLM response limit
    'temperature': 0.3,          # Generation creativity
    'enable_pii_detection': True, # PII analysis
    'enable_data_profiling': True # Statistical analysis
}
```

### Performance Tuning

```javascript
// Client-side caching (automatic)
window.generationOptionsCache = {};  // Generation options
localStorage.coverage_v6_*;          // Coverage data  
localStorage.savedFilter_*;          // Filter preferences

// SQL optimization (built-in)
- information_schema queries for fast counts
- Parallel API calls for data loading
- Progressive rendering for large datasets
- Debounced user interactions
```

---

## 🔧 Permissions

### Required Unity Catalog Permissions

```sql
-- For reading catalogs and metadata
GRANT USE CATALOG ON CATALOG <target_catalog> TO `<service-principal>`;
GRANT USE SCHEMA ON SCHEMA <target_catalog>.* TO `<service-principal>`;
GRANT SELECT ON SCHEMA <target_catalog>.* TO `<service-principal>`;

-- For writing metadata (MANAGE required for COMMENT statements)
GRANT MANAGE ON CATALOG <target_catalog> TO `<service-principal>`;

-- For app infrastructure (auto-created)
GRANT CREATE CATALOG ON METASTORE TO `<service-principal>`;
GRANT CREATE SCHEMA ON CATALOG uc_metadata_assistant TO `<service-principal>`;
```

### LLM Serving Endpoint Access

```sql
-- Grant access to serving endpoints
GRANT EXECUTE ON MODEL <model_name> TO `<service-principal>`;

-- Examples for supported models:
GRANT EXECUTE ON MODEL databricks-gpt-oss-120b TO `<service-principal>`;
GRANT EXECUTE ON MODEL databricks-llama-3-1-70b TO `<service-principal>`;
GRANT EXECUTE ON MODEL databricks-gemma-3-12b TO `<service-principal>`;
GRANT EXECUTE ON MODEL databricks-claude-3-5-sonnet TO `<service-principal>`;
```

---

## 📊 API Reference

### Core Endpoints

```http
# Metadata Discovery
GET  /api/catalogs                           # List available catalogs
GET  /api/fast-counts/<catalog>              # Fast metadata counts
GET  /api/missing-metadata/<catalog>/<type>  # Missing metadata objects
GET  /api/coverage/<catalog>                 # Coverage by month analysis

# AI Generation  
POST /api/enhanced/run                       # Start enhanced generation
GET  /api/enhanced/status/<run_id>           # Check generation status
GET  /api/enhanced/results                   # Import generated results

# Quality & Analytics
GET  /api/quality-metrics/<catalog>          # Quality dashboard data
GET  /api/metadata-history/<catalog>         # Audit trail and history

# Metadata Management
POST /api/submit-metadata                    # Submit to Unity Catalog
GET  /api/generation-options/<catalog>/<type> # Generation scope options

# Settings & Configuration
GET  /api/settings/models                    # Get model configurations
POST /api/settings/models/toggle             # Enable/disable models (batch)
POST /api/settings/models/add                # Add custom model with validation
DELETE /api/settings/models/remove           # Remove custom model

GET  /api/settings/pii                       # Get PII detection configuration
POST /api/settings/pii/add                   # Add custom PII pattern
DELETE /api/settings/pii/remove              # Remove custom PII pattern
POST /api/settings/pii/toggle-assessment     # Toggle LLM assessment
POST /api/settings/pii/toggle-detection      # Toggle LLM PII detection

GET  /api/settings/tags                      # Get tags policy configuration
POST /api/settings/tags/update               # Update tags policy (batch)

GET  /api/governed-tags                      # Get Unity Catalog governed tags
```

### Filter Parameters

```http
# All endpoints support filtering
?object_type=schema|table|column
?data_object=schema.table.column  
?owner=user@domain.com
```

---

## 🚀 Performance & Scalability

### Performance Characteristics

**Note**: These are estimated performance characteristics based on system architecture. Actual performance varies by workspace configuration, LLM endpoint availability, and concurrent usage.

| Catalog Size | Discovery Time* | Generation Time** | UI Response*** |
|-------------|----------------|-------------------|----------------|
| Small (1K objects) | <2s | 5-15min | <100ms |
| Medium (10K objects) | <5s | 1-3hr | <150ms |
| Large (100K objects) | <15s | 8-24hr | <300ms |
| Enterprise (1M+ objects) | <60s | 3-7 days | <500ms |

**Performance Notes:**
- ***Discovery Time**: Metadata scanning using `information_schema` queries (SQL-optimized)
- ****Generation Time**: AI description generation with default batch size (10 objects/request) and standard LLM endpoints. Varies significantly based on:
  - **LLM Model Speed**: GPT vs Llama vs Gemma response times
  - **Batch Size**: Configurable from 1-50 objects per request
  - **Concurrency**: Single-threaded generation (can be parallelized)
  - **Endpoint Availability**: Model serving endpoint queue times
- ****UI Response**: Frontend interaction times (tab switching, filtering, data loading) with client-side caching enabled

### Real-World Performance Factors

**Generation Speed Variables:**
- **LLM Model Choice**: 
  - GPT models: ~2-5s per batch (10 objects)
  - Llama models: ~3-8s per batch  
  - Gemma models: ~1-3s per batch (fastest)
  - Claude models: ~2-6s per batch
- **Batch Size Impact**: Larger batches (20-50 objects) reduce total time but increase individual request time
- **Endpoint Queuing**: Shared model serving endpoints may have wait times during peak usage
- **Object Complexity**: Tables with many columns take longer to analyze than simple schemas

**Realistic Generation Examples:**
```
1,000 objects ÷ 10 objects/batch = 100 batches
100 batches × 3s average = 5 minutes (optimal)
100 batches × 8s with queuing = 13 minutes (realistic)

10,000 objects ÷ 10 objects/batch = 1,000 batches  
1,000 batches × 3s average = 50 minutes (optimal)
1,000 batches × 8s with queuing = 2.2 hours (realistic)
```

**UI Performance Optimization:**
- **Discovery Operations**: Sub-second response via optimized SQL queries
- **Tab Switching**: Instant with client-side caching (localStorage)
- **Filtering**: Real-time updates without server round-trips
- **Progress Tracking**: Live updates during generation without blocking UI

### Optimization Features

**Backend Performance**:
- **SQL-first approach** - Direct `information_schema` queries for sub-second discovery
- **Async processing** - Non-blocking generation with real-time progress tracking
- **Configurable batching** - Adjustable batch sizes (1-50 objects) for optimal throughput
- **Intelligent caching** - Multi-layer caching for repeated operations

**Frontend Performance**:  
- **Progressive loading** - Staggered component rendering for large datasets
- **Client-side caching** - Instant tab switching with localStorage persistence
- **Lazy loading** - On-demand data fetching for improved initial load times
- **Debounced interactions** - Smooth user experience with optimized event handling

**Resource Management**:
- **Memory efficient** - Streaming data processing for large catalogs
- **Timeout handling** - Configurable timeouts prevent hanging requests
- **Error recovery** - Graceful failure handling with retry mechanisms
- **Rate limiting** - Respects LLM endpoint limits and quotas

---

## 🔍 Monitoring & Troubleshooting

### Built-in Monitoring

**Quality Metrics Dashboard**:
- **Completeness trends** - Historical metadata coverage
- **PII risk analysis** - Security and compliance monitoring
- **Generation success rates** - AI performance tracking
- **User activity** - Audit trails and usage patterns

**Performance Monitoring**:
- **API response times** - Endpoint performance tracking
- **LLM usage** - Token consumption and costs
- **Cache hit rates** - Optimization effectiveness
- **Error rates** - System health monitoring

### Common Issues & Solutions

**1. Slow Performance**
```bash
# Check SQL warehouse status
# Verify client-side caching is working
# Monitor LLM endpoint response times
# Review filter complexity
```

**2. Generation Failures**  
```bash
# Verify LLM endpoint permissions
# Check token limits and quotas
# Review input data quality
# Validate service principal permissions
```

**3. UI Issues**
```bash
# Clear browser cache and localStorage
# Check browser console for JavaScript errors  
# Verify network connectivity
# Test in incognito mode (extension conflicts)
```

**4. Permission Errors**
```bash
# Validate Unity Catalog permissions
# Check service principal configuration
# Verify SQL warehouse access
# Test LLM endpoint connectivity
```

**5. Catalog Creation Failures**
```bash
# Error: "Metastore storage root URL does not exist"
# Solution: Create catalog manually in Databricks UI
# 1. Go to Catalog → Create Catalog
# 2. Name: uc_metadata_assistant  
# 3. Use Default Storage
# 4. Click Create
# App will auto-detect existing catalog and continue setup
```

### Debug Mode

```python
# Enable detailed logging in app.py
import logging
logging.basicConfig(level=logging.DEBUG)

# Check browser console for client-side issues
# Monitor Databricks app logs for backend issues
# Use SQL warehouse query history for performance analysis
```

---

## 🆚 Comparison with Alternatives

| Feature | UC Metadata Assistant | Manual Process | External Tools |
|---------|----------------------|----------------|----------------|
| **Setup Time** | ✅ 5 minutes | ❌ Days/weeks | ❌ Hours/days |
| **Maintenance** | ✅ Zero | ❌ Ongoing | ❌ Regular updates |
| **Data Security** | ✅ Never leaves Databricks | ⚠️ Manual handling | ❌ External systems |
| **PII Detection** | ✅ Built-in enterprise | ❌ Manual review | ⚠️ Additional cost |
| **Unity Catalog Integration** | ✅ Native | ⚠️ Manual SQL | ⚠️ API complexity |
| **Multi-Model Support** | ✅ 4+ models | ❌ Single approach | ⚠️ Limited options |
| **Quality Analytics** | ✅ Comprehensive dashboard | ❌ No visibility | ⚠️ Basic reporting |
| **Audit Trails** | ✅ Complete history | ❌ Manual tracking | ⚠️ Limited auditing |
| **Cost** | ✅ Only LLM usage | ❌ Manual labor | ❌ Licensing + usage |

---

## 📚 Resources

### Documentation
- [Databricks Apps Platform](https://docs.databricks.com/dev-tools/databricks-apps/)
- [Unity Catalog Governance](https://docs.databricks.com/data-governance/unity-catalog/)
- [LLM Serving Endpoints](https://docs.databricks.com/machine-learning/model-serving/)
- [SQL Warehouse Configuration](https://docs.databricks.com/sql/admin/sql-endpoints.html)

### Support Channels
- **GitHub Issues** - Bug reports and feature requests
- **Databricks Community** - General questions and discussions
- **Enterprise Support** - Priority support for production deployments

### Training Materials
- **Video Tutorials** - Step-by-step deployment and usage guides
- **Best Practices** - Governance patterns and recommendations
- **API Documentation** - Complete endpoint reference
- **Troubleshooting Guide** - Common issues and solutions

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Built with ❤️ using:
- **Databricks Platform** - Unified analytics and AI platform
- **Unity Catalog** - Data governance and security layer  
- **Flask** - Lightweight web application framework
- **Chart.js** - Beautiful and responsive charts
- **Modern Web Standards** - Progressive enhancement and accessibility

**Special thanks to the Databricks community for feedback and contributions!**

---

<div align="center">

### 🚀 Ready to transform your metadata governance?

**[Deploy Now](#deployment)** | **[View Demo](https://your-demo-url)** | **[Get Support](#-resources)**

</div>

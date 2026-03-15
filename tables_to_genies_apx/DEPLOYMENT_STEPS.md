# Deployment Steps for Tables-to-Genies APX

## Overview
This guide walks you through deploying the updated Tables-to-Genies APX application to Databricks with the merged folder structure and parameter size fix.

## What Changed

### 1. Folder Structure (Merged)
```
tables_to_genies_apx/
├── src/
│   └── tables_genies/
│       ├── backend/         # FastAPI backend
│       ├── ui/              # React frontend
│       ├── data_synthesis/  # ✨ Moved from old folder
│       ├── etl/             # ✨ Moved from old folder (includes updated enrich_tables_direct.py)
│       └── graphrag/        # ✨ Moved from old folder
├── app.yaml                 # ✨ Updated with environment variables
├── requirements.txt
└── run-dev.sh
```

### 2. Fixed Issues
- ✅ Merged `tables_to_genies` into `tables_to_genies_apx` (single source of truth)
- ✅ Fixed parameter size limit error (now uses temp Delta table instead of long comma-separated list)
- ✅ Updated paths to point to merged folder location
- ✅ Added environment variables in `app.yaml` for proper configuration

## Deployment Instructions

### Step 1: Verify Prerequisites
```bash
# Check Databricks CLI version (need 0.250.0 or above)
databricks -v

# Verify authentication
databricks auth profiles
```

### Step 2: Deploy the Application

From the project root, deploy the entire app bundle:

```bash
# Deploy to Databricks Workspace
databricks apps deploy tables-to-genies-apx \
  --source-code-path /Workspace/Users/yang.yang@databricks.com/tables_to_genies_apx \
  --profile PROD
```

This command will:
- Upload all files in `tables_to_genies_apx/` to Databricks Workspace
- Deploy the FastAPI backend (APX framework)
- Deploy the React frontend
- Deploy the enrichment script (`etl/enrich_tables_direct.py`)
- Deploy the GraphRAG module (`graphrag/build_table_graph.py`)
- Deploy data synthesis scripts

### Step 3: Verify Deployment

After deployment completes, verify the files are in Workspace:

```bash
# List deployed files
databricks workspace list /Workspace/Users/yang.yang@databricks.com/tables_to_genies_apx/src/tables_genies/etl/

# You should see: enrich_tables_direct.py
```

### Step 4: Start the App

The app should start automatically after deployment. You can check status:

```bash
databricks apps status tables-to-genies-apx --profile PROD
```

### Step 5: Test the Enrichment Workflow

1. Open the app in your browser (URL provided after deployment)
2. Navigate through the workflow:
   - **Catalog Browser** → Select tables
   - **Enrichment** → Click "Run Enrichment"
   - The backend will:
     - Create a temp Delta table with your table list
     - Submit a Databricks job pointing to the deployed `enrich_tables_direct.py`
     - The job reads tables from the temp table (no 10KB limit!)
     - Results are written to Unity Catalog

## Environment Variables (Configured in app.yaml)

The following environment variables are now configured in `app.yaml`:

```yaml
env:
  - name: ENRICHMENT_SCRIPT_PATH
    value: "/Workspace/Users/yang.yang@databricks.com/tables_to_genies_apx/src/tables_genies/etl/enrich_tables_direct.py"
  - name: DATABRICKS_SQL_WAREHOUSE_ID
    value: "a4ed2ccbda385db9"
```

These are automatically set when the app runs on Databricks.

## How the Parameter Size Fix Works

### Before (Failed with many tables)
```
Backend → Job with notebook_params={"tables": "table1,table2,table3,...table500"}
                                    ↓
                           11,778 bytes > 10KB limit ❌
```

### After (Works with unlimited tables)
```
Backend → Creates temp Delta table: serverless_dbx_unifiedchat_catalog.gold.temp_enrichment_tables_{uuid}
       → Writes table list to temp table
       → Job with notebook_params={"table_list_table": "...temp_enrichment_tables_abc123"}
       → Script reads tables from temp table ✅
```

## Troubleshooting

### Issue: "Job failed - notebook not found"
**Solution:** Verify the enrichment script was deployed:
```bash
databricks workspace export /Workspace/Users/yang.yang@databricks.com/tables_to_genies_apx/src/tables_genies/etl/enrich_tables_direct.py
```

### Issue: "GraphRAG module not found"
**Solution:** The import path was updated to use the merged structure. Verify deployment and restart the app.

### Issue: "Environment variable not set"
**Solution:** Redeploy with updated `app.yaml`:
```bash
databricks apps deploy tables-to-genies-apx --source-code-path /Workspace/Users/yang.yang@databricks.com/tables_to_genies_apx --profile PROD
```

## Next Steps

1. **Deploy to production** - Follow the same steps with `--profile PROD`
2. **Monitor jobs** - Check job runs in Databricks Workflows UI
3. **Clean up old files** - The old `tables_to_genies/` folder has been removed locally; you may want to remove it from Workspace too:
   ```bash
   databricks workspace rm -r /Workspace/Users/yang.yang@databricks.com/tables_to_genies
   ```

## Files Modified in This Update

- ✅ `app.yaml` - Added environment variables
- ✅ `router.py` - Updated ENRICHMENT_SCRIPT_PATH default and added temp table logic
- ✅ `etl/enrich_tables_direct.py` - Added support for `table_list_table` parameter
- ✅ `router.py` - Updated GraphRAG import path to use merged structure

## Support

If you encounter issues, check:
1. Databricks CLI version (`databricks -v`)
2. App logs in Databricks UI
3. Job run logs in Workflows UI
4. Environment variables in App details page

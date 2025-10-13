# User Roles and Workflows

<img src="../images/personas.png" alt="User Personas" width="500">

## Who Uses dbxmetagen

### Data Engineer
Handles setup, configuration, and running metadata generation jobs. Configures `variables.yml`, manages Unity Catalog permissions, and schedules workflows. Troubleshoots issues and coordinates with Data Stewards on output quality.

**Key tasks:**
- Deploy dbxmetagen (notebook, asset bundle, or Streamlit app)
- Configure model endpoints and privacy settings
- Run metadata generation on table lists
- Export run logs for review
- Apply approved DDL to Unity Catalog

### Data Steward
Reviews AI-generated metadata for accuracy and business relevance. Approves or edits descriptions and classifications before they're applied to Unity Catalog.

**Review process:**
1. Access run logs (Streamlit app or Excel/TSV exports)
2. Review generated metadata:
   - **Comments**: Check descriptions are accurate and don't expose sensitive data
   - **PI Classification**: Verify PII/PHI/PCI tags are correct
   - **Domains**: Validate business domain assignments
3. Edit as needed and submit for DDL application
4. Create manual overrides CSV for recurring corrections

**Review locations:**
- Streamlit app: `/Workspace/Apps/dbxmetagen-app`
- Exported logs: `/Volumes/{catalog}/{schema}/generated_metadata/{user}/{date}/exportable_run_logs/`
- Reviewed outputs: `/Volumes/{catalog}/{schema}/generated_metadata/{user}/reviewed_outputs/`

### Compliance Officer
Ensures PII/PHI/PCI classification meets regulatory requirements. Audits metadata for compliance and coordinates data access policies.

**Key responsibilities:**
- Configure privacy settings (`allow_data`, `allow_data_in_comments`)
- Verify model endpoints are HIPAA/SOC2 compliant if required
- Audit PI classifications for accuracy
- Review generated comments for data leakage
- Document compliance decisions

**Important settings:**
- `allow_data: false` - No data sent to LLMs (maximum privacy)
- `disable_medical_information_value: true` - All medical data treated as PHI
- `include_deterministic_pi: true` - Use Presidio for rule-based detection
- Default PPT endpoints are NOT HIPAA-compliant

### Data Scientist / Data Analyst
Uses enriched metadata to discover and understand datasets. Searches Unity Catalog by descriptions, domains, and classification tags. Provides feedback on metadata quality.

**Benefits:**
- Faster dataset discovery through better search
- Understand tables without inspecting data
- Improved Databricks Genie accuracy
- Awareness of PII/PHI/PCI for compliance

---

## Common Workflows

### Initial Setup
1. Clone repo into Databricks workspace
2. Update `variables.yml` with your catalog/schema settings
3. Test on 5-10 tables with `apply_ddl: false`
4. Review output quality
5. Deploy for production use (asset bundle or Streamlit app)

### Bulk Generation
1. Prepare table list (CSV or `catalog.schema.*` wildcards)
2. Configure job: `sample_size: 10`, `columns_per_call: 10`, `apply_ddl: false`
3. Run metadata generation
4. Export logs for Data Steward review
5. After approval, re-run with `apply_ddl: true`

### Metadata Review
Data Steward reviews exported run logs:
- Download Excel/TSV from volume
- Edit `column_content` (comments), `classification`/`type` (PI), or `domain`/`subdomain` columns
- Save to `reviewed_outputs` folder
- Data Engineer runs `sync_reviewed_ddl` notebook with filename
- Approved metadata applied to Unity Catalog

### Manual Overrides
For consistent corrections across tables, create `metadata_overrides.csv`:
```csv
catalog,schema,table,column,override_type,override_value
prod,claims,*,member_id,classification,pii
prod,*,*,mrn,classification,phi
```
Enable with `allow_manual_override: true` in `variables.yml`. Overrides take precedence over AI-generated metadata.

---

## Tips

**Start small:** Test on 5-10 tables, validate quality, then scale up.

**Balance detail vs speed:** More columns per call = faster but less detailed. Recommended: 5-10 columns per call.

**Privacy first:** For sensitive data, start with `allow_data: false` and only enable after validating model endpoint security.

**Iterative refinement:** Generate → Review → Create overrides for recurring issues → Regenerate.

**Review criteria:** Accuracy, clarity, no sensitive data exposure, alignment with organizational standards.

---

For technical configuration details, see the main [README](../README.md).

For deployment instructions, see [ENV_SETUP.md](ENV_SETUP.md).

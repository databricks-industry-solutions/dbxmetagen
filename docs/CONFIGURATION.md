# Configuration Reference

## Key Settings

Most important settings in `variables.yml`:

**Privacy & Security:**
- `allow_data`: Send data to LLM (false = maximum privacy)
- `allow_data_in_comments`: Allow data in generated comments
- `sample_size`: Rows sampled per table (0 = no data sampling)
- `disable_medical_information_value`: Treat all medical data as PHI

**Model & Performance:**
- `model`: LLM endpoint (recommend `databricks-meta-llama-3-3-70b-instruct` or `databricks-claude-3-7-sonnet`)
- `columns_per_call`: Columns per LLM call (5-10 recommended)
- `temperature`: Model creativity (0.1 for consistency)
- `max_tokens`: Maximum output length

**Workflow:**
- `apply_ddl`: Apply changes directly to Unity Catalog (false = generate only)
- `ddl_output_format`: Output format (excel, tsv, or sql)
- `allow_manual_override`: Enable CSV-based overrides

**PI Detection:**
- `include_deterministic_pi`: Use Presidio for rule-based PII detection
- `tag_none_fields`: Tag columns classified as non-sensitive

## Full Variable Reference

| Variable | Description | Default |
|----------|-------------|---------|
| catalog_name | Target catalog | None |
| schema_name | Output schema | metadata_results |
| volume_name | Output volume | generated_metadata |
| allow_data | Send data to LLM | false |
| allow_data_in_comments | Include data in comments | true |
| sample_size | Rows to sample | 10 |
| add_metadata | Include extended metadata | true |
| include_datatype_from_metadata | Include data types | false |
| include_possible_data_fields_in_metadata | Include min/max (may leak PII) | true |
| disable_medical_information_value | Treat medical data as PHI | true |
| solo_medical_identifier | MRN classification (pii or phi) | pii |
| model | LLM endpoint | databricks-meta-llama-3-3-70b-instruct |
| temperature | Model temperature | 0.1 |
| max_tokens | Maximum output tokens | 4096 |
| max_prompt_length | Maximum prompt length | 4096 |
| columns_per_call | Columns per LLM call | 5 |
| word_limit_per_cell | Max words per cell | 100 |
| limit_prompt_based_on_cell_len | Truncate long cells | true |
| apply_ddl | Apply DDL to tables | false |
| ddl_output_format | DDL format (sql/tsv/excel) | excel |
| reviewable_output_format | Review file format | excel |
| review_input_file_type | Review input format | tsv |
| review_output_file_type | Review output format | excel |
| review_apply_ddl | Apply reviewed DDL | false |
| include_deterministic_pi | Use Presidio detection | true |
| spacy_model_names | SpaCy model for Presidio | en_core_web_lg |
| tag_none_fields | Tag non-sensitive columns | true |
| allow_manual_override | Enable CSV overrides | true |
| override_csv_path | Override CSV path | metadata_overrides.csv |
| acro_content | Acronym dictionary | {"DBX":"Databricks"} |
| table_names_source | Table list source | csv_file_path |
| source_file_path | Table list file | table_names.csv |
| control_table | Checkpoint table | metadata_control_{} |
| catalog_tokenizable | Tokenizable catalog name | __CATALOG_NAME__ |
| format_catalog | Format catalog variable | false |

See `variables.yml` for complete descriptions and additional advanced options.

## Privacy Controls

Maximize privacy by combining these settings:
```yaml
allow_data: false                           # No data sent to LLM
allow_data_in_comments: false              # No data in output
sample_size: 0                             # No sampling
include_possible_data_fields_in_metadata: false  # No min/max
```

For healthcare data:
```yaml
disable_medical_information_value: true    # All medical = PHI
solo_medical_identifier: phi               # MRN always PHI
include_deterministic_pi: true             # Use Presidio
```

Note: Default PPT endpoints are NOT HIPAA-compliant. Configure secure endpoints as needed.

## Usage Patterns

### Output Review Workflow

Each run exports logs to `/Volumes/{catalog}/{schema}/generated_metadata/{user}/{date}/exportable_run_logs/`

To review and edit:
1. Download Excel/TSV file
2. Edit metadata:
   - **Comments**: Edit `column_content` column
   - **PI**: Edit `classification` and `type` columns
   - **Domain**: Edit `domain` and `subdomain` columns
3. Save to `/Volumes/{catalog}/{schema}/generated_metadata/{user}/reviewed_outputs/`
4. Run `sync_reviewed_ddl` notebook with filename
5. Set `review_apply_ddl: true` to apply changes

### Manual Overrides

Create `metadata_overrides.csv` for consistent corrections:
```csv
catalog,schema,table,column,override_type,override_value
prod,claims,*,member_id,classification,pii
prod,*,*,mrn,classification,phi
*,*,*,ssn,classification,pii
```

Use `*` wildcards for broad application. Enable with `allow_manual_override: true`.

### Performance Tuning

**Faster, less detail:**
```yaml
columns_per_call: 20
sample_size: 5
```

**Slower, more detail:**
```yaml
columns_per_call: 3
sample_size: 20
```

Recommended balanced settings: `columns_per_call: 5-10`, `sample_size: 10`

## PI Classification Rules

**PII (Personally Identifiable Information):** Name, address, SSN, email, phone number

**PHI (Protected Health Information):** Medical records, diagnoses, treatment dates, plus any PII linked to health data

**PCI (Payment Card Information):** Card numbers, CVV, expiration dates, cardholder names

**Classification logic:**
- Individual columns classified by content (name = PII, diagnosis = PHI)
- Tables inherit highest classification from columns
- Exception: Name/address columns in medical tables remain PII (not PHI) unless they contain health information
- Medical tables with any PHI column = PHI table

Configure with `solo_medical_identifier` and `disable_medical_information_value` for stricter/looser rules.

## Implementation Notes

### Data Sampling
- Samples `sample_size` rows per table, filtered for non-null values
- Cells truncated to `word_limit_per_cell` words
- Chunked by `columns_per_call` for scalable LLM processing

### Metadata Extraction
- `DESCRIBE EXTENDED` metadata optionally included
- Filtered based on privacy settings
- Acronyms expanded via `acro_content`

### PI Detection
- Presidio (rule-based) runs first if `include_deterministic_pi: true`
- LLM reviews Presidio results and provides final classification
- Classification enforced at column and table levels

### DDL Generation
- Generated as SQL, TSV, or Excel based on `ddl_output_format`
- Paths constructed: `/{user}/{date}/` for isolation
- Applied directly if `apply_ddl: true`, otherwise written to volume

### Checkpointing
- Control table tracks processed tables
- Supports resuming incomplete runs
- Prevents duplicate processing

## Compatibility

**Databricks Runtime:**
- Tested: DBR 14.3 LTS, 15.4 LTS, 16.4 LTS (+ ML versions)
- Views: Only DBR 16.4+
- Excel: ML runtimes only (use TSV on standard runtimes)

**Cross-version DDL:**
- DDL generated on 16.4 may not apply on 14.3
- Test in same environment where applying

For deployment details, see [ENV_SETUP.md](ENV_SETUP.md).

For user workflows, see [PERSONAS_AND_WORKFLOWS.md](PERSONAS_AND_WORKFLOWS.md).


# Migration Guide: From Codebook-Based to YAML-Based System

This guide helps users of the legacy codebook-based system migrate to the new YAML-based architecture.

## Overview of Changes

The cdm-cbioportal-etl pipeline underwent a major refactoring in 2025:

| Aspect | Legacy System | New System |
|--------|---------------|------------|
| **Configuration** | Google Sheets codebook (CSV exports) | YAML files (version controlled) |
| **Data Storage** | MinIO object storage | Databricks volumes |
| **Summary Pipeline** | Monolithic wrapper scripts | 4 modular, independent scripts |
| **Timeline Processing** | Hardcoded configurations | YAML-based batch processing |
| **Dependencies** | MinioAPI, codebook parser | DatabricksAPI, YAML parser |
| **Monitoring** | SFTP sensor-based | Filesystem-based, exit codes |
| **Processing** | All-at-once, opaque | Step-by-step, inspectable |

## Why the Change?

**Problems with the old system:**
- Configuration changes required Google Sheets + CSV export + git commit
- MinIO added complexity and wasn't well-integrated with Databricks
- Monolithic scripts were hard to debug
- Adding new data sources required code changes
- Intermediate steps not inspectable

**Benefits of the new system:**
- ✅ Configuration is version-controlled and code-reviewable
- ✅ Single storage system (Databricks)
- ✅ Can run and debug individual pipeline steps
- ✅ Adding new data sources is just creating a YAML file
- ✅ All intermediate files saved for inspection
- ✅ Clearer data flow and dependencies

## Migration Checklist

### For Summary Data

**Old workflow:**
1. Add columns to Google Sheets codebook (Metadata tab)
2. Export codebook CSVs
3. Commit CSVs to docs repo
4. Pull changes to server
5. Run `bash_summary_creator.sh` or `bash_yaml_summary_creator.sh`

**New workflow:**
1. Create YAML file in `config/summaries/`
2. Commit YAML to repo
3. Run `bash_summary_modular_pipeline.sh`

**Migration steps:**
1. Use `codebook_to_yaml_converter.py` to convert existing codebook entries
2. Review and edit generated YAML files
3. Test with dev tables first
4. Update to production tables

### For Timeline Data

**Old workflow:**
1. Hardcode timeline config in Python script
2. Modify script to add new timeline
3. Update bash wrapper if needed

**New workflow:**
1. Create YAML file in `config/timelines/`
2. Run `bash_timeline_batch_deid.sh`

**Migration steps:**
1. Extract hardcoded configs into YAML files (already done for 21 timelines)
2. Review YAML files for accuracy
3. Add new timelines by creating new YAMLs

## Converting Codebook to YAML

If you have existing codebook entries not yet converted:

```bash
python pipeline/utils/codebook_to_yaml_converter.py \
  --metadata /path/to/CDM-Codebook-metadata.csv \
  --tables /path/to/CDM-Codebook-tables.csv \
  --output_dir config/summaries \
  --catalog_prod cdsi_prod \
  --schema_prod cdsi_data_deid \
  --volume_prod cdsi_data_deid_volume \
  --catalog_dev cdsi_dev \
  --schema_dev cdsi_data_deid_dev \
  --volume_dev cdsi_data_deid_volume_dev
```

This generates one YAML file per summary table.

## Key Concept Changes

### 1. MinIO → Databricks Volumes

**Old:**
```python
from msk_cdm.minio import MinioAPI
minio = MinioAPI(fname_minio_env='minio_env.txt')
df = minio.load_df(path='path/in/minio/file.tsv')
```

**New:**
```python
from msk_cdm.databricks import DatabricksAPI
db = DatabricksAPI(fname_databricks_env='databricks_env.txt')
df = db.read_db_obj(volume_path='/Volumes/catalog/schema/volume/file.tsv')
```

### 2. Codebook CSV → YAML Config

**Old (Metadata CSV):**
```csv
field_name,table_id,source_table_name,label,datatype,...
GENDER,demographics,t01_epic_ddp_demographics,Sex,STRING,...
```

**New (YAML):**
```yaml
summary_id: demographics
source_table_prod: cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics
columns:
  - GENDER
column_metadata:
  GENDER:
    label: "Sex"
    datatype: STRING
    comment: "Sex ---DESCRIPTION: ... ---SOURCE: ..."
```

### 3. Monolithic → Modular Pipeline

**Old:**
```bash
# One script does everything
bash_yaml_summary_creator.sh [many args]
```

**New:**
```bash
# 4 independent scripts
python pipeline/summary/create_intermediate_summaries.py [args]
python pipeline/summary/merge_intermediate_summaries.py [args]
python pipeline/summary/create_summary_header.py [args]
python pipeline/summary/combine_header_and_data.py [args]

# Or use wrapper
bash_summary_modular_pipeline.sh [args]
```

## Deprecated Components

These files/classes are no longer used:

### Python Classes (moved to `pipeline/lib/summary/legacy/`)
- `RedcapToCbioportalFormat` → Replaced by `SummaryConfigProcessor`
- `cbioportalSummaryFileCombiner` → Replaced by modular scripts
- Old template generator → New utility script

### Bash Scripts (deprecated but still in repo)
- `bash_yaml_summary_creator.sh` → Use `bash_summary_modular_pipeline.sh`
- `bash_summary_creator.sh` → Use `bash_summary_modular_pipeline.sh`

### Documentation (moved to `docs/archive/`)
- `summary_data_formatting.md`
- `summary_template_generation.md`
- `timeline_files.md`
- `modifying_the_codebook.md`

## Step-by-Step Migration Example

### Example: Migrating a Custom Summary

**Scenario:** You have a custom biomarker summary in the codebook

**Step 1:** Check if YAML already exists
```bash
ls config/summaries/ | grep biomarker
```

**Step 2:** If not, convert from codebook
```bash
# Extract your summary from codebook CSV
python pipeline/utils/codebook_to_yaml_converter.py \
  --metadata codebook-metadata.csv \
  --tables codebook-tables.csv \
  --output_dir config/summaries
```

**Step 3:** Review and edit YAML
```bash
# Edit the generated YAML
vim config/summaries/my_biomarker.yaml

# Verify YAML syntax
python -c "import yaml; yaml.safe_load(open('config/summaries/my_biomarker.yaml'))"
```

**Step 4:** Test with dev tables
```bash
# Update YAML to use dev tables
# Run pipeline with production_or_test='test'
bash pipeline/bash/bash_summary_modular_pipeline.sh \
  [...args...] \
  test \
  mskimpact \
  --sample
```

**Step 5:** Switch to production
```bash
# Update YAML to use prod tables
# Run pipeline with production_or_test='production'
bash pipeline/bash/bash_summary_modular_pipeline.sh \
  [...args...] \
  production \
  mskimpact \
  --sample
```

## Troubleshooting Migration Issues

### Issue: "Can't find MinioAPI"

**Cause:** Code still references legacy MinIO

**Solution:** Update to use DatabricksAPI:
```python
# Old
from msk_cdm.minio import MinioAPI

# New
from msk_cdm.databricks import DatabricksAPI
```

### Issue: "Codebook CSV not found"

**Cause:** Pipeline still looking for codebook files

**Solution:** Ensure you're using YAML-based scripts:
- Use `bash_summary_modular_pipeline.sh` (not `bash_summary_creator.sh`)
- Point to `config/summaries/` directory (not codebook CSVs)

### Issue: "Column not in output but was in old system"

**Cause:** Column might not be in generated YAML

**Solution:**
1. Check if column exists in source table
2. Verify column is listed in YAML `columns` section
3. Check `column_metadata` has entry for the column

### Issue: "Different output format than before"

**Cause:** New pipeline might order columns differently

**Solution:**
- Column ordering is preserved from template + YAML order
- Header format is standard cBioPortal (5 rows)
- Data should be equivalent even if column order differs

## Getting Help

If you encounter migration issues:

1. **Review current docs:** [Documentation Index](../README.md)
2. **Check examples:** Look at existing YAMLs in `config/summaries/` and `config/timelines/`
3. **Test incrementally:** Convert one summary/timeline at a time
4. **Compare outputs:** Run old and new pipelines side-by-side and diff outputs
5. **File an issue:** Include old configuration and error messages

## Additional Resources

- [Architecture Overview](../architecture.md) - Understand new system design
- [Adding New Summary Data](../guides/adding_new_summary_data.md) - YAML configuration guide
- [Adding New Timeline Data](../guides/adding_new_timeline_data.md) - Timeline YAML guide
- [Summary YAML Format](../configuration/summary_yaml_format.md) - Complete specification
- [DAG Migration Guide](DAG_MIGRATION_GUIDE.md) - For Airflow DAG updates

## Timeline

- **Pre-2025:** Legacy codebook-based system
- **2025 Q1:** Refactoring to YAML-based system
- **2025 Q2:** Production deployment
- **2026 Q1:** Legacy code moved to `legacy/` folders
- **2026-03-25:** Documentation refresh, old docs archived

## Questions?

Contact the CDM engineering team or file an issue in the repository.

# DAG Migration Guide: YAML-Based cBioPortal ETL

## Summary

This guide documents the migration from hardcoded timeline configurations and monolithic summary creation to YAML-based, modular pipelines.

---

## Key Changes

### 1. **Timeline Batch Deidentification** (UPDATED)

**Old Approach:**
- Hardcoded `TIMELINE_CONFIGS` in Python
- No support for production/test table switching
- Hardcoded anchor dates table

**New Approach:**
- YAML configuration files in `config/timelines/` (21 files)
- Support for `source_table_prod` and `source_table_dev`
- Configurable anchor dates table
- Column metadata for documentation

**Updated Files:**
- ✅ `pipeline/bash/bash_timeline_batch_deid.sh` - Updated to pass new parameters
- ✅ `pipeline/timeline/cbioportal_timeline_batch_deidentify.py` - Now reads from YAML
- ✅ `pipeline/timeline/cbioportal_timeline_deidentify.py` - Added `--fname_deid` parameter

**New Parameters:**
```bash
bash_timeline_batch_deid.sh \
    $REPO_PATH \
    $CONDA_PATH \
    $CONDA_ENV \
    $CONFIG_DIR \              # NEW: config/timelines
    $PRODUCTION_OR_TEST \      # NEW: production or test
    $FNAME_DBX \
    $ANCHOR_DATES \            # NEW: catalog.schema.table
    $SAMPLE_LIST \
    $VOLUME_BASE_PATH \
    $GPFS_OUTPUT_PATH \
    $COHORT_NAME
```

---

### 2. **Summary Creation** (NEW MODULAR PIPELINE)

**Old Approach:**
- `wrapper_yaml_summary_creator.py` - Monolithic script
- `bash_yaml_summary_creator.sh` - Wrapper
- `bash_summary_creator.sh` - Legacy codebook-based

**New Approach:**
- 4 modular scripts that can be run independently:
  1. `create_intermediate_summaries.py` - Create TSV files from YAMLs
  2. `merge_intermediate_summaries.py` - Merge into single data file
  3. `create_summary_header.py` - Create header metadata
  4. `combine_header_and_data.py` - Combine header + data
- `wrapper_modular_summary_pipeline.py` - Orchestrates all 4 scripts

**New Files:**
- ✅ `pipeline/bash/bash_summary_modular_pipeline.sh` - NEW bash wrapper

**Parameters:**
```bash
bash_summary_modular_pipeline.sh \
    $REPO_PATH \
    $CONDA_PATH \
    $CONDA_ENV \
    $CONFIG_DIR \              # config/summaries
    $DATABRICKS_ENV \
    $ANCHOR_DATES \            # catalog.schema.table
    $TEMPLATE_PATIENT \
    $TEMPLATE_SAMPLE \
    $OUTPUT_DIR_DATABRICKS \
    $OUTPUT_DIR_LOCAL \
    $CATALOG \
    $SCHEMA \
    $PRODUCTION_OR_TEST \
    $COHORT \
    --patient \                # Optional flag
    --sample                   # Optional flag
```

---

### 3. **Monitoring Completeness** (REFACTORED)

**Old Approach:**
- Required YAML config file
- Required Databricks environment file
- Used Databricks API to read files from volumes
- Created log file and used SFTP sensor to wait for completion
- Depended on `cbioportal_update_config` utility

**New Approach:**
- Takes only `path_datahub` as input
- Scans directory for `data_clinical_*.txt` and `data_timeline_*.txt` files
- Reads from local filesystem using pandas
- Returns exit code (0 for pass, non-zero for fail)
- No log file or sensor needed

**Updated Files:**
- ✅ `pipeline/monitoring/monitoring_completeness.py` - Simplified to use filesystem only
- ✅ `pipeline/bash/bash_monitor_completeness.sh` - Reduced from 6 to 4 parameters

**New Parameters:**
```bash
bash_monitor_completeness.sh \
    $REPO_PATH \
    $CONDA_PATH \
    $CONDA_ENV \
    $PATH_DATAHUB  # Only the directory path needed
```

**Benefits:**
- ✅ No Databricks/Minio dependencies in monitoring
- ✅ Simpler parameters (just directory path)
- ✅ Faster execution (no API calls)
- ✅ Removed SFTP sensor (monitoring task returns exit code directly)

---

### 4. **DAG Updates**

**File:** `sandbox/cdm_etl_cbioportal_cdsi_dev.py`

**Major Changes:**

| Old | New | Status |
|-----|-----|--------|
| Commented out timeline deid | `cbioportal_timelines_deid` with YAML | ✅ Enabled |
| Commented out summaries | `cbioportal_summaries` with modular pipeline | ✅ Enabled |
| Commented out monitoring | `trigger_cdm_etl_cbioportal` task group | ✅ Enabled |
| Commented out email | `send_mail` operator | ✅ Enabled |

**New Variables Added:**
```python
# YAML config directories
config_dir_timelines = f"{cdm_cbio_etl_repo_dir}/config/timelines"
config_dir_summaries = f"{cdm_cbio_etl_repo_dir}/config/summaries"

# Anchor dates table (configurable)
anchor_dates_table = "cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates"

# Updated volume path
volume_base_path = f"/Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume_dev/cbioportal"

# Catalog/schema for tables
catalog = "cdsi_eng_phi"
schema = "cdm_eng_cbioportal_etl"
```

---

## Files That Can Be Archived/Removed

### **Legacy Scripts (Can be archived):**

1. ❌ `pipeline/bash/bash_yaml_summary_creator.sh` - Replaced by `bash_summary_modular_pipeline.sh`
2. ❌ `pipeline/bash/bash_summary_creator.sh` - Old codebook-based approach
3. ❌ `pipeline/summary/wrapper_yaml_summary_creator.py` - Replaced by modular pipeline

### **Deprecated (Keep for reference but not used):**

4. ⚠️ `bash_summary_overall_survival.sh` - Overall survival now in summary pipeline
5. ⚠️ `bash_age_attributes.sh` - Age attributes now in summary pipeline

---

## Migration Steps

### Step 1: Update Bash Scripts
- ✅ Updated `bash_timeline_batch_deid.sh` with new parameters
- ✅ Created `bash_summary_modular_pipeline.sh` for summaries

### Step 2: Update Python Scripts
- ✅ Updated `cbioportal_timeline_batch_deidentify.py` to read YAML
- ✅ Updated `cbioportal_timeline_deidentify.py` to accept `--fname_deid`
- ✅ Fixed pandas deprecation warnings

### Step 3: Update DAG
- ✅ Created `cdm_etl_cbioportal_cdsi_dev_UPDATED.py` with:
  - YAML-based timeline deidentification
  - Modular summary pipeline
  - Re-enabled monitoring and email tasks
  - Added new configuration variables

### Step 4: Test
- Run updated DAG with test data
- Verify all 21 timeline files are created
- Verify patient and sample summaries are created
- Verify monitoring passes

### Step 5: Production Deployment
- Review and merge `cdm_etl_cbioportal_cdsi_dev_UPDATED.py` → `cdm_etl_cbioportal_cdsi_dev.py`
- Archive legacy scripts to `pipeline/bash/legacy/` or `pipeline/summary/legacy/`

---

## Benefits of New Approach

### **Maintainability**
- ✅ No code changes needed to add/modify/remove timeline files
- ✅ Single source of truth for column metadata (used for both ETL and docs)
- ✅ Clear separation of configuration from logic

### **Flexibility**
- ✅ Easy to switch between production and test tables
- ✅ Different anchor dates tables for different environments
- ✅ Can run summary steps independently for debugging

### **Documentation**
- ✅ YAML files contain all metadata needed for codebook website
- ✅ Column descriptions, NLP flags, and source info all in one place

### **Debuggability**
- ✅ Modular summary pipeline allows debugging individual steps
- ✅ Can re-run failed steps without re-running entire pipeline

---

## Testing Checklist

- [ ] Test timeline batch deidentification with test tables
- [ ] Test timeline batch deidentification with production tables
- [ ] Test modular summary pipeline (patient only)
- [ ] Test modular summary pipeline (sample only)
- [ ] Test modular summary pipeline (both patient and sample)
- [ ] Verify all 21 timeline files are created correctly
- [ ] Verify patient summary file format matches legacy
- [ ] Verify sample summary file format matches legacy
- [ ] Run monitoring/completeness check
- [ ] Verify S3 push trigger works
- [ ] Verify email notifications work

---

## Rollback Plan

If issues arise, rollback is simple:

1. Comment out new tasks in DAG:
   ```python
   # cbioportal_timelines_deid = ...
   # cbioportal_summaries = ...
   ```

2. Uncomment old tasks (they're already in the file, just commented)

3. Revert bash script changes if needed:
   ```bash
   git checkout HEAD -- pipeline/bash/bash_timeline_batch_deid.sh
   ```

---

## Contact

For questions or issues:
- **Calvin Fong** - fongc2@mskcc.org
- **Chaitanya Chenna** - chennac@mskcc.org

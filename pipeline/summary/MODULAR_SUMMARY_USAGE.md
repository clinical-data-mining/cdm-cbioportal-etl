# Modular Summary Creation Scripts

This document describes the 4 modular scripts for creating cBioPortal summary files from YAML configurations.

## Overview

The monolithic `wrapper_yaml_summary_creator.py` has been refactored into 4 independent, composable scripts:

1. **`create_intermediate_summaries.py`** - Create intermediate TSV files from YAML configs
2. **`merge_intermediate_summaries.py`** - Merge intermediates into single data file (volume + table)
3. **`create_summary_header.py`** - Create header metadata from YAMLs (volume + table, tall format)
4. **`combine_header_and_data.py`** - Combine header + data into final file (volume + local)

---

## Script Details

### 1. Create Intermediate Summaries

**Purpose**: Process all YAML configs and create intermediate TSV files (data only, no headers).

**Inputs**:
- `--config_dir`: Directory containing YAML config files
- `--databricks_env`: Databricks environment file
- `--anchor_dates`: Databricks table name for anchor dates (e.g., `cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates`)
- `--template`: Local filesystem path to template file
- `--patient_or_sample`: 'patient' or 'sample'
- `--production_or_test`: 'production' or 'test'
- `--cohort`: Cohort name (e.g., 'mskimpact')
- `--output_manifest`: Databricks volume path for manifest CSV

**Outputs**:
- Intermediate TSV files → `/Volumes/{catalog}/{schema}/{volume}/cbioportal/intermediate_files/{cohort}/{filename}.tsv`
- Manifest CSV → `--output_manifest` location

**Example**:
```bash
python pipeline/summary/create_intermediate_summaries.py \
  --config_dir config/summaries \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --anchor_dates cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates \
  --template /gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_sample.txt \
  --patient_or_sample patient \
  --production_or_test test \
  --cohort mskimpact \
  --output_manifest /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/intermediate_files/mskimpact/manifest_patient.csv
```

---

### 2. Merge Intermediate Summaries

**Purpose**: Merge all intermediate files horizontally into a single data file.

**Inputs**:
- `--manifest`: Path to manifest CSV from Script 1
- `--databricks_env`: Databricks environment file
- `--template`: Local filesystem path to template file
- `--patient_or_sample`: 'patient' or 'sample'
- `--output_volume_path`: Output Databricks volume path
- `--output_catalog`: Catalog for output table
- `--output_schema`: Schema for output table
- `--output_table`: Table name

**Outputs**:
- Merged data → Volume + Table
- Format: Standard TSV with 1 header row (column names)

**Example**:
```bash
python pipeline/summary/merge_intermediate_summaries.py \
  --manifest /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/intermediate_files/mskimpact/manifest_patient.csv \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --template /gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_sample.txt \
  --patient_or_sample patient \
  --output_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_data.txt \
  --output_catalog cdsi_eng_phi \
  --output_schema cdm_eng_cbioportal_etl \
  --output_table data_clinical_patient
```

---

### 3. Create Summary Header

**Purpose**: Generate cBioPortal-compliant header from YAML configs in tall format.

**Inputs**:
- `--manifest`: Path to manifest CSV from Script 1
- `--databricks_env`: Databricks environment file
- `--merged_data_path`: Databricks volume path to merged data
- `--patient_or_sample`: 'patient' or 'sample'
- `--output_volume_path`: Output Databricks volume path
- `--output_catalog`: Catalog for output table
- `--output_schema`: Schema for output table
- `--output_table`: Table name

**Outputs**:
- Header → Volume + Table
- Format: Tall format (5 columns × N rows)
  - Columns: `column_name`, `display_label`, `description`, `datatype`, `priority`
  - Priority: "0" for ID columns, "1" for other columns

**Example**:
```bash
python pipeline/summary/create_summary_header.py \
  --manifest /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/intermediate_files/mskimpact/manifest_patient.csv \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --merged_data_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_data.txt \
  --patient_or_sample patient \
  --output_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_header.txt \
  --output_catalog cdsi_eng_phi \
  --output_schema cdm_eng_cbioportal_etl \
  --output_table data_clinical_patient_header
```

---

### 4. Combine Header and Data

**Purpose**: Transpose header to wide format and combine with data into final cBioPortal file.

**Inputs**:
- `--header_volume_path`: Databricks volume path to header
- `--data_volume_path`: Databricks volume path to data
- `--databricks_env`: Databricks environment file
- `--output_volume_path`: Output Databricks volume path
- `--output_local_path`: Output local filesystem path

**Outputs**:
- Combined file → Databricks Volume + Local Filesystem
- Format: 5 header rows + N data rows (cBioPortal format)
  - Row 0: Display label (e.g., `#Patient Identifier`, `Age`)
  - Row 1: Description
  - Row 2: Datatype (e.g., `#STRING`, `NUMBER`)
  - Row 3: Priority (e.g., `#0`, `1`)
  - Row 4: Column name (e.g., `PATIENT_ID`, `AGE`)
  - Note: First column values in rows 0-3 have "#" prefix

**Example**:
```bash
python pipeline/summary/combine_header_and_data.py \
  --header_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_header.txt \
  --data_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_data.txt \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --output_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient.txt \
  --output_local_path /gpfs/mindphidata/cdm_repos/dev/data/cdm-data/mskimpact/data_clinical_patient.txt
```

---

## Complete Pipeline Example

### Patient Summaries

```bash
# Step 1: Create intermediates
python pipeline/summary/create_intermediate_summaries.py \
  --config_dir config/summaries \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --anchor_dates cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates \
  --template /gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_sample.txt \
  --patient_or_sample patient \
  --production_or_test test \
  --cohort mskimpact \
  --output_manifest /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/intermediate_files/mskimpact/manifest_patient.csv

# Step 2: Merge intermediates
python pipeline/summary/merge_intermediate_summaries.py \
  --manifest /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/intermediate_files/mskimpact/manifest_patient.csv \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --template /gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_sample.txt \
  --patient_or_sample patient \
  --output_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_data.txt \
  --output_catalog cdsi_eng_phi \
  --output_schema cdm_eng_cbioportal_etl \
  --output_table data_clinical_patient

# Step 3: Create header
python pipeline/summary/create_summary_header.py \
  --manifest /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/intermediate_files/mskimpact/manifest_patient.csv \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --merged_data_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_data.txt \
  --patient_or_sample patient \
  --output_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_header.txt \
  --output_catalog cdsi_eng_phi \
  --output_schema cdm_eng_cbioportal_etl \
  --output_table data_clinical_patient_header

# Step 4: Combine
python pipeline/summary/combine_header_and_data.py \
  --header_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_header.txt \
  --data_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient_data.txt \
  --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  --output_volume_path /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume/cbioportal/mskimpact/data_clinical_patient.txt \
  --output_local_path /gpfs/mindphidata/cdm_repos/dev/data/cdm-data/mskimpact/data_clinical_patient.txt
```

### Sample Summaries

Same commands as above, but replace:
- `patient` → `sample`
- `manifest_patient.csv` → `manifest_sample.csv`
- `data_clinical_patient*` → `data_clinical_sample*`

---

## Benefits

✅ **Modularity**: Each script has a single responsibility
✅ **Debuggability**: Can test/debug each step independently
✅ **Flexibility**: Can re-run any step without re-running entire pipeline
✅ **Table Persistence**: Scripts 2 & 3 save to Databricks tables for downstream queries
✅ **Dual Output**: Script 4 saves to both Databricks and local filesystem
✅ **Manifest Tracking**: Clear tracking of all intermediate files and configs

---

## File Locations

### Intermediate Files
- Location: `/Volumes/{catalog}/{schema}/{volume}/cbioportal/intermediate_files/{cohort}/`
- Format: TSV (data only, no headers)
- One file per YAML config

### Manifest
- Location: `/Volumes/{catalog}/{schema}/{volume}/cbioportal/intermediate_files/{cohort}/manifest_{patient_or_sample}.csv`
- Format: CSV
- Columns: `summary_id`, `yaml_config_path`, `intermediate_data_path`, `patient_or_sample`

### Merged Data
- Volume: `/Volumes/{catalog}/{schema}/{volume}/cbioportal/{cohort}/data_clinical_{patient_or_sample}_data.txt`
- Table: `{catalog}.{schema}.data_clinical_{patient_or_sample}`
- Format: TSV with 1 header row (column names)

### Header
- Volume: `/Volumes/{catalog}/{schema}/{volume}/cbioportal/{cohort}/data_clinical_{patient_or_sample}_header.txt`
- Table: `{catalog}.{schema}.data_clinical_{patient_or_sample}_header`
- Format: Tall (5 columns × N rows)
- Columns: `column_name`, `display_label`, `description`, `datatype`, `priority`

### Final Combined
- Databricks: `/Volumes/{catalog}/{schema}/{volume}/cbioportal/{cohort}/data_clinical_{patient_or_sample}.txt`
- Local: User-specified path
- Format: cBioPortal (5 header rows + N data rows)

---

## Troubleshooting

### Script 1 Errors
- **YAML not found**: Check `--config_dir` path
- **SQL errors**: Check YAML `source_table_prod`/`source_table_dev` and `columns`
- **Template column missing**: Ensure template has `PATIENT_ID` or `SAMPLE_ID`

### Script 2 Errors
- **Manifest not found**: Run Script 1 first
- **Column mismatch**: Check intermediate files have ID column

### Script 3 Errors
- **Merged data not found**: Run Script 2 first
- **YAML path invalid**: Check manifest has correct absolute paths

### Script 4 Errors
- **Header/data not found**: Run Scripts 2 & 3 first
- **Local directory doesn't exist**: Script creates it automatically
- **Column order mismatch**: Script reorders automatically

---

## Migration from Old Wrapper

**Old command**:
```bash
bash_yaml_summary_creator.sh <args> --save-to-table
```

**New approach**: Run 4 scripts sequentially (see examples above)

**Advantages**:
- Can debug each step
- Can re-merge without re-processing YAMLs
- Tables persisted for queries
- Local + Databricks outputs

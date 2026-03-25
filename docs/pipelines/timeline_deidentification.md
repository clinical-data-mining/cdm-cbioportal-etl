# Timeline Deidentification Scripts

This directory contains scripts for deidentifying timeline data from various domains (medications, labs, diagnoses, etc.) for cBioPortal.

## Files

- **`cbioportal_timeline_deidentify.py`** - Self-contained generic timeline deidentification script (works for medications, labs, diagnoses, etc.)
  - All utility functions are included within this script (no external dependencies beyond installed packages)

## Shared Components

### Fixed Upstream (Configured in code)
- `FNAME_DEMO`: Demographics table (`cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics`)
- `FNAME_DEID`: Anchor dates table (`cdsi_prod.cdm_idbw_impact_pipeline_prod.timeline_anchor_dates`)
- `prod_or_dev`: Environment (configured upstream)
- `cohort`: Cohort name (configured upstream)

### Configurable via Arguments
- `--fname_dbx`: Path to Databricks environment file
- `--fname_timeline`: Source Databricks table for timeline data
- `--fname_sample`: Path to sample list file
- `--fname_output_volume`: Output path for PHI version (Databricks volume)
- `--fname_output_gpfs`: Output path for deidentified version (GPFS)
- `--columns_cbio`: Comma-separated list of cBioPortal columns
- `--truncate_by_os_date`: Flag to enable date truncation

## Usage

### Direct Python Execution

```bash
python pipeline/timeline/cbioportal_timeline_deidentify.py \
  --fname_dbx=/path/to/databricks_env.txt \
  --fname_timeline=cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_medications \
  --fname_sample=/path/to/data_clinical_sample.txt \
  --fname_output_volume=/Volumes/cdsi_prod/path/data_timeline_treatment_phi.tsv \
  --fname_output_gpfs=/gpfs/path/data_timeline_treatment.txt \
  --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,TREATMENT_TYPE,AGENT,RX_INVESTIGATIVE" \
  --truncate_by_os_date
```

### Via Bash Script

Use the generic bash executor:

```bash
bash pipeline/bash/bash_timeline_deid_generic.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  /path/to/databricks_env.txt \
  pipeline/timeline/cbioportal_timeline_deidentify.py \
  --fname_timeline=cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_medications \
  --fname_sample=/path/to/data_clinical_sample.txt \
  --fname_output_volume=/Volumes/cdsi_prod/path/data_timeline_treatment_phi.tsv \
  --fname_output_gpfs=/gpfs/path/data_timeline_treatment.txt \
  --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,TREATMENT_TYPE,AGENT,RX_INVESTIGATIVE" \
  --truncate_by_os_date
```

**Bash Script Arguments:**
1. `ROOT_PATH_REPO` - Repository root path
2. `CONDA_INSTALL_PATH` - Conda installation path
3. `CONDA_ENV_NAME` - Conda environment name
4. `FNAME_DBX` - Databricks environment file
5. `PATH_SCRIPT` - Path to Python script (relative to repo root)
6. `...` - All remaining arguments are passed to the Python script

### Real Example

```bash
bash pipeline/bash/bash_timeline_deid_generic.sh \
  /gpfs/mindphidata/cdm_repos/prod/github/cdm-cbioportal-etl \
  /gpfs/mindphidata/fongc2/miniconda3 \
  cdm-cbioportal-etl \
  /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
  pipeline/timeline/cbioportal_timeline_deidentify.py \
  --fname_timeline=cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_medications \
  --fname_sample=/gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_sample.txt \
  --fname_output_volume=/Volumes/cdsi_prod/cdm_idbw_impact_pipeline_prod/epic_testing/mskimpact/data_timeline_treatment_phi.tsv \
  --fname_output_gpfs=/gpfs/mindphidata/cdm_repos/prod/data/cdm-data/mskimpact/data_timeline_treatment.txt \
  --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,TREATMENT_TYPE,AGENT,RX_INVESTIGATIVE" \
  --truncate_by_os_date
```

## Date Truncation

The `--truncate_by_os_date` flag controls whether dates are truncated to not exceed the patient's OS_DATE:

- **With flag**:
  - `START_DATE > OS_DATE` → `START_DATE = OS_DATE`
  - `STOP_DATE > OS_DATE` → `STOP_DATE = OS_DATE`
- **Without flag**: Dates are not modified

**When to use:**
- **Medications**: YES (use `--truncate_by_os_date`)
- **Labs**: Depends on use case
- **Imaging**: Depends on use case
- **Diagnoses**: Probably not needed

## Processing Steps

Each timeline deidentification script follows these steps:

1. Load sample list and extract patient/sample IDs
2. Compute OS dates from demographics table
3. Load anchor dates (sequencing dates)
4. Load timeline raw data from Databricks
5. Validate date parsing (check for coercion errors)
6. Merge all data together
7. Optionally truncate dates by OS_DATE
8. Calculate deidentified dates (days from anchor)
9. Save PHI version to Databricks volume
10. Save deidentified version to GPFS

## Output Files

### PHI Version (Databricks Volume)
Contains all original data plus deidentified dates:
- Original: `MRN`, `START_DATE`, `STOP_DATE`
- Deidentified: `PATIENT_ID`, `START_DATE_DEID`, `STOP_DATE_DEID`
- Additional columns preserved

**Purpose:** Quality control and validation

### Deidentified Version (GPFS)
Contains only deidentified data:
- `PATIENT_ID`, `START_DATE` (days), `STOP_DATE` (days)
- Additional non-PHI columns
- No MRN or original dates

**Purpose:** Upload to cBioPortal

## Using for Different Data Domains

The `cbioportal_timeline_deidentify.py` script is generic and works for all timeline data domains.
Simply change the input table and columns for each domain:

**Medications:**
```bash
bash pipeline/bash/bash_timeline_deid_generic.sh \
  /path/to/repo /path/to/conda cdm-cbioportal-etl /path/to/databricks_env.txt \
  pipeline/timeline/cbioportal_timeline_deidentify.py \
  --fname_timeline=schema.table_timeline_medications \
  --fname_sample=/path/to/data_clinical_sample.txt \
  --fname_output_volume=/Volumes/path/data_timeline_treatment_phi.tsv \
  --fname_output_gpfs=/gpfs/path/data_timeline_treatment.txt \
  --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,TREATMENT_TYPE,AGENT,RX_INVESTIGATIVE" \
  --truncate_by_os_date
```

**Labs:**
```bash
bash pipeline/bash/bash_timeline_deid_generic.sh \
  /path/to/repo /path/to/conda cdm-cbioportal-etl /path/to/databricks_env.txt \
  pipeline/timeline/cbioportal_timeline_deidentify.py \
  --fname_timeline=schema.table_timeline_labs \
  --fname_sample=/path/to/data_clinical_sample.txt \
  --fname_output_volume=/Volumes/path/data_timeline_lab_test_phi.tsv \
  --fname_output_gpfs=/gpfs/path/data_timeline_lab_test.txt \
  --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,TEST,RESULT"
```

**Diagnoses:**
```bash
bash pipeline/bash/bash_timeline_deid_generic.sh \
  /path/to/repo /path/to/conda cdm-cbioportal-etl /path/to/databricks_env.txt \
  pipeline/timeline/cbioportal_timeline_deidentify.py \
  --fname_timeline=schema.table_timeline_diagnoses \
  --fname_sample=/path/to/data_clinical_sample.txt \
  --fname_output_volume=/Volumes/path/data_timeline_diagnosis_phi.tsv \
  --fname_output_gpfs=/gpfs/path/data_timeline_diagnosis.txt \
  --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,DIAGNOSIS_CODE,DIAGNOSIS_DESCRIPTION"
```

## Troubleshooting

**Issue: "Missing columns in output"**
- Check that `--columns_cbio` matches your source data columns
- The script will warn about missing columns but continue

**Issue: "Databricks connection failed"**
- Verify `--fname_dbx` points to valid environment file
- Check Databricks credentials

**Issue: "High percentage of date coercion"**
- Check source data date formats
- Some null dates are expected
- Script reports this for awareness

## Statistics Reported

During execution, the script reports:
- Number of sample/patient IDs
- Date parsing validation (coercion rates)
- Number of patients with missing data
- Number of dates truncated (if enabled)
- Final row count in deidentified output

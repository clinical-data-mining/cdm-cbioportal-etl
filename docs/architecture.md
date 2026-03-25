# Architecture Overview

This document describes the architecture of the cdm-cbioportal-etl pipeline.

## System Design

The cdm-cbioportal-etl pipeline is a **YAML-based, modular ETL system** that transforms clinical data from Databricks tables into cBioPortal-formatted files.

### Key Design Principles

1. **Configuration over Code** - Data sources defined in YAML, not hardcoded
2. **Modularity** - Independent scripts that can be run separately
3. **Transparency** - Clear data flow with inspectable intermediate files
4. **Dual Output** - Write to both Databricks volumes (for QC) and local filesystem (for cBioPortal)
5. **De-identification** - All dates converted to intervals from anchor date

## Architecture Diagram

```
Source Data (Databricks)
    ↓
YAML Configurations
    ├── summaries/ (14 configs)
    └── timelines/ (21 configs)
    ↓
Pipeline Processing
    ├── Summary Pipeline (4 modular scripts)
    └── Timeline Pipeline (batch deidentification)
    ↓
Intermediate Files (Databricks Volumes)
    ├── Individual summaries
    ├── Headers
    └── PHI timeline files (for QC)
    ↓
Final Output (Dual)
    ├── Databricks Volumes → data_clinical_*.txt
    └── Local Filesystem → /gpfs/path/data_clinical_*.txt
    ↓
cBioPortal Import
```

## Data Storage

### Legacy vs. Current

| Aspect | Legacy System | Current System |
|--------|---------------|----------------|
| Storage | MinIO object storage | Databricks volumes |
| Configuration | Google Sheets codebook | YAML files |
| Processing | Monolithic scripts | Modular pipeline |
| Dependencies | MinioAPI, codebook parser | DatabricksAPI, YAML |

### Databricks Volumes

Data is stored in Databricks volumes with this structure:

```
/Volumes/{catalog}/{schema}/{volume}/cbioportal/
├── intermediate_files/{cohort}/
│   ├── demographics_summary.tsv
│   ├── specimen_summary.tsv
│   ├── manifest_patient.csv
│   └── manifest_sample.csv
├── {cohort}/
│   ├── data_clinical_patient_data.txt
│   ├── data_clinical_patient_header.txt
│   ├── data_clinical_patient.txt        # Final patient summary
│   ├── data_clinical_sample_data.txt
│   ├── data_clinical_sample_header.txt
│   ├── data_clinical_sample.txt         # Final sample summary
│   ├── data_timeline_treatment_phi.tsv  # PHI version (QC)
│   ├── data_timeline_diagnosis_phi.tsv
│   └── ...
└── {cohort}_local/
    ├── data_clinical_patient.txt         # Copied to local filesystem
    ├── data_clinical_sample.txt
    ├── data_timeline_treatment.txt       # De-identified for cBioPortal
    ├── data_timeline_diagnosis.txt
    └── ...
```

## Component Architecture

### 1. Configuration Layer

**Location:** `config/`

- **`summaries/*.yaml`** - 14 summary configurations (demographics, biomarkers, predictions)
- **`timelines/*.yaml`** - 21 timeline configurations (medications, labs, diagnoses, imaging)
- **`etl_config_*.yml`** - Cohort-specific settings (mskimpact, mskaccess, etc.)
- **`cbioportal_headers/`** - Header templates

Each YAML defines:
- Source table (prod and dev)
- Columns to extract
- Metadata (labels, datatypes, descriptions)
- Output destinations

### 2. Processing Layer

**Location:** `pipeline/`

#### Summary Pipeline (4 Scripts)

**Modular design** - each script performs one task:

1. **`create_intermediate_summaries.py`**
   - Input: YAML configs + source tables
   - Processing: Extract data, merge with anchor dates, deidentify dates
   - Output: Individual TSV files (one per YAML)

2. **`merge_intermediate_summaries.py`**
   - Input: Manifest of intermediate files
   - Processing: Horizontal merge (join on patient/sample ID)
   - Output: Single data file + Databricks table

3. **`create_summary_header.py`**
   - Input: YAML metadata + merged data
   - Processing: Extract headers, align with data columns
   - Output: Header file (tall format) + Databricks table

4. **`combine_header_and_data.py`**
   - Input: Header + data files
   - Processing: Transpose header, prepend to data
   - Output: Final cBioPortal file (volume + local)

#### Timeline Pipeline

**Batch processing** - processes all timelines at once:

- **`cbioportal_timeline_batch_deidentify.py`**
  - Input: YAML configs, sample list, anchor dates
  - Processing: Deidentify dates, truncate by OS_DATE, filter to cohort samples
  - Output: PHI files (volume) + deidentified files (local)

### 3. Library Layer

**Location:** `pipeline/lib/`

Core classes and utilities:

- **`summary/`**
  - `SummaryConfigProcessor` - Process individual YAML config
  - `SummaryMerger` - Merge intermediate files
  - Legacy classes (archived)

- **`utils/`**
  - `get_anchor_dates()` - Load deidentification anchor dates
  - `age_at_sequencing.py` - Calculate age columns
  - `sequencing_date.py` - Handle sequencing dates
  - `cbioportal_update_config.py` - Config helpers

### 4. Wrapper Layer

**Location:** `pipeline/bash/`

20 bash scripts for common workflows:

- `bash_summary_modular_pipeline.sh` - Run full summary pipeline
- `bash_timeline_batch_deid.sh` - Run all timeline deidentification
- `bash_monitor_completeness.sh` - Validate output files
- Individual utility wrappers

## Data Flow

### Summary Data Flow

```
1. Source Tables (Databricks)
   ↓ (SQL query from YAML)
2. Raw Data Extract
   ↓ (merge with anchor dates)
3. Deidentified Data + Metadata
   ↓ (save intermediate TSV)
4. Individual Summary Files
   ↓ (horizontal merge on ID)
5. Combined Data File
   ↓ (extract headers from YAML)
6. Header Metadata
   ↓ (transpose and combine)
7. Final cBioPortal File (5 header rows + N data rows)
   ↓ (dual write)
8. Databricks Volume + Local Filesystem
```

### Timeline Data Flow

```
1. Source Timeline Tables (Databricks)
   ↓ (YAML config specifies table and columns)
2. Raw Timeline Data (with PHI dates)
   ↓ (filter to cohort samples)
3. Cohort Timeline Data
   ↓ (merge with anchor dates)
4. Anchor Date Merge
   ↓ (calculate days from anchor)
5. Deidentified Dates
   ↓ (optional: truncate by OS_DATE)
6. Date Truncation
   ↓ (dual write)
7. PHI File (volume, for QC) + Deidentified File (local, for cBioPortal)
```

## De-identification Strategy

All dates are converted to **days from anchor date** (sequencing date).

### Anchor Dates

**Table:** Configured per environment (e.g., `cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates`)

**Structure:**
```
MRN | ANCHOR_DATE
----|------------
123 | 2020-05-15
456 | 2021-03-20
```

### Date Transformation

```
Original Date: 2020-06-20
Anchor Date:   2020-05-15
              ↓
Deidentified:  36 (days)
```

### Date Truncation (Timeline Only)

Optional truncation ensures events don't extend beyond patient's last known date:

```
if STOP_DATE > OS_DATE:
    STOP_DATE = OS_DATE
```

Use `--truncate_by_os_date` flag for medications, optional for other timelines.

## Output Format

### cBioPortal Summary File Format

5 header rows + N data rows:

```
#Display Name    Sex         Age at Sequencing
#Description     Patient...  Age at time...
#Datatype        #STRING     NUMBER
#Priority        #1          1
PATIENT_ID       SEX         AGE_AT_SEQ
P-0001           Female      65
P-0002           Male        52
```

### cBioPortal Timeline File Format

Standard tab-delimited with required columns:

```
PATIENT_ID  START_DATE  STOP_DATE  EVENT_TYPE  SUBTYPE  ...
P-0001      0           0          DIAGNOSIS   Primary  ...
P-0001      45          90         TREATMENT   Systemic ...
```

## Cohort Support

The pipeline supports multiple cohorts simultaneously:

| Cohort | Config File | Description |
|--------|-------------|-------------|
| mskimpact | `config/etl_config_mskimpact.yml` | MSK-IMPACT solid tumor |
| mskimpact_heme | `config/etl_config_mskimpact_heme.yml` | MSK-IMPACT hematologic |
| mskaccess | `config/etl_config_mskaccess.yml` | MSK-ACCESS liquid biopsy |
| mskarcher | `config/etl_config_mskarcher.yml` | MSK-ARCHER fusion panel |

Each cohort has its own:
- Sample list (template file)
- Output directories (Databricks + local)
- Potentially different source tables (dev vs. prod)

## Scalability

The architecture is designed to scale:

- **Adding data sources:** Create new YAML config (no code changes)
- **Adding cohorts:** Add config file and run pipeline
- **Debugging:** Run individual scripts independently
- **Reprocessing:** Re-run specific steps without full pipeline
- **Parallel processing:** Scripts can process patient and sample summaries concurrently

## Migration from Legacy System

Key changes:

| Component | Old | New | Impact |
|-----------|-----|-----|--------|
| Storage | MinIO | Databricks volumes | Change all I/O code |
| Config | Google Sheets codebook | YAML files | Simplified, version-controlled |
| Processing | Monolithic wrappers | Modular scripts | Independent execution, debugging |
| Dependencies | codebook parser | YAML parser | Simpler dependencies |
| Monitoring | SFTP sensor | Exit codes | Simpler orchestration |

See [Migration Guide](archive/MIGRATION_FROM_OLD_SYSTEM.md) for details.

## Technology Stack

- **Language:** Python 3.11
- **Data Processing:** Pandas, PySpark (via Databricks)
- **Configuration:** YAML
- **Storage:** Databricks volumes, local filesystem
- **Orchestration:** Bash scripts, Airflow DAGs (optional)
- **Dependencies:** msk_cdm package (custom internal library)

## Next Steps

- [Installation Guide](installation.md) - Set up your environment
- [Running the Full ETL Pipeline](guides/running_full_etl.md) - Execute the pipeline
- [Adding New Data](guides/adding_new_summary_data.md) - Extend the pipeline

# cdm-cbioportal-etl

ETL processing to transform CDM data into cBioPortal formatted files using a YAML-based, modular pipeline architecture with Databricks integration.

## Table of Contents
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Architecture Overview](#architecture-overview)
- [Adding New Data](#adding-new-data)
- [Running the Pipeline](#running-the-pipeline)
- [Documentation](#documentation)

## Quick Start

**For existing users:** This repository has been refactored to use YAML-based configuration with Databricks volumes. See [Migration Guide](./docs/archive/MIGRATION_FROM_OLD_SYSTEM.md) if you're familiar with the old codebook-based approach.

**New users:**
1. Install dependencies: `conda env create -f environment.yml && conda activate cdm-cbioportal-etl`
2. Configure Databricks connection (see [Installation](#installation))
3. Add data by creating YAML configs (see [Adding New Data](#adding-new-data))
4. Run pipelines (see [Running the Pipeline](#running-the-pipeline))

## Installation

### 1. Create Conda Environment
```bash
conda env create -f environment.yml
conda activate cdm-cbioportal-etl
```

Or install directly into your existing environment:
```bash
pip install .
```

### 2. Configure Databricks Connection

Create a Databricks environment file with your credentials:
```bash
# Example: databricks_env.txt
DATABRICKS_HOST=your-databricks-host
DATABRICKS_TOKEN=your-token
```

See [Installation Guide](./docs/installation.md) for detailed setup instructions.

## Architecture Overview

This ETL pipeline transforms clinical data from Databricks tables into cBioPortal-formatted files.

### Key Components

**Data Storage:** Databricks volumes (replaces legacy MinIO)
**Configuration:** YAML files for each data source (replaces codebook-based approach)
**Processing:** Modular, independently-runnable scripts
**Output:** Dual output to both Databricks volumes and local filesystem

### File Types

cBioPortal clinical data comes in 3 types:

1. **Patient Summary** - Patient-level information (demographics, overall survival)
2. **Sample Summary** - Sample-level information (tumor type, biomarkers)
3. **Timeline** - Temporal events (treatments, diagnoses, lab tests)

### Current Configuration
- **14 summary configurations** in `config/summaries/` (demographics, biomarkers, predictions, etc.)
- **21 timeline configurations** in `config/timelines/` (medications, labs, diagnoses, imaging, etc.)
- **4 cohort configs** in `config/` (mskimpact, mskaccess, mskarcher, mskimpact_heme)

## Adding New Data

### Adding a New Summary Column

1. **Create a YAML configuration** in `config/summaries/`:

```yaml
# config/summaries/my_biomarker.yaml
summary_id: my_biomarker
patient_or_sample: sample  # or 'patient'
source_table_prod: catalog.schema.biomarker_table
source_table_dev: catalog_dev.schema.biomarker_table_dev
key_column: SAMPLE_ID  # or 'MRN' for patient
columns:
  - SAMPLE_ID
  - BIOMARKER_STATUS
  - BIOMARKER_VALUE
date_columns: []

column_metadata:
  BIOMARKER_STATUS:
    label: "Biomarker Status"
    datatype: STRING
    comment: "Status of biomarker (Positive/Negative)"
    fill_value: "Unknown"
```

2. **Run the pipeline** - your new summary will be automatically included

See [Adding Summary Data Guide](./docs/guides/adding_new_summary_data.md) for complete details.

### Adding a New Timeline

1. **Create a YAML configuration** in `config/timelines/`:

```yaml
# config/timelines/my_events.yaml
timeline_id: my_events
source_table_prod: catalog.schema.events_table
source_table_dev: catalog_dev.schema.events_table_dev
output_filename: data_timeline_my_events
columns:
  PATIENT_ID:
    field_name: PATIENT_ID
    field_label: Patient ID
    nlp_derived: false
  START_DATE:
    field_name: START_DATE
    field_label: Event Start Date
    nlp_derived: false
```

2. **Run the timeline deidentification pipeline**

See [Adding Timeline Data Guide](./docs/guides/adding_new_timeline_data.md) for complete details.

## Running the Pipeline

### Summary Pipeline (Modular 4-Script Approach)

The summary pipeline consists of 4 independent scripts:

```bash
# 1. Create intermediate summaries from YAML configs
python pipeline/summary/create_intermediate_summaries.py \
  --config_dir config/summaries \
  --databricks_env /path/to/databricks_env.txt \
  --anchor_dates catalog.schema.anchor_dates_table \
  --template /path/to/template.txt \
  --patient_or_sample patient \
  --production_or_test production \
  --cohort mskimpact \
  --output_manifest /Volumes/path/manifest_patient.csv

# 2. Merge intermediates into single data file
python pipeline/summary/merge_intermediate_summaries.py \
  --manifest /Volumes/path/manifest_patient.csv \
  --databricks_env /path/to/databricks_env.txt \
  --template /path/to/template.txt \
  --patient_or_sample patient \
  --output_volume_path /Volumes/path/data_clinical_patient_data.txt \
  --output_catalog catalog \
  --output_schema schema \
  --output_table data_clinical_patient

# 3. Create header metadata from YAMLs
python pipeline/summary/create_summary_header.py \
  --manifest /Volumes/path/manifest_patient.csv \
  --databricks_env /path/to/databricks_env.txt \
  --merged_data_path /Volumes/path/data_clinical_patient_data.txt \
  --patient_or_sample patient \
  --output_volume_path /Volumes/path/data_clinical_patient_header.txt \
  --output_catalog catalog \
  --output_schema schema \
  --output_table data_clinical_patient_header

# 4. Combine header and data into final cBioPortal file
python pipeline/summary/combine_header_and_data.py \
  --header_volume_path /Volumes/path/data_clinical_patient_header.txt \
  --data_volume_path /Volumes/path/data_clinical_patient_data.txt \
  --databricks_env /path/to/databricks_env.txt \
  --output_volume_path /Volumes/path/data_clinical_patient.txt \
  --output_local_path /local/path/data_clinical_patient.txt
```

Or use the bash wrapper:
```bash
bash pipeline/bash/bash_summary_modular_pipeline.sh [args...]
```

See [Modular Summary Pipeline Guide](./docs/pipelines/summary_modular_pipeline.md) for details.

### Timeline Deidentification

Process all timeline files at once using batch deidentification:

```bash
bash pipeline/bash/bash_timeline_batch_deid.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  config/timelines \
  production \
  /path/to/databricks_env.txt \
  catalog.schema.anchor_dates_table \
  /path/to/data_clinical_sample.txt \
  /Volumes/base/path \
  /local/output/path \
  mskimpact
```

See [Timeline Deidentification Guide](./docs/pipelines/timeline_deidentification.md) for details.

### Complete ETL Workflow

For a full end-to-end ETL run, see [Running the Full ETL Pipeline](./docs/guides/running_full_etl.md).

## Documentation

### User Guides
- [Installation Guide](./docs/installation.md)
- [Adding New Summary Data](./docs/guides/adding_new_summary_data.md)
- [Adding New Timeline Data](./docs/guides/adding_new_timeline_data.md)
- [Running the Full ETL Pipeline](./docs/guides/running_full_etl.md)
- [Troubleshooting Guide](./docs/guides/troubleshooting.md)

### Pipeline Documentation
- [Modular Summary Pipeline](./docs/pipelines/summary_modular_pipeline.md) - 4-script modular approach
- [Timeline Deidentification](./docs/pipelines/timeline_deidentification.md) - Batch processing with YAML configs
- [Monitoring & Validation](./docs/pipelines/monitoring.md)

### Configuration Reference
- [Summary YAML Format](./docs/configuration/summary_yaml_format.md)
- [Timeline YAML Format](./docs/configuration/timeline_yaml_format.md)
- [Cohort Configuration](./docs/configuration/cohort_configs.md)

### Reference
- [Data File Formatting Requirements](./docs/data_file_formatting.md)
- [Bash Scripts Reference](./docs/reference/bash_scripts.md)
- [Utility Scripts Reference](./docs/reference/utility_scripts.md)

### Legacy Documentation
- [Migration Guide from Old System](./docs/archive/MIGRATION_FROM_OLD_SYSTEM.md)
- [Archived Documentation](./docs/archive/) - Old codebook-based approach

## Support

For questions or issues:
- Check the [Troubleshooting Guide](./docs/guides/troubleshooting.md)
- Review the relevant documentation above
- File an issue in the repository





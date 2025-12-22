# cBioPortal Summary Configuration Files

This directory contains YAML configuration files for generating cBioPortal summary files using the new config-based pipeline.

## Overview

The refactored summary processing pipeline uses individual YAML files to configure each summary table. This approach is:
- **Modular**: Each summary is processed independently
- **Transparent**: All configuration is explicit in YAML files
- **Simplified**: No codebook parsing or interactive features
- **Maintainable**: Easy to add, remove, or modify summaries

## Directory Structure

```
pipeline/
├── config/
│   └── summaries/
│       ├── README.md                    # This file
│       ├── demographics.yaml            # Patient demographics summary
│       ├── specimen.yaml                # Sample specimen summary
│       └── ...                          # More summary configs
├── lib/
│   └── summary/
│       ├── summary_config_processor.py  # Processes one summary from YAML
│       ├── summary_merger.py            # Merges intermediate files
│       └── ...
├── summary/
│   └── wrapper_cbioportal_summary_creator_v2.py  # Main wrapper script
└── utils/
    └── codebook_to_yaml_converter.py    # Convert codebooks to YAML
```

## YAML Configuration Format

Each summary is defined in a single YAML file with the following structure:

```yaml
# Summary Configuration
summary_id: demographics              # Unique identifier
patient_or_sample: patient            # 'patient' or 'sample' level

# Source Tables
source_table_prod: cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics
source_table_dev: cdsi_dev.cdm_impact_pipeline_dev.t01_epic_ddp_demographics

# Data Extraction
key_column: MRN                       # Column to join with anchor dates
columns:                              # Columns to extract
  - MRN
  - GENDER
  - RACE
  - ETHNICITY

date_columns:                         # Columns to convert to intervals
  - BIRTH_DATE

# Destination Configuration
dest_prod:
  catalog: cdsi_prod
  schema: cdsi_data_deid
  volume_name: cdsi_data_deid_volume
  filename: demographics_summary.tsv

dest_dev:
  catalog: cdsi_dev
  schema: cdsi_data_deid_dev
  volume_name: cdsi_data_deid_volume_dev
  filename: demographics_summary.tsv

# Column Metadata
column_metadata:
  GENDER:
    label: "Sex"
    datatype: STRING
    comment: "Patient sex ---DESCRIPTION: ... ---SOURCE: ..."
    fill_value: "NA"

  RACE:
    label: "Race"
    datatype: STRING
    comment: "Patient race ---DESCRIPTION: ... ---SOURCE: ..."
    fill_value: "NA"

  # ... more columns
```

## Workflow

### 1. Generate YAML Files from Codebook (One-Time Setup)

Convert your existing codebook CSV files to YAML configuration files:

```bash
python pipeline/utils/codebook_to_yaml_converter.py \
    --metadata sandbox/CDM-Codebook\ -\ metadata.csv \
    --tables sandbox/CDM-Codebook\ -\ tables.csv \
    --output_dir pipeline/config/summaries \
    --catalog_prod cdsi_prod \
    --schema_prod cdsi_data_deid \
    --volume_prod cdsi_data_deid_volume \
    --catalog_dev cdsi_dev \
    --schema_dev cdsi_data_deid_dev \
    --volume_dev cdsi_data_deid_volume_dev
```

This will create one YAML file per summary table with both production and dev source tables included.

### 2. Create Summary Files

Run the wrapper script to process all summaries and create final files:

```bash
python pipeline/summary/wrapper_cbioportal_summary_creator_v2.py \
    --config_dir pipeline/config/summaries \
    --databricks_env /path/to/databricks.env \
    --template_patient /Volumes/.../patient_template.tsv \
    --template_sample /Volumes/.../sample_template.tsv \
    --output_patient /Volumes/.../data_clinical_patient.tsv \
    --output_sample /Volumes/.../data_clinical_sample.tsv \
    --production_or_test production \
    --cohort mskimpact
```

### 3. Process Individual Summaries (Optional)

You can also process summaries individually for testing/debugging:

```python
from lib.summary import SummaryConfigProcessor
from lib.utils import get_anchor_dates
from msk_cdm.databricks import DatabricksAPI

# Load anchor dates and template
df_anchor = get_anchor_dates(fname_databricks_env)
obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)
df_template = obj_db.read_db_obj(volume_path=template_path, sep='\t')
df_template = df_template.iloc[4:].reset_index(drop=True)  # Skip header rows

# Process one summary
processor = SummaryConfigProcessor(
    fname_yaml_config='pipeline/config/summaries/demographics.yaml',
    fname_databricks_env=fname_databricks_env,
    production_or_test='production',
    cohort='mskimpact'
)

df_header, df_data = processor.process_summary(df_anchor, df_template)
processor.save_intermediate(df_header, df_data)
```

## Processing Flow

The new pipeline follows these explicit steps:

### Step A: Create Intermediate Files

For each YAML config:
1. Load source table from Databricks
2. Subset to specified columns
3. Merge with anchor dates (for deidentification)
4. Convert date columns to intervals
5. Merge with template (ensure all patients/samples included)
6. Backfill missing data with fill values
7. Save intermediate file with header

### Step B: Create Manifest

Track all intermediate files created.

### Step C: Merge Intermediates

Merge all intermediate files horizontally:
1. Start with template (ID column)
2. Add columns from each intermediate file
3. Replace duplicates if they exist

### Step D: Create Header

Combine headers from all intermediate files, aligning with data columns.

### Step E: Merge Header and Data

Concatenate header (4 rows) and data into final cBioPortal format.

## Key Differences from Old Pipeline

| Aspect | Old Pipeline | New Pipeline |
|--------|--------------|--------------|
| Configuration | Codebook CSV files | YAML files per summary |
| Processing | All-at-once, opaque | Modular, one-at-a-time |
| Metadata | Parsed from codebook | Explicit in YAML |
| Merging | Interactive features | Simplified, explicit |
| Inspection | Limited | Easy to inspect intermediates |
| Debugging | Difficult | Process summaries individually |

## Adding a New Summary

1. Create a new YAML file in `pipeline/config/summaries/`
2. Follow the format above
3. Run the wrapper script - it will automatically pick up the new config

Example for a new "genomic_alterations.yaml":

```yaml
summary_id: genomic_alterations
patient_or_sample: sample

source_table_prod: cdsi_prod.genomics.alterations
source_table_dev: cdsi_dev.genomics.alterations_dev

key_column: SAMPLE_ID
columns:
  - SAMPLE_ID
  - GENE
  - ALTERATION
  - VARIANT_TYPE

date_columns: []

dest_prod:
  catalog: cdsi_prod
  schema: cdsi_data_deid
  volume_name: cdsi_data_deid_volume
  filename: genomic_alterations_summary.tsv

dest_dev:
  catalog: cdsi_dev
  schema: cdsi_data_deid_dev
  volume_name: cdsi_data_deid_volume_dev
  filename: genomic_alterations_summary.tsv

column_metadata:
  GENE:
    label: "Gene Symbol"
    datatype: STRING
    comment: "HUGO gene symbol"
    fill_value: "NA"
  # ... more columns
```

## Troubleshooting

### Issue: Summary not being processed

**Solution**: Check that:
- YAML file is in the config directory
- `patient_or_sample` matches the level you're processing
- Source table paths are correct

### Issue: Missing columns in output

**Solution**: Verify:
- Columns exist in source table
- Columns are listed in `columns` section of YAML
- Column names match exactly (case-sensitive)

### Issue: Date conversion errors

**Solution**: Ensure:
- Date columns are listed in `date_columns`
- Source data has valid date formats
- Anchor dates are loaded correctly

## Files Generated

The pipeline creates:

**Intermediate files** (one per summary):
```
/Volumes/{catalog}/{schema}/{volume}/cbioportal/intermediate_files/{cohort}/{summary_id}_summary.tsv
```

**Final summary files**:
```
{output_patient}  # e.g., data_clinical_patient.tsv
{output_sample}   # e.g., data_clinical_sample.tsv
```

## Support

For questions or issues:
1. Check this README
2. Review YAML file format
3. Test individual summary processing
4. Check logs for detailed error messages

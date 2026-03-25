# Adding New Summary Data

This guide explains how to add a new summary YAML and how that YAML turns into the final cBioPortal patient or sample summary file.

## What You Are Really Adding

When you add a new summary YAML, you are adding one or more columns to one of these final files:

- `data_clinical_patient.txt`
- `data_clinical_sample.txt`

You are not creating a full summary file from scratch. The pipeline starts from the cohort template and adds your columns onto those existing rows.

## Mental Model

Use this model when deciding what to build:

- the template defines which patients or samples appear
- each summary YAML defines which columns get added
- one YAML creates one intermediate TSV
- all patient intermediates are merged into the patient summary
- all sample intermediates are merged into the sample summary

## Choose The Right Target First

Use `patient_or_sample: patient` when the value is one row per patient.

Examples:

- demographics
- overall survival
- patient-level comorbidity scores

Use `patient_or_sample: sample` when the value is one row per sample.

Examples:

- sample-level biomarker calls
- specimen or pathology attributes tied to a specific sample

If you get this wrong, the YAML may still run, but it will end up in the wrong final file or fail to merge correctly.

## End-To-End Flow

```text
new summary YAML
  -> intermediate TSV for that YAML only
  -> merged with same-level intermediates
  -> header rows generated from column_metadata
  -> final data_clinical_patient.txt or data_clinical_sample.txt
```

Concretely:

1. `create_intermediate_summaries.py` creates one intermediate TSV per YAML.
2. `merge_intermediate_summaries.py` left-joins all intermediates onto the cohort template.
3. `create_summary_header.py` builds the cBioPortal header from `column_metadata`.
4. `combine_header_and_data.py` writes the final cBioPortal file.

## Before You Start

Confirm these items:

- the Databricks source table already exists
- you know whether the data is patient-level or sample-level
- the source table has a stable join key such as `MRN` or `SAMPLE_ID`
- you know the output columns you want to expose in cBioPortal
- you have labels, datatypes, descriptions, and fill values for each new column

## Step 1: Identify The Source Table

You need:

- production table name
- development table name
- join key
- list of source columns to select
- any date columns that should be deidentified into day offsets

Example:

```text
Source table: cdsi_prod.cdm_biomarkers.pdl1_results
Level: sample
Key column: SAMPLE_ID
Columns: SAMPLE_ID, PDL1_STATUS, PDL1_PERCENTAGE
Date columns: none
```

## Step 2: Create The YAML

Create a new file in `config/summaries/`.

Example:

```yaml
summary_id: pdl1_biomarker
patient_or_sample: sample

source_table_prod: cdsi_prod.cdm_biomarkers.pdl1_results
source_table_dev: cdsi_dev.cdm_biomarkers.pdl1_results_dev

key_column: SAMPLE_ID

columns:
  - SAMPLE_ID
  - PDL1_STATUS
  - PDL1_PERCENTAGE

date_columns: []

dest_prod:
  catalog: cdsi_eng_phi
  schema: cdm_eng_cbioportal_etl
  volume_name: cdm_eng_cbioportal_etl_volume
  filename: pdl1_biomarker_summary.tsv

dest_dev:
  catalog: cdsi_eng_phi
  schema: cdm_eng_cbioportal_etl
  volume_name: cdm_eng_cbioportal_etl_volume_dev
  filename: pdl1_biomarker_summary.tsv

column_metadata:
  PDL1_STATUS:
    label: "PD-L1 Status"
    datatype: STRING
    comment: "PD-L1 Status ---DESCRIPTION: PD-L1 immunohistochemistry result ---MISSING DATA: Not tested or result unavailable ---SOURCE: Pathology reports"
    fill_value: "Not Tested"

  PDL1_PERCENTAGE:
    label: "PD-L1 Percentage"
    datatype: NUMBER
    comment: "PD-L1 Percentage ---DESCRIPTION: Percentage of tumor cells expressing PD-L1 ---MISSING DATA: Not tested or result unavailable ---SOURCE: Pathology reports"
    fill_value: "NA"
```

## Step 3: Understand What This YAML Will Produce

Using the example above:

### Intermediate output

The first summary script creates one intermediate file for this YAML only:

```text
pdl1_biomarker_summary.tsv

SAMPLE_ID | PATIENT_ID | PDL1_STATUS | PDL1_PERCENTAGE
```

This file is not the final cBioPortal output. It is one input into the sample summary merge step.

### Merged sample data

The merge step starts from the sample template and left-joins your intermediate file with the other sample summary intermediates:

```text
data_clinical_sample_data.txt

SAMPLE_ID | PATIENT_ID | CANCER_TYPE | PDL1_STATUS | PDL1_PERCENTAGE | ...
```

### Final cBioPortal sample file

The header-generation step uses `column_metadata` to create label, description, datatype, and priority rows. Then those header rows are prepended to the merged data:

```text
data_clinical_sample.txt
```

That is the file that cBioPortal consumes.

## Step 4: Validate The YAML

You can at least verify the YAML parses:

```bash
python -c "import yaml; yaml.safe_load(open('config/summaries/pdl1_biomarker.yaml'))"
```

You should also verify that:

- every column listed in `columns` exists in the source table
- every non-key output column has a `column_metadata` entry
- `patient_or_sample` matches the real grain of the data

## Step 5: Run The Summary Pipeline

The summary wrapper can build both patient and sample outputs. Your new YAML is picked up automatically as long as it is in `config/summaries/`.

```bash
bash pipeline/bash/bash_summary_modular_pipeline.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  config/summaries \
  /path/to/databricks_env.txt \
  catalog.schema.anchor_dates_table \
  /path/to/data_clinical_patient_template.txt \
  /path/to/data_clinical_sample_template.txt \
  /Volumes/output/base/cbioportal \
  /local/output/cohort \
  catalog \
  schema \
  production \
  mskimpact \
  --patient \
  --sample
```

If your YAML is patient-level only, it still lives in the same config directory. The pipeline will filter it by `patient_or_sample` during execution.

## Step 6: Verify The Result

Check the final file for your new columns:

```bash
head -5 /local/output/cohort/data_clinical_sample.txt
```

What to verify:

- row 5 contains your physical column names
- metadata rows above it use the label and description you configured
- downstream data rows contain values or the expected `fill_value`

## Common Patterns

### Patient-Level Example

Use a patient summary when the source is one row per patient or can be reduced to one row per patient.

Example pattern:

```yaml
patient_or_sample: patient
key_column: MRN
```

Final destination:

```text
data_clinical_patient.txt
```

### Sample-Level Example

Use a sample summary when the data is tied to a specific tumor sample or accessioned specimen.

Example pattern:

```yaml
patient_or_sample: sample
key_column: SAMPLE_ID
```

Final destination:

```text
data_clinical_sample.txt
```

## Date Columns

If your source table includes date columns that should be deidentified, include them in `date_columns`.

Example:

```yaml
columns:
  - MRN
  - LAST_CONTACT_DATE
  - OS_STATUS

date_columns:
  - LAST_CONTACT_DATE
```

Those values are converted to integer day offsets relative to `DATE_TUMOR_SEQUENCING`.

## Common Mistakes

- Putting sample-level data into a patient YAML
- Forgetting to include the key column in `columns`
- Adding a new output column but not adding `column_metadata`
- Assuming the YAML creates new rows instead of only adding columns
- Forgetting to mark date columns in `date_columns`
- Choosing a poor `fill_value` that makes missingness hard to interpret

## Useful References

- [Summary YAML Format](../configuration/summary_yaml_format.md)
- [Modular Summary Pipeline](../pipelines/summary_modular_pipeline.md)
- [Adding New Timeline Data](./adding_new_timeline_data.md)

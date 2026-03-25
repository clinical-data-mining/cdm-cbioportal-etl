# Summary YAML Configuration Format

This document explains the YAML files in `config/summaries/` and, more importantly, how those files become the final cBioPortal summary outputs:

- `data_clinical_patient.txt`
- `data_clinical_sample.txt`

## Core Idea

Each summary YAML describes one data source and one intermediate output file. That intermediate file contributes one or more columns to either the patient summary or the sample summary.

Think of the final summary tables as wide tables assembled column-by-column:

```text
data_clinical_patient.txt
PATIENT_ID | SEX | RACE | ETHNICITY | OS_STATUS | ...
           ^      ^                    ^
           |      |                    |
     demographics.yaml          cbioportal_overall_survival.yaml

data_clinical_sample.txt
SAMPLE_ID | PATIENT_ID | PDL1_POSITIVE | GLEASON_PRIMARY | ...
                         ^               ^
                         |               |
      pathology_pd_l1_sample_summary.yaml
      pathology_gleason_scores_for_cbioportal_sample_summary.yaml
```

## What One Summary YAML Produces

One YAML file does not directly create a final cBioPortal file. It creates one intermediate TSV that is later merged with the other intermediate TSVs for the same level.

For example:

- `demographics.yaml` contributes patient-level columns such as `GENDER`, `RACE`, and `ETHNICITY`
- `pathology_pd_l1_sample_summary.yaml` contributes sample-level columns such as `PDL1_POSITIVE`

## Summary Pipeline Flow

The summary pipeline is implemented in four scripts:

1. `pipeline/summary/create_intermediate_summaries.py`
2. `pipeline/summary/merge_intermediate_summaries.py`
3. `pipeline/summary/create_summary_header.py`
4. `pipeline/summary/combine_header_and_data.py`

The flow is:

```text
summary YAMLs
  -> one intermediate TSV per YAML
  -> left-join all intermediates onto the cohort template
  -> create header metadata from YAML column_metadata
  -> combine header rows + merged data
  -> final cBioPortal file
```

### Step 1: Create One Intermediate TSV Per YAML

`create_intermediate_summaries.py`:

- loads each YAML in `config/summaries/`
- filters to the requested level with `patient_or_sample`
- queries the configured Databricks source table
- deidentifies any configured date columns into day offsets from `DATE_TUMOR_SEQUENCING`
- merges onto the template so all expected patients or samples remain in scope
- fills missing values using `column_metadata.*.fill_value`
- writes one intermediate TSV to Databricks volume storage

Typical intermediate outputs:

- patient: `PATIENT_ID` plus contributed data columns
- sample: `SAMPLE_ID`, `PATIENT_ID` plus contributed data columns

### Step 2: Merge Intermediate TSVs Into the Data Table

`merge_intermediate_summaries.py` starts from the cohort template and left-joins each intermediate file:

- patient summary merges on `PATIENT_ID`
- sample summary merges on `SAMPLE_ID`

This creates:

- `data_clinical_patient_data.txt`
- `data_clinical_sample_data.txt`

These are data-only files with a single column-name row. They are not yet in final cBioPortal format.

### Step 3: Build Header Metadata From YAML

`create_summary_header.py` reads the same YAML files and builds one tall metadata table with:

- `column_name`
- `display_label`
- `description`
- `datatype`
- `priority`

Those values come from:

- `column_metadata.<column>.label`
- `column_metadata.<column>.comment`
- `column_metadata.<column>.datatype`
- optional `column_metadata.<column>.priority`

### Step 4: Combine Header + Data Into Final cBioPortal Format

`combine_header_and_data.py` transposes the tall header and prepends five metadata rows to the merged data:

1. display label
2. description
3. datatype
4. priority
5. physical column name

Then it appends the data rows and writes the final file:

- patient: `data_clinical_patient.txt`
- sample: `data_clinical_sample.txt`

## Why the Template Matters

The template file defines the row backbone of the final output.

- patient template must contain `PATIENT_ID`
- sample template must contain `SAMPLE_ID` and `PATIENT_ID`

The pipeline left-joins summary data onto that template. This means:

- every patient or sample in the template stays in the final file
- a YAML only adds columns; it does not decide which rows belong in the cohort
- if a patient or sample has no value for a contributed column, the configured `fill_value` is used

This is the key mental model for going from smaller summary inputs to the final summary tables:

```text
template rows define who appears
summary YAMLs define what columns get added
```

## Minimal Annotated Example

```yaml
summary_id: demographics
patient_or_sample: patient

source_table_prod: cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics
source_table_dev: cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics

key_column: MRN

columns:
  - MRN
  - GENDER
  - RACE
  - ETHNICITY
  - CURRENT_AGE_DEID

date_columns: []

dest_prod:
  catalog: cdsi_eng_phi
  schema: cdm_eng_cbioportal_etl
  volume_name: cdm_eng_cbioportal_etl_volume
  filename: demographics_summary.tsv

dest_dev:
  catalog: cdsi_eng_phi
  schema: cdm_eng_cbioportal_etl
  volume_name: cdm_eng_cbioportal_etl_volume_dev
  filename: demographics_summary.tsv

column_metadata:
  GENDER:
    label: Sex
    datatype: STRING
    comment: "Sex ---DESCRIPTION: Gender assigned at birth ---MISSING DATA: Patient did not provide ---SOURCE: IDB (Revenue Management System (RMS))"
    fill_value: Unknown

  RACE:
    label: Race
    datatype: STRING
    comment: "Race ---DESCRIPTION: Race, self-reported ---MISSING DATA: Patient did not provide ---SOURCE: IDB (Revenue Management System (RMS))"
    fill_value: Unknown

  ETHNICITY:
    label: Ethnicity
    datatype: STRING
    comment: "Ethnicity ---DESCRIPTION: Ethnicity, self-reported ---MISSING DATA: Patient did not provide ---SOURCE: IDB (Revenue Management System (RMS))"
    fill_value: Unknown

  CURRENT_AGE_DEID:
    label: Age at Last Follow-up
    datatype: NUMBER
    comment: "Age at Last Follow-up ---DESCRIPTION: Age at Last Follow-up (limit 89) ---MISSING DATA: Contact Revenue Management System (RMS) Database Maintainers ---SOURCE: Derived from IDB/Revenue Management System (RMS) dates"
    fill_value: NA
```

What this YAML means in practice:

- read one source table from Databricks
- join source data to anchor dates using `MRN`
- output a patient-level intermediate TSV
- contribute four non-key columns to the patient summary
- use the metadata block to describe those columns in cBioPortal

## Field Reference

### `summary_id`

- Required
- Unique identifier for the summary configuration
- Used in logs and manifest output

Example:

```yaml
summary_id: demographics
```

### `patient_or_sample`

- Required
- Must be `patient` or `sample`
- Determines which final table this YAML contributes to

Rules:

- `patient` contributes to `data_clinical_patient.txt`
- `sample` contributes to `data_clinical_sample.txt`

### `source_table_prod` and `source_table_dev`

- Required
- Full Databricks table names
- The pipeline chooses one based on `production_or_test`

Example:

```yaml
source_table_prod: cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_summary_pdl1_sample
source_table_dev: cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_summary_pdl1_sample
```

### `key_column`

- Required
- Join key used when processing the source data

Common values:

- `MRN`
- `SAMPLE_ID`
- `DMP_ID`
- `PATIENT_ID`

Practical guidance:

- use `MRN` for most patient-level source tables
- use `SAMPLE_ID` for sample-level source tables
- use `DMP_ID` or `PATIENT_ID` only if the source already uses cBioPortal-style identifiers

### `columns`

- Required
- Ordered list of source columns to select from the Databricks table
- Must include the `key_column`

Example:

```yaml
columns:
  - SAMPLE_ID
  - PDL1_POSITIVE
```

Notes:

- names must match the source table exactly
- non-key columns listed here become candidate output columns
- column names are uppercased during processing

### `date_columns`

- Required
- List of columns from `columns` that should be converted from dates to integer day offsets from `DATE_TUMOR_SEQUENCING`

Example:

```yaml
date_columns:
  - LAST_CONTACT_DATE
```

If you have no date fields:

```yaml
date_columns: []
```

### `dest_prod` and `dest_dev`

- Required
- Tell the pipeline where to write the intermediate TSV for this YAML

Fields:

- `catalog`
- `schema`
- `volume_name`
- `filename`

The final intermediate path is:

```text
/Volumes/{catalog}/{schema}/{volume_name}/cbioportal/intermediate_files/{cohort}/{filename}
```

### `column_metadata`

- Required for every non-key output column you want properly described in the final file
- Used to generate the cBioPortal header and to fill missing values

Each entry typically includes:

- `label`
- `datatype`
- `comment`
- `fill_value`
- optional `priority`

Example:

```yaml
column_metadata:
  PDL1_POSITIVE:
    label: Sample PD-L1 Positive (NLP)
    datatype: STRING
    comment: "Sample PD-L1 Positive (NLP) ---DESCRIPTION: Indication if specimen was labeled as PD-L1 positive in surgical pathology report ---MISSING DATA: Patient not tested for PD-L1 ---SOURCE: NLP generated from pathology reports"
    fill_value: NA
```

## Patient vs Sample Summary Rules

### Patient Summary YAMLs

Use `patient_or_sample: patient` when the output represents one row per patient.

Typical examples:

- demographics
- overall survival
- comorbidity summaries

Typical pattern:

```yaml
patient_or_sample: patient
key_column: MRN
```

Intermediate shape:

```text
PATIENT_ID | COL_A | COL_B | ...
```

Final destination:

```text
data_clinical_patient.txt
```

### Sample Summary YAMLs

Use `patient_or_sample: sample` when the output represents one row per sample.

Typical examples:

- PD-L1 sample summary
- Gleason sample summary

Typical pattern:

```yaml
patient_or_sample: sample
key_column: SAMPLE_ID
```

Intermediate shape:

```text
SAMPLE_ID | PATIENT_ID | COL_A | COL_B | ...
```

Final destination:

```text
data_clinical_sample.txt
```

## Real Repository Examples

Patient-level examples:

- `config/summaries/demographics.yaml`
- `config/summaries/cbioportal_overall_survival.yaml`
- `config/summaries/comorbidity_index_score.yaml`

Sample-level examples:

- `config/summaries/pathology_pd_l1_sample_summary.yaml`
- `config/summaries/pathology_gleason_scores_for_cbioportal_sample_summary.yaml`

## Common Mistakes

- Setting `patient_or_sample: patient` for sample-level data or vice versa
- Omitting the key column from `columns`
- Forgetting to add metadata for a new column in `column_metadata`
- Using a source column name that does not exist in the Databricks table
- Adding date columns to `columns` but not listing them in `date_columns`
- Assuming a YAML creates new cohort rows instead of only adding columns to template-defined rows

## Related Documentation

- [Adding New Summary Data](../guides/adding_new_summary_data.md)
- [Modular Summary Pipeline](../pipelines/summary_modular_pipeline.md)
- [Timeline YAML Format](./timeline_yaml_format.md)

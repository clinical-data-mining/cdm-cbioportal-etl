# Timeline YAML Configuration Format

This document explains the YAML files in `config/timelines/` and what the timeline batch pipeline actually uses from them.

## Core Idea

Each timeline YAML describes one timeline domain and one output file.

Examples from this repository:

- `treatment.yaml` -> `data_timeline_treatment`
- `diagnosis.yaml` -> `data_timeline_diagnosis`
- `psa_labs.yaml` -> `data_timeline_psa_labs`

Unlike summary YAMLs, timeline YAMLs do not get horizontally merged together. Each YAML produces its own standalone timeline file.

## Timeline Pipeline Flow

The batch timeline workflow is driven by:

- `pipeline/timeline/cbioportal_timeline_batch_deidentify.py`
- `pipeline/timeline/cbioportal_timeline_deidentify.py`

For each YAML in `config/timelines/`, the batch script:

1. loads the YAML
2. chooses `source_table_prod` or `source_table_dev`
3. reads the ordered column keys under `columns`
4. passes those column names to the generic deidentification script
5. writes:
   - a PHI version to Databricks volume storage and a Databricks table
   - a deidentified cBioPortal `.txt` file to the local output path

In other words:

```text
one timeline YAML
  -> one source timeline table
  -> one deidentification run
  -> one final data_timeline_*.txt file
```

## What the Pipeline Consumes From Timeline YAML

The batch script uses these fields directly:

- `timeline_id`
- `source_table_prod`
- `source_table_dev`
- `output_filename`
- `patient_or_sample`
- `output_table.catalog`
- `output_table.schema`
- the ordered keys of `columns`

The `table_metadata` block is still important, but it is documentation metadata rather than a driver of the deidentification command.

## Minimal Annotated Example

This is trimmed from the existing treatment timeline config pattern:

```yaml
timeline_id: treatment
source_table_prod: cdsi_eng_phi.cdm_eng_treatments.table_timeline_medications
source_table_dev: cdsi_eng_phi.cdm_eng_treatments.table_timeline_medications
output_filename: data_timeline_treatment
patient_or_sample: patient

output_table:
  catalog: cdsi_eng_phi
  schema: cdm_eng_cbioportal_etl

table_metadata:
  form_name: Anti-Cancer Medications
  cdm_source_table: medications/table_timeline_medications.tsv
  table_description: Timeline of medications administered at MSK.
  cbio_timeline_reason_for_missing_data: No medications administered at MSK.
  cbio_timeline_data_source: IDB, Antineoplastics patient medications table
  cbio_data_source_link: https://github.mskcc.org/cdsi/msk-impact/blob/master/msk_solid_heme/data_timeline_treatment.txt
  for_docs_site_timeline: true

columns:
  PATIENT_ID:
    field_name: PATIENT_ID
    field_label: PATIENT_ID
    field_note: Medical Record Number
    nlp_derived: false

  START_DATE:
    field_name: START_DATE
    field_label: Start Date (Medication)
    field_note: Earliest date this medication was taken
    nlp_derived: false

  STOP_DATE:
    field_name: STOP_DATE
    field_label: End Date (Medication)
    field_note: Latest date this medication was taken
    nlp_derived: false

  EVENT_TYPE:
    field_name: EVENT_TYPE
    field_label: Event Type
    field_note: cBioPortal formatting scheme. All TREATMENT
    nlp_derived: false

  TREATMENT_TYPE:
    field_name: TREATMENT_TYPE
    field_label: Subtype
    field_note: cBioPortal formatting scheme. All Medical Therapy
    nlp_derived: false

  AGENT:
    field_name: AGENT
    field_label: Generic Drug Name
    field_note: Generic MULTUM name for formulary drugs or keyword for non-formulary.
    nlp_derived: false
```

What this YAML means in practice:

- read one Databricks source table
- keep the columns listed under `columns`, in that order
- deidentify dates relative to anchor dates
- write one output timeline file named `data_timeline_treatment.txt`

## Field Reference

### `timeline_id`

- Required
- Unique identifier for the timeline configuration
- Used for logging and PHI table naming

Example:

```yaml
timeline_id: treatment
```

### `source_table_prod` and `source_table_dev`

- Required
- Full Databricks source table names
- The batch script chooses one based on `production_or_test`

Example:

```yaml
source_table_prod: cdsi_eng_phi.cdm_eng_treatments.table_timeline_medications
source_table_dev: cdsi_eng_phi.cdm_eng_treatments.table_timeline_medications
```

### `output_filename`

- Required
- Base name for the generated timeline file
- Should follow the cBioPortal convention `data_timeline_*`

Example:

```yaml
output_filename: data_timeline_treatment
```

Generated outputs:

- Databricks volume PHI file: `{output_filename}_phi.tsv`
- Local deidentified file: `{output_filename}.txt`

### `patient_or_sample`

- Required
- Must be `patient` or `sample`
- Passed to the deidentification script as `--merge_level`

Typical usage:

- most timeline files are `patient`
- use `sample` only when the timeline rows are fundamentally sample-specific

### `output_table`

- Required in practice
- Tells the batch script where to save the PHI table version

Fields:

- `catalog`
- `schema`

The PHI table name is built automatically as:

```text
{output_filename}_{cohort_name}_phi
```

### `table_metadata`

- Required for documentation and data provenance
- Not used to build the deidentification command, but still important to keep complete

Expected fields:

- `form_name`
- `cdm_source_table`
- `table_description`
- `cbio_timeline_reason_for_missing_data`
- `cbio_timeline_data_source`
- `cbio_data_source_link`
- `for_docs_site_timeline`

Use this block to explain:

- what the timeline contains
- why a patient may have no events
- whether the timeline is NLP-derived
- where the upstream data came from

### `columns`

- Required
- Ordered mapping of output column names to documentation metadata
- The batch pipeline passes the top-level keys of this block, in order, to the deidentification script

Example:

```yaml
columns:
  PATIENT_ID:
    field_name: PATIENT_ID
    field_label: Patient ID
    field_note: Medical Record Number
    nlp_derived: false

  START_DATE:
    field_name: START_DATE
    field_label: Event Start Date
    field_note: Date when event started
    nlp_derived: false
```

Important behavior:

- the key names under `columns` are what drive the output column order
- the source table must already contain columns that match those names
- the pipeline does not rename arbitrary source columns based on `field_name`

That last point is important: `field_name` is descriptive metadata here, not a transformation rule.

## Required Timeline Columns

Most timeline source tables should include at least:

- `PATIENT_ID`
- `START_DATE`
- `STOP_DATE`
- `EVENT_TYPE`

For sample-level timelines, `SAMPLE_ID` may also be needed.

### `PATIENT_ID`

- patient identifier used for deidentification and cohort matching

### `START_DATE`

- start date of the event
- converted into a deidentified day offset from the anchor date

### `STOP_DATE`

- end date of the event
- may be blank for point events
- also converted into a deidentified day offset

### `EVENT_TYPE`

- cBioPortal event category

Common values in practice:

- `TREATMENT`
- `STATUS`
- `PROCEDURE`
- `LAB_TEST`
- `IMAGING`
- `SPECIMEN`
- `DIAGNOSIS`

## Common Optional Columns

Depending on the timeline domain, common additional columns include:

- `SUBTYPE`
- `TEST`
- `RESULT`
- `AGENT`
- `TREATMENT_TYPE`
- `REFERENCE_RANGE`
- `STYLE_COLOR`
- `STYLE_SHAPE`
- `SOURCE`

The important rule is not which optional fields exist globally. The important rule is that the source table and the YAML must agree exactly on the columns for that timeline.

## Practical Rules For Authors

When you add a new timeline YAML:

1. Make sure the upstream Databricks table already has cBioPortal-ready column names.
2. Add the desired output columns under `columns` in the exact order you want them emitted.
3. Fill out `table_metadata` so users understand what the file represents.
4. Keep `output_filename` aligned with cBioPortal naming conventions.
5. Set `patient_or_sample` correctly so deidentification uses the right merge level.

## Real Repository Examples

- `config/timelines/treatment.yaml`
- `config/timelines/diagnosis.yaml`
- `config/timelines/specimen.yaml`
- `config/timelines/psa_labs.yaml`
- `config/timelines/progression.yaml`

## Common Mistakes

- Treating `field_name` as a rename instruction instead of metadata
- Listing columns in YAML that are not present in the source table
- Using the wrong `patient_or_sample` level
- Using an `output_filename` that does not follow `data_timeline_*`
- Leaving `table_metadata` incomplete so users cannot interpret missing data or provenance

## Related Documentation

- [Adding New Timeline Data](../guides/adding_new_timeline_data.md)
- [Timeline Deidentification](../pipelines/timeline_deidentification.md)
- [Summary YAML Format](./summary_yaml_format.md)

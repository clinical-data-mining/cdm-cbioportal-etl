# Adding New Timeline Data

This guide walks you through adding a new timeline file to cBioPortal.

## Overview

Timeline data represents temporal events in a patient's journey:
- Treatments (medications, surgeries, radiation)
- Diagnoses
- Lab tests
- Imaging studies
- Clinical assessments (ECOG, KPS)
- Disease progression events

## Quick Start

To add a new timeline:
1. Create a YAML config in `config/timelines/`
2. Define source table, columns, and metadata
3. Run the batch deidentification pipeline
4. Your timeline file will be automatically generated

## YAML Configuration Template

```yaml
# config/timelines/my_timeline.yaml
timeline_id: my_timeline
source_table_prod: catalog.schema.my_events_prod
source_table_dev: catalog_dev.schema.my_events_dev
output_filename: data_timeline_my_events
patient_or_sample: patient

# Output table configuration
output_table:
  catalog: cdsi_eng_phi
  schema: cdm_eng_cbioportal_etl

# Metadata
table_metadata:
  form_name: "My Events"
  cdm_source_table: "my_domain/my_events.tsv"
  table_description: "Description of these events"
  cbio_timeline_reason_for_missing_data: "Event not recorded for patient"
  cbio_timeline_data_source: "Source system name"
  cbio_data_source_link: "https://link-to-docs"
  for_docs_site_timeline: true

# Column definitions
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

  STOP_DATE:
    field_name: STOP_DATE
    field_label: Event Stop Date
    field_note: Date when event ended (blank if point event)
    nlp_derived: false

  EVENT_TYPE:
    field_name: EVENT_TYPE
    field_label: Event Type
    field_note: cBioPortal event category (TREATMENT, LAB_TEST, etc.)
    nlp_derived: false

  # Add your custom columns here
  MY_COLUMN:
    field_name: MY_COLUMN
    field_label: My Column Label
    field_note: Description of this column
    nlp_derived: false
```

## Step-by-Step Guide

### Step 1: Understand cBioPortal Timeline Requirements

**Required columns:**
- `PATIENT_ID` - De-identified patient identifier
- `START_DATE` - Event start (will be converted to days from anchor)
- `STOP_DATE` - Event end (can be blank)
- `EVENT_TYPE` - Must be a valid cBioPortal event type

**Valid EVENT_TYPE values:**
- `TREATMENT` - Therapies, medications, procedures
- `STATUS` - Disease status, ECOG/KPS scores
- `PROCEDURE` - Surgeries, biopsies
- `LAB_TEST` - Laboratory results
- `IMAGING` - Radiology, scans
- `SPECIMEN` - Sample collection
- `DIAGNOSIS` - Disease diagnoses

See [cBioPortal Timeline Documentation](https://docs.cbioportal.org/file-formats/#timeline-data) for full details.

### Step 2: Prepare Your Source Data

Ensure your Databricks table has:
- Patient identifier column (`MRN` or `DMP_ID`)
- Date columns for START_DATE and STOP_DATE
- EVENT_TYPE column (or constant value)
- Any additional metadata columns

### Step 3: Create YAML Configuration

Create `config/timelines/your_timeline.yaml` following the template above.

**Key decisions:**
- Choose appropriate `EVENT_TYPE`
- Determine if `SUBTYPE` is needed (sub-categories within EVENT_TYPE)
- Decide which metadata columns to include
- Consider if date truncation by OS_DATE is needed

### Step 4: Run Batch Deidentification

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

This will process all timeline YAMLs including your new one.

### Step 5: Verify Output

Check both outputs:

**PHI version (QC):** `/Volumes/.../data_timeline_my_events_phi.tsv`
- Contains original dates and MRN
- Use for quality control

**De-identified version (cBioPortal):** `/local/path/data_timeline_my_events.txt`
- Contains days from anchor date
- Ready for cBioPortal import

## Configuration Details

### Date Truncation

Use `--truncate_by_os_date` flag to ensure events don't extend beyond patient's last known date:

**When to use:**
- ✅ Medications (treatments shouldn't extend past death)
- ✅ Active disease states
- ❓ Lab tests (depends on use case)
- ❌ Diagnoses (historical events)

### Column Metadata

For each column, specify:
- `field_name` - Column name in output
- `field_label` - Human-readable label
- `field_note` - Detailed description
- `nlp_derived` - Boolean, true if extracted via NLP

## Examples

See [Timeline Deidentification Guide](../pipelines/timeline_deidentification.md) for complete examples of:
- Medications timeline
- Lab tests timeline
- Diagnoses timeline

## Troubleshooting

### Issue: Timeline file not generated

**Check:**
1. YAML file in `config/timelines/` directory
2. YAML syntax is valid
3. Source table exists and is accessible
4. Output paths have write permissions

### Issue: Date parsing errors

**Check:**
1. Source dates are in parseable format
2. Date columns are named START_DATE/STOP_DATE in source or mapped correctly
3. Anchor dates table has entries for your patients

### Issue: Missing patients

**Check:**
1. Sample list file includes expected patients
2. Source table has data for those patients
3. JOIN with anchor dates successful

## Next Steps

- [Timeline Deidentification Pipeline](../pipelines/timeline_deidentification.md) - Detailed pipeline documentation
- [Troubleshooting Guide](troubleshooting.md) - Common issues
- [Running Full ETL](running_full_etl.md) - Complete workflow

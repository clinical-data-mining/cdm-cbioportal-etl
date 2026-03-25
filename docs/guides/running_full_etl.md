# Running the Full ETL Pipeline

This guide covers running the complete end-to-end ETL process to generate all cBioPortal files.

## Overview

A complete ETL run produces:
- Patient summary file (`data_clinical_patient.txt`)
- Sample summary file (`data_clinical_sample.txt`)
- 21 timeline files (`data_timeline_*.txt`)

## Prerequisites

- Databricks connection configured
- Template files generated (patient and sample)
- Anchor dates table populated
- All YAML configurations up to date

## Full Pipeline Steps

### 1. Generate Template Files

Templates contain the list of patient/sample IDs for the cohort.

```bash
python pipeline/utils/generate_cbioportal_template.py \
  --databricks_env /path/to/databricks_env.txt \
  --sample_source_table catalog.schema.sample_list \
  --output_patient /path/to/patient_template.txt \
  --output_sample /path/to/sample_template.txt
```

### 2. Save Anchor Dates

Ensure anchor dates are up to date:

```bash
bash pipeline/bash/bash_save_anchor_dates.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  /path/to/databricks_env.txt \
  catalog.schema.anchor_dates_table
```

### 3. Run Summary Pipeline (Patient)

```bash
bash pipeline/bash/bash_summary_modular_pipeline.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  config/summaries \
  /path/to/databricks_env.txt \
  catalog.schema.anchor_dates_table \
  /path/to/patient_template.txt \
  /path/to/sample_template.txt \
  /Volumes/output/base/path \
  /local/output/base/path \
  cdsi_eng_phi \
  cdm_eng_cbioportal_etl \
  production \
  mskimpact \
  --patient
```

### 4. Run Summary Pipeline (Sample)

```bash
bash pipeline/bash/bash_summary_modular_pipeline.sh \
  [same args as above] \
  --sample
```

### 5. Run Timeline Deidentification

```bash
bash pipeline/bash/bash_timeline_batch_deid.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  config/timelines \
  production \
  /path/to/databricks_env.txt \
  catalog.schema.anchor_dates_table \
  /local/output/base/path/data_clinical_sample.txt \
  /Volumes/output/base/path \
  /local/output/base/path \
  mskimpact
```

### 6. Monitor Completeness

Verify all expected files were generated:

```bash
bash pipeline/bash/bash_monitor_completeness.sh \
  /path/to/repo \
  /path/to/conda \
  cdm-cbioportal-etl \
  /local/output/base/path
```

This checks for:
- `data_clinical_patient.txt`
- `data_clinical_sample.txt`
- All expected `data_timeline_*.txt` files

## Output Files

After a successful run, you should have:

```
/local/output/base/path/
├── data_clinical_patient.txt       # Patient summary
├── data_clinical_sample.txt        # Sample summary
├── data_timeline_treatment.txt     # Medications
├── data_timeline_diagnosis.txt     # Diagnoses
├── data_timeline_lab_test.txt      # Lab results
├── data_timeline_specimen.txt      # Specimen collection
├── data_timeline_progression.txt   # Disease progression
└── ... (16 more timeline files)
```

## Running for Multiple Cohorts

To process multiple cohorts, run the pipeline for each:

```bash
for COHORT in mskimpact mskaccess mskarcher mskimpact_heme; do
  echo "Processing $COHORT..."

  # Update paths for cohort
  SAMPLE_TEMPLATE="/path/to/${COHORT}_sample_template.txt"
  OUTPUT_PATH="/path/to/output/${COHORT}"

  # Run pipeline
  bash pipeline/bash/bash_summary_modular_pipeline.sh ... $COHORT --patient
  bash pipeline/bash/bash_summary_modular_pipeline.sh ... $COHORT --sample
  bash pipeline/bash/bash_timeline_batch_deid.sh ... $COHORT
done
```

## Using Airflow DAG

For production, use the Airflow DAG orchestration:

```python
# See sandbox/cdm_etl_cbioportal_cdsi_dev.py for DAG example
```

The DAG handles:
- Dependency ordering
- Parallel execution (patient and sample summaries)
- Error handling and retries
- Monitoring and notifications

## Troubleshooting

### Issue: Template generation fails

**Check:**
- Sample source table exists and is accessible
- Databricks credentials are valid
- Output directory has write permissions

### Issue: Summary pipeline fails mid-way

**Benefit of modular approach:** You can re-run individual scripts!

```bash
# Re-run just the merge step
python pipeline/summary/merge_intermediate_summaries.py [args...]
```

### Issue: Timeline deidentification fails for one timeline

**Check:**
- Source table exists for that timeline
- YAML configuration is correct
- Anchor dates available for all patients

### Issue: Monitoring reports missing files

**Check:**
1. All pipelines completed successfully
2. Output paths are correct
3. File permissions allow reading

## Performance Optimization

**Run patient and sample summaries in parallel:**

```bash
# In separate terminals or via parallel tool
bash pipeline/bash/bash_summary_modular_pipeline.sh ... --patient &
bash pipeline/bash/bash_summary_modular_pipeline.sh ... --sample &
wait
```

**Process multiple cohorts in parallel:**
```bash
# Use GNU parallel or run in separate shells
parallel bash pipeline/bash/bash_summary_modular_pipeline.sh ... {} ::: mskimpact mskaccess mskarcher
```

## Next Steps

- [Troubleshooting Guide](troubleshooting.md) - Debug issues
- [Monitoring Documentation](../pipelines/monitoring.md) - Understand validation
- [Adding New Data](adding_new_summary_data.md) - Extend the pipeline

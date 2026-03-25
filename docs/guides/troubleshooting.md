# Troubleshooting Guide

Common issues and solutions for the cdm-cbioportal-etl pipeline.

## Connection Issues

### Databricks Connection Failed

**Symptoms:**
```
Error: Unable to connect to Databricks
```

**Solutions:**
1. Verify environment file format:
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-token-here
   ```
2. Check token is valid (tokens can expire)
3. Verify network access to Databricks
4. Test connection:
   ```python
   from msk_cdm.databricks import DatabricksAPI
   db = DatabricksAPI(fname_databricks_env='your_env.txt')
   db.spark.sql("SHOW CATALOGS").show()
   ```

### Permission Denied Accessing Tables

**Symptoms:**
```
Error: User does not have SELECT privilege on table catalog.schema.table
```

**Solutions:**
- Contact Databricks admin to grant READ access to source catalogs
- Verify you have WRITE access to destination volumes
- Check USAGE permissions on catalog and schema

## Pipeline Failures

### Summary Pipeline: Missing Columns

**Symptoms:**
```
KeyError: 'COLUMN_NAME' not found in DataFrame
```

**Solutions:**
1. Verify column exists in source table:
   ```sql
   SELECT * FROM source_table LIMIT 1
   ```
2. Check column name spelling in YAML (case-sensitive)
3. Ensure columns list in YAML matches source table
4. Check if table schema changed recently

### Summary Pipeline: All Values Show as Fill Value

**Symptoms:**
All data for a column shows the `fill_value` (e.g., "Unknown", "NA")

**Solutions:**
1. Check key_column matches table structure (`MRN` vs. `SAMPLE_ID`)
2. Verify data exists for your cohort in source table
3. Check join with template succeeded:
   ```python
   # Inspect intermediate file
   import pandas as pd
   df = pd.read_csv('/Volumes/.../intermediate_files/{cohort}/{summary_id}_summary.tsv', sep='\t')
   print(df.head())
   ```
4. Verify anchor dates merge succeeded

### Timeline Pipeline: Date Parsing Errors

**Symptoms:**
```
Warning: High percentage of date coercion (X%)
```

**Solutions:**
1. Check source date format is parseable
2. Verify START_DATE and STOP_DATE columns exist
3. Some null dates are expected (STOP_DATE often null)
4. If >50% coercion, investigate source data quality

### Timeline Pipeline: Missing Patients

**Symptoms:**
Timeline file has fewer patients than expected

**Solutions:**
1. Check sample list file includes expected patients
2. Verify source table has data for those patients
3. Check anchor dates table has entries for all patients
4. Review merge statistics in pipeline output

## Configuration Issues

### YAML Syntax Error

**Symptoms:**
```
yaml.scanner.ScannerError: while scanning...
```

**Solutions:**
1. Validate YAML syntax:
   ```bash
   python -c "import yaml; yaml.safe_load(open('your_file.yaml'))"
   ```
2. Common issues:
   - Incorrect indentation (use 2 spaces, not tabs)
   - Missing colons after keys
   - Unquoted strings with special characters
   - Missing quotes around strings starting with `#`

### YAML Not Found or Ignored

**Symptoms:**
Pipeline runs but your new YAML isn't processed

**Solutions:**
1. Verify file is in `config/summaries/` or `config/timelines/`
2. Check file has `.yaml` extension (not `.yml`)
3. Verify `patient_or_sample` field matches pipeline run (patient vs. sample)
4. Check file permissions (readable)

## Data Quality Issues

### Unexpected NULL Values

**Symptoms:**
More NULL/NA values than expected in output

**Solutions:**
1. Check source table data quality
2. Verify `fill_value` in YAML is appropriate
3. Check if date deidentification failed (dates showing as NA)
4. Review intermediate files for issues

### Date Conversion Produces Negative Values

**Symptoms:**
Timeline dates are negative numbers

**Cause:**
Event occurred before anchor date (sequencing date)

**Solutions:**
- This is expected for historical events (diagnoses, prior treatments)
- If unexpected, check:
  1. Anchor date is correct for patient
  2. Source dates are valid
  3. Date columns are correctly identified

### Duplicate Rows

**Symptoms:**
Final file has duplicate patient/sample IDs

**Solutions:**
1. Check source tables don't have duplicates
2. For summaries: ensure one row per patient/sample in source
3. For timelines: duplicates are OK (multiple events per patient)
4. Review intermediate files to identify source

## File System Issues

### Permission Denied Writing Output

**Symptoms:**
```
PermissionError: [Errno 13] Permission denied: '/path/to/output.txt'
```

**Solutions:**
1. Check write permissions on output directory
2. Verify volume paths are correct
3. Check Databricks volume access permissions
4. Ensure parent directories exist

### Output Files Not Found

**Symptoms:**
Pipeline completes but files don't exist

**Solutions:**
1. Check output paths in pipeline commands
2. Verify both volume and local paths
3. Check disk space availability
4. Review pipeline logs for errors

### Large File Processing Slow

**Symptoms:**
Pipeline takes very long for large cohorts

**Solutions:**
1. Process patient and sample summaries in parallel
2. Use Databricks compute with more resources
3. Consider partitioning large tables
4. Check network speed (Databricks to local transfer)

## Validation Issues

### Monitoring Reports Missing Files

**Symptoms:**
```
Error: Expected file not found: data_timeline_X.txt
```

**Solutions:**
1. Check all timeline YAMLs processed successfully
2. Verify output paths are correct
3. Check file naming matches expected pattern
4. Review timeline deidentification logs

### cBioPortal Import Fails

**Symptoms:**
Files generated but cBioPortal import fails

**Solutions:**
1. Verify file format:
   ```bash
   # Check header rows (should be 5 for summaries)
   head -5 data_clinical_patient.txt
   ```
2. Check required columns present:
   - Patient summary: `PATIENT_ID`
   - Sample summary: `PATIENT_ID`, `SAMPLE_ID`
   - Timeline: `PATIENT_ID`, `START_DATE`, `STOP_DATE`, `EVENT_TYPE`
3. Validate datatype row (row 3) has correct format
4. Check for special characters in data values

## Performance Issues

### Out of Memory Errors

**Symptoms:**
```
MemoryError: Unable to allocate array
```

**Solutions:**
1. Process smaller batches
2. Increase Databricks cluster memory
3. Use Spark operations instead of Pandas for large data
4. Close unused objects to free memory

### Slow Query Performance

**Symptoms:**
Pipeline stuck on data extraction

**Solutions:**
1. Check source table size and query complexity
2. Add indexes on frequently joined columns
3. Use appropriate Databricks cluster size
4. Consider materializing intermediate tables

## Getting Help

If you can't resolve an issue:

1. **Check logs:** Review full pipeline output for error details
2. **Inspect intermediates:** Look at files in `intermediate_files/` directory
3. **Test components:** Run individual scripts to isolate issue
4. **Verify data:** Query source tables directly to confirm data exists
5. **File an issue:** Create detailed bug report with:
   - Error message
   - Pipeline command used
   - Sample YAML config
   - Source table structure
   - Expected vs. actual behavior

## Debugging Tips

### Enable Verbose Logging

Add debug prints to pipeline scripts:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect Intermediate Files

Check data at each pipeline stage:
```bash
# Summary intermediates
ls /Volumes/.../intermediate_files/{cohort}/
head /Volumes/.../intermediate_files/{cohort}/demographics_summary.tsv

# Timeline PHI files (for QC)
head /Volumes/.../data_timeline_treatment_phi.tsv
```

### Test Individual Components

Run scripts individually:
```bash
# Test just intermediate creation
python pipeline/summary/create_intermediate_summaries.py [args...]

# Test just one timeline
python pipeline/timeline/cbioportal_timeline_deidentify.py [args for one timeline]
```

### Validate YAML Configs

```python
import yaml

# Load and print
with open('config/summaries/my_config.yaml') as f:
    config = yaml.safe_load(f)
    print(yaml.dump(config, indent=2))
```

## Common Error Patterns

### Pattern: "Table or view not found"
→ Check source_table_prod/dev in YAML matches actual table name

### Pattern: "Column X not found"
→ Column name in YAML doesn't match source table (check case)

### Pattern: "Cannot merge due to key mismatch"
→ key_column value incorrect (MRN vs SAMPLE_ID vs DMP_ID)

### Pattern: "All dates parsed to None"
→ Date format in source incompatible, or wrong column identified

### Pattern: "Pipeline completes but output empty"
→ Join with template or anchor dates failed, check key columns

## Next Steps

- [Architecture Documentation](../architecture.md) - Understand system design
- [Pipeline Documentation](../pipelines/) - Detailed pipeline guides
- [Configuration Reference](../configuration/) - YAML format specifications

# Summary Data Formatting
This page explains how to generate patient and sample summary files that can be pushed to cBioPortal. 

Either a patient or sample summary files is generated using the `create_cbioportal_summary.py` function. The script formats data using two key classes, `RedcapToCbioportalFormat` and `cbioportalSummaryFileCombiner`, to process patient and sample summary data file into the cBioPortal format.

This code determines the sample and patient summary files to generate based on the annotations and metadata in the [CDM Codebook](https://docs.google.com/spreadsheets/d/1po0GdSwqmmXibz4e-7YvTPUbXpi0WYv3c2ImdHXxyuc/edit?usp=sharing)


##  Workflow
Processing a cBioPortal summary is done with the`create_cbioportal_summary` function.
The processing flow of this function follows these steps:

### Initialize the `RedcapToCbioportalFormat` Object

The `RedcapToCbioportalFormat` class is used to read metadata from the CDM-Codebook and transform them into cBioPortal-compatible summaries and headers:
```python
from cdm_cbioportal_etl.summary import RedcapToCbioportalFormat 

obj_format_cbio = RedcapToCbioportalFormat(
    fname_minio_env=fname_minio_env,
    path_minio_summary_intermediate=path_minio_summary_intermediate,
    fname_metadata=fname_meta_data,
    fname_metaproject=fname_meta_project,
    fname_metatables=fname_meta_table
)
```
- **`fname_minio_env`**: Configuration for the MinIO storage environment (used for storing intermediate files).
- **`path_minio_summary_intermediate`**: Minio path (folder) for storing the intermediate summary files.
- **`fname_metadata`**: Path location of the CDM-Codebook (Metadata tab, csv file)
- **`fname_metaproject`**: Path location of the CDM-Codebook (Project tab, csv file)
- **`fname_metatables`**: Path location of the CDM-Codebook (Table tab, csv file)

### Generate Summaries and Headers Using `create_summaries_and_headers`

This step creates individual data summary and header files using the `create_summaries_and_headers` method. The filenames for the header and data files are stored in a separate manifest file (`fname_manifest`)  
```python
obj_format_cbio.create_summaries_and_headers(
    patient_or_sample=patient_or_sample,
    fname_manifest=fname_manifest,
    fname_template=fname_current_summary,
    production_or_test=production_or_test
)
```
- **`patient_or_sample`**: Determines whether the processing is for patient or sample data.
- **`fname_manifest`**: Manifest filename (MinIO) to be saved listing patient or sample data to process.
- **`fname_template`**: Template file containing patient or sample IDs. Thsi file is created with the [Summary File Template Generator](summary_template_generation.md) 
- **`production_or_test`**: (Deprecated) Decides whether the output is for production or testing.

### Combine the Individual Summaries Using the `cbioportalSummaryFileCombiner` Class

Once individual summaries and headers are generated and manifest file created, data and headers are combined into a single file for cBioPortal using the `cbioportalSummaryFileCombiner` class
```python
from cdm_cbioportal_etl.summary import cbioportalSummaryFileCombiner

obj_p_combiner = cbioportalSummaryFileCombiner(
    fname_minio_env=fname_minio_env,
    fname_manifest=fname_manifest,
    fname_summary_template=fname_summary_template, 
    patient_or_sample=patient_or_sample,
    production_or_test=production_or_test,
)
```
- **`fname_minio_env`**: Configuration for the MinIO storage environment (used for storing intermediate files).
- **`fname_manifest`**: Manifest filename (MinIO) to be saved listing patient or sample data to process.
- **`fname_summary_template`**: Template file containing patient or sample IDs. This file is created with the [Summary File Template Generator](summary_template_generation.md)
- **`patient_or_sample`**: Determines whether the processing is for patient or sample data.
- **`production_or_test`**: (Deprecated) Decides whether the output is for production or testing.



### Save the Final Merged Summary File
This creates the summary file to be imported into cBioPortal

The final merged summary is saved to MinIO using `save_update` and can be returned for inspection using `return_final`:
```python
obj_p_combiner.save_update(fname=fname_new_summary)
df_cbio_summary = obj_p_combiner.return_final()
```


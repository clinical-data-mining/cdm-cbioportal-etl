# Modifying the Codebook
The [CDM Codebook](https://docs.google.com/spreadsheets/d/1po0GdSwqmmXibz4e-7YvTPUbXpi0WYv3c2ImdHXxyuc/edit?usp=sharing) is used to describe data tables and their columns. 

For `cdm-cbioportal-etl` purposes, the codebook is used for code to process information to transform and import data into a cBioPortal study properly.

## Codebook Description
### Metadata Tab

| COLUMN                             | COLUMN_USED_FOR_CDM_CBIOPORTAL_ETL_CODE | COLUMN_USED_FOR_CDSI_CODEBOOK | DESCRIPTION                                                                                                      |
|------------------------------------|----------------------------------------|-------------------------------|------------------------------------------------------------------------------------------------------------------|
| record_id                          |                                        |                               | Redcap annotation. Denotes project number                                                                        |
| redcap_repeat_instrument           |                                        |                               | Redcap annotation.                                                                                               |
| redcap_repeat_instance             |                                        |                               | Redcap annotation.                                                                                               |
| form_name                          |                                        |                               | Name of the table. This must be the same as the metadata tab.                                                    |
| field_name                         | x                                      | x                             | Column name. Must be the column name used in the data file.                                                      |
| nlp_derived                        | x                                      | x                             | `y` denotes if NLP is used to derive this column                                                                 |
| field_type                         | x                                      | x                             | Data type for this column. For Redcap purposes                                                                   |
| field_label                        | x                                      | x                             | Display name for this column. This will be shown on cbioportal                                                   |
| field_note                         | x                                      | x                             | Description for this column. This will be the hoverover on cbioportal                                            |
| text_validation_type_or_sh         | x                                      |                               | Indicates a date with `date_mdy`. Otherwise blank.                                                               |
| identifier                         | x                                      |                               | `y` indicates column is PHI                                                                                      |
| extract_label                      |                                        |                               | EXTRACT concept ID for this column                                                                               |
| for_cbioportal                     | x                                      |                               | `x` Indicates this column is for a cbioportal study SUMMARY (Not timeline, exclude column key as this is in the tables tab) |
| for_docs_site                      |                                        | x                             | `x` Indicates this column is to be included in the documentation website. The data itself may be sourced from a group outside of CDM |
| for_test_portal                    | x                                      |                               | `x` Indicates this column is for the TEST cbioportal study SUMMARY (Not timeline, exclude column key as this is in the tables tab) |
| fill_value                         | x                                      | x                             | Missing data is filled in with this value                                                                       |
| reasons_for_missing_data           | x                                      | x                             | Short description for why data is missing and would need to be imputed by 'fill_value'                           |
| souce_from_idb_or_cdm              | x                                      | x                             | Description of source of data. Either from IDB (with specific source), or CDM generated data                     |
| adapted_from_col                   |                                        |                               | Column name from the table this data is DIRECTLY coming from. (ie duplicated data added over)                    |
| adapted_from_table                 |                                        |                               | Table name used for `adapted_from_col`                                                                           |
| extract_name_id                    |                                        |                               |                                                                                                                  |
| palantir_dataset                   |                                        |                               |                                                                                                                  |
| palantir_unified_column            |                                        |                               |                                                                                                                  |
| epic_table_name                    |                                        |                               |                                                                                                                  |
| epic_column_name                   |                                        |                               |                                                                                                                  |

---

### Table Tab

| COLUMN                             | COLUMN_USED_FOR_CDM_CBIOPORTAL_ETL_CODE | COLUMN_USED_FOR_CDSI_CODEBOOK | DESCRIPTION                                                                                                      |
|------------------------------------|----------------------------------------|-------------------------------|------------------------------------------------------------------------------------------------------------------|
| record_id                          |                                        |                               | Redcap annotation. Denotes project number                                                                        |
| redcap_repeat_instrument           |                                        |                               | Redcap annotation.                                                                                               |
| redcap_repeat_instance             |                                        |                               | Redcap annotation.                                                                                               |
| form_name                          | x                                      | x                             | Name of the table. This must be the same as the metadata tab.                                                    |
| meta_project_name                  | x                                      |                               | Designator for the project. This must be the same as the project tab.                                            |
| cdm_source_table                   | x                                      |                               | Location of the data on MinIO                                                                                    |
| table_description                  |                                        | x                             | Description of what this table contains                                                                          |
| Dremio role that has access        |                                        |                               |                                                                                                                  |
| script_that_generates              |                                        |                               | Link to code on github that creates this table                                                                   |
| cdm_sql_query_pathname             |                                        |                               | SQL query pathname used to capture this data. Only exists if this table is directly from IDB.                    |
| cbio_timeline_file_production       | x                                      | x                             | Denote with an 'x' that file is to be used for cbioportal importing (Production study)                            |
| cbio_timeline_file_testing          | x                                      | x                             | Denote with an 'x' that file is to be used for cbioportal importing (Testing study)                               |
| yaml_timeline_variable_map         | x                                      | x                             | Python variable name used in the msk_cdm package for data files on Minio                                         |
| cbio_deid_filename                 | x                                      | x                             | Filename of de-identified timeline data pushed to cBioPortal                                                     |
| cbio_timeline_reason_for_missing_data | x                                    | x                             | Description why an entry would be missing for a patient                                                          |
| cbio_timeline_data_source          | x                                      | x                             | Description of source of data. Either from IDB (with specific source), or NLP generated data                     |
| cbio_timeline_data_source_link_or_path |                                    |                               | Pathname on Minio or elsewhere of where data exists                                                              |
| cbio_summary_id_patient            | x                                      |                               | Column name used to map to a cbioportal study summary (Patient). This ID will be mapped to "PATIENT_ID"          |
| cbio_summary_id_sample             | x                                      |                               | Column name used to map to a cbioportal study summary (Sample)                                                   |
| key_primary                       |                                        |                               | The primary key for this table                                                                                   |
| key_secondary                     |                                        |                               | The secondary key for this table                                                                                 |

---

### Project Tab

| COLUMN                             | COLUMN_USED_FOR_CDM_CBIOPORTAL_ETL_CODE | COLUMN_USED_FOR_CDSI_CODEBOOK | DESCRIPTION                                                                                                      |
|------------------------------------|----------------------------------------|-------------------------------|------------------------------------------------------------------------------------------------------------------|
| record_id                          |                                        |                               | Redcap annotation. Denotes project number                                                                        |
| redcap_repeat_instrument           |                                        |                               | Redcap annotation.                                                                                               |
| redcap_repeat_instance             |                                        |                               | Redcap annotation.                                                                                               |
| project_name                       |                                        |                               | Designator for the project. This must be the same as the tables tab.                                              |
| project_display_name               |                                        |                               | Display name for the project. This will go on our website.                                                       |
| project_description                |                                        |                               | Description for the project. This will go on our website.                                                        |
| project_data_status                |                                        |                               | Status for data generation for this project                                                                      |
| project_blog_link                  |                                        |                               | Website link for the blog                                                                                        |

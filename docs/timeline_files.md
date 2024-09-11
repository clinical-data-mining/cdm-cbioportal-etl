# Transformation of Timeline Data Attributes
The `cdm-cbioportal-etl.timeline.cbioportal_deid_timeline_files` function is designed to de-identify patient timeline files and save the processed data to object storage using the MinioAPI. It replaces identifiable patient data (dates and MRNs) with de-identified values based on anchor dates and organizes the data for later use in cBioPortal.

The resulting data format adheres to [cBioPortal's formatting for timeline files](https://docs.cbioportal.org/file-formats/#timeline-data)

## Parameters:
`fname_minio_env`: A file that contains Minio environment configurations (used to load and save data from object storage).

`dict_files_timeline`: A dictionary containing timeline file paths (with identifiable information) as keys and corresponding file names for the de-identified output files as values.

## Key Steps:
- Load Anchor Dates: The function retrieves anchor dates via the get_anchor_dates() utility, which provides a DataFrame that maps medical record numbers (MRNs) to anchor dates (used for de-identification).

- MinioAPI Setup: A Minio object is instantiated using MinioAPI(fname_minio_env), allowing the function to load and save files from/to object storage.

- Process each timeline file in dictionary: For each file in `dict_files_timeline`, the function:

  - Loads the timeline data (which contains patient information and protected health information - PHI) from Minio.
  - Ensures the presence of the STOP_DATE column (if not present, it adds a blank column).
  - De-identifying Dates: Converts START_DATE and STOP_DATE columns to datetime format.
  Merges the timeline data with the anchor dates on MRNs to replace actual dates with relative days (differences between the timeline dates and the anchor date).

  - Reorganizing Columns: The columns are rearranged based on the order defined in COLS_ORDER_GENERAL (PATIENT_ID, START_DATE, STOP_DATE, EVENT_TYPE, and SUBTYPE) followed by other metadata.

  - Saving De-identified Data: The processed, de-identified data is saved back to object storage using save_appended_df.


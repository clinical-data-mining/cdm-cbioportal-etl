# Data Formatting Requirements for Generating cBioPortal Files
Curators of cBioPortal must follow these file formats for the `cdm-cbioportal-etl` process to transform data correctly.
For more info describing the cBioPortal format is described here for [summaries](https://docs.cbioportal.org/file-formats/#clinical-data) and [timelines](https://docs.cbioportal.org/file-formats/#timeline-data).

**Files without one or more of these will have unexpected outcomes or result in an error.**

## File Requirements
### Summary Files

Individual summary files must:
- Be a tab-delimited file 
- Contain one column representing the patient or sample ID. 
  - Patient IDs: Acceptable column names are `MRN` or `DMP_ID` (MSK-IMPACT patient ID)
  - Sample IDs: Column name must be `SAMPLE_ID` (MSK-IMPACT Sample ID)
- Have one additional column containing data to be hosted in the final summary file (Also see [Modifying the Codebook](modifying_the_codebook.md))
- Have one row per patient

The `cdm-cbioportal-etl` package have modules to combine the individual summary files into one for import.


### Timeline Files
Timeline files describe events during a patient's cancer treatment. Typically, timeline files contains at least one row per patient. For example, a patient can have several radiology scans during treatment, and may be good to visualize on the timeline.

Timeline files follow cBioPortal's [timeline format](https://docs.cbioportal.org/file-formats/#timeline-data).

Your timeline files have the following column names (All caps required):
#### Required columns
- A patient identifier. Only `MRN` or `DMP_ID` (MSK-IMPACT Sample ID) are accepted.
- START_DATE: Actual start date of event such as an MM/DD/YYYY format
- STOP_DATE: Actual stop or end of event such as an MM/DD/YYYY format. Leave an empty string if this doesn't apply or equal to start date.
- EVENT_TYPE: The category of the event. This will be the highest level track label. Acceptable `EVENT_TYPE` values are described [here](https://docs.cbioportal.org/file-formats/#event-types)
  - If an `EVENT_TYPE` outside of the acceptable value is used, you will cause an cBioPortal import error! 

#### Metadata columns
- SUBTYPE: Depending upon the `EVENT_TYPE`, this will be used as the subtrack label under `EVENT_TYPE`
  - Not all `EVENT_TYPE` categories use SUBTYPE
- Additional subtrack labels: For example, if `EVENT_TYPE` == 'TREATMENT', additional subtracks will be created. See full timeline formatting info for proper labeling.
- Other metadata: All other metadata will appear in the hover-over information in the portal study timeline. Again, these column names must be in ALL_CAPS


#### Optional columns with special effects
- STYLE_SHAPE: when this column has a valid value, this event will be rendered using that shape. The valid shapes are circle, square, triangle, diamond, star, and camera.
- STYLE_COLOR: when this column has a hexadecimal color value (e.g. #ffffff), it will be used as the color for rendering this event.






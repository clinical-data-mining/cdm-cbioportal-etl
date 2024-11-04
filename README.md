# cdm-cbioportal-etl
ETL processing to transform CDM data into cBioPortal formatted files


## Requirements
This package requires use of the [`msk_cdm` Python package](https://github.com/clinical-data-mining/msk_cdm).

## Using/installing this repo.
### Direct installation into your virtual environment
The simplest way to use this repo is to set up a conda env or environment of your choice
and then, after `git clone`-ing this directory, simply issue

```
pip install .
```


## I'm here to add data to an existing pipeline!
As long as your data is in the correct format and required info is added to the codebook, data will be formatted in the cBioPortal format. For this, follow the steps in [Setup](#processes)   

### Basics about cBioPortal formatted (clinical) data
#### File Types
For cBioPortal clinical data files, there are 3 flavors
1) **Patient summary**: Information about the patient (Female, Living)
2) **Sample summary**: Information specific to a sequenced sample (Cancer type, PD-L1 positive)
3) **Timeline**: Patient events relative to an anchor date (Surgery, cancer progression).

Patient and sample summary data will exist on the summary table of a study, while timeline data will exist on the patient tab. 

Multiple timeline files can be created, but one sample and one patient summary files is required. 

#### How (summary) files are generated
This is important to mention mainly for summary files since only one file generated, yet there are separate efforts that may generate only one column within the summary. 

These separate efforts generate individual files. A PD-L1 extractor will produce a file and a demographics file will create another. **The combination of the metadata in the codebook and this code will combine individual summary files appropriately into summary files.** 

The result will be cBioPortal summary files that be directly imported into cBioPortal.

#### Timeline file generation is simple
Timeline file generation is a much simpler, one-in-one-out process where each bespoke file feed into code will result in one cBioPortal formatted file out.   


## Processes
### Setup
1) [Data File Formatting](docs/data_file_formatting.md)
    - Data files formatted one patient or sample per row 
    - "Timeline" files formatted one event per row  
2) [Updating the Codebook](docs/modifying_the_codebook.md)
   - CDM uses [this codebook](https://docs.google.com/spreadsheets/d/1po0GdSwqmmXibz4e-7YvTPUbXpi0WYv3c2ImdHXxyuc/edit?usp=sharing) to determine data files to be used for the cBioPortal ETL, including relevant metadata

### Generation of cBioPortal formatted files

1) [Summary (Patient or Sample) Template File Generation]()
2) [Summary File Formatting and Combining]()
3) [De-identification and Transformation of Timeline Data Attributes]()

### Workflow Diagram
![cdm-cbioportal-etl workflow](https://github.com/clinical-data-mining/cdm-cbioportal-etl/blob/main/docs/CDM-cBioPortal-ETL%20Process.png)





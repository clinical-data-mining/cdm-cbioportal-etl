# Modifying the Codebook
The [CDM Codebook](https://docs.google.com/spreadsheets/d/1po0GdSwqmmXibz4e-7YvTPUbXpi0WYv3c2ImdHXxyuc/edit?usp=sharing) is used to describe data tables and their columns. 

For `cdm-cbioportal-etl` purposes, the codebook is used for code to process information to transform and import data into a cBioPortal study properly.

## Codebook Columns to Modify for the ETL
The description tab (1st tab) contains a list of all columns in the codebook along with annotations for what columns are required for the ETL.

The other columns are informational to users and creators of the data, but not necessarily needed for ETL processing.

## Integrating the Codebook into the ETL Process
After modifying the codebook, the tables need to be `git` pushed to the [CDSI docs repo](https://github.mskcc.org/cdsi/docs/tree/main/docs/tables). There individual files for the metadata, table, and project tabs.

### Downloading the Codebook Files
This process can be done by downloading the Codebook tabs individually to the `docs` repo and commiting the changes.

### Committing Codebook Changes
Navigate to your `docs` repo and push changes to github. Here's how to get started:
```bash
git clone https://github.mskcc.org/cdsi/docs.git
cd docs/

# Modify the codebook and download changes to /path/to/docs/docs/tables
git add -u    # Add all changes
git commit -m "Updating metadata page"
git push origin main

```

### Propogate Changes to the CDSI Server
After pushing the changes to the `docs` repo, pull the changes to the CDSI server.

> **Note:** Pulling codebook changes to the CDSI servers will be an automated process in a future version. 

To properly pull the codebook changes to the CDSI server, run
```bash
cd /gpfs/mindphidata/cdm_repos/github/docs
git pull
```

## You're Done!
Once changes are pulled, you're ready to run the ETL!




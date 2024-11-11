"""
cmd-cbioportal-summary-creator.py

Create summary files and corresponding headers from CDM data, 
then combine into a single file to be pushed to cbioportal

"""
import os
import argparse
from pathlib import Path

import pandas as pd

from cdm_cbioportal_etl.summary import cbioportalSummaryFileCombiner
from cdm_cbioportal_etl.summary import RedcapToCbioportalFormat
from cdm_cbioportal_etl.utils import cbioportal_update_config
from msk_cdm.databricks import DatabricksAPI


def create_cbioportal_summary(
    fname_minio_env,
    dict_databricks,
    patient_or_sample,
    fname_manifest, 
    fname_summary_template,
    fname_summary_save,
    production_or_test,
    path_minio_summary_intermediate,
    fname_meta_data,
    fname_meta_table,
    fname_meta_project
):
    obj_format_cbio = RedcapToCbioportalFormat(
        fname_minio_env=fname_minio_env,
        path_minio_summary_intermediate=path_minio_summary_intermediate,
        fname_metadata=fname_meta_data,
        fname_metaproject=fname_meta_project,
        fname_metatables=fname_meta_table

    )
    
    ## Create individual summary and header files, with a manifest file summarizing the outputs
    obj_format_cbio.create_summaries_and_headers(
        patient_or_sample=patient_or_sample,
        fname_manifest=fname_manifest,
        fname_template=fname_summary_template,
        production_or_test=production_or_test
    )

    ## Merge summaries and headers
    obj_p_combiner = cbioportalSummaryFileCombiner(
        fname_minio_env=fname_minio_env,
        fname_manifest=fname_manifest, 
        fname_current_summary=fname_summary_template,
        patient_or_sample=patient_or_sample,
        production_or_test=production_or_test
    )

    # Fill values in summary file
    obj_p_combiner.backfill_missing_data(fname_meta_data=fname_meta_data)
    df_cbio_summary = obj_p_combiner.return_final()
    df_cbio_summary['Ethnicity'].value_counts()

    # Save the merged summaries to file
    print('Saving summary file: %s' % fname_summary_save)
    obj_p_combiner.save_update(fname=fname_summary_save)



def main():
    parser = argparse.ArgumentParser(description="Wrapper for creating patient and sample summary files for cBioPortal.")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()
    path_minio_summary_intermediate = obj_yaml.return_intermediate_folder_path()
    fname_meta_data = obj_yaml.return_filename_codebook_metadata()
    fname_meta_project = obj_yaml.return_filename_codebook_projects()
    fname_meta_table = obj_yaml.return_filename_codebook_tables()
    production_or_test = obj_yaml.return_production_or_test_indicator()

    # Databricks configs
    dict_databricks = obj_yaml.return_databricks_configs()

    fname_manifest_patient = obj_yaml.return_manifest_filename_patient()
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_p_sum_template_cdsi']
    fname_summary_patient = obj_yaml.return_filenames_deid_datahub()['summary_patient']

    fname_manifest_sample = obj_yaml.return_manifest_filename_sample()
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_s_sum_template_cdsi']
    fname_summary_sample = obj_yaml.return_filenames_deid_datahub()['summary_sample']


    # Create patient summary
    patient_or_sample = 'patient'
    create_cbioportal_summary(
        fname_minio_env=fname_minio_env,
        dict_databricks=dict_databricks,
        patient_or_sample=patient_or_sample,
        fname_manifest=fname_manifest_patient,
        fname_summary_template=fname_summary_template_patient,
        fname_summary_save=fname_summary_patient,
        production_or_test=production_or_test,
        path_minio_summary_intermediate=path_minio_summary_intermediate,
        fname_meta_data = fname_meta_data,
        fname_meta_table = fname_meta_table,
        fname_meta_project = fname_meta_project

    )

    # Create sample summary
    patient_or_sample = 'sample'
    create_cbioportal_summary(
        fname_minio_env = fname_minio_env,
        dict_databricks=dict_databricks,
        patient_or_sample=patient_or_sample,
        fname_manifest=fname_manifest_sample,
        fname_summary_template=fname_summary_template_sample,
        fname_summary_save=fname_summary_sample,
        production_or_test=production_or_test,
        path_minio_summary_intermediate=path_minio_summary_intermediate,
        fname_meta_data = fname_meta_data,
        fname_meta_table = fname_meta_table,
        fname_meta_project = fname_meta_project
    )

if __name__ == "__main__":
    main()
    
    
  
"""
cmd-cbioportal-summary-creator.py

Create summary files and corresponding headers from CDM data, 
then combine into a single file to be pushed to cbioportal

"""
import sys
import os
import argparse

from cdm_cbioportal_etl.summary import cbioportalSummaryFileCombiner
from cdm_cbioportal_etl.summary import RedcapToCbioportalFormat
from cdm_cbioportal_etl.utils import yaml_config_parser
# sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
# # User defined variables
# from variables import (
#     FNAME_MANIFEST_PATIENT,
#     FNAME_MANIFEST_SAMPLE,
#     FNAME_SUMMARY_TEMPLATE_P,
#     FNAME_SUMMARY_TEMPLATE_S,
#     FNAME_SUMMARY_P,
#     FNAME_SUMMARY_S,
#     ENV_MINIO,
#     FNAME_METADATA,
#     FNAME_PROJECT,
#     FNAME_TABLES,
#     PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE
# )


def create_cbioportal_summary(
    fname_minio_env,
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
    
    # Save the merged summaries to file
    obj_p_combiner.save_update(fname=fname_summary_save)
    df_cbio_summary = obj_p_combiner.return_final()


def main():
    parser = argparse.ArgumentParser(description="Wrapper for creating patient and sample summary files for cBioPortal.")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    # parser.add_argument(
    #     "--fname_manifest_patient",
    #     action="store",
    #     dest="fname_manifest_patient",
    #     default=FNAME_MANIFEST_PATIENT,
    #     help="CSV file containing list of patient level summary files.",
    # )
    # parser.add_argument(
    #     "--fname_manifest_sample",
    #     action="store",
    #     dest="fname_manifest_sample",
    #     default=FNAME_MANIFEST_SAMPLE,
    #     help="CSV file containing list of sample level summary files.",
    # )
    # parser.add_argument(
    #     "--fname_summary_template_patient",
    #     action="store",
    #     dest="fname_summary_template_patient",
    #     default=FNAME_SUMMARY_TEMPLATE_P,
    #     help="TSV template file containing list of patient IDs.",
    # )
    # parser.add_argument(
    #     "--fname_summary_template_sample",
    #     action="store",
    #     dest="fname_summary_template_sample",
    #     default=FNAME_SUMMARY_TEMPLATE_S,
    #     help="TSV template file containing list of sample IDs.",
    # )
    # parser.add_argument(
    #     "--fname_summary_patient",
    #     action="store",
    #     dest="fname_summary_patient",
    #     default=FNAME_SUMMARY_P,
    #     help="Output file for the patient level summary to be pushed to cBioPortal.",
    # )
    # parser.add_argument(
    #     "--fname_summary_sample",
    #     action="store",
    #     dest="fname_summary_sample",
    #     default=FNAME_SUMMARY_S,
    #     help="Output file for the sample level summary to be pushed to cBioPortal.",
    # )
    # parser.add_argument(
    #     "--production_or_test",
    #     action="store",
    #     dest="production_or_test",
    #     default='production',
    #     help="For logic to decide if production portal or testing portal will be updated.",
    # )
    args = parser.parse_args()

    obj_yaml = yaml_config_parser(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()
    path_minio_summary_intermediate = obj_yaml.return_intermediate_folder_path()
    fname_meta_data = obj_yaml.return_filename_codebook_metadata()
    fname_meta_project = obj_yaml.return_filename_codebook_projects()
    fname_meta_table = obj_yaml.return_filename_codebook_tables()
    production_or_test = obj_yaml.return_production_or_test_indicator()

    fname_manifest_patient = obj_yaml.return_manifest_filename_patient()
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_cbio_header_template_p']
    fname_summary_patient = obj_yaml.return_filenames_deid_datahub()['summary_patient']

    fname_manifest_sample = obj_yaml.return_manifest_filename_sample()
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_cbio_header_template_s']
    fname_summary_sample = obj_yaml.return_filenames_deid_datahub()['summary_sample']


    # fname_minio_env = ENV_MINIO
    # path_minio_summary_intermediate = PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE
    # fname_meta_data = FNAME_METADATA
    # fname_meta_project = FNAME_PROJECT
    # fname_meta_table = FNAME_TABLES
    #

    # Create patient summary
    patient_or_sample = 'patient'
    create_cbioportal_summary(
        fname_minio_env = fname_minio_env,
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
    
    
  
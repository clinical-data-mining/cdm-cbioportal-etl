"""
cmd-cbioportal-summary-creator.py

Create summary files and corresponding headers from CDM data, 
then combine into a single file to be pushed to cbioportal

"""
import argparse

from cdm_cbioportal_etl.summary import cbioportalSummaryFileCombiner
from cdm_cbioportal_etl.summary import RedcapToCbioportalFormat
from cdm_cbioportal_etl.utils import cbioportal_update_config


def create_cbioportal_summary(
    fname_minio_env,
    patient_or_sample,
    fname_manifest, 
    fname_summary_template,
    fname_summary_save,
    production_or_test,
    path_minio_summary_intermediate,
    fname_metadata=fname_metadata,
    fname_tables=fname_tables
):
    obj_format_cbio = RedcapToCbioportalFormat(
        fname_minio_env=fname_minio_env,
        path_minio_summary_intermediate=path_minio_summary_intermediate,
        fname_metadata=fname_metadata,
        fname_tables=fname_tables
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
    parser.add_argument(
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    parser.add_argument(
        "--path_datahub",
        action="store",
        dest="path_datahub",
        required=True,
        help="Path to datahub",
    )
    parser.add_argument(
        "--production_or_test",
        action="store",
        dest="production_or_test",
        required=True,
        help="Enter test or production to indicate the columns/files to use for portal file generation",
    )
    args = parser.parse_args()

    production_or_test = args.production_or_test
    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    path_minio_summary_intermediate = obj_yaml.return_intermediate_folder_path()

    fname_metadata = obj_yaml.return_filename_codebook_metadata()
    fname_tables = obj_yaml.return_filename_codebook_tables()
    fname_manifest_patient = obj_yaml.return_manifest_filename_patient()
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_p_sum_template_cdsi']
    fname_summary_patient = obj_yaml.return_filenames_deid_datahub(path_datahub=args.path_datahub)['summary_patient']

    fname_manifest_sample = obj_yaml.return_manifest_filename_sample()
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_s_sum_template_cdsi']
    fname_summary_sample = obj_yaml.return_filenames_deid_datahub(path_datahub=args.path_datahub)['summary_sample']


    # Create patient summary
    patient_or_sample = 'patient'
    create_cbioportal_summary(
        fname_minio_env=args.minio_env,
        patient_or_sample=patient_or_sample,
        fname_manifest=fname_manifest_patient,
        fname_summary_template=fname_summary_template_patient,
        fname_summary_save=fname_summary_patient,
        production_or_test=production_or_test,
        path_minio_summary_intermediate=path_minio_summary_intermediate,
        fname_metadata=fname_metadata,
        fname_tables=fname_tables
    )

    # Create sample summary
    patient_or_sample = 'sample'
    create_cbioportal_summary(
        fname_minio_env = args.minio_env,
        patient_or_sample=patient_or_sample,
        fname_manifest=fname_manifest_sample,
        fname_summary_template=fname_summary_template_sample,
        fname_summary_save=fname_summary_sample,
        production_or_test=production_or_test,
        path_minio_summary_intermediate=path_minio_summary_intermediate,
        fname_metadata=fname_metadata,
        fname_tables=fname_tables
    )

if __name__ == "__main__":
    main()
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
from cdm_cbioportal_etl.utils import constants
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from variables import (
    FNAME_MANIFEST_PATIENT,
    FNAME_MANIFEST_SAMPLE,
    FNAME_SUMMARY_TEMPLATE_P,
    FNAME_SUMMARY_TEMPLATE_S,
    FNAME_SUMMARY_P,
    FNAME_SUMMARY_S,
    FNAME_SUMMARY_P_MINIO,
    FNAME_SUMMARY_S_MINIO,
    ENV_MINIO,
    FNAME_METADATA,
    FNAME_PROJECT,
    FNAME_TABLES,
    PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE
)

COL_SUMMARY_FNAME_SAVE = constants.COL_SUMMARY_FNAME_SAVE
COL_SUMMARY_HEADER_FNAME_SAVE = constants.COL_SUMMARY_HEADER_FNAME_SAVE


def create_cbioportal_summary(
    fname_minio_env,
    patient_or_sample,
    fname_manifest, 
    fname_current_summary, 
    fname_new_summary,
    production_or_test,
    fname_save_var_summary,
    fname_save_header_summary,
    path_minio_summary_intermediate
):
    obj_format_cbio = RedcapToCbioportalFormat(
        fname_minio_env=fname_minio_env,
        path_minio_summary_intermediate=path_minio_summary_intermediate

    )
    
    ## Create individual summary and header files, with a manifest file summarizing the outputs
    obj_format_cbio.create_summaries_and_headers(
        patient_or_sample=patient_or_sample,
        fname_manifest=fname_manifest,
        fname_template=fname_current_summary,
        production_or_test=production_or_test
    )
    
    ## Merge summaries and headers
    obj_p_combiner = cbioportalSummaryFileCombiner(
        fname_minio_env=fname_minio_env,
        fname_manifest=fname_manifest, 
        fname_current_summary=fname_current_summary, 
        patient_or_sample=patient_or_sample,
        production_or_test=production_or_test,
        fname_save_var_summary=fname_save_var_summary,
        fname_save_header_summary=fname_save_header_summary

    )
    
    # Save the merged summaries to file
    obj_p_combiner.save_update(fname=fname_new_summary)
    df_cbio_summary = obj_p_combiner.return_final()


def main():
    parser = argparse.ArgumentParser(description="Wrapper for creating patient and sample summary files for cBioPortal.")
    parser.add_argument(
        "--fname_manifest_patient",
        action="store",
        dest="fname_manifest_patient",
        default=FNAME_MANIFEST_PATIENT,
        help="CSV file containing list of patient level summary files.",
    )
    parser.add_argument(
        "--fname_manifest_sample",
        action="store",
        dest="fname_manifest_sample",
        default=FNAME_MANIFEST_SAMPLE,
        help="CSV file containing list of patient level summary files.",
    )
    parser.add_argument(
        "--fname_summary_template_patient",
        action="store",
        dest="fname_summary_template_patient",
        default=FNAME_SUMMARY_TEMPLATE_P,
        help="TSV template file containing list of patient IDs.",
    )
    parser.add_argument(
        "--fname_summary_template_sample",
        action="store",
        dest="fname_summary_template_sample",
        default=FNAME_SUMMARY_TEMPLATE_S,
        help="TSV template file containing list of sample IDs.",
    )
    parser.add_argument(
        "--fname_summary_patient",
        action="store",
        dest="fname_summary_patient",
        default=FNAME_SUMMARY_P,
        help="Output file for the patient level summary to be pushed to cBioPortal.",
    )
    parser.add_argument(
        "--fname_summary_sample",
        action="store",
        dest="fname_summary_sample",
        default=FNAME_SUMMARY_S,
        help="Output file for the sample level summary to be pushed to cBioPortal.",
    )
    parser.add_argument(
        "--production_or_test",
        action="store",
        dest="production_or_test",
        default='production',
        help="For logic to decide if production portal or testing portal will be updated.",
    )
    parser.add_argument(
        "--fname_save_var_summary",
        action="store",
        dest="fname_save_var_summary",
        default=COL_SUMMARY_FNAME_SAVE,
        help=".",
    )
    parser.add_argument(
        "--fname_save_header_summary",
        action="store",
        dest="fname_save_header_summary",
        default=COL_SUMMARY_HEADER_FNAME_SAVE,
        help=".",
    )

    # TODO Put the following variables in the argparsing
    fname_minio_env = ENV_MINIO
    path_minio_summary_intermediate = PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE
    
    args = parser.parse_args()
    # Create patient summary
    patient_or_sample = 'patient'
    create_cbioportal_summary(
        fname_minio_env = fname_minio_env,
        patient_or_sample=patient_or_sample,
        fname_manifest=args.fname_manifest_patient, 
        fname_current_summary=args.fname_summary_template_patient, 
        fname_new_summary=args.fname_summary_patient,
        production_or_test=args.production_or_test,
        fname_save_var_summary=args.fname_save_var_summary,
        fname_save_header_summary=args.fname_save_header_summary,
        path_minio_summary_intermediate=path_minio_summary_intermediate
    )

    # Create sample summary
    patient_or_sample = 'sample'
    create_cbioportal_summary(
        fname_minio_env = fname_minio_env,
        patient_or_sample=patient_or_sample,
        fname_manifest=args.fname_manifest_sample, 
        fname_current_summary=args.fname_summary_template_sample, 
        fname_new_summary=args.fname_summary_sample,
        production_or_test=args.production_or_test,
        fname_save_var_summary=args.fname_save_var_summary,
        fname_save_header_summary=args.fname_save_header_summary,
        path_minio_summary_intermediate=path_minio_summary_intermediate
    )

if __name__ == "__main__":
    main()
    
    
  
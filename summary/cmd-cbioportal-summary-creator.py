"""
cmd-cbioportal-summary-creator.py


Create summary files and corresponding headers from CDM data, 
then combine into a single file to be pushed to cbioportal


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
from data_classes_cdm import CDMProcessingVariables as config_cdm
from cbioportal_summary_file_combiner import cbioportalSummaryFileCombiner
from create_summary_from_redcap_reports import RedcapToCbioportalFormat
from minio_api import MinioAPI
from constants import (
    FNAME_MANIFEST_PATIENT,
    FNAME_MANIFEST_SAMPLE,
    FNAME_SUMMARY_TEMPLATE_P,
    FNAME_SUMMARY_TEMPLATE_S,
    FNAME_SUMMARY_P,
    FNAME_SUMMARY_S,
    FNAME_SUMMARY_P_MINIO,
    FNAME_SUMMARY_S_MINIO,
    ENV_MINIO
) 


def create_cbioportal_summary(
    patient_or_sample,
    fname_manifest, 
    fname_current_summary, 
    fname_new_summary
):
    obj_format_cbio = RedcapToCbioportalFormat()
    
    ## Create individual summary and header files, with a manifest file summarizing the outputs
    obj_format_cbio.create_summaries_and_headers(
        patient_or_sample=patient_or_sample
    )
    
    ## Merge summaries and headers
    obj_p_combiner = cbioportalSummaryFileCombiner(
        fname_minio_env=ENV_MINIO,
        fname_manifest=fname_manifest, 
        fname_current_summary=fname_current_summary, 
        patient_or_sample=patient_or_sample
    )
    
    # Save the merged summaries to file
    obj_p_combiner.save_update(fname=fname_new_summary)
    df_cbio_summary = obj_p_combiner.return_final()


def main():

    # Create patient summary
    patient_or_sample = 'patient'
    create_cbioportal_summary(
        patient_or_sample=patient_or_sample,
        fname_manifest=FNAME_MANIFEST_PATIENT, 
        fname_current_summary=FNAME_SUMMARY_TEMPLATE_P, 
        fname_new_summary=FNAME_SUMMARY_P
    )

    # Create sample summary
    patient_or_sample = 'sample'
    create_cbioportal_summary(
        patient_or_sample=patient_or_sample,
        fname_manifest=FNAME_MANIFEST_SAMPLE, 
        fname_current_summary=FNAME_SUMMARY_TEMPLATE_S, 
        fname_new_summary=FNAME_SUMMARY_S
    )

if __name__ == "__main__":
    main()
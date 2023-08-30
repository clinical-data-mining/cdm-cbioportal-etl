"""
cdm-cbioportal-summary-merging.py


Notes:

See 
Summary: ../cdm-utilities/cbioportal/mind-cdm-cbioportal-summary-merger.ipynb


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
from data_classes_cdm import CDMProcessingVariables as config_cdm
from cbioportal_summary_file_combiner import cbioportalSummaryFileCombiner
from minio_api import MinioAPI


## Define hardcoded variables
### Filenames
#### Redcap report manifest files
FNAME_MANIFEST_PATIENT = '/cbioportal/summary_manifest_patient.csv'
FNAME_MANIFEST_SAMPLE = 'cbioportal/summary_manifest_sample_test.csv'
#### Current summary files
FNAME_SUMMARY_TEMPLATE_P = config_cdm.fname_p_sum_template
FNAME_SUMMARY_TEMPLATE_S = config_cdm.fname_s_sum_template
#### Summary files to save (datahub)
FNAME_SUMMARY_P = config_cdm.fname_summary_patient + '.tsv'
FNAME_SUMMARY_S = config_cdm.fname_summary_sample + '.tsv'
#### Summary files to save (MinIO)
FNAME_SUMMARY_P_MINIO = config_cdm.fname_summary_patient_minio
FNAME_SUMMARY_S_MINIO = config_cdm.fname_summary_sample_minio
# MINIO
ENV_MINIO = config_cdm.minio_env


## Patient summary merging
obj_p_combiner = cbioportalSummaryFileCombiner(
    fname_minio_env=ENV_MINIO,
    fname_manifest=FNAME_MANIFEST_PATIENT, 
    fname_current_summary=FNAME_SUMMARY_TEMPLATE_P, 
    patient_or_sample='patient'
)

obj_p_combiner.save_update(fname=FNAME_SUMMARY_P)
df_cbio_summary_p = obj_p_combiner.return_final()


## Sample summary merging
obj_s_combiner = cbioportalSummaryFileCombiner(
    fname_minio_env=ENV_MINIO,
    fname_manifest=FNAME_MANIFEST_SAMPLE, 
    fname_current_summary=FNAME_SUMMARY_TEMPLATE_S, 
    patient_or_sample='sample'
)

obj_s_combiner.save_update(fname=FNAME_SUMMARY_S)
df_cbio_summary_s = obj_s_combiner.return_final()


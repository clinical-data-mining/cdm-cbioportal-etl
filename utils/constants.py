import sys
import os

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "cdm-utilities")
    ),
)
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from data_classes_cdm import CDMProcessingVariables as config_cdm


# Data dictionary
path = '/mind_data/cdm_repos/cdm-utilities/docs/dev/'
FNAME_METADATA = os.path.join(path, 'CDM-Codebook - metadata.csv')
FNAME_PROJECT = os.path.join(path, 'CDM-Codebook - project.csv')
FNAME_TABLES = os.path.join(path, 'CDM-Codebook - tables.csv')

### Filenames
#### Redcap report manifest files
FNAME_MANIFEST_PATIENT = 'cbioportal/summary_manifests/summary_manifest_patient.csv'
FNAME_MANIFEST_SAMPLE = 'cbioportal/summary_manifests/summary_manifest_sample.csv'
### Intermediate summary file and header location
PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE = 'cbioportal/intermediate_files/'
#### Current summary files
FNAME_SUMMARY_TEMPLATE_P = config_cdm.fname_p_sum_template
FNAME_SUMMARY_TEMPLATE_S = config_cdm.fname_s_sum_template
#### Summary files to save (datahub)
FNAME_SUMMARY_P = config_cdm.fname_summary_patient
FNAME_SUMMARY_S = config_cdm.fname_summary_sample
#### Summary files to save (MinIO)
FNAME_SUMMARY_P_MINIO = config_cdm.fname_summary_patient_minio
FNAME_SUMMARY_S_MINIO = config_cdm.fname_summary_sample_minio
ENV_MINIO=config_cdm.minio_env
### Columns of interest for joining or removing
COL_PID = 'DMP_ID'
COL_PID_CBIO = 'PATIENT_ID'

### Column names for the manifest file
COL_SUMMARY_FNAME_SAVE = 'SUMMARY_FILENAME'
COL_SUMMARY_HEADER_FNAME_SAVE = 'SUMMARY_HEADER_FILENAME'
COL_RPT_NAME = 'REPORT_NAME'



"""
For use in `utils/cmd_cbioportal_copy_to_minio.py`
Dictionary of files to copy from datahub to Minio
"""
DICT_FILES_TO_COPY = {
    config_cdm.fname_summary_patient: config_cdm.fname_summary_patient_minio,                  
    config_cdm.fname_summary_sample: config_cdm.fname_summary_sample_minio,
    config_cdm.fname_save_surg_timeline: config_cdm.fname_save_surg_timeline_minio,
    config_cdm.fname_save_rt_timeline: config_cdm.fname_save_rt_timeline_minio,
    config_cdm.fname_save_meds_timeline: config_cdm.fname_save_meds_timeline_minio,
    config_cdm.fname_save_disease_status_timeline: config_cdm.fname_save_disease_status_timeline_minio,
    config_cdm.fname_save_dx_prim_timeline: config_cdm.fname_save_dx_prim_timeline_minio,
    config_cdm.fname_save_dx_met_timeline: config_cdm.fname_save_dx_met_timeline_minio,
    config_cdm.fname_save_dx_ln_timeline: config_cdm.fname_save_dx_ln_timeline_minio,
    config_cdm.fname_save_spec_timeline: config_cdm.fname_save_spec_timeline_minio,
    config_cdm.fname_save_progression: config_cdm.fname_save_progression_minio,
    config_cdm.fname_save_labs_cea: config_cdm.fname_save_labs_cea_minio
    
    
    
}

fname_minio_env = config_cdm.minio_env


"""
Constants for cBioPortal timeline files
"""
# Dictionary of files to convert to a cbioportal timeline data file format, from a PHI version of it.
# Columns must AT LEAST contain the columns as defined in COLS_ORDER
DICT_FILES_TIMELINE = {
    config_cdm.fname_dx_timeline_prim: config_cdm.fname_save_dx_prim_timeline,
    config_cdm.fname_dx_timeline_met: config_cdm.fname_save_dx_met_timeline,
    config_cdm.fname_dx_timeline_ln: config_cdm.fname_save_dx_ln_timeline,
    config_cdm.fname_timeline_surg: config_cdm.fname_save_surg_timeline,
    config_cdm.fname_timeline_rt: config_cdm.fname_save_rt_timeline,
    config_cdm.fname_timeline_meds: config_cdm.fname_save_meds_timeline
}
COLS_ORDER_GENERAL = [
    'PATIENT_ID', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE'
]


# Constants for custom built timeline file generators

## IMPACT sequencing and surgical specimens
FNAME_CBIO_SID = config_cdm.fname_cbio_sid
FNAME_IMPACT_SUMMARY_SAMPLE = config_cdm.fname_impact_summary_sample
FNAME_SAVE_TIMELINE_SEQ = config_cdm.fname_save_spec_timeline
FNAME_SAVE_TIMELINE_SPEC = config_cdm.fname_save_spec_surg_timeline
COL_ORDER_SEQ = [
    'PATIENT_ID', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SAMPLE_ID', 
    'CANCER_TYPE', 
    'CANCER_TYPE_DETAILED', 
    'SAMPLE_TYPE'
]
COL_ORDER_SPEC = [
    'PATIENT_ID', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SAMPLE_ID', 
    'CANCER_TYPE', 
    'CANCER_TYPE_DETAILED', 
    'SAMPLE_TYPE', 
    'SURGICAL_METHOD'
]

## CEA Labs
FNAME_SAVE_CEA = config_cdm.fname_save_labs_cea
FILE_CEA = 'labs/CA_Antigen_PSA_TSH__with_TESTNAMES_as_Select_LCI_CTLG_ITEM_GUID_LCI_SUBTEST_NAME_fr_202303151545.csv'
COLS_ORDER_CEA = [
    'PATIENT_ID', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'TEST', 
    'RESULT'
]

## Cancer progression predictions
FNAME_PROGRESSION = config_cdm.fname_radiology_progression_pred
FNAME_RADIOLOGY = config_cdm.fname_radiology_full_parsed
FNAME_SAVE_PROGRESSION_TIMELINE = config_cdm.fname_save_progression

COL_ORDER_PROGRESSION = [
    'PATIENT_ID', 
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'SOURCE_SPECIFIC',
    'PROGRESSION',
    'INFERRED_PROGRESSION_PROB',
    'STYLE_COLOR'
]

## Disease status
COL_ORDER_DISEASE_STATUS = [
    'PATIENT_ID', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE', 
    'SOURCE', 
    'SOURCE_SPECIFIC', 
    'DISEASE_STATUS_PREDICTED', 
    'DISEASE_STATUS_KNOWN', 
    'STYLE_COLOR'
]

FNAME_DEMO = config_cdm.fname_demo, 
FNAME_IDS = config_cdm.fname_id_map,
FNAME_RESULTS_DISEASE_STATUS = '/mind_data/watersm/Projects/cdm_breast_dmt/data/initial_first_met_test_predictions_Clinical_Longform_Breast_DMT_met_recurrence_ft_9-12-2022_epoch_9.csv',
FNAME_RESULTS_STATS = '/mind_data/watersm/Projects/cdm_breast_dmt/data/percent_met_mrns_stats.csv'
FNAME_SAVE_TIMELINE_DATAHUB = config_cdm.fname_save_disease_status_timeline

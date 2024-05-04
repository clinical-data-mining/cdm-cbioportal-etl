import os

from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl


# Data dictionary
path_docs = '/gpfs/mindphidata/fongc2/github/docs/docs/tables/'
FNAME_METADATA = os.path.join(path_docs, 'CDM-Codebook - metadata.csv')
FNAME_PROJECT = os.path.join(path_docs, 'CDM-Codebook - project.csv')
FNAME_TABLES = os.path.join(path_docs, 'CDM-Codebook - tables.csv')

### Filenames
#### CDSI files!!
#### Summary header template files
PATH_HEADER_SAMPLE = config_cbio_etl.fname_cbio_header_template_s
PATH_HEADER_PATIENT = config_cbio_etl.fname_cbio_header_template_p
#### Current summary files
FNAME_SUMMARY_TEMPLATE_P = config_cbio_etl.fname_p_sum_template_cdsi
FNAME_SUMMARY_TEMPLATE_S = config_cbio_etl.fname_s_sum_template_cdsi

#### Redcap report manifest files
FNAME_MANIFEST_PATIENT = 'cbioportal/summary_manifests/summary_manifest_patient.csv'
FNAME_MANIFEST_SAMPLE = 'cbioportal/summary_manifests/summary_manifest_sample.csv'
### Intermediate summary file and header location
PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE = 'cbioportal/intermediate_files/'
#### Summary files to save (datahub)
FNAME_SUMMARY_P = config_cbio_etl.fname_summary_patient
FNAME_SUMMARY_S = config_cbio_etl.fname_summary_sample
#### Summary files to save (MinIO)
FNAME_SUMMARY_P_MINIO = config_cbio_etl.fname_summary_patient_minio
FNAME_SUMMARY_S_MINIO = config_cbio_etl.fname_summary_sample_minio
# ENV_MINIO=config_cdm.minio_env
ENV_MINIO = '/gpfs/mindphidata/fongc2/minio_env.txt'
### Columns of interest for joining or removing
FNAME_CBIO_SID = '/gpfs/mindphidata/cdm_repos/datahub/impact-data/data_clinical_sample.txt'
FNAME_SAMPLE_REMOVE = os.path.abspath(os.path.join(os.path.dirname( __file__ ), "mskimpact_clinical_data_remove.tsv"))



"""
For use in `utils/cmd_cbioportal_copy_to_minio.py`
Dictionary of files to copy from datahub to Minio
"""
DICT_FILES_TO_COPY = {
    config_cbio_etl.fname_summary_patient: config_cbio_etl.fname_summary_patient_minio,                  
    config_cbio_etl.fname_summary_sample: config_cbio_etl.fname_summary_sample_minio,
    config_cbio_etl.fname_save_surg_timeline: config_cbio_etl.fname_save_surg_timeline_minio,
    config_cbio_etl.fname_save_rt_timeline: config_cbio_etl.fname_save_rt_timeline_minio,
    config_cbio_etl.fname_save_meds_timeline: config_cbio_etl.fname_save_meds_timeline_minio,
    # config_cbio_etl.fname_save_disease_status_timeline: config_cbio_etl.fname_save_disease_status_timeline_minio,
    config_cbio_etl.fname_save_dx_prim_timeline: config_cbio_etl.fname_save_dx_prim_timeline_minio,
    # config_cbio_etl.fname_save_dx_met_timeline: config_cbio_etl.fname_save_dx_met_timeline_minio,
    # config_cbio_etl.fname_save_dx_ln_timeline: config_cbio_etl.fname_save_dx_ln_timeline_minio,
    config_cbio_etl.fname_save_spec_timeline: config_cbio_etl.fname_save_spec_timeline_minio,
    # config_cbio_etl.fname_save_progression: config_cbio_etl.fname_save_progression_minio,
    # config_cbio_etl.fname_save_labs_cea: config_cbio_etl.fname_save_labs_cea_minio,
    config_cbio_etl.fname_save_timeline_gleason: config_cbio_etl.fname_save_timeline_gleason_minio,
    config_cbio_etl.fname_save_timeline_pdl1: config_cbio_etl.fname_save_timeline_pdl1_minio,
    config_cbio_etl.fname_save_timeline_prior_meds: config_cbio_etl.fname_save_timeline_prior_meds_minio,
    config_cbio_etl.fname_save_timeline_tumor_sites: config_cbio_etl.fname_save_timeline_tumor_sites_minio,
    config_cbio_etl.fname_save_spec_surg_timeline: config_cbio_etl.fname_save_spec_surg_timeline_minio,
    config_cbio_etl.fname_save_timeline_follow_up: config_cbio_etl.fname_save_timeline_follow_up_minio
}


"""
Constants for cBioPortal timeline files
"""
# Dictionary of files to convert to a cbioportal timeline data file format, from a PHI version of it.
# Columns must AT LEAST contain the columns as defined in COLS_ORDER_GENERAL
DICT_FILES_TIMELINE = {
    config_cdm.fname_path_sequencing_cbio_timeline: config_cbio_etl.fname_save_spec_timeline,
    config_cdm.fname_path_specimen_surgery_cbio_timeline: config_cbio_etl.fname_save_spec_surg_timeline,
    
    config_cdm.fname_dx_timeline_prim: config_cbio_etl.fname_save_dx_prim_timeline,
    # config_cdm.fname_dx_timeline_met: config_cbio_etl.fname_save_dx_met_timeline,
    # config_cdm.fname_dx_timeline_ln: config_cbio_etl.fname_save_dx_ln_timeline,
    config_cdm.fname_timeline_surg: config_cbio_etl.fname_save_surg_timeline,
    config_cdm.fname_timeline_rt: config_cbio_etl.fname_save_rt_timeline,
    config_cdm.fname_timeline_meds: config_cbio_etl.fname_save_meds_timeline,
    config_cdm.fname_path_gleason_cbio_timeline: config_cbio_etl.fname_save_timeline_gleason,
    config_cdm.fname_path_pdl1_cbio_timeline: config_cbio_etl.fname_save_timeline_pdl1,
    config_cdm.fname_prior_meds_predictions_timeline: config_cbio_etl.fname_save_timeline_prior_meds,
    config_cdm.fname_tumor_sites_timeline_cbio: config_cbio_etl.fname_save_timeline_tumor_sites,
    config_cbio_etl.fname_timeline_fu: config_cbio_etl.fname_save_timeline_follow_up
}


path_datahub_testing = '/gpfs/mindphidata/cdm_repos/github/datahubs/impact_beta_testing/mskimpact_cdm_cdsi/'
timeline_surg = 'data_timeline_surgery.txt'
timeline_rt = 'data_timeline_radiation.txt'
timeline_meds = 'data_timeline_treatment.txt'
timeline_disease_status = 'data_timeline_disease_status.txt'
timeline_dx_primary = 'data_timeline_diagnosis.txt'
timeline_spec = 'data_timeline_specimen.txt'
timeline_spec_surg = 'data_timeline_specimen_surgery.txt'
timeline_gleason = 'data_timeline_gleason.txt'
timeline_pdl1 = 'data_timeline_pdl1.txt'
timeline_prior_meds = 'data_timeline_prior_meds.txt'
timeline_tumor_sites = 'data_timeline_tumor_sites.txt'
timeline_follow_up: str = 'data_timeline_timeline_follow_up.txt'
fname_save_surg_timeline: str = os.path.join(path_datahub_testing, timeline_surg)
fname_save_rt_timeline: str = os.path.join(path_datahub_testing, timeline_rt)
fname_save_meds_timeline: str = os.path.join(path_datahub_testing, timeline_meds)
fname_save_dx_prim_timeline: str = os.path.join(path_datahub_testing, timeline_dx_primary)
fname_save_spec_timeline: str = os.path.join(path_datahub_testing, timeline_spec)
fname_save_spec_surg_timeline: str = os.path.join(path_datahub_testing, timeline_spec_surg)
fname_save_timeline_gleason: str = os.path.join(path_datahub_testing, timeline_gleason)
fname_save_timeline_pdl1: str = os.path.join(path_datahub_testing, timeline_pdl1)
fname_save_timeline_prior_meds: str = os.path.join(path_datahub_testing, timeline_prior_meds)
fname_save_timeline_tumor_sites: str = os.path.join(path_datahub_testing, timeline_tumor_sites)
fname_save_timeline_follow_up: str = os.path.join(path_datahub_testing, timeline_follow_up)

DICT_FILES_TIMELINE_TESTING = {
    config_cdm.fname_path_sequencing_cbio_timeline: fname_save_spec_timeline,
    config_cdm.fname_path_specimen_surgery_cbio_timeline: fname_save_spec_surg_timeline,    
    config_cdm.fname_dx_timeline_prim: fname_save_dx_prim_timeline,
    config_cdm.fname_timeline_surg: fname_save_surg_timeline,
    config_cdm.fname_timeline_rt: fname_save_rt_timeline,
    config_cdm.fname_timeline_meds: fname_save_meds_timeline,
    config_cdm.fname_path_gleason_cbio_timeline: fname_save_timeline_gleason,
    config_cdm.fname_path_pdl1_cbio_timeline: fname_save_timeline_pdl1,
    config_cdm.fname_prior_meds_predictions_timeline: fname_save_timeline_prior_meds,
    config_cdm.fname_tumor_sites_timeline_cbio: fname_save_timeline_tumor_sites,
    config_cbio_etl.fname_timeline_fu: fname_save_timeline_follow_up
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
FNAME_DEMO = config_cdm.fname_demo
FNAME_TIMELINE_FU = config_cbio_etl.fname_timeline_fu
FNAME_OS = 'demographics/overall_survival_cbioportal.tsv'

import os

from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from pathnames import CDMProcessingVariablesCbioportal as config_cbio_etl


## IMPACT sequencing and surgical specimens
FNAME_DEMO = config_cdm.fname_demo
FNAME_TIMELINE_FU = config_cbio_etl.fname_timeline_fu
FNAME_OS = 'demographics/overall_survival_cbioportal.tsv'

# Data dictionary
path_docs = '/gpfs/mindphidata/fongc2/github/docs/docs/tables/'
FNAME_METADATA = os.path.join(path_docs, 'CDM-Codebook - metadata.csv')
FNAME_PROJECT = os.path.join(path_docs, 'CDM-Codebook - project.csv')
FNAME_TABLES = os.path.join(path_docs, 'CDM-Codebook - tables.csv')

### Filenames
#### CDSI testing
path_datahub_testing = '/gpfs/mindphidata/fongc2/datahubs/impact/mskimpact_cdm_cdsi'
#### Summary header template files
PATH_HEADER_SAMPLE = config_cbio_etl.fname_cbio_header_template_s
PATH_HEADER_PATIENT = config_cbio_etl.fname_cbio_header_template_p
#### Current summary files
# TODO move these template files to a testing folder to avoid mix up
FNAME_SUMMARY_TEMPLATE_P = 'cbioportal/intermediate_files/data_clinical_patient_template_testing_study.txt'
FNAME_SUMMARY_TEMPLATE_S = 'cbioportal/intermediate_files/data_clinical_sample_template_testing_study.txt'

fname_timeline_progression_phi = 'radiology/progression/impact/table_timeline_radiology_cancer_progression_predictions.tsv'
fname_timeline_cancer_presence_phi = 'radiology/cancer_presence/impact/table_timeline_cancer_presence.tsv'
fname_timeline_pathology_mmr_phi = 'pathology/table_timeline_mmr_calls.tsv'
fname_timeline_ecog_phi = 'clindoc/ecog/impact/table_timeline_ecog_kps.tsv'

summary_p = "data_clinical_patient.txt"
summary_s = "data_clinical_sample.txt"
timeline_surg = 'data_timeline_surgery.txt'
timeline_rt = 'data_timeline_radiation.txt'
timeline_meds = 'data_timeline_treatment.txt'
timeline_disease_status = 'data_timeline_disease_status.txt'
timeline_dx_primary = 'data_timeline_diagnosis.txt'
timeline_spec = 'data_timeline_specimen.txt'
timeline_spec_surg = 'data_timeline_specimen_surgery.txt'
timeline_gleason = 'data_timeline_gleason.txt'
timeline_pdl1 = 'data_timeline_pdl1.txt'
timeline_pathology_mmr = 'data_timeline_mmr.txt'
timeline_prior_meds = 'data_timeline_prior_meds.txt'
timeline_tumor_sites = 'data_timeline_tumor_sites.txt'
timeline_follow_up: str = 'data_timeline_timeline_follow_up.txt'
timeline_progression: str = 'data_timeline_progression.txt'
timeline_cancer_presence: str = 'data_timeline_cancer_presence.txt'
timeline_ecog_kps: str = 'data_timeline_ecog_kps.txt'
fname_summary_patient: str = os.path.join(path_datahub_testing, summary_p)
fname_summary_sample: str = os.path.join(path_datahub_testing, summary_s)
fname_save_surg_timeline: str = os.path.join(path_datahub_testing, timeline_surg)
fname_save_rt_timeline: str = os.path.join(path_datahub_testing, timeline_rt)
fname_save_meds_timeline: str = os.path.join(path_datahub_testing, timeline_meds)
fname_save_dx_prim_timeline: str = os.path.join(path_datahub_testing, timeline_dx_primary)
fname_save_spec_timeline: str = os.path.join(path_datahub_testing, timeline_spec)
fname_save_spec_surg_timeline: str = os.path.join(path_datahub_testing, timeline_spec_surg)
fname_save_timeline_gleason: str = os.path.join(path_datahub_testing, timeline_gleason)
fname_save_timeline_pdl1: str = os.path.join(path_datahub_testing, timeline_pdl1)
fname_save_timeline_pathology_mmr: str = os.path.join(path_datahub_testing, timeline_pathology_mmr)
fname_save_timeline_prior_meds: str = os.path.join(path_datahub_testing, timeline_prior_meds)
fname_save_timeline_tumor_sites: str = os.path.join(path_datahub_testing, timeline_tumor_sites)
fname_save_timeline_follow_up: str = os.path.join(path_datahub_testing, timeline_follow_up)
fname_save_timeline_progression: str = os.path.join(path_datahub_testing, timeline_progression)
fname_save_timeline_cancer_presence: str = os.path.join(path_datahub_testing, timeline_cancer_presence)
fname_save_timeline_ecog:  str = os.path.join(path_datahub_testing, timeline_ecog_kps)

#### Redcap report manifest files
FNAME_MANIFEST_PATIENT = 'cbioportal/summary_manifests/summary_manifest_patient_testing_study.csv'
FNAME_MANIFEST_SAMPLE = 'cbioportal/summary_manifests/summary_manifest_sample_testing_study.csv'
### Intermediate summary file and header location
PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE = 'cbioportal/intermediate_files_testing_study/'
#### Summary files to save (datahub)
FNAME_SUMMARY_P = fname_summary_patient
FNAME_SUMMARY_S = fname_summary_sample
#### Summary files to save (MinIO)
FNAME_SUMMARY_P_MINIO = os.path.join(PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE, summary_p)
FNAME_SUMMARY_S_MINIO = os.path.join(PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE, summary_s)
# ENV_MINIO=config_cdm.minio_env
ENV_MINIO = '/gpfs/mindphidata/fongc2/minio_env.txt'
### Columns of interest for joining or removing
FNAME_CBIO_SID = '/gpfs/mindphidata/cdm_repos/datahub/impact-data/data_clinical_sample.txt'
FNAME_SAMPLE_REMOVE = os.path.abspath(os.path.join(os.path.dirname( __file__ ), "mskimpact_clinical_data_remove.tsv"))


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
    config_cbio_etl.fname_timeline_fu: fname_save_timeline_follow_up,
    fname_timeline_progression_phi: fname_save_timeline_progression,
    fname_timeline_pathology_mmr_phi: fname_save_timeline_pathology_mmr,
    fname_timeline_cancer_presence_phi: fname_save_timeline_cancer_presence,
    fname_timeline_ecog_phi: fname_save_timeline_ecog
}



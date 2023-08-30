import sys
import os

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from data_classes_cdm import CDMProcessingVariables as config_cdm


# Dictionary of files to copy
dict_files_to_copy = {
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
    config_cdm.fname_save_spec_surg_timeline: config_cdm.fname_save_spec_surg_timeline_minio
}

fname_minio_env = config_cdm.minio_env
"""
mind-cdm-cbioportal-formatting-patient-summary.py

By Chris Fong MSKCC 2022



"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-treatments', 'cbioportal')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'minio_api')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'cbioportal-study-merge-tools')))

from minio_api import MinioAPI
from data_classes_cdm import CDMProcessingVariables as config_cdm
from create_summary_from_redcap_reports import RedcapToCbioportalFormat
from mind_cdm_cbioportal_formatting_patient_dx_summary import cdm_custom_summary_dx_patient
from mind_cdm_cbioportal_formatting_patient_met_site_summary import cdm_custom_summary_met_site_patient
from medications_patient_summary_cbioportal import medications_patient_summary_cbioportal

# ----------------------------------------------
### Define filenames for pulling Redcap data
fname_vars = config_cdm.path_vars
fname_rpt_map = config_cdm.redcap_rpt_map
fname_redcap_rpt = config_cdm.path_map

### Minio location for saving data
fname_minio_env = config_cdm.minio_env
pathname_cbio_summaries = 'redcap_exports/cdm_cbioportal_codebook'
pathname_redcap_config = 'config'

### Manifest for merging cBioPortal summary data
fname_manifest_patient = '/cbioportal/summary_manifest_patient.csv'

## Create Minio object --------------------------------------------------
obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

## Format data for cBioPortal --------------------------------------------------
### Pull the Redcap codebook
obj_redcap_to_cbio = RedcapToCbioportalFormat(fname_minio_env=fname_minio_env,
                                              fname_report_api=fname_redcap_rpt, 
                                              path_config=pathname_redcap_config,
                                              fname_report_map=fname_rpt_map, 
                                              fname_variables=fname_vars, 
                                              path_save=pathname_cbio_summaries)

df_header_all = obj_redcap_to_cbio.return_full_header()
df_report_map = obj_redcap_to_cbio.return_report_map()
df_redcap_report_summary = obj_redcap_to_cbio.return_redcap_report_summary()
col_fname_save = obj_redcap_to_cbio._col_fname_save
col_redcap_id = obj_redcap_to_cbio._col_darwin_id

# ----------------------------------------------
# Create a CUSTOM sample summary tables from Redcap reports
## ----
## Sample-level dx summary
instr_name_patient_dx = 'patient_diagnosis_summary_instrument'
fname_save_dx_p = '/cbioportal/IMPACT_Dx_Patient_Summary.csv'
fname_save_dx_p_header = '/cbioportal/IMPACT_Dx_Patient_Summary_header.csv'
fname_demo = config_cdm.fname_demo
fname_summary = config_cdm.fname_impact_summary_patient

df_header_dx_p, df_patient_cbio = cdm_custom_summary_dx_patient(
    obj_minio=obj_minio,
    df_redcap_map=df_report_map, 
    df_header=df_header_all, 
    REPORT_NAME=instr_name_patient_dx,
    col_redcap_id=col_redcap_id,
    fname_patient_summary=fname_summary,
    fname_demo=fname_demo,
    col_fname_save=col_fname_save
)

## ----
instr_name_patient_met_dx = 'patient_metastatic_summary'
fname_met_timeline = config_cdm.fname_save_dx_met_timeline
fname_save_met_site_dx_p = '/cbioportal/IMPACT_met_site_patient_summary.csv'
fname_save_met_site_dx_p_header = '/cbioportal/IMPACT_met_site_patient_summary_header.csv'
df_met_sites_p_header, df_met_sites_p = cdm_custom_summary_met_site_patient(obj_minio=obj_minio,
                                                              df_redcap_map=df_report_map, 
                                                              df_header=df_header_all, 
                                                              REPORT_NAME=instr_name_patient_met_dx,
                                                              col_redcap_id=col_redcap_id,
                                                              fname_met_dx_timeline=fname_met_timeline,
                                                              col_fname_save=col_fname_save)

## -----
instr_name_patient_treatment = 'patient_treatment_summary'
fname_meds_timeline = config_cdm.fname_save_meds_timeline
fname_rt_timeline = config_cdm.fname_save_rt_timeline
fname_save_treatment_summary_p = '/cbioportal/IMPACT_treatment_patient_summary.csv'
fname_save_treatment_summary_p_header = '/cbioportal/IMPACT_treatment_patient_summary_header.csv'
df_treatments_p_header, df_treatments_p = medications_patient_summary_cbioportal(obj_minio=obj_minio,
                                                                              df_redcap_map=df_report_map, 
                                                                              df_header=df_header_all, 
                                                                              REPORT_NAME=instr_name_patient_treatment,
                                                                              col_redcap_id=col_redcap_id,
                                                                                col_fname_save=col_fname_save,
                                                                              fname_meds_timeline=fname_meds_timeline,
                                                                              fname_rt_timeline=fname_rt_timeline)
    

## Modify/create manifest files --------------------------------------------------
##### This file will be used for merging data
obj_redcap_to_cbio.summary_manifest_init()
obj_redcap_to_cbio.summary_manifest_append(instr_name=instr_name_patient_dx,
                                           fname_df_save=fname_save_dx_p,
                                           fname_header_save=fname_save_dx_p_header)
obj_redcap_to_cbio.summary_manifest_append(instr_name=instr_name_patient_met_dx,
                                           fname_df_save=fname_save_met_site_dx_p,
                                           fname_header_save=fname_save_met_site_dx_p_header)
obj_redcap_to_cbio.summary_manifest_append(instr_name=instr_name_patient_treatment,
                                           fname_df_save=fname_save_treatment_summary_p,
                                           fname_header_save=fname_save_treatment_summary_p_header)
                                                                            
## Note: Continue appending manifest files here
df_manifest_s = obj_redcap_to_cbio.return_manifest()
##### Save manifest
obj_redcap_to_cbio.summary_manifest_save(fname_save=fname_manifest_patient)

## Save data and header files --------------------------------------------------
### Save cbioportal formatted patient level dx data and header files 
print('Saving %s' % fname_save_dx_p)
print(df_patient_cbio.head())
obj_minio.save_obj(df=df_patient_cbio, 
                   path_object=fname_save_dx_p, 
                   sep=',')
print('Saving %s' % fname_save_dx_p_header)
print(df_header_dx_p.head())
obj_minio.save_obj(df=df_header_dx_p, 
                   path_object=fname_save_dx_p_header, 
                   sep=',')
                                                                            
### Save cbioportal formatted patient level met site data and header files 
print('Saving %s' % fname_save_met_site_dx_p)
print(df_met_sites_p.head())
obj_minio.save_obj(df=df_met_sites_p, 
                   path_object=fname_save_met_site_dx_p, 
                   sep=',')
print('Saving %s' % fname_save_met_site_dx_p_header)
print(df_met_sites_p_header.head())
obj_minio.save_obj(df=df_met_sites_p_header, 
                   path_object=fname_save_met_site_dx_p_header, 
                   sep=',')

### Save formatted patient level treatment summary files
print('Saving %s' % fname_save_treatment_summary_p)
print(df_treatments_p.head())
obj_minio.save_obj(df=df_treatments_p, 
                   path_object=fname_save_treatment_summary_p, 
                   sep=',')
print('Saving %s' % fname_save_treatment_summary_p_header)
print(df_treatments_p_header.head())
obj_minio.save_obj(df=df_treatments_p_header, 
                   path_object=fname_save_treatment_summary_p_header, 
                   sep=',')

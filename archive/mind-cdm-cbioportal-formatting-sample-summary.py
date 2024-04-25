"""
mind-cdm-cbioportal-formatting-sample-summary.py

By Chris Fong MSKCC 2022




"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'minio_api')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'cbioportal-study-merge-tools')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'diagnosis_event_abstraction_icd')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-treatments', 'cbioportal')))
from minio_api import MinioAPI
from data_classes_cdm import CDMProcessingVariables as config_cdm
from create_summary_from_redcap_reports import RedcapToCbioportalFormat
from mind_cdm_cbioportal_formatting_sample_dx_summary import cdm_custom_summary_dx_sample
from medications_sample_summary_cbioportal import medications_sample_summary_cbioportal

# ----------------------------------------------
### Define filenames for pulling Redcap data
fname_vars = config_cdm.path_vars
fname_rpt_map = config_cdm.redcap_rpt_map
fname_redcap_rpt = config_cdm.path_map

### Minio location for saving data
fname_minio_env = config_cdm.minio_env
pathname_cbio_summaries = '/cbioportal'
pathname_redcap_config = '/config'

### Manifest for merging cBioPortal summary data
fname_manifest_sample = '/cbioportal/summary_manifest_sample.csv'

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
col_redcap_id = obj_redcap_to_cbio._col_sample_id

# ----------------------------------------------
# Create a CUSTOM sample summary tables from Redcap reports
## ----
## Sample-level dx summary
instr_name_sample_dx = 'sample_diagnosis_summary_instrument'
fname_save_dx_s = '/cbioportal/IMPACT_Dx_Sample_Summary.csv'
fname_save_dx_s_header = '/cbioportal/IMPACT_Dx_Sample_Summary_header.csv'

df_header_dx_s, df_sample_cbio = cdm_custom_summary_dx_sample(obj_minio=obj_minio,
                                                              df_redcap_map=df_report_map, 
                                                              df_header=df_header_all, 
                                                              REPORT_NAME=instr_name_sample_dx,
                                                              col_redcap_id=col_redcap_id,
                                                              fname_sample_summary=config_cdm.fname_impact_summary_sample,
                                                              fname_demo=config_cdm.fname_demo,
                                                              col_fname_save=col_fname_save)

## Sample-level prior-treatment summary
instr_name_sample_treatments = 'sample_treatment_summary'
fname_chemo = '/medications/ddp_chemo.tsv'
fname_summary = '/summary/IMPACT_Darwin_Sample_Summary.tsv'
fname_rad_onc = '/radonc/ddp_radonc.tsv'
fname_save_prior_rx_s = '/cbioportal/IMPACT_prior_treatment_sample_summary.csv'
fname_save_prior_rx_s_header = '/cbioportal/IMPACT_prior_treatment_sample_summary_header.csv'

df_header_prior_rx, df_prior_rx = medications_sample_summary_cbioportal(obj_minio=obj_minio,
                                                                      df_redcap_map=df_report_map, 
                                                                      df_header=df_header_all, 
                                                                      REPORT_NAME=instr_name_sample_treatments,
                                                                      col_redcap_id=col_redcap_id,
                                                                      col_fname_save=col_fname_save,
                                                                       fname_chemo=fname_chemo, 
                                                                        fname_rad_onc=fname_rad_onc, 
                                                                        fname_summary=fname_summary)

## Modify/create manifest files --------------------------------------------------
##### This file will be used for merging data
obj_redcap_to_cbio.summary_manifest_init()
obj_redcap_to_cbio.summary_manifest_append(instr_name=instr_name_sample_dx,
                                           fname_df_save=fname_save_dx_s,
                                           fname_header_save=fname_save_dx_s_header)
obj_redcap_to_cbio.summary_manifest_append(instr_name=instr_name_sample_treatments,
                                           fname_df_save=fname_save_prior_rx_s,
                                           fname_header_save=fname_save_prior_rx_s_header)
## Note: Continue appending manifest files here
df_manifest_s = obj_redcap_to_cbio.return_manifest()
##### Save manifest
obj_redcap_to_cbio.summary_manifest_save(fname_save=fname_manifest_sample)

## Save data and header files --------------------------------------------------
### Save cbioportal formatted sample level dx data and header files 
print('Saving %s' % fname_save_dx_s)
obj_minio.save_obj(df=df_sample_cbio, 
                   path_object=fname_save_dx_s, 
                   sep=',')
print('Saving %s' % fname_save_dx_s_header)
obj_minio.save_obj(df=df_header_dx_s, 
                   path_object=fname_save_dx_s_header, 
                   sep=',')

### Save cbioportal formatted prior treatments for IMPACT samples
print('Saving %s' % fname_save_prior_rx_s)
obj_minio.save_obj(df=df_prior_rx, 
                   path_object=fname_save_prior_rx_s, 
                   sep=',')
print('Saving %s' % fname_save_prior_rx_s_header)
obj_minio.save_obj(df=df_header_prior_rx, 
                   path_object=fname_save_prior_rx_s_header, 
                   sep=',')

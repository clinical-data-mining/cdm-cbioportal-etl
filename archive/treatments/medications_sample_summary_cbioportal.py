""""
medications_sample_summary_cbioportal.py

By Chris Fong - MSKCC 2022


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
import pandas as pd
import numpy as np
from utils import mrn_zero_pad, convert_to_int


def medications_sample_summary_cbioportal(obj_minio, df_redcap_map, df_header, REPORT_NAME, col_redcap_id, fname_chemo, fname_rad_onc, fname_summary, col_fname_save):
    
    ### Columns of interest for joining or removing
    id_label = '#Sample Identifier'
    col_bday = 'PT_BIRTH_DTE'
    col_pid_phi = 'MRN'
    col_pid = 'DMP_ID'
    col_sid = 'SAMPLE_ID'
    
    # ## Format data for cBioPortal -------------------------------------------------
    ### Load Redcap report of interest
    REPORT_FILENAME = df_redcap_map.loc[df_redcap_map['REPORT_NAME'] == REPORT_NAME, col_fname_save].iloc[0]
    obj = obj_minio.load_obj(path_object=REPORT_FILENAME)
    current_redcap_report = pd.read_csv(obj, sep='\t')

    #### Format sample dx header
    df_header_dx_s = df_header[df_header['heading'].isin(current_redcap_report.columns)]
    df_header_dx_s = df_header_dx_s.drop(columns=['is_date'])
    df_header_dx_s = df_header_dx_s[df_header_dx_s['heading'] != 'record_id']
    df_header_dx_s = df_header_dx_s.reset_index(drop=True)

    label_s = df_header_dx_s.loc[df_header_dx_s['heading'] == col_redcap_id, 'label'].iloc[0]
    df_header_dx_s.loc[df_header_dx_s['heading'] == col_redcap_id, 'data_type'] = np.NaN
    df_header_dx_s.loc[df_header_dx_s['heading'] == col_redcap_id, 'visible'] = ''
    df_header_dx_s = df_header_dx_s.replace({'heading': {col_redcap_id: col_sid}, 
                                             'label': {label_s: id_label}})
    df_header_dx_s['heading'] = df_header_dx_s['heading'].str.upper()
    
    ### Format sample-level treatment summary data
    #### Load data required
    ##### Medications
    # fname = '/medications/ddp_chemo.tsv'
    print('Loading %s' % fname_chemo)
    obj = obj_minio.load_obj(path_object=fname_chemo)
    df_meds1 = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
    df_meds = mrn_zero_pad(df=df_meds1, col_mrn='MRN')
    df_meds['APR_START_DTE'] = pd.to_datetime(df_meds['APR_START_DTE'])
    
    ##### IMPACT summary
    # fname_summary = '/summary/IMPACT_Darwin_Sample_Summary.tsv'
    print('Loading %s' % fname_summary)
    obj = obj_minio.load_obj(path_object=fname_summary)
    df_summary = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=['SAMPLE_ID', 'DMP_ID', 'MRN', 'DATE_OF_PROCEDURE_SURGICAL_EST'])
    df_summary = convert_to_int(df=df_summary, list_cols=['MRN'])
    df_summary = df_summary.loc[df_summary['DMP_ID'].notnull() & df_summary['MRN'].notnull()].drop_duplicates()
    df_summary = mrn_zero_pad(df=df_summary, col_mrn='MRN')
    df_summary['DATE_OF_PROCEDURE_SURGICAL_EST'] = pd.to_datetime(df_summary['DATE_OF_PROCEDURE_SURGICAL_EST'])
    
    ##### Rad Onc
    # fname_rad_onc = '/radonc/ddp_radonc.tsv'
    print('Loading %s' % fname_rad_onc)
    obj = obj_minio.load_obj(path_object=fname_rad_onc)
    df_radonc = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
    df_radonc = mrn_zero_pad(df=df_radonc, col_mrn='MRN')
    df_radonc['RADONC_START_DTE'] = pd.to_datetime(df_radonc['RADONC_START_DTE'])
    df_radonc['RADONC_END_DTE'] = pd.to_datetime(df_radonc['RADONC_END_DTE'])

    #### Transform medications to compute prior treatments to IMPACT
    df_meds_first = df_meds.groupby(['MRN'])['APR_START_DTE'].min().reset_index()

    df_meds_pre_treat1 = df_summary.merge(right=df_meds_first, how='left', on='MRN')
    log1 = (df_meds_pre_treat1['DATE_OF_PROCEDURE_SURGICAL_EST'] > df_meds_pre_treat1['APR_START_DTE'])
    log_na = df_meds_pre_treat1['DATE_OF_PROCEDURE_SURGICAL_EST'].isnull() | df_meds_pre_treat1['APR_START_DTE'].isnull()
    df_meds_pre_treat1 = df_meds_pre_treat1.assign(PRE_TREAT_MEDS=log1)
    df_meds_pre_treat1.loc[log_na, 'PRE_TREAT_MEDS'] = np.NaN
    df_meds_pre_treat1 = df_meds_pre_treat1.replace({'PRE_TREAT_MEDS': {False: 'No', True: 'Yes'}})
    df_meds_pre_treat_f = df_meds_pre_treat1.drop(columns=['DMP_ID', 'MRN', 'DATE_OF_PROCEDURE_SURGICAL_EST', 'APR_START_DTE'])
    df_meds_pre_treat_f = df_meds_pre_treat_f.dropna().drop_duplicates()
    
    #### Transform rad onc to compute prior treatments to IMPACT
    df_rt_first = df_radonc.groupby(['MRN'])['RADONC_START_DTE'].min().reset_index()
    
    df_rt_pre_treat1 = df_summary.merge(right=df_rt_first, how='left', on='MRN')
    log1 = (df_rt_pre_treat1['DATE_OF_PROCEDURE_SURGICAL_EST'] > df_rt_pre_treat1['RADONC_START_DTE'])
    log_na = df_rt_pre_treat1['DATE_OF_PROCEDURE_SURGICAL_EST'].isnull() | df_rt_pre_treat1['RADONC_START_DTE'].isnull()
    df_rt_pre_treat1 = df_rt_pre_treat1.assign(PRE_TREAT_RT=log1)
    df_rt_pre_treat1.loc[log_na, 'PRE_TREAT_RT'] = np.NaN
    df_rt_pre_treat1 = df_rt_pre_treat1.replace({'PRE_TREAT_RT': {False: 'No', True: 'Yes'}})
    df_rt_pre_treat_f = df_rt_pre_treat1.drop(columns=['DMP_ID', 'MRN', 'DATE_OF_PROCEDURE_SURGICAL_EST', 'RADONC_START_DTE'])
    df_rt_pre_treat_f = df_rt_pre_treat_f.dropna().drop_duplicates()
    
    ## Merge prior meds 
    df_f = df_meds_pre_treat_f.merge(right=df_rt_pre_treat_f, how='outer', on='SAMPLE_ID')

 
    return df_header_dx_s, df_f

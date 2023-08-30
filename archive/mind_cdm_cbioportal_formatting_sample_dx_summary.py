"""
mind-cdm-cbioportal-formatting-sample-dx-summary.py

By Chris Fong MSKCC 2022


"""
import os
import sys
import pandas as pd
import numpy as np
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from utils import  mrn_zero_pad


def cdm_custom_summary_dx_sample(obj_minio, df_redcap_map, df_header, REPORT_NAME, col_redcap_id, fname_sample_summary, fname_demo, col_fname_save):

    ### Columns of interest for joining or removing
    id_label = '#Sample Identifier'
    col_bday = 'PT_BIRTH_DTE'
    col_pid_phi = 'MRN'
    col_pid = 'DMP_ID'
    col_sid = 'SAMPLE_ID'
    
    ### List of dates that will be converted to age, columns containing PHI to be dropped
    list_dates_sample = ['DATE_SEQUENCING_REPORT',
                         'REPORT_CMPT_DATE_SOURCE_0', 
                         'REPORT_CMPT_DATE_SOURCE_0b',
                         'DATE_OF_PROCEDURE_SURGICAL', 
                         'DATE_OF_PROCEDURE_SURGICAL_EST', 
                         'DATE_AT_FIRST_MET_DX']
    list_drop_sample = ['MRN', 
                        'SPECIMEN_NUMBER_DMP',
                        'SOURCE_ACCESSION_NUMBER_0', 
                        'SOURCE_SPEC_NUM_0',
                        'SOURCE_ACCESSION_NUMBER_0b', 
                        'SOURCE_SPEC_NUM_0b',
                        'ACCESSION_NUMBER_DMP']

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
    
    print(col_redcap_id)
    print(df_header_dx_s.head())

    label_s = df_header_dx_s.loc[df_header_dx_s['heading'] == col_redcap_id, 'label'].iloc[0]
    df_header_dx_s.loc[df_header_dx_s['heading'] == col_redcap_id, 'data_type'] = np.NaN
    df_header_dx_s.loc[df_header_dx_s['heading'] == col_redcap_id, 'visible'] = ''
    df_header_dx_s = df_header_dx_s.replace({'heading': {col_redcap_id: col_sid}, 
                                             'label': {label_s: id_label}})
    df_header_dx_s['heading'] = df_header_dx_s['heading'].str.upper()

    #### Format patient and sample dx summary data
    ##### Load patient and summary summary and censor dates using birthdates from demographics
    obj = obj_minio.load_obj(path_object=fname_sample_summary)
    df_sample = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
    df_sample = mrn_zero_pad(df=df_sample, col_mrn=col_pid_phi)

    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
    df_demo = df_demo[[col_pid_phi, col_bday]].copy()
    df_demo = mrn_zero_pad(df=df_demo, col_mrn=col_pid_phi)

    ###### Add birthdates
    df_sample = df_sample.merge(right=df_demo, how='left', on=col_pid_phi)

    ###### Deidentify dates for sample summary
    cols_dates_all_s = [col_bday] + list_dates_sample
    df_sample[cols_dates_all_s] = df_sample[cols_dates_all_s].apply(lambda x: pd.to_datetime(x))

    df_sample[list_dates_sample] = df_sample[list_dates_sample].apply(lambda x: (x - df_sample[col_bday]).dt.days)
    df_sample = df_sample.drop(columns=list_drop_sample)

    ###### Drop birthday
    df_sample = df_sample.drop(columns=[col_bday])

    ##### Format sample level data
    dict_cols = dict(zip(df_header_dx_s['label'], df_header_dx_s['heading']))
    df_sample_cbio = df_sample.rename(columns=dict_cols)
    df_sample_cbio = df_sample_cbio.rename(columns={'REPORT_CMPT_DATE_SOURCE_0b': 'REPORT_CMPT_DATE_SOURCE_0B'})
    
    df_sample_cbio = df_sample_cbio[list(df_header_dx_s['heading'])]
    df_sample_cbio = df_sample_cbio[~df_sample_cbio[col_sid].duplicated(keep='first')]

    return df_header_dx_s, df_sample_cbio

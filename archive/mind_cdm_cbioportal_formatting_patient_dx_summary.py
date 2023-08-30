"""
mind-cdm-cbioportal-formatting-patient-dx-summary.py

By Chris Fong MSKCC 2022



"""
import os
import sys
import pandas as pd
import numpy as np
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from utils import  mrn_zero_pad


def cdm_custom_summary_dx_patient(obj_minio, df_redcap_map, df_header, REPORT_NAME, col_redcap_id, fname_patient_summary, fname_demo, col_fname_save):

    ### Columns of interest for joining or removing
    id_label = '#Patient Identifier'
    col_bday = 'PT_BIRTH_DTE'
    col_pid_phi = 'MRN'
    col_pid = 'DMP_ID'
    col_pid_cbio = 'PATIENT_ID'

    ### List of dates that will be converted to age, columns containing PHI to be dropped
    list_dates_patient = ['DATE_AT_FIRST_ICDO_DX',
                          'DATE_AT_FIRST_MET_DX_ICD10', 
                          'DATE_AT_FIRST_MET_DX_ICD10_NON_LN_OTHER',
                          'DATE_AT_FIRST_MET_DX_ICDO_SOLID',
                          'DATE_AT_FIRST_MET_DX_IMPACT', 
                          'DATE_AT_FIRST_MET_DX']
    list_drop_patient = [col_pid_phi]

    ## Format data for cBioPortal -------------------------------------------------
    ### Load Redcap report of interest
    REPORT_FILENAME = df_redcap_map.loc[df_redcap_map['REPORT_NAME'] == REPORT_NAME, col_fname_save].iloc[0]
    obj = obj_minio.load_obj(path_object=REPORT_FILENAME)
    current_redcap_report = pd.read_csv(obj, sep='\t')

    #### Format patient dx header
    df_header_dx_p = df_header[df_header['heading'].isin(current_redcap_report.columns)]
    df_header_dx_p = df_header_dx_p.drop(columns=['is_date'])
    df_header_dx_p = df_header_dx_p[df_header_dx_p['heading'] != 'record_id']
    df_header_dx_p = df_header_dx_p.reset_index(drop=True)

    label_p = df_header_dx_p.loc[df_header_dx_p['heading'] == col_redcap_id, 'label'].iloc[0]
    df_header_dx_p.loc[df_header_dx_p['heading'] == col_redcap_id, 'data_type'] = np.NaN
    df_header_dx_p.loc[df_header_dx_p['heading'] == col_redcap_id, 'visible'] = ''
    df_header_dx_p = df_header_dx_p.replace({'heading': {col_redcap_id: col_pid_cbio}, 
                                             'label': {label_p: id_label}})
    df_header_dx_p['heading'] = df_header_dx_p['heading'].str.upper()

    #### Format patient and sample dx summary data
    ##### Load patient and summary summary and censor dates using birthdates from demographics
    obj = obj_minio.load_obj(path_object=fname_patient_summary)
    df_patient = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
    df_patient = mrn_zero_pad(df=df_patient, col_mrn=col_pid_phi)

    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
    df_demo = df_demo[[col_pid_phi, col_bday]].copy()
    df_demo = mrn_zero_pad(df=df_demo, col_mrn=col_pid_phi)

    ###### Add birthdates
    df_patient = df_patient.merge(right=df_demo, how='left', on=col_pid_phi)

    ###### Deidentify dates for patient summary
    cols_dates_all = [col_bday] + list_dates_patient
    df_patient[cols_dates_all] = df_patient[cols_dates_all].apply(lambda x: pd.to_datetime(x))

    df_patient[list_dates_patient] = df_patient[list_dates_patient].apply(lambda x: (x - df_patient[col_bday]).dt.days)
    df_patient = df_patient.drop(columns=list_drop_patient)

    col = df_patient.pop(col_pid)
    df_patient.insert(0, col.name, col)

    ###### Drop birthday
    df_patient = df_patient.drop(columns=[col_bday])

    ##### Format patient level data
    df_patient_cbio = df_patient.rename(columns={col_pid: col_pid_cbio})
    print(df_patient_cbio.columns)
    df_patient_cbio = df_patient_cbio[list(df_header_dx_p['heading'])]
    df_patient_cbio = df_patient_cbio[~df_patient_cbio[col_pid_cbio].duplicated(keep='first')]

    return df_header_dx_p, df_patient_cbio


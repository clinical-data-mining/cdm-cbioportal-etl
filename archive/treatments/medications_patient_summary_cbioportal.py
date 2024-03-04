""""
medications_patient_summary_cbioportal.py

By Chris Fong - MSKCC 2022


"""
import pandas as pd
import numpy as np


def medications_patient_summary_cbioportal(obj_minio, df_redcap_map, df_header, REPORT_NAME, col_redcap_id, fname_meds_timeline, fname_rt_timeline, col_fname_save):
    
    ### Columns of interest for joining or removing
    id_label = '#Patient Identifier'
    col_bday = 'PT_BIRTH_DTE'
    col_pid_phi = 'MRN'
    col_pid = 'DMP_ID'
    col_pid_cbio = 'PATIENT_ID'
    
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
    print('Loading %s' % fname_meds_timeline)
    df_meds_f = pd.read_csv(fname_meds_timeline, header=0, low_memory=False, sep='\t')
    
    fname = fname_rt_timeline
    print('Loading %s' % fname_rt_timeline)
    df_radonc_f = pd.read_csv(fname_rt_timeline, header=0, low_memory=False, sep='\t')
    
    ## ---- 
        
    df_meds_grouped = (df_meds_f.groupby(['PATIENT_ID', 'SUBTYPE'])['START_DATE'].count() > 0).reset_index()
    # df_meds_grouped['START_DATE'].fillna(False).value_counts()
    # df_meds_grouped['START_DATE'] = df_meds_grouped['START_DATE'].astype(bool)
    df_meds_grouped = pd.pivot_table(df_meds_grouped, columns='SUBTYPE', values='START_DATE', index='PATIENT_ID', fill_value=False).reset_index().replace({1: 'Yes', False: 'No'})
    cols_old = df_meds_grouped.columns
    cols_new = ['PATIENT_ID' if i == 'PATIENT_ID' else i.upper().replace(' ', '_') + '_THERAPY' for i in cols_old]
    dict_cols_rep = dict(zip(cols_old, cols_new))
    df_meds_grouped = df_meds_grouped.rename(columns=dict_cols_rep)
    
    # ---- 
    df_rt_grouped = (df_radonc_f.groupby(['PATIENT_ID'])['START_DATE'].count() > 0).reset_index()
    df_rt_grouped = df_rt_grouped.rename(columns={'START_DATE': 'HISTORY_OF_RT'})
    df_rt_grouped = df_rt_grouped.replace({True: 'Yes'})
    
    df_f = df_meds_grouped.merge(right=df_rt_grouped, how='outer', on='PATIENT_ID')
    df_f['HISTORY_OF_RT'] = df_f['HISTORY_OF_RT'].fillna('No')

    return df_header_dx_p, df_f
    

        


 


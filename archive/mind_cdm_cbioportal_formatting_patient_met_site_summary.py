"""
mind_cdm_cbioportal_formatting_patient_met_site_summary.py

By Chris Fong MSKCC 2022


Notes:



"""
import pandas as pd
import numpy as np


def cdm_custom_summary_met_site_patient(obj_minio, df_redcap_map, df_header, REPORT_NAME, col_redcap_id, fname_met_dx_timeline, col_fname_save):

    ### Columns of interest for joining or removing
    id_label = '#Patient Identifier'
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
    df_patient = pd.read_csv(fname_met_dx_timeline, sep='\t')
    df_patient_g = df_patient.groupby([col_pid_cbio, 'ANATOMIC_LOCATION'])['START_DATE'].min().reset_index()
    sites = df_patient_g['ANATOMIC_LOCATION'].unique()
    df_sites = pd.pivot_table(df_patient_g, columns='ANATOMIC_LOCATION', index=col_pid_cbio, values='START_DATE').reset_index()
    df_sites[sites] = df_sites[sites].notnull()

    ## Rename and remove columns
    # col_rename_dict = {'PERIPHERAL_NERVOUS_SYSTEM': 'PNS',
    #                   'BLADDER_OR_URINARY_TRACT': 'BLADDER_UT',
    #                   'GENITAL_FEMALE': 'FEMALE_GENITAL',
    #                   'GENITAL_MALE': 'MALE_GENITAL'}
    # col_drop = ['DIST_LYMPH', 'REGIONAL_LYMPH', 'OTHER']
    # df_sites = df_sites.rename(columns=col_rename_dict)
    # df_sites = df_sites.drop(columns=col_drop)
    cols_new = ['METASTATIC_SITE_' + x for x in list(df_sites.columns) if x not in col_pid_cbio]
    cols_old = [x for x in list(df_sites.columns) if x not in col_pid_cbio]

    # set(cols_new) - set(df_header_dx_p['heading'].unique())
    col_rename_dict_new = dict(zip(cols_old, cols_new))
    df_sites = df_sites.rename(columns=col_rename_dict_new)
    df_sites = df_sites[list(df_header_dx_p['heading'])]

    # Replace True/False with Yes/No
    df_sites = df_sites.replace({True: 'Yes', False: 'No'})

    return df_header_dx_p, df_sites


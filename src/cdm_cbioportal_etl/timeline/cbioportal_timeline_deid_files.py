import os

import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import get_anchor_dates, constants
from msk_cdm.data_processing import (
    mrn_zero_pad,
    convert_to_int, 
    save_appended_df
)

COLS_ORDER_GENERAL = constants.COLS_ORDER_GENERAL
COL_ANCHOR_DATE = constants.COL_ANCHOR_DATE
COL_OS_DATE = 'OS_DATE'

def process_df_os(
        df,
        col_id,
        col_os_date
):
    df_os = df[[col_id, col_os_date]].copy()
    df_os.columns = ['MRN', COL_OS_DATE]

    return df_os

def process_codebook(fname_metadata, fname_tables):
    cols_keep_meta = [
        'form_name',
        'field_name',
        'text_validation_type_or_sh',
        'identifier'
    ]
    cols_keep_tables = [
        'form_name',
        'cbio_deid_filename'
    ]

    df_metadata = pd.read_csv(
        fname_metadata,
        usecols=cols_keep_meta
    )

    df_tables = pd.read_csv(fname_tables, usecols=cols_keep_tables)
    df_tables = df_tables.dropna(subset=['cbio_deid_filename'])

    df = df_tables.merge(right=df_metadata, how='left', on='form_name')

    return df


def cbioportal_deid_timeline_files(
    fname_minio_env,
    dict_files_timeline,
    df_patient_os_date,
    col_os_date,
    col_id,
    fname_metadata,
    fname_tables,
    list_dmp_ids=None
):
    """ De-identifies timeline files listed in `dict_files_timeline` and saves to object storage. Dates are deidentified using `get_anchor_dates`
    :param fname_minio_env: Minio environment file for loading and saving data
    :param dict_files_timeline: Dictionary of cbioportal formatted timeline file names to load (containing PHI) and corresponding filenames of files to be pushed to cbioportal
    :return: None
    """
    df_path_g = get_anchor_dates()
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    df_os = process_df_os(
        df=df_patient_os_date,
        col_id=col_id,
        col_os_date=col_os_date
    )

    df_codebook = process_codebook(
        fname_tables=fname_tables,
        fname_metadata=fname_metadata
    )
    
    for fname in dict_files_timeline:
        print(fname)
        file_deid = dict_files_timeline[fname]
        print(file_deid)
        obj = obj_minio.load_obj(path_object=fname)
        df_ = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df_ = mrn_zero_pad(df=df_, col_mrn='MRN')

        fname_base = os.path.basename(file_deid)
        df_codebook_current = df_codebook[df_codebook['cbio_deid_filename'] == fname_base].copy()
        logic_1 = df_codebook_current['field_name'] != 'MRN'
        logic_2 = df_codebook_current['text_validation_type_or_sh'].notnull()
        logic_3 = df_codebook_current['identifier'].notnull()
        logic_f = logic_3 & ~(logic_1 | logic_2)
        list_rmv_cols = list(df_codebook_current.loc[logic_f, 'field_name'])

        print(df_codebook_current.head())
        print('Drop Columns')
        print(list_rmv_cols)

        if 'STOP_DATE' not in df_.columns:
            df_['STOP_DATE'] = ''

        df_['START_DATE'] = pd.to_datetime(df_['START_DATE'], errors='coerce')
        df_['STOP_DATE'] = pd.to_datetime(df_['STOP_DATE'], errors='coerce')

        # Merge deid date
        df_ = df_.merge(right=df_path_g, how='inner', on='MRN')
        df_ = df_.merge(right=df_os, how='inner', on='MRN')
        logic_error_1 =  df_['START_DATE'] > df_['OS_DATE']
        logic_error_2 =  df_['STOP_DATE'] > df_['OS_DATE']
        df_.loc[logic_error_1, 'START_DATE'] = pd.NaT
        df_.loc[logic_error_2, 'STOP_DATE'] = pd.NaT

        cols_drop = ['MRN', COL_OS_DATE] + list_rmv_cols
        df_ = df_.drop(columns=cols_drop)
        df_ = df_.rename(columns={'DMP_ID': 'PATIENT_ID'})

        # DeID dates
        start_date = (df_['START_DATE'] - df_[COL_ANCHOR_DATE]).dt.days
        stop_date = (df_['STOP_DATE'] - df_[COL_ANCHOR_DATE]).dt.days

        df_['START_DATE'] = start_date
        df_['STOP_DATE'] = stop_date
        df_ = df_.drop(columns=[COL_ANCHOR_DATE])
        df_ = df_[df_['START_DATE'].notnull()]
        df_ = convert_to_int(
            df=df_,
            list_cols=['START_DATE', 'STOP_DATE']
        )
        df_['START_DATE'] = df_['START_DATE'].astype(str)
        df_['STOP_DATE'] = df_['STOP_DATE'].astype(str)
        df_['STOP_DATE'] = df_['STOP_DATE'].str.replace('<NA>','')

        cols_other = [x for x in list(df_.columns) if x not in COLS_ORDER_GENERAL]
        cols_order_f = COLS_ORDER_GENERAL + cols_other

        df_f = df_[cols_order_f].copy()

        # Filter by list of patients, if a list exists
        if list_dmp_ids is not None:
            print('Number of patients in timeline template: %s' % str(len(list_dmp_ids)))
            df_f = df_f[df_f['PATIENT_ID'].isin(list_dmp_ids)].copy()
        else:
            print('No patient list in timeline template')


        if df_f.shape[0] > 0:
            save_appended_df(
                df=df_f,
                filename=file_deid,
                sep='\t'
            )
        else:
            print('No data to save. Exiting.')
        
    return None
    

import os
import sys

import pandas as pd

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from msk_cdm.minio import MinioAPI
from variables import (
    FNAME_DEMO,
    FNAME_CBIO_SID,
    FNAME_OS,
    ENV_MINIO
)
from msk_cdm.data_processing import (
    mrn_zero_pad,
    convert_col_to_datetime
)
from cdm_cbioportal_etl.utils import get_anchor_dates


COLS_OS = ['DMP_ID', 'OS_MONTHS', 'OS_STATUS']
COL_P_ID = 'PATIENT_ID'

def _load_data(
    obj_minio,
    fname_sid, 
    fname_demo
):
    # Demographics
    print('Loading %s' % fname_demo)
    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, sep='\t', low_memory=False)
    df_demo = df_demo.drop_duplicates()

    # Pathology table for sequencing date
    df_path_g = get_anchor_dates()

    # IMPACT ids
    print('Loading %s' % fname_sid)
    usecols=[COL_P_ID]
    dict_patient = {COL_P_ID:'DMP_ID'}
    df_ids = pd.read_csv(
        fname_sid, 
        sep='\t', 
        header=0,
        low_memory=False, 
        usecols=usecols
    )
    df_ids = df_ids.rename(columns=dict_patient)
    df_ids = df_ids.drop_duplicates()

    print('Data loaded')
    
    return df_demo, df_path_g, df_ids


def _clean_and_merge(
    df_demo, 
    df_path_g, 
    df_ids
):
    
    # Clean demographics
    df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
    col_os = ['MRN', 'PT_DEATH_DTE', 'PLA_LAST_CONTACT_DTE']
    df_demo_f = df_demo[col_os].copy()
    df_demo_f = convert_col_to_datetime(df=df_demo_f, col='PT_DEATH_DTE')
    df_demo_f = convert_col_to_datetime(df=df_demo_f, col='PLA_LAST_CONTACT_DTE')

    df_os = df_ids.merge(right=df_path_g, how='left', on='DMP_ID')
    df_os = df_os.merge(right=df_demo_f, how='left', on='MRN')
    
    return df_os


def _create_os_cols(df_os):
    # Create interval and event data
    df_os['OS_DATE'] = df_os['PT_DEATH_DTE'].fillna(df_os['PLA_LAST_CONTACT_DTE'])
    df_os['OS_INT'] = (df_os['OS_DATE'] - df_os['DTE_PATH_PROCEDURE']).dt.days/30.417
    df_os['OS_MONTHS'] = df_os['OS_INT']
    df_os['OS_STATUS'] = df_os['PT_DEATH_DTE'].notnull().replace({True: '1:DECEASED', False:'0:LIVING'})
    OS_INT_ERROR = df_os['OS_INT'] > 150
    OS_INT_ERROR2 = df_os['PLA_LAST_CONTACT_DTE'] < df_os['DTE_PATH_PROCEDURE']
    df_os['OS_MONTHS'] = df_os['OS_MONTHS'].astype(str)
    df_os.loc[OS_INT_ERROR | OS_INT_ERROR2, 'OS_MONTHS'] = 'NA'
    df_os_f = df_os[COLS_OS]
    
    return df_os_f


def _process_data(
    fname_minio_env,
    fname_save,
    fname_demo,
    fname_sid
):
    # Create MinIO object
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    
    # Load data
    df_demo, df_path_g, df_ids = _load_data(
        obj_minio=obj_minio,
        fname_demo=fname_demo,
        fname_sid=fname_sid
    )
    
    # Clean and merge data
    df_os = _clean_and_merge(
        df_demo=df_demo, 
        df_path_g=df_path_g, 
        df_ids=df_ids
    )
    
    # Add annotations for OS
    df_os_f = _create_os_cols(df_os=df_os)
    
    # Save data
    obj_minio.save_obj(
        df=df_os_f, 
        path_object=fname_save, 
        sep='\t'
    )

    return df_os_f


def main():
    fname_save = FNAME_OS
    fname_demo = FNAME_DEMO
    fname_sid = FNAME_CBIO_SID
    fname_minio_env = ENV_MINIO
    
    df_os_f = _process_data(
        fname_minio_env=fname_minio_env,
        fname_save=fname_save,
        fname_demo=fname_demo,
        fname_sid=fname_sid
    )
    
    print(df_os_f.sample())
          
if __name__ == '__main__':
    main()
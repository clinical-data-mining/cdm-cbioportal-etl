import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as cdm_files
from cdm_cbioportal_etl.utils import cbioportal_update_config
from msk_cdm.data_processing import (
    mrn_zero_pad,
    convert_col_to_datetime
)
from cdm_cbioportal_etl.utils import get_anchor_dates


COLS_OS = ['DMP_ID', 'OS_MONTHS', 'OS_STATUS']
COL_P_ID = 'PATIENT_ID'

def _load_data(
    obj_minio,
    fname_demo
):
    # Demographics
    print('Loading %s' % fname_demo)
    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, sep='\t', low_memory=False)
    df_demo = df_demo.drop_duplicates()

    # Pathology table for sequencing date
    df_path_g = get_anchor_dates()
    print(df_path_g.head())

    print('Data loaded')
    
    return df_demo, df_path_g


def _clean_and_merge(
    df_demo, 
    df_path_g
):
    
    # Clean demographics
    df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
    col_os = ['MRN', 'PT_DEATH_DTE', 'PLA_LAST_CONTACT_DTE']
    df_demo_f = df_demo[col_os].copy()
    df_demo_f = convert_col_to_datetime(df=df_demo_f, col='PT_DEATH_DTE')
    df_demo_f = convert_col_to_datetime(df=df_demo_f, col='PLA_LAST_CONTACT_DTE')

    df_os = df_path_g.merge(right=df_demo_f, how='left', on='MRN')
    
    return df_os


def _create_os_cols(df_os):
    # Create interval and event data
    df_os['OS_DATE'] = df_os['PT_DEATH_DTE'].fillna(df_os['PLA_LAST_CONTACT_DTE'])
    df_os['OS_INT'] = (df_os['OS_DATE'] - df_os['DTE_TUMOR_SEQUENCING']).dt.days/30.417
    df_os['OS_MONTHS'] = df_os['OS_INT']
    df_os['OS_STATUS'] = df_os['PT_DEATH_DTE'].notnull().replace({True: '1:DECEASED', False:'0:LIVING'})
    OS_INT_ERROR = df_os['OS_INT'] > 150
    OS_INT_ERROR2a = df_os['PLA_LAST_CONTACT_DTE'] < df_os['DTE_TUMOR_SEQUENCING']
    OS_INT_ERROR2b = df_os['PT_DEATH_DTE'] < df_os['DTE_TUMOR_SEQUENCING']
    OS_INT_ERROR2 = OS_INT_ERROR2a | OS_INT_ERROR2b
    df_os.loc[OS_INT_ERROR2, 'OS_MONTHS'] = 0
    df_os['OS_MONTHS'] = df_os['OS_MONTHS'].astype(str)
    df_os.loc[OS_INT_ERROR, 'OS_MONTHS'] = 'NA'
    df_os_f = df_os[COLS_OS]
    
    return df_os_f


def _process_data(
    fname_minio_env,
    fname_save,
    fname_demo
):
    # Create MinIO object
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    
    # Load data
    df_demo, df_path_g = _load_data(
        obj_minio=obj_minio,
        fname_demo=fname_demo
    )
    
    # Clean and merge data
    df_os = _clean_and_merge(
        df_demo=df_demo, 
        df_path_g=df_path_g
    )
    
    # Add annotations for OS
    df_os_f = _create_os_cols(df_os=df_os)

    print('Shape of OS file: %s' % str(df_os_f.shape))
    
    # Save data
    obj_minio.save_obj(
        df=df_os_f, 
        path_object=fname_save, 
        sep='\t'
    )

    return df_os_f


def main():
    parser = argparse.ArgumentParser(description="Script for creating data for OS")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()

    fname_save = cdm_files.fname_overall_survival
    fname_demo = cdm_files.fname_demo
    
    df_os_f = _process_data(
        fname_minio_env=fname_minio_env,
        fname_save=fname_save,
        fname_demo=fname_demo
    )
    
    print(df_os_f.sample())
    print('Missing data:')
    print(df_os_f.isnull().sum())
    print('Shape of OS file: %s' % str(df_os_f.shape))
          
if __name__ == '__main__':
    main()
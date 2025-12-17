import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import mrn_zero_pad, set_debug_console
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as cdm_files
from msk_cdm.data_classes.legacy import CDMProcessingVariables as cdm_files_old
from lib.utils import cbioportal_update_config, get_anchor_dates


FNAME_SAVE_PATIENT_AGE = 'epic_ddp_concat/cbioportal/patient_age_summary_cbioportal.tsv'
COLS_KEEP = ['MRN', 'AGE_LAST_FOLLOWUP', 'AGE_FIRST_CANCER_DIAGNOSIS', 'AGE_FIRST_SEQUENCING']


def convert_col_to_datetime(df, list_cols):
    for col in list_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

def _load_data(
        fname_minio_env,
        obj_minio,
        fname_demo,
        fname_dx
):
    # Demographics
    print('Loading %s' % fname_demo)
    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, sep='\t', low_memory=False)
    df_demo = df_demo.drop_duplicates()

    # Pathology table for sequencing date
    df_path_g = get_anchor_dates(fname_minio_env=fname_minio_env)

    # Diagnosis
    print('Loading %s' % fname_dx)
    obj = obj_minio.load_obj(path_object=fname_dx)
    df_dx = pd.read_csv(obj, sep='\t', low_memory=False)

    return df_demo, df_path_g, df_dx


def _clean_and_merge(
        df_demo,
        df_path_g,
        df_dx
):
    # Clean demographics
    df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
    df_demo = convert_col_to_datetime(df=df_demo, list_cols=['PT_BIRTH_DTE'])
    df_demo_f = df_demo[['MRN', 'PT_BIRTH_DTE', 'CURRENT_AGE_DEID']].copy()

    # Clean diagnosis
    df_dx = mrn_zero_pad(df=df_dx, col_mrn='MRN')
    df_dx = convert_col_to_datetime(df=df_dx, list_cols=['DATE_AT_FIRST_ICDO_DX'])
    df_dx_f = df_dx[['MRN', 'DATE_AT_FIRST_ICDO_DX']].copy()

    # Clean anchor dates
    df_path_g = mrn_zero_pad(df=df_path_g, col_mrn='MRN')
    df_path_g = convert_col_to_datetime(df=df_path_g, list_cols=['DTE_TUMOR_SEQUENCING'])
    print(df_path_g.shape)
    print(df_path_g['MRN'].nunique())
    print(df_path_g.head())

    ## Merge data
    df_f = df_demo_f.merge(right=df_dx_f, how='left', on='MRN')
    df_f = df_f.merge(right=df_path_g, how='left', on='MRN')

    return df_f


def deidentify_dates(df_f):
    logic1 = df_f['CURRENT_AGE_DEID'] >= 89
    df_f.loc[logic1, 'DATE_AT_FIRST_ICDO_DX'] = pd.NaT
    df_f.loc[logic1, 'DTE_TUMOR_SEQUENCING'] = pd.NaT

    df_f['AGE_FIRST_SEQUENCING'] = ((df_f['DTE_TUMOR_SEQUENCING'] - df_f['PT_BIRTH_DTE']).dt.days / 365.25).fillna(
        0).astype(int)
    df_f['AGE_FIRST_CANCER_DIAGNOSIS'] = (
                (df_f['DATE_AT_FIRST_ICDO_DX'] - df_f['PT_BIRTH_DTE']).dt.days / 365.25).fillna(0).astype(int)
    df_f.loc[df_f['AGE_FIRST_SEQUENCING'] > 89, 'AGE_FIRST_SEQUENCING'] = 89
    df_f.loc[df_f['AGE_FIRST_CANCER_DIAGNOSIS'] > 89, 'AGE_FIRST_CANCER_DIAGNOSIS'] = 89

    df_f_save = df_f.rename(columns={'CURRENT_AGE_DEID': 'AGE_LAST_FOLLOWUP'})
    df_f_save = df_f_save[COLS_KEEP].copy()
    df_f_save = df_f_save.replace({0: ''})

    return df_f_save


def _process_data(
        fname_minio_env,
        fname_save,
        fname_demo,
        fname_dx
):
    # Create MinIO object
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    # Load data
    print('Loading data')
    df_demo, df_path_g, df_dx = _load_data(
        fname_minio_env=fname_minio_env,
        obj_minio=obj_minio,
        fname_demo=fname_demo,
        fname_dx=fname_dx
    )

    print('Cleaning and merging data')
    # Clean and merge data
    df_merged = _clean_and_merge(
        df_demo=df_demo,
        df_path_g=df_path_g,
        df_dx=df_dx
    )
    print(f"Shape of df_merged: {df_merged.shape}")
    print(df_merged.head())

    df_f = deidentify_dates(df_f=df_merged)
    print(f"Shape of deid df: {df_f.shape}")
    print(df_f.head())

    # Save data
    print(f'Saving data to {fname_save}')
    obj_minio.save_obj(
        df=df_f,
        path_object=fname_save,
        sep='\t'
    )

    return df_f


def main():
    parser = argparse.ArgumentParser(description="Script for creating data for OS")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    parser.add_argument(
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="--location of Minio environment file",
    )
    args = parser.parse_args()

    fname_save = FNAME_SAVE_PATIENT_AGE
    fname_demo = cdm_files.fname_demo
    fname_dx = cdm_files_old.fname_dx_summary

    df_merged = _process_data(
        fname_minio_env=args.minio_env,
        fname_save=fname_save,
        fname_demo=fname_demo,
        fname_dx=fname_dx
    )

    print('Shape of age attribute file: %s' % str(df_merged.shape))


if __name__ == '__main__':
    main()
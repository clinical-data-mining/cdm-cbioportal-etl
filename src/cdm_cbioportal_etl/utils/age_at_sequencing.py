"""
age_at_sequencing.py

Computes age at sequencing based on the available samples in the pathology report table (Darwin)
Uses date of birth from demographics table

Age at sequencing computed in days

Data is saved to MinIO
"""
from datetime import date

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import mrn_zero_pad, convert_to_int
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from cdm_cbioportal_etl.utils.get_anchor_dates import get_anchor_dates

AGE_CONVERSION_FACTOR = 365.2422

def compute_age_at_sequencing(
        *,
        minio_env,
        fname_demo,
        fname_samples,
        fname_save_age_at_seq
):
    """

    :param minio_env: MinIO environment filename
    :param fname_demo: object path to demographics table
    :param fname_samples: object path to the pathology report table
    :param fname_save_age_at_seq: object path to where age at sequencing is saved
    :return: df_f: dataframe with age at sequencing
    """
    today = date.today()

    # Load data
    ## Create Minio object
    obj_minio = MinioAPI(fname_minio_env=minio_env)

    ## Load demographics for date of birth
    col_keep = ['MRN', 'PT_BIRTH_DTE', 'PT_DEATH_DTE', 'PLA_LAST_CONTACT_DTE']
    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, sep='\t', usecols=col_keep)
    df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
    df_demo['PT_BIRTH_DTE'] = pd.to_datetime(df_demo['PT_BIRTH_DTE'])
    df_demo['PT_DEATH_DTE'] = pd.to_datetime(df_demo['PT_DEATH_DTE'])
    df_demo['PLA_LAST_CONTACT_DTE'] = df_demo['PLA_LAST_CONTACT_DTE'].fillna(today)
    df_demo['PLA_LAST_CONTACT_DTE'] = pd.to_datetime(df_demo['PLA_LAST_CONTACT_DTE'])
    df_demo['OS_DTE'] = df_demo['PT_DEATH_DTE'].fillna(df_demo['PLA_LAST_CONTACT_DTE'])

    ## Load pathology report table
    col_keep = ['MRN', 'DTE_PATH_PROCEDURE', 'DMP_ID', 'SAMPLE_ID']
    obj = obj_minio.load_obj(path_object=fname_samples)
    df_path = pd.read_csv(obj, sep='\t', usecols=col_keep, low_memory=False)
    df_path = mrn_zero_pad(df=df_path, col_mrn='MRN')

    ## Load anchor dates
    df_archor_dates = get_anchor_dates()
    list_sample_ids_used = list(set(df_archor_dates['DMP_ID']))

    # Clean and Combine data
    df_path_clean = df_path[df_path['SAMPLE_ID'].notnull() & df_path['DMP_ID'].isin(list_sample_ids_used)].copy()
    df_path_clean = df_path_clean[df_path_clean['SAMPLE_ID'].str.contains('-T')].copy()
    df_path_clean['DMP_ID_DERIVED'] = df_path_clean['SAMPLE_ID'].apply(lambda x: x[:9])
    df_path_clean = df_path_clean[df_path_clean['DMP_ID_DERIVED'] == df_path_clean['DMP_ID']].copy()
    df_path_clean['DTE_PATH_PROCEDURE'] = pd.to_datetime(df_path_clean['DTE_PATH_PROCEDURE'])

    ## Merge dataframes
    df_f = df_path_clean.merge(right=df_demo, how='left', on=['MRN'])

    ## Compute age at sequencing
    df_f['AGE_AT_SEQUENCING_DAYS_PHI'] = (df_f['DTE_PATH_PROCEDURE'] - df_f['PT_BIRTH_DTE']).dt.days

    ## Compute OS interval
    df_f['OS_INT'] = (df_f['OS_DTE'] - df_f['DTE_PATH_PROCEDURE']).dt.days

    df_f['AGE_AT_SEQUENCING_YEARS_PHI'] = (df_f['AGE_AT_SEQUENCING_DAYS_PHI']/AGE_CONVERSION_FACTOR)
    df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'] = ((df_f['AGE_AT_SEQUENCING_DAYS_PHI'] + df_f['OS_INT'])/AGE_CONVERSION_FACTOR)

    df_f['AGE_AT_SEQUENCING_YEARS_PHI'] = df_f['AGE_AT_SEQUENCING_YEARS_PHI'].astype(int)
    df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'] = df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'].astype(int)

    ## Deidentify age
    log_under18 = df_f['AGE_AT_SEQUENCING_YEARS_PHI'] < 18
    log_over89 = (df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'] > 89) | (df_f['AGE_AT_SEQUENCING_YEARS_PHI'] > 89)

    df_f['AGE_AT_SEQUENCING_YEARS'] = df_f['AGE_AT_SEQUENCING_YEARS_PHI'].astype(str)
    df_f.loc[log_under18, 'AGE_AT_SEQUENCING_YEARS'] = '<18'
    df_f.loc[log_over89, 'AGE_AT_SEQUENCING_YEARS'] = '>' + df_f.loc[log_over89, 'AGE_AT_SEQUENCING_YEARS']

    ## Deidentify age
    log_under18 = df_f['AGE_AT_SEQUENCING_YEARS_PHI'] < 18
    log_over89_fix = (df_f['AGE_AT_SEQUENCING_YEARS_PHI'] > 89)
    log_over89 = (df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'] > 89) | log_over89_fix

    ### Create new anonymized column for age at seq
    df_f['AGE_AT_SEQUENCING_YEARS'] = df_f['AGE_AT_SEQUENCING_YEARS_PHI']
    df_f.loc[log_over89_fix, 'AGE_AT_SEQUENCING_YEARS'] = 89
    df_f['AGE_AT_SEQUENCING_YEARS'] = df_f['AGE_AT_SEQUENCING_YEARS'].astype(str)

    df_f.loc[log_under18, 'AGE_AT_SEQUENCING_YEARS'] = '<18'
    df_f.loc[log_over89, 'AGE_AT_SEQUENCING_YEARS'] = '>' + df_f.loc[log_over89, 'AGE_AT_SEQUENCING_YEARS']

    ## Drop columns that contain PHI
    cols_keep = ['DMP_ID', 'SAMPLE_ID', 'AGE_AT_SEQUENCING_YEARS']
    df_f = df_f[cols_keep]

    # Save dataframe
    obj_minio.save_obj(
        df=df_f,
        path_object=fname_save_age_at_seq,
        sep='\t'
    )

    return df_f


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="cBioPortal timeline file for cancer progression predictions")
    # parser.add_argument(
    #     "--fname_log",
    #     action="store",
    #     dest="fname_log",
    #     help="Log to indicate data is complete and can be pushed to datahub."
    # )
    # args = parser.parse_args()

    ENV_MINIO = config_cdm.minio_env
    fname_demo = config_cdm.fname_demo
    fname_samples = config_cdm.fname_path_clean
    fname_save_age_at_seq = 'cbioportal/age_at_sequencing.tsv'

    compute_age_at_sequencing(
        minio_env=ENV_MINIO,
        fname_demo=fname_demo,
        fname_samples=fname_samples,
        fname_save_age_at_seq=fname_save_age_at_seq
    )

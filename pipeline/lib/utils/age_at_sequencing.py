"""
age_at_sequencing.py

Computes age at sequencing based on the available samples in the pathology report table (Darwin)
Uses date of birth from demographics table

Age at sequencing computed in days

Data is saved to Databricks
"""
from datetime import date

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import mrn_zero_pad
from .get_anchor_dates import get_anchor_dates

AGE_CONVERSION_FACTOR = 365.2422

def compute_age_at_sequencing(
        *,
        databricks_env,
        table_demo,
        table_samples,
        volume_path_save_age_at_seq,
        table_save_age_at_seq=None,
        catalog=None,
        schema=None
):
    """

    :param databricks_env: Databricks environment filename
    :param table_demo: Full table name for demographics table (e.g., 'catalog.schema.table')
    :param table_samples: Full table name for pathology report table
    :param volume_path_save_age_at_seq: Volume path where age at sequencing is saved
    :param table_save_age_at_seq: Optional table name to create from the data
    :param catalog: Optional catalog name for table creation
    :param schema: Optional schema name for table creation
    :return: df_f: dataframe with age at sequencing
    """
    today = date.today()

    # Load data
    ## Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=databricks_env)

    ## Load demographics for date of birth
    col_keep_demo = ['MRN', 'PT_BIRTH_DTE', 'PT_DEATH_DTE', 'PLA_LAST_CONTACT_DTE']
    cols_str_demo = ', '.join(col_keep_demo)
    sql_demo = f"SELECT {cols_str_demo} FROM {table_demo}"
    df_demo = obj_db.query_from_sql(sql=sql_demo)
    df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')

    # Convert date columns to datetime with timezone handling
    df_demo['PT_BIRTH_DTE'] = pd.to_datetime(df_demo['PT_BIRTH_DTE'], errors='coerce')
    if isinstance(df_demo['PT_BIRTH_DTE'].dtype, pd.DatetimeTZDtype):
        df_demo['PT_BIRTH_DTE'] = df_demo['PT_BIRTH_DTE'].dt.tz_localize(None)

    df_demo['PT_DEATH_DTE'] = pd.to_datetime(df_demo['PT_DEATH_DTE'], errors='coerce')
    if isinstance(df_demo['PT_DEATH_DTE'].dtype, pd.DatetimeTZDtype):
        df_demo['PT_DEATH_DTE'] = df_demo['PT_DEATH_DTE'].dt.tz_localize(None)

    df_demo['PLA_LAST_CONTACT_DTE'] = df_demo['PLA_LAST_CONTACT_DTE'].fillna(today)
    df_demo['PLA_LAST_CONTACT_DTE'] = pd.to_datetime(df_demo['PLA_LAST_CONTACT_DTE'], errors='coerce')
    if isinstance(df_demo['PLA_LAST_CONTACT_DTE'].dtype, pd.DatetimeTZDtype):
        df_demo['PLA_LAST_CONTACT_DTE'] = df_demo['PLA_LAST_CONTACT_DTE'].dt.tz_localize(None)

    df_demo['OS_DTE'] = df_demo['PT_DEATH_DTE'].fillna(df_demo['PLA_LAST_CONTACT_DTE'])

    ## Load pathology report table
    col_keep_samples = ['MRN', 'DATE_TUMOR_SEQUENCING', 'DMP_ID', 'SAMPLE_ID']
    cols_str_samples = ', '.join(col_keep_samples)
    sql_samples = f"SELECT {cols_str_samples} FROM {table_samples}"
    df_path1 = obj_db.query_from_sql(sql=sql_samples)
    df_path = df_path1.dropna()
    df_path = mrn_zero_pad(df=df_path, col_mrn='MRN')

    ## Load anchor dates
    df_archor_dates = get_anchor_dates(databricks_env, table_pathology=table_samples)
    list_sample_ids_used = list(set(df_archor_dates['DMP_ID']))

    # Clean and Combine data
    df_path_clean = df_path[df_path['SAMPLE_ID'].notnull() & df_path['DMP_ID'].isin(list_sample_ids_used)].copy()
    df_path_clean = df_path_clean[df_path_clean['SAMPLE_ID'].str.contains('-T')].copy()
    df_path_clean['DMP_ID_DERIVED'] = df_path_clean['SAMPLE_ID'].apply(lambda x: x[:9])
    df_path_clean = df_path_clean[df_path_clean['DMP_ID_DERIVED'] == df_path_clean['DMP_ID']].copy()
    df_path_clean['DATE_TUMOR_SEQUENCING'] = pd.to_datetime(df_path_clean['DATE_TUMOR_SEQUENCING'], errors='coerce')
    if isinstance(df_path_clean['DATE_TUMOR_SEQUENCING'].dtype, pd.DatetimeTZDtype):
        df_path_clean['DATE_TUMOR_SEQUENCING'] = df_path_clean['DATE_TUMOR_SEQUENCING'].dt.tz_localize(None)

    ## Merge dataframes
    df_f = df_path_clean.merge(right=df_demo, how='left', on=['MRN'])

    ## Compute age at sequencing
    df_f['AGE_AT_SEQUENCING_DAYS_PHI'] = (df_f['DATE_TUMOR_SEQUENCING'] - df_f['PT_BIRTH_DTE']).dt.days

    ## Compute OS interval
    df_f['OS_INT'] = (df_f['OS_DTE'] - df_f['DATE_TUMOR_SEQUENCING']).dt.days

    df_f['AGE_AT_SEQUENCING_YEARS_PHI'] = (df_f['AGE_AT_SEQUENCING_DAYS_PHI']/AGE_CONVERSION_FACTOR)
    df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'] = ((df_f['AGE_AT_SEQUENCING_DAYS_PHI'] + df_f['OS_INT'])/AGE_CONVERSION_FACTOR)

    print(df_f['AGE_AT_SEQUENCING_YEARS_PHI'].head())
    print(df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'].head())

    df_f['AGE_AT_SEQUENCING_YEARS_PHI'] = df_f['AGE_AT_SEQUENCING_YEARS_PHI'].fillna(-1)
    df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'] = df_f['AGE_AT_SEQUENCING_YEARS_WITH_OS_INT_PHI'].fillna(-1)

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

    # Save dataframe to Databricks
    # Prepare table info dictionary if table name provided
    dict_database_table_info = None
    if table_save_age_at_seq and catalog and schema:
        dict_database_table_info = {
            'catalog': catalog,
            'schema': schema,
            'table': table_save_age_at_seq,
            'volume_path': volume_path_save_age_at_seq,
            'sep': '\t'
        }
        print(f'Creating table: {catalog}.{schema}.{table_save_age_at_seq}')

    obj_db.write_db_obj(
        df=df_f,
        volume_path=volume_path_save_age_at_seq,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    return df_f


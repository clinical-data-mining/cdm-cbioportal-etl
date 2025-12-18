import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import mrn_zero_pad, set_debug_console
from lib.utils import cbioportal_update_config, get_anchor_dates


COLS_KEEP = ['MRN', 'AGE_LAST_FOLLOWUP', 'AGE_FIRST_CANCER_DIAGNOSIS', 'AGE_FIRST_SEQUENCING']
TABLE_DEMO = 'cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics'
TABLE_DX = 'cdsi_prod.cdm_impact_pipeline_prod.table_diagnosis_clean'
table_anchor_dates = 'cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates'
COL_ANCHOR_DATE = 'DATE_TUMOR_SEQUENCING'


def convert_col_to_datetime(df, list_cols):
    for col in list_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

def _load_data(
        fname_databricks_env,
        obj_db,
        table_demo,
        table_dx
):
    # Demographics from Databricks table
    print('Loading demographics table: %s' % table_demo)
    sql_demo = f"SELECT * FROM {table_demo}"
    df_demo = obj_db.query_from_sql(sql=sql_demo)
    df_demo = df_demo.drop_duplicates()

    # Pathology table for sequencing date
    sql_anchor_dates = f"SELECT * FROM {table_anchor_dates}"
    df_path_g = obj_db.query_from_sql(sql=sql_anchor_dates)
    # df_path_g = get_anchor_dates(fname_databricks_env=fname_databricks_env)

    # Diagnosis from Databricks table
    print('Loading diagnosis table: %s' % table_dx)
    sql_dx = f"SELECT * FROM {table_dx}"
    df_dx = obj_db.query_from_sql(sql=sql_dx)

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
    df_path_g = convert_col_to_datetime(df=df_path_g, list_cols=[COL_ANCHOR_DATE])
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
    df_f.loc[logic1, COL_ANCHOR_DATE] = pd.NaT

    df_f['AGE_FIRST_SEQUENCING'] = ((df_f[COL_ANCHOR_DATE] - df_f['PT_BIRTH_DTE']).dt.days / 365.25).fillna(
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
        fname_databricks_env,
        volume_path_save,
        table_demo,
        table_dx,
        catalog=None,
        schema=None,
        table_name=None
):
    # Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    # Load data
    print('Loading data')
    df_demo, df_path_g, df_dx = _load_data(
        fname_databricks_env=fname_databricks_env,
        obj_db=obj_db,
        table_demo=table_demo,
        table_dx=table_dx
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

    # Save data to Databricks volume
    print(f'Saving data to {volume_path_save}')

    dict_database_table_info = None
    if catalog and schema and table_name:
        dict_database_table_info = {
            'catalog': catalog,
            'schema': schema,
            'table': table_name,
            'volume_path': volume_path_save,
            'sep': '\t'
        }

    obj_db.write_db_obj(
        df=df_f,
        volume_path=volume_path_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    return df_f


def main():
    parser = argparse.ArgumentParser(description="Script for creating patient age info")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    parser.add_argument(
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="--location of Databricks environment file",
    )
    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    databricks_config = obj_yaml.config_dict.get('inputs_databricks', {})
    catalog = databricks_config.get('catalog', 'cdsi_prod')
    schema = databricks_config.get('schema', 'cdsi_data_deid')
    volume = databricks_config.get('volume', 'cdsi_data_deid_volume')
    volume_path_intermediate = databricks_config.get('volume_path_intermediate', 'cbioportal/intermediate_files/')

    # Construct paths
    volume_path_save = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}patient_age_summary_cbioportal.tsv"
    table_name = "patient_age_summary_cbioportal"

    df_merged = _process_data(
        fname_databricks_env=args.databricks_env,
        volume_path_save=volume_path_save,
        table_demo=TABLE_DEMO,
        table_dx=TABLE_DX,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )

    print('Shape of age attribute file: %s' % str(df_merged.shape))


if __name__ == '__main__':
    main()
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from lib.utils import cbioportal_update_config, get_anchor_dates
from msk_cdm.data_processing import (
    mrn_zero_pad,
    convert_col_to_datetime
)


COLS_OS = ['DMP_ID', 'OS_MONTHS', 'OS_STATUS']
COL_P_ID = 'PATIENT_ID'
TABLE_DEMO = 'cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics'

def _load_data(
    obj_db,
    table_demo,
    fname_databricks_env
):
    # Demographics from Databricks table
    print('Loading demographics table: %s' % table_demo)
    sql = f"SELECT * FROM {table_demo}"
    df_demo = obj_db.query_from_sql(sql=sql)
    df_demo = df_demo.drop_duplicates()

    # Pathology table for sequencing date (using get_anchor_dates which queries Databricks)
    df_path_g = get_anchor_dates(fname_databricks_env)
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
    fname_databricks_env,
    volume_path_save,
    table_demo,
    catalog=None,
    schema=None,
    table_name=None
):
    # Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    # Load data
    df_demo, df_path_g = _load_data(
        obj_db=obj_db,
        table_demo=table_demo,
        fname_databricks_env=fname_databricks_env
    )

    # Clean and merge data
    df_os = _clean_and_merge(
        df_demo=df_demo,
        df_path_g=df_path_g
    )

    # Add annotations for OS
    df_os_f = _create_os_cols(df_os=df_os)

    print('Shape of OS file: %s' % str(df_os_f.shape))

    # Save data to Databricks volume
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
        df=df_os_f,
        volume_path=volume_path_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
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
    volume_path_save = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}data_overall_survival.tsv"
    table_name = "data_overall_survival"

    df_os_f = _process_data(
        fname_databricks_env=args.databricks_env,
        volume_path_save=volume_path_save,
        table_demo=TABLE_DEMO,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )

    print(df_os_f.sample())
    print('Missing data:')
    print(df_os_f.isnull().sum())
    print('Shape of OS file: %s' % str(df_os_f.shape))
          
if __name__ == '__main__':
    main()
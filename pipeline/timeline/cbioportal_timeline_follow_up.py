import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pandas as pd

from lib.utils import cbioportal_update_config
from msk_cdm.databricks import DatabricksAPI


## Constants
TABLE_DEMO = 'cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics'
col_keep = ['MRN', 'MRN_CREATE_DTE', 'PLA_LAST_CONTACT_DTE', 'PT_DEATH_DTE']
col_order = ['MRN', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SUBTYPE', 'SOURCE']
rep_dict = {
    'SOURCE': {
        'MRN_CREATE_DTE': 'First Consult',
        'PT_DEATH_DTE': 'Patient Deceased',
        'PLA_LAST_CONTACT_DTE': 'Last Contact'
    }
}

def cbioportal_timeline_follow_up(
        yaml_config,
        table_demo,
        volume_path_save,
        fname_databricks_env,
        catalog=None,
        schema=None,
        table_name=None
):
    print('Parsing config file %s' % yaml_config)
    obj_yaml = cbioportal_update_config(fname_yaml_config=yaml_config)
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    ## Create timeline file for follow-up
    ### Load data from Databricks table
    print('Loading demographics table: %s' % table_demo)
    cols_str = ', '.join(col_keep)
    sql = f"SELECT {cols_str} FROM {table_demo}"
    df_demo = obj_db.query_from_sql(sql=sql)

    df_demo_f = df_demo.copy()
    df_demo_f['PT_DEATH_DTE'] = pd.to_datetime(df_demo_f['PT_DEATH_DTE'], errors='coerce')
    df_demo_f['PLA_LAST_CONTACT_DTE'] = pd.to_datetime(df_demo_f['PLA_LAST_CONTACT_DTE'], errors='coerce', format='mixed')
    if isinstance(df_demo_f['PLA_LAST_CONTACT_DTE'].dtype, pd.DatetimeTZDtype):
        df_demo_f['PLA_LAST_CONTACT_DTE'] = df_demo_f['PLA_LAST_CONTACT_DTE'].dt.tz_localize(None)
    # Remove last contact date if patient is deceased
    logic_deceased = df_demo_f['PT_DEATH_DTE'].notnull()
    df_demo_f.loc[logic_deceased, 'PLA_LAST_CONTACT_DTE'] = pd.NA

    print('Creating timeline')
    df_os_ = pd.melt(
        frame=df_demo_f,
        id_vars='MRN',
        value_vars=[
            'MRN_CREATE_DTE',
            'PLA_LAST_CONTACT_DTE',
            'PT_DEATH_DTE'
        ],
        value_name='START_DATE',
        var_name='SOURCE'
    )

    df_os_ = df_os_.assign(STOP_DATE='')
    df_os_ = df_os_.assign(EVENT_TYPE='STATUS')
    df_os_ = df_os_.assign(SUBTYPE='Follow-Up')

    df_os_ = df_os_[df_os_['START_DATE'].notnull()].copy()
    df_os_['START_DATE'] = df_os_['START_DATE'].dt.strftime('%Y-%m-%d')
    df_os_ = df_os_[col_order]
    df_os_ = df_os_.replace(rep_dict)

    ### Save data to Databricks volume
    print('Saving to Databricks volume: %s' % volume_path_save)

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
        df=df_os_,
        volume_path=volume_path_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating timeline file for follow-up")
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

    # Get configuration
    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    databricks_config = obj_yaml.config_dict.get('inputs_databricks', {})
    catalog = databricks_config.get('catalog', 'cdsi_prod')
    schema = databricks_config.get('schema', 'cdsi_data_deid')
    volume = databricks_config.get('volume', 'cdsi_data_deid_volume')
    volume_path_intermediate = databricks_config.get('volume_path_intermediate', 'cbioportal/intermediate_files/')

    # Construct paths
    table_name = "table_timeline_follow_up"
    volume_path_save = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}{table_name}.tsv"


    cbioportal_timeline_follow_up(
        yaml_config=args.config_yaml,
        table_demo=TABLE_DEMO,
        volume_path_save=volume_path_save,
        fname_databricks_env=args.databricks_env,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )

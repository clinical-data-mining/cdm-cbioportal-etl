""""
cbioportal_timeline_sequencing.py

Generates cBioPortal timeline files for sequencing dates
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from lib.utils import cbioportal_update_config


# Table and column constants
TABLE_ID_MAP = 'cdsi_prod.cdm_impact_pipeline_prod.t03_id_mapping_pathology_sample_xml_parsed'
COL_DTE_SEQ = 'DATE_TUMOR_SEQUENCING'
COL_ORDER_SEQ = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SAMPLE_ID'
]


def sequencing_timeline(
        fname_databricks_env,
        table_id_map,
        volume_path_save,
        catalog=None,
        schema=None,
        table_name=None
):
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    print('Loading ID mapping table: %s' % table_id_map)
    sql = f"SELECT MRN, DMP_ID, SAMPLE_ID, {COL_DTE_SEQ} FROM {table_id_map}"
    df_path = obj_db.query_from_sql(sql=sql)
    print('After loading')
    print(df_path.head())

    df_path = df_path.dropna().copy()
    df_path[COL_DTE_SEQ] = pd.to_datetime(
        df_path[COL_DTE_SEQ],
        errors='coerce'
    )
    if isinstance(df_path[COL_DTE_SEQ].dtype, pd.DatetimeTZDtype):
        df_path[COL_DTE_SEQ] = df_path[COL_DTE_SEQ].dt.tz_localize(None)
    print('After datetime')
    print(df_path.head())
    log1 = df_path['SAMPLE_ID'].notnull()
    log2 = df_path['SAMPLE_ID'].str.contains('T')
    log3 = df_path[COL_DTE_SEQ].notnull()  # Drop samples without sequencing date
    log = log1 & log2 & log3
    df_path_filt = df_path[log].copy()

    df_path_filt = df_path_filt.rename(columns={COL_DTE_SEQ: 'START_DATE'})
    df_path_filt = df_path_filt.assign(STOP_DATE='')
    df_path_filt = df_path_filt.assign(EVENT_TYPE='Sequencing')
    df_path_filt = df_path_filt.assign(SUBTYPE='')

    # Reorder columns
    df_samples_seq_f = df_path_filt[COL_ORDER_SEQ]

    df_samples_seq_f = df_samples_seq_f.dropna()

    print('After cleaning')
    print(df_samples_seq_f.head())

    # Save timeline to Databricks volume
    print('Saving to: %s' % volume_path_save)

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
        df=df_samples_seq_f,
        volume_path=volume_path_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    return df_samples_seq_f

def main():
    parser = argparse.ArgumentParser(description="Script for creating timeline file for sequencing dates")
    parser.add_argument(
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="--location of Databricks environment file",
    )
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
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
    table_name = "table_timeline_sequencing"
    volume_path_save = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}{table_name}.tsv"


    df_seq_timeline = sequencing_timeline(
        fname_databricks_env=args.databricks_env,
        table_id_map=TABLE_ID_MAP,
        volume_path_save=volume_path_save,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()
    
    

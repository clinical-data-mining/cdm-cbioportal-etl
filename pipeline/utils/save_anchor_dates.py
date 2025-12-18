"""
save_anchor_dates.py

This script will compute the anchor dates and save the file to Databricks
"""
import os
import sys
import argparse

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.utils import get_anchor_dates, cbioportal_update_config


def save_anchor_dates(fname_databricks_env, volume_path_save, catalog, schema, table_name):
    # Compute anchor dates
    df_path_g = get_anchor_dates(fname_databricks_env)

    # Create dictionary for table info
    dict_database_table_info = {
        'catalog': catalog,
        'schema': schema,
        'volume_path': volume_path_save,
        'table': table_name,
        'sep': '\t'
    }

    print(f'Saving anchor dates to Databricks: {volume_path_save}')
    print(f'Creating table: {catalog}.{schema}.{table_name}')

    # Save to Databricks volume and create table
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)
    obj_db.write_db_obj(
        df=df_path_g,
        volume_path=volume_path_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    print('Done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anchor dates used in the cBioPortal timeline files to deidentify")
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
    volume_path_save = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}timeline_anchor_dates.tsv"
    table_name = "timeline_anchor_dates"

    save_anchor_dates(
        fname_databricks_env=args.databricks_env,
        volume_path_save=volume_path_save,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )

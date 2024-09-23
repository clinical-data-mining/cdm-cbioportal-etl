import argparse
import os
from pathlib import Path

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from cdm_cbioportal_etl.utils import cbioportal_update_config



# Function to copy files from Datahub to Minio
def copy_files(obj_db, fname_source, dest_path_object, sep, overwrite):
    df = pd.read_csv(fname_source, sep='\t', header=0, low_memory=False)
    obj_db.write_db_obj(
        df=df,
        volume_path=dest_path_object,
        sep=sep,
        overwrite=overwrite  #,
        # dict_database_table_info=dict_database_table_info
    )

    # obj_db.save_obj(df=df, path_object=dest_path_object, sep='\t')
    print('Copied %s to %s' % (fname_source, dest_path_object))
    
    return None

def transfer_to_databricks(config_yaml):
    print('Using config file: %s:' % config_yaml)
    obj_yaml = cbioportal_update_config(fname_yaml_config=config_yaml)
    dict_files_to_copy = obj_yaml.return_dict_datahub_to_minio()
    # Databricks configs
    dict_databricks = obj_yaml.return_databricks_configs()
    print(dict_databricks)
    # Databricks processing for saving data
    fname_databricks_env = dict_databricks['fname_databricks_config']
    catalog = dict_databricks['catalog']
    schema = dict_databricks['schema']
    volume = dict_databricks['volume']
    sep = dict_databricks['sep']
    overwrite = dict_databricks['overwrite']

    dir_volume = os.path.join('/Volumes',catalog,schema,volume)

    # Setup MinIO
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)


    # Copy files
    for i,fname_source in enumerate(dict_files_to_copy):
        dest_path_object = dict_files_to_copy[fname_source]
        fname_save_databricks = os.path.join(dir_volume, dest_path_object)
        table = Path(dest_path_object).stem

        dict_database_table_info = {
            'catalog': catalog,
            'schema': schema,
            'volume_path': fname_save_databricks,
            'table': table,
            'sep': sep
        }

        ## Save files to Databricks
        copy_files(
            obj_db=obj_db,
            fname_source=fname_source,
            dest_path_object=fname_save_databricks,
            sep=sep,
            overwrite=overwrite
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for monitoring completeness of data elements")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    transfer_to_databricks(config_yaml=args.config_yaml)

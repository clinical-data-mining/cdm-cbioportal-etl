import argparse
import os
from pathlib import Path

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import cbioportal_update_config


def fix_col_names(df):
    cols_old = list(df.columns)
    cols_new = [x.upper().replace('-','_').replace(' ','_') for x in cols_old]
    dict_col_rep = dict(zip(cols_old, cols_new))
    dict_col_rep
    df = df.rename(columns=dict_col_rep)

    return df


# Function to copy files from Datahub to Minio
def copy_files(
        obj_db,
        obj_minio,
        fname_source,
        dest_path_object,
        sep,
        overwrite,
        dict_database_table_info
):
    obj = obj_minio.load_obj(path_object=fname_source)
    df = pd.read_csv(obj, sep='\t', header=0, low_memory=False)
    df = fix_col_names(df=df)

    obj_db.write_db_obj(
        df=df,
        volume_path=dest_path_object,
        sep=sep,
        overwrite=overwrite,
        dict_database_table_info=dict_database_table_info
    )

    print('Copied %s to %s' % (fname_source, dest_path_object))

    return None


def transfer_to_databricks(config_yaml):
    print('Using config file: %s:' % config_yaml)
    obj_yaml = cbioportal_update_config(fname_yaml_config=config_yaml)

    fname_minio_env = obj_yaml.return_credential_filename()

    fname_codebook_tables = obj_yaml.return_filename_codebook_tables()
    df_codebook_tables = pd.read_csv(fname_codebook_tables, sep=',')
    list_minio = list(df_codebook_tables.loc[df_codebook_tables['copy_to_databricks'].notnull(), 'cdm_source_table'])

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
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)


    # Copy files
    for i,fname_source in enumerate(list_minio):
        fname_save_databricks = os.path.join(dir_volume, fname_source)
        table = Path(fname_source).stem

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
            obj_minio=obj_minio,
            fname_source=fname_source,
            dest_path_object=fname_save_databricks,
            sep=sep,
            overwrite=overwrite,
            dict_database_table_info=dict_database_table_info
        )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for copying files on Minio into Databricks")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    transfer_to_databricks(config_yaml=args.config_yaml)

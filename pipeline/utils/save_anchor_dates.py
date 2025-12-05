"""
save_anchor_dates.py

This script will compute the anchor dates and save the file to MinIO and Databricks
"""
import os
import sys
import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.databricks import DatabricksAPI
from cdm_cbioportal_etl.utils.get_anchor_dates import get_anchor_dates
from cdm_cbioportal_etl.utils import cbioportal_update_config

FNAME_SAVE_ANCHOR_DATES = 'epic_ddp_concat/cbioportal/timeline_anchor_dates.tsv'
# Databricks configurations
catalog = 'cdsi_prod'
schema = 'cdm_idbw_impact_pipeline_prod'
volume_path = '/Volumes/cdsi_prod/cdm_idbw_impact_pipeline_prod/epic_testing/timeline_anchor_dates.tsv'
table = 'timeline_anchor_dates'
sep = '\t'
overwrite = True


def save_anchor_dates(fname_minio_env, fname_databricks_env, fname_save):

    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    # Anchor dates
    df_path_g = get_anchor_dates(fname_minio_env)

    print('Saving anchor dates to MinIO: %s' % fname_save)
    # Save dataframe to MinIO
    obj_minio.save_obj(
        df=df_path_g,
        path_object=fname_save,
        sep='\t'
    )

    # Create dictionary for table info
    dict_database_table_info = {
        'catalog': catalog,
        'schema': schema,
        'volume_path': volume_path,
        'table': table,
        'sep': sep
    }

    print(f'Saving anchor dates to Databricks: {volume_path}')
    print(f'Creating table: {catalog}.{schema}.{table}')

    # Save to Databricks volume and create table
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)
    obj_db.write_db_obj(
        df=df_path_g,
        volume_path=volume_path,
        sep=sep,
        overwrite=overwrite,
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
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="--location of Minio environment file",
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
    fname_save = FNAME_SAVE_ANCHOR_DATES

    save_anchor_dates(
        fname_minio_env=args.minio_env,
        fname_databricks_env=args.databricks_env,
        fname_save=fname_save
    )

# import os
# import sys
import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import yaml_config_parser
# sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
# from variables import (
#     DICT_FILES_TO_COPY,
#     ENV_MINIO
# )


# Function to copy files from Datahub to Minio
def copy_files(obj_minio, fname_source, dest_path_object):
    df = pd.read_csv(fname_source, sep='\t', header=0, low_memory=False)
    obj_minio.save_obj(df=df, path_object=dest_path_object, sep='\t')
    print('Copied %s to %s' % (fname_source, dest_path_object))
    
    return None

def transfer_to_minio(config_yaml):

    obj_yaml = yaml_config_parser(fname_yaml_config=args.config_yaml)
    dict_files_to_copy = obj_yaml.return_dict_datahub_to_minio()
    fname_minio_env = obj_yaml.return_credential_filename()

    # Setup MinIO
    obj_minio = MinioAPI(
        fname_minio_env=fname_minio_env
    )

    # Copy files
    for i,fname_source in enumerate(dict_files_to_copy):
        dest_path_object = dict_files_to_copy[fname_source]
        ## Save files to MinIO
        copy_files(
            obj_minio=obj_minio,
            fname_source=fname_source,
            dest_path_object=dest_path_object
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

    transfer_to_minio(config_yaml=args.config_yaml)

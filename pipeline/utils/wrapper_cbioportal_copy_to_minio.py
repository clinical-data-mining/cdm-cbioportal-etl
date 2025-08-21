import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import cbioportal_update_config


# Function to copy files from Datahub to Minio
def copy_files(obj_minio, fname_source, dest_path_object):
    df = pd.read_csv(fname_source, sep='\t', header=0, low_memory=False)
    obj_minio.save_obj(df=df, path_object=dest_path_object, sep='\t')
    print('Copied %s to %s' % (fname_source, dest_path_object))
    
    return None

def transfer_to_minio(config_yaml, fname_minio_env, path_datahub, path_minio):

    obj_yaml = cbioportal_update_config(fname_yaml_config=config_yaml)
    dict_files_to_copy = obj_yaml.return_dict_datahub_to_minio(path_datahub=path_datahub, path_minio=path_minio)

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
    parser.add_argument(
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="Location of Minio environment file",
    )
    parser.add_argument(
        "--path_minio",
        action="store",
        dest="path_minio",
        required=True,
        help="Path to MinIO files",
    )
    parser.add_argument(
        "--path_datahub",
        action="store",
        dest="path_datahub",
        required=True,
        help="Path to Datahub",
    )

    args = parser.parse_args()

    transfer_to_minio(config_yaml=args.config_yaml,
                      fname_minio_env=args.minio_env,
                      path_datahub=args.path_datahub,
                      path_minio=args.path_minio)

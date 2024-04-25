import pandas as pd

from msk_cdm.minio import MinioAPI
from constants import (
    DICT_FILES_TO_COPY, 
    ENV_MINIO
)


# Function to copy files from Datahub to Minio
def transfer_to_minio(obj_minio, fname_source, dest_path_object):
    df = pd.read_csv(fname_source, sep='\t', header=0, low_memory=False)
    obj_minio.save_obj(df=df, path_object=dest_path_object, sep='\t')
    print('Copied %s to %s' % (fname_source, dest_path_object))
    
    return None


# Setup MinIO
obj_minio = MinioAPI(
    fname_minio_env=ENV_MINIO
)

# Copy files
for i,fname_source in enumerate(DICT_FILES_TO_COPY):
    dest_path_object = DICT_FILES_TO_COPY[fname_source]
    ## Save files to MinIO
    transfer_to_minio(
        obj_minio=obj_minio, 
        fname_source=fname_source, 
        dest_path_object=dest_path_object
    )

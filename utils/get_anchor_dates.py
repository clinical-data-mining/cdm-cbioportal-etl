import os
import sys

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join('', "..", "cdm-utilities")
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            '', "..", "cdm-utilities", "minio_api"
        )
    ),
)
from data_classes_cdm import CDMProcessingVariables as config_rrpt
from minio_api import MinioAPI
from utils import mrn_zero_pad, print_df_without_index, set_debug_console, convert_to_int


FNAME_MINIO_ENV = config_rrpt.minio_env
FNAME_PATHOLOGY = config_rrpt.fname_path_clean
COLS_PATHOLOGY = ['MRN', 'DTE_PATH_PROCEDURE', 'DMP_ID', 'SAMPLE_ID']



def get_anchor_dates():
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
    print('Loading %s' % FNAME_PATHOLOGY)
    obj = obj_minio.load_obj(path_object=FNAME_PATHOLOGY)
    df_path = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=COLS_PATHOLOGY)
    
    df_path = df_path.dropna().copy()
    df_path['DTE_PATH_PROCEDURE'] = pd.to_datetime(
        df_path['DTE_PATH_PROCEDURE'],
        errors='coerce'
    )

    df_path = df_path.sort_values(by=['MRN', 'DTE_PATH_PROCEDURE'])
    df_path_g = df_path.groupby(['MRN', 'DMP_ID'])['DTE_PATH_PROCEDURE'].first().reset_index()
    
    df_path_g = mrn_zero_pad(df=df_path_g, col_mrn='MRN')
    
    # Remove mismapped cases
    filt_p0 = df_path_g['DMP_ID'] != 'P-0000000'
    df_path_g = df_path_g[filt_p0]
    filt_mismapped = df_path_g['MRN'].duplicated(keep=False) | df_path_g['DMP_ID'].duplicated(keep=False)
    df_path_g = df_path_g[~filt_mismapped]
    
    return df_path_g
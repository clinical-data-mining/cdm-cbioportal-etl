import os
import sys

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join('', "..", "..", "cdm-utilities")
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            '', "..", "utils"
        )
    ),
)

from data_classes_cdm import CDMProcessingVariables as config_rrpt
from minio_api import MinioAPI
from utils import mrn_zero_pad, print_df_without_index, set_debug_console, convert_to_int

from constants import DICT_FILES_TO_COPY


FNAME_MINIO_ENV = config_rrpt.minio_env
FNAME_PATHOLOGY = config_rrpt.fname_path_clean
COLS_PATHOLOGY = ['MRN', 'DTE_PATH_PROCEDURE', 'SAMPLE_ID']


def get_anchor_dates():
    print('Creating anchor date table from first sequencing date..')
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
    print('Loading %s' % FNAME_PATHOLOGY)
    obj = obj_minio.load_obj(path_object=FNAME_PATHOLOGY)
    df_path = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=COLS_PATHOLOGY)
    
    df_path = df_path.dropna().copy()
    df_path['DTE_PATH_PROCEDURE'] = pd.to_datetime(
        df_path['DTE_PATH_PROCEDURE'],
        errors='coerce'
    )
    
    df_path_filt = df_path[df_path['SAMPLE_ID'].notnull() & df_path['SAMPLE_ID'].str.contains('T')]
    df_path_filt['DMP_ID'] = df_path_filt['SAMPLE_ID'].apply(lambda x: x[:9])

    df_path_filt = df_path_filt.sort_values(by=['MRN', 'DTE_PATH_PROCEDURE'])
    df_path_g = df_path_filt.groupby(['MRN', 'DMP_ID'])['DTE_PATH_PROCEDURE'].first().reset_index()
    
    df_path_g = mrn_zero_pad(df=df_path_g, col_mrn='MRN')
    
    # Remove mismapped cases
    filt_mismapped = df_path_g['MRN'].duplicated(keep=False) | df_path_g['DMP_ID'].duplicated(keep=False)
    df_path_g_f = df_path_g[~filt_mismapped]
    
    df_path_g_error = df_path_g[filt_mismapped].copy()
    print('Error in mapping summary:')
    print(df_path_g_error)

    
    print('Done!')
    
    return df_path_g_f

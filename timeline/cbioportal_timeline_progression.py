""""
cbioportal_timeline_progression.py



"""
import os
import sys
import argparse

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "cdm-utilities")
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "cdm-utilities", "minio_api"
        )
    ),
)
from data_classes_cdm import CDMProcessingVariables as config_rrpt
from minio_api import MinioAPI
from get_anchor_dates import get_anchor_dates
from utils import mrn_zero_pad, print_df_without_index, set_debug_console, convert_to_int, save_appended_df


FNAME_MINIO_ENV = config_rrpt.minio_env
FNAME_PROGRESSION = config_rrpt.fname_radiology_progression_pred
FNAME_RADIOLOGY = config_rrpt.fname_radiology_full_parsed
FNAME_SAVE = config_rrpt.fname_save_progression

COL_ORDER = [
    'PATIENT_ID', 
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'SOURCE_SPECIFIC',
    'PROGRESSION',
    'INFERRED_PROGRESSION_PROB',
    'STYLE_COLOR'
]


class cBioPortalTimelineProgression:
    def __init__(self, fname_minio_env, fname_rad, fname_progression, fname_save):
        self._fname_output = fname_save
        self._fname_rad = fname_rad
        self._fname_progression_phi = fname_progression
        self._fname_output = fname_save 
        self._minio_env = fname_minio_env
        
        self._obj_minio = None

        self._df_rad_rpt = None
        self._df_anchor = None
        self._df_progression = None
        self._df_timeline = None
        
        self._init_process()
        
    def return_data(self):
        return self._df_timeline
        
    def _init_process(self):
        # Init Minio
        obj_minio = MinioAPI(fname_minio_env=self._minio_env)
        self._obj_minio = obj_minio
        
        # Load data files
        self._load_data()
        
        # Transform data
        df_f = self._transform_data()
        self._df_timeline = df_f
        
        if self._fname_output is not None:
            save_appended_df(df_f, filename=self._fname_output, sep='\t')
        
    def _load_data(self):
        # Load radiology report data
        cols_rad = ['ACCESSION_NUMBER', 'RADIOLOGY_PERFORMED_DATE', 'PROCEDURE_TYPE']
        obj = self._obj_minio.load_obj(path_object=FNAME_RADIOLOGY)
        df_rad = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=cols_rad)
        df_rad['RADIOLOGY_PERFORMED_DATE'] = pd.to_datetime(
            df_rad['RADIOLOGY_PERFORMED_DATE'],
            errors='coerce'
        )
        
        # Progression
        obj = self._obj_minio.load_obj(path_object=FNAME_PROGRESSION)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df = mrn_zero_pad(df=df, col_mrn='MRN')
        prog_threshold = 0.5
        df['progression'] = (df['inferred_progression_prob'] > prog_threshold).replace(
            {
                True: 'Progressing or Mixed', 
                False:'Improving, Stable, or Indeterminate'
            }
        )
        
        # Anchor dates
        df_path_g = get_anchor_dates()
        
        self._df_anchor = df_path_g
        self._df_rad_rpt = df_rad
        self._df_progression = df
    
    def _transform_data(self):
        df_path_g = self._df_anchor
        df_rad = self._df_rad_rpt
        df = self._df_progression
        
        # Merge
        # df_path_g = df_path_g.drop(columns='DMP_ID')
        df_f = df_path_g.merge(right=df, how='inner', on='MRN')
        df_f = df_f.merge(right=df_rad, how='inner', on='ACCESSION_NUMBER')

        df_f = df_f.dropna()
        df_f['START_DATE'] = (df_f['RADIOLOGY_PERFORMED_DATE'] - df_f['DTE_PATH_PROCEDURE']).dt.days.astype(int)
        cols_drop = ['MRN', 'DTE_PATH_PROCEDURE', 'ACCESSION_NUMBER', 'RADIOLOGY_PERFORMED_DATE']
        df_f = df_f.drop(columns=cols_drop)

        df_f = df_f.rename(
            columns={
                'DMP_ID': 'PATIENT_ID',
                'progression': 'PROGRESSION',
                'inferred_progression_prob': 'INFERRED_PROGRESSION_PROB',
                'PROCEDURE_TYPE': 'SOURCE_SPECIFIC'
            }
        )
        
        df_f = df_f.assign(STOP_DATE='')
        df_f = df_f.assign(EVENT_TYPE='Diagnosis')
        df_f = df_f.assign(SUBTYPE='Imaging Assessment')
        df_f = df_f.assign(SOURCE='Radiology')
        df_f = df_f.assign(STYLE_COLOR='')
        
        # Color scheme
        df_f.loc[df_f['PROGRESSION'] == 'Yes', 'STYLE_COLOR'] = '#D62727'
        df_f.loc[df_f['PROGRESSION'] == 'No', 'STYLE_COLOR'] = '#2AA02B'
        
        # Reorder
        df_f = df_f[COL_ORDER]
        
        return df_f


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate bert on recist model.")
    parser.add_argument(
        "--fname_minio_env",
        action="store",
        dest="fname_minio_env",
        default=FNAME_MINIO_ENV,
        help="CSV file with Minio environment variables."
    )
    args = parser.parse_args()

    cBioPortalTimelineProgression(
        fname_minio_env=FNAME_MINIO_ENV, 
        fname_rad=FNAME_RADIOLOGY, 
        fname_progression=FNAME_PROGRESSION, 
        fname_save=FNAME_SAVE
    )


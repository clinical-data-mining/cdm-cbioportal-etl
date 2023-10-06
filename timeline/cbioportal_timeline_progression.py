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
        os.path.join(os.path.dirname(__file__), '..', "..", "cdm-utilities")
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), '..', "..", "cdm-utilities", "minio_api"
        )
    ),
)
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
from data_classes_cdm import CDMProcessingVariables as config_rrpt
from minio_api import MinioAPI
from get_anchor_dates import get_anchor_dates
from utils import mrn_zero_pad, print_df_without_index, set_debug_console, convert_to_int, save_appended_df
from constants import (
    COL_ORDER_PROGRESSION,
    FNAME_SAVE_PROGRESSION_TIMELINE,
    FNAME_RADIOLOGY,
    FNAME_PROGRESSION,
    ENV_MINIO
) 


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
        df = pd.read_csv(
            obj, 
            header=0, 
            low_memory=False, 
            sep='\t',
            usecols=['MRN', 'ACCESSION_NUMBER', 'inferred_progression_prob']
        )
        df = convert_to_int(df=df, list_cols=['MRN'])
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
        print('------------ progression')
        print(df.head())
        print('------------ radiology')
        print(df_rad.head())
        print('------------ map')
        print(df_path_g.head())
        
        
        # Merge
        # df_path_g = df_path_g.drop(columns='DMP_ID')
        df_f = df_path_g.merge(right=df, how='inner', on='MRN')
        df_f = df_f.merge(right=df_rad, how='inner', on='ACCESSION_NUMBER')
        
        print('------------ Merged')
        print(df_f.head())
        print('------------------------------------')

        df_f['START_DATE'] = (df_f['RADIOLOGY_PERFORMED_DATE'] - df_f['DTE_PATH_PROCEDURE']).dt.days
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
        df_f = df_f[COL_ORDER_PROGRESSION]
        
        # Clean dates
        df_f = convert_to_int(df=df_f, list_cols=['START_DATE'])
        
        # Drop rows without data
        logic_no_pred = df_f['START_DATE'].notnull()
        logic_no_dates = df_f['INFERRED_PROGRESSION_PROB'].notnull()
        df_f = df_f[logic_no_pred & logic_no_dates]
        
        return df_f


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cBioPortal timeline file for cancer progression predictions")
    parser.add_argument(
        "--fname_minio_env",
        action="store",
        dest="fname_minio_env",
        default=ENV_MINIO,
        help="CSV file with Minio environment variables."
    )
    args = parser.parse_args()

    cBioPortalTimelineProgression(
        fname_minio_env=args.fname_minio_env, 
        fname_rad=FNAME_RADIOLOGY, 
        fname_progression=FNAME_PROGRESSION, 
        fname_save=FNAME_SAVE_PROGRESSION_TIMELINE
    )


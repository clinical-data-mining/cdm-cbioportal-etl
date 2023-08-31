""""
disease_status_cbioportal_timeline.py


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities/')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api/')))
import pandas as pd
from minio_api import MinioAPI
from utils import drop_cols, mrn_zero_pad, convert_to_int, read_minio_api_config, save_appended_df
from constants import (
    COL_ORDER_DISEASE_STATUS,
    ENV_MINIO,
    FNAME_DEMO,
    FNAME_IDS,
    FNAME_RESULTS_DISEASE_STATUS,
    FNAME_RESULTS_STATS,
    FNAME_SAVE_TIMELINE_DATAHUB
) 


class cBioPortalDiseaseStatusTimeline(object):
    def __init__(self, fname_minio_config, fname_demo, fname_results_stats, fname_id_map, fname_results, fname_save_timeline_datahub):
        self._fname_demo = fname_demo
        self._fname_id_map = fname_id_map
        self._fname_results = fname_results
        self._fname_results_stats = fname_results_stats
        self._fname_minio_config = fname_minio_config
        self._fname_save_timeline_datahub = fname_save_timeline_datahub

        # Objects
        self._obj_minio = None

        # Data frames
        ## Input dataframes
        self._df_demo = None
        self._df_ids = None
        
        ## Timeline dfs
        ### Individual
        self._df_pred_results = None
        self._df_results_stats = None
        
        # Out put frame
        self._df_merged = None
        
        # cBioPortal timeline color theme
        

        # Hardcode column names for cBioPortal formatting
        self._col_order = COL_ORDER_DISEASE_STATUS

        self._init_process()
        self._save_files()
        
    def _init_process(self):
        # Init Minio
        self._obj_minio = MinioAPI(fname_minio_env=self._fname_minio_config)
        
        # Load data files
        self._load_data()
        
        # Load organ site mapping object
        self._format_timeline()
        
    def return_timeline(self):
        return self._df_merged
        
    def _save_files(self):
        self._df_merged.to_csv(self._fname_save_timeline_datahub, 
                               sep='\t', 
                               index=False)   

    def _load_data(self):
        # Load files
        self._load_demographics()
        self._load_impact_mapping()
        self._load_pred()
        
        return None
    
    def _load_demographics(self):
        print('Loading %s' % self._fname_demo)
        obj = self._obj_minio.load_obj(path_object=self._fname_demo)
        df_demo1 = pd.read_csv(
            obj, 
            sep='\t', 
            low_memory=False, 
            dtype={'MRN': object}
        )

        cols_keep = ['MRN', 'PT_BIRTH_DTE']
        df_demo = df_demo1[cols_keep]
        
        self._df_demo = df_demo
        
        return None
    
    def _load_impact_mapping(self):
        print('Loading %s' % self._fname_id_map)
        
        obj = self._obj_minio.load_obj(path_object=self._fname_id_map)
        df_ids1 = pd.read_csv(obj, sep='\t', dtype={'MRN': object}, low_memory=False)
        df_ids1 = mrn_zero_pad(df=df_ids1, col_mrn='MRN')
        cols_keep = ['MRN', 'DMP_ID']
        df_ids = df_ids1[cols_keep].dropna().drop_duplicates()
        
        self._df_ids = df_ids
    
    def _load_pred(self):
        # Radiology NLP predictions
        print('Loading %s' % self._fname_results)
        df_results1 = pd.read_csv(self._fname_results, dtype={'CDD_MRN': object})
        # df_results1 = mrn_zero_pad(df=df_results1, col_mrn='MRN')
        col_dict_results = {'CDD_MRN': 'MRN',
                            'CDD_SERVICE_DTE': 'START_DATE',
                            'CDD_DOC_NAME': 'SOURCE_SPECIFIC',
                            'predictions': 'DISEASE_STATUS_PREDICTED',
                            'label': 'DISEASE_STATUS_KNOWN',
                           }

        df_results = df_results1.rename(columns=col_dict_results)
        df_results = df_results[list(col_dict_results.values())]

        df_results = df_results.assign(STOP_DATE='')
        df_results = df_results.assign(EVENT_TYPE='Diagnosis')
        df_results = df_results.assign(SUBTYPE='Disease Status')
        df_results = df_results.assign(SOURCE='ClinDoc')
        
        self._df_pred_results = df_results
        
        # ----------------------------------
        print('Loading %s' % self._fname_results_stats)
        df_results_stats = pd.read_csv(self._fname_results_stats, dtype={'CDD_MRN': object})
        col_dict_val = {'CDD_MRN': 'MRN',
                        'percent_notes_predicted_met_after_flip_date_predicted': 'SCORE'
                        }
        df_results_stats = df_results_stats.rename(columns=col_dict_val)
        df_results_stats = df_results_stats[['MRN', 'SCORE']]
        
        self._df_results_stats = df_results_stats
        
        return None
    
    def _format_timeline(self):
        df_merged = self._df_pred_results.merge(right=self._df_results_stats, how='left', on='MRN')
        df_merged = df_merged.merge(right=self._df_demo, how='left', on='MRN')
        df_merged = self._df_ids.merge(right=df_merged, how='right', on='MRN')
        df_merged = df_merged[df_merged['DMP_ID'].notnull()]

        df_merged['START_DATE'] = pd.to_datetime(df_merged['START_DATE'])
        df_merged['PT_BIRTH_DTE'] = pd.to_datetime(df_merged['PT_BIRTH_DTE'])

        start_date_days = (df_merged['START_DATE'] - df_merged['PT_BIRTH_DTE']).dt.days
        df_merged['START_DATE'] = start_date_days
        df_merged = df_merged[df_merged['START_DATE'].notnull()]
        df_merged['START_DATE'] = df_merged['START_DATE'].astype(int)
        df_merged = df_merged.drop(columns=['PT_BIRTH_DTE', 'MRN'])

        df_merged = df_merged.rename(columns={'DMP_ID': 'PATIENT_ID'})
        df_merged = df_merged.sort_values(by=['PATIENT_ID', 'START_DATE'])

        df_merged = df_merged.replace({'DISEASE_STATUS_PREDICTED': {0: 'Not Metastatic', 1: 'Metastatic'},
                                       'DISEASE_STATUS_KNOWN': {0: 'Not Metastatic', 1: 'Metastatic'}})
        
        ## Define color encoding
        df_merged['idx'] = df_merged.groupby(['PATIENT_ID']).cumcount()
        df_merged['DISEASE_STATUS_PREDICTED_TMP'] = df_merged['DISEASE_STATUS_PREDICTED']
        log1 = (df_merged['DISEASE_STATUS_PREDICTED_TMP'] == 'Metastatic') & (df_merged['idx'] == 1) & (df_merged['SCORE'] < 50)
        df_merged.loc[log1, 'DISEASE_STATUS_PREDICTED_TMP'] = 'indeterminate'

        ## Color encode
        df_merged['STYLE_COLOR'] = df_merged['DISEASE_STATUS_PREDICTED_TMP'].map({'Metastatic': '#D62727', 
                                                                                  'Not Metastatic': '#2AA02B',
                                                                                  'indeterminate': '#DAA520'}
                                                                                )
        df_merged_f = df_merged[self._col_order]
        
        self._df_merged = df_merged_f
        
        return None

    
def main():

    
    obj_ds_timeline = cBioPortalDiseaseStatusTimeline(
        fname_minio_config=ENV_MINIO,
        fname_demo=FNAME_DEMO, 
        fname_id_map=FNAME_IDS,
        fname_results=FNAME_RESULTS_DISEASE_STATUS,
        fname_results_stats=FNAME_RESULTS_STATS,
        fname_save_timeline_datahub=FNAME_SAVE_TIMELINE_DATAHUB
    )

if __name__ == '__main__':
    main()

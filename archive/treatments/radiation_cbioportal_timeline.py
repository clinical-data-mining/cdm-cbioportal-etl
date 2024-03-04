""""
radiation_cbioportal_timeline.py

By Chris Fong - MSKCC 2022


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import pandas as pd
from minio_api import MinioAPI
from utils import drop_cols, mrn_zero_pad, convert_to_int, read_minio_api_config, save_appended_df


class cBioPortalRadiationTherapy(object):
    def __init__(self, fname_minio_config, fname_rt, fname_demo, fname_path, fname_save):
        self._fname_rt = fname_rt
        self._fname_demo = fname_demo
        self._fname_path = fname_path
        self._fname_save_timeline = fname_save
        self._minio_env = fname_minio_config

        # Objects
        self._obj_minio = None

        # Data frames
        ## Input dataframes
        self._df_demo = None
        self._df_ids = None
        self._df_rt = None
        
        ## Timeline dfs
        self._df = None
        
        # Hardcode column names for cBioPortal formatting        
        self._dict_rename = {'DMP_ID': 'PATIENT_ID',
                             'PLAN_NAME': 'PLAN'
                            }
        self._cols_order = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 
                            'EVENT_TYPE', 'SUBTYPE', 'PLAN', 
                            'DELIVERED_DOSE', 'PLANNED_FRACTIONS', 
                            'REFERENCE_POINT']
        self._init_process()
        
    def _init_process(self):
        # Init Minio
        obj_minio = MinioAPI(fname_minio_env=self._minio_env)
        self._obj_minio = obj_minio
        
        # Load data files
        self._load_data()
        
        # Load organ site mapping object
        df_timeline = self._create_rt_timeline()
        self._df = df_timeline
        
        if self._fname_save_timeline is not None:
            save_appended_df(df_timeline, filename=self._fname_save_timeline, sep='\t')

    def _load_data(self):
        # Load files
        self._load_demographics()
        self._load_path()
        self._load_rt()
        
        return None
    
    def _load_demographics(self):
        print('Loading %s' % self._fname_demo)
        obj = self._obj_minio.load_obj(path_object=self._fname_demo)
        usecols = ['MRN', 'PT_BIRTH_DTE']
        df_demo = pd.read_csv(obj, header=0, low_memory=False, sep='\t',usecols=usecols)  
        df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
        df_demo['PT_BIRTH_DTE'] = pd.to_datetime(df_demo['PT_BIRTH_DTE'])
        
        self._df_demo = df_demo
        
        return None
    
    def _load_path(self):
        ### Load Dx timeline data
        fname = self._fname_path
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_id = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=['MRN', 'DMP_ID'])
        df_id.loc[df_id['DMP_ID'].notnull()].drop_duplicates()
        df_id = mrn_zero_pad(df=df_id, col_mrn='MRN').drop_duplicates()
        
        self._df_ids = df_id
        
        return None
    
    def _load_rt(self):
        ## Meds
        fname = self._fname_rt
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_radonc1 = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df_radonc = mrn_zero_pad(df=df_radonc1, col_mrn='MRN')
        df_radonc['RADONC_START_DTE'] = pd.to_datetime(df_radonc['RADONC_START_DTE'])
        df_radonc['RADONC_END_DTE'] = pd.to_datetime(df_radonc['RADONC_END_DTE'])
        
        self._df_rt = df_radonc
        
    def _create_rt_timeline(self):
        df_radonc = self._df_rt
        df_demo = self._df_demo
        df_id = self._df_ids
        
        df_radonc_f = df_radonc.merge(right=df_demo, how='left', on='MRN') 
        df_radonc_f = df_id.merge(right=df_radonc_f, how='right', on='MRN')
        df_radonc_f = df_radonc_f[df_radonc_f['DMP_ID'].notnull()]

        ### Convert dates to age
        START_DATE = (df_radonc_f['RADONC_START_DTE'] - df_radonc_f['PT_BIRTH_DTE']).dt.days
        STOP_DATE = (df_radonc_f['RADONC_END_DTE'] - df_radonc_f['PT_BIRTH_DTE']).dt.days
        df_radonc_f = df_radonc_f.assign(START_DATE=START_DATE)
        df_radonc_f = df_radonc_f.assign(STOP_DATE=STOP_DATE)
        ### Add columns to complete formatting
        df_radonc_f = df_radonc_f.assign(EVENT_TYPE='TREATMENT')
        df_radonc_f = df_radonc_f.assign(SUBTYPE='Radiation Therapy')

        ### Rename and reorder columns
        df_radonc_f = df_radonc_f.rename(columns=self._dict_rename)
        df_radonc_f = df_radonc_f[self._cols_order]
        
        return df_radonc_f
 

def main():
    import os
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as config_cdm

    
    fname_rt = config_cdm.fname_rt
    fname_path = config_cdm.fname_path_clean
    fname_demo = config_cdm.fname_demo
    fname_save_rt_timeline = config_cdm.fname_save_rt_timeline

    obj_dx_timeline = cBioPortalRadiationTherapy(fname_minio_config=config_cdm.minio_env, 
                                            fname_rt=fname_rt, 
                                            fname_demo=fname_demo, 
                                            fname_path=fname_path, 
                                            fname_save=fname_save_rt_timeline)


if __name__ == '__main__':
    main()

""""
surgery_cbioportal_timeline.py

By Chris Fong - MSKCC 2022


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import pandas as pd
import numpy as np
from minio_api import MinioAPI
from utils import drop_cols, mrn_zero_pad, convert_to_int, read_minio_api_config, save_appended_df


class cBioPortalSurgery(object):
    def __init__(self, fname_minio_config, fname_surgery, fname_ir, fname_demo, fname_path, fname_save):
        self._fname_surg = fname_surgery
        self._fname_ir = fname_ir
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
        self._df_ir = None
        self._df_surg = None
        
        ## Timeline dfs
        self._df = None
        
        # Hardcode column names for cBioPortal formatting (TODO)        

        self._init_process()
        
    def _init_process(self):
        # Init Minio
        obj_minio = MinioAPI(fname_minio_env=self._minio_env)
        self._obj_minio = obj_minio
        
        # Load data files
        self._load_data()
        
        # Transform data
        df_ir_timeline = self._create_ir_timeline()
        df_surg_timeline = self._create_surg_timeline()
        
        # Combine timeline data
        df_surg_and_ir = pd.concat([df_surg_timeline, df_ir_timeline], axis=0, sort=False)
        df_surg_and_ir = df_surg_and_ir.sort_values(by=['PATIENT_ID', 'START_DATE'])
        
        self._df = df_surg_and_ir
        
        if self._fname_save_timeline is not None:
            save_appended_df(df_surg_and_ir, filename=self._fname_save_timeline, sep='\t')

    def _load_data(self):
        # Load files
        self._load_demographics()
        self._load_path()
        self._load_surg()
        self._load_ir()
        
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
    
    def _load_surg(self):
        ## Surgery
        fname = self._fname_surg
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_surg = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df_surg = mrn_zero_pad(df=df_surg, col_mrn='MRN')
        df_surg['DATE_OF_PROCEDURE'] = pd.to_datetime(df_surg['DATE_OF_PROCEDURE'])
        
        self._df_surg = df_surg
        
    def _load_ir(self):
        ## Interventional radiology
        fname = self._fname_ir
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_ir = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=['MRN', 'REPORT_DATE', 'RADIOLOGY_PROCEDURE_DESC'])
        df_ir = mrn_zero_pad(df=df_ir, col_mrn='MRN')
        df_ir['REPORT_DATE'] = pd.to_datetime(df_ir['REPORT_DATE'])
        
        self._df_ir = df_ir
        
    def _create_ir_timeline(self):
        df_ir = self._df_ir
        df_demo = self._df_demo
        df_id = self._df_ids
        
        dict_rename = {'DMP_ID': 'PATIENT_ID',
              'RADIOLOGY_PROCEDURE_DESC': 'PROCEDURE_DESCRIPTION'
              }
        cols_drop = ['MRN', 'REPORT_DATE', 'PT_BIRTH_DTE']
        cols_order = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SUBTYPE', 'PROCEDURE_DESCRIPTION']

        df_ir_f = df_ir.merge(right=df_demo, how='left', on='MRN') 
        df_ir_f = df_id.merge(right=df_ir_f, how='right', on='MRN')
        df_ir_f = df_ir_f[df_ir_f['DMP_ID'].notnull()]

        ### Convert dates to age
        START_DATE = (df_ir_f['REPORT_DATE'] - df_ir_f['PT_BIRTH_DTE']).dt.days
        df_ir_f = df_ir_f.assign(START_DATE=START_DATE)
        df_ir_f = df_ir_f.assign(STOP_DATE=np.NaN)
        ### Add columns to complete formatting
        df_ir_f = df_ir_f.assign(EVENT_TYPE='SURGERY')
        df_ir_f = df_ir_f.assign(SUBTYPE='Interventional Radiology')

        ### Rename and reorder columns
        df_ir_f = df_ir_f.rename(columns=dict_rename)
        df_ir_f = df_ir_f[cols_order]
        
        return df_ir_f
    
    def _create_surg_timeline(self):
        df_surg = self._df_surg
        df_demo = self._df_demo
        df_id = self._df_ids
        
        dict_rename = {'DMP_ID': 'PATIENT_ID'
              }
        cols_order = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SUBTYPE', 'PROCEDURE_DESCRIPTION']

        df_surg_f = df_surg.merge(right=df_demo, how='left', on='MRN') 
        df_surg_f = df_id.merge(right=df_surg_f, how='right', on='MRN')
        df_surg_f = df_surg_f[df_surg_f['DMP_ID'].notnull()]

        ### Convert dates to age
        START_DATE = (df_surg_f['DATE_OF_PROCEDURE'] - df_surg_f['PT_BIRTH_DTE']).dt.days
        df_surg_f = df_surg_f.assign(START_DATE=START_DATE)
        df_surg_f = df_surg_f.assign(STOP_DATE=np.NaN)
        ### Add columns to complete formatting
        df_surg_f = df_surg_f.assign(EVENT_TYPE='SURGERY')
        df_surg_f = df_surg_f.assign(SUBTYPE='Surgery')

        ### Rename and reorder columns
        df_surg_f = df_surg_f.rename(columns=dict_rename)
        df_surg_f = df_surg_f[cols_order]
        
        return df_surg_f
 

def main():
    import os
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as config_cdm
    
    fname_ir = config_cdm.fname_ir
    fname_surg = config_cdm.fname_surg
    fname_path = config_cdm.fname_path_clean
    fname_demo = config_cdm.fname_demo
    fname_save_rt_timeline = config_cdm.fname_save_surg_timeline

    obj_dx_timeline = cBioPortalSurgery(fname_minio_config=config_cdm.minio_env, 
                                         fname_ir=fname_ir,
                                         fname_surgery=fname_surg,
                                         fname_demo=fname_demo, 
                                         fname_path=fname_path, 
                                         fname_save=fname_save_rt_timeline)


if __name__ == '__main__':
    main()

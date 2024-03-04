""""
medications_cbioportal_timeline.py

By Chris Fong - MSKCC 2022


"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import pandas as pd
from minio_api import MinioAPI
from utils import drop_cols, mrn_zero_pad, convert_to_int, read_minio_api_config, save_appended_df


class cBioPortalMedicationsTimeline(object):
    def __init__(self, fname_minio_config, fname_meds, fname_demo, fname_path, fname_save):
        self._fname_meds = fname_meds
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
        self._df_meds = None
        
        ## Outputs
        self._df_meds_timeline = None
        
        # Hardcode column names for cBioPortal formatting
        self._cols_treatment_type = ['CDG_CHEMO_FLAG', 
                       'CDG_BIOLOGIC_IMMUNO_FLAG', 
                       'CDG_TARGETED_FLAG', 
                       'CDG_HORMONE_FLAG', 
                       'CDG_IMMUNOTHERAPY_FLAG', 
                       'APR_PLACEBO_FLAG', 
                       'CDG_BONE_AGENTS_FLAG']
        self._dict_replace_treat = {'CDG_CHEMO_FLAG': 'Chemo', 
                               'CDG_BIOLOGIC_IMMUNO_FLAG': 'Biologic', 
                               'CDG_TARGETED_FLAG': 'Targeted', 
                               'CDG_HORMONE_FLAG': 'Hormone', 
                               'CDG_IMMUNOTHERAPY_FLAG': 'Immuno', 
                               'APR_PLACEBO_FLAG': 'Other', 
                               'CDG_BONE_AGENTS_FLAG': 'Bone Treatment'}
        
        self._dict_rename = {'DMP_ID': 'PATIENT_ID',
              'CDG_COMMON_GENERIC_NAME': 'AGENT',
              'APR_SET_NAME': 'THERAPY',
              'APR_ORD_RX_STATUS': 'RX_STATUS',
              'APR_ROUTE': 'RX_ROUTE',
              'APR_INVESTIGATIVE_FLAG': 'RX_INVESTIGATIVE'}
        self._col_order = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'TREATMENT_TYPE', 'AGENT', 'THERAPY', 'SUBTYPE', 'RX_STATUS', 'RX_ROUTE', 'RX_INVESTIGATIVE']
        
        
        self._col_regimen = 'APR_SET_NAME'
        self._col_investigative = 'APR_INVESTIGATIVE_FLAG'
        self._col_admin_route = 'APR_ROUTE'
        self._col_generic_name = 'CDG_COMMON_GENERIC_NAME'
        self._col_status = 'APR_ORD_RX_STATUS'
        self._col_id = 'MRN'
        self._cols_dates = ['APR_START_DTE', 'APR_END_DTE']
        self._cols_index = [self._col_id] + [self._col_generic_name] + [self._col_regimen] + [self._col_status] + [self._col_admin_route] + [self._col_investigative] + self._cols_dates + ['OTHER_DESC']
        self._cols_groupby_index = [self._col_id] + [self._col_generic_name] + [self._col_regimen] + [self._col_status] + [self._col_admin_route] + [self._col_investigative] + ['OTHER_DESC', 'SUBTYPE']


        self._cols_strip = [self._col_generic_name] + [self._col_regimen] + [self._col_status] + [self._col_admin_route] + [self._col_investigative] + self._cols_treatment_type
        self._cols_keep = [self._col_id] + self._cols_strip + self._cols_dates

        self._init_process()
        
    def _init_process(self):
        # Init Minio
        obj_minio = MinioAPI(fname_minio_env=self._minio_env)
        self._obj_minio = obj_minio
        
        # Load data files
        self._load_data()
        
        # Load organ site mapping object
        df_meds_timeline = self._create_meds_timeline()
        self._df_meds_timeline = df_meds_timeline
        
        if self._fname_save_timeline is not None:
            save_appended_df(df_meds_timeline, filename=self._fname_save_timeline, sep='\t')

    def _load_data(self):
        # Load files
        self._load_demographics()
        self._load_path()
        self._load_meds()
        
        return None
    
    def _load_demographics(self):
        print('Loading %s' % self._fname_demo)
        obj = self._obj_minio.load_obj(path_object=self._fname_demo)
        usecols = ['MRN', 'PT_BIRTH_DTE']
        df_demo = pd.read_csv(obj, header=0, low_memory=False, sep='\t',usecols=usecols)  
        df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
        df_demo['PT_BIRTH_DTE'] = pd.to_datetime(df_demo['PT_BIRTH_DTE'], errors='coerce')
        
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
    
    def _load_meds(self):
        ## Meds
        fname = self._fname_meds
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_meds1 = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df_meds = mrn_zero_pad(df=df_meds1, col_mrn='MRN')
        df_meds[self._cols_strip] = df_meds[self._cols_strip].apply(lambda x: x.str.strip())
        df_meds.loc[df_meds['APR_SET_NAME'] == '', 'APR_SET_NAME'] = 'Unlabeled Regimen'
        df_meds = df_meds[self._cols_keep].copy()
        df_meds[self._cols_dates] = df_meds[self._cols_dates].apply(lambda x: pd.to_datetime(x, errors='coerce'))
        
        self._df_meds = df_meds
        
    def _create_meds_timeline(self):
        df_meds_clean = self._df_meds
        df_demo = self._df_demo
        df_id = self._df_ids
        
        cols_treatment_type_ex_chemo = [x for x in self._cols_treatment_type if x is not 'CDG_CHEMO_FLAG']

        ### Clear chemo flag if others are marked
        logic_clear_chemo = ((df_meds_clean[cols_treatment_type_ex_chemo] == 'Y').sum(axis=1) > 0) & (df_meds_clean['CDG_CHEMO_FLAG'] == 'Y')
        df_meds_clean.loc[logic_clear_chemo, 'CDG_CHEMO_FLAG'] = ''

        ### Clear immuno/biologic flag is immuno is marked
        logic_clear_io_biologic = (df_meds_clean['CDG_IMMUNOTHERAPY_FLAG'] == 'Y') & (df_meds_clean['CDG_BIOLOGIC_IMMUNO_FLAG'] == 'Y')
        df_meds_clean.loc[logic_clear_io_biologic, 'CDG_BIOLOGIC_IMMUNO_FLAG'] = ''

        ### For remaining rows with multiple flags, mark as Other. Combine flags that are marked
        log = (df_meds_clean[self._cols_treatment_type] == 'Y').sum(axis=1) > 1
        df_meds_clean = df_meds_clean.assign(Other=log).replace({'Other': {True: 'Y', False: ''}})
        col_combined = df_meds_clean.loc[(df_meds_clean['Other'] == 'Y'), self._cols_treatment_type].apply(lambda row: str(list(row[row == 'Y'].index)), axis=1)
        df_meds_clean = df_meds_clean.assign(OTHER_DESC=col_combined)
        #### Clear all other columns if Other is 'Y'
        df_meds_clean.loc[log, self._cols_treatment_type] = ''

        ### Fill in Other Desc with APR_PLACEBO_FLAG, CDG_BONE_AGENTS_FLAG
        df_meds_clean.loc[df_meds_clean['APR_PLACEBO_FLAG'] == 'Y', 'OTHER_DESC'] = 'Placebo'
        df_meds_clean.loc[df_meds_clean['CDG_BONE_AGENTS_FLAG'] == 'Y', 'OTHER_DESC'] = 'Bone Agent'

        ## Melt medication subtype flags into a single columns
        df_meds_m1 = pd.melt(frame=df_meds_clean, 
                             id_vars=self._cols_index, 
                             value_vars=self._cols_treatment_type + ['Other'], 
                             var_name='SUBTYPE', 
                             value_name='value')
        df_meds_m1 = df_meds_m1[df_meds_m1['value'] != '']
        df_meds_m1 = df_meds_m1.replace({'SUBTYPE': self._dict_replace_treat})
        df_meds_m1 = df_meds_m1.drop(columns='value')
        df_meds_m1 = df_meds_m1.fillna('')

        ## Consolidate med admin such that a single drug in a regimen is grouped together
        df_start_date = df_meds_m1.groupby(self._cols_groupby_index)['APR_START_DTE'].min().reset_index()
        df_start_date = df_start_date.rename(columns={'APR_START_DTE': 'START_DATE_'})
        df_meds_m1['APR_END_DTE'] = pd.to_datetime(df_meds_m1['APR_END_DTE'], errors='coerce')
        df_stop_date = df_meds_m1.groupby(self._cols_groupby_index)['APR_END_DTE'].max().reset_index()
        df_stop_date = df_stop_date.rename(columns={'APR_END_DTE': 'STOP_DATE_'})
        df_meds_g = df_start_date.merge(right=df_stop_date, how='outer', on=self._cols_groupby_index)

        ## Remove PHI and organize/clean columns
        ### join data
        df_meds_f = df_meds_g.merge(right=df_demo, how='left', on='MRN') 
        df_meds_f = df_id.merge(right=df_meds_f, how='right', on='MRN')
        df_meds_f = df_meds_f[df_meds_f['DMP_ID'].notnull()]

        ### CHANGE DATES TO AGE
        START_DATE = (df_meds_f['START_DATE_'] - df_meds_f['PT_BIRTH_DTE']).dt.days
        STOP_DATE = (df_meds_f['STOP_DATE_'] - df_meds_f['PT_BIRTH_DTE']).dt.days
        df_meds_f = df_meds_f.assign(START_DATE=START_DATE)
        df_meds_f = df_meds_f.assign(STOP_DATE=STOP_DATE)
        df_meds_f = convert_to_int(df=df_meds_f, list_cols=['STOP_DATE'])

        ### Add extra columns
        df_meds_f = df_meds_f.assign(EVENT_TYPE='TREATMENT')
        df_meds_f = df_meds_f.assign(TREATMENT_TYPE='Medical Therapy')

        ### Rename columns
        df_meds_f = df_meds_f.rename(columns=self._dict_rename)

        ### Drop columns
        df_meds_f = df_meds_f.drop(columns=['MRN', 'START_DATE_' ,'STOP_DATE_'])

        ### Reorder columns
        df_meds_f = df_meds_f[self._col_order]

        ### Switch end and start date if end date is before start date
        log_switch = (df_meds_f['START_DATE'] > df_meds_f['STOP_DATE'])
        START_DATE_switch = df_meds_f.loc[log_switch, 'STOP_DATE']
        STOP_DATE_switch = df_meds_f.loc[log_switch, 'START_DATE']

        df_meds_f.loc[log_switch, 'START_DATE'] = START_DATE_switch
        df_meds_f.loc[log_switch, 'STOP_DATE'] = STOP_DATE_switch
        
        return df_meds_f
    
def main():
    import os
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as config_cdm
    
    fname_meds = config_cdm.fname_meds
    fname_path = config_cdm.fname_path_clean
    fname_demo = config_cdm.fname_demo
    fname_save_meds_timeline = config_cdm.fname_save_meds_timeline

    obj_dx_timeline = cBioPortalMedicationsTimeline(fname_minio_config=config_cdm.minio_env, 
                                            fname_meds=fname_meds, 
                                            fname_demo=fname_demo, 
                                            fname_path=fname_path, 
                                            fname_save=fname_save_meds_timeline)


if __name__ == '__main__':
    main()

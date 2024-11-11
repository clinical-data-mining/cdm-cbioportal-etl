""""
cbioportal_summary_merger.py

This class will update clinical patient and sample summary files on cbioportal.
Object requires the path to the original sample/patient file, and for new/replacement annotations, a header and dataframe file (csv format)
"""
import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import constants

COLS_PRODUCTION = constants.COLS_PRODUCTION
COLS_TESTING = constants.COLS_TESTING


class cBioPortalSummaryMergeTool(object):
    def __init__(
        self, 
        fname_minio_env, 
        fname_current_summary, 
        production_or_test='production'
    ):
        # Input filenames
        self._fname_minio_env = fname_minio_env
        self._fname_current_summary = fname_current_summary
        self._production_or_test = production_or_test
        
        # Transpose header df
        if self._production_or_test == 'production':
            self._cols_header = COLS_PRODUCTION
            self._row_label = 3
        elif self._production_or_test == 'test':
            self._cols_header = COLS_TESTING
            self._row_label = 4
        else:
            cols = COLS_PRODUCTION
            self._row_label = 3
        
        # DataFrame variables
        self._df_summary_orig = None
        self._df_summary_addition = None
        self._df_summary_output = None
        self._df_header_orig = None
        self._df_header_addition = None
        self._df_header_output = None
        self._df_summary_final = None
        
        # Process data
        self._obj_minio = MinioAPI(fname_minio_env=self._fname_minio_env)
        self._process_data()
        
    def return_orig(self):
        return self._df_header_orig, self._df_summary_orig
    
    def return_addition(self):
        return self._df_header_addition, self._df_summary_addition
    
    def return_output(self):
        return self._df_header_output, self._df_summary_output
    
    def return_final(self):
        return self._df_summary_final
        
    def _process_data(self):
        # Load summary data
        df_header, df_data = self._summary_loader(fname=self._fname_current_summary)
        
        self._df_summary_orig = df_data
        self._df_header_orig = df_header
        
    def merge_annotations(self, patient_or_sample):
        # Check if new annotation data exists
        if patient_or_sample == 'sample':
            col_key = '#Sample Identifier'
        elif patient_or_sample == 'patient':
            col_key = '#Patient Identifier'
            
        if (self._df_summary_addition is not None) and (self._df_header_addition is not None):
            # print('----------------------------------------------------------------')
            # Sample summary
            df_data_merged = self._summary_data_merger(
                df_main=self._df_summary_orig, 
                df_new=self._df_summary_addition, 
                df_header_main=self._df_header_orig, 
                df_header_new=self._df_header_addition, 
                col_key=col_key
            )
            
            df_header_merged = self._summary_header_merger(
                header_main=self._df_header_orig, 
                header_new=self._df_header_addition, 
                list_keys_new=[col_key]
            )
            
            # Clean header so that all label values are upper case
            df_header_merged.loc[self._row_label, :] = df_header_merged.loc[self._row_label, :].str.upper()
            df_data_merged.columns = df_data_merged.columns.str.upper() 
            
            # Save as member variable
            self._df_summary_output = df_data_merged
            self._df_header_output = df_header_merged
            
            # print(df_header_merged)
            # print(df_data_merged)
            
            # Merge header and data file
            self._summary_header_data_combiner()
        
    def _summary_loader(self, fname):
        print('Loading %s' % fname)
        if self._production_or_test == 'test':
            nrows = 5
        elif self._production_or_test == 'production':
            nrows = 4
            
        obj = self._obj_minio.load_obj(path_object=fname)
        df_data = pd.read_csv(obj, header=nrows, sep='\t', dtype=str)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_header = pd.read_csv(obj, header=0, sep='\t', nrows=nrows)

        return df_header, df_data

    def add_annotation_loader(
        self, 
        fname_header, 
        fname_data
    ):
        print('Loading %s' % fname_data)
        obj = self._obj_minio.load_obj(path_object=fname_data)
        df_data = pd.read_csv(obj, header=0, sep=',', dtype=str)
        print(df_data.sample(1))
        print('Loading %s' % fname_header)        
        obj = self._obj_minio.load_obj(path_object=fname_header)
        df_header = pd.read_csv(obj, header=0, sep=',', dtype=str)
        print(df_header.sample(1))
            
        df_header = df_header[self._cols_header]
        df_header_f = df_header.T.reset_index(drop=True)

        # Convert first row to column names
        df_header_f = df_header_f.rename(columns=df_header_f.iloc[0]).drop(df_header_f.index[0]).reset_index(drop=True)
        
        self._df_summary_addition = df_data
        self._df_header_addition = df_header_f

    def _summary_header_merger(self, header_main, header_new, list_keys_new):
        header_new = header_new.drop(columns=list_keys_new)
        
        # Check if current summary has same columns and drop common from current summary (This will replace columns)
        cols_current_raw = list(header_main.loc[self._row_label])
        cols_replace = list(set.intersection(set(cols_current_raw), set(list(header_new.loc[self._row_label]))))

        cols_drop_index = [x in cols_replace for x in cols_current_raw]
        cols_drop = header_main.columns[cols_drop_index]
        header_main = header_main.drop(columns=cols_drop)
    
        df_header = pd.concat([header_main, header_new], axis=1, sort=False)

        return df_header

    def _summary_data_merger(self, df_main, df_new, df_header_main, df_header_new, col_key):
        print('MERGE NEW WITH MAIN----------------')
        print(df_header_new.head())
        
        key_new = df_header_new[col_key][self._row_label]
        print(key_new)
        key_current = df_header_main[col_key][self._row_label]
        
        # Drop columns from old df and replace if exists
        cols_drop = list(set.intersection(set(df_main.columns), set(list(df_new.columns))))
        if key_new in cols_drop:
            cols_drop.remove(key_new)
        if key_current in cols_drop:
            cols_drop.remove(key_current)
            
        df_new = df_new.astype(object)
        df_main = df_main.astype(object)
        print('NEW-------------------------------')    
        print(df_new.head())
        print('MAIN-------------------------------')    
        print(df_main.head())
        print(df_main.shape)

        df_main = df_main.drop(columns=cols_drop)
        df_main_f = df_main.merge(right=df_new, how='left', left_on=key_current, right_on=key_new) 
        print('MERGED-------------------------------')    
        print(df_main_f.head())
        

        # Drop key column if different
        if key_new != key_current:
            df_main_f = df_main_f.drop(columns=[key_new])

        return df_main_f

    def _summary_header_data_combiner(self):
        df_header, df_data = self.return_output()
        col_name_data = list(df_header.loc[self._row_label, :])
        col_name_new = list(df_header.loc[self._row_label, :].index)
        dict_col_name = dict(zip(col_name_data, col_name_new))

        df_summary_final = pd.concat([df_header, df_data.rename(columns=dict_col_name)], axis=0)
        
        self._df_summary_final = df_summary_final
        
    def reset_origin(self):
        # This function will set the output header and frame to be the "original" dataset.
        # Additional annotations and output will be reset to None
        # add_annotation_loader and merge_annotations can be used again
        if self._df_summary_output is not None:
            self._df_summary_orig = self._df_summary_output
            self._df_header_orig = self._df_header_output
            self._df_summary_addition = None
            self._df_summary_output = None
            self._df_header_addition = None
            self._df_header_output = None
            self._df_summary_final = None

    def save_data(self, fname_save):
        # Check if summary file is available
        if self._df_summary_final is not None:
            print('Saving: %s' % fname_save)
            self._df_summary_final.to_csv(fname_save, index=False, sep='\t')
        else:
            print('No appended summary data to save.')

    def backfill_missing_data(self, fname_meta_data):
        # Create function that backfills missing summary data
        df_cbio_summary = self.return_final()

        df_metadata = pd.read_csv(fname_meta_data, sep=',', dtype=object)

        df_metadata['fill_value'] = df_metadata['fill_value'].fillna('NA')
        df_fill_value = df_metadata[['field_label', 'fill_value']].copy()

        df_cbio_summary_copy = df_cbio_summary.copy()

        cols_summary = list(df_cbio_summary_copy.columns)
        for i,col in enumerate(cols_summary):
            logic_fill = df_fill_value['field_label'] == col
            if logic_fill.any():
                df_cbio_summary_copy[col] = df_cbio_summary_copy[col].str.strip()
                fill_value = df_fill_value.loc[logic_fill, 'fill_value'].iloc[0]
                df_cbio_summary_copy[col] = df_cbio_summary_copy[col].fillna(fill_value)
                df_cbio_summary_copy = df_cbio_summary_copy.replace({col: {'NA': fill_value, 'N/A': fill_value}})

        self._df_summary_final = df_cbio_summary_copy

        return True




            

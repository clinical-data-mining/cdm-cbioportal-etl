""""
cbioportal_summary_merger.py

By Chris Fong - MSKCC 2021

This class will update clinical patient and sample summary files on cbioportal.
Object requires the path to the original sample/patient file, and for new/replacement annotations, a header and dataframe file (csv format)
"""
import pandas as pd
import sys
import os
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'minio_api/')))
from minio_api import MinioAPI
from utils import convert_to_int, set_debug_console

set_debug_console()



class cBioPortalSummaryMergeTool(object):
    def __init__(self, fname_minio_env, fname_current_summary):
        # Input filenames
        self._fname_minio_env = fname_minio_env
        self._fname_current_summary = fname_current_summary
        
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
            df_header_merged.loc[3, :] = df_header_merged.loc[3, :].str.upper()
            df_data_merged.columns = df_data_merged.columns.str.upper() 
            
            # Save as member variable
            self._df_summary_output = df_data_merged
            self._df_header_output = df_header_merged
            print('HERE 2')
            
            
            # print(df_header_merged.shape)
            # print(df_data_merged.shape)
            # print(list(df_header_merged.iloc[3, :]))
            # print(list(df_data_merged.columns))
            # print(list(df_header_merged.columns))
            # print(list(df_data_merged.columns))
            # print(list(df_header_merged.iloc[3, :]) == list(df_data_merged.columns))
            
            # Merge header and data file
            self._summary_header_data_combiner()
            
        
    def _summary_loader(self, fname):
        print('Loading %s' % fname)
        # obj = self._obj_minio.load_obj(path_object=fname)
        df_data = pd.read_csv(fname, header=4, sep='\t')
        df_header = pd.read_csv(fname, header=0, sep='\t', nrows=4)

        return df_header, df_data

    def add_annotation_loader(self, fname_header, fname_data):
        print('Loading %s' % fname_data)
        obj = self._obj_minio.load_obj(path_object=fname_data)
        df_data = pd.read_csv(obj, header=0, sep=',')
        print('Loading %s' % fname_header)        
        obj = self._obj_minio.load_obj(path_object=fname_header)
        df_header = pd.read_csv(obj, header=0, sep=',')
        
        df_header = convert_to_int(df=df_header, list_cols=['visible'])

        # Transpose header df
        cols = ['label', 'comment', 'data_type', 'visible', 'heading']
        df_header = df_header[cols]
        df_header_f = df_header.T.reset_index(drop=True)

        # Convert first row to column names
        df_header_f = df_header_f.rename(columns=df_header_f.iloc[0]).drop(df_header_f.index[0]).reset_index(drop=True)
#         print(len(df_header_f.iloc[3, :]))
#         print(list(df_header_f.iloc[3, :]))
        
#         print(len(df_data.columns))
#         print(df_data.columns)
        
        self._df_summary_addition = df_data
        self._df_header_addition = df_header_f

    def _summary_header_merger(self, header_main, header_new, list_keys_new):
        header_new = header_new.drop(columns=list_keys_new)
        
        # Check if current summary has same columns and drop common from current summary (This will replace columns)
        cols_current_raw = list(header_main.loc[3])
        cols_replace = list(set.intersection(set(cols_current_raw), set(list(header_new.loc[3]))))

        cols_drop_index = [x in cols_replace for x in cols_current_raw]
        cols_drop = header_main.columns[cols_drop_index]
        header_main = header_main.drop(columns=cols_drop)
    
        df_header = pd.concat([header_main, header_new], axis=1, sort=False)

        return df_header

    def _summary_data_merger(self, df_main, df_new, df_header_main, df_header_new, col_key):
        key_new = df_header_new[col_key][3]
        key_current = df_header_main[col_key][3]
        print('HERE 4')
        print('key_new: %s' % key_new)
        print('key_current: %s' % key_current)
        
        # Drop columns from old df and replace if exists
        cols_drop = list(set.intersection(set(df_main.columns), set(list(df_new.columns))))
        print('cols_drop:')
        print(cols_drop)
        if key_new in cols_drop:
            print('Dropped key_new: %s' % key_new)
            cols_drop.remove(key_new)
        if key_current in cols_drop:
            print('Dropped key_current: %s' % key_current)
            cols_drop.remove(key_current)
            
        if len(cols_drop) > 0:
            df_main = df_main.drop(columns=cols_drop)
            
        print(df_main.columns)
        print(df_new.columns)
        
        df_main_f = df_main.merge(right=df_new, how='left', left_on=key_current, right_on=key_new) 

        # Drop key column if different
        if key_new != key_current:
            df_main_f = df_main_f.drop(columns=[key_new])

        return df_main_f

    def _summary_header_data_combiner(self):
        df_header, df_data = self.return_output()
        print('HERE 3')
        print(list(df_header.iloc[3, :]) == list(df_data.columns))
        
        col_name_data = list(df_data.columns)
        col_name_new = list(df_header.iloc[3, :])
        dict_col_name = dict(zip(col_name_data, col_name_new))
        # df_data_comb = df_data.rename(columns=dict_col_name)
        df_data_comb = df_data
        df_data_comb.columns = df_header.columns
        
        print(list(df_header.columns) == list(df_data_comb.columns))
        
        # print(list(df_header.columns))
        # print(list(df_data_comb.columns))
        print(len(list(col_name_data)))
        print(len(list(col_name_new)))
        print(set(list(df_header.columns))- set(list(df_data_comb.columns)))
        # print(dict_col_name)
        # print(df_header.shape)
        # print(df_data.shape)
        # print(df_data_comb.shape)
        # print(list(df_header.iloc[3, :]))
        # print(list(df_data_comb.columns))
        # print(list(df_header.iloc[3, :]) == list(df_data.columns))

        df_summary_final = pd.concat([df_header, df_data_comb], axis=0)
        
        self._df_summary_final = df_summary_final
        
    def _keep_columns(self, list_cols_keep):
        # This function will keep columns from the *_orig dataframes and recombine frame and header.
        df_header, df_summary = self.return_orig()
        
        cols_keep_logic = df_header.loc[3, :].isin(list_cols_keep)
        cols_keep_header = list(df_header.columns[cols_keep_logic])
        
        # Subset sample data
        df_subset = df_sample[cols_keep]
        df_header_subset = df_header[cols_keep_header]
        
        # Set as output
        self._df_header_output = df_header_subset
        self._df_summary_output = df_subset
        
        # Combine header and data
        obj_sample_merge._summary_header_data_combiner()
        
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
            
    def export_header_file(self):
        # TODO: Create csv of the header from from the original summary file
        
        return None

    def save_data(self, fname_save):
        # Check if summary file is available
        if self._df_summary_final is not None:
            print('Saving: %s' % fname_save)
            self._df_summary_final.to_csv(fname_save, index=False, sep='\t')
        else:
            print('No appended summary data to save.')
            

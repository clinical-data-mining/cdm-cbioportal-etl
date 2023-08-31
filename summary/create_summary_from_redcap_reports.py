""""
create_summary_from_redcap_reports.py

This function will input format cbioportal summary data

Initiation:
Load metadata file and format
Load important variables 
Define path for saving files

Creating summary files, for direct redcap to cbio formatting (Inputs: report API key, patient/sample summary):
Load report API key
Load spec. sheet for converting report to cbioportal format
Output will be one or more formatted cbioportal summary files and a manifest of the files generated

[???] Creating summary files, for CUSTOM redcap to cbio formatting (Inputs: report API key, patient/sample summary, function for processing data):
Load report API key
Load spec. sheet for converting report to cbioportal format
Load function to do the custom transformations and aggregations

"""
import os
import sys

import pandas as pd
import numpy as np

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api/')))
from minio_api import MinioAPI
from utils import convert_to_int, mrn_zero_pad
from data_classes_cdm import CDMProcessingVariables as config_cdm
from data_loader import init_metadata
from get_anchor_dates import get_anchor_dates
from constants import (
    FNAME_MANIFEST_PATIENT,
    FNAME_MANIFEST_SAMPLE,
    FNAME_SUMMARY_TEMPLATE_P,
    FNAME_SUMMARY_TEMPLATE_S,
    FNAME_SUMMARY_P,
    FNAME_SUMMARY_S,
    FNAME_SUMMARY_P_MINIO,
    FNAME_SUMMARY_S_MINIO,
    PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE,
    ENV_MINIO,
    COL_PID,
    COL_PID_CBIO,
    COL_SUMMARY_FNAME_SAVE,
    COL_SUMMARY_HEADER_FNAME_SAVE,
    COL_RPT_NAME
) 


class RedcapToCbioportalFormat(object):
    def __init__(self):
        
        # Dataframes
        # self._df_rc_summary = None
        self._df_manifest = None
        self._df_header = None
        self._df_anchor = None
        self._df_metadata = None
        self._df_tables = None 
        self._df_project = None
        
        self._init()

    def _init(self):
         # Process data
        self._obj_minio = MinioAPI(
            fname_minio_env=ENV_MINIO
        )
        
        # Load anchor data containing data to deidentify tables
        self._df_anchor = get_anchor_dates()
        
        df_metadata, df_tables, df_project = init_metadata()
        self._df_metadata = df_metadata
        self._df_tables = df_tables 
        self._df_project = df_project
        
        return None
    
    def return_codebook(self):
        return self._df_metadata, self._df_tables, self._df_project
        
    # def return_full_header(self):
    #     return self._df_header
    
    def return_manifest(self):  # ----------------------------------------------------
        return self._df_manifest
    
    # def return_redcap_report_summary(self):
    #     return self._df_rc_summary
    
#     def return_report_map(self):
#         fname_map = self._fname_report_map
#         print('Loading %s' % fname_map)
#         obj = self._obj_minio.load_obj(path_object=fname_map)
#         df_redcap_map = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        
#         return df_redcap_map
        
    def _format_data_dictionary(self, df): # -----------------------------------------------------------
        # Reformat the data_types to cbioportal format

        ## Constants
        list_order_header = ['label', 'comment', 'data_type', 'visible', 'heading', 'is_date']
        dict_redcap_header = {
            'field_label': 'label', 
            'field_note': 'comment', 
            'data_type': 'data_type',
            'field_name': 'heading',
            'is_date': 'is_date'
        }
        list_redcap_map = list(dict_redcap_header.keys())

        # For checkboxes, create new rows with actual names
        df_tmp = df.copy()
        df_checkbox = df_tmp[df_tmp['field_type'] == 'checkbox']
        for i in df_checkbox.index:
            row_checkbox = df_checkbox.loc[i].copy()
            checkbox_cat = row_checkbox['select_choices_or_calculations']
            counts = checkbox_cat.count('|') + 1
            field_name = df_checkbox.loc[i, 'field_name']
            field_label = df_checkbox.loc[i, 'field_label']
            for i in range(counts):
                field_name_new = field_name + '___' + str(i+1)
                field_label_new = field_label + ' (' + str(i+1) + ')'
                row_checkbox_new = row_checkbox
                row_checkbox_new['field_name'] = field_name_new
                row_checkbox_new['field_label'] = field_label_new

                df_tmp = df_tmp.append(row_checkbox_new, ignore_index=True)

            # Remove old row
            df_tmp = df_tmp[df_tmp['field_name'] != field_name]

        df = df_tmp.copy()

        # Transform into a cbioportal format
        df = df.assign(data_type='STRING')
        # print(df.columns)
        logic_num = df['text_validation_type_or_sh'].isin(['number', 'date_mdy'])
        logic_is_date = df['text_validation_type_or_sh'].isin(['date_mdy'])
        df.loc[logic_num, 'data_type'] = 'NUMBER'
        df = df.assign(is_date=logic_is_date)

        # Create header dataframe
        df_header = df[list_redcap_map].rename(columns=dict_redcap_header)
        df_header = df_header.assign(visible=1)
        df_header = df_header[list_order_header]

        # Fill comment section with header if section is NA
        df_header['comment'] = df_header['comment'].fillna(df_header['label'])

        return df_header

#     def _summary_deid_dates(self, data_dictionary, report, col_dob):
#         header = data_dictionary[list(data_dictionary['heading'].isin(report.columns))]
#         cols_dates = list(header.loc[header['is_date'] == True, 'heading'])

#         if len(cols_dates) > 0:
#             cols_dates_fix = cols_dates.copy()
#             cols_dates_fix.remove(col_dob)
#             report[cols_dates] = report[cols_dates].apply(lambda x: pd.to_datetime(x))
#             report[cols_dates_fix] = report[cols_dates_fix].apply(lambda x: (x-report[col_dob]).dt.days)
#             report = convert_to_int(df=report, list_cols=cols_dates_fix)
#             header = header.drop(columns=['is_date'])

#             # Drop DOB from header and summary file
#             report = report.drop(columns=[col_dob])
#             header = header[header['heading'] != col_dob].reset_index(drop=True)

#         return header, report
    
    def create_summaries_and_headers(    # -----------------------------------------------------
        self, 
        patient_or_sample
    ):
        """
        This function creates cbioportal header and data files through the following steps:
        1) Computes the CDM data files that include data that we will expose, based on codebook
        2) Loads the metadata file, filters by the elements that will be exposed
        3) Loads CDM data file that includes the elements to be exposed
        4) Computes the columns that are dates, as indicated by code book
        5) Computes the patient or sample ID and properly deidentifies IDs and dates
        6) From the codebook, creates a header file that suppliments 
        7) Create a manifest file that contains the file locations of header files 
        
        """
        # Initialize the manifest file
        self.summary_manifest_init()
        
        # Load anchor data containing data to deidentify tables
        df_anchor = self._df_anchor
        
        # Load the CDM codebook
        df_metadata, df_tables, df_project = self.return_codebook()
        
        # Get form names that contain data elements to host on cbioportal
        logic_for_cbio = df_metadata['for_cbioportal'] == 'x'
        forms = df_metadata.loc[logic_for_cbio, 'form_name'].unique()

        # From form names, get corresponding tables and their filenames
        f1 = df_tables['form_name'].isin(forms)
        
        if patient_or_sample == 'sample':
            id_label = '#Sample Identifier'
            fname_manifest = FNAME_MANIFEST_SAMPLE
            fname_template = FNAME_SUMMARY_TEMPLATE_S
            f2 = df_tables['cbio_summary_id_sample'].notnull()
            col_id_change = 'SAMPLE_ID'
        elif patient_or_sample == 'patient':
            id_label = '#Patient Identifier'
            fname_manifest = FNAME_MANIFEST_PATIENT
            fname_template = FNAME_SUMMARY_TEMPLATE_P
            f2 = df_tables['cbio_summary_id_sample'].isnull()
            col_id_change = 'PATIENT_ID'
            
        active_tables = df_tables.loc[f1&f2]
        list_fname_minio = active_tables['cdm_source_table']

        # Cycle through the list of CDM dataset to be loaded
        for i,fname in enumerate(list_fname_minio):
            obj = self._obj_minio.load_obj(path_object=fname)
            df_ = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
            form = df_tables.loc[list_fname_minio.index[i], 'form_name']
            # print('-----------------------')
            print(fname)
            # print(form)
            filt_table_use = logic_for_cbio & (df_metadata['form_name'] == form)
            filt_col_dates = df_metadata.loc[filt_table_use]['text_validation_type_or_sh'] == 'date_mdy'
            cols_to_use = list(df_metadata.loc[filt_table_use, 'field_name'])
            cols_dates = list(df_metadata.loc[filt_table_use&filt_col_dates, 'field_name'])
            key_patient = active_tables['cbio_summary_id_patient'].iloc[i]
            key_sample = active_tables['cbio_summary_id_sample'].iloc[i]

            if pd.isna(key_sample):

                key = [key_patient]
            else:
                key = [key_sample, key_patient]

            cols_filter = key + list(cols_to_use)    

            # print(key)
            # print(cols_filter)
            # print(cols_dates)

            # Merge with anchor dates and de-id
            df_select = df_[cols_filter]

            if (key_patient == 'MRN'):
                df_select = mrn_zero_pad(df=df_select, col_mrn='MRN')
                df_select = df_select.merge(right=df_anchor, how='inner', on='MRN')
                df_select = df_select.drop(columns=['MRN'])
            else:
                df_anchor1 = df_anchor.drop(columns=['MRN'])
                df_select = df_select.merge(
                    right=df_anchor1, 
                    how='inner', 
                    left_on=key_patient, 
                    right_on='DMP_ID'
                ) 
            # Convert dates to intervals
            df_select[cols_dates] = df_select[cols_dates].apply(lambda x: pd.to_datetime(x, errors='coerce'))
            df_select[cols_dates] = df_select[cols_dates].apply(lambda x: (x - df_select['DTE_PATH_PROCEDURE']).dt.days)
            df_select = df_select.drop(columns=['DTE_PATH_PROCEDURE'])
            df_select = df_select.rename(columns={'DMP_ID': COL_PID_CBIO})
            df_select.columns = [x.upper().replace(' ', '_') for x in df_select.columns]

            # Create header
            cols_header = list(df_select.columns)
            df_header = self._format_data_dictionary(
                df=df_metadata[filt_table_use]
            )
            length_header = df_header.shape[0]
            df_header.loc[length_header, 'label'] = id_label
            df_header.loc[length_header, 'data_type'] = np.NaN
            df_header.loc[length_header, 'comment'] = col_id_change
            df_header.loc[length_header, 'heading'] = col_id_change
            # Reorder header entries to match summary file
            sorterIndex = dict(zip(cols_header, range(len(cols_header))))
            df_header['Rank'] = df_header['heading'].map(sorterIndex)
            df_header.sort_values(by='Rank', ascending = True, inplace = True)
            df_header.drop(columns=['Rank', 'is_date'], axis=1, inplace = True)
            df_header['heading'] = df_header['heading'].str.upper().str.replace(' ', '_')

            # Reorder data columns to the header
            df_select = df_select[list(df_header['heading'])]

            # Create manifest file
            ## Modify/create manifest files --------------------------------------------------
            fname_save_data = PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE + form.lower().replace(' ','_') + '_data.csv'
            fname_save_header = PATH_MINIO_CBIO_SUMMARY_INTERMEDIATE + form.lower().replace(' ','_') + '_header.csv'
            ##### This file will be used for merging data    
            self.summary_manifest_append(
                instr_name=form,
                fname_df_save=fname_save_data,
                fname_header_save=fname_save_header
            )

            ## Save data and header files --------------------------------------------------
            ### Save cbioportal formatted patient level dx data and header files 
            print('Saving %s' % fname_save_data)
            print(df_select.head())
            self._obj_minio.save_obj(df=df_select, 
                               path_object=fname_save_data, 
                               sep=',')
            print('Saving %s' % fname_save_header)
            print(df_header.head())
            self._obj_minio.save_obj(df=df_header, 
                               path_object=fname_save_header, 
                               sep=',')


        ## Note: Continue appending manifest files here
        df_manifest_s = self.return_manifest()
        print(df_manifest_s)
        
        ##### Save manifest
        self.summary_manifest_save(fname_save=fname_manifest)    
        
        return None

    
#     def create_timeline_files(self):
#         df_header = self._df_header
#         df_redcap = self._df_rc_summary
#         df_redcap_map = self._df_header
#         pathname_cbio_summaries = self._path_cbio_save
#         pathname_cbio_config = self._path_config
#         col_darwin_id = self._col_darwin_id
#         col_dte_birth = self._col_dte_birth
        
#         # Load metadata files
#         for report_name in df_redcap['TIMELINE_LEVEL']:
#             if report_name != 'nan':
#                 # print(report_name)
#                 fname_timeline_save = 'data_timeline_' + report_name + '.txt'
#                 pathfilename_save = os.path.join(pathname_cbio_summaries, fname_timeline_save)

#                 pathfilename = os.path.join(pathname_cbio_config, report_name + '.csv')
#                 df_specs = pd.read_csv(pathfilename)

#                 current_timeline_type = df_redcap.loc[df_redcap['TIMELINE_LEVEL'] == report_name, 'TIMELINE_TYPE'].iloc[0]

#                 # Load Redcap report
#                 fname_report = df_redcap.loc[df_redcap[COL_RPT_NAME] == report_name, 'REPORT_FILENAME'].iloc[0]
#                 df_report = pd.read_csv(fname_report, sep='\t')

#                 # Steps

#                 ## Establish which timeline type is being used and columns required
#                 if current_timeline_type == 'treatment':
#                     cols_required = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'TREATMENT_TYPE', 'AGENT']
#                     EVENT_TYPE = 'TREATMENT'

#                 elif current_timeline_type == 'toxicity':
#                     cols_required = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'TOXICITY_TYPE', 'SUBTYPE']
#                     EVENT_TYPE = 'TOXICITY'

#                 ## For repeating instruments, clean data so all IDs and birthdates are in all rows
#                 if df_report['redcap_repeat_instance'].notnull().any():
#                     df_report[col_darwin_id] = df_report[col_darwin_id].ffill()
#                     df_report[col_dte_birth] = df_report[col_dte_birth].ffill()

#                 ## De-identify dates
#                 header, df_report_deid = self._summary_deid_dates(data_dictionary=df_redcap_map, report=df_report, col_dob=col_dte_birth)

#                 ## Filter columns by values in COLUMN_NAME_REDCAP
#                 cols_keep = list(df_specs['COLUMN_NAME_REDCAP'])
#                 df_report_current = df_report_deid[cols_keep]

#                 # Key for column renaming
#                 dict_col_rename = dict(zip(df_specs['COLUMN_NAME_REDCAP'], df_specs['COLUMN_NAME_CBIOPORTAL']))


#                 ## Check for data to melt. If yes, get varables, else, do straight conversion
#                 logic_melt_check = df_specs['MELT_ID_VARS'] == 'x'
#                 var_name = 'REDCAP_TIMELINE_NAME'
#                 if (logic_melt_check).any():
#                     id_vars = list(df_specs.loc[logic_melt_check, 'COLUMN_NAME_REDCAP'])
#                     # Get value variables
#                     logic_melt_vals = df_specs['MELT_VAL_VARS'] == 'x'
#                     val_vars = list(df_specs.loc[logic_melt_vals, 'COLUMN_NAME_REDCAP'])
#                     value_name = list(df_specs.loc[logic_melt_vals, 'COLUMN_NAME_CBIOPORTAL'])[0]

#                     ## Melt data on variables
#                     df_report_current_melt = pd.melt(frame=df_report_current, id_vars=id_vars, value_vars=val_vars, var_name=var_name, value_name=value_name, col_level=None, ignore_index=True)


#                 else:
#                     df_report_current_melt = df_report_current        

#                 ## Rename column names
#                 df_report_current_melt = df_report_current_melt.rename(columns=dict_col_rename)        
#                 df_report_current_melt = df_report_current_melt[df_report_current_melt['START_DATE'].notnull()]
#         #         print(df_report.head())

#                 ## Add missing columns, add EVENT TYPE
#                 cols_requried_tmp = cols_required.copy()
#                 cols_requried_tmp.remove('EVENT_TYPE')
#                 cols_req_missing = [x for x in cols_requried_tmp if x not in df_report_current_melt.columns]
#                 for col_missing in cols_req_missing:
#                     kwargs = {col_missing : lambda x: np.NaN}
#                     df_report_current_melt = df_report_current_melt.assign(**kwargs)
#                 df_report_current_melt = df_report_current_melt.assign(EVENT_TYPE=EVENT_TYPE)

#                 ## Reformat/reorder columns to fit cbioportal format
#                 cols_other = [x for x in df_report_current_melt.columns if x not in cols_required]
#                 cols_order = cols_required + cols_other
#                 df_report_current_melt = df_report_current_melt[cols_order]

#                 ## Fill in end dates with start dates if blank
#                 df_report_current_melt['STOP_DATE'] = df_report_current_melt['STOP_DATE'].fillna(df_report_current_melt['START_DATE'])

#                 ## Fix specific timeline formats if required data is blank
#                 if current_timeline_type == 'treatment':
#                     df_report_current_melt['TREATMENT_TYPE'] = df_report_current_melt['TREATMENT_TYPE'].fillna('NS')
#                     df_report_current_melt['AGENT'] = df_report_current_melt['AGENT'].fillna('NS')

#                 elif current_timeline_type == 'toxicity':
#                     do = None

#                 # Save timeline file
#                 df_report_current_melt.to_csv(pathfilename_save, index=False, sep='\t')
    
    def summary_manifest_init(self):   # ---------------------------------------------------------
        cols_manifest = [
            COL_RPT_NAME, 
            COL_SUMMARY_FNAME_SAVE, 
            COL_SUMMARY_HEADER_FNAME_SAVE
        ]
        df_manifest = pd.DataFrame(columns=cols_manifest)
        
        self._df_manifest = df_manifest

        return None
    
    def summary_manifest_append(  # -------------------------------------------------------
        self, 
        instr_name, 
        fname_df_save, 
        fname_header_save
    ):
        df_manifest = self._df_manifest
        dict_append = {COL_RPT_NAME:instr_name, 
                       COL_SUMMARY_FNAME_SAVE:fname_df_save, 
                       COL_SUMMARY_HEADER_FNAME_SAVE:fname_header_save}
        df_manifest = df_manifest.append(dict_append, ignore_index=True)
        
        self._df_manifest = df_manifest
        
        return None
        
    def summary_manifest_save(self, fname_save):    # ------------------------------------------
        print('Saving %s' % fname_save)
        self._obj_minio.save_obj(
            df=self._df_manifest, 
            path_object=fname_save, 
            sep=','
        )
        
        return None
    
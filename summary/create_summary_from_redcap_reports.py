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

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'minio_api/')))
from minio_api import MinioAPI
from utils import convert_to_int
from data_classes_cdm import CDMProcessingVariables as config_cdm

FNAME_MINIO_ENV=config_cdm.minio_env



class RedcapToCbioportalFormat(object):
    def __init__(
        self, 
        fname_minio_env=None, 
        fname_report_api=None, 
        path_config=None, 
        fname_report_map=None, 
        fname_variables=None, 
        path_save=None
    ):
        # Input filenames
        ## Report API metadata. This file provides the API code for each report
        self._fname_minio_env = FNAME_MINIO_ENV
        self._fname_report_api = fname_report_api
        ## Pathname of config files
        self._path_config = path_config
        ## Report map between Redcap report name and report filename saved from Redcap API
        self._fname_report_map = fname_report_map
        ## Redcap variable names of interest, including dob
        self._fname_variables = fname_variables
        ## Pathname where summary files will be saved
        self._path_cbio_save = path_save
        
        # file headers expected
        self._col_summary_fname_save = 'SUMMARY_FILENAME'
        self._col_summary_header_fname_save = 'SUMMARY_HEADER_FILENAME'
        self._col_rpt_name = 'REPORT_NAME'
        self._col_fname_save = 'REPORT_FILENAME'
        
        # Variable names
        self._col_dte_birth = None
        self._col_darwin_id = None
        self._col_sample_id = None
        self._col_record_id = None
        
        # Dataframes
        self._df_rc_summary = None
        self._df_manifest = None
        self._df_header = None
        
        # Process data
        self._obj_minio = MinioAPI(fname_minio_env=self._fname_minio_env)
        # self._init_metadata()
        
    def return_col_id(self):
        return self._col_darwin_id
        
    def return_full_header(self):
        return self._df_header
    
    def return_manifest(self):
        return self._df_manifest
    
    def return_redcap_report_summary(self):
        return self._df_rc_summary
    
    def return_report_map(self):
        fname_map = self._fname_report_map
        print('Loading %s' % fname_map)
        obj = self._obj_minio.load_obj(path_object=fname_map)
        df_redcap_map = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        
        return df_redcap_map
        
    def _init_metadata(self):
        # Load files
        ## Load API mapping file
        df_redcap = self._load_api_map(fname_map=self._fname_report_map, 
                                       fname_api=self._fname_report_api)
        
        self._df_rc_summary = df_redcap
        
        ## Load variables
        self._load_variables(fname=self._fname_variables)
        
        ## Open metadata file
        logic_metadata = df_redcap[self._col_rpt_name] == 'Metadata'
        fname_meta = df_redcap.loc[logic_metadata, self._col_fname_save].iloc[0]
        print('Loading %s' % fname_meta)
        obj = self._obj_minio.load_obj(path_object=fname_meta)
        df_redcap_codebook = pd.read_csv(obj, header=0, sep='\t')
        
        # Format data
        ## Format Redcap metadata into a cBioPortal header format
        df_header = self._format_data_dictionary(df=df_redcap_codebook)
        self._df_header = df_header
        
        return None
    
    def _load_api_map(self, fname_map, fname_api):
        df_redcap_map = self.return_report_map()
                
        print('Loading %s' % fname_api)
        obj = self._obj_minio.load_obj(path_object=fname_api)
        df_redcap = pd.read_csv(obj, header=0, low_memory=False, sep=',')        
        df_redcap = df_redcap.merge(right=df_redcap_map, how='right', on=self._col_rpt_name)
        col_ind = ['TIMELINE_LEVEL', 'PATIENT_LEVEL', 'SAMPLE_LEVEL']
        df_redcap[col_ind] = df_redcap[col_ind].apply(lambda x: x.astype(str))
        
        
        return df_redcap
    
    def create_summary_default(self, patient_or_sample):
        df_rc_summary = self._df_rc_summary
        logic_cbio = (df_rc_summary['FOR_CBIOPORTAL'] == 1)
        if patient_or_sample == 'patient':
            logic_p_or_s = (df_rc_summary['PATIENT_LEVEL'] == 'ALL') & logic_cbio
        elif patient_or_sample == 'sample':
            logic_p_or_s = (df_rc_summary['SAMPLE_LEVEL'] == 'ALL') & logic_cbio
    
        
        ## TODO: Split this into a another callable function
        ## Create cbioportal summary folder for project
        
        ### Format patient and sample level summary data
        df_summary_manifest = df_rc_summary[logic_p_or_s].copy()
        df_summary_manifest_patient = self._format_summary_and_header(df_redcap_map=df_summary_manifest, 
                                                                      df_header=self._df_header, 
                                                                      patient_or_sample=patient_or_sample, 
                                                                      col_id=self._col_darwin_id)
        self._df_manifest = df_summary_manifest_patient
        
    def _load_variables(self, fname):
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_vars = pd.read_csv(obj, header=0, sep=',')
        
        # Set values to member variables
        
        logic_dob = df_vars['variable'] == 'col_dte_birth'
        logic_deid = df_vars['variable'] == 'col_darwin_id'
        logic_sid = df_vars['variable'] == 'col_sample_id'
        logic_record_id = df_vars['variable'] == 'col_record_id'
        
        self._col_dte_birth = df_vars.loc[logic_dob, 'redcap_field'].iloc[0]
        self._col_darwin_id = df_vars.loc[logic_deid, 'redcap_field'].iloc[0]
        self._col_sample_id = df_vars.loc[logic_sid, 'redcap_field'].iloc[0]
        self._col_record_id = df_vars.loc[logic_record_id, 'redcap_field'].iloc[0]
        
        return None
        
    def _format_data_dictionary(self, df):
        # Reformat the data_types to cbioportal format

        ## Constants
        list_order_header = ['label', 'comment', 'data_type', 'visible', 'heading', 'is_date']
        dict_redcap_header = {'field_label': 'label', 
                              'field_note': 'comment', 
                              'data_type': 'data_type',
                              'field_name': 'heading',
                              'is_date': 'is_date'}
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

    def _summary_deid_dates(self, data_dictionary, report, col_dob):
        header = data_dictionary[list(data_dictionary['heading'].isin(report.columns))]
        cols_dates = list(header.loc[header['is_date'] == True, 'heading'])

        if len(cols_dates) > 0:
            cols_dates_fix = cols_dates.copy()
            cols_dates_fix.remove(col_dob)
            report[cols_dates] = report[cols_dates].apply(lambda x: pd.to_datetime(x))
            report[cols_dates_fix] = report[cols_dates_fix].apply(lambda x: (x-report[col_dob]).dt.days)
            report = convert_to_int(df=report, list_cols=cols_dates_fix)
            header = header.drop(columns=['is_date'])

            # Drop DOB from header and summary file
            report = report.drop(columns=[col_dob])
            header = header[header['heading'] != col_dob].reset_index(drop=True)

        return header, report
    
    def _format_summary_and_header(self, df_redcap_map, df_header, patient_or_sample, col_id):
        if patient_or_sample == 'sample':
            id_label = '#Sample Identifier'
            fname_manifest = 'summary_manifest_sample.csv'
            col_id_change = 'SAMPLE_ID'
        elif patient_or_sample == 'patient':
            id_label = '#Patient Identifier'
            fname_manifest = 'summary_manifest_patient.csv'
            col_id_change = 'PATIENT_ID'
        else:
            print('Error')
            return

        df_fname_summary_manifest = df_redcap_map[[self._col_rpt_name]].copy()
        kwargs = {self._col_summary_fname_save : np.NaN, self._col_summary_header_fname_save : np.NaN}
        df_fname_summary_manifest = df_fname_summary_manifest.assign(**kwargs)


        for i in df_redcap_map.index:
            REPORT_NAME = df_redcap_map.loc[i, self._col_rpt_name]
            print('Formatting %s' % REPORT_NAME)
            REPORT_FILENAME = df_redcap_map.loc[i, self._col_fname_save]
            current_redcap_report = pd.read_csv(REPORT_FILENAME, header=0, sep='\t')
    #         print(current_redcap_report.head())
            current_redcap_report = current_redcap_report.drop(columns=[self._col_record_id])
            current_redcap_report = current_redcap_report[current_redcap_report[self._col_darwin_id].notnull()]
            if (patient_or_sample == 'patient') & (current_redcap_report[self._col_darwin_id].dtype == np.float64):
                current_redcap_report = convert_to_int(df=current_redcap_report, list_cols=[self._col_darwin_id])

            # Make ID columns first
            col = current_redcap_report.pop(self._col_darwin_id)
            current_redcap_report.insert(0, col.name, col)

            # Create header based on patient/sample level data
            current_header = df_header[df_header['heading'].isin(list(current_redcap_report.columns))].copy()
            current_header.loc[current_header['heading'] == self._col_darwin_id, 'label'] = id_label
            current_header.loc[current_header['heading'] == self._col_darwin_id, 'data_type'] = np.NaN
            # Reorder header entries to match summary file
            cols_order = list(current_redcap_report.columns)
            sorterIndex = dict(zip(cols_order, range(len(cols_order))))
            current_header['Rank'] = current_header['heading'].map(sorterIndex)
            current_header.sort_values(by='Rank', ascending = True, inplace = True)
            current_header.drop(columns=['Rank'], axis=1, inplace = True)

            # Change ID headers to be consistent with cbioportal format
            current_header.loc[current_header['heading'] == self._col_darwin_id, 'heading'] = col_id_change
            current_redcap_report = current_redcap_report.rename(columns={self._col_darwin_id: col_id_change})

            # (TEMP) Remove columns from summary not in header. The occurs with checkbox items that have multiple entries
            col_keep = current_header['heading']
            current_redcap_report = current_redcap_report[col_keep]

            # De-identify dates
    #         print(current_header.head())
            (current_header, current_redcap_report) = self._summary_deid_dates(data_dictionary=current_header, report=current_redcap_report, col_dob=self._col_dte_birth)

            # (TODO) Transform report into patient/sample level data

            # Save header and summary files
            pathfilename_summary = os.path.join(self._path_cbio_save, REPORT_NAME + '.csv')
            pathfilename_header = os.path.join(self._path_cbio_save, REPORT_NAME + '_header.csv')

            current_redcap_report.to_csv(pathfilename_summary, index=False)
            current_header.to_csv(pathfilename_header, index=False)

            # Input filename
            df_fname_summary_manifest.loc[df_fname_summary_manifest[self._col_rpt_name] == REPORT_NAME, self._col_summary_fname_save] = pathfilename_summary
            df_fname_summary_manifest.loc[df_fname_summary_manifest[self._col_rpt_name] == REPORT_NAME, self._col_summary_header_fname_save ] = pathfilename_header

        df_fname_summary_manifest = df_fname_summary_manifest.reset_index(drop=True)
        
        pathfilename_manifest_patient = os.path.join(self._path_cbio_save, fname_manifest)
        df_fname_summary_manifest.to_csv(pathfilename_manifest_patient, index=False)
        
        return df_fname_summary_manifest
    
    def create_timeline_files(self):
        df_header = self._df_header
        df_redcap = self._df_rc_summary
        df_redcap_map = self._df_header
        pathname_cbio_summaries = self._path_cbio_save
        pathname_cbio_config = self._path_config
        col_darwin_id = self._col_darwin_id
        col_dte_birth = self._col_dte_birth
        
        # Load metadata files
        for report_name in df_redcap['TIMELINE_LEVEL']:
            if report_name != 'nan':
                # print(report_name)
                fname_timeline_save = 'data_timeline_' + report_name + '.txt'
                pathfilename_save = os.path.join(pathname_cbio_summaries, fname_timeline_save)

                pathfilename = os.path.join(pathname_cbio_config, report_name + '.csv')
                df_specs = pd.read_csv(pathfilename)

                current_timeline_type = df_redcap.loc[df_redcap['TIMELINE_LEVEL'] == report_name, 'TIMELINE_TYPE'].iloc[0]

                # Load Redcap report
                fname_report = df_redcap.loc[df_redcap[self._col_rpt_name] == report_name, 'REPORT_FILENAME'].iloc[0]
                df_report = pd.read_csv(fname_report, sep='\t')

                # Steps

                ## Establish which timeline type is being used and columns required
                if current_timeline_type == 'treatment':
                    cols_required = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'TREATMENT_TYPE', 'AGENT']
                    EVENT_TYPE = 'TREATMENT'

                elif current_timeline_type == 'toxicity':
                    cols_required = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'TOXICITY_TYPE', 'SUBTYPE']
                    EVENT_TYPE = 'TOXICITY'

                ## For repeating instruments, clean data so all IDs and birthdates are in all rows
                if df_report['redcap_repeat_instance'].notnull().any():
                    df_report[col_darwin_id] = df_report[col_darwin_id].ffill()
                    df_report[col_dte_birth] = df_report[col_dte_birth].ffill()

                ## De-identify dates
                header, df_report_deid = self._summary_deid_dates(data_dictionary=df_redcap_map, report=df_report, col_dob=col_dte_birth)

                ## Filter columns by values in COLUMN_NAME_REDCAP
                cols_keep = list(df_specs['COLUMN_NAME_REDCAP'])
                df_report_current = df_report_deid[cols_keep]

                # Key for column renaming
                dict_col_rename = dict(zip(df_specs['COLUMN_NAME_REDCAP'], df_specs['COLUMN_NAME_CBIOPORTAL']))


                ## Check for data to melt. If yes, get varables, else, do straight conversion
                logic_melt_check = df_specs['MELT_ID_VARS'] == 'x'
                var_name = 'REDCAP_TIMELINE_NAME'
                if (logic_melt_check).any():
                    id_vars = list(df_specs.loc[logic_melt_check, 'COLUMN_NAME_REDCAP'])
                    # Get value variables
                    logic_melt_vals = df_specs['MELT_VAL_VARS'] == 'x'
                    val_vars = list(df_specs.loc[logic_melt_vals, 'COLUMN_NAME_REDCAP'])
                    value_name = list(df_specs.loc[logic_melt_vals, 'COLUMN_NAME_CBIOPORTAL'])[0]

                    ## Melt data on variables
                    df_report_current_melt = pd.melt(frame=df_report_current, id_vars=id_vars, value_vars=val_vars, var_name=var_name, value_name=value_name, col_level=None, ignore_index=True)


                else:
                    df_report_current_melt = df_report_current        

                ## Rename column names
                df_report_current_melt = df_report_current_melt.rename(columns=dict_col_rename)        
                df_report_current_melt = df_report_current_melt[df_report_current_melt['START_DATE'].notnull()]
        #         print(df_report.head())

                ## Add missing columns, add EVENT TYPE
                cols_requried_tmp = cols_required.copy()
                cols_requried_tmp.remove('EVENT_TYPE')
                cols_req_missing = [x for x in cols_requried_tmp if x not in df_report_current_melt.columns]
                for col_missing in cols_req_missing:
                    kwargs = {col_missing : lambda x: np.NaN}
                    df_report_current_melt = df_report_current_melt.assign(**kwargs)
                df_report_current_melt = df_report_current_melt.assign(EVENT_TYPE=EVENT_TYPE)

                ## Reformat/reorder columns to fit cbioportal format
                cols_other = [x for x in df_report_current_melt.columns if x not in cols_required]
                cols_order = cols_required + cols_other
                df_report_current_melt = df_report_current_melt[cols_order]

                ## Fill in end dates with start dates if blank
                df_report_current_melt['STOP_DATE'] = df_report_current_melt['STOP_DATE'].fillna(df_report_current_melt['START_DATE'])

                ## Fix specific timeline formats if required data is blank
                if current_timeline_type == 'treatment':
                    df_report_current_melt['TREATMENT_TYPE'] = df_report_current_melt['TREATMENT_TYPE'].fillna('NS')
                    df_report_current_melt['AGENT'] = df_report_current_melt['AGENT'].fillna('NS')

                elif current_timeline_type == 'toxicity':
                    do = None

                # Save timeline file
                df_report_current_melt.to_csv(pathfilename_save, index=False, sep='\t')
    
    def summary_manifest_init(self):
        cols_manifest = [self._col_rpt_name, 
                 self._col_summary_fname_save, 
                 self._col_summary_header_fname_save]
        df_manifest = pd.DataFrame(columns=cols_manifest)
        
        self._df_manifest = df_manifest

        return None
    
    def summary_manifest_append(self, instr_name, fname_df_save, fname_header_save):
        df_manifest = self._df_manifest
        dict_append = {self._col_rpt_name:instr_name, 
                       self._col_summary_fname_save:fname_df_save, 
                       self._col_summary_header_fname_save:fname_header_save}
        df_manifest = df_manifest.append(dict_append, ignore_index=True)
        
        self._df_manifest = df_manifest
        
        return None
        
    def summary_manifest_save(self, fname_save):
        print('Saving %s' % fname_save)
        self._obj_minio.save_obj(df=self._df_manifest, 
                                   path_object=fname_save, 
                                   sep=',')
        
        return None
    
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
import pandas as pd
import numpy as np

from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import mrn_zero_pad

from cdm_cbioportal_etl.utils import (
    get_anchor_dates
)
from cdm_cbioportal_etl.utils import constants

COL_SUMMARY_FNAME_SAVE = constants.COL_SUMMARY_FNAME_SAVE
COL_SUMMARY_HEADER_FNAME_SAVE = constants.COL_SUMMARY_HEADER_FNAME_SAVE
COL_RPT_NAME = constants.COL_RPT_NAME
COL_PID = constants.COL_PID
COL_PID_CBIO = constants.COL_PID_CBIO


class RedcapToCbioportalFormat(object):
    def __init__(
        self,
        fname_minio_env,
        path_minio_summary_intermediate,
        fname_metadata,
        fname_metaproject,
        fname_metatables
    ):
        # Filenames
        self._fname_metadata = fname_metadata
        self._fname_metaproject = fname_metaproject
        self._fname_metatables = fname_metatables
        
        # Dataframes
        # self._df_rc_summary = None
        self._df_manifest = None
        self._df_header = None
        self._df_anchor = None
        self._df_metadata = None
        self._df_tables = None 
        self._df_project = None

        self._fname_minio_env = fname_minio_env
        self._path_minio_summary_intermediate = path_minio_summary_intermediate
        
        self._init()

    def _init(self):
         # Process data
        self._obj_minio = MinioAPI(
            fname_minio_env=self._fname_minio_env
        )
        
        # Load anchor data containing data to deidentify tables
        self._df_anchor = get_anchor_dates()
        
        df_metadata, df_tables, df_project = self.init_metadata()
        self._df_metadata = df_metadata
        self._df_tables = df_tables 
        self._df_project = df_project
        
        return None
    
    def return_codebook(self):
        return self._df_metadata, self._df_tables, self._df_project
    
    def return_manifest(self):  # ----------------------------------------------------
        return self._df_manifest
        
    def _format_data_dictionary(
        self, 
        df,
        patient_or_sample,
        production_or_test
    ): # -----------------------------------------------------------
        # Reformat the data_types to cbioportal format
        ## Constants
        if production_or_test == 'test':
            list_order_header = [
                'label', 
                'comment', 
                'data_type', 
                'patient_or_sample',
                'visible', 
                'heading', 
                'is_date', 
                'fill_value'
            ]
            if patient_or_sample == 'patient':
                col_patient_or_sample = 'PATIENT'
            else:
                col_patient_or_sample = 'SAMPLE'
        else:
            list_order_header = [
                'label', 
                'comment', 
                'data_type', 
                'visible', 
                'heading', 
                'is_date', 
                'fill_value'
            ]
            
        dict_redcap_header = {
            'field_label': 'label', 
            'data_type': 'data_type',
            'comment': 'comment',
            'field_name': 'heading',
            'is_date': 'is_date',
            'fill_value': 'fill_value'
        }

        list_redcap_map = list(dict_redcap_header.keys())

        # For checkboxes, create new rows with actual names
        df_tmp = df.copy()
        df_checkbox = df_tmp[df_tmp['field_type'] == 'checkbox']
        print(df_checkbox.head())
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

        df_header1 = df_tmp.copy()
        # print('HEADER------------------------------------------')
        # print(df_header1.head())

        # Transform into a cbioportal format
        df_header1 = df_header1.rename(columns={'field_type': 'data_type'})
        # print(df.columns)
        logic_is_date = df_header1['text_validation_type_or_sh'].isin(['date_mdy'])
        df_header1 = df_header1.assign(is_date=logic_is_date)

        # Create header dataframe
        df_header = df_header1[list_redcap_map].rename(columns=dict_redcap_header)
        if production_or_test == 'test':
            df_header = df_header.assign(patient_or_sample=col_patient_or_sample)
        
        df_header = df_header.assign(visible='1')
        df_header = df_header[list_order_header]

        # Fill comment section with header if section is NA
        df_header['comment'] = df_header['comment'].fillna(df_header['label'])

        print('Sample of final header')
        print(df_header)

        return df_header

    def init_metadata(self):
        # Load CDM Codebook files
        df_metadata = pd.read_csv(self._fname_metadata)
        df_project = pd.read_csv(self._fname_metaproject)
        df_tables = pd.read_csv(self._fname_metatables)

        return df_metadata, df_tables, df_project
    
    def create_summaries_and_headers(
        self, 
        patient_or_sample,
        fname_manifest,
        fname_template,
        production_or_test='production'
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
        if production_or_test == 'production':
            col_metadata_list = 'for_cbioportal'
            header_len = 4
        elif production_or_test == 'test':
            col_metadata_list = 'for_test_portal'
            header_len = 5
        else:
            col_metadata_list = 'for_test_portal'
            header_len = 5
        
        # Initialize the manifest file
        self.summary_manifest_init()
        
        # Load anchor data containing data to deidentify tables
        df_anchor = self._df_anchor
        
        # Load the CDM codebook
        df_metadata, df_tables, df_project = self.return_codebook()
        
        ## Merge metadata info into "comment" column. Will include description, reason for missing, source
        df_metadata['comment'] = ' ---DESCRIPTION: ' + df_metadata['field_note'] + ' ---MISSING DATA: ' + df_metadata['reasons_for_missing_data'] + ' ---SOURCE:' + df_metadata['souce_from_idb_or_cdm']
        
        # Get form names that contain data elements to host on cbioportal
        logic_for_cbio = df_metadata[col_metadata_list] == 'x'
        forms = df_metadata.loc[logic_for_cbio, 'form_name'].unique()

        # From form names, get corresponding tables and their filenames
        f1 = df_tables['form_name'].isin(forms)
        
        if patient_or_sample == 'sample':
            id_label = '#Sample Identifier'
            f2 = df_tables['cbio_summary_id_sample'].notnull()
            col_id_change = 'SAMPLE_ID'
        elif patient_or_sample == 'patient':
            id_label = '#Patient Identifier'
            f2 = df_tables['cbio_summary_id_sample'].isnull()
            col_id_change = 'PATIENT_ID'
            
        active_tables = df_tables.loc[f1&f2]
        list_fname_minio = active_tables['cdm_source_table']
        
        print('Loading %s' % fname_template)
        obj = self._obj_minio.load_obj(path_object=fname_template)
        df_template = pd.read_csv(
            obj, 
            header=header_len, 
            low_memory=False,
            sep='\t',
            dtype=str
        )
        print('TEMPLATE------------------------------------------------')
        print(df_template)
        print(df_template.shape)
        print(list_fname_minio)

        print('CYCLE THROUGH CDM DATA SUMMARIES')
        # Cycle through the list of CDM dataset to be loaded
        for i,fname in enumerate(list_fname_minio):
            print('------------------------------------------------')
            print('Loading %s' % fname)
            obj = self._obj_minio.load_obj(path_object=fname)
            df_ = pd.read_csv(
                obj, 
                header=0, 
                low_memory=False, 
                sep='\t',
                dtype=str
            )
            print('Gathering metadata from documentation tables')
            form = df_tables.loc[list_fname_minio.index[i], 'form_name']
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

            # Merge with anchor dates and de-id
            print('Determining column key with patient ID')
            df_select = df_[cols_filter]

            if (key_patient == 'MRN'):
                df_select = mrn_zero_pad(df=df_select, col_mrn='MRN')
                df_select = df_anchor.merge(right=df_select, how='inner', on='MRN')
                df_select = df_select.drop(columns=['MRN'])
            else:
                df_anchor1 = df_anchor.drop(columns=['MRN'])
                df_select = df_anchor1.merge(
                    right=df_select, 
                    how='inner', 
                    right_on=key_patient, 
                    left_on='DMP_ID'
                )

            # Convert dates to intervals
            print('Converting dates to intervals')
            df_select[cols_dates] = df_select[cols_dates].apply(lambda x: pd.to_datetime(x, errors='coerce'))
            df_select[cols_dates] = df_select[cols_dates].apply(lambda x: (x - df_select['DTE_PATH_PROCEDURE']).dt.days)
            df_select = df_select.drop(columns=['DTE_PATH_PROCEDURE'])
            df_select = df_select.rename(columns={'DMP_ID': COL_PID_CBIO})
            df_select.columns = [x.upper().replace(' ', '_') for x in df_select.columns]

            # Create header
            print('Creating header')
            cols_header = list(df_select.columns)
            
            df_header = self._format_data_dictionary(
                df=df_metadata[filt_table_use],
                patient_or_sample=patient_or_sample,
                production_or_test=production_or_test
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
            df_header['heading'] = df_header['heading'].str.upper().str.replace(' ', '_')

            # Reorder data columns to the header
            cols_summary_order = list(df_header['heading'])
            df_select = df_select[cols_summary_order]
            
            print('df_header--------------------')
            print(df_header.head())
            print(df_header.shape)
            
            print('df_select--------------------')
            print(df_select.head())
            print(df_select.shape)
            
            # Merge with template cases and reformat according to heading
            print('Merging with template')
            df_select_f = df_template.merge(
                right=df_select, 
                how='left', 
                on=col_id_change
            )

            # Impute fill values
            print('Imputing fill values')
            for current_col in cols_summary_order:
                fill_value = df_header.loc[df_header['heading'] == current_col, 'fill_value'].iloc[0]
                print('fill_value--------------------')
                print(fill_value)
                # if fill_value == 'NA':
                #     fill_value = np.NaN
                data_type = df_header.loc[df_header['heading'] == current_col, 'data_type'].iloc[0]
                print('data_type--------------------')
                print(data_type)
                
                df_select_f[current_col] = df_select_f[current_col].fillna(fill_value)

            print('Sample of final result')
            print('df_select_f--------------------')
            print(df_select_f.head())
            print(df_select_f.shape)

            # Remove columns not needed in header file
            df_header.drop(columns=['Rank', 'is_date', 'fill_value'], axis=1, inplace = True)
            # Replace all values that are INT or FLOAT to NUMBER
            df_header = df_header.replace({'data_type': {'INT':'NUMBER', 'FLOAT':'NUMBER'}})

            # Create manifest file
            ## Modify/create manifest files --------------------------------------------------
            print('Adding table and info to manifest file')
            fname_save_data = self._path_minio_summary_intermediate + form.lower().replace(' ','_') + '_data.csv'
            fname_save_header = self._path_minio_summary_intermediate + form.lower().replace(' ','_') + '_header.csv'

            print(form)
            ##### This file will be used for merging data    
            self.summary_manifest_append(
                instr_name=form,
                fname_df_save=fname_save_data,
                fname_header_save=fname_save_header
            )

            ## Save data and header files --------------------------------------------------
            ### Save cbioportal formatted patient level dx data and header files
            print('Saving header and summary data')
            print('Saving %s' % fname_save_data)
            print(df_select_f.head())
            self._obj_minio.save_obj(
                df=df_select_f, 
                path_object=fname_save_data, 
                sep=','
            )
            print('Saving %s' % fname_save_header)
            print(df_header.head())
            self._obj_minio.save_obj(
                df=df_header, 
                path_object=fname_save_header, 
                sep=','
            )

        ## Note: Continue appending manifest files here
        df_manifest_s = self.return_manifest()
        print(df_manifest_s)
        
        ##### Save manifest
        self.summary_manifest_save(fname_save=fname_manifest)    
        
        return None

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
        print(df_manifest.head())
        print(dict_append)

        new_row_df = pd.DataFrame.from_dict(dict_append, orient='index').T
        df_manifest = pd.concat([df_manifest, new_row_df], ignore_index=True)
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
    

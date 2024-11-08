""""
cbioportal_summary_file_combiner.py


This class will put together all summary files with the current summary files in datahub
Object requires 
- path to the manifest file containing all of the redcap summary file and header paths
- path to current patient and sample summary paths 
"""
import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.summary import cBioPortalSummaryMergeTool
from cdm_cbioportal_etl.utils import constants

#Constants defined in python package for manifest file column names
COL_SUMMARY_FNAME_SAVE = constants.COL_SUMMARY_FNAME_SAVE
COL_SUMMARY_HEADER_FNAME_SAVE = constants.COL_SUMMARY_HEADER_FNAME_SAVE


class cbioportalSummaryFileCombiner(object):
    def __init__(
        self, 
        *,
        fname_minio_env, 
        fname_manifest, 
        fname_current_summary, 
        patient_or_sample,
        production_or_test
    ):
        # Input filenames
        self._fname_minio_env = fname_minio_env
        self._fname_current_summary = fname_current_summary
        self._fname_manifest = fname_manifest
        self._patient_or_sample = patient_or_sample
        self._production_or_test = production_or_test
        
        self._col_manifest_summary_data_fname = COL_SUMMARY_FNAME_SAVE
        self._col_manifest_header_fname = COL_SUMMARY_HEADER_FNAME_SAVE
        
        # DataFrame variables
        self._obj_patient_merge = None      
        
        # Process data
        self._obj_minio = MinioAPI(fname_minio_env=self._fname_minio_env)
        self._process_data()
        
    def _read_manifest(self):
        # load patient manifest
        print('Loading %s' % self._fname_manifest)
        obj = self._obj_minio.load_obj(path_object=self._fname_manifest)
        df_manifest = pd.read_csv(obj)
        return df_manifest
    
    def _load_current_summary(self):
        obj_patient_merge = cBioPortalSummaryMergeTool(
            fname_minio_env=self._fname_minio_env,
            fname_current_summary=self._fname_current_summary,
            production_or_test=self._production_or_test
        )
        self._obj_patient_merge = obj_patient_merge
        
    def return_orig(self):
        df_header, df_summary = self._obj_patient_merge.return_orig()
        return df_header, df_summary
    
    def return_final(self):
        df = self._obj_patient_merge.return_final()
        return df
    
    def save_update(self, fname):
        # Save data
        self._obj_patient_merge.save_data(fname_save=fname)
        
    def _process_data(self):
        # Load summary data
        df_manifest = self._read_manifest()
        
        # Load current summary
        self._load_current_summary()
        
        # Combine redcap reports with current summary
        self._combine_reports(df_manifest=df_manifest)
    
    def _combine_reports(self, df_manifest):
        # Combines all headers and corresponding data files into a portal summary table
        print('COMBINE REPORTS ------------------------------------')
        for ind, (i, row) in enumerate(df_manifest.iterrows()):
            fname_summary = df_manifest.loc[i, self._col_manifest_summary_data_fname]
            fname_header = df_manifest.loc[i, self._col_manifest_header_fname]
            print('Combining header %s and data %s into summary.' % (fname_header, fname_summary))
            
            self._obj_patient_merge.add_annotation_loader(
                fname_header=fname_header, 
                fname_data=fname_summary
            )
            df_p_header_new, df_p_data_new = self._obj_patient_merge.return_addition()
            # print('ADDITIONAL HEADER ------------------------------------')
            # print(df_p_header_new.head())
            # print('ADDITIONAL DATA ------------------------------------')
            # print(df_p_data_new.head())

            self._obj_patient_merge.merge_annotations(
                patient_or_sample=self._patient_or_sample
            )
            df_summary_header_f, df_summary_f = self._obj_patient_merge.return_output()
            # print('COMBINED HEADER ------------------------------------')
            # print(df_summary_header_f.head())
            # print('COMBINED DATA ------------------------------------')
            # print(df_summary_f.head())

            if ind < df_manifest.shape[0] - 1:
                self._obj_patient_merge.reset_origin()

    def backfill_missing_data(self, df_backfill_map):
        # TODO Create function that backfills missing summary data
        return None




# import os
# import sys
import argparse
from datetime import date

import pandas as pd

from cdm_cbioportal_etl.utils import yaml_config_parser

# sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
# from variables import DICT_FILES_TO_COPY


cols_fixed = [
    'SAMPLE_ID',
    'PATIENT_ID',
    'STOP_DATE',
    'SUBTYPE'
]
today = date.today()


def monitor_completeness(
        dict_files_to_copy,
        fname_log
):
    list_files = list(dict_files_to_copy.keys())
    list_files_timeline = [x for x in list_files if 'timeline' in x]
    list_files_summary = list(set.difference(set(list_files), set(list_files_timeline)))
    
    ## Test for empty columns in summary tables
    test = []
    for fname in list_files_summary:
        df_sum = pd.read_csv(fname, sep='\t', header=4)
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()
        test.append(any_empty)

    test_for_empty_summary = pd.concat(test, axis=0)
    
    ## Test for empty columns in timeline tables
    test = []
    for fname in list_files_timeline:
        # print('Loading %s' % fname)
        df_sum = pd.read_csv(fname, sep='\t', header=0)
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()
        # print(sum(any_empty))
        test.append(any_empty)
    
    test_for_empty_timeline = pd.concat(test, axis=0)
    
    ## Combine error tests from timeline and summary
    test_for_empty_all = pd.concat([test_for_empty_timeline, test_for_empty_summary], axis=0).reset_index()

    print('------------------------------')
    print(test_for_empty_all)
    print('------------------------------')
    
    if test_for_empty_all[0].any():
        print('Monitoring test FAILED.')
        
        return True
    
    else:
        print('Monitoring test passed. Writing results to log: %s' % fname_log)
        test_for_empty_all.to_csv(fname_log, sep=',')
        
        return False

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for monitoring completeness of data elements")
    parser.add_argument(
        "--fname_log",
        action="store",
        dest="fname_log",
        help="Log to indicate data is complete and can be pushed to datahub."
    )
    parser = argparse.ArgumentParser(description="Anchor dates used in the cBioPortal timeline files to deidentify")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    obj_yaml = yaml_config_parser(fname_yaml_config=args.config_yaml)
    dict_files_to_copy = obj_yaml.return_dict_datahub_to_minio()

    test = monitor_completeness(
        fname_log=args.fname_log,
        dict_files_to_copy=dict_files_to_copy
    )
    
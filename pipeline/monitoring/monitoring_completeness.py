import argparse
import os
from datetime import date

import pandas as pd

from cdm_cbioportal_etl.utils import cbioportal_update_config


cols_fixed = [
    'SAMPLE_ID',
    'PATIENT_ID',
    'STOP_DATE',
    'SUBTYPE'
]
today = date.today()


def monitor_completeness(
        dict_files_to_copy,
        incomplete_fields_csv
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
        print('Monitoring test passed. Writing results to log: %s' % incomplete_fields_csv)
        os.makedirs(os.path.dirname(incomplete_fields_csv), exist_ok=True)
        test_for_empty_all.to_csv(incomplete_fields_csv, sep=',')
        
        return False

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for monitoring completeness of data elements")
    parser.add_argument(
        "--incomplete_fields_csv",
        action="store",
        dest="incomplete_fields_csv",
        required=True,
        help="Log to indicate data is complete and can be pushed to datahub."
    )
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    parser.add_argument(
        "--path_datahub",
        action="store",
        dest="path_datahub",
        help="Path to datahub",
    )
    parser.add_argument(
        "--path_minio",
        action="store",
        dest="path_minio_cbio",
        help="Path to minio",
    )

    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    dict_files_to_copy = obj_yaml.return_dict_datahub_to_minio(path_datahub=args.path_datahub, path_minio=args.path_minio_cbio)

    test = monitor_completeness(
        incomplete_fields_csv=args.incomplete_fields_csv,
        dict_files_to_copy=dict_files_to_copy
    )
    
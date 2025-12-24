import argparse
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from datetime import date

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from lib.utils import cbioportal_update_config


cols_fixed = [
    'SAMPLE_ID',
    'PATIENT_ID',
    'STOP_DATE',
    'SUBTYPE'
]
today = date.today()


def monitor_completeness(
        dict_files_to_copy,
        obj_db
):
    list_files = list(dict_files_to_copy.keys())
    list_files_timeline = [x for x in list_files if 'timeline' in x]
    list_files_summary = list(set.difference(set(list_files), set(list_files_timeline)))

    ## Test for empty columns in summary tables
    test = []
    for fname in list_files_summary:
        print(f'Processing summary file: {fname}')
        # Read from Databricks volume
        df_full = obj_db.read_db_obj(volume_path=fname, sep='\t')
        # Skip first 4 header rows for summary files
        df_sum = df_full.iloc[4:].reset_index(drop=True)
        df_sum.columns = df_full.iloc[3]  # Use 4th row as column names
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()
        print(f'  Has completely empty columns: {any_empty.any()}')
        if any_empty.any():
            empty_cols = any_empty[any_empty].index.tolist()
            print(f'  Empty columns: {empty_cols}')
        test.append(any_empty)

    test_for_empty_summary = pd.concat(test, axis=0)

    ## Test for empty columns in timeline tables
    test = []
    for fname in list_files_timeline:
        print(f'Processing timeline file: {fname}')
        # Read from Databricks volume (timeline files have header at row 0)
        df_sum = obj_db.read_db_obj(volume_path=fname, sep='\t')
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()
        print(f'  Has completely empty columns: {any_empty.any()}')
        if any_empty.any():
            empty_cols = any_empty[any_empty].index.tolist()
            print(f'  Empty columns: {empty_cols}')
        test.append(any_empty)

    test_for_empty_timeline = pd.concat(test, axis=0)

    ## Combine error tests from timeline and summary
    test_for_empty_all = pd.concat([test_for_empty_timeline, test_for_empty_summary], axis=0).reset_index()

    print('------------------------------')
    print(test_for_empty_all)
    print('------------------------------')

    if test_for_empty_all[0].any():
        # Get the list of fields that are completely empty
        empty_fields = test_for_empty_all[test_for_empty_all[0] == True]['index'].tolist()
        error_msg = f"Data completeness check FAILED. The following fields are completely empty: {empty_fields}"
        print(error_msg)
        raise ValueError(error_msg)

    else:
        print('Data completeness check PASSED. All required fields contain data.')
        return True

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for monitoring completeness of data elements")
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
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="Path to Databricks environment file",
    )

    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    dict_files_to_copy = obj_yaml.return_dict_datahub_to_databricks(path_datahub=args.path_datahub, path_databricks=args.databricks_env)

    # Create Databricks API object
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Run monitoring - will raise ValueError if checks fail
    monitor_completeness(
        dict_files_to_copy=dict_files_to_copy,
        obj_db=obj_db
    )
    
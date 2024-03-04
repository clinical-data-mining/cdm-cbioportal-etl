import os
import sys
import argparse
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.abspath('/mind_data/cdm_repos/cdm-cbioportal-etl/utils'))
from constants import DICT_FILES_TO_COPY


cols_fixed = ['SAMPLE_ID', 'PATIENT_ID', 'STOP_DATE', 'SUBTYPE']
today = date.today()
FNAME_ERROR_LOG = f'/mind_data/cdm_repos/cdm-cbioportal-etl/logs/log_error_cdm_cbioportal_etl_{today}.csv'


def monitor_completeness(fname_log):
    list_files = list(DICT_FILES_TO_COPY.keys())
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
        print('Monitoring test FAILED. Writing results to log: %s' % FNAME_ERROR_LOG)
        
        test_for_empty_all.to_csv(FNAME_ERROR_LOG, sep=',')
        
        return True
    
    else:
        print('Monitoring test passed. Writing results to log: %s' % fname_log)
        test_for_empty_all.to_csv(fname_log, sep=',')
        
        return False

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cBioPortal timeline file for cancer progression predictions")
    parser.add_argument(
        "--fname_log",
        action="store",
        dest="fname_log",
        help="Log to indicate data is complete and can be pushed to datahub."
    )
    args = parser.parse_args()

    test = monitor_completeness(fname_log=args.fname_log)
    
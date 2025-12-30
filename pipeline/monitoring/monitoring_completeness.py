import argparse
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from datetime import date
from pathlib import Path

import pandas as pd


cols_fixed = [
    'SAMPLE_ID',
    'PATIENT_ID',
    'STOP_DATE',
    'SUBTYPE'
]
today = date.today()


def monitor_completeness(path_datahub):
    """
    Monitor completeness of cBioPortal data files.

    Parameters
    ----------
    path_datahub : str
        Path to directory containing cBioPortal data files

    Returns
    -------
    bool
        True if all completeness checks pass

    Raises
    ------
    ValueError
        If any required fields are completely empty
    """
    # Find all summary and timeline files
    datahub_path = Path(path_datahub)

    all_files = list(datahub_path.glob('*.txt'))
    list_files_timeline = [str(f) for f in all_files if f.name.startswith('data_timeline_')]
    list_files_summary = [str(f) for f in all_files if f.name.startswith('data_clinical_')]

    print(f"Found {len(list_files_summary)} summary files and {len(list_files_timeline)} timeline files")
    print(f"Summary files: {[Path(f).name for f in list_files_summary]}")
    print(f"Timeline files: {[Path(f).name for f in list_files_timeline]}")

    ## Test for empty columns in summary tables
    test = []
    for fname in list_files_summary:
        print(f'Processing summary file: {Path(fname).name}')
        # Read from local filesystem
        df_full = pd.read_csv(fname, sep='\t', header=None, dtype=str)
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

    test_for_empty_summary = pd.concat(test, axis=0) if test else pd.Series(dtype=bool)

    ## Test for empty columns in timeline tables
    test = []
    for fname in list_files_timeline:
        print(f'Processing timeline file: {Path(fname).name}')
        # Read from local filesystem (timeline files have header at row 0)
        df_sum = pd.read_csv(fname, sep='\t', dtype=str)
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()
        print(f'  Has completely empty columns: {any_empty.any()}')
        if any_empty.any():
            empty_cols = any_empty[any_empty].index.tolist()
            print(f'  Empty columns: {empty_cols}')
        test.append(any_empty)

    test_for_empty_timeline = pd.concat(test, axis=0) if test else pd.Series(dtype=bool)

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
    parser = argparse.ArgumentParser(
        description="Monitor completeness of cBioPortal data files",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--path_datahub",
        required=True,
        help="Path to directory containing cBioPortal data files (data_clinical_*.txt and data_timeline_*.txt)",
    )

    args = parser.parse_args()

    # Validate path exists
    if not os.path.exists(args.path_datahub):
        raise FileNotFoundError(f"Path does not exist: {args.path_datahub}")

    if not os.path.isdir(args.path_datahub):
        raise NotADirectoryError(f"Path is not a directory: {args.path_datahub}")

    # Run monitoring - will raise ValueError if checks fail
    monitor_completeness(path_datahub=args.path_datahub)
    
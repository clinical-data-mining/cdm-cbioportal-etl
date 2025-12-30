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
    print("\n" + "="*80)
    print("SUMMARY FILES - Completeness Check")
    print("="*80)

    summary_results = []
    for fname in list_files_summary:
        filename = Path(fname).name
        print(f'\n[{filename}]')
        # Read from local filesystem
        df_full = pd.read_csv(fname, sep='\t', header=None, dtype=str)
        # Skip first 4 header rows for summary files
        df_sum = df_full.iloc[4:].reset_index(drop=True)
        df_sum.columns = df_full.iloc[3]  # Use 4th row as column names
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()

        # Create per-file result
        empty_cols = any_empty[any_empty].index.tolist() if any_empty.any() else []
        has_empty = len(empty_cols) > 0

        print(f'  Total columns checked: {len(cols_test)}')
        print(f'  Empty columns found: {len(empty_cols)}')
        print(f'  Status: {"❌ FAIL" if has_empty else "✅ PASS"}')

        if has_empty:
            print(f'  Empty column names: {empty_cols}')
            summary_results.append({
                'file': filename,
                'empty_columns': empty_cols
            })

    ## Test for empty columns in timeline tables
    print("\n" + "="*80)
    print("TIMELINE FILES - Completeness Check")
    print("="*80)

    timeline_results = []
    for fname in list_files_timeline:
        filename = Path(fname).name
        print(f'\n[{filename}]')
        # Read from local filesystem (timeline files have header at row 0)
        df_sum = pd.read_csv(fname, sep='\t', dtype=str)
        cols_test = list(set(df_sum.columns) - set(cols_fixed))
        any_empty = df_sum[cols_test].isnull().all()

        # Create per-file result
        empty_cols = any_empty[any_empty].index.tolist() if any_empty.any() else []
        has_empty = len(empty_cols) > 0

        print(f'  Total columns checked: {len(cols_test)}')
        print(f'  Empty columns found: {len(empty_cols)}')
        print(f'  Status: {"❌ FAIL" if has_empty else "✅ PASS"}')

        if has_empty:
            print(f'  Empty column names: {empty_cols}')
            timeline_results.append({
                'file': filename,
                'empty_columns': empty_cols
            })

    ## Final summary
    print("\n" + "="*80)
    print("OVERALL COMPLETENESS SUMMARY")
    print("="*80)

    all_failed_files = summary_results + timeline_results

    if len(all_failed_files) > 0:
        print(f"\n❌ FAILED: {len(all_failed_files)} file(s) have completely empty columns\n")
        for result in all_failed_files:
            print(f"  • {result['file']}")
            print(f"    Empty columns: {result['empty_columns']}\n")

        error_msg = f"Data completeness check FAILED. {len(all_failed_files)} files have empty columns."
        raise ValueError(error_msg)
    else:
        total_files = len(list_files_summary) + len(list_files_timeline)
        print(f"\n✅ PASSED: All {total_files} files have complete data")
        print(f"  • {len(list_files_summary)} summary files checked")
        print(f"  • {len(list_files_timeline)} timeline files checked")
        print("\nData completeness check PASSED. All required fields contain data.")
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
    
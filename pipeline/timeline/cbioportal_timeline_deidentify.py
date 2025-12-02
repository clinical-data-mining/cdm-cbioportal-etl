"""
Generic timeline deidentification script for cBioPortal.

This script works for all timeline data domains (medications, labs, diagnoses, etc.).

Processing steps:
1. Loads timeline data from Databricks
2. Merges with anchor dates and OS dates
3. Optionally truncates dates that exceed OS_DATE
4. Calculates deidentified dates (days from anchor)
5. Saves PHI version to Databricks volume
6. Saves deidentified version to GPFS

Usage examples:

Medications:
  python pipeline/timeline/cbioportal_timeline_deidentify.py \
    --fname_dbx=/path/to/databricks_env.txt \
    --fname_timeline=schema.table_timeline_medications \
    --fname_sample=/path/to/data_clinical_sample.txt \
    --fname_output_volume=/Volumes/path/data_timeline_treatment_phi.tsv \
    --fname_output_gpfs=/gpfs/path/data_timeline_treatment.txt \
    --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,TREATMENT_TYPE,AGENT" \
    --truncate_by_os_date

Labs:
  python pipeline/timeline/cbioportal_timeline_deidentify.py \
    --fname_dbx=/path/to/databricks_env.txt \
    --fname_timeline=schema.table_timeline_labs \
    --fname_sample=/path/to/data_clinical_sample.txt \
    --fname_output_volume=/Volumes/path/data_timeline_lab_test_phi.tsv \
    --fname_output_gpfs=/gpfs/path/data_timeline_lab_test.txt \
    --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,TEST,RESULT"
"""

import argparse

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import mrn_zero_pad
from cdm_cbioportal_etl.utils import constants
from pipeline.timeline.timeline_utils import (
    load_dbx_table,
    compute_os_date,
    process_df_os,
    validate_date_parsing,
    report_deidentification_stats,
    COL_OS_DATE,
    COL_ID
)

COLS_ORDER_GENERAL = constants.COLS_ORDER_GENERAL
COL_ANCHOR_DATE = constants.COL_ANCHOR_DATE

# Fixed table names (configured upstream)
FNAME_DEMO = 'cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics'
FNAME_DEID = 'cdsi_prod.cdm_idbw_impact_pipeline_prod.timeline_anchor_dates'


def main():
    parser = argparse.ArgumentParser(
        description="Generic timeline deidentification for cBioPortal (medications, labs, diagnoses, etc.)"
    )
    parser.add_argument(
        "--fname_dbx",
        action="store",
        dest="fname_dbx",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--fname_timeline",
        action="store",
        dest="fname_timeline",
        required=True,
        help="Databricks table name for timeline data (e.g., 'schema.table_timeline_medications')"
    )
    parser.add_argument(
        "--fname_sample",
        action="store",
        dest="fname_sample",
        required=True,
        help="Path to sample list file (data_clinical_sample.txt)"
    )
    parser.add_argument(
        "--fname_output_volume",
        action="store",
        dest="fname_output_volume",
        required=True,
        help="Output path for PHI version in Databricks volume"
    )
    parser.add_argument(
        "--fname_output_gpfs",
        action="store",
        dest="fname_output_gpfs",
        required=True,
        help="Output path for deidentified version on GPFS"
    )
    parser.add_argument(
        "--columns_cbio",
        action="store",
        dest="columns_cbio",
        required=True,
        help="Comma-separated list of columns for final cBioPortal output"
    )
    parser.add_argument(
        "--truncate_by_os_date",
        action="store_true",
        dest="truncate_by_os_date",
        default=False,
        help="If set, truncate START_DATE and STOP_DATE that exceed OS_DATE"
    )

    args = parser.parse_args()

    # Parse comma-separated columns list
    list_cols_cbio_timeline = [col.strip() for col in args.columns_cbio.split(',')]

    print("=" * 80)
    print("TIMELINE DEIDENTIFICATION FOR CBIOPORTAL")
    print("=" * 80)
    print(f"Timeline table: {args.fname_timeline}")
    print(f"Sample list: {args.fname_sample}")
    print(f"Output volume (PHI): {args.fname_output_volume}")
    print(f"Output GPFS (deid): {args.fname_output_gpfs}")
    print(f"Truncate by OS_DATE: {args.truncate_by_os_date}")
    print(f"cBioPortal columns: {list_cols_cbio_timeline}")
    print("=" * 80)

    # =========================================================================
    # 1. Load sample list and get patient/sample IDs
    # =========================================================================
    print(f'\nLoading sample list: {args.fname_sample}')
    df_samples_used = pd.read_csv(args.fname_sample, sep='\t')
    list_dmp_ids = list(df_samples_used['PATIENT_ID'].drop_duplicates())
    list_sample_ids = list(df_samples_used['SAMPLE_ID'].drop_duplicates())
    print(f'Number of sample IDs: {len(list_sample_ids)}')
    print(f'Number of patient IDs: {len(list_dmp_ids)}')

    # =========================================================================
    # 2. Compute OS dates from demographics
    # =========================================================================
    df_patient_os_date = compute_os_date(
        fname_dbx=args.fname_dbx,
        fname_demo=FNAME_DEMO
    )

    df_os = process_df_os(
        df=df_patient_os_date,
        col_id=COL_ID,
        col_os_date=COL_OS_DATE
    )

    # =========================================================================
    # 3. Load anchor dates
    # =========================================================================
    print(f'\nLoading anchor dates: {FNAME_DEID}')
    df_anchor = load_dbx_table(fname_dbx=args.fname_dbx, table_name=FNAME_DEID)
    df_anchor = mrn_zero_pad(df=df_anchor, col_mrn='MRN')
    df_anchor[COL_ANCHOR_DATE] = pd.to_datetime(df_anchor[COL_ANCHOR_DATE], errors='coerce')

    # =========================================================================
    # 4. Load timeline raw data
    # =========================================================================
    print(f'\nLoading timeline data: {args.fname_timeline}')
    df_timeline_raw = load_dbx_table(fname_dbx=args.fname_dbx, table_name=args.fname_timeline)
    df_timeline_raw = mrn_zero_pad(df=df_timeline_raw, col_mrn='MRN')

    # Parse dates and validate
    df_timeline_raw['START_DATE_FORMATTED'] = pd.to_datetime(df_timeline_raw['START_DATE'], errors='coerce')
    df_timeline_raw['STOP_DATE_FORMATTED'] = pd.to_datetime(df_timeline_raw['STOP_DATE'], errors='coerce')

    validate_date_parsing(df_timeline_raw)

    # =========================================================================
    # 5. Merge data and create timeline
    # =========================================================================
    print('\nMerging data...')
    df_f = df_samples_used[['PATIENT_ID']].drop_duplicates()
    df_f = df_f.merge(right=df_anchor, how='left', left_on='PATIENT_ID', right_on='DMP_ID')
    df_f = df_f.merge(right=df_os, how='left', on='MRN')
    df_f = df_f.merge(right=df_timeline_raw, how='left', on='MRN')

    # =========================================================================
    # 6. Truncate dates by OS_DATE if requested
    # =========================================================================
    if args.truncate_by_os_date:
        print('\nTruncating dates by OS_DATE...')
        logic_fix_start = df_f['START_DATE_FORMATTED'] > df_f['OS_DATE']
        logic_fix_stop = df_f['STOP_DATE_FORMATTED'] > df_f['OS_DATE']

        df_f['START_DATE_FORMATTED_FIXED'] = df_f['START_DATE_FORMATTED'].copy()
        df_f['STOP_DATE_FORMATTED_FIXED'] = df_f['STOP_DATE_FORMATTED'].copy()

        df_f.loc[logic_fix_start, 'START_DATE_FORMATTED_FIXED'] = df_f['OS_DATE']
        df_f.loc[logic_fix_stop, 'STOP_DATE_FORMATTED_FIXED'] = df_f['OS_DATE']

        patients_dates_fixed = df_f.loc[logic_fix_start | logic_fix_stop, 'PATIENT_ID'].nunique()
        print(f'Number of patients with dates fixed: {patients_dates_fixed}')
    else:
        print('\nSkipping date truncation (use --truncate_by_os_date to enable)')
        df_f['START_DATE_FORMATTED_FIXED'] = df_f['START_DATE_FORMATTED']
        df_f['STOP_DATE_FORMATTED_FIXED'] = df_f['STOP_DATE_FORMATTED']

    # =========================================================================
    # 7. Calculate deidentified dates (days from anchor)
    # =========================================================================
    print('\nCalculating deidentified dates...')
    start_date = (df_f['START_DATE_FORMATTED_FIXED'] - df_f[COL_ANCHOR_DATE]).dt.days
    stop_date = (df_f['STOP_DATE_FORMATTED_FIXED'] - df_f[COL_ANCHOR_DATE]).dt.days

    df_f['START_DATE_DEID'] = start_date
    df_f['STOP_DATE_DEID'] = stop_date

    # Report statistics
    report_deidentification_stats(df_f)

    # =========================================================================
    # 8. Save PHI version to Databricks volume
    # =========================================================================
    print(f'\nSaving PHI version to: {args.fname_output_volume}')
    obj_dbx = DatabricksAPI(fname_databricks_env=args.fname_dbx)
    obj_dbx.write_db_obj(df=df_f, volume_path=args.fname_output_volume, sep='\t', overwrite=True)

    # =========================================================================
    # 9. Create deidentified version and save to GPFS
    # =========================================================================
    print('\nCreating deidentified version...')
    df_deid = df_f.drop(columns=['START_DATE', 'STOP_DATE'])
    df_deid = df_deid.rename(columns={'START_DATE_DEID': 'START_DATE', 'STOP_DATE_DEID': 'STOP_DATE'})

    # Select only requested columns
    missing_cols = [col for col in list_cols_cbio_timeline if col not in df_deid.columns]
    if missing_cols:
        print(f'WARNING: Missing columns in output: {missing_cols}')

    available_cols = [col for col in list_cols_cbio_timeline if col in df_deid.columns]
    df_deid = df_deid[available_cols].copy()

    # Drop rows with null START_DATE or PATIENT_ID
    df_deid_f = df_deid.dropna(subset=['START_DATE', 'PATIENT_ID'], how='any').sort_values(by=['PATIENT_ID', 'START_DATE'])

    print(f'Final deidentified rows: {len(df_deid_f)}')

    print(f'\nSaving deidentified version to: {args.fname_output_gpfs}')
    df_deid_f.to_csv(args.fname_output_gpfs, sep='\t', index=False)

    print('\n' + '=' * 80)
    print('DEIDENTIFICATION COMPLETE')
    print('=' * 80)


if __name__ == '__main__':
    main()

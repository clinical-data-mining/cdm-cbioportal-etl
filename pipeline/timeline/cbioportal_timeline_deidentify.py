"""
Generic timeline deidentification script for cBioPortal.

This script works for all timeline data domains (medications, labs, diagnoses, etc.).

Processing steps:
1. Loads timeline data from Databricks
2. Merges with anchor dates and OS dates
3. Removes invalid future dates (sets to null)
4. Optionally truncates dates that exceed OS_DATE
5. Calculates deidentified dates (days from anchor)
6. Saves PHI version to Databricks volume
7. Saves deidentified version to GPFS

Usage examples:

Medications (patient-level):
  python pipeline/timeline/cbioportal_timeline_deidentify.py \
    --fname_dbx=/path/to/databricks_env.txt \
    --fname_timeline=schema.table_timeline_medications \
    --fname_sample=/path/to/data_clinical_sample.txt \
    --fname_output_volume=/Volumes/path/data_timeline_treatment_phi.tsv \
    --fname_output_gpfs=/gpfs/path/data_timeline_treatment.txt \
    --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,TREATMENT_TYPE,AGENT" \
    --truncate_by_os_date

Labs (patient-level):
  python pipeline/timeline/cbioportal_timeline_deidentify.py \
    --fname_dbx=/path/to/databricks_env.txt \
    --fname_timeline=schema.table_timeline_labs \
    --fname_sample=/path/to/data_clinical_sample.txt \
    --fname_output_volume=/Volumes/path/data_timeline_lab_test_phi.tsv \
    --fname_output_gpfs=/gpfs/path/data_timeline_lab_test.txt \
    --columns_cbio="PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,TEST,RESULT"

Sample-level timeline (e.g., sequencing data):
  python pipeline/timeline/cbioportal_timeline_deidentify.py \
    --fname_dbx=/path/to/databricks_env.txt \
    --fname_timeline=schema.table_timeline_sequencing \
    --fname_sample=/path/to/data_clinical_sample.txt \
    --fname_output_volume=/Volumes/path/data_timeline_sequencing_phi.tsv \
    --fname_output_gpfs=/gpfs/path/data_timeline_sequencing.txt \
    --columns_cbio="PATIENT_ID,SAMPLE_ID,START_DATE,EVENT_TYPE" \
    --merge_level=sample
"""

import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import mrn_zero_pad
from lib.utils import constants

COLS_ORDER_GENERAL = constants.COLS_ORDER_GENERAL
COL_ANCHOR_DATE = constants.COL_ANCHOR_DATE
COL_OS_DATE = 'OS_DATE'
COL_ID = 'MRN'

# Fixed table names (configured upstream)
FNAME_DEMO = 'cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics'
FNAME_DEID = 'cdsi_prod.cdm_idbw_impact_pipeline_prod.timeline_anchor_dates'


# =============================================================================
# Utility Functions
# =============================================================================

def load_dbx_table(fname_dbx, table_name):
    """Load a table from Databricks.

    Args:
        fname_dbx: Path to Databricks environment file
        table_name: Full table name (e.g., 'schema.table')

    Returns:
        DataFrame with table data
    """
    obj_dbx = DatabricksAPI(fname_databricks_env=fname_dbx)

    sql = f"""
    select * FROM {table_name}
    """

    df = obj_dbx.query_from_sql(sql=sql)

    return df


def compute_os_date(fname_dbx, fname_demo):
    """Compute overall survival date from demographics table.

    OS_DATE is the date of death if available, otherwise last contact date.
    If death date is earlier than last contact, use death date.

    Args:
        fname_dbx: Path to Databricks environment file
        fname_demo: Demographics table name

    Returns:
        DataFrame with MRN and OS_DATE columns
    """
    print(f'Loading {fname_demo}')
    df_demo = load_dbx_table(
        fname_dbx=fname_dbx,
        table_name=fname_demo
    )

    df_demo = mrn_zero_pad(df=df_demo, col_mrn=COL_ID)
    df_demo['PLA_LAST_CONTACT_DTE'] = pd.to_datetime(df_demo['PLA_LAST_CONTACT_DTE'], errors='coerce')
    df_demo['PT_DEATH_DTE'] = pd.to_datetime(df_demo['PT_DEATH_DTE'], errors='coerce')

    # Remove timezone info to ensure dates are tz-naive
    if pd.api.types.is_datetime64tz_dtype(df_demo['PLA_LAST_CONTACT_DTE']):
        df_demo['PLA_LAST_CONTACT_DTE'] = df_demo['PLA_LAST_CONTACT_DTE'].dt.tz_localize(None)
    if pd.api.types.is_datetime64tz_dtype(df_demo['PT_DEATH_DTE']):
        df_demo['PT_DEATH_DTE'] = df_demo['PT_DEATH_DTE'].dt.tz_localize(None)

    df_demo[COL_OS_DATE] = df_demo['PT_DEATH_DTE'].fillna(df_demo['PLA_LAST_CONTACT_DTE'])

    # If death date is earlier than last contact, use death date
    log_err = df_demo['PT_DEATH_DTE'] < df_demo['PLA_LAST_CONTACT_DTE']
    df_demo.loc[log_err, COL_OS_DATE] = df_demo['PT_DEATH_DTE']

    return df_demo


def process_df_os(df, col_id=COL_ID, col_os_date=COL_OS_DATE):
    """Extract and standardize OS date columns.

    Args:
        df: DataFrame with patient OS data
        col_id: Column name for patient ID
        col_os_date: Column name for OS date

    Returns:
        DataFrame with standardized MRN and OS_DATE columns
    """
    df_os = df[[col_id, col_os_date]].copy()
    df_os.columns = ['MRN', COL_OS_DATE]

    return df_os


def validate_date_parsing(df, start_col='START_DATE', stop_col='STOP_DATE',
                          start_formatted='START_DATE_FORMATTED',
                          stop_formatted='STOP_DATE_FORMATTED'):
    """Validate that dates were successfully parsed and report statistics.

    Args:
        df: DataFrame with original and formatted date columns
        start_col: Original start date column name
        stop_col: Original stop date column name
        start_formatted: Formatted start date column name
        stop_formatted: Formatted stop date column name

    Returns:
        Dict with validation statistics
    """
    # Count rows that were NOT null originally but became null after conversion
    coerced_to_null_start = ((df[start_col].notnull()) &
                             (df[start_formatted].isnull())).sum()

    coerced_to_null_stop = ((df[stop_col].notnull()) &
                            (df[stop_formatted].isnull())).sum()

    # Total non-null values in original columns
    total_non_null_start = df[start_col].notnull().sum()
    total_non_null_stop = df[stop_col].notnull().sum()

    # Calculate percentage of non-null values that failed to parse
    pct_coerced_start = coerced_to_null_start / total_non_null_start if total_non_null_start > 0 else 0
    pct_coerced_stop = coerced_to_null_stop / total_non_null_stop if total_non_null_stop > 0 else 0

    print(f"START_DATE: {coerced_to_null_start} values coerced to null ({pct_coerced_start:.2%})")
    print(f"STOP_DATE: {coerced_to_null_stop} values coerced to null ({pct_coerced_stop:.2%})")

    return {
        'coerced_to_null_start': coerced_to_null_start,
        'coerced_to_null_stop': coerced_to_null_stop,
        'pct_coerced_start': pct_coerced_start,
        'pct_coerced_stop': pct_coerced_stop
    }


def days_to_readable_compact(days):
    """Convert days to 'Ny Mm Xd' format. Handles negative values.

    Args:
        days: Number of days (can be negative)

    Returns:
        String in format like "2y 3m 15d" or "-1y 2m 5d"
    """
    if pd.isna(days):
        return ""

    # Handle negative values
    is_negative = days < 0
    days = abs(int(days))

    years = days // 365
    remaining_days = days % 365
    months = remaining_days // 30
    final_days = remaining_days % 30

    parts = []
    if years > 0:
        parts.append(f"{years}y")
    if months > 0:
        parts.append(f"{months}m")
    if final_days > 0 or len(parts) == 0:
        parts.append(f"{final_days}d")

    result = " ".join(parts)

    if is_negative:
        result = "-" + result

    return result


def report_deidentification_stats(df, anchor_col='ANCHOR_DATE', os_col=COL_OS_DATE):
    """Report statistics about the deidentification process.

    Args:
        df: DataFrame after deidentification processing
        anchor_col: Anchor date column name
        os_col: OS date column name
    """
    ids_with_missing_os_dates = df.loc[df[os_col].isnull(), 'PATIENT_ID'].drop_duplicates()
    ids_with_missing_mrns = df.loc[df['MRN'].isnull(), 'PATIENT_ID'].drop_duplicates()
    ids_with_missing_seq_date = df.loc[df[COL_ANCHOR_DATE].isnull(), 'PATIENT_ID'].drop_duplicates()
    ids_with_missing_timeline_data = df.loc[df['START_DATE'].isnull(), 'PATIENT_ID'].drop_duplicates()
    timepoints_missing_start = df.loc[df['START_DATE_DEID'].isnull(), 'PATIENT_ID'].drop_duplicates()

    print(f"Number of patients with missing MRN: {len(ids_with_missing_mrns)}")
    print(f"Number of patients with missing SEQ DATE: {len(ids_with_missing_seq_date)}")
    print(f"Number of patients with missing OS dates: {len(ids_with_missing_os_dates)}")
    print(f"Number of patients with missing timeline data: {len(ids_with_missing_timeline_data)}")
    print(f"Number of timepoints with de-id error: {len(timepoints_missing_start)}")


# =============================================================================
# Main Function
# =============================================================================


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
        "--fname_deid",
        action="store",
        dest="fname_deid",
        default=FNAME_DEID,
        help=f"Databricks table name for anchor dates (default: {FNAME_DEID})"
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
    parser.add_argument(
        "--merge_level",
        action="store",
        dest="merge_level",
        default="patient",
        choices=["patient", "sample"],
        help="Level at which to merge timeline data: 'patient' (default) or 'sample'"
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
    print(f"Merge level: {args.merge_level}")
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
    print(f'\nLoading anchor dates: {args.fname_deid}')
    df_anchor = load_dbx_table(fname_dbx=args.fname_dbx, table_name=args.fname_deid)
    df_anchor = mrn_zero_pad(df=df_anchor, col_mrn='MRN')
    df_anchor[COL_ANCHOR_DATE] = pd.to_datetime(df_anchor[COL_ANCHOR_DATE], errors='coerce')

    # Remove timezone info to ensure dates are tz-naive
    if pd.api.types.is_datetime64tz_dtype(df_anchor[COL_ANCHOR_DATE]):
        df_anchor[COL_ANCHOR_DATE] = df_anchor[COL_ANCHOR_DATE].dt.tz_localize(None)

    # =========================================================================
    # 4. Load timeline raw data
    # =========================================================================
    print(f'\nLoading timeline data: {args.fname_timeline}')
    df_timeline_raw = load_dbx_table(fname_dbx=args.fname_dbx, table_name=args.fname_timeline)
    df_timeline_raw = mrn_zero_pad(df=df_timeline_raw, col_mrn='MRN')

    # Ensure START_DATE and STOP_DATE columns exist
    if 'START_DATE' not in df_timeline_raw.columns:
        print("WARNING: START_DATE column not found, creating empty column")
        df_timeline_raw['START_DATE'] = pd.NaT

    if 'STOP_DATE' not in df_timeline_raw.columns:
        print("WARNING: STOP_DATE column not found, creating empty column")
        df_timeline_raw['STOP_DATE'] = pd.NaT

    # Parse dates and validate
    df_timeline_raw['START_DATE_FORMATTED'] = pd.to_datetime(df_timeline_raw['START_DATE'], errors='coerce')
    df_timeline_raw['STOP_DATE_FORMATTED'] = pd.to_datetime(df_timeline_raw['STOP_DATE'], errors='coerce')

    # Remove timezone info to ensure all dates are tz-naive
    if pd.api.types.is_datetime64tz_dtype(df_timeline_raw['START_DATE_FORMATTED']):
        df_timeline_raw['START_DATE_FORMATTED'] = df_timeline_raw['START_DATE_FORMATTED'].dt.tz_localize(None)
    if pd.api.types.is_datetime64tz_dtype(df_timeline_raw['STOP_DATE_FORMATTED']):
        df_timeline_raw['STOP_DATE_FORMATTED'] = df_timeline_raw['STOP_DATE_FORMATTED'].dt.tz_localize(None)

    validate_date_parsing(df_timeline_raw)

    # =========================================================================
    # 5. Merge data and create timeline
    # =========================================================================
    print(f'\nMerging data at {args.merge_level} level...')

    if args.merge_level == 'patient':
        # Patient-level merge: merge timeline data on MRN (patient level)
        df_f = df_samples_used[['PATIENT_ID']].drop_duplicates()
        df_f = df_f.merge(right=df_anchor, how='left', left_on='PATIENT_ID', right_on='DMP_ID')
        df_f = df_f.merge(right=df_os, how='left', on='MRN')
        df_f = df_f.merge(right=df_timeline_raw, how='left', on='MRN')
    else:  # sample level
        # Sample-level merge: merge timeline data on SAMPLE_ID
        df_f = df_samples_used[['SAMPLE_ID', 'PATIENT_ID']].drop_duplicates()
        df_f = df_f.merge(right=df_anchor, how='left', left_on='PATIENT_ID', right_on='DMP_ID')
        df_f = df_f.merge(right=df_os, how='left', on='MRN')
        df_f = df_f.merge(right=df_timeline_raw, how='left', on=['SAMPLE_ID', 'MRN'])

    # =========================================================================
    # 6. Remove future dates (dates in the future are invalid)
    # =========================================================================
    print('\nChecking for future dates...')
    today = pd.Timestamp.today().normalize()

    logic_future_start = df_f['START_DATE_FORMATTED'] > today
    logic_future_stop = df_f['STOP_DATE_FORMATTED'] > today

    # Create copies of the formatted dates
    df_f['START_DATE_FORMATTED_FIXED'] = df_f['START_DATE_FORMATTED'].copy()
    df_f['STOP_DATE_FORMATTED_FIXED'] = df_f['STOP_DATE_FORMATTED'].copy()

    # Null out future dates
    df_f.loc[logic_future_start, 'START_DATE_FORMATTED_FIXED'] = pd.NaT
    df_f.loc[logic_future_stop, 'STOP_DATE_FORMATTED_FIXED'] = pd.NaT

    patients_future_dates = df_f.loc[logic_future_start | logic_future_stop, 'PATIENT_ID'].nunique()
    rows_future_start = logic_future_start.sum()
    rows_future_stop = logic_future_stop.sum()
    print(f'Number of rows with future START_DATE (set to null): {rows_future_start}')
    print(f'Number of rows with future STOP_DATE (set to null): {rows_future_stop}')
    print(f'Number of patients affected: {patients_future_dates}')

    # =========================================================================
    # 7. Truncate dates by OS_DATE if requested
    # =========================================================================
    if args.truncate_by_os_date:
        print('\nTruncating dates by OS_DATE...')
        logic_fix_start = df_f['START_DATE_FORMATTED_FIXED'] > df_f['OS_DATE']
        logic_fix_stop = df_f['STOP_DATE_FORMATTED_FIXED'] > df_f['OS_DATE']

        # Truncate to OS_DATE (working on already FIXED columns from future date check)
        df_f.loc[logic_fix_start, 'START_DATE_FORMATTED_FIXED'] = df_f['OS_DATE']
        df_f.loc[logic_fix_stop, 'STOP_DATE_FORMATTED_FIXED'] = df_f['OS_DATE']

        patients_dates_truncated = df_f.loc[logic_fix_start | logic_fix_stop, 'PATIENT_ID'].nunique()
        rows_truncated_start = logic_fix_start.sum()
        rows_truncated_stop = logic_fix_stop.sum()
        print(f'Number of rows with START_DATE truncated to OS_DATE: {rows_truncated_start}')
        print(f'Number of rows with STOP_DATE truncated to OS_DATE: {rows_truncated_stop}')
        print(f'Number of patients affected: {patients_dates_truncated}')
    else:
        print('\nSkipping OS_DATE truncation (use --truncate_by_os_date to enable)')

    # =========================================================================
    # 8. Calculate deidentified dates (days from anchor)
    # =========================================================================
    print('\nCalculating deidentified dates...')
    start_date = (df_f['START_DATE_FORMATTED_FIXED'] - df_f[COL_ANCHOR_DATE]).dt.days
    stop_date = (df_f['STOP_DATE_FORMATTED_FIXED'] - df_f[COL_ANCHOR_DATE]).dt.days

    df_f['START_DATE_DEID'] = start_date
    df_f['STOP_DATE_DEID'] = stop_date

    # Create readable date columns
    df_f['START_DATE_READABLE'] = df_f['START_DATE_DEID'].apply(days_to_readable_compact)
    df_f['STOP_DATE_READABLE'] = df_f['STOP_DATE_DEID'].apply(days_to_readable_compact)

    # Report statistics
    report_deidentification_stats(df_f)

    # =========================================================================
    # 9. Save PHI version to Databricks volume
    # =========================================================================
    print(f'\nSaving PHI version to: {args.fname_output_volume}')
    obj_dbx = DatabricksAPI(fname_databricks_env=args.fname_dbx)
    obj_dbx.write_db_obj(df=df_f, volume_path=args.fname_output_volume, sep='\t', overwrite=True)

    # =========================================================================
    # 10. Create deidentified version and save to GPFS
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

    # Convert START_DATE and STOP_DATE to integers (use Int64 to handle NaN values)
    df_deid_f['START_DATE'] = df_deid_f['START_DATE'].astype('Int64')
    df_deid_f['STOP_DATE'] = df_deid_f['STOP_DATE'].astype('Int64')

    print(f'Final deidentified rows: {len(df_deid_f)}')

    print(f'\nSaving deidentified version to: {args.fname_output_gpfs}')
    df_deid_f.to_csv(args.fname_output_gpfs, sep='\t', index=False)

    print('\n' + '=' * 80)
    print('DEIDENTIFICATION COMPLETE')
    print('=' * 80)


if __name__ == '__main__':
    main()

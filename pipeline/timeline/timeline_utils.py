"""
Shared utility functions for timeline deidentification across different data domains.

This module contains reusable functions for:
- Loading data from Databricks
- Computing OS dates from demographics
- Processing dataframes for timeline deidentification
"""

import pandas as pd
from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import mrn_zero_pad


COL_OS_DATE = 'OS_DATE'
COL_ID = 'MRN'


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


def report_deidentification_stats(df, anchor_col='ANCHOR_DATE', os_col=COL_OS_DATE):
    """Report statistics about the deidentification process.

    Args:
        df: DataFrame after deidentification processing
        anchor_col: Anchor date column name
        os_col: OS date column name
    """
    ids_with_missing_os_dates = df.loc[df[os_col].isnull(), 'PATIENT_ID'].drop_duplicates()
    ids_with_missing_mrns = df.loc[df['MRN'].isnull(), 'PATIENT_ID'].drop_duplicates()
    ids_with_missing_seq_date = df.loc[df['DTE_TUMOR_SEQUENCING'].isnull(), 'PATIENT_ID'].drop_duplicates()
    ids_with_missing_timeline_data = df.loc[df['START_DATE'].isnull(), 'PATIENT_ID'].drop_duplicates()
    timepoints_missing_start = df.loc[df['START_DATE_DEID'].isnull(), 'PATIENT_ID'].drop_duplicates()

    print(f"Number of patients with missing MRN: {len(ids_with_missing_mrns)}")
    print(f"Number of patients with missing SEQ DATE: {len(ids_with_missing_seq_date)}")
    print(f"Number of patients with missing OS dates: {len(ids_with_missing_os_dates)}")
    print(f"Number of patients with missing timeline data: {len(ids_with_missing_timeline_data)}")
    print(f"Number of timepoints with de-id error: {len(timepoints_missing_start)}")

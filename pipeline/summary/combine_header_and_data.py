"""
combine_header_and_data.py

Script 4 of 4: Combine header and data into final cBioPortal file.

This script:
1. Loads tall-format header from Script 3
2. Transposes header to cBioPortal wide format (4 rows × N columns)
3. Loads merged data from Script 2
4. Combines header + data vertically
5. Saves to BOTH Databricks volume and local filesystem

The final format is:
    Row 0: Display labels (#Patient Identifier, Age at Sequencing, ...)
    Row 1: Datatypes (STRING, NUMBER, ...)
    Row 2: Descriptions
    Row 3: Column names (PATIENT_ID, AGE_AT_SEQUENCING, ...)
    Rows 4+: Data rows

Usage:
    python combine_header_and_data.py \
        --header_volume_path /Volumes/.../data_clinical_patient_header.txt \
        --data_volume_path /Volumes/.../data_clinical_patient_data.txt \
        --databricks_env /path/to/databricks.env \
        --output_volume_path /Volumes/.../data_clinical_patient.txt \
        --output_local_path /gpfs/.../data_clinical_patient.txt
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from msk_cdm.databricks import DatabricksAPI


def transpose_header_to_wide(df_header_tall: pd.DataFrame) -> pd.DataFrame:
    """
    Transpose tall-format header to cBioPortal wide format.

    Input (tall):
        column_name | display_label | description | datatype | priority
        PATIENT_ID  | #Patient Identifier | Identifier to... | STRING | 0
        AGE         | Age at Sequencing | Age in years | NUMBER | 1

    Output (wide):
        PATIENT_ID                        | AGE
        #Patient Identifier               | Age at Sequencing
        #Identifier to...                 | Age in years
        #STRING                           | NUMBER
        #0                                | 1
        PATIENT_ID                        | AGE

    Note: First column values in rows 0-3 get "#" prefix (cBioPortal convention)

    Parameters
    ----------
    df_header_tall : pd.DataFrame
        Header in tall format (5 columns × N rows)

    Returns
    -------
    pd.DataFrame
        Header in wide format (5 rows × N columns)
    """
    print(f"\n{'='*80}")
    print(f"TRANSPOSING HEADER TO CBIOPORTAL WIDE FORMAT")
    print(f"{'='*80}")
    print(f"Input (tall):  {df_header_tall.shape[0]} rows × {df_header_tall.shape[1]} columns")

    # Build wide format dictionary
    header_dict = {}
    first_column = True

    for idx, row_data in df_header_tall.iterrows():
        col_name = row_data['column_name']

        # Get values for this column and ensure they are strings
        display_label = str(row_data['display_label'])
        description = str(row_data['description'])
        datatype = str(row_data['datatype'])
        priority = str(row_data['priority'])

        # For the first column, add # prefix to rows 0-3
        # (cBioPortal convention: metadata rows in first column start with #)
        if first_column:
            # Only add # if not already present
            if not display_label.startswith('#'):
                display_label = '#' + display_label
            if not description.startswith('#'):
                description = '#' + description
            if not datatype.startswith('#'):
                datatype = '#' + datatype
            if not priority.startswith('#'):
                priority = '#' + priority
            first_column = False

        header_dict[col_name] = [
            display_label,   # Row 0: Display label
            description,     # Row 1: Description
            datatype,        # Row 2: Datatype
            priority,        # Row 3: Priority
            col_name         # Row 4: Column name
        ]

    # Create wide DataFrame
    df_header_wide = pd.DataFrame(header_dict)

    print(f"Output (wide): {df_header_wide.shape[0]} rows × {df_header_wide.shape[1]} columns")
    print(f"Columns: {list(df_header_wide.columns)}")

    return df_header_wide


def combine_header_and_data(
    df_header_wide: pd.DataFrame,
    df_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine header and data into final cBioPortal format.

    Parameters
    ----------
    df_header_wide : pd.DataFrame
        Header in wide format (5 rows × N columns)
    df_data : pd.DataFrame
        Data (M rows × N columns)

    Returns
    -------
    pd.DataFrame
        Combined file (5 + M rows × N columns)
    """
    print(f"\n{'='*80}")
    print(f"COMBINING HEADER AND DATA")
    print(f"{'='*80}")
    print(f"Header shape: {df_header_wide.shape}")
    print(f"Data shape:   {df_data.shape}")

    # Ensure column order matches
    if list(df_header_wide.columns) != list(df_data.columns):
        print("\n⚠ WARNING: Column order mismatch. Reordering header to match data...")
        df_header_wide = df_header_wide[df_data.columns]
        print(f"  Header reordered: {list(df_header_wide.columns)}")

    # Combine vertically
    df_combined = pd.concat([df_header_wide, df_data], axis=0, ignore_index=True)

    print(f"\nCombined shape: {df_combined.shape}")
    print(f"  Header rows: 5")
    print(f"  Data rows:   {df_data.shape[0]}")
    print(f"  Total rows:  {df_combined.shape[0]}")

    return df_combined


def save_to_databricks(
    df_combined: pd.DataFrame,
    output_volume_path: str,
    obj_db: DatabricksAPI
):
    """
    Save combined file to Databricks volume.

    Parameters
    ----------
    df_combined : pd.DataFrame
        Combined header + data
    output_volume_path : str
        Databricks volume path
    obj_db : DatabricksAPI
        Databricks API object
    """
    print(f"\n{'='*80}")
    print(f"SAVING TO DATABRICKS")
    print(f"{'='*80}")
    print(f"Volume path: {output_volume_path}")

    obj_db.write_db_obj(
        df=df_combined,
        volume_path=output_volume_path,
        sep='\t',
        overwrite=True,
        dict_database_table_info=None  # Volume only
    )

    print(f"✓ Saved to Databricks: {output_volume_path}")
    print(f"  Shape: {df_combined.shape}")


def save_to_local(
    df_combined: pd.DataFrame,
    output_local_path: str
):
    """
    Save combined file to local filesystem.

    Parameters
    ----------
    df_combined : pd.DataFrame
        Combined header + data
    output_local_path : str
        Local filesystem path
    """
    print(f"\n{'='*80}")
    print(f"SAVING TO LOCAL FILESYSTEM")
    print(f"{'='*80}")
    print(f"Local path: {output_local_path}")

    # Ensure directory exists
    output_dir = os.path.dirname(output_local_path)
    if output_dir and not os.path.exists(output_dir):
        print(f"  Creating directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

    # Save to local file
    # cBioPortal format: no column names row (header is in first 4 rows)
    df_combined.to_csv(
        output_local_path,
        sep='\t',
        index=False,
        header=False  # No column names row
    )

    print(f"✓ Saved to local filesystem: {output_local_path}")
    print(f"  Shape: {df_combined.shape}")

    # Verify file exists
    if os.path.exists(output_local_path):
        file_size = os.path.getsize(output_local_path)
        print(f"  File size: {file_size:,} bytes")
    else:
        print(f"  ⚠ WARNING: File not found after save!")


def main():
    parser = argparse.ArgumentParser(
        description="Combine header and data into final cBioPortal file"
    )
    parser.add_argument(
        "--header_volume_path",
        required=True,
        help="Databricks volume path to header file from create_summary_header.py"
    )
    parser.add_argument(
        "--data_volume_path",
        required=True,
        help="Databricks volume path to merged data from merge_intermediate_summaries.py"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--output_volume_path",
        required=True,
        help="Output Databricks volume path for final combined file"
    )
    parser.add_argument(
        "--output_local_path",
        required=True,
        help="Output local filesystem path for final combined file"
    )

    args = parser.parse_args()

    print(f"\n{'#'*80}")
    print(f"# HEADER + DATA COMBINER")
    print(f"{'#'*80}")
    print(f"Header (Databricks): {args.header_volume_path}")
    print(f"Data (Databricks):   {args.data_volume_path}")
    print(f"Output (Databricks): {args.output_volume_path}")
    print(f"Output (Local):      {args.output_local_path}")
    print(f"{'#'*80}\n")

    # Initialize Databricks API
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load tall-format header from Databricks
    print(f"Loading header from Databricks: {args.header_volume_path}")
    df_header_tall = obj_db.read_db_obj(volume_path=args.header_volume_path, sep='\t')
    print(f"  Header loaded: {df_header_tall.shape}")
    print(f"  Columns: {list(df_header_tall.columns)}")

    # Transpose to wide format
    df_header_wide = transpose_header_to_wide(df_header_tall)

    # Load data from Databricks
    print(f"\nLoading data from Databricks: {args.data_volume_path}")
    df_data = obj_db.read_db_obj(volume_path=args.data_volume_path, sep='\t')
    print(f"  Data loaded: {df_data.shape}")

    # Combine header + data
    df_combined = combine_header_and_data(df_header_wide, df_data)

    print(df_combined.head())

    # Save to Databricks volume
    save_to_databricks(
        df_combined=df_combined,
        output_volume_path=args.output_volume_path,
        obj_db=obj_db
    )

    # Save to local filesystem
    save_to_local(
        df_combined=df_combined,
        output_local_path=args.output_local_path
    )

    print(f"\n{'#'*80}")
    print(f"# COMBINING COMPLETE")
    print(f"{'#'*80}")
    print(f"Databricks: {args.output_volume_path}")
    print(f"Local:      {args.output_local_path}")
    print(f"Shape:      {df_combined.shape[0]} rows × {df_combined.shape[1]} columns")
    print(f"            (5 header rows + {df_data.shape[0]} data rows)")
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()

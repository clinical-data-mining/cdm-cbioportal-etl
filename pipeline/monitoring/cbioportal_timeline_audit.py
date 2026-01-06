"""
cbioportal_timeline_audit.py

Analyzes cBioPortal timeline files in Databricks volumes to:
- Quantify available data and coverage
- Compute last dates to show data recency
- Compare with reference sample lists (i.e., Overlapping Patients/Samples)
- Generate summary report and save to Databricks

Based on sandbox/cbioportal_debugging/cbioportal_deid_timeline_audit.ipynb
"""
import os
import sys
import argparse
from pathlib import Path

import pandas as pd
from msk_cdm.databricks import DatabricksAPI


# List of timeline file names (without path)
TIMELINE_FILE_NAMES = [
    "data_timeline_treatment_phi.tsv",
    "data_timeline_surgery_phi.tsv",
    "data_timeline_radiation_phi.tsv",
    "data_timeline_specimen_phi.tsv",
    "data_timeline_specimen_surgery_phi.tsv",
    "data_timeline_diagnosis_phi.tsv",
    "data_timeline_follow_up_phi.tsv",
    "data_timeline_progression_phi.tsv",
    "data_timeline_cancer_presence_phi.tsv",
    "data_timeline_tumor_sites_phi.tsv",
    "data_timeline_cea_labs_phi.tsv",
    "data_timeline_ca_125_labs_phi.tsv",
    "data_timeline_ca_15-3_labs_phi.tsv",
    "data_timeline_ca_19-9_labs_phi.tsv",
    "data_timeline_psa_labs_phi.tsv",
    "data_timeline_tsh_labs_phi.tsv",
    "data_timeline_bmi_phi.tsv",
    "data_timeline_ecog_kps_phi.tsv",
    "data_timeline_mmr_phi.tsv",
    "data_timeline_pdl1_phi.tsv",
    "data_timeline_gleason_phi.tsv",
]


def analyze_databricks_timeline_files(fname_dbx, volume_file_paths, reference_file_path):
    """
    Analyze PHI timeline files in Databricks volumes for data availability and recency.

    Based on report_deidentification_stats from pipeline/timeline/cbioportal_timeline_deidentify.py
    and analyze_txt_files from check_flat_files.ipynb.

    NOTE: These are PHI files that already have re-identified dates (e.g., START_DATE_FORMATTED_FIXED)

    Parameters
    ----------
    fname_dbx : str
        Path to Databricks environment file
    volume_file_paths : list
        List of full volume paths to timeline files
    reference_file_path : str
        Path to reference sample list file (e.g., data_clinical_sample.txt)

    Returns
    -------
    dict
        Dictionary with filename as key and analysis results as value
    """
    results = {}

    # Initialize Databricks connection
    obj_dbx = DatabricksAPI(fname_databricks_env=fname_dbx)

    # Load reference file from Databricks volume
    try:
        ref_df = obj_dbx.read_db_obj(volume_path=reference_file_path, sep='\t')
        ref_patients = set(ref_df['PATIENT_ID'].dropna()) if 'PATIENT_ID' in ref_df.columns else set()
        ref_samples = set(ref_df['SAMPLE_ID'].dropna()) if 'SAMPLE_ID' in ref_df.columns else set()
        print(f"Reference file loaded: {len(ref_patients)} unique patients, {len(ref_samples)} unique samples\n")
    except Exception as e:
        print(f"Error loading reference file: {e}")
        return results

    print(f"Analyzing {len(volume_file_paths)} Databricks volume files\n")

    for volume_path in volume_file_paths:
        filename = volume_path.split('/')[-1]
        print(f"{'='*80}")
        print(f"Analyzing: {filename}")
        print(f"{'='*80}")

        try:
            # Load file from Databricks
            df = obj_dbx.read_db_obj(volume_path=volume_path, sep='\t')

            # Determine if this is a timeline file
            is_timeline = "timeline" in filename.lower()

            # Get patient and sample sets
            file_patients = set(df['PATIENT_ID'].dropna()) if 'PATIENT_ID' in df.columns else set()
            file_samples = set(df['SAMPLE_ID'].dropna()) if 'SAMPLE_ID' in df.columns else set()

            # Calculate overlaps
            patient_overlap = file_patients.intersection(ref_patients) if ref_patients else set()
            patient_not_in_ref = file_patients - ref_patients if ref_patients else file_patients
            patient_not_in_file = ref_patients - file_patients if ref_patients else set()

            sample_overlap = file_samples.intersection(ref_samples) if ref_samples else set()
            sample_not_in_ref = file_samples - ref_samples if ref_samples else file_samples
            sample_not_in_file = ref_samples - file_samples if ref_samples else set()

            # Calculate percentages
            patient_overlap_pct = (len(patient_overlap) / len(ref_patients) * 100) if ref_patients else 0
            sample_overlap_pct = (len(sample_overlap) / len(ref_samples) * 100) if ref_samples else 0

            # Calculate missing data statistics (similar to report_deidentification_stats)
            ids_with_missing_mrns = df.loc[df['MRN'].isnull(), 'PATIENT_ID'].drop_duplicates() if 'MRN' in df.columns else pd.Series()
            ids_with_missing_seq_date = df.loc[df['DTE_TUMOR_SEQUENCING'].isnull(), 'PATIENT_ID'].drop_duplicates() if 'DTE_TUMOR_SEQUENCING' in df.columns else pd.Series()
            ids_with_missing_start_date = df.loc[df['START_DATE'].isnull(), 'PATIENT_ID'].drop_duplicates() if 'START_DATE' in df.columns else pd.Series()

            # Count null values
            null_counts = df.isnull().sum().to_dict()
            total_rows = len(df)

            # Data completeness
            data_completeness = {}
            for col in df.columns:
                non_null_count = df[col].notna().sum()
                data_completeness[col] = (non_null_count / total_rows * 100) if total_rows > 0 else 0

            # Initialize result dictionary
            file_results = {
                "volume_path": volume_path,
                "total_rows": total_rows,
                "total_columns": len(df.columns),
                "is_timeline": is_timeline,
                "has_sample_id": 'SAMPLE_ID' in df.columns,
                "unique_patients": len(file_patients),
                "unique_samples": len(file_samples),
                "patient_overlap_count": len(patient_overlap),
                "patient_overlap_pct": round(patient_overlap_pct, 2),
                "patients_not_in_ref": len(patient_not_in_ref),
                "patients_not_in_file": len(patient_not_in_file),
                "sample_overlap_count": len(sample_overlap),
                "sample_overlap_pct": round(sample_overlap_pct, 2),
                "samples_not_in_ref": len(sample_not_in_ref),
                "samples_not_in_file": len(sample_not_in_file),
                "patients_with_missing_mrn": len(ids_with_missing_mrns),
                "patients_with_missing_seq_date": len(ids_with_missing_seq_date),
                "patients_with_missing_start_date": len(ids_with_missing_start_date),
                "null_counts": null_counts,
                "data_completeness_pct": {k: round(v, 2) for k, v in data_completeness.items()},
                "columns": list(df.columns)
            }

            # For timeline files, find the last date from re-identified date columns
            if is_timeline:
                try:
                    # Look for date columns in priority order (these are already re-identified in PHI files)
                    date_col_candidates = ['START_DATE_FORMATTED_FIXED', 'START_DATE_FORMATTED', 'START_DATE']
                    date_col = None
                    for col in date_col_candidates:
                        if col in df.columns:
                            date_col = col
                            break

                    if date_col:
                        # Convert to datetime
                        df[f'{date_col}_DT'] = pd.to_datetime(df[date_col], errors='coerce')

                        # Get max date and calculate recency
                        last_date = df[f'{date_col}_DT'].max()
                        if pd.notna(last_date):
                            last_date_str = last_date.strftime('%Y-%m-%d')
                            days_since_last = (pd.Timestamp.today() - last_date).days
                            file_results["last_date"] = last_date_str
                            file_results["days_since_last_date"] = days_since_last
                            file_results["date_column_used"] = date_col
                            print(f"Last date in timeline ({date_col}): {last_date_str} ({days_since_last} days ago)")
                        else:
                            file_results["last_date"] = "No valid dates"
                            file_results["days_since_last_date"] = None
                    else:
                        file_results["last_date"] = "No date column found"
                        file_results["days_since_last_date"] = None

                except Exception as e:
                    print(f"Warning: Could not compute last date: {e}")
                    file_results["last_date"] = f"Error: {e}"
                    file_results["days_since_last_date"] = None

            # Print summary
            print(f"Rows: {total_rows:,}")
            print(f"Columns: {len(df.columns)}")
            print(f"Unique Patients: {len(file_patients):,}")
            if file_results['has_sample_id']:
                print(f"Unique Samples: {len(file_samples):,}")

            print(f"\nPatient Overlap:")
            print(f"  In both file and reference: {len(patient_overlap):,} ({patient_overlap_pct:.1f}%)")
            print(f"  Only in file: {len(patient_not_in_ref):,}")
            print(f"  Only in reference: {len(patient_not_in_file):,}")

            if file_results['has_sample_id']:
                print(f"\nSample Overlap:")
                print(f"  In both file and reference: {len(sample_overlap):,} ({sample_overlap_pct:.1f}%)")
                print(f"  Only in file: {len(sample_not_in_ref):,}")
                print(f"  Only in reference: {len(sample_not_in_file):,}")

            print(f"\nData Quality:")
            print(f"  Patients with missing MRN: {len(ids_with_missing_mrns)}")
            print(f"  Patients with missing SEQ DATE: {len(ids_with_missing_seq_date)}")
            print(f"  Patients with missing START_DATE: {len(ids_with_missing_start_date)}")

            # Print significant null counts
            print(f"\nNull Value Counts (>0):")
            for col, null_count in null_counts.items():
                if null_count > 0 and null_count < total_rows:  # Don't show completely empty columns
                    pct = (null_count / total_rows) * 100
                    print(f"  {col}: {null_count:,} ({pct:.1f}%)")

            print()  # Empty line for spacing

            results[filename] = file_results

        except Exception as e:
            print(f"ERROR: {str(e)}\n")
            results[filename] = {"error": str(e)}

    return results


def create_summary_dataframe(results):
    """
    Create summary DataFrame from analysis results.

    Parameters
    ----------
    results : dict
        Dictionary returned by analyze_databricks_timeline_files

    Returns
    -------
    pd.DataFrame
        Summary table with key metrics
    """
    summary_data = []
    for filename, data in results.items():
        if "error" not in data:
            row = {
                'File': filename.replace('_phi.tsv', '').replace('data_timeline_', ''),
                'Rows': data['total_rows'],
                'Patients': data['unique_patients'],
                'Samples': data['unique_samples'] if data['has_sample_id'] else 'N/A',
                'Patient_Coverage_Pct': data['patient_overlap_pct'],
                'Last_Date': data.get('last_date', 'N/A'),
                'Days_Since': data.get('days_since_last_date', 'N/A')
            }
            summary_data.append(row)

    df_summary = pd.DataFrame(summary_data)
    return df_summary


def run_timeline_audit(fname_dbx, cohort_name, reference_file_path, volume_base_path, output_volume_path, create_table=True):
    """
    Run timeline audit and save summary to Databricks.

    Parameters
    ----------
    fname_dbx : str
        Path to Databricks environment file
    cohort_name : str
        Cohort name (e.g., 'mskimpact')
    reference_file_path : str
        Path to reference sample list file
    volume_base_path : str
        Base path for timeline files (e.g., '/Volumes/cdsi_prod/cdm_idbw_impact_pipeline_prod/epic_testing/cbioportal')
    output_volume_path : str
        Full path to save summary file in Databricks volume
    create_table : bool
        Whether to create a Databricks table from the summary file
    """
    print("=" * 80)
    print("CBIOPORTAL TIMELINE AUDIT")
    print("=" * 80)
    print(f"Cohort: {cohort_name}")
    print(f"Reference file: {reference_file_path}")
    print(f"Volume base path: {volume_base_path}")
    print(f"Output path: {output_volume_path}")
    print("=" * 80)
    print()

    # Build full paths to timeline files
    volume_file_paths = [
        f"{volume_base_path}/{cohort_name}/{filename}"
        for filename in TIMELINE_FILE_NAMES
    ]

    # Run analysis
    results = analyze_databricks_timeline_files(
        fname_dbx=fname_dbx,
        volume_file_paths=volume_file_paths,
        reference_file_path=reference_file_path
    )

    # Create summary DataFrame
    df_summary = create_summary_dataframe(results)

    # Print summary table
    print(f"\n{'='*120}")
    print("SUMMARY TABLE")
    print(f"{'='*120}")
    print(df_summary.to_string(index=False))
    print()

    # Print data quality issues
    print(f"\n{'='*80}")
    print("DATA QUALITY ISSUES")
    print(f"{'='*80}")

    quality_issues_found = False
    for filename, data in results.items():
        if 'error' not in data:
            issues = []

            if data['patients_with_missing_mrn'] > 0:
                issues.append(f"{data['patients_with_missing_mrn']} patients with missing MRN")

            if data['patients_with_missing_seq_date'] > 0:
                issues.append(f"{data['patients_with_missing_seq_date']} patients with missing SEQ DATE")

            if data['patients_with_missing_start_date'] > 0:
                issues.append(f"{data['patients_with_missing_start_date']} patients with missing START_DATE")

            if data['patients_not_in_ref'] > 100:
                issues.append(f"{data['patients_not_in_ref']} patients not in reference")

            if issues:
                quality_issues_found = True
                print(f"\n{filename}:")
                for issue in issues:
                    print(f"  - {issue}")

    if not quality_issues_found:
        print("\nNo significant data quality issues found!")

    print()

    # Save to Databricks
    print("=" * 80)
    print("SAVING SUMMARY TO DATABRICKS")
    print("=" * 80)
    print(f"Volume path: {output_volume_path}")

    obj_dbx = DatabricksAPI(fname_databricks_env=fname_dbx)

    # Parse table info from output path if creating table
    dict_database_table_info = None
    if create_table:
        # Extract catalog, schema from volume path
        # Example: /Volumes/cdsi_prod/cdm_idbw_impact_pipeline_prod/epic_testing/timeline_audit_summary.tsv
        parts = output_volume_path.split('/')
        if len(parts) >= 4 and parts[1] == 'Volumes':
            catalog = parts[2]
            schema = parts[3]
            table = 'timeline_audit_summary'

            dict_database_table_info = {
                'catalog': catalog,
                'schema': schema,
                'volume_path': output_volume_path,
                'table': table,
                'sep': '\t'
            }

            print(f"Creating table: {catalog}.{schema}.{table}")

    # Write to Databricks
    obj_dbx.write_db_obj(
        df=df_summary,
        volume_path=output_volume_path,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    print("✓ Summary saved successfully!")
    print("=" * 80)
    print()

    return df_summary, results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Audit cBioPortal timeline files and generate summary report"
    )
    parser.add_argument(
        "--fname_dbx",
        action="store",
        dest="fname_dbx",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--cohort_name",
        action="store",
        dest="cohort_name",
        required=True,
        help="Cohort name (e.g., mskimpact, mskimpact_heme)"
    )
    parser.add_argument(
        "--reference_file",
        action="store",
        dest="reference_file",
        required=True,
        help="Path to reference sample list file (data_clinical_sample.txt)"
    )
    parser.add_argument(
        "--volume_base_path",
        action="store",
        dest="volume_base_path",
        required=True,
        help="Base path for timeline files in Databricks (e.g., /Volumes/cdsi_prod/cdm_idbw_impact_pipeline_prod/epic_testing/cbioportal)"
    )
    parser.add_argument(
        "--output_volume_path",
        action="store",
        dest="output_volume_path",
        required=True,
        help="Full path to save summary file in Databricks volume"
    )
    parser.add_argument(
        "--create_table",
        action="store_true",
        dest="create_table",
        default=True,
        help="Create Databricks table from summary file (default: True)"
    )

    args = parser.parse_args()

    df_summary, results = run_timeline_audit(
        fname_dbx=args.fname_dbx,
        cohort_name=args.cohort_name,
        reference_file_path=args.reference_file,
        volume_base_path=args.volume_base_path,
        output_volume_path=args.output_volume_path,
        create_table=args.create_table
    )

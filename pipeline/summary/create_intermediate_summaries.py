"""
create_intermediate_summaries.py

Script 1 of 4: Create intermediate summary files from YAML configurations.

This script:
1. Loads all YAML config files from a directory
2. Filters by patient_or_sample level
3. Processes each config to create intermediate TSV files (data only, no headers)
4. Saves intermediates to Databricks volumes
5. Creates a manifest CSV tracking all outputs

Usage:
    python create_intermediate_summaries.py \
        --config_dir config/summaries \
        --databricks_env /path/to/databricks.env \
        --template /path/to/local/template.txt \
        --patient_or_sample patient \
        --production_or_test test \
        --cohort mskimpact \
        --output_manifest /Volumes/.../manifest_patient.csv
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import glob
import pandas as pd
from typing import List, Dict

from lib.summary.summary_config_processor import SummaryConfigProcessor
from lib.utils import get_anchor_dates
from msk_cdm.databricks import DatabricksAPI


def load_template_from_local(fname_template: str, patient_or_sample: str) -> pd.DataFrame:
    """
    Load template from local filesystem and extract ID column.

    Parameters
    ----------
    fname_template : str
        Local filesystem path to template file
    patient_or_sample : str
        'patient' or 'sample'

    Returns
    -------
    pd.DataFrame
        Template with only ID column (deduplicated)
    """
    print(f"Loading template from local filesystem: {fname_template}")

    # Read from local file
    df_template = pd.read_csv(fname_template, sep='\t', dtype=str)

    # Determine ID column
    id_column = 'PATIENT_ID' if patient_or_sample == 'patient' else 'SAMPLE_ID'

    # Extract ID column
    if id_column in df_template.columns:
        df_template = df_template[[id_column]].copy()

        # Drop duplicates
        original_rows = df_template.shape[0]
        df_template = df_template.drop_duplicates()
        deduplicated_rows = df_template.shape[0]

        if original_rows != deduplicated_rows:
            print(f"  Dropped {original_rows - deduplicated_rows} duplicate rows")

        print(f"  Loaded {deduplicated_rows} unique {id_column} values")
    else:
        raise ValueError(
            f"Template missing {id_column} column. "
            f"Available columns: {list(df_template.columns)}"
        )

    return df_template


def process_all_configs(
    config_dir: str,
    fname_databricks_env: str,
    df_anchor: pd.DataFrame,
    df_template: pd.DataFrame,
    patient_or_sample: str,
    production_or_test: str,
    cohort: str
) -> List[Dict]:
    """
    Process all YAML configs and create intermediate files.

    Parameters
    ----------
    config_dir : str
        Directory containing YAML config files
    fname_databricks_env : str
        Path to Databricks environment file
    df_anchor : pd.DataFrame
        Anchor dates for deidentification
    df_template : pd.DataFrame
        Template with ID column
    patient_or_sample : str
        'patient' or 'sample'
    production_or_test : str
        'production' or 'test'
    cohort : str
        Cohort name

    Returns
    -------
    List[Dict]
        List of manifest entries for successfully processed configs
    """
    # Find all YAML files
    yaml_pattern = os.path.join(config_dir, '*.yaml')
    yaml_files = glob.glob(yaml_pattern)

    print(f"\n{'='*80}")
    print(f"PROCESSING {patient_or_sample.upper()} SUMMARIES")
    print(f"{'='*80}")
    print(f"Config directory: {config_dir}")
    print(f"Found {len(yaml_files)} YAML files")
    print(f"Production/Test: {production_or_test}")
    print(f"Cohort: {cohort}")
    print(f"{'='*80}\n")

    manifest_entries = []
    processed_count = 0
    skipped_count = 0
    error_count = 0

    for yaml_file in sorted(yaml_files):
        yaml_basename = os.path.basename(yaml_file)
        print(f"\n{'-'*80}")
        print(f"Processing: {yaml_basename}")
        print(f"{'-'*80}")

        try:
            # Create processor
            processor = SummaryConfigProcessor(
                fname_yaml_config=yaml_file,
                fname_databricks_env=fname_databricks_env,
                production_or_test=production_or_test,
                cohort=cohort
            )

            # Check if this config matches the patient/sample level we're processing
            config_patient_or_sample = processor.config.get('patient_or_sample', '')
            if config_patient_or_sample != patient_or_sample:
                print(f"  ⊘ Skipping (this is a '{config_patient_or_sample}' summary)")
                skipped_count += 1
                continue

            # Process the summary
            df_data = processor.process_summary(
                df_anchor=df_anchor,
                df_template=df_template
            )

            # Save intermediate file (data only, no headers)
            volume_path = processor.save_intermediate(
                df_data=df_data,
                save_to_table=False  # Intermediates not saved to tables
            )

            # Create manifest entry
            manifest_entry = {
                'summary_id': processor.config['summary_id'],
                'yaml_config_path': os.path.abspath(yaml_file),
                'intermediate_data_path': volume_path,
                'patient_or_sample': patient_or_sample
            }
            manifest_entries.append(manifest_entry)

            processed_count += 1
            print(f"✓ Successfully processed: {processor.config['summary_id']}")

        except Exception as e:
            error_count += 1
            print(f"✗ ERROR processing {yaml_basename}:")
            print(f"  {str(e)}")
            import traceback
            traceback.print_exc()
            continue

    print(f"\n{'='*80}")
    print(f"PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully processed: {processed_count}")
    print(f"Skipped (wrong level):  {skipped_count}")
    print(f"Errors:                 {error_count}")
    print(f"{'='*80}\n")

    return manifest_entries


def save_manifest(
    manifest_entries: List[Dict],
    output_manifest: str,
    obj_db: DatabricksAPI
):
    """
    Save manifest CSV to Databricks volume.

    Parameters
    ----------
    manifest_entries : List[Dict]
        List of manifest entries
    output_manifest : str
        Databricks volume path for manifest
    obj_db : DatabricksAPI
        Databricks API object
    """
    print(f"Creating manifest with {len(manifest_entries)} entries")

    # Create DataFrame
    df_manifest = pd.DataFrame(manifest_entries)

    # Save to Databricks volume
    print(f"Saving manifest to: {output_manifest}")
    obj_db.write_db_obj(
        df=df_manifest,
        volume_path=output_manifest,
        sep=',',
        overwrite=True,
        dict_database_table_info=None
    )

    print(f"✓ Manifest saved: {output_manifest}")
    print(f"  Columns: {list(df_manifest.columns)}")
    print(f"  Rows: {len(df_manifest)}")


def main():
    parser = argparse.ArgumentParser(
        description="Create intermediate summary files from YAML configurations"
    )
    parser.add_argument(
        "--config_dir",
        required=True,
        help="Directory containing YAML configuration files"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--template",
        required=True,
        help="Local filesystem path to template file"
    )
    parser.add_argument(
        "--patient_or_sample",
        required=True,
        choices=['patient', 'sample'],
        help="Process patient or sample level summaries"
    )
    parser.add_argument(
        "--production_or_test",
        required=True,
        choices=['production', 'test'],
        help="Use production or test source tables"
    )
    parser.add_argument(
        "--cohort",
        required=True,
        help="Cohort name (e.g., mskimpact, mskaccess)"
    )
    parser.add_argument(
        "--output_manifest",
        required=True,
        help="Databricks volume path where manifest CSV should be saved"
    )

    args = parser.parse_args()

    print(f"\n{'#'*80}")
    print(f"# INTERMEDIATE SUMMARY CREATOR")
    print(f"{'#'*80}")
    print(f"Config directory:    {args.config_dir}")
    print(f"Template:            {args.template}")
    print(f"Patient/Sample:      {args.patient_or_sample}")
    print(f"Production/Test:     {args.production_or_test}")
    print(f"Cohort:              {args.cohort}")
    print(f"Output manifest:     {args.output_manifest}")
    print(f"{'#'*80}\n")

    # Initialize Databricks API
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load anchor dates
    print("Loading anchor dates for deidentification...")
    df_anchor = get_anchor_dates(args.databricks_env)
    print(f"  Loaded {df_anchor.shape[0]} anchor dates\n")

    # Load template from local filesystem
    df_template = load_template_from_local(
        fname_template=args.template,
        patient_or_sample=args.patient_or_sample
    )
    print()

    # Process all configs
    manifest_entries = process_all_configs(
        config_dir=args.config_dir,
        fname_databricks_env=args.databricks_env,
        df_anchor=df_anchor,
        df_template=df_template,
        patient_or_sample=args.patient_or_sample,
        production_or_test=args.production_or_test,
        cohort=args.cohort
    )

    # Save manifest
    if manifest_entries:
        save_manifest(
            manifest_entries=manifest_entries,
            output_manifest=args.output_manifest,
            obj_db=obj_db
        )
    else:
        print("⚠ WARNING: No summaries were successfully processed. Manifest not created.")
        sys.exit(1)

    print(f"\n{'#'*80}")
    print(f"# INTERMEDIATE SUMMARY CREATION COMPLETE")
    print(f"{'#'*80}")
    print(f"Created {len(manifest_entries)} intermediate files")
    print(f"Manifest: {args.output_manifest}")
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()

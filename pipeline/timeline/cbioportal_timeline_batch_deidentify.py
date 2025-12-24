"""
cbioportal_timeline_batch_deidentify.py

Batch wrapper script to execute timeline deidentification for all timeline files.
Reads timeline configurations from YAML files in config/timelines/ directory.
"""
import os
import sys
import argparse
import subprocess
from pathlib import Path
import yaml


def load_timeline_configs(config_dir, production_or_test):
    """
    Load all timeline YAML configurations from directory.

    Parameters
    ----------
    config_dir : str
        Directory containing timeline YAML config files
    production_or_test : str
        'production' or 'test' - determines which source_table to use

    Returns
    -------
    list of dict
        List of timeline configurations with keys:
        - timeline_id
        - source_table
        - output_filename
        - columns (list)
        - patient_or_sample
    """
    config_path = Path(config_dir)
    if not config_path.exists():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")

    yaml_files = sorted(config_path.glob("*.yaml"))

    if not yaml_files:
        raise ValueError(f"No YAML files found in {config_dir}")

    configs = []
    for yaml_file in yaml_files:
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)

            # Choose source table based on production_or_test
            if production_or_test == 'production':
                source_table = config['source_table_prod']
            else:
                source_table = config['source_table_dev']

            # Extract ETL-relevant fields
            etl_config = {
                'timeline_id': config['timeline_id'],
                'source_table': source_table,
                'output_filename': config['output_filename'],
                'columns': list(config['columns'].keys()),  # Extract column names
                'patient_or_sample': config['patient_or_sample']
            }
            configs.append(etl_config)

    print(f"Loaded {len(configs)} timeline configurations from {config_dir}")
    return configs


def run_timeline_deidentification(config_dir, production_or_test, fname_dbx, anchor_dates, fname_sample, volume_base_path, gpfs_output_path, cohort_name):
    """
    Run timeline deidentification for all configured timeline files.

    Parameters
    ----------
    config_dir : str
        Directory containing timeline YAML config files
    production_or_test : str
        'production' or 'test' - determines which source_table to use
    fname_dbx : str
        Path to Databricks environment file
    anchor_dates : str
        Databricks table name for anchor dates (e.g., catalog.schema.table)
    fname_sample : str
        Path to sample list file
    volume_base_path : str
        Base path for Databricks volume output (without cohort name)
    gpfs_output_path : str
        Base path for GPFS output files
    cohort_name : str
        Name of the cohort (e.g., 'mskimpact', 'mskimpact_heme')
    """

    # Load timeline configurations from YAML files
    timeline_configs = load_timeline_configs(config_dir, production_or_test)

    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    deidentify_script = script_dir / "cbioportal_timeline_deidentify.py"

    if not deidentify_script.exists():
        raise FileNotFoundError(f"Deidentification script not found at {deidentify_script}")

    # Build full volume base path with cohort name
    volume_path_full = f"{volume_base_path}/{cohort_name}"

    print("=" * 80)
    print("TIMELINE BATCH DEIDENTIFICATION")
    print("=" * 80)
    print(f"Config directory: {config_dir}")
    print(f"Production/Test: {production_or_test}")
    print(f"Total timeline files to process: {len(timeline_configs)}")
    print(f"Databricks env: {fname_dbx}")
    print(f"Anchor dates: {anchor_dates}")
    print(f"Sample list: {fname_sample}")
    print(f"Volume base path: {volume_path_full}")
    print(f"GPFS output path: {gpfs_output_path}")
    print(f"Cohort: {cohort_name}")
    print("=" * 80)
    print()

    successful = []
    failed = []

    for idx, config in enumerate(timeline_configs, 1):
        timeline_id = config['timeline_id']
        source_table = config['source_table']
        output_filename = config['output_filename']
        columns = config['columns']  # This is a list
        patient_or_sample = config['patient_or_sample']

        print(f"\n[{idx}/{len(timeline_configs)}] Processing: {timeline_id}")
        print("-" * 80)

        # Build paths
        fname_output_volume = f"{volume_path_full}/{output_filename}_phi.tsv"
        fname_output_gpfs = f"{gpfs_output_path}/{output_filename}.txt"

        # Convert columns list to comma-separated string
        columns_str = ",".join(columns)

        # Build command arguments
        cmd = [
            "python",
            str(deidentify_script),
            f"--fname_dbx={fname_dbx}",
            f"--fname_deid={anchor_dates}",
            f"--fname_timeline={source_table}",
            f"--fname_sample={fname_sample}",
            f"--fname_output_volume={fname_output_volume}",
            f"--fname_output_gpfs={fname_output_gpfs}",
            f"--columns_cbio={columns_str}",
            f"--merge_level={patient_or_sample}"  # Always pass merge_level
        ]

        print(f"Table: {source_table}")
        print(f"Output (PHI): {fname_output_volume}")
        print(f"Output (DEID): {fname_output_gpfs}")
        print(f"Merge level: {patient_or_sample}")
        print()

        try:
            # Run the deidentification script
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            successful.append(timeline_id)
            print(f"✓ Successfully processed: {timeline_id}")
        except subprocess.CalledProcessError as e:
            print(f"✗ FAILED to process: {timeline_id}")
            print(f"Error code: {e.returncode}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            failed.append(timeline_id)
        except Exception as e:
            print(f"✗ FAILED to process: {timeline_id}")
            print(f"Error: {str(e)}")
            failed.append(timeline_id)

        print("-" * 80)

    # Print summary
    print("\n" + "=" * 80)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Total processed: {len(timeline_configs)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        print(f"\nSuccessful files ({len(successful)}):")
        for timeline_id in successful:
            print(f"  ✓ {timeline_id}")

    if failed:
        print(f"\nFailed files ({len(failed)}):")
        for timeline_id in failed:
            print(f"  ✗ {timeline_id}")
        print("\n⚠ WARNING: Some timeline files failed to process")
        sys.exit(1)
    else:
        print("\n✓ All timeline files processed successfully!")

    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch deidentification of all cBioPortal timeline files"
    )
    parser.add_argument(
        "--config_dir",
        action="store",
        dest="config_dir",
        required=True,
        help="Directory containing timeline YAML config files (e.g., config/timelines)"
    )
    parser.add_argument(
        "--production_or_test",
        action="store",
        dest="production_or_test",
        required=True,
        choices=["production", "test"],
        help="Use production or test source tables"
    )
    parser.add_argument(
        "--fname_dbx",
        action="store",
        dest="fname_dbx",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--anchor_dates",
        action="store",
        dest="anchor_dates",
        required=True,
        help="Databricks table name for anchor dates (e.g., catalog.schema.table)"
    )
    parser.add_argument(
        "--fname_sample",
        action="store",
        dest="fname_sample",
        required=True,
        help="Path to sample list file (data_clinical_sample.txt)"
    )
    parser.add_argument(
        "--volume_base_path",
        action="store",
        dest="volume_base_path",
        required=True,
        help="Base path for Databricks volume output (e.g., /Volumes/cdsi_prod/cdm_idbw_impact_pipeline_prod/epic_testing/cbioportal)"
    )
    parser.add_argument(
        "--gpfs_output_path",
        action="store",
        dest="gpfs_output_path",
        required=True,
        help="Base path for GPFS deidentified output files"
    )
    parser.add_argument(
        "--cohort_name",
        action="store",
        dest="cohort_name",
        required=True,
        help="Cohort name (e.g., mskimpact, mskimpact_heme, mskaccess, mskarcher)"
    )

    args = parser.parse_args()

    run_timeline_deidentification(
        config_dir=args.config_dir,
        production_or_test=args.production_or_test,
        fname_dbx=args.fname_dbx,
        anchor_dates=args.anchor_dates,
        fname_sample=args.fname_sample,
        volume_base_path=args.volume_base_path,
        gpfs_output_path=args.gpfs_output_path,
        cohort_name=args.cohort_name
    )

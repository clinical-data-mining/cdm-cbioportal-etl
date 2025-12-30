"""
wrapper_modular_summary_pipeline.py

Wrapper script to orchestrate the complete modular summary pipeline.

This wrapper runs all 4 scripts in sequence:
1. create_intermediate_summaries.py - Create intermediate TSV files from YAMLs
2. merge_intermediate_summaries.py - Merge intermediates into single data file
3. create_summary_header.py - Create header metadata from YAMLs
4. combine_header_and_data.py - Combine header + data into final file

Usage:
    python pipeline/summary/wrapper_modular_summary_pipeline.py \
        --config_dir config/summaries \
        --databricks_env /gpfs/mindphidata/fongc2/databricks_env_prod.txt \
        --anchor_dates cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates \
        --template_patient /gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_patient.txt \
        --template_sample /gpfs/mindphidata/cdm_repos/prod/data/impact-data/mskimpact/data_clinical_sample.txt \
        --output_dir_databricks /Volumes/cdsi_eng_phi/cdm_eng_cbioportal_etl/cdm_eng_cbioportal_etl_volume_dev/cbioportal \
        --output_dir_local /gpfs/mindphidata/cdm_repos/dev/data/cdm-data/mskimpact \
        --catalog cdsi_eng_phi \
        --schema cdm_eng_cbioportal_etl \
        --production_or_test test \
        --cohort mskimpact \
        --patient \
        --sample

File Structure:
    Databricks Volume:
    /Volumes/.../cbioportal/
    ├── intermediate_files/
    │   └── {cohort}/
    │       ├── {summary_id}_patient.txt
    │       └── manifest_patient.csv
    └── {cohort}/
        ├── data_clinical_patient_data.txt
        ├── data_clinical_patient_header.txt
        └── data_clinical_patient.txt

    Local Filesystem:
    /gpfs/.../cdm-data/{cohort}/
    ├── data_clinical_patient.txt
    └── data_clinical_sample.txt
"""
import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd: list, description: str):
    """
    Run a shell command and handle errors.

    Parameters
    ----------
    cmd : list
        Command and arguments as a list
    description : str
        Description of what this command does
    """
    print(f"\n{'='*80}")
    print(f"RUNNING: {description}")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}\n")

    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode != 0:
        print(f"\n✗ ERROR: {description} failed with exit code {result.returncode}")
        sys.exit(1)

    print(f"\n✓ {description} completed successfully")


def run_patient_pipeline(args):
    """Run complete pipeline for patient summaries."""
    print(f"\n{'#'*80}")
    print(f"# PATIENT SUMMARY PIPELINE")
    print(f"{'#'*80}\n")

    # Define paths
    # Intermediates: {base}/intermediate_files/{cohort}/
    # Finals: {base}/{cohort}/
    manifest_path = f"{args.output_dir_databricks}/intermediate_files/{args.cohort}/manifest_patient.csv"
    data_path = f"{args.output_dir_databricks}/{args.cohort}/data_clinical_patient_data.txt"
    header_path = f"{args.output_dir_databricks}/{args.cohort}/data_clinical_patient_header.txt"
    final_volume_path = f"{args.output_dir_databricks}/{args.cohort}/data_clinical_patient.txt"
    final_local_path = f"{args.output_dir_local}/data_clinical_patient.txt"

    # Step 1: Create intermediates
    cmd_step1 = [
        "python", "pipeline/summary/create_intermediate_summaries.py",
        "--config_dir", args.config_dir,
        "--databricks_env", args.databricks_env,
        "--anchor_dates", args.anchor_dates,
        "--template", args.template_patient,
        "--patient_or_sample", "patient",
        "--production_or_test", args.production_or_test,
        "--cohort", args.cohort,
        "--output_manifest", manifest_path
    ]
    run_command(cmd_step1, "Step 1: Create intermediate patient summaries")

    # Step 2: Merge intermediates
    cmd_step2 = [
        "python", "pipeline/summary/merge_intermediate_summaries.py",
        "--manifest", manifest_path,
        "--databricks_env", args.databricks_env,
        "--template", args.template_patient,
        "--patient_or_sample", "patient",
        "--output_volume_path", data_path,
        "--output_catalog", args.catalog,
        "--output_schema", args.schema,
        "--output_table", "data_clinical_patient"
    ]
    run_command(cmd_step2, "Step 2: Merge patient intermediates")

    # Step 3: Create header
    cmd_step3 = [
        "python", "pipeline/summary/create_summary_header.py",
        "--manifest", manifest_path,
        "--databricks_env", args.databricks_env,
        "--merged_data_path", data_path,
        "--patient_or_sample", "patient",
        "--output_volume_path", header_path,
        "--output_catalog", args.catalog,
        "--output_schema", args.schema,
        "--output_table", "data_clinical_patient_header"
    ]
    run_command(cmd_step3, "Step 3: Create patient header")

    # Step 4: Combine header and data
    cmd_step4 = [
        "python", "pipeline/summary/combine_header_and_data.py",
        "--header_volume_path", header_path,
        "--data_volume_path", data_path,
        "--databricks_env", args.databricks_env,
        "--output_volume_path", final_volume_path,
        "--output_local_path", final_local_path
    ]
    run_command(cmd_step4, "Step 4: Combine patient header and data")

    print(f"\n{'#'*80}")
    print(f"# PATIENT PIPELINE COMPLETE")
    print(f"{'#'*80}")
    print(f"Databricks: {final_volume_path}")
    print(f"Local:      {final_local_path}")
    print(f"{'#'*80}\n")


def run_sample_pipeline(args):
    """Run complete pipeline for sample summaries."""
    print(f"\n{'#'*80}")
    print(f"# SAMPLE SUMMARY PIPELINE")
    print(f"{'#'*80}\n")

    # Define paths
    # Intermediates: {base}/intermediate_files/{cohort}/
    # Finals: {base}/{cohort}/
    manifest_path = f"{args.output_dir_databricks}/intermediate_files/{args.cohort}/manifest_sample.csv"
    data_path = f"{args.output_dir_databricks}/{args.cohort}/data_clinical_sample_data.txt"
    header_path = f"{args.output_dir_databricks}/{args.cohort}/data_clinical_sample_header.txt"
    final_volume_path = f"{args.output_dir_databricks}/{args.cohort}/data_clinical_sample.txt"
    final_local_path = f"{args.output_dir_local}/data_clinical_sample.txt"

    # Step 1: Create intermediates
    cmd_step1 = [
        "python", "pipeline/summary/create_intermediate_summaries.py",
        "--config_dir", args.config_dir,
        "--databricks_env", args.databricks_env,
        "--anchor_dates", args.anchor_dates,
        "--template", args.template_sample,
        "--patient_or_sample", "sample",
        "--production_or_test", args.production_or_test,
        "--cohort", args.cohort,
        "--output_manifest", manifest_path
    ]
    run_command(cmd_step1, "Step 1: Create intermediate sample summaries")

    # Step 2: Merge intermediates
    cmd_step2 = [
        "python", "pipeline/summary/merge_intermediate_summaries.py",
        "--manifest", manifest_path,
        "--databricks_env", args.databricks_env,
        "--template", args.template_sample,
        "--patient_or_sample", "sample",
        "--output_volume_path", data_path,
        "--output_catalog", args.catalog,
        "--output_schema", args.schema,
        "--output_table", "data_clinical_sample"
    ]
    run_command(cmd_step2, "Step 2: Merge sample intermediates")

    # Step 3: Create header
    cmd_step3 = [
        "python", "pipeline/summary/create_summary_header.py",
        "--manifest", manifest_path,
        "--databricks_env", args.databricks_env,
        "--merged_data_path", data_path,
        "--patient_or_sample", "sample",
        "--output_volume_path", header_path,
        "--output_catalog", args.catalog,
        "--output_schema", args.schema,
        "--output_table", "data_clinical_sample_header"
    ]
    run_command(cmd_step3, "Step 3: Create sample header")

    # Step 4: Combine header and data
    cmd_step4 = [
        "python", "pipeline/summary/combine_header_and_data.py",
        "--header_volume_path", header_path,
        "--data_volume_path", data_path,
        "--databricks_env", args.databricks_env,
        "--output_volume_path", final_volume_path,
        "--output_local_path", final_local_path
    ]
    run_command(cmd_step4, "Step 4: Combine sample header and data")

    print(f"\n{'#'*80}")
    print(f"# SAMPLE PIPELINE COMPLETE")
    print(f"{'#'*80}")
    print(f"Databricks: {final_volume_path}")
    print(f"Local:      {final_local_path}")
    print(f"{'#'*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper for modular cBioPortal summary pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Input paths
    parser.add_argument(
        "--config_dir",
        required=True,
        help="Directory containing YAML config files"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--anchor_dates",
        required=True,
        help="Databricks table name for anchor dates (e.g., catalog.schema.table)"
    )
    parser.add_argument(
        "--template_patient",
        help="Local filesystem path to patient template file"
    )
    parser.add_argument(
        "--template_sample",
        help="Local filesystem path to sample template file"
    )

    # Output paths
    parser.add_argument(
        "--output_dir_databricks",
        required=True,
        help="Databricks volume base output directory (e.g., /Volumes/catalog/schema/volume/cbioportal). "
             "Intermediates will be saved to {base}/intermediate_files/{cohort}/, "
             "finals will be saved to {base}/{cohort}/"
    )
    parser.add_argument(
        "--output_dir_local",
        required=True,
        help="Local filesystem output directory for final files (e.g., /gpfs/.../cohort)"
    )

    # Database info
    parser.add_argument(
        "--catalog",
        required=True,
        help="Databricks catalog for output tables"
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Databricks schema for output tables"
    )

    # Config
    parser.add_argument(
        "--production_or_test",
        required=True,
        choices=["production", "test"],
        help="Use production or test tables"
    )
    parser.add_argument(
        "--cohort",
        required=True,
        help="Cohort name (e.g., mskimpact, mskaccess)"
    )

    # Process selection
    parser.add_argument(
        "--patient",
        action="store_true",
        help="Process patient summaries"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Process sample summaries"
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.patient and not args.sample:
        parser.error("At least one of --patient or --sample must be specified")

    if args.patient and not args.template_patient:
        parser.error("--template_patient is required when using --patient")

    if args.sample and not args.template_sample:
        parser.error("--template_sample is required when using --sample")

    # Print configuration
    print(f"\n{'#'*80}")
    print(f"# MODULAR SUMMARY PIPELINE WRAPPER")
    print(f"{'#'*80}")
    print(f"Config dir:          {args.config_dir}")
    print(f"Databricks env:      {args.databricks_env}")
    print(f"Anchor dates:        {args.anchor_dates}")
    print(f"Cohort:              {args.cohort}")
    print(f"Mode:                {args.production_or_test}")
    print(f"Catalog:             {args.catalog}")
    print(f"Schema:              {args.schema}")
    print(f"Output (Databricks): {args.output_dir_databricks}")
    print(f"Output (Local):      {args.output_dir_local}")
    print(f"Process patient:     {args.patient}")
    print(f"Process sample:      {args.sample}")
    print(f"{'#'*80}\n")

    # Run pipelines
    if args.patient:
        run_patient_pipeline(args)

    if args.sample:
        run_sample_pipeline(args)

    # Final summary
    print(f"\n{'#'*80}")
    print(f"# ALL PIPELINES COMPLETE")
    print(f"{'#'*80}")
    if args.patient:
        print(f"Patient summary: {args.output_dir_local}/data_clinical_patient.txt")
    if args.sample:
        print(f"Sample summary:  {args.output_dir_local}/data_clinical_sample.txt")
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()

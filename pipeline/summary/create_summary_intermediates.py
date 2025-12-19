"""
create_summary_intermediates.py

Script to create intermediate summary files from YAML configurations.
This processes individual summaries and creates intermediate files with headers.

Can be used to:
1. Process a single summary (--yaml_config)
2. Process all summaries in a directory (--config_dir)

Usage for single summary:
    python pipeline/summary/create_summary_intermediates.py \
        --yaml_config config/summaries/cancer_diagnosis_summary.yaml \
        --databricks_env /path/to/databricks.env \
        --template /Volumes/.../patient_template.tsv \
        --production_or_test production \
        --cohort mskimpact

Usage for all summaries:
    python pipeline/summary/create_summary_intermediates.py \
        --config_dir config/summaries \
        --databricks_env /path/to/databricks.env \
        --template_patient /Volumes/.../patient_template.tsv \
        --template_sample /Volumes/.../sample_template.tsv \
        --manifest_patient /Volumes/.../manifest_patient.csv \
        --manifest_sample /Volumes/.../manifest_sample.csv \
        --production_or_test production \
        --cohort mskimpact
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.summary import YamlConfigToCbioportalFormat


def process_single_summary(args):
    """Process a single YAML config file."""
    print(f"\n{'='*80}")
    print(f"PROCESSING SINGLE SUMMARY")
    print(f"{'='*80}")
    print(f"YAML: {args.yaml_config}")
    print(f"Mode: {args.production_or_test}")
    print(f"Cohort: {args.cohort}")
    print(f"{'='*80}\n")

    # Determine patient or sample level from YAML
    import yaml
    with open(args.yaml_config, 'r') as f:
        config = yaml.safe_load(f)

    patient_or_sample = config['patient_or_sample']
    print(f"Detected level: {patient_or_sample}")

    # Create processor object
    obj = YamlConfigToCbioportalFormat(
        fname_databricks_env=args.databricks_env,
        config_dir=os.path.dirname(args.yaml_config),
        volume_path_summary_intermediate='',  # Not used for single file
        production_or_test=args.production_or_test,
        cohort=args.cohort
    )

    # Load template
    df_template = obj._load_template(args.template)

    # Process the single summary
    manifest_entry = obj.process_single_summary(
        yaml_file=args.yaml_config,
        df_anchor=obj._df_anchor,
        df_template=df_template,
        patient_or_sample=patient_or_sample,
        save_to_table=args.save_to_table
    )

    if manifest_entry:
        print(f"\n{'='*80}")
        print(f"SUCCESS!")
        print(f"{'='*80}")
        print(f"Summary ID: {manifest_entry['summary_id']}")
        print(f"Intermediate file: {manifest_entry['data_path']}")
        print(f"{'='*80}\n")
    else:
        print(f"\n{'='*80}")
        print(f"FAILED or SKIPPED")
        print(f"{'='*80}\n")
        sys.exit(1)


def process_all_summaries(args):
    """Process all YAML configs in a directory."""
    print(f"\n{'='*80}")
    print(f"PROCESSING ALL SUMMARIES")
    print(f"{'='*80}")
    print(f"Config dir: {args.config_dir}")
    print(f"Mode: {args.production_or_test}")
    print(f"Cohort: {args.cohort}")
    print(f"{'='*80}\n")

    # Create processor object
    obj = YamlConfigToCbioportalFormat(
        fname_databricks_env=args.databricks_env,
        config_dir=args.config_dir,
        volume_path_summary_intermediate='',  # Not used
        production_or_test=args.production_or_test,
        cohort=args.cohort
    )

    # Process patient summaries
    if args.template_patient and args.manifest_patient:
        print(f"\n{'#'*80}")
        print(f"# PATIENT SUMMARIES")
        print(f"{'#'*80}\n")

        obj.create_summaries_and_headers(
            patient_or_sample='patient',
            fname_manifest=args.manifest_patient,
            table_template=args.template_patient,
            save_to_table=args.save_to_table
        )

    # Process sample summaries
    if args.template_sample and args.manifest_sample:
        print(f"\n{'#'*80}")
        print(f"# SAMPLE SUMMARIES")
        print(f"{'#'*80}\n")

        obj.create_summaries_and_headers(
            patient_or_sample='sample',
            fname_manifest=args.manifest_sample,
            table_template=args.template_sample,
            save_to_table=args.save_to_table
        )

    print(f"\n{'='*80}")
    print(f"ALL SUMMARIES PROCESSED")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Create intermediate summary files from YAML configurations"
    )

    # Common arguments
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file"
    )
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
    parser.add_argument(
        "--save_to_table",
        action="store_true",
        help="Save intermediate files to Databricks tables"
    )

    # Single file mode
    parser.add_argument(
        "--yaml_config",
        help="Path to single YAML config file to process"
    )
    parser.add_argument(
        "--template",
        help="Path to template file (for single file mode)"
    )

    # Batch mode
    parser.add_argument(
        "--config_dir",
        help="Directory containing YAML config files (for batch mode)"
    )
    parser.add_argument(
        "--template_patient",
        help="Path to patient template file (for batch mode)"
    )
    parser.add_argument(
        "--template_sample",
        help="Path to sample template file (for batch mode)"
    )
    parser.add_argument(
        "--manifest_patient",
        help="Path to save patient manifest file (for batch mode)"
    )
    parser.add_argument(
        "--manifest_sample",
        help="Path to save sample manifest file (for batch mode)"
    )

    args = parser.parse_args()

    # Determine mode
    if args.yaml_config:
        # Single file mode
        if not args.template:
            parser.error("--template is required when using --yaml_config")
        process_single_summary(args)
    elif args.config_dir:
        # Batch mode
        if not (args.template_patient or args.template_sample):
            parser.error("At least one of --template_patient or --template_sample is required when using --config_dir")
        if args.template_patient and not args.manifest_patient:
            parser.error("--manifest_patient is required when using --template_patient")
        if args.template_sample and not args.manifest_sample:
            parser.error("--manifest_sample is required when using --template_sample")
        process_all_summaries(args)
    else:
        parser.error("Either --yaml_config or --config_dir must be specified")


if __name__ == "__main__":
    main()

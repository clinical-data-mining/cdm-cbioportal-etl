"""
create_intermediate_summaries_single_yaml.py

Script to create intermediate summary files from YAML configurations.
This processes individual summaries and creates intermediate files with headers.

Can be used to process a single summary (--yaml_config)

Usage for single summary:
    python pipeline/summary/create_intermediate_summaries_single_yaml.py \
        --yaml_config config/summaries/cancer_diagnosis_summary.yaml \
        --databricks_env /path/to/databricks.env \
        --template /Volumes/.../template.tsv \
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
        production_or_test=args.production_or_test,
        cohort=args.cohort
    )

    # Load template (subset to relevant ID column)
    df_template = obj._load_template(args.template, patient_or_sample)

    # Process the single summary
    summary_info = obj.process_single_summary(
        yaml_file=args.yaml_config,
        df_anchor=obj._df_anchor,
        df_template=df_template,
        patient_or_sample=patient_or_sample,
        save_to_table=args.save_to_table
    )

    if summary_info:
        print(f"\n{'='*80}")
        print(f"SUCCESS!")
        print(f"{'='*80}")
        print(f"Summary ID: {summary_info['summary_id']}")
        print(f"Intermediate file: {summary_info['intermediate_path']}")
        print(f"\nFile contains data only (no header rows)")
        print(f"Column names are in the first row")
        print(f"You can inspect this file directly on Databricks")
        print(f"{'='*80}\n")
    else:
        print(f"\n{'='*80}")
        print(f"FAILED or SKIPPED")
        print(f"{'='*80}\n")
        sys.exit(1)


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
        help="Path to template file supplied by cbioportal backend"
    )

    args = parser.parse_args()

    # Determine mode
    if args.yaml_config:
        # Single file mode
        if not args.template:
            parser.error("--template is required when using --yaml_config")
        process_single_summary(args)
    else:
        parser.error("Either --yaml_config or --config_dir must be specified")


if __name__ == "__main__":
    main()

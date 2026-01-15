"""
wrapper_yaml_summary_creator.py

Create summary files from YAML configurations.
This replaces the codebook-based approach with YAML configs.

Flow:
1. Create intermediate files (data only) from YAML configs
2. Merge intermediates into summary data files (volume + table)
3. Create header files from YAML metadata (volume + table)
4. Combine header + data into final files (volume only)
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.summary import YamlConfigToCbioportalFormat, SummaryMerger
from lib.utils import cbioportal_update_config


def create_yaml_summary(
    fname_databricks_env: str,
    config_dir: str,
    template_patient: str,
    template_sample: str,
    output_patient_data: str,
    output_sample_data: str,
    output_patient_header: str,
    output_sample_header: str,
    output_patient_combined: str,
    output_sample_combined: str,
    production_or_test: str,
    cohort: str,
    save_to_table: bool = False
):
    """
    Create cBioPortal summary files from YAML configurations.

    Parameters
    ----------
    fname_databricks_env : str
        Path to Databricks environment file
    config_dir : str
        Directory containing YAML configuration files
    template_patient : str
        Path to patient template file
    template_sample : str
        Path to sample template file
    output_patient_data : str
        Output path for patient summary data (volume + optional table)
    output_sample_data : str
        Output path for sample summary data (volume + optional table)
    output_patient_header : str
        Output path for patient header file (volume + optional table)
    output_sample_header : str
        Output path for sample header file (volume + optional table)
    output_patient_combined : str
        Output path for combined patient file (volume only)
    output_sample_combined : str
        Output path for combined sample file (volume only)
    production_or_test : str
        'production' or 'test'
    cohort : str
        Cohort name (e.g., 'mskimpact')
    save_to_table : bool
        Whether to save data and header files to Databricks tables
    """
    print(f"\n{'='*80}")
    print(f"YAML-BASED SUMMARY CREATION")
    print(f"{'='*80}")
    print(f"Config directory: {config_dir}")
    print(f"Production/Test: {production_or_test}")
    print(f"Cohort: {cohort}")
    print(f"Save to table: {save_to_table}")
    print(f"{'='*80}\n")

    # Step 1: Create intermediate files
    print(f"\n{'#'*80}")
    print(f"# STEP 1: CREATE INTERMEDIATE FILES")
    print(f"{'#'*80}\n")

    obj = YamlConfigToCbioportalFormat(
        fname_databricks_env=fname_databricks_env,
        config_dir=config_dir,
        production_or_test=production_or_test,
        cohort=cohort
    )

    # Process patient summaries
    print(f"\n{'-'*80}")
    print(f"Processing PATIENT summaries")
    print(f"{'-'*80}\n")

    processed_patient = obj.create_summaries_and_headers(
        patient_or_sample='patient',
        table_template=template_patient,
        save_to_table=False  # Intermediates not saved to tables
    )

    print(f"\n✓ Created {len(processed_patient)} patient intermediate files")

    # Process sample summaries
    print(f"\n{'-'*80}")
    print(f"Processing SAMPLE summaries")
    print(f"{'-'*80}\n")

    processed_sample = obj.create_summaries_and_headers(
        patient_or_sample='sample',
        table_template=template_sample,
        save_to_table=False  # Intermediates not saved to tables
    )

    print(f"\n✓ Created {len(processed_sample)} sample intermediate files")

    # Step 2-4: Merge, create headers, and save
    for patient_or_sample, processed_summaries, template, output_data, output_header, output_combined in [
        ('patient', processed_patient, template_patient, output_patient_data, output_patient_header, output_patient_combined),
        ('sample', processed_sample, template_sample, output_sample_data, output_sample_header, output_sample_combined)
    ]:
        if not processed_summaries:
            print(f"\nNo {patient_or_sample} summaries to merge, skipping...")
            continue

        print(f"\n{'#'*80}")
        print(f"# STEP 2-4: MERGE AND SAVE {patient_or_sample.upper()} SUMMARIES")
        print(f"{'#'*80}\n")

        # Create merger
        merger = SummaryMerger(
            fname_databricks_env=fname_databricks_env,
            fname_template=template,
            patient_or_sample=patient_or_sample
        )

        # Merge all intermediates (creates headers from YAML configs)
        merger.merge_all_intermediates(processed_summaries)

        # Get merged header and data
        df_header, df_data = merger.get_merged_summary()

        # Save data file (volume + table)
        print(f"\nSaving {patient_or_sample} data file: {output_data}")

        if save_to_table:
            # Extract catalog, schema, table from output path or use defaults
            table_name = f"data_clinical_{patient_or_sample}"
            dict_database_table_info = {
                'catalog': 'cdsi_eng_phi',
                'schema': 'cdm_eng_cbioportal_etl',
                'table': table_name,
                'volume_path': output_data,
                'sep': '\t'
            }
        else:
            dict_database_table_info = None

        merger.obj_db.write_db_obj(
            df=df_data,
            volume_path=output_data,
            sep='\t',
            overwrite=True,
            dict_database_table_info=dict_database_table_info
        )

        print(f"✓ Saved {patient_or_sample} data: {output_data}")
        if save_to_table:
            print(f"  + Table: cdsi_eng_phi.cdm_eng_cbioportal_etl.{table_name}")

        # Save header file (volume + table)
        print(f"\nSaving {patient_or_sample} header file: {output_header}")

        if save_to_table:
            table_name_header = f"data_clinical_{patient_or_sample}_header"
            dict_database_table_info_header = {
                'catalog': 'cdsi_eng_phi',
                'schema': 'cdm_eng_cbioportal_etl',
                'table': table_name_header,
                'volume_path': output_header,
                'sep': '\t'
            }
        else:
            dict_database_table_info_header = None

        merger.obj_db.write_db_obj(
            df=df_header,
            volume_path=output_header,
            sep='\t',
            overwrite=True,
            dict_database_table_info=dict_database_table_info_header
        )

        print(f"✓ Saved {patient_or_sample} header: {output_header}")
        if save_to_table:
            print(f"  + Table: cdsi_eng_phi.cdm_eng_cbioportal_etl.{table_name_header}")

        # Create and save combined file (volume only)
        print(f"\nCreating combined {patient_or_sample} file (header + data)")
        df_combined = merger.create_final_summary()

        merger.obj_db.write_db_obj(
            df=df_combined,
            volume_path=output_combined,
            sep='\t',
            overwrite=True,
            dict_database_table_info=None  # Combined file is volume only
        )

        print(f"✓ Saved {patient_or_sample} combined file: {output_combined}")
        print(f"  Shape: {df_combined.shape}")

    print(f"\n{'='*80}")
    print(f"ALL SUMMARY FILES CREATED SUCCESSFULLY")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Create patient and sample summary files from YAML configurations"
    )
    parser.add_argument(
        "--config_yaml",
        required=True,
        help="YAML file containing run parameters and file locations"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--path_datahub",
        required=True,
        help="Path to datahub for output files"
    )
    parser.add_argument(
        "--template",
        required=True,
        help="Path to template file containing sample IDs (local file or Databricks path). Used for both patient and sample - different columns are subset and duplicates dropped."
    )
    parser.add_argument(
        "--production_or_test",
        required=True,
        choices=["test", "production"],
        help="Use production or test source tables"
    )
    parser.add_argument(
        "--save_to_table",
        action="store_true",
        help="Save data and header files to Databricks tables"
    )

    args = parser.parse_args()

    # Load configuration
    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)

    # Get paths from config
    databricks_config = obj_yaml.return_databricks_configs()
    catalog = databricks_config['catalog']
    schema = databricks_config['schema']
    volume = databricks_config['volume']

    # YAML config directory (hardcode for now - should be in repo)
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    config_dir = os.path.join(repo_root, 'config', 'summaries')

    # Cohort (extract from volume path or hardcode)
    cohort = 'mskimpact'

    # Template path (same file for both patient and sample)
    # Patient: extracts PATIENT_ID column and drops duplicates
    # Sample: extracts SAMPLE_ID column and drops duplicates
    template_file = args.template

    # Output paths for data and header files (volume)
    volume_intermediate = databricks_config['volume_path_intermediate']
    volume_base_path = f"/Volumes/{catalog}/{schema}/{volume}/{volume_intermediate}"

    # Data files (volume + table)
    output_patient_data = f"{volume_base_path}data_clinical_patient_data.txt"
    output_sample_data = f"{volume_base_path}data_clinical_sample_data.txt"

    # Header files (volume + table)
    output_patient_header = f"{volume_base_path}data_clinical_patient_header.txt"
    output_sample_header = f"{volume_base_path}data_clinical_sample_header.txt"

    # Combined files (volume only)
    output_files = obj_yaml.return_filenames_deid_datahub(path_datahub=args.path_datahub)
    output_patient_combined = output_files['summary_patient']
    output_sample_combined = output_files['summary_sample']

    # Create summaries (using same template file for both patient and sample)
    create_yaml_summary(
        fname_databricks_env=args.databricks_env,
        config_dir=config_dir,
        template_patient=template_file,
        template_sample=template_file,
        output_patient_data=output_patient_data,
        output_sample_data=output_sample_data,
        output_patient_header=output_patient_header,
        output_sample_header=output_sample_header,
        output_patient_combined=output_patient_combined,
        output_sample_combined=output_sample_combined,
        production_or_test=args.production_or_test,
        cohort=cohort,
        save_to_table=args.save_to_table
    )


if __name__ == "__main__":
    main()

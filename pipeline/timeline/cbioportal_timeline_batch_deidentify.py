"""
cbioportal_timeline_batch_deidentify.py

Batch wrapper script to execute timeline deidentification for all timeline files.
This script loops through all timeline configurations and calls the deidentification
process for each one.
"""
import os
import sys
import argparse
import subprocess
from pathlib import Path


# Hardcoded timeline configurations
# Format: (task_id, databricks_table, output_filename, columns, merge_level)
TIMELINE_CONFIGS = [
    ("treatment", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_medications", "data_timeline_treatment",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,TREATMENT_TYPE,AGENT,SUBTYPE,RX_INVESTIGATIVE", None),
    ("surgery", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_surgery", "data_timeline_surgery",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,PROCEDURE_DESCRIPTION", None),
    ("radiation", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_radiation", "data_timeline_radiation",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,PLAN,DELIVERED_DOSE,PLANNED_FRACTIONS,REFERENCE_POINT", None),
    ("specimen", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_sequencing", "data_timeline_specimen",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SAMPLE_ID", "sample"),
    ("specimen_surgery", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_specimen_surgery", "data_timeline_specimen_surgery",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SAMPLE_ID", "sample"),
    ("diagnosis", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_dx_timeline_primary", "data_timeline_diagnosis",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,DX_DESCRIPTION,AJCC,CLINICAL_GROUP,PATH_GROUP,SUMMARY,STAGE_CDM_DERIVED,STAGE_CDM_DERIVED_GRANULAR", None),
    ("follow_up", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_follow_up", "data_timeline_follow_up",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE", None),
    ("progression", "cdsi_eng_phi.cdm_eng_rad_progression.table_timeline_radiology_cancer_progression_predictions", "data_timeline_progression",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,SOURCE_SPECIFIC,RADIOLOGY_PROCEDURE_NAME,PROGRESSION,PROGRESSION_PROBABILITY,STYLE_COLOR", None),
    ("cancer_presence", "cdsi_eng_phi.cdm_eng_rad_cancer_presence.table_timeline_cancer_presence", "data_timeline_cancer_presence",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,CANCER_PRESENT", None),
    ("tumor_sites", "cdsi_eng_phi.cdm_eng_rad_tumor_sites.table_timeline_radiology_tumor_sites_predictions", "data_timeline_tumor_sites",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,SOURCE_SPECIFIC,TUMOR_SITE,RADIOLOGY_PROCEDURE_NAME,STYLE_COLOR", None),
    ("cea_labs", "cdsi_prod.cdm_impact_pipeline_prod.t34_epic_ddp_cea_labs", "data_timeline_cea_labs",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,LR_ABNORMALITY_CD,LR_TEST_UP_LIMIT,LR_TEST_LOW_LIMIT,LR_UNIT_MEASURE,RESULT,LR_ABNORMALITY_CD_TEXT", None),
    ("ca_125_labs", "cdsi_prod.cdm_impact_pipeline_prod.t35_epic_ddp_ca_125_labs", "data_timeline_ca_125_labs",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,LR_ABNORMALITY_CD,LR_TEST_UP_LIMIT,LR_TEST_LOW_LIMIT,LR_UNIT_MEASURE,RESULT,LR_ABNORMALITY_CD_TEXT", None),
    ("ca_15_3_labs", "cdsi_prod.cdm_impact_pipeline_prod.t36_epic_ddp_ca_15_3_labs", "data_timeline_ca_15-3_labs",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,LR_ABNORMALITY_CD,LR_TEST_UP_LIMIT,LR_TEST_LOW_LIMIT,LR_UNIT_MEASURE,RESULT,LR_ABNORMALITY_CD_TEXT", None),
    ("ca_19_9_labs", "cdsi_prod.cdm_impact_pipeline_prod.t37_epic_ddp_ca_19_9_labs", "data_timeline_ca_19-9_labs",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,LR_ABNORMALITY_CD,LR_TEST_UP_LIMIT,LR_TEST_LOW_LIMIT,LR_UNIT_MEASURE,RESULT,LR_ABNORMALITY_CD_TEXT", None),
    ("psa_labs", "cdsi_prod.cdm_impact_pipeline_prod.t38_epic_ddp_psa_labs", "data_timeline_psa_labs",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,LR_ABNORMALITY_CD,LR_TEST_UP_LIMIT,LR_TEST_LOW_LIMIT,LR_UNIT_MEASURE,RESULT,LR_ABNORMALITY_CD_TEXT", None),
    ("tsh_labs", "cdsi_prod.cdm_impact_pipeline_prod.t39_epic_ddp_tsh_labs", "data_timeline_tsh_labs",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,LR_ABNORMALITY_CD,LR_TEST_UP_LIMIT,LR_TEST_LOW_LIMIT,LR_UNIT_MEASURE,RESULT,LR_ABNORMALITY_CD_TEXT", None),
    ("bmi", "cdsi_prod.cdm_impact_pipeline_prod.t33_epic_ddp_bmi", "data_timeline_bmi",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,TEST,RESULT", None),
    ("ecog_kps", "cdsi_prod.cdm_impact_pipeline_prod.t31_epic_ddp_ecog_kps", "data_timeline_ecog_kps",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,ECOG_KPS", None),
    ("mmr", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_mmr_calls", "data_timeline_mmr",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,MMR", None),
    ("pdl1", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_pdl1_calls", "data_timeline_pdl1",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,PDL1_POSITIVE", None),
    ("gleason", "cdsi_prod.cdm_idbw_impact_pipeline_prod.table_timeline_gleason_scores", "data_timeline_gleason",
     "PATIENT_ID,START_DATE,STOP_DATE,EVENT_TYPE,SUBTYPE,SOURCE,GLEASON_SCORE", None),
]


def run_timeline_deidentification(fname_dbx, fname_sample, volume_base_path, gpfs_output_path, cohort_name):
    """
    Run timeline deidentification for all configured timeline files.

    Parameters
    ----------
    fname_dbx : str
        Path to Databricks environment file
    fname_sample : str
        Path to sample list file
    volume_base_path : str
        Base path for Databricks volume output (without cohort name)
    gpfs_output_path : str
        Base path for GPFS output files
    cohort_name : str
        Name of the cohort (e.g., 'mskimpact', 'mskimpact_heme')
    """

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
    print(f"Total timeline files to process: {len(TIMELINE_CONFIGS)}")
    print(f"Databricks env: {fname_dbx}")
    print(f"Sample list: {fname_sample}")
    print(f"Volume base path: {volume_path_full}")
    print(f"GPFS output path: {gpfs_output_path}")
    print(f"Cohort: {cohort_name}")
    print("=" * 80)
    print()

    successful = []
    failed = []

    for idx, (task_id, dbx_table, output_name, columns, merge_level) in enumerate(TIMELINE_CONFIGS, 1):
        print(f"\n[{idx}/{len(TIMELINE_CONFIGS)}] Processing: {task_id}")
        print("-" * 80)

        # Build paths
        fname_output_volume = f"{volume_path_full}/{output_name}_phi.tsv"
        fname_output_gpfs = f"{gpfs_output_path}/{output_name}.txt"

        # Build command arguments
        cmd = [
            "python",
            str(deidentify_script),
            f"--fname_dbx={fname_dbx}",
            f"--fname_timeline={dbx_table}",
            f"--fname_sample={fname_sample}",
            f"--fname_output_volume={fname_output_volume}",
            f"--fname_output_gpfs={fname_output_gpfs}",
            f"--columns_cbio={columns}"
        ]

        # Add merge_level if specified
        if merge_level:
            cmd.append(f"--merge_level={merge_level}")

        print(f"Table: {dbx_table}")
        print(f"Output (PHI): {fname_output_volume}")
        print(f"Output (DEID): {fname_output_gpfs}")
        if merge_level:
            print(f"Merge level: {merge_level}")
        print()

        try:
            # Run the deidentification script
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            successful.append(task_id)
            print(f"✓ Successfully processed: {task_id}")
        except subprocess.CalledProcessError as e:
            print(f"✗ FAILED to process: {task_id}")
            print(f"Error code: {e.returncode}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            failed.append(task_id)
        except Exception as e:
            print(f"✗ FAILED to process: {task_id}")
            print(f"Error: {str(e)}")
            failed.append(task_id)

        print("-" * 80)

    # Print summary
    print("\n" + "=" * 80)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Total processed: {len(TIMELINE_CONFIGS)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        print(f"\nSuccessful files ({len(successful)}):")
        for task_id in successful:
            print(f"  ✓ {task_id}")

    if failed:
        print(f"\nFailed files ({len(failed)}):")
        for task_id in failed:
            print(f"  ✗ {task_id}")
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
        "--fname_dbx",
        action="store",
        dest="fname_dbx",
        required=True,
        help="Path to Databricks environment file"
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
        fname_dbx=args.fname_dbx,
        fname_sample=args.fname_sample,
        volume_base_path=args.volume_base_path,
        gpfs_output_path=args.gpfs_output_path,
        cohort_name=args.cohort_name
    )

import os
import sys
import argparse

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from variables import (
    DICT_FILES_TIMELINE,
    ENV_MINIO
)
from variables_testing_study import DICT_FILES_TIMELINE_TESTING
from cdm_cbioportal_etl.timeline import cbioportal_deid_timeline_files


def main():
    parser = argparse.ArgumentParser(
        description="Script for deidentifying timeline files for cbioportal."
    )
    parser.add_argument(
        "--fname_minio_env",
        action="store",
        dest="fname_minio_env",
        default=ENV_MINIO,
        help="Minio environment file.",
    )
    parser.add_argument(
        "--production_or_test",
        action="store",
        dest="production_or_test",
        default="production",
        help="Logic for using the timelines for testing or production.",
    )

    args = parser.parse_args()
    if args.production_or_test == 'production':
        dict_files_timeline = DICT_FILES_TIMELINE
    elif args.production_or_test == 'test':
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING
    else:
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING


    _ = cbioportal_deid_timeline_files(
        fname_minio_env=args.fname_minio_env,
        dict_files_timeline=dict_files_timeline
    )


if __name__ == '__main__':
    main()
import os
import sys
import shutil
import argparse


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "utils")))
from constants import DICT_FILES_TO_COPY


def copy_files(files_to_copy, dest_folder):
    for file in files_to_copy:
        filename = os.path.basename(file)
        dest_path = os.path.join(dest_folder, filename)
        print(f"Copying: {file} to destination: {dest_path}")
        shutil.copyfile(file, dest_path)


def main():
    parser = argparse.ArgumentParser(prog="copy_cbio_files_to_automation_folder.py")
    parser.add_argument(
        "-d",
        "--dest-folder",
        dest="dest_folder",
        action="store",
        required=True,
        help="destination folder for cBioPortal files",
    )
    args = parser.parse_args()
    dest_folder = args.dest_folder

    if not os.path.exists(dest_folder):
        print(f"No such directory: {dest_folder}")
        parser.print_help()

    files_to_copy = DICT_FILES_TO_COPY.keys()
    copy_files(files_to_copy, dest_folder)

    print("Complete!")

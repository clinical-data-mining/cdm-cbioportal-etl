import os
import shutil
import argparse

path_datahub = '/gpfs/mindphidata/fongc2/datahubs/cdm/msk-chord/'
path_minio_cbio = 'cbioportal'
summary_p = 'data_clinical_patient.txt'
summary_s = 'data_clinical_sample.txt'
timeline_surg = 'data_timeline_surgery.txt'
timeline_rt = 'data_timeline_radiation.txt'
timeline_meds = 'data_timeline_treatment.txt'
timeline_dx_primary = 'data_timeline_diagnosis.txt'
timeline_spec = 'data_timeline_specimen.txt'
timeline_spec_surg = 'data_timeline_specimen_surgery.txt'
timeline_gleason = 'data_timeline_gleason.txt'
timeline_pdl1 = 'data_timeline_pdl1.txt'
timeline_prior_meds = 'data_timeline_prior_meds.txt'
timeline_tumor_sites = 'data_timeline_tumor_sites.txt'
timeline_follow_up = 'data_timeline_timeline_follow_up.txt'

DICT_FILES_TO_COPY = {
    os.path.join(path_datahub, summary_p): os.path.join(path_minio_cbio, summary_p),
    os.path.join(path_datahub, summary_s): os.path.join(path_minio_cbio, summary_s),
    os.path.join(path_datahub, timeline_surg): os.path.join(path_minio_cbio, timeline_surg),
    os.path.join(path_datahub, timeline_rt): os.path.join(path_minio_cbio, timeline_rt),
    os.path.join(path_datahub, timeline_meds): os.path.join(path_minio_cbio, timeline_meds),
    os.path.join(path_datahub, timeline_dx_primary): os.path.join(path_minio_cbio, timeline_dx_primary),
    os.path.join(path_datahub, timeline_spec): os.path.join(path_minio_cbio, timeline_spec),
    os.path.join(path_datahub, timeline_gleason): os.path.join(path_minio_cbio, timeline_gleason),
    os.path.join(path_datahub, timeline_pdl1): os.path.join(path_minio_cbio, timeline_pdl1),
    os.path.join(path_datahub, timeline_prior_meds): os.path.join(path_minio_cbio, timeline_prior_meds),
    os.path.join(path_datahub, timeline_tumor_sites): os.path.join(path_minio_cbio, timeline_tumor_sites),
    os.path.join(path_datahub, timeline_spec_surg): os.path.join(path_minio_cbio, timeline_spec_surg),
    os.path.join(path_datahub, timeline_follow_up): os.path.join(path_minio_cbio, timeline_follow_up)
}

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

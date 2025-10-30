"""
cbioportal_timeline_truncate_to_fu.py

Truncates STOP_DATE in timeline files to not exceed follow-up or death dates
"""
import argparse
import os

import pandas as pd


# Follow-up timeline file name containing follow-up and death events
FNAME_FOLLOWUP_TIMELINE = 'data_timeline_follow_up.txt'

# List of timeline file names to truncate
TIMELINE_FILES = [
    'data_timeline_treatment.txt',
    # Add other timeline file names here as needed
]


def truncate_timeline_to_followup(timeline_folder):
    """
    Truncates STOP_DATE in timeline files to not exceed follow-up or death dates.

    This function loads the follow-up timeline file containing follow-up and death events,
    then loads other timeline files and ensures that STOP_DATE values do not exceed
    the earliest follow-up or death date for each patient.

    Parameters
    ----------
    timeline_folder : str
        Path to folder containing timeline files

    Returns
    -------
    None
    """
    # Construct full path to follow-up timeline file
    followup_path = os.path.join(timeline_folder, FNAME_FOLLOWUP_TIMELINE)

    # Load follow-up timeline
    print(f'Loading follow-up timeline: {followup_path}')
    df_followup = pd.read_csv(followup_path, sep='\t')

    # Convert START_DATE to int (days from anchor date)
    df_followup['START_DATE'] = df_followup['START_DATE'].astype(int)

    # Get the maximum follow-up or death date for each patient
    # This represents the last known date for the patient
    df_max_date = df_followup.groupby('PATIENT_ID')['START_DATE'].max().reset_index()
    df_max_date.columns = ['PATIENT_ID', 'MAX_FU_DATE']

    print(f'Found {len(df_max_date)} unique patients in follow-up timeline')

    # Process each timeline file
    for fname_timeline in TIMELINE_FILES:
        print(f'\nProcessing: {fname_timeline}')

        try:
            # Construct full path to timeline file
            timeline_path = os.path.join(timeline_folder, fname_timeline)

            # Load timeline file
            df_timeline = pd.read_csv(timeline_path, sep='\t')

            print(f'  Loaded {len(df_timeline)} records')

            # Convert dates to int
            df_timeline['START_DATE'] = df_timeline['START_DATE'].astype(int)
            df_timeline['STOP_DATE'] = pd.to_numeric(df_timeline['STOP_DATE'], errors='coerce')

            # Merge with follow-up max dates
            df_timeline = df_timeline.merge(
                df_max_date,
                on='PATIENT_ID',
                how='left'
            )

            # Count records before truncation
            records_before = df_timeline['STOP_DATE'].notnull().sum()

            # Truncate STOP_DATE to not exceed MAX_FU_DATE
            # Only modify STOP_DATE if it exists and exceeds the follow-up date
            mask = (df_timeline['STOP_DATE'].notnull()) & \
                   (df_timeline['MAX_FU_DATE'].notnull()) & \
                   (df_timeline['STOP_DATE'] > df_timeline['MAX_FU_DATE'])

            records_truncated = mask.sum()

            if records_truncated > 0:
                # Get list of patients that will be truncated
                patients_to_truncate = df_timeline.loc[mask, 'PATIENT_ID'].unique()
                print(f'  Truncated {records_truncated} records for {len(patients_to_truncate)} patients')
                print(f'  Patient IDs (PATIENT_ID) with truncated records:')
                print(f'  {sorted(patients_to_truncate.tolist())}')

                # Perform truncation
                df_timeline.loc[mask, 'STOP_DATE'] = df_timeline.loc[mask, 'MAX_FU_DATE']
            else:
                print(f'  No records needed truncation')

            # Drop the helper column
            df_timeline = df_timeline.drop(columns=['MAX_FU_DATE'])

            # Convert STOP_DATE back to int (or empty string for NaN)
            df_timeline['STOP_DATE'] = df_timeline['STOP_DATE'].apply(
                lambda x: int(x) if pd.notnull(x) else ''
            )

            # Save the modified timeline file
            print(f'  Saving modified file to: {timeline_path}')
            df_timeline.to_csv(timeline_path, sep='\t', index=False)

            print(f'  Successfully processed {fname_timeline}')

        except Exception as e:
            print(f'  Error processing {fname_timeline}: {str(e)}')
            continue

    print('\nTruncation complete')
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Script for truncating timeline STOP_DATE to follow-up dates"
    )
    parser.add_argument(
        "--timeline_folder",
        action="store",
        dest="timeline_folder",
        required=True,
        help="Path to folder containing timeline files",
    )
    args = parser.parse_args()

    truncate_timeline_to_followup(timeline_folder=args.timeline_folder)


if __name__ == '__main__':
    main()
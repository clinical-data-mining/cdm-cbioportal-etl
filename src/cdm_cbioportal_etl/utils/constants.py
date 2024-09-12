import os
from dataclasses import dataclass

@dataclass
class constants:
    ### Column names for the manifest file
    COL_SUMMARY_FNAME_SAVE: str = 'SUMMARY_FILENAME'
    COL_SUMMARY_HEADER_FNAME_SAVE: str = 'SUMMARY_HEADER_FILENAME'
    COL_RPT_NAME: str = 'REPORT_NAME'

    COL_PID: str = 'DMP_ID'
    COL_PID_CBIO: str = 'PATIENT_ID'

    COL_P_ID_CBIO = 'PATIENT_ID'
    COL_S_ID_CBIO = 'SAMPLE_ID'

    COLS_PRODUCTION = ['label', 'comment', 'data_type', 'visible', 'heading']
    COLS_TESTING = ['label', 'comment', 'data_type', 'patient_or_sample', 'visible', 'heading']

    COLS_ORDER_GENERAL = [
        'PATIENT_ID',
        'START_DATE',
        'STOP_DATE',
        'EVENT_TYPE',
        'SUBTYPE'
    ]
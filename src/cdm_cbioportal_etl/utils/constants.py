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
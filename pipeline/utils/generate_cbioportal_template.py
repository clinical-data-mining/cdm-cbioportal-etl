import sys
import os

from cdm_cbioportal_etl.summary import generate_cbioportal_template
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from constants import (
    ENV_MINIO,
    PATH_HEADER_SAMPLE,
    PATH_HEADER_PATIENT,
    FNAME_CBIO_SID,
    FNAME_SUMMARY_TEMPLATE_P,
    FNAME_SUMMARY_TEMPLATE_S,
    FNAME_SAMPLE_REMOVE
)


if __name__ == "__main__":
    generate_cbioportal_template(
        env_minio=ENV_MINIO,
        path_header_sample=PATH_HEADER_SAMPLE,
        path_header_patient=PATH_HEADER_PATIENT,
        fname_cbio_sid=FNAME_CBIO_SID,
        fname_sample_rmv=FNAME_SAMPLE_REMOVE,
        fname_summary_template_p=FNAME_SUMMARY_TEMPLATE_P,
        fname_summary_template_s=FNAME_SUMMARY_TEMPLATE_S
    )
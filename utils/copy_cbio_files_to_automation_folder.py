import os
import sys
import shutil

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from constants import (
    DICT_FILES_TO_COPY
) 
# dest_folder = '/mind_data/cdm_repos/datahubs/'    $ Use this line to test this script
dest_folder = '/mind_data/cdm_repos/datahubs/cdm-automation/msk-chord/'


list_files = list(DICT_FILES_TO_COPY.keys())

for i,file in enumerate(list_files):
    filename = os.path.basename(list_files[i])
    dest_path = os.path.join(dest_folder, filename)
    print('Copying: %s to destination: %s' % (file, dest_path))
    shutil.copyfile(file, dest_path)
    
print('Complete!')
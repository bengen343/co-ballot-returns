from config import *
import pandas as pd
import numpy as np
from datetime import datetime
import gcsfs


def calc_targets(output_df, target_files_lst):
    _targets_df = pd.DataFrame(columns=['State Voter ID'])
    _fs = gcsfs.GCSFileSystem(project=bq_project_id)

    for _file in target_files_lst:
        _f_str = _file.split('/')[-1].split('.')[0]

        _df = pd.DataFrame()
        with _fs.open(_file) as _f:
            _df = pd.read_csv(_f)

        _df[_f_str] = _f_str

        _targets_df = pd.merge(_targets_df, _df, how='outer', on='State Voter ID')

    output_df = pd.merge(output_df, _targets_df, how='left', left_on='VOTER_ID', right_on='State Voter ID')

    return output_df

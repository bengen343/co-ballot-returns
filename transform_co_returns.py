from config import *
import pandas as pd
import numpy as np
from datetime import datetime
import gcsfs


def calc_targets(output_df: pd.DataFrame, target_files_lst: list) -> pd.DataFrame:
    df = pd.DataFrame(columns=['State Voter ID'])
    fs = gcsfs.GCSFileSystem(project=bq_project_name)

    for file in target_files_lst:
        file_str = file.split('/')[-1].split('.')[0]

        _df = pd.DataFrame()
        with fs.open(file) as _file:
            _df = pd.read_csv(_file)

        _df[file_str] = file_str

        df = pd.merge(df, _df, how='outer', on='State Voter ID')

    df['State Voter ID'] = df['State Voter ID'].astype('float64').astype('Int64')
    output_df = pd.merge(output_df, df, how='left', left_on='VOTER_ID', right_on='State Voter ID')

    return output_df


def set_dtypes_on(df: pd.DataFrame, integer_col_lst: list) -> pd.DataFrame:
    print(f"Setting data types.")
    for column in list(df):
        if 'date' in column.lower():
            df[column] = pd.to_datetime(df[column], errors='coerce')
        elif column in integer_col_lst:
            df[column] = df[column].astype('float64').astype('Int64')
        else:
            df[column] = df[column].astype('str')

    df = df.replace('nan', np.nan)

    return df
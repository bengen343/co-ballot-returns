import zipfile

import numpy as np
import pandas as pd
from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient, types

from transform_co_returns import set_dtypes_on
from load_to_gcp import create_bq_schema

from config import *


# Use storage API to read in voter file table from BigQuery.
# In the future update this to use multiple threads for better speed.
def read_bq_table(bq_project_name: str, bq_dataset_name: str, table_name: str, fields_lst: list) -> pd.DataFrame:
    table_id = f"projects/{bq_project_name}/datasets/{bq_dataset_name}/tables/{table_name}"

    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=bq_credentials)

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=fields_lst 
    )

    parent = f'projects/{bq_project_name}'

    requested_session = types.ReadSession(
        table=table_id,
        data_format=types.DataFormat.ARROW,
        read_options=read_options,
    )
    read_session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    stream = read_session.streams[0]
    reader = bqstorageclient.read_rows(stream.name)

    frame_lst = []
    for message in reader.rows().pages:
        frame_lst.append(message.to_dataframe())
    df = pd.concat(frame_lst)
    
    return df


# Unzip return file
def unzip(file_str: str) -> str:
    file_str = zipfile.ZipFile(file_str)
    file_str.extractall('.')
    file_str.close()

    return(f"Successfully unzipped {file_str}.")
    

# Load complete CO voter file
def voters_to_df(bq_voters_table_name: str, voter_file_col_lst: list, integer_col_lst: list) -> pd.DataFrame:
    df = read_bq_table(bq_project_name, bq_dataset_name, bq_voters_table_name, voter_file_col_lst)
    print(f"Total Registration: {len(df):,}")

    # Clean up voter file data types
    df = set_dtypes_on(df, integer_col_lst)
    
    # Replace minor party designations with 'OTH'
    df.loc[((df['PARTY'] != 'REP') & (df['PARTY'] != 'DEM') & (df['PARTY'] != 'UAF')), 'PARTY'] = 'OTH'
    df.loc[((df['PREFERENCE'] != 'REP') & (df['PREFERENCE'] != 'DEM') & (df['PREFERENCE'] != 'UAF') & (~df['PREFERENCE'].isna())), 'PREFERENCE'] = 'OTH'
    df['PREFERENCE'] = df['PREFERENCE'] + ' Pref'
    
    return df


# Load the Colorado outstanding ballot file into a dataframe
def returns_to_df(return_txt_file: str, integer_col_lst: list) -> pd.DataFrame:
    # Import returned ballots to dataframe
    print("Loading returns to dataframe.")
    df = pd.DataFrame()
    df = pd.read_csv (return_txt_file, sep='|', encoding='cp437', index_col=None, header=0, low_memory=False, on_bad_lines='skip')
    print(f"Loaded the return file with {len(df):,} records.")

    # These lines are a hard coded fix for some bad data, remove them in the future:
    df['MAIL_BALLOT_RECEIVE_DATE'] = df['MAIL_BALLOT_RECEIVE_DATE'].replace('11/06/0202', '11/06/2022')
    df['MAIL_BALLOT_RECEIVE_DATE'] = df['MAIL_BALLOT_RECEIVE_DATE'].replace('11/08/0200', '11/08/2022')
    df['MAIL_BALLOT_RECEIVE_DATE'] = df['MAIL_BALLOT_RECEIVE_DATE'].replace('11/08/0202', '11/08/2022')
    
    # Make sure datatypes are set correctly for return dataframe.
    df = set_dtypes_on(df, integer_col_lst)
   
    # Set a master date column for when the voter voted
    df['RECEIVED_DATE'] = pd.to_datetime(df['MAIL_BALLOT_RECEIVE_DATE'], errors='coerce')
    df['RECEIVED_DATE'].fillna(df['IN_PERSON_VOTE_DATE'], inplace=True)

    # Replace minor party designations with 'OTH'
    df.loc[((df['PARTY'] != 'REP') & (df['PARTY'] != 'DEM') & (df['PARTY'] != 'UAF') & (~df['PARTY'].isna())), 'PARTY'] = 'OTH'
    
    df.loc[((df['PREFERENCE'] != 'REP') & (df['PREFERENCE'] != 'DEM') & (df['PREFERENCE'] != 'UAF') & (~df['PREFERENCE'].isna())), 'PREFERENCE'] = 'OTH'
    df['PREFERENCE'] = df['PREFERENCE'] + ' Pref'
    
    df.loc[((df['VOTED_PARTY'] != 'REP') & (df['VOTED_PARTY'] != 'DEM') & (df['VOTED_PARTY'] != 'UAF') & (~df['VOTED_PARTY'].isna())), 'VOTED_PARTY'] = 'OTH'
    df['VOTED_PARTY'] = df['VOTED_PARTY'] + ' Voted'

    return df


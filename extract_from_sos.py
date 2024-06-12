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

    return print(f"Successfully unzipped {file_str}.")
    

# Load complete CO voter file
def voters_to_df(bq_query_str: str, integer_col_lst: list) -> pd.DataFrame:
    df = pd.read_gbq(bq_query_str, project_id=bq_project_name, location=bq_project_location, credentials=bq_credentials, progress_bar_type='tqdm')
    print(f"Total Registration: {len(df):,}")

    # Clean up voter file data types
    df = set_dtypes_on(df, integer_col_lst)
    
    # Replace minor party designations with 'OTH'
    df.loc[((df['PARTY'] != 'REP') & (df['PARTY'] != 'DEM') & (df['PARTY'] != 'UAF')), 'PARTY'] = 'OTH'
    
    return df


# Load the Colorado outstanding ballot file into a dataframe
def returns_to_df(return_txt_file: str, integer_col_lst: list) -> pd.DataFrame:
    # Import returned ballots to dataframe
    print("Loading returns to dataframe.")
    df = pd.DataFrame()
    df = pd.read_csv (return_txt_file, sep='|', encoding='cp437', index_col=None, header=0, low_memory=False, on_bad_lines='skip')
    print(f"Loaded the return file with {len(df):,} records.")
    
    # Make sure datatypes are set correctly for return dataframe.
    df['GENDER'] = df['GENDER'].str.title()
    df['COUNTY'] = df['COUNTY'].str.title()
    df = set_dtypes_on(df, integer_col_lst)
    
    # Set a master date column for when the voter voted
    print("Setting ballot received date.")
    df['RECEIVED_DATE'] = pd.to_datetime(df['MAIL_BALLOT_RECEIVE_DATE'], errors='coerce')
    df['RECEIVED_DATE'].fillna(df['IN_PERSON_VOTE_DATE'], inplace=True)

    # Replace minor party designations with 'OTH'
    print("Replace minor parties with OTH.")
    df.loc[((df['PARTY'] != 'REP') & (df['PARTY'] != 'DEM') & (df['PARTY'] != 'UAF') & (~df['PARTY'].isna())), 'PARTY'] = 'OTH'
    
    df.loc[((df['VOTED_PARTY'] != 'REP') & (df['VOTED_PARTY'] != 'DEM') & (df['VOTED_PARTY'] != 'UAF') & (~df['VOTED_PARTY'].isna())), 'VOTED_PARTY'] = 'OTH'
    df['VOTED_PARTY'] = df['VOTED_PARTY'] + ' Voted'

    return df



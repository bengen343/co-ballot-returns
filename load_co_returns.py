import zipfile

import numpy as np
import pandas as pd
from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient, types

from config import *


def create_bq_schema(_df):
    schema_list = []
    for column in list(_df):
        if 'DATE' in column:
            sql_type = 'DATETIME'
        else:
            sql_type = 'STRING'
                
        if column == 'VOTER_ID':
            sql_mode = 'REQUIRED'
        else:
            sql_mode = 'NULLABLE'
            
        schema_list.append({'name':  column, 'type': sql_type, 'mode': sql_mode})
    
    return schema_list


def save_to_bq(_df, bq_table_schema, table_id, project_id=bq_project_id ):
    _df.to_gbq(destination_table=table_id, project_id=project_id, if_exists='replace', table_schema=bq_table_schema, credentials=bq_credentials)


def read_bq_table(table_id: str, selected_fields: list):
    table = f"projects/{bq_project_id}/datasets/{bq_dataset}/tables/{table_id}"

    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=bq_credentials)

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=selected_fields 
    )

    parent = f'projects/{bq_project_id}'

    requested_session = types.ReadSession(
        table=table,
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
def unzip(_file=return_zip):
    _file = zipfile.ZipFile(_file)
    _file.extractall('.')
    _file.close()
    

# Load complete CO voter file
def voters_to_df():
    _df = read_bq_table(bq_voters_table_name, voter_file_column_lst)
    #_df = pd.read_gbq(bq_query_str, project_id=bq_project_id, location=bq_project_location, credentials=bq_credentials, progress_bar_type='tqdm')
    print(f"Total Registration: {len(_df):.0f}")

    # Clean up voter file data types
    _df = _df[_df['VOTER_ID'] != 'nan']
    _df = _df.replace('nan', np.nan)
    _df['VOTER_ID'] =  _df['VOTER_ID'].astype('float').astype('int64')
    #_df['BIRTH_YEAR'] = _df['BIRTH_YEAR'].astype('float').astype('int64')

    # Replace minor party designations with 'OTH'
    _df.loc[((_df['PARTY'] != 'REP') & (_df['PARTY'] != 'DEM') & (_df['PARTY'] != 'UAF')), 'PARTY'] = 'OTH'
    _df.loc[((_df['PREFERENCE'] != 'REP') & (_df['PREFERENCE'] != 'DEM') & (_df['PREFERENCE'] != 'UAF') & (~_df['PREFERENCE'].isna())), 'PREFERENCE'] = 'OTH'
    _df['PREFERENCE'] = _df['PREFERENCE'] + ' Pref'
    
    return _df


# Load vote history dataframe with elections of interest
# def vote_history_to_df(bq_query_str=bq_history_str):
#     _df = pd.read_gbq(bq_query_str, project_id=bq_project_id, location=bq_project_location, credentials=bq_credentials, progress_bar_type='tqdm')
#     print(f"Total Vote History Records: {len(_df):.0f}")

#     # Collapse history into single binary row
#     _df = pd.get_dummies(_df.set_index('VOTER_ID')['ELECTION_DATE'])
#     _df = _df.reset_index().groupby('VOTER_ID').sum()
#     _df.reset_index(level=0, inplace=True)

#     # Fix VOTER_ID datatype
#     _df['VOTER_ID'] = _df['VOTER_ID'].astype('int64')

#     # Reset column headings to strings
#     columns_lst = list(_df)
#     columns_lst = ['VOTER_ID'] + [x.date().strftime('%Y-%m-%d') for x in columns_lst[1:]]
#     _df.columns = columns_lst

#     return _df


# Load the Colorado outstanding ballot file into a dataframe
def returns_to_df(return_txt_file):
    
    # Import returned ballots to dataframe
    print("Loading returns to dataframe.")
    ballots_sent_df = pd.DataFrame()
    ballots_sent_df = pd.read_csv (return_txt_file, sep='|', encoding='cp437', index_col=None, header=0, low_memory=False, error_bad_lines=False)

    # Create a new column that unifies the in-person & mail-in return vote dates
    print("Setting return data types.")
    
    # This line is a hard coded fix for some bad data, remove it in the future:
    ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'] = ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'].replace('11/06/0202', '11/06/2022')
    ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'] = ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'].replace('11/08/0200', '11/08/2022')
    ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'] = ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'].replace('11/08/0202', '11/08/2022')
    
    ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'].fillna(ballots_sent_df['IN_PERSON_VOTE_DATE'], inplace=True)
    ballots_sent_df['RECEIVED'] = ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE']

    # Replace minor party designations with 'OTH'
    ballots_sent_df.loc[((ballots_sent_df['VOTED_PARTY'] != 'REP') & (ballots_sent_df['VOTED_PARTY'] != 'DEM') & (ballots_sent_df['VOTED_PARTY'] != 'UAF') & (~ballots_sent_df['VOTED_PARTY'].isna())), 'VOTED_PARTY'] = 'OTH'
    ballots_sent_df['VOTED_PARTY'] = ballots_sent_df['VOTED_PARTY'] + ' Voted'

    # Save the returns up to BigQuery
    for column in list(ballots_sent_df):
        if 'DATE' in column:
            try:
                ballots_sent_df[column] = pd.to_datetime(ballots_sent_df[column], format='%m/%d/%Y', infer_datetime_format=True)
            except Exception as e:
                if 'bounds' in str(e):
                    bad_value = str(e).split(': ')[1]
                    print(f"{e}. Dropping record that contains bad value: {bad_value} from {column}.")
                    ballots_sent_df = ballots_sent_df.drop(ballots_sent_df.index[ballots_sent_df[column] == bad_value].tolist())
                    ballots_sent_df[column] = pd.to_datetime(ballots_sent_df[column], format='%m/%d/%Y', infer_datetime_format=True)                
                else:
                    raise
        else:
            ballots_sent_df[column] = ballots_sent_df[column].astype('str')
    
    print("Uploading returns to BigQuery.")
    bq_table_schema = create_bq_schema(ballots_sent_df)
    save_to_bq(ballots_sent_df, bq_table_schema, bq_return_table_id, bq_project_id)

    # Narrow returned ballots frame to only those that have come back
    ballots_sent_df = ballots_sent_df[(ballots_sent_df['RECEIVED'].notnull())]

    # Narrow returned ballots data frame to only return info
    ballots_sent_df = ballots_sent_df[['VOTER_ID', 'RECEIVED', 'VOTE_METHOD', 'VOTED_PARTY']]

    # Make the date of voting column formatting standardized
    ballots_sent_df['RECEIVED'] = pd.to_datetime(ballots_sent_df['RECEIVED']).dt.strftime('%m/%d/%Y')
    ballots_sent_df['VOTER_ID'] = ballots_sent_df['VOTER_ID'].astype('int64')

    # Output the number of rows/total voters
    print("Total Ballots Returned: {:,}".format(len(ballots_sent_df)))
    
    return ballots_sent_df

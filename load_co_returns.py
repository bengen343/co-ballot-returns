import zipfile

import pandas as pd

from config import *


# Unzip return file
def unzip(_file=return_zip):
    _file = zipfile.ZipFile(_file)
    _file.extractall('.')
    _file.close()
    

# Load complete CO voter file
def voters_to_df(bq_query_str=bq_voter_str):
    _df = pd.read_gbq(bq_query_str, project_id=bq_project_id, location=bq_project_location, credentials=bq_credentials, progress_bar_type='tqdm')
    print(f"Total Registration: {len(_df):.0f}")

    # Replace minor party designations with 'OTH'
    _df.loc[((_df['PARTY'] != 'REP') & (_df['PARTY'] != 'DEM') & (_df['PARTY'] != 'UAF')), 'PARTY'] = 'OTH'

    return _df


# Load vote history dataframe with elections of interest
def vote_history_to_df(bq_query_str=bq_history_str):
    _df = pd.read_gbq(bq_query_str, project_id=bq_project_id, location=bq_project_location, credentials=bq_credentials)
    print(f"Total Vote History Records: {len(_df):.0f}")

    # Collapse history into single binary row
    print("Collapsing history into binary values...")
    _df = pd.get_dummies(_df.set_index('VOTER_ID')['ELECTION_DATE'])
    _df = _df.reset_index().groupby('VOTER_ID').sum()
    _df.reset_index(level=0, inplace=True)

    return _df


# Load the Colorado outstanding ballot file into a dataframe
def returns_to_df(return_txt_file):
    
    # Import returned ballots to dataframe
    ballots_sent_df = pd.DataFrame()
    ballots_sent_df = pd.read_csv (return_txt_file, sep='|', encoding='cp437', index_col=None, header=0, low_memory=False, error_bad_lines=False)

    # Create a new column that unifies the in-person & mail-in return vote dates
    ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE'].fillna(ballots_sent_df['IN_PERSON_VOTE_DATE'], inplace=True)
    ballots_sent_df['RECEIVED'] = ballots_sent_df['MAIL_BALLOT_RECEIVE_DATE']

    # Narrow returned ballots frame to only those that have come back
    ballots_sent_df = ballots_sent_df[(ballots_sent_df['RECEIVED'].notnull())]

    # Narrow returned ballots data frame to only date returned and Voter ID
    ballots_sent_df = ballots_sent_df[['VOTER_ID', 'RECEIVED', 'VOTED_PARTY']]

    # Make the date of voting column formatting standardized
    ballots_sent_df['RECEIVED'] = pd.to_datetime(ballots_sent_df['RECEIVED']).dt.strftime('%m/%d/%Y')

    # Output the number of rows/total voters
    print("Total Ballots Returned: {:,}".format(len(ballots_sent_df)))
    
    return ballots_sent_df

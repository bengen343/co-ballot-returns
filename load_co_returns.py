import zipfile

import pandas as pd

from config import *


# Unzip return file
def unzip(_file=return_zip):
    _file = zipfile.ZipFile(_file)
    _file.extractall('.')
    _file.close()

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

return_txt_file = return_zip.split('.')[0] + '.txt'

unzip(_file=return_zip)
test_df = returns_to_df(return_txt_file)
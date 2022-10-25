import pandas as pd

from analyze_co_returns import calc_crosstabs
from config import *
from gcs_put import gcs_put
from load_co_returns import (returns_to_df, unzip, vote_history_to_df,
                             voters_to_df)
from transform_co_returns import calc_age, calc_pv, calc_race, calc_targets
from sos_fetch import sos_fetch
from flask import Flask

app = Flask(__name__)

@app.route('/')
def main():
    # Get the ballot returns from the Colorado Secretary of State FTP
    sos_fetch()
    
    unzip(_file=return_zip)
    returns_df = returns_to_df(return_txt_file)

    # Load the voters and their voting history from your data ware house
    voters_df = voters_to_df(bq_query_str=bq_voter_str)
    vote_history_df = vote_history_to_df(bq_query_str=bq_history_str)

    # Match the various data sources together
    voters_df = pd.merge(voters_df, vote_history_df, how='left', on='VOTER_ID')
    voters_df = pd.merge(voters_df, returns_df, how='left', on='VOTER_ID')

    # Augment the voter registration data with additional demographic information
    voters_df = calc_pv(voters_df, generals_lst=generals_lst, primaries_lst=primaries_lst)
    voters_df = calc_age(voters_df)
    voters_df = calc_race(voters_df)
    voters_df = calc_targets(voters_df, target_files_lst)

    # Change the values for UAF vote choice so they don't conflict with party affiliation
    for party in voters_df['VOTED_PARTY'].unique():
        if not pd.isnull(party):
            voters_df.loc[voters_df['VOTED_PARTY'] == party, 'VOTED_PARTY'] = ('Voted ' + party)

    # Narrow voter filedataframe to only data of interest
    voters_df = voters_df.drop_duplicates('VOTER_ID')
    voters_df = voters_df[['VOTER_ID',  'PRECINCT', 'RECEIVED'] + crosstab_criteria_lst]

    # Run crosstabs on all registered voters
    registration_crosstabs_df = calc_crosstabs(voters_df, crosstab_criteria_lst=crosstab_criteria_lst)

    # Create a new frame with only those individuals who have voted
    ballots_cast_df = voters_df[voters_df['RECEIVED'].notnull()]
    # Run crosstabs on those that have returned ballots
    ballots_crosstabs_df = calc_crosstabs(ballots_cast_df, crosstab_criteria_lst=crosstab_criteria_lst)

    # Create a dictionary of target dataframes and populate them with registration and ballots cast for their districts
    target_dataframes_dict = {}
    for geography in target_geographies_dict.keys():
        target_dataframes_dict[geography + ' Registration'] = voters_df[voters_df[target_geographies_dict.get(geography)] == geography]
        target_dataframes_dict[geography + ' Ballots Cast'] = ballots_cast_df[ballots_cast_df[target_geographies_dict.get(geography)] == geography]

    # Create crosstabs for the registration and ballot returns of each target district
    for geography in target_geographies_dict.keys():
        target_dataframes_dict[geography + ' Registration Crosstabs'] = calc_crosstabs(target_dataframes_dict[geography + ' Registration'], crosstab_criteria_lst=crosstab_criteria_lst)
        target_dataframes_dict[geography + ' Ballots Cast Crosstabs'] = calc_crosstabs(target_dataframes_dict[geography + ' Ballots Cast'], crosstab_criteria_lst=crosstab_criteria_lst)


    # Save ballots cast to Excel
    writer = pd.ExcelWriter(crosstabs_xlsx_file, engine='xlsxwriter')
    registration_crosstabs_df.to_excel(writer, 'RegistrationCrosstabs')
    ballots_crosstabs_df.to_excel(writer, 'CastCrosstabs')  
    for geography in target_geographies_dict.keys():
        target_dataframes_dict.get(geography + ' Registration Crosstabs').to_excel(writer, geography + ' Registration')
        target_dataframes_dict.get(geography + ' Ballots Cast Crosstabs').to_excel(writer, geography + ' Ballots Cast')
    writer.save()
    print("Excel Export Complete.")

    # Send the Excel file to Google Cloud Storage
    gcs_put(crosstabs_xlsx_file)
    
    return("Updated successfully.")

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=8080)

import numpy as np
import pandas as pd
from flask import Flask

from analyze_co_returns import async_crosstabs
from config import *
from extract_from_sos import returns_to_df, unzip, voters_to_df
from fetch_from_sos import sos_file_fetch
from load_to_gcp import gcs_put, save_to_bq
from transform_co_returns import calc_targets

app = Flask(__name__)

@app.route('/')
def main():
    # Get the ballot returns from the Colorado Secretary of State FTP
    sos_file_fetch(
        ftp_address=ftp_address,
        ftp_user=ftp_user,
        ftp_pass=ftp_pass,
        ftp_directory=ftp_directory,
        ftp_file=return_zip
    )
    
    # Unzip the return file and load it into a dataframe.
    unzip(file_str=return_zip)
    returns_df = returns_to_df(return_txt_file, returns_integer_col_lst)
    
    # Calculate the current total returns by county and statewide.
    sos_returns_df = pd.DataFrame(returns_df[~returns_df['RECEIVED_DATE'].isna()].value_counts('COUNTY').reset_index())
    sos_returns_df.columns = ['COUNTY', 'SOS_RETURNS']
    sos_returns_int = sos_returns_df['SOS_RETURNS'].sum()
    
    # Query BigQuery to receive the last total returns by county and statewide.
    bq_returns_df = pd.read_gbq(
        bq_returns_query_str, 
        project_id=bq_project_name, 
        location=bq_project_location, 
        credentials=bq_credentials, 
        progress_bar_type='tqdm'
    )
    bq_returns_int = bq_returns_df['BQ_RETURNS'].sum()

    # Compare the last update to the most recently downloaded return file to check for differences.
    return_counts_df = pd.merge(sos_returns_df, bq_returns_df, how='left', on='COUNTY')
    return_counts_df['SOS-BQ'] = return_counts_df['SOS_RETURNS'] - return_counts_df['BQ_RETURNS']
    
    # We only updated BigQuery and carry out the remainder of the function if the Secretary of State data hasn't shrank.
    print(f"SoS Records: {sos_returns_int:,} GBQ Records: {bq_returns_int:,} -- SoS has {(sos_returns_int - bq_returns_int):,} more records than GBQ.")
    if ((sos_returns_int - bq_returns_int) > 50) & ((return_counts_df['SOS-BQ'] >= -50).all()):
        save_to_bq(returns_df, bq_project_name, bq_return_table_id, returns_integer_col_lst)
        
        # Narrow returned ballots data frame to only necessary return info.
        returns_df = returns_df[['VOTER_ID', 'COUNTY', 'PRECINCT', 'GENDER', 'VOTE_METHOD', 'PARTY', 'PREFERENCE', 'VOTED_PARTY', 'RECEIVED_DATE']]
        # Calculate district information that isn't already present in the return file. 
        returns_df['CONGRESSIONAL'] = returns_df['PRECINCT'].apply(lambda x: 'Congressional ' + str(int(str(x)[:1])))
        returns_df['STATE_SENATE'] = returns_df['PRECINCT'].apply(lambda x: 'State Senate ' + str(int(str(x)[1:3])))
        returns_df['STATE_HOUSE'] = returns_df['PRECINCT'].apply(lambda x: 'State House ' + str(int(str(x)[3:5])))
        
        # Load the voters and their voting history from your data warehouse.
        voters_df = voters_to_df(bq_voters_query_str, voters_integer_col_lst)
        
        # Rename return columns so they don't conflict with voter file column names.
        returns_df[['PVG', 'PVP', 'RACE', 'AGE_RANGE']] = np.nan
        returns_df.columns = [f'RETURNS_{x}' if x in list(voters_df) else x for x in list(returns_df)]
        
        # Match the various data sources together
        voters_df = pd.merge(voters_df, returns_df, how='outer', left_on='VOTER_ID', right_on='RETURNS_VOTER_ID')
        # Populate missing voter file fields with those that were able to be sourced from returns.
        for column in (demographic_criteria_lst + ['VOTER_ID']):
            voters_df[column] = voters_df[column].fillna(voters_df['RETURNS_' + column])

        # Augment the voter registration data with additional demographic information
        print("Calculating targeted voters.")
        voters_df = calc_targets(voters_df, target_files_lst)

        # Narrow voter file dataframe to only data of interest.
        voters_df = voters_df.drop_duplicates('VOTER_ID')
        voters_df = voters_df[['VOTER_ID'] + crosstab_criteria_lst + ['PRECINCT', 'RECEIVED_DATE']]

        # Run crosstabs on all registered voters
        registration_crosstabs_df = async_crosstabs(crosstab_criteria_lst, voters_df)
        
        # Create a new frame with only those individuals who have voted
        ballots_cast_df = voters_df[voters_df['RECEIVED_DATE'].notnull()]
        # Run crosstabs on those that have returned ballots
        ballots_crosstabs_df = async_crosstabs(crosstab_criteria_lst, ballots_cast_df)

        # Create a dictionary of target dataframes and populate them with registration and ballots cast for their districts
        target_dataframes_dict = {}
        for geography in target_geographies_dict.keys():
            target_dataframes_dict[geography + ' Registration'] = voters_df[voters_df[target_geographies_dict.get(geography)] == geography]
            target_dataframes_dict[geography + ' Ballots Cast'] = ballots_cast_df[ballots_cast_df[target_geographies_dict.get(geography)] == geography]

        # Create crosstabs for the registration and ballot returns of each target district
        for geography in target_geographies_dict.keys():
            print(f"Running {geography} registration crosstabs.")
            target_dataframes_dict[geography + ' Registration Crosstabs'] = async_crosstabs(crosstab_criteria_lst, target_dataframes_dict[geography + ' Registration'])
            print(f"Running {geography} ballots cast crosstabs.")
            target_dataframes_dict[geography + ' Ballots Cast Crosstabs'] = async_crosstabs(crosstab_criteria_lst, target_dataframes_dict[geography + ' Ballots Cast'])

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
        gcs_put(crosstabs_xlsx_file, gcs_bucket_name)

        outcome_str = "Updated successfully."

    else:
        outcome_str = "New Secretary of State File contains no significant updates."
    
    return(outcome_str)

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=8080)

import pandas as pd
from flask import Flask

from analyze_co_returns import calc_crosstabs
from config import *
from gcs_put import gcs_put
from load_co_returns import returns_to_df, unzip, voters_to_df
from sos_fetch import sos_fetch
from transform_co_returns import calc_targets

app = Flask(__name__)

@app.route('/')
def main():
    # Get the ballot returns from the Colorado Secretary of State FTP
    sos_fetch()
    
    unzip(_file=return_zip)
    returns_df = returns_to_df(return_txt_file)

    # Load the voters and their voting history from your data ware house
    voters_df = voters_to_df()

    # Match the various data sources together
    voters_df = pd.merge(voters_df, returns_df, how='left', on='VOTER_ID')

    # Augment the voter registration data with additional demographic information
    print("Calculating targeted voters.")
    voters_df = calc_targets(voters_df, target_files_lst)

    # Narrow voter filedataframe to only data of interest
    voters_df = voters_df.drop_duplicates('VOTER_ID')
    voters_df = voters_df[['VOTER_ID'] + crosstab_criteria_lst + ['PRECINCT', 'RECEIVED']]

    # Run crosstabs on all registered voters
    print("Running Colorado registration crosstabs.")
    registration_crosstabs_df = calc_crosstabs(voters_df, crosstab_criteria_lst=crosstab_criteria_lst)

    # Create a new frame with only those individuals who have voted
    ballots_cast_df = voters_df[voters_df['RECEIVED'].notnull()]
    # Run crosstabs on those that have returned ballots
    print("Running Colorado ballots cast crosstabs.")
    ballots_crosstabs_df = calc_crosstabs(ballots_cast_df, crosstab_criteria_lst=crosstab_criteria_lst)

    # Create a dictionary of target dataframes and populate them with registration and ballots cast for their districts
    target_dataframes_dict = {}
    for geography in target_geographies_dict.keys():
        target_dataframes_dict[geography + ' Registration'] = voters_df[voters_df[target_geographies_dict.get(geography)] == geography]
        target_dataframes_dict[geography + ' Ballots Cast'] = ballots_cast_df[ballots_cast_df[target_geographies_dict.get(geography)] == geography]

    # Create crosstabs for the registration and ballot returns of each target district
    for geography in target_geographies_dict.keys():
        print(f"Running {geography} registration crosstabs.")
        target_dataframes_dict[geography + ' Registration Crosstabs'] = calc_crosstabs(target_dataframes_dict[geography + ' Registration'], crosstab_criteria_lst=crosstab_criteria_lst)
        print(f"Running {geography} ballots cast crosstabs.")
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

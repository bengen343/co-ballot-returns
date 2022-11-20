import json
import os
from datetime import date, timedelta

from google.oauth2 import service_account

# FTP variables
ftp_address = 'ftps.sos.state.co.us'
ftp_directory = r'/CE-068_Voters_With_Ballots_List_Public/'
return_zip = r'CE-068_Voters_With_Ballots_List_Public_08Nov_600017156_null.zip'
return_txt_file = return_zip.split('.')[0] + '.txt'
# Secret variables
ftp_user = os.environ.get('FTP_USER')
ftp_pass = os.environ.get('FTP_PASS')

# Google Cloud Storage/Files Variables
crosstabs_xlsx_file = r'2022COGeneralBallotsCast.xlsx'
bucket_name = 'co-ballot-returns-artifacts'
target_files_lst = [
    'gs://co-ballot-returns-artifacts/2022-general-universe-1.csv',
    'gs://co-ballot-returns-artifacts/2022-general-universe-2.csv',
    'gs://co-ballot-returns-artifacts/2022-general-universe-3.csv',
    'gs://co-ballot-returns-artifacts/2022-general-universe-4.csv', 
    'gs://co-ballot-returns-artifacts/2022-general-universe-5.csv',
    'gs://co-ballot-returns-artifacts/2022-general-affinity-1.csv',
    'gs://co-ballot-returns-artifacts/2022-general-affinity-2.csv',
    'gs://co-ballot-returns-artifacts/2022-general-affinity-3.csv'
]

# Voter file variables
demographic_criteria_lst = [
    'PARTY',
    'PREFERENCE',
    'PVP', 
    'PVG',
    'GENDER',  
    'RACE', 
    'AGE_RANGE', 
    'CONGRESSIONAL', 
    'STATE_SENATE', 
    'STATE_HOUSE', 
    'COUNTY'
]

voter_file_col_lst = ['VOTER_ID', 'PRECINCT'] + demographic_criteria_lst
crosstab_criteria_lst = demographic_criteria_lst + ['VOTED_PARTY', 'VOTE_METHOD'] + [x.split('/')[-1].split('.')[0] for x in target_files_lst]

target_geographies_dict = {
    'State Senate 20': 'STATE_SENATE'
}

# BQ Variables
bq_project_name = os.environ.get('BQ_PROJECT_ID')
bq_project_location = 'us-west1'
bq_dataset_name = 'co_voterfile'
bq_voters_table_name = f'voters_{str((date.today() + timedelta(-30)).year)}{((date.today() + timedelta(-30)).month):02d}01'
bq_return_table_name = '2022-general-returns'
bq_voters_table_id = f'{bq_project_name}.{bq_dataset_name}.{bq_voters_table_name}'
bq_return_table_id = f'{bq_project_name}.{bq_dataset_name}.{bq_return_table_name}'

bq_returns_query_str = '''
    SELECT
        COUNTY,
        COUNT(VOTER_ID) AS BQ_RETURNS
    FROM `''' + bq_return_table_id + '''`
    WHERE RECEIVED_DATE != 'nan'
    GROUP BY COUNTY
    ORDER BY COUNTY
'''

# Establish BigQuery credentials
bq_account_creds = json.loads(os.environ.get('BQ_ACCOUNT_CREDS'))
bq_credentials = service_account.Credentials.from_service_account_info(bq_account_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"])

# Data type variables
returns_integer_col_lst = [
    'VOTER_ID',
    'YOB',
    'PRECINCT'
]

voters_integer_col_lst = [
    'VOTER_ID',
    'COUNTY_CODE',
    'PRECINCT_NAME',
    'ADDRESS_LIBRARY_ID',
    'HOUSE_NUM',
    'RESIDENTIAL_ZIP_CODE',
    'RESIDENTIAL_ZIP_PLUS',
    'BIRTH_YEAR',
    'PRECINCT',
    'VOTER_STATUS_ID',
    'MAILING_ZIP_CODE',
    'MAILING_ZIP_PLUS'
]

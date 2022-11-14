import json
import os
from datetime import date

from google.oauth2 import service_account

# FTP variables
ftp_address = 'ftps.sos.state.co.us'
ftp_directory = r'/CE-068_Voters_With_Ballots_List_Public/'
return_zip = r'CE-068_Voters_With_Ballots_List_Public_08Nov_600017156_null.zip'
return_txt_file = return_zip.split('.')[0] + '.txt'

# Google Cloud Storage/Files Variables
crosstabs_xlsx_file = r'2022COGeneralBallotsCast.xlsx'

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
# Select elections of interest
generals_lst = ['2020-11-03', '2018-11-06', '2016-11-08', '2014-11-04']
primaries_lst = ['2020-06-30', '2018-06-26', '2016-06-28', '2014-06-24']

election_str = '\''
for election in (generals_lst + primaries_lst):
    election_str = election_str + '\', \'' + election
election_str = election_str + '\''
election_str = election_str[4:]

crosstab_criteria_lst = [
    'PARTY',
    'PREFERENCE', 
    'VOTED_PARTY', 
    'VOTE_METHOD',
    'PVP', 
    'PVG',
    'GENDER',  
    'RACE', 
    'AGE_RANGE', 
    'CONGRESSIONAL', 
    'STATE_SENATE', 
    'STATE_HOUSE', 
    'COUNTY', 
    'RESIDENTIAL_CITY'
] + [x.split('/')[-1].split('.')[0] for x in target_files_lst]

target_geographies_dict = {
    'State Senate 20': 'STATE_SENATE'
}

# BQ Variables
bq_project_id = os.environ.get('BQ_PROJECT_ID')
bq_project_location = 'us-west1'

bq_table_stem = bq_project_id + '.co_voterfile.'
bq_return_table_id = bq_table_stem + '2022-general-returns'
bq_table_id = bq_table_stem + 'voters_' + str(date.today().year) + f"{(date.today().month - 1):02d}" + '01'

bq_voter_str = '''
SELECT
    VOTER_ID,
    COUNTY,
    LAST_NAME,
    FIRST_NAME,
    MIDDLE_NAME,
    NAME_SUFFIX,
    RESIDENTIAL_CITY,
    BIRTH_YEAR,
    GENDER,
    RACE,
    AGE_RANGE,
    PRECINCT,
    PARTY,
    REGISTRATION_DATE,
    PREFERENCE,
    CONGRESSIONAL,
    STATE_SENATE,
    STATE_HOUSE,
    PVG,
    PVP
FROM `''' + bq_table_id + '`'

bq_history_str = '''
SELECT *
FROM `cpc-datawarehouse-51210.co_voterfile.vote-history`
WHERE ELECTION_DATE IN (''' + election_str + ''')
    OR ELECTION_DATE IS NULL'''

# Establish BigQuery credentials
bq_account_creds = json.loads(os.environ.get('BQ_ACCOUNT_CREDS'))
bq_credentials = service_account.Credentials.from_service_account_info(bq_account_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"])

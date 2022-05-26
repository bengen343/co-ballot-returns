import json
import os
from datetime import date

from google.oauth2 import service_account


# FTP variables
ftp_address = 'ftps.sos.state.co.us'
ftp_directory = r'/CE-068_Voters_With_Ballots_List_Public/'
return_zip = r'CE-068_Voters_With_Ballots_List_Public_28Jun_600017176_null.zip'

# BQ Variables
bq_project_id = os.environ.get('BQ_PROJECT_ID')
bq_project_location = 'us-west1'

bq_table_stem = bq_project_id + '.co_voterfile.'
bq_table_id = bq_table_stem + str(date.today().year) + '-' f"{(date.today().month - 1):02d}"

bq_query_str = 'SELECT * FROM ' + bq_table_id

# Establish BigQuery credentials
bq_account_creds = json.loads(os.environ.get('BQ_ACCOUNT_CREDS'))
bq_credentials = service_account.Credentials.from_service_account_info(bq_account_creds)

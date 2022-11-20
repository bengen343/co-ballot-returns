import pandas as pd
from config import *
from google.cloud import storage

def create_bq_schema(df: pd.DataFrame, integer_col_lst: list) -> list:
    schema_lst = []
    for column in list(df):
        if 'date' in column.lower():
            sql_type = 'DATE'
        elif column in integer_col_lst:
            sql_type = 'INT64'
        else:
            sql_type = 'STRING'
                
        if column == 'VOTER_ID':
            sql_mode = 'REQUIRED'
        else:
            sql_mode = 'NULLABLE'
            
        schema_lst.append({'name':  column, 'type': sql_type, 'mode': sql_mode})
    
    return schema_lst


def save_to_bq(df: pd.DataFrame, project_name:str, table_id: str, integer_col_lst: list) -> str:
    print(f"Uploading {df} to BigQuery.")
    # Create a schema to use for the BigQuery upload
    bq_table_schema = create_bq_schema(df, integer_col_lst)

    # Upload the dataframe to BigQuery using the schema just created.
    df.to_gbq(destination_table=table_id, project_id=project_name, if_exists='replace', table_schema=bq_table_schema, credentials=bq_credentials)

    return(f"Successfully uploaded {df} to  BigQuery")

def gcs_put(file: str, bucket_name) -> str:
    client = storage.Client(project=bq_project_name)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file)
    blob.upload_from_filename(file)

    return(f"Successfully uploaded {file} to GCS Bucket {bucket_name}.")

from config import *
from google.cloud import storage

def gcs_put(file):
    client = storage.Client(project=bq_project_id)
    bucket = client.get_bucket('co-ballot-returns-artifacts')
    blob = bucket.blob(file)
    blob.upload_from_filename(file)

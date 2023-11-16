from google.cloud import bigquery
import os


SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
    "https://www.googleapis.com/auth/bigquery"
]

def create_client(credential_path: str) -> bigquery.Client:
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    client = bigquery.Client()
    client._credentials._scopes = SCOPES

    return client
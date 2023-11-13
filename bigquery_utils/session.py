from google.cloud import bigquery
import os


def create_client(credential_path: str) -> bigquery.Client:
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    client = bigquery.Client()

    return client
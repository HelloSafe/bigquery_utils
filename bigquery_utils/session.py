from typing import List

from google.cloud import bigquery
import os 


def create_client(credential_path: str) -> bigquery.Client:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    client = bigquery.Client()

    return client

def get_dataset_ids(client: bigquery.Client) -> List:

    datasets = client.list_datasets()
    dataset_ids = [dataset.dataset_id for dataset in datasets]

    return dataset_ids

def get_table_ids(client: bigquery.Client, dataset_id: str) -> List:
    
    ds = client.dataset(dataset_id)
    tables = client.list_tables(ds)
    table_ids = [table.table_id for table in tables]

    return table_ids



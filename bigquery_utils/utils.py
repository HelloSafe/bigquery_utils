from typing import List, Optional

from google.cloud import bigquery
import pandas as pd

from .enums import PROJECT


def get_dataset_ids(client: bigquery.Client) -> List:

    datasets = client.list_datasets()
    dataset_ids = [dataset.dataset_id for dataset in datasets]

    return dataset_ids

def get_table_ids(client: bigquery.Client, dataset_id: str) -> List:
    
    ds = client.dataset(dataset_id)
    tables = client.list_tables(ds)
    table_ids = [table.table_id for table in tables]

    return table_ids

def get_table_path(dataset_id: str, table_id: str) -> str: 

    return f'{PROJECT}.{dataset_id}.{table_id}'

def query_to_pandas(
    client: bigquery.Client, query: str, progress_bar_type: Optional[str]=None
) -> pd.DataFrame: 

    df = client.query(query).result().to_dataframe(progress_bar_type=progress_bar_type)
    
    return df

def get_last_timestamp(client: bigquery.Client, table_path: str, timestamp_field: str) -> int:

    query = f'''
    SELECT MAX({timestamp_field}) AS last_timestamp
    FROM `{table_path}`
    '''

    result = query_to_pandas(client, query)
    last_timestamp = result.last_timestamp.values[0]

    return last_timestamp



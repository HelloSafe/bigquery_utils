from typing import List
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

def query_to_pandas(client: bigquery.Client, query: str) -> pd.DataFrame: 

    df = client.query(query).result().to_dataframe(progress_bar_type='tqdm')
    return df



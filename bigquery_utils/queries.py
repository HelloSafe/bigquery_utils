from typing import List, Optional, Union
from .enums import EVENT_PARAM_TYPES


def unnest_event_params(
    bq_table_path: str, 
    param_keys: Optional[List[str]]=None, 
    event_name: Optional[str]=None
) -> str: 

    unnested = f'''
    SELECT 
        user_pseudo_id, 
        event_timestamp, 
        event_name, 
        ep.key AS event_param_key,
        COALESCE(
            ep.value.string_value,
            CAST(ep.value.int_value AS STRING),
            CAST(ep.value.float_value AS STRING),
            CAST(ep.value.double_value AS STRING)
        ) AS event_param_value
    FROM 
        `{bq_table_path}`, 
        UNNEST(event_params) AS ep
    '''

    conditions = ''

    if param_keys is not None:
        conditions += f'ep.key IN {tuple(param_keys)}'
    if event_name is not None:
        conditions += f'\nevent_name = {event_name}'

    if conditions != '':
        unnested += f'WHERE \n{conditions}'

    return unnested

def get_dates(table_path: str, date_field: str) -> str: 

    if 'timestamp' in date_field: 
        tmp = f'''
        SELECT 
            PARSE_DATETIME('%s', CAST(TRUNC(min_timestamp) AS STRING)) AS min_datetime, 
            PARSE_DATETIME('%s', CAST(TRUNC(max_timestamp) AS STRING)) AS max_datetime
        FROM (
            SELECT 
                MIN(event_timestamp/1000000) AS min_timestamp, 
                MAX(event_timestamp/1000000) AS max_timestamp
            FROM `{table_path}` 
        )
        ''' 
        query = f'''
        WITH tmp AS ({tmp}))
        SELECT 
            DATE(min_datetime) AS start_, 
            DATE(max_datetime) AS end_
        FROM tmp
        '''

    elif 'date' in date_field: 
        query = f'''
        SELECT
            MIN({date_field}) AS start_, 
            MAX({date_field}) AS end_ 
        FROM {table_path}
        '''

    else:
        raise ValueError(f"{date_field} invalid. date_field must either contains 'timestamp' or 'date'.")

    return query

def aggregate_event_param(param_key: str) -> str:

    query = f"MAX(CASE WHEN event_param_key = '{param_key}' THEN event_param_value END) AS {param_key}"
    return query

def select_users(
    selection_field: str, 
    selection_value: Union[List, str], 
    condition_param: str='=',
    table_path: Optional[str]=None,
    table: Optional[str]=None
) -> str: 

    if isinstance(table_path, str):
        if table is None:
            table = f'''`SELECT * FROM {table_path}`'''
        else:
            raise ValueError('table_path and table cannot be both specified.')

    else:
        if table is None:
            raise ValueError('table_path and table cannot be both None.')
        else:
            table = f'({table})'

    if 'LIKE' in condition_param:
        if isinstance(selection_value, List):
            selection_value = [f'%{value}%' for value in selection_value]
        else:
            selection_value = f'%{selection_value}%'

    if isinstance(selection_value, List):
        condition = ''
        for value in selection_value[:-1]:
            condition += f"tmp.{selection_field} {condition_param} '{value}' OR "
        condition += f"tmp.{selection_field} {condition_param} '{selection_value[-1]}'"
    else:
        condition = f"tmp.{field_name} {condition_param} '{field_value}'"

    query = f'''
    WITH tmp AS ({table})
    SELECT DISTINCT(user_pseudo_id) AS user_pseudo_id
        FROM tmp
        WHERE {condition}
    '''

    return query
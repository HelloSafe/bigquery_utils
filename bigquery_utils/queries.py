from typing import List, Optional, Union
from .enums import EVENT_PARAM_TYPES


def unnest_event_params(
    bq_table_path: str, event_param_key: str, event_param_type: str
) -> str: 

    if event_param_type not in EVENT_PARAM_TYPES:
        raise ValueError(f'event_param_type must be in {EVENT_PARAM_TYPES}.')

    unnested = f'''
    SELECT 
        user_pseudo_id, 
        event_timestamp, 
        event_name, 
        COALESCE(ep.value.{event_param_type}_value) AS {event_param_key}
    FROM 
        `{bq_table_path}`, 
        UNNEST(event_params) AS ep
    WHERE
        ep.key = '{event_param_key}'
    '''

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
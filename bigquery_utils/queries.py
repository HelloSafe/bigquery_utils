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
        ep.key AS event_param_key,
        COALESCE(ep.value.{event_param_type}_value) AS event_param_value
    FROM 
        `{bq_table_path}`, 
        UNNEST(event_params) AS ep
    WHERE
        ep.key = '{event_param_key}'
    '''

    return unnested
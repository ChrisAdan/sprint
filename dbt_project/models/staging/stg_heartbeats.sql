{{ 
  config(
    materialized = 'incremental',
    unique_key   = 'session_id||heartbeat_ts'
  ) 
}}

with source as (

    select
        parse_json(raw_response) as json_data
    from {{ source('raw', 'event_session') }}

    {% if is_incremental() %}
        -- Only pull sessions with startTime later than the newest in target
        where cast(json_extract_scalar(raw_response, '$.startTime') as timestamp) 
              > (select max(session_start_time) from {{ this }})
    {% endif %}

),

exploded as (

    select
        json_data->>'sessionId'      as session_id,
        cast(json_data->>'startTime' as timestamp) as session_start_time,
        cast(json_data->>'endTime'   as timestamp) as session_end_time,
        cast(hb.value->>'timestamp'  as timestamp) as heartbeat_ts,
        hb.value->>'playerId'        as player_id,
        hb.value->>'sessionId'       as heartbeat_session_id,
        hb.value->>'teamId'          as team_id,
        cast(hb.value->>'positionX'  as double) as position_x,
        cast(hb.value->>'positionY'  as double) as position_y,
        cast(hb.value->>'positionZ'  as double) as position_z
    from source,
         lateral flatten(input => json_data->'heartbeats') as hb

)

select *
from exploded;

with activity as (
  select playerId, calendar_date
  from {{ ref('player_activity_daily') }}
),
heartbeats as (
  select distinct playerId, cast(event_datetime as date) as calendar_date
  from {{ source('raw', 'event_heartbeats') }}
)
select a.playerId, a.calendar_date
from activity a
left join heartbeats h on a.playerId = h.playerId and a.calendar_date = h.calendar_date
where h.playerId is null

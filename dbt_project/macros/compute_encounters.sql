{% macro compute_encounters(centroids_table, distance_threshold=50, cooldown_heartbeats=6) %}

with team_pairs as (
    select
        a.session_id,
        a.event_datetime,
        a.team_id as team_1_id,
        b.team_id as team_2_id,
        sqrt(
            power(a.centroid_x - b.centroid_x, 2) +
            power(a.centroid_y - b.centroid_y, 2) +
            power(a.centroid_z - b.centroid_z, 2)
        ) as distance
    from {{ centroids_table }} a
    join {{ centroids_table }} b
      on a.session_id = b.session_id
     and a.event_datetime = b.event_datetime
     and a.team_id < b.team_id
),

flagged as (
    select
        session_id,
        team_1_id,
        team_2_id,
        event_datetime,
        case when distance <= {{ distance_threshold }} then 1 else 0 end as is_close
    from team_pairs
),

lagged as (
    select
        *,
        lag(event_datetime) over (partition by session_id, team_1_id, team_2_id order by event_datetime) as lag_event_datetime
    from flagged
),

diffed as (
    select
        *,
        coalesce(
          extract(epoch from event_datetime) - extract(epoch from lag_event_datetime),
          0
        ) as seconds_since_prev
    from lagged
),

grouped_flags as (
    select
        *,
        sum(
          case 
            when is_close = 0 and seconds_since_prev > {{ cooldown_heartbeats * 30 }} then 1
            else 0
          end
        ) over (partition by session_id, team_1_id, team_2_id order by event_datetime rows between unbounded preceding and current row) as encounter_group
    from diffed
),

encounter_windows as (
    select
        session_id,
        team_1_id,
        team_2_id,
        min(event_datetime) as encounter_start,
        max(event_datetime) as encounter_end
    from grouped_flags
    where is_close = 1
    group by session_id, team_1_id, team_2_id, encounter_group
)

select * from encounter_windows
{% endmacro %}

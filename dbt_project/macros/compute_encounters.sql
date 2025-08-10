{% macro compute_encounters(centroids_table, distance_threshold=50, cooldown_seconds=180) %}

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

distance_flags as (
    select
        *,
        case when distance <= {{ distance_threshold }} then 1 else 0 end as is_close
    from team_pairs
),

ordered as (
    select
        *,
        lag(is_close) over (partition by session_id, team_1_id, team_2_id order by event_datetime) as prev_is_close,
        lag(event_datetime) over (partition by session_id, team_1_id, team_2_id order by event_datetime) as prev_event_datetime
    from distance_flags
),

-- Mark boundaries of new groups when:
-- 1) This is the first row (prev_is_close is null), OR
-- 2) Previous was not close AND current is close (encounter starts), OR
-- 3) Gap between this and previous event is > cooldown and previous was not close (long break)
group_boundaries as (
    select
        *,
        case 
          when prev_is_close is null then 1
          when is_close = 1 and prev_is_close = 0 then 1
          when extract(epoch from event_datetime - prev_event_datetime) > {{ cooldown_seconds }} and prev_is_close = 0 then 1
          else 0
        end as new_group_flag
    from ordered
),

grouped as (
    select
        *,
        sum(new_group_flag) over (partition by session_id, team_1_id, team_2_id order by event_datetime rows unbounded preceding) as encounter_group
    from group_boundaries
),

-- Filter only rows inside encounters (is_close=1) or within cooldown after encounter ended
-- Because we want to include the cooldown period after last close heartbeat as part of encounter duration
final_encounter as (
    select
        session_id,
        team_1_id,
        team_2_id,
        encounter_group,
        event_datetime,
        is_close,
        lead(event_datetime) over (partition by session_id, team_1_id, team_2_id, encounter_group order by event_datetime) as next_event_datetime
    from grouped
    where encounter_group > 0
),

-- Calculate encounter window start and end times
encounter_windows as (
    select
        session_id,
        team_1_id,
        team_2_id,
        encounter_group,
        min(event_datetime) as encounter_start,
        -- Extend encounter end by cooldown seconds after last close heartbeat
        max(
            case 
                when next_event_datetime is null then event_datetime + interval '{{ cooldown_seconds }} seconds'
                else next_event_datetime
            end
        ) as encounter_end
    from final_encounter
    group by session_id, team_1_id, team_2_id, encounter_group
),

-- Remove encounters with zero or negative duration (just in case)
cleaned_encounters as (
    select
        session_id,
        team_1_id,
        team_2_id,
        encounter_start,
        encounter_end
    from encounter_windows
    where encounter_end > encounter_start
)

select * from cleaned_encounters

{% endmacro %}

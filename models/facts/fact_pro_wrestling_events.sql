select
    d.dim_date_key,
    e.dim_event_key,
    a.dim_arena_key,
    l.dim_location_key,
    stg.event_attendance,
    count(distinct m.dim_match_key) as matches,
    round(avg(m.match_duration_seconds), 2) as avg_match_duration_seconds,
    max(m.match_duration_seconds) as longest_match_duration_seconds,
    min(m.match_duration_seconds) as shortest_match_duration_seconds,
    count(
        distinct case when m.match_title_change = true then m.dim_match_key end
    ) as matches_with_title_changes
from {{ ref("stage_pro_wrestling_events") }} stg
left join
    {{ ref("dim_event") }} e
    on stg.calendar_date = e.calendar_date
    and stg.promotion = e.promotion
    and (
        (stg.event_name = e.event_name)
        or (stg.event_name is null and e.event_name is null)
    )
    and (
        (stg.event_type = e.event_type)
        or (stg.event_type is null and e.event_type is null)
    )
    and (
        (stg.event_show_number = e.event_show_number)
        or (stg.event_show_number is null and e.event_show_number is null)
    )
    and (
        (stg.event_show_name = e.event_show_name)
        or (stg.event_show_name is null and e.event_show_name is null)
    )
left join {{ ref("dim_date") }} d on stg.calendar_date = d.calendar_date
left join
    {{ ref("dim_arena") }} a
    on (stg.event_arena_name = a.event_arena_name)
    or (stg.event_arena_name is null and a.event_arena_name is null)
left join
    {{ ref("dim_location") }} l
    on (stg.event_city = l.event_city)
    or (stg.event_city is null and l.event_city is null)
    and (stg.event_state = l.event_state)
    or (stg.event_state is null and l.event_state is null)
    and (stg.event_country = l.event_country)
    or (stg.event_country is null and l.event_country is null)
left join
    {{ ref("dim_match") }} m
    on stg.calendar_date = m.calendar_date
    and stg.promotion = m.promotion
    and (
        (stg.event_name = m.event_name)
        or (stg.event_name is null and m.event_name is null)
    )
    and (
        (stg.event_type = m.event_type)
        or (stg.event_type is null and m.event_type is null)
    )
    and (
        (stg.event_show_number = m.event_show_number)
        or (stg.event_show_number is null and m.event_show_number is null)
    )
    and (
        (stg.event_show_name = m.event_show_name)
        or (stg.event_show_name is null and m.event_show_name is null)
    )
group by 1, 2, 3, 4, 5
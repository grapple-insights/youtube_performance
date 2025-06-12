select
    d.dim_date_key,
    m.dim_match_key,
    m.dim_event_key,
    l.dim_location_key,
    a.dim_arena_key,
    case when m.match_type is not null then true else false end as is_match_stipulation,
    case
        when m.match_time_limit is not null then true else false
    end as is_time_limit_draw,
    m.match_title_change as is_match_title_change,
    m.match_duration_seconds
from {{ ref("stage_pro_wrestling_matches") }} stg
left join {{ ref("dim_date") }} d on stg.calendar_date = d.calendar_date
left join
    {{ ref("dim_arena") }} a
    on (stg.event_arena_name = a.event_arena_name)
    or (stg.event_arena_name is null and a.event_arena_name is null)
left join {{ ref("dim_location") }} l
    on (
        stg.match_city = l.event_city
        and stg.match_state = l.event_state
        and stg.match_country = l.event_country
    )
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
        (stg.event_show_name = m.event_show_name)
        or (stg.event_show_name is null and m.event_show_name is null)
    )
    and (
        (stg.event_show_number = m.event_show_number)
        or (stg.event_show_number is null and m.event_show_number is null)
    )
    and (
        (stg.match_type = m.match_type)
        or (stg.match_type is null and m.match_type is null)
    )
    and (
        (stg.match_winner = m.match_winner)
        or (stg.match_winner is null and m.match_winner is null)
    )
    and (
        (stg.match_loser = m.match_loser)
        or (stg.match_loser is null and m.match_loser is null)
    )
    and (
        (stg.match_ended_by = m.match_ended_by)
        or (stg.match_ended_by is null and m.match_ended_by is null)
    )
    and stg.match_duration_seconds = m.match_duration_seconds
    and stg.match_title_change = m.match_title_change
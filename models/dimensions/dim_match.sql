select distinct
    {{
        dbt_utils.generate_surrogate_key(
            [
                "stg.calendar_date",
                "stg.promotion",
                "stg.event_name",
                "stg.event_type",
                "stg.event_show_number",
                "stg.event_show_name",
                "stg.match_type",
                "stg.match_winner",
                "stg.match_loser",
                "stg.match_ended_by",
                "stg.match_duration_seconds",
                "stg.match_title_change",
            ]
        )
    }} as dim_match_key,
    stg.calendar_date,
    stg.promotion,
    stg.event_name,
    stg.event_type,
    stg.event_show_number,
    stg.event_show_name,
    stg.event_arena_name,
    stg.match_type,
    stg.match_winner,
    stg.match_loser,
    stg.match_ended_by,
    stg.match_duration_seconds,
    stg.match_time_limit,
    stg.match_title_change,
    e.dim_event_key
from {{ ref("stage_pro_wrestling_matches") }} stg
left join {{ ref("dim_event") }} e 
on stg.calendar_date = e.calendar_date
and stg.promotion = e.promotion
and ((stg.event_name = e.event_name) or (stg.event_name is null and e.event_name is null))
and ((stg.event_type = e.event_type) or (stg.event_type is null and e.event_type is null))
and ((stg.event_show_name = e.event_show_name) or (stg.event_show_name is null and e.event_show_name is null))
and ((stg.event_show_number = e.event_show_number) or (stg.event_show_number is null and e.event_show_number is null))
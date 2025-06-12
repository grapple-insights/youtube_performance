select 
    d.calendar_date,
    e.event_name,
    e.event_type,
    e.event_show_name,
    e.event_show_number,
    e.event_broadcast_type,
    e.event_broadcast_network,
    a.event_arena_name,
    l.event_city,
    l.event_state,
    l.event_country,
    m.match_type,
    m.match_winner,
    m.match_loser,
    m.match_ended_by,
    m.match_time_limit,
    m.match_title_change,
    m.match_duration_seconds
from {{ ref("fact_pro_wrestling_matches") }} f 

left join {{ ref('dim_date') }} d
  on f.dim_date_key = d.dim_date_key

left join {{ ref('dim_event') }} e
  on f.dim_event_key = e.dim_event_key

left join {{ ref('dim_arena') }} a
  on f.dim_arena_key = a.dim_arena_key

left join {{ ref('dim_location') }} l
  on f.dim_location_key = l.dim_location_key

left join {{ ref('dim_match' )}} m
on f.dim_match_key = m.dim_match_key
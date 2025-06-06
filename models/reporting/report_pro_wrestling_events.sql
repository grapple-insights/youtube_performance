select 
    d.calendar_date
    , d.calendar_year
    , d.day_name
    , d.month_name
    , d.calendar_quarter
    , d.is_weekend
    , e.brand
    , e.event_name
    , e.event_type
    , e.broadcast_type
    , e.broadcast_network
    , e.show_name
    , e.show_number
    , a.arena_name
    , l.city
    , l.state
    , l.country
    , f.attendance
from {{ ref('fact_pro_wrestling_events') }} f

left join {{ ref('dim_date') }} d
  on f.dim_date_key = d.dim_date_key

left join {{ ref('dim_event') }} e
  on f.dim_event_key = e.dim_event_key

left join {{ ref('dim_arena') }} a
  on f.dim_arena_key = a.dim_arena_key

left join {{ ref('dim_location') }} l
  on f.dim_location_key = l.dim_location_key
where e.brand in ('AEW', 'WWE')
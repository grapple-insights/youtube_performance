select
    d.dim_date_key
  , e.dim_event_key
  , a.dim_arena_key
  , l.dim_location_key

  -- Example measures (replace/add as needed)
  , stg.attendance

from {{ ref('stage_pro_wrestling_events') }} stg

left join {{ ref('dim_event') }} e
  on stg.event_name = e.event_name_full

left join {{ ref('dim_date') }} d
  on stg.calendar_date = d.calendar_date

left join {{ ref('dim_arena') }} a
  on stg.arena = a.arena_name

left join {{ ref('dim_location') }} l
  on stg.location = l.location

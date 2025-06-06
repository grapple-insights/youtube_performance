with cte_location_base as 
(
select distinct location
from {{ ref('stage_pro_wrestling_events') }}
where location is not null
)

select
    {{ dbt_utils.generate_surrogate_key([
        "location"
    ]) }} as dim_location_key
  , location
  , trim(split(location, ',')[safe_offset(0)]) as city
  , trim(split(location, ',')[safe_offset(1)]) as state
  , trim(split(location, ',')[safe_offset(2)]) as country
from cte_location_base

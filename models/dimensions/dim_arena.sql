with cte_arena_base as 
(
    select distinct arena
    from {{ ref('stage_pro_wrestling_events') }}
    where arena is not null
)

select 
  {{ dbt_utils.generate_surrogate_key([
      "trim(arena)"
  ]) }} as dim_arena_key
, arena as arena_name
from cte_arena_base

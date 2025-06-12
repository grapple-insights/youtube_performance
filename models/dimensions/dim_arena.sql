select distinct
    {{ dbt_utils.generate_surrogate_key(["event_arena_name"]) }} as dim_arena_key,
    event_arena_name
from {{ ref("stage_pro_wrestling_events") }}
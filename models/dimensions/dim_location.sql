select distinct
    {{
        dbt_utils.generate_surrogate_key(
            ["event_city", 
            "event_state", 
            "event_country"]
        )
    }} as dim_location_key, 
    event_city, 
    event_state, 
    event_country
from {{ ref("stage_pro_wrestling_events") }}
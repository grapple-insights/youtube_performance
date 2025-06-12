
select distinct
    {{
        dbt_utils.generate_surrogate_key(
            [
                "promotion",
                "event_name",
                "event_type",
                "event_show_name",
                "event_show_number",
                "event_broadcast_type",
                "event_broadcast_network",
                "calendar_date",
            ]
        )
    }} as dim_event_key,
    calendar_date,
    promotion,
    event_name,
    event_type,
    event_show_name,
    event_show_number,
    event_broadcast_type,
    event_broadcast_network
from {{ ref("stage_pro_wrestling_events") }}
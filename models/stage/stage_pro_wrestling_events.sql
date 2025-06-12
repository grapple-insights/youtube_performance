-- stage table for raw pro wrestling events
-- pulled from BigQuery
-- data from cagematch.net
-- filters for dates after 1/1/2021
-- filters for TV-Show, PPV, PLE event types
select
    safe.parse_date('%d.%m.%Y', `date`) as calendar_date,
    -- Brand: before '/' if present, else first word
    case
        when instr(event_name, '/') > 0 and instr(event_name, 'AEW') > 0
        then 'AEW'
        when instr(event_name, '/') > 0
        then trim(split(event_name, '/')[offset(0)])
        when trim(split(event_name, ' ')[offset(0)]) = 'NJPW'
        then 'AEW'
        else trim(split(event_name, ' ')[offset(0)])
    end as promotion,
    -- Event name: handles all formats
    case
        when instr(event_name, '/') > 0
        then
            trim(
                array_to_string(
                    array_slice(
                        split(
                            trim(
                                split(event_name, '/')[
                                    safe_offset(
                                        array_length(split(event_name, '/')) - 1
                                    )
                                ]
                            ),
                            ' '
                        ),
                        1,
                        1000
                    ),
                    ' '
                )
            )
        when regexp_contains(event_name, r'#\d+')
        then trim(regexp_extract(event_name, r'^[^ ]+\s+(.*?)\s+#\d+'))
        when instr(event_name, '-') > 0
        then trim(regexp_extract(event_name, r'^[^ ]+\s+(.*?)\s+-'))
        else trim(regexp_extract(event_name, r'^[^ ]+\s+(.*)$'))
    end as event_name,

    event_type,
    -- Show name: after '-' if present
    case
        when instr(event_name, '-') > 0
        then
            nullif(
                trim(
                    regexp_replace(
                        split(event_name, '-')[safe_offset(1)], r' *Tag \d+ *', ''
                    )
                ),
                ''
            )
        else null
    end as event_show_name,
    -- Show number: digits after the last '#', if present
    regexp_extract(event_name, r'#([0-9]+(?:\.[0-9]+)?)') as event_show_number,
    arena as event_arena_name,
    -- City: only if 2 or 3 parts
    case
        when array_length(split(location, ',')) = 3
        then trim(split(location, ',')[safe_offset(0)])
        when array_length(split(location, ',')) = 2
        then trim(split(location, ',')[safe_offset(0)])
        else null
    end as event_city,
    -- State: only if 3 parts
    case
        when array_length(split(location, ',')) = 3
        then trim(split(location, ',')[safe_offset(1)])
        else null
    end as event_state,
    -- Country: 3rd part if 3 parts, 2nd if 2 parts, only part if 1 part
    case
        when array_length(split(location, ',')) = 3
        then trim(split(location, ',')[safe_offset(2)])
        when array_length(split(location, ',')) = 2
        then trim(split(location, ',')[safe_offset(1)])
        when array_length(split(location, ',')) = 1
        then trim(location)
        else null
    end as event_country,
    broadcast_type as event_broadcast_type,
    broadcast_network as event_broadcast_network,
    safe_cast(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower(attendance), r'^(ca\.|approx\.|~|about|around)\s*', ''  -- Remove common prefixes
                ),
                r'[^0-9]',
                ''  -- Remove all non-numeric characters
            ),
            r'^0+',
            ''  -- Remove leading zeros
        ) as int64
    ) as event_attendance,
    event_name as event_name_original,
    location as event_location_original
from {{ source("pro_wrestling_events", "raw_events") }}
where
    safe.parse_date('%d.%m.%Y', `date`) is not null
    and safe.parse_date('%d.%m.%Y', `date`) between '2021-01-01' and current_date
    and event_type in ('TV-Show', 'Pay Per View', 'Premium Live Event')
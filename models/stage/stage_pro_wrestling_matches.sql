-- stage table for raw pro wrestling matches
-- pulled from BigQuery
-- data from cagematch.net
-- filters for dates after 1/1/2021 in the underlying data 
-- filters for TV-Show, PPV, PLE event types in the underlying data
select
    safe.parse_date('%d.%m.%Y', `date`) as calendar_date,

    case
        when `promotion` in ('All Elite Wrestling', 'New Japan Pro Wrestling')
        then 'AEW'
        when `promotion` = 'World Wrestling Entertainment'
        then 'WWE'
    end as promotion,

    replace(match_type, ":", "") as match_type,

    -- Winner: Everything before ' defeat' or ' defeats'
    regexp_replace(
        regexp_extract(match, r'^(.*?) defeat[s]?'), r' *\([^)]*\)', ''
    ) as match_winner,

    -- Loser: Everything after 'defeat' or 'defeats ' up to ' by ', ' (', or ' -'
    regexp_replace(
        regexp_extract(match, r'defeat[s]? (.*?)(?: by | \(| -)'), r' *\([^)]*\)', ''
    ) as match_loser,

    -- Match Ended: Any phrase starting with 'by ' after loser
    regexp_extract(match, r'defeat[s]? .*? by ([^()\-]+)') as match_ended_by,

    -- Match Time: Text inside parentheses that looks like a time
    cast(
        split(regexp_extract(match, r'\((\d{1,2}:\d{2})\)'), ':')[
            safe_offset(0)
        ] as int64
    )
    * 60
    + cast(
        split(regexp_extract(match, r'\((\d{1,2}:\d{2})\)'), ':')[
            safe_offset(1)
        ] as int64
    ) as match_duration_seconds,

    -- Add the wrestlers names in case there is a time limit draw. otherwise winner
    -- and loser columns are null
    case
        when
            regexp_replace(
                regexp_extract(match, r'^(.*?) defeat[s]?'), r' *\([^)]*\)', ''
            )
            is null
            and regexp_replace(
                regexp_extract(match, r'defeat[s]? (.*?)(?: by | \(| -)'),
                r' *\([^)]*\)',
                ''
            )
            is null
            and regexp_contains(lower(match), r' vs\. ')
        then regexp_extract(match, r'^(.*? vs\. .*?)(?: -| \(|$)')

        else null
    end as match_time_limit,

    -- Title Change: True if 'TITLE CHANGE' is present (case-insensitive)
    if(
        regexp_contains(lower(match), r'title change'), true, false
    ) as match_title_change,

    -- Event Name: robust extraction, strips brands and show numbers
    trim(
        regexp_replace(
            regexp_extract(event_type_arena_location, r'^(?:[A-Z0-9/]+ )?([^-@#]+)'),
            r' $',
            ''
        )
    ) as event_name,

    -- Event Type: last hyphen-separated value before '@'
    trim(
        array_reverse(
            split(split(event_type_arena_location, ' @ ')[safe_offset(0)], ' - ')
        )[safe_offset(0)]
    ) as event_type,

    regexp_extract(
        event_type_arena_location, r'#([0-9]+(?:\.[0-9]+)?)'
    ) as event_show_number,

    -- Extract event_show_name only if it is not an event type (TV-Show, Pay Per View,
    -- Premium Live Event, etc.)
    case
        when
            trim(regexp_extract(event_type_arena_location, r'^[^-]+ - ([^-@]+)')) in (
                'Pay Per View',
                'TV-Show',
                'Premium Live Event',
                'TV',
                'PPV',
                'PLE',
                'Blood & Guts 2021'  -- Add any other special events you want to exclude
            )
            or regexp_contains(
                trim(regexp_extract(event_type_arena_location, r'^[^-]+ - ([^-@]+)')),
                r'^Tag \d+$'
            )
        then null
        else trim(regexp_extract(event_type_arena_location, r'^[^-]+ - ([^-@]+)'))
    end as event_show_name,

    -- Arena Name: between '@' and 'in'
    trim(
        split(split(event_type_arena_location, ' @ ')[safe_offset(1)], ' in ')[
            safe_offset(0)
        ]
    ) as event_arena_name,

    -- City
    case
        when
            array_length(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )
            )
            = 3
        then
            trim(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )[safe_offset(0)]
            )
        when
            array_length(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )
            )
            = 2
        then
            trim(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )[safe_offset(0)]
            )
        else null
    end as match_city,

    -- State
    case
        when
            array_length(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )
            )
            = 3
        then
            trim(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )[safe_offset(1)]
            )
        else null
    end as match_state,

    -- Country
    case
        when
            array_length(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )
            )
            = 3
        then
            trim(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )[safe_offset(2)]
            )
        when
            array_length(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )
            )
            = 2
        then
            trim(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )[safe_offset(1)]
            )
        when
            array_length(
                split(
                    trim(split(event_type_arena_location, ' in ')[safe_offset(1)]), ','
                )
            )
            = 1
        then trim(trim(split(event_type_arena_location, ' in ')[safe_offset(1)]))
        else null
    end as match_country,

    event_type_arena_location as match_event_type_arena_location_original,

    match as match_original

from {{ source("pro_wrestling_events", "raw_matches") }}
where safe.parse_date('%d.%m.%Y', `date`) is not null
    
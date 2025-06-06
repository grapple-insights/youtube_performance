with cte_parsed_events as (
  select
    trim(event_name) as event_name_full

    -- Brand: before '/' if present, else first word
    , case
        when instr(event_name, '/') > 0 then trim(split(event_name, '/')[offset(0)])
        else trim(split(event_name, ' ')[offset(0)])
      end as brand

    -- Show number: digits after the last '#', if present
    , regexp_extract(event_name, r'#(\d+)') as show_number

    -- Show name: after '-' if present
    , case
        when instr(event_name, '-') > 0 then trim(split(event_name, '-')[safe_offset(1)])
        else null
      end as show_name

    -- Event name: handles all formats
    , case
        when instr(event_name, '/') > 0 then
          trim(regexp_extract(event_name, r'/\s*([^\#-]+)'))
        when regexp_contains(event_name, r'#\d+') then
          trim(regexp_extract(event_name, r'^[^ ]+\s+(.*?)\s+#\d+'))
        when instr(event_name, '-') > 0 then
          trim(regexp_extract(event_name, r'^[^ ]+\s+(.*?)\s+-'))
        else
          trim(regexp_extract(event_name, r'^[^ ]+\s+(.*)$'))
      end as event_name

    -- Add the extra fields
    , event_type
    , broadcast_type
    , broadcast_network

  from (
    select distinct
      event_name
      , event_type
      , broadcast_type
      , broadcast_network
    from {{ ref('stage_pro_wrestling_events') }}
    where event_name is not null
  )
)

select
  {{ dbt_utils.generate_surrogate_key([
      "brand"
    , "event_name"
    , "show_number"
    , "show_name"
    , "event_type"
    , "broadcast_type"
    , "broadcast_network"
  ]) }} as dim_event_key
  , brand
  , event_name
  , show_number
  , show_name
  , event_type
  , broadcast_type
  , broadcast_network
  , event_name_full
from cte_parsed_events
